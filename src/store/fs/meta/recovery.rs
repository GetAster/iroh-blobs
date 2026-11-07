//! Recovery after the store may have been open during an OS crash or power loss.
//!
//! With [`CrashConsistency::ProcessCrash`](crate::store::fs::options::CrashConsistency)
//! no data files are synced, so an OS crash can leave entries whose files lost
//! writes that the database already recorded.
//!
//! Recovery does not read any data. It turns every entry with owned files into
//! a partial entry without a bitfield. When such an entry is loaded, its
//! bitfield is rebuilt from what the files really hold, and if that is all of
//! the blob, it becomes complete again. Until then it claims less than it has,
//! never more.

use std::{fs, io, path::Path};

use bao_tree::io::sync::WriteAt;
use bytes::Bytes;
use n0_error::StdResultExt;
use redb::ReadableTable;

use super::{row::to_owned_state, tables::Tables, ActorError};
use crate::{
    store::{
        fs::{
            entry_state::{DataLocation, EntryState, OutboardLocation},
            options::PathOptions,
        },
        util::size_slot,
    },
    Hash,
};

/// What recovery did.
#[derive(Debug, Default)]
pub(super) struct Recovered {
    /// Complete entries with owned files, now partial until they are loaded.
    pub complete: usize,
    /// Partial entries, whose bitfield is rebuilt when they are loaded.
    pub partial: usize,
    /// Complete entries with external data, left as they are since the data is not ours.
    pub external: usize,
}

/// Makes every entry with owned files verify itself against its files when it is next loaded.
///
/// Entries that live in the database alone cannot have lost anything.
pub(super) fn recover(
    tables: &mut Tables<'_>,
    paths: &PathOptions,
) -> Result<Recovered, ActorError> {
    let mut entries = Vec::new();
    for item in tables.blobs.iter()? {
        let (hash, state) = item?;
        entries.push((hash.value(), to_owned_state(state.value())));
    }
    let mut res = Recovered::default();
    for (hash, state) in entries {
        match state {
            EntryState::Partial { .. } => {
                remove_if_exists(&paths.bitfield_path(&hash)).std_context("removing a bitfield")?;
                res.partial += 1;
            }
            EntryState::Complete {
                data_location: DataLocation::Inline(_),
                outboard_location: OutboardLocation::Inline(_) | OutboardLocation::NotNeeded,
            } => {}
            EntryState::Complete {
                data_location: DataLocation::External(..),
                ..
            } => res.external += 1,
            EntryState::Complete {
                data_location,
                outboard_location,
            } => {
                let size = match &data_location {
                    DataLocation::Inline(data) => data.len() as u64,
                    DataLocation::Owned(size) | DataLocation::External(_, size) => *size,
                };
                make_partial(
                    tables,
                    paths,
                    &hash,
                    size,
                    &data_location,
                    &outboard_location,
                )?;
                res.complete += 1;
            }
        }
    }
    Ok(res)
}

/// Turns a complete entry into a partial one with the same files.
///
/// Inline parts are written to files, since a partial entry lives in files
/// only; missing files are created when it is loaded. The bitfield file is
/// removed, so the load rebuilds it from what the files really hold.
fn make_partial(
    tables: &mut Tables<'_>,
    paths: &PathOptions,
    hash: &Hash,
    size: u64,
    data_location: &DataLocation<Bytes, u64>,
    outboard_location: &OutboardLocation<Bytes>,
) -> Result<(), ActorError> {
    if let DataLocation::Inline(data) = data_location {
        fs::write(paths.data_path(hash), data).std_context("writing data")?;
    }
    if let OutboardLocation::Inline(outboard) = outboard_location {
        fs::write(paths.outboard_path(hash), outboard).std_context("writing outboard")?;
    }
    // the size goes in the slot of the last chunk group
    let mut sizes = fs::File::create(paths.sizes_path(hash)).std_context("creating sizes")?;
    sizes
        .write_all_at(size_slot(size.saturating_sub(1)), &size.to_le_bytes())
        .std_context("writing sizes")?;
    remove_if_exists(&paths.bitfield_path(hash)).std_context("removing a bitfield")?;
    tables
        .blobs
        .insert(hash, EntryState::Partial { size: Some(size) })?;
    Ok(())
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        res => res,
    }
}
