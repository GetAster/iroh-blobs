//! The value of the `blobs-1` table: an entry state with its inline data and
//! inline outboard embedded, so a complete inline blob is exactly one row.
//!
//! Encoding, version 1 (the version is carried by the table name `blobs-1`
//! and by the redb type name [`BlobRow::type_name`], so a later change shows
//! up as a type mismatch when the table is opened):
//!
//! ```text
//! postcard(EntryState<()>) ++ trailer
//! ```
//!
//! The header is the postcard encoding of the entry state with the inline
//! payloads erased, which is byte-for-byte what the legacy `blobs-0` table
//! stored. File-backed and partial entries have an empty trailer, so they are
//! exactly as compact as before. The trailer depends on which payloads are
//! inline, as announced by the header:
//!
//! - data inline only: the data bytes;
//! - outboard inline only: the outboard bytes;
//! - both: `u32` little-endian data length, the data bytes, the outboard bytes;
//! - neither: empty.
//!
//! A store with `blobs-1` can no longer be opened by code that only knows
//! `blobs-0`, `inline-data-0` and `inline-outboard-0`.
use serde::{Serialize, Serializer};

use crate::store::fs::entry_state::{DataLocation, EntryState, OutboardLocation};

/// Marker type for the version 1 row encoding. Its `SelfType` borrows the
/// inline payloads from the database page, so reading a row never copies them.
#[derive(Debug)]
pub(crate) struct BlobRow;

/// Serializes as a unit, i.e. to zero bytes in postcard, standing in for an
/// inline payload that is stored in the trailer instead.
struct Erased;

impl Serialize for Erased {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_unit()
    }
}

fn erased(state: &EntryState<&[u8]>) -> EntryState<Erased> {
    match state {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => EntryState::Complete {
            data_location: match data_location {
                DataLocation::Inline(_) => DataLocation::Inline(Erased),
                DataLocation::Owned(size) => DataLocation::Owned(*size),
                DataLocation::External(paths, size) => DataLocation::External(paths.clone(), *size),
            },
            outboard_location: match outboard_location {
                OutboardLocation::Inline(_) => OutboardLocation::Inline(Erased),
                OutboardLocation::Owned => OutboardLocation::Owned,
                OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
            },
        },
        EntryState::Partial { size } => EntryState::Partial { size: *size },
    }
}

/// Encode a row.
pub(crate) fn encode_row(state: &EntryState<&[u8]>) -> Vec<u8> {
    let mut out = postcard::to_extend(&erased(state), Vec::new()).unwrap();
    if let EntryState::Complete {
        data_location,
        outboard_location,
    } = state
    {
        let data = match data_location {
            DataLocation::Inline(d) => Some(*d),
            _ => None,
        };
        let outboard = match outboard_location {
            OutboardLocation::Inline(o) => Some(*o),
            _ => None,
        };
        match (data, outboard) {
            (Some(d), Some(o)) => {
                out.extend_from_slice(&(d.len() as u32).to_le_bytes());
                out.extend_from_slice(d);
                out.extend_from_slice(o);
            }
            (Some(x), None) | (None, Some(x)) => out.extend_from_slice(x),
            (None, None) => {}
        }
    }
    out
}

/// Decode a row, borrowing the inline payloads from `bytes`.
pub(crate) fn decode_row(bytes: &[u8]) -> EntryState<&[u8]> {
    let (header, trailer) = postcard::take_from_bytes::<EntryState<()>>(bytes).unwrap();
    match header {
        EntryState::Partial { size } => EntryState::Partial { size },
        EntryState::Complete {
            data_location,
            outboard_location,
        } => {
            let data_inline = matches!(data_location, DataLocation::Inline(()));
            let outboard_inline = matches!(outboard_location, OutboardLocation::Inline(()));
            let (data, outboard) = match (data_inline, outboard_inline) {
                (true, true) => {
                    let (len, rest) = trailer.split_at(4);
                    let len = u32::from_le_bytes(len.try_into().unwrap()) as usize;
                    let (d, o) = rest.split_at(len);
                    (Some(d), Some(o))
                }
                (true, false) => (Some(trailer), None),
                (false, true) => (None, Some(trailer)),
                (false, false) => (None, None),
            };
            EntryState::Complete {
                data_location: match data_location {
                    DataLocation::Inline(()) => DataLocation::Inline(data.unwrap()),
                    DataLocation::Owned(size) => DataLocation::Owned(size),
                    DataLocation::External(paths, size) => DataLocation::External(paths, size),
                },
                outboard_location: match outboard_location {
                    OutboardLocation::Inline(()) => OutboardLocation::Inline(outboard.unwrap()),
                    OutboardLocation::Owned => OutboardLocation::Owned,
                    OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
                },
            }
        }
    }
}

impl redb::Value for BlobRow {
    type SelfType<'a> = EntryState<&'a [u8]>;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        decode_row(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        encode_row(value)
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("BlobRow/v1")
    }
}

/// Borrow an owned entry state as a row value.
pub(crate) fn as_slices(state: &EntryState<bytes::Bytes>) -> EntryState<&[u8]> {
    match state {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => EntryState::Complete {
            data_location: match data_location {
                DataLocation::Inline(d) => DataLocation::Inline(d.as_ref()),
                DataLocation::Owned(size) => DataLocation::Owned(*size),
                DataLocation::External(paths, size) => DataLocation::External(paths.clone(), *size),
            },
            outboard_location: match outboard_location {
                OutboardLocation::Inline(o) => OutboardLocation::Inline(o.as_ref()),
                OutboardLocation::Owned => OutboardLocation::Owned,
                OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
            },
        },
        EntryState::Partial { size } => EntryState::Partial { size: *size },
    }
}

/// Copy a borrowed row into an owned entry state.
pub(crate) fn to_owned_state(state: EntryState<&[u8]>) -> EntryState<bytes::Bytes> {
    match state {
        EntryState::Complete {
            data_location,
            outboard_location,
        } => EntryState::Complete {
            data_location: match data_location {
                DataLocation::Inline(d) => DataLocation::Inline(bytes::Bytes::copy_from_slice(d)),
                DataLocation::Owned(size) => DataLocation::Owned(size),
                DataLocation::External(paths, size) => DataLocation::External(paths, size),
            },
            outboard_location: match outboard_location {
                OutboardLocation::Inline(o) => {
                    OutboardLocation::Inline(bytes::Bytes::copy_from_slice(o))
                }
                OutboardLocation::Owned => OutboardLocation::Owned,
                OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
            },
        },
        EntryState::Partial { size } => EntryState::Partial { size },
    }
}
