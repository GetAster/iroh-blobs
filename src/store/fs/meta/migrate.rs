//! One-way migration of the legacy metadata layout to the merged `blobs-1` row.
//!
//! The legacy layout kept an entry's state in `blobs-0`, its inline data in
//! `inline-data-0` and its inline outboard in `inline-outboard-0`, all keyed by
//! hash. The new layout has one row per entry (see [`super::row`]), so a small
//! blob dirties one random leaf per insert instead of two or three.
//!
//! [`migrate`] runs when the store is opened. It moves the rows in bounded
//! write transactions. Each transaction copies a chunk of the lowest legacy
//! keys into `blobs-1` and removes exactly those keys from the legacy tables,
//! so a row is in one table or the other, never both and never neither.
//! An interrupted migration therefore resumes at the next open from whatever
//! is left in `blobs-0`; there is no separate progress marker. When `blobs-0`
//! is empty, the last transaction deletes the three legacy tables.
//!
//! Once any chunk has committed, code that only knows the legacy tables can no
//! longer read the store. This is accepted.
use std::time::Instant;

use bytes::Bytes;
use redb::{Database, ReadableTable, TableDefinition, TableHandle};
use smallvec::SmallVec;
use tracing::{info, warn};

use super::{row::as_slices, tables::BLOBS_TABLE, ActorError, ActorResult};
use crate::{
    store::fs::entry_state::{DataLocation, EntryState, OutboardLocation},
    Hash,
};

/// Marker for the legacy `blobs-0` value: postcard of `EntryState<()>`.
#[derive(Debug)]
pub(super) struct LegacyEntryState;

impl redb::Value for LegacyEntryState {
    type SelfType<'a> = EntryState;

    type AsBytes<'a> = SmallVec<[u8; 128]>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        postcard::from_bytes(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        postcard::to_extend(value, SmallVec::new()).unwrap()
    }

    fn type_name() -> redb::TypeName {
        // The legacy type name, so the legacy table opens.
        redb::TypeName::new("EntryState")
    }
}

pub(super) const LEGACY_BLOBS: TableDefinition<Hash, LegacyEntryState> =
    TableDefinition::new("blobs-0");
pub(super) const LEGACY_INLINE_DATA: TableDefinition<Hash, &[u8]> =
    TableDefinition::new("inline-data-0");
pub(super) const LEGACY_INLINE_OUTBOARD: TableDefinition<Hash, &[u8]> =
    TableDefinition::new("inline-outboard-0");

/// How much one migration transaction may move.
#[derive(Debug, Clone, Copy)]
pub(super) struct Chunk {
    /// Most rows moved by one transaction.
    pub rows: usize,
    /// Stop a chunk once this many payload bytes are buffered.
    pub bytes: usize,
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            rows: 20_000,
            bytes: 16 * 1024 * 1024,
        }
    }
}

/// Result of one migration transaction.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Step {
    /// There are no legacy tables.
    NotLegacy,
    /// A chunk was moved; more may remain.
    Moved { rows: u64, dropped: u64 },
    /// The legacy tables were empty and have been deleted.
    Finished,
}

/// Move one chunk, or finish if nothing is left.
pub(super) fn step(db: &Database, chunk: Chunk) -> ActorResult<Step> {
    let tx = db.begin_write()?;
    let legacy = tx
        .list_tables()
        .map_err(|e| n0_error::e!(ActorError::Storage, e))?
        .any(|t| t.name() == LEGACY_BLOBS.name());
    if !legacy {
        return Ok(Step::NotLegacy);
    }
    let mut old_blobs = tx.open_table(LEGACY_BLOBS)?;
    let mut old_data = tx.open_table(LEGACY_INLINE_DATA)?;
    let mut old_outboard = tx.open_table(LEGACY_INLINE_OUTBOARD)?;
    let mut new_blobs = tx.open_table(BLOBS_TABLE)?;

    let storage = |e| n0_error::e!(ActorError::Storage, e);
    let mut moved: Vec<(Hash, Option<EntryState<Bytes>>)> = Vec::new();
    let mut buffered = 0usize;
    for item in old_blobs.iter().map_err(storage)? {
        if moved.len() >= chunk.rows || buffered >= chunk.bytes {
            break;
        }
        let (k, v) = item.map_err(storage)?;
        let hash = k.value();
        let state = match v.value() {
            EntryState::Partial { size } => Some(EntryState::Partial { size }),
            EntryState::Complete {
                data_location,
                outboard_location,
            } => 'complete: {
                let data_location = match data_location {
                    DataLocation::Inline(()) => {
                        let Some(d) = old_data.get(hash).map_err(storage)? else {
                            break 'complete None;
                        };
                        buffered += d.value().len();
                        DataLocation::Inline(Bytes::copy_from_slice(d.value()))
                    }
                    DataLocation::Owned(size) => DataLocation::Owned(size),
                    DataLocation::External(paths, size) => DataLocation::External(paths, size),
                };
                let outboard_location = match outboard_location {
                    OutboardLocation::Inline(()) => {
                        let Some(o) = old_outboard.get(hash).map_err(storage)? else {
                            break 'complete None;
                        };
                        buffered += o.value().len();
                        OutboardLocation::Inline(Bytes::copy_from_slice(o.value()))
                    }
                    OutboardLocation::Owned => OutboardLocation::Owned,
                    OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
                };
                Some(EntryState::Complete {
                    data_location,
                    outboard_location,
                })
            }
        };
        moved.push((hash, state));
    }

    if moved.is_empty() {
        drop((old_blobs, old_data, old_outboard, new_blobs));
        tx.delete_table(LEGACY_BLOBS)?;
        tx.delete_table(LEGACY_INLINE_DATA)?;
        tx.delete_table(LEGACY_INLINE_OUTBOARD)?;
        tx.commit()?;
        return Ok(Step::Finished);
    }

    let mut rows = 0;
    let mut dropped = 0;
    for (hash, state) in moved {
        old_blobs.remove(hash).map_err(storage)?;
        match state {
            Some(state) => {
                if matches!(
                    &state,
                    EntryState::Complete {
                        data_location: DataLocation::Inline(_),
                        ..
                    }
                ) {
                    old_data.remove(hash).map_err(storage)?;
                }
                if matches!(
                    &state,
                    EntryState::Complete {
                        outboard_location: OutboardLocation::Inline(_),
                        ..
                    }
                ) {
                    old_outboard.remove(hash).map_err(storage)?;
                }
                new_blobs.insert(hash, as_slices(&state)).map_err(storage)?;
                rows += 1;
            }
            None => {
                // An entry that points at inline data the legacy tables do
                // not have. Every read of it failed, so it is dropped.
                warn!("migration: dropping {hash}: inline payload missing");
                old_data.remove(hash).map_err(storage)?;
                old_outboard.remove(hash).map_err(storage)?;
                dropped += 1;
            }
        }
    }
    drop((old_blobs, old_data, old_outboard, new_blobs));
    tx.commit()?;
    Ok(Step::Moved { rows, dropped })
}

/// Migrate a legacy store to the merged row layout. Does nothing for a store
/// that has no legacy tables.
pub(super) fn migrate(db: &Database, chunk: Chunk) -> ActorResult<()> {
    let start = Instant::now();
    let mut rows = 0u64;
    let mut dropped = 0u64;
    let mut chunks = 0u64;
    loop {
        match step(db, chunk)? {
            Step::NotLegacy => return Ok(()),
            Step::Moved {
                rows: r,
                dropped: d,
            } => {
                rows += r;
                dropped += d;
                chunks += 1;
            }
            Step::Finished => break,
        }
    }
    info!(
        rows,
        dropped,
        chunks,
        elapsed = ?start.elapsed(),
        "migrated blob store to merged rows (blobs-1)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path, sync::Arc};

    use n0_future::StreamExt;
    use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

    use super::*;
    use crate::{
        api::Tag,
        store::fs::{
            delete_set::{self},
            meta::{row::to_owned_state, Actor},
            options::{BatchOptions, PathOptions},
            FsStore,
        },
        BlobFormat, HashAndFormat,
    };

    type Rows = BTreeMap<Hash, EntryState<Bytes>>;

    fn legacy_tables_exist(db: &Database) -> Vec<String> {
        let tx = db.begin_write().unwrap();
        let names = tx
            .list_tables()
            .unwrap()
            .map(|t| t.name().to_string())
            .collect();
        names
    }

    /// Write `rows` in the legacy layout, as the code before the merged row did.
    fn write_legacy(db: &Database, rows: &Rows, tags: &[(Tag, HashAndFormat)]) {
        let tx = db.begin_write().unwrap();
        {
            let mut blobs = tx.open_table(LEGACY_BLOBS).unwrap();
            let mut data = tx.open_table(LEGACY_INLINE_DATA).unwrap();
            let mut outboard = tx.open_table(LEGACY_INLINE_OUTBOARD).unwrap();
            for (hash, state) in rows {
                let stripped = match state {
                    EntryState::Complete {
                        data_location,
                        outboard_location,
                    } => {
                        if let DataLocation::Inline(d) = data_location {
                            data.insert(*hash, d.as_ref()).unwrap();
                        }
                        if let OutboardLocation::Inline(o) = outboard_location {
                            outboard.insert(*hash, o.as_ref()).unwrap();
                        }
                        EntryState::Complete {
                            data_location: match data_location {
                                DataLocation::Inline(_) => DataLocation::Inline(()),
                                DataLocation::Owned(s) => DataLocation::Owned(*s),
                                DataLocation::External(p, s) => {
                                    DataLocation::External(p.clone(), *s)
                                }
                            },
                            outboard_location: match outboard_location {
                                OutboardLocation::Inline(_) => OutboardLocation::Inline(()),
                                OutboardLocation::Owned => OutboardLocation::Owned,
                                OutboardLocation::NotNeeded => OutboardLocation::NotNeeded,
                            },
                        }
                    }
                    EntryState::Partial { size } => EntryState::Partial { size: *size },
                };
                blobs.insert(*hash, stripped).unwrap();
            }
            let mut t = tx.open_table(super::super::tables::TAGS_TABLE).unwrap();
            for (name, value) in tags {
                t.insert(name.clone(), *value).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    fn read_rows(db: &Database) -> Rows {
        let tx = db.begin_read().unwrap();
        let t = tx.open_table(BLOBS_TABLE).unwrap();
        t.iter()
            .unwrap()
            .map(|e| {
                let (k, v) = e.unwrap();
                (k.value(), to_owned_state(v.value()))
            })
            .collect()
    }

    /// One of each state shape, plus `n` filler inline blobs.
    fn synthetic_rows(n: u32) -> Rows {
        let mut rows = Rows::new();
        let mut add = |i: u32, state| {
            rows.insert(Hash::new(i.to_be_bytes()), state);
        };
        let blob = |len: usize, seed: u8| Bytes::from(vec![seed; len]);
        add(
            1_000_001,
            EntryState::Complete {
                data_location: DataLocation::Inline(blob(100, 1)),
                outboard_location: OutboardLocation::NotNeeded,
            },
        );
        add(
            1_000_002,
            EntryState::Complete {
                data_location: DataLocation::Inline(blob(5000, 2)),
                outboard_location: OutboardLocation::Inline(blob(64, 3)),
            },
        );
        add(
            1_000_003,
            EntryState::Complete {
                data_location: DataLocation::Owned(1 << 20),
                outboard_location: OutboardLocation::Inline(blob(960, 4)),
            },
        );
        add(
            1_000_004,
            EntryState::Complete {
                data_location: DataLocation::Owned(1 << 30),
                outboard_location: OutboardLocation::Owned,
            },
        );
        add(
            1_000_005,
            EntryState::Complete {
                data_location: DataLocation::External(vec!["/a/b".into(), "/c/d".into()], 777),
                outboard_location: OutboardLocation::Inline(blob(3, 5)),
            },
        );
        add(1_000_006, EntryState::Partial { size: None });
        add(1_000_007, EntryState::Partial { size: Some(99) });
        add(
            1_000_008,
            EntryState::Complete {
                data_location: DataLocation::Inline(Bytes::new()),
                outboard_location: OutboardLocation::NotNeeded,
            },
        );
        for i in 0..n {
            add(
                i,
                EntryState::Complete {
                    data_location: DataLocation::Inline(blob(1 + (i as usize % 300), i as u8)),
                    outboard_location: OutboardLocation::NotNeeded,
                },
            );
        }
        rows
    }

    fn open_actor(dir: &Path) -> Actor {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let (_protect, ds) = delete_set::pair(Arc::new(PathOptions::new(dir)));
        Actor::new(dir.join("meta.redb"), rx, ds, BatchOptions::default(), None).unwrap()
    }

    #[test]
    fn row_encoding_roundtrip() {
        for (_, state) in synthetic_rows(0) {
            let slices = as_slices(&state);
            let bytes = super::super::row::encode_row(&slices);
            assert_eq!(to_owned_state(super::super::row::decode_row(&bytes)), state);
        }
    }

    /// A file-backed or partial row is exactly what the legacy table stored.
    #[test]
    fn file_backed_rows_are_as_compact_as_legacy() {
        let state = EntryState::<Bytes>::Complete {
            data_location: DataLocation::Owned(1 << 20),
            outboard_location: OutboardLocation::Owned,
        };
        let new = super::super::row::encode_row(&as_slices(&state));
        let old = postcard::to_extend(
            &EntryState::<()>::Complete {
                data_location: DataLocation::Owned(1u64 << 20),
                outboard_location: OutboardLocation::Owned,
            },
            Vec::new(),
        )
        .unwrap();
        assert_eq!(new, old);
    }

    /// A step moves one chunk and stops; the next open finishes the job.
    #[test]
    fn interrupted_migration_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.redb");
        let rows = synthetic_rows(300);
        let tag = (
            Tag::from("keep"),
            HashAndFormat {
                hash: Hash::new(b"x"),
                format: BlobFormat::Raw,
            },
        );
        {
            let db = Database::create(&path).unwrap();
            write_legacy(&db, &rows, std::slice::from_ref(&tag));
            let chunk = Chunk {
                rows: 50,
                bytes: usize::MAX,
            };
            let first = step(&db, chunk).unwrap();
            assert_eq!(
                first,
                Step::Moved {
                    rows: 50,
                    dropped: 0
                }
            );
            // Mid-migration: both layouts exist and together hold every row.
            let names = legacy_tables_exist(&db);
            assert!(names.contains(&"blobs-0".to_string()));
            assert!(names.contains(&"blobs-1".to_string()));
            let tx = db.begin_read().unwrap();
            let old = tx.open_table(LEGACY_BLOBS).unwrap().len().unwrap();
            let new = tx.open_table(BLOBS_TABLE).unwrap().len().unwrap();
            assert_eq!((old, new), (rows.len() as u64 - 50, 50));
        }
        // "Crash": the database is closed with the migration half done.
        let actor = open_actor(dir.path());
        assert_eq!(read_rows(&actor.db), rows);
        let names = legacy_tables_exist(&actor.db);
        for gone in ["blobs-0", "inline-data-0", "inline-outboard-0"] {
            assert!(!names.contains(&gone.to_string()), "{gone} still exists");
        }
        let tx = actor.db.begin_read().unwrap();
        let tags = tx.open_table(super::super::tables::TAGS_TABLE).unwrap();
        assert_eq!(tags.get(tag.0.clone()).unwrap().unwrap().value(), tag.1);
    }

    /// An inline entry whose payload is missing is dropped, the rest migrate.
    #[test]
    fn dangling_inline_entry_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("meta.redb")).unwrap();
        let mut rows = synthetic_rows(10);
        write_legacy(&db, &rows, &[]);
        let bad = Hash::new(b"dangling");
        {
            let tx = db.begin_write().unwrap();
            tx.open_table(LEGACY_BLOBS)
                .unwrap()
                .insert(
                    bad,
                    EntryState::Complete {
                        data_location: DataLocation::Inline(()),
                        outboard_location: OutboardLocation::NotNeeded,
                    },
                )
                .unwrap();
            tx.commit().unwrap();
        }
        migrate(&db, Chunk::default()).unwrap();
        rows.remove(&bad);
        assert_eq!(read_rows(&db), rows);
    }

    /// A store created by the legacy layout opens, reads back byte for byte,
    /// and no longer has the legacy tables.
    #[tokio::test]
    async fn legacy_store_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let sizes = [1usize, 1000, 16 * 1024, 16 * 1024 + 1, 1024 * 1024];
        let db_path = dir.path().join("blobs.db");
        let mut contents = Vec::new();
        {
            let store = FsStore::load(dir.path()).await.unwrap();
            for (i, size) in sizes.iter().enumerate() {
                let data: Vec<u8> = (0..*size).map(|j| (j * 31 + i) as u8).collect();
                let tt = store.add_bytes(data.clone()).temp_tag().await.unwrap();
                store
                    .tags()
                    .set(format!("t{i}"), tt.hash_and_format())
                    .await
                    .unwrap();
                contents.push((tt.hash(), data));
            }
            store.shutdown().await.unwrap();
        }
        // Rewrite the database in the legacy layout.
        let mut expected = {
            let db = Database::open(&db_path).unwrap();
            read_rows(&db)
        };
        assert!(expected.values().any(|s| matches!(
            s,
            EntryState::Complete {
                data_location: DataLocation::Owned(_),
                outboard_location: OutboardLocation::Inline(_)
            }
        )));
        assert!(expected.values().any(|s| matches!(
            s,
            EntryState::Complete {
                data_location: DataLocation::Inline(_),
                ..
            }
        )));
        let partial = Hash::new(b"partial");
        expected.insert(partial, EntryState::Partial { size: Some(5) });
        {
            let db = Database::open(&db_path).unwrap();
            let tx = db.begin_write().unwrap();
            tx.delete_table(BLOBS_TABLE).unwrap();
            tx.commit().unwrap();
            write_legacy(&db, &expected, &[]);
        }
        assert!(legacy_tables_exist(&Database::open(&db_path).unwrap())
            .contains(&"blobs-0".to_string()));

        let store = FsStore::load(dir.path()).await.unwrap();
        for (hash, data) in &contents {
            assert_eq!(store.get_bytes(*hash).await.unwrap().as_ref(), &data[..]);
            let status = store.blobs().status(*hash).await.unwrap();
            assert_eq!(
                status,
                crate::api::blobs::BlobStatus::Complete {
                    size: data.len() as u64
                }
            );
        }
        let mut listed = store.blobs().list().hashes().await.unwrap();
        listed.sort();
        let mut want: Vec<_> = expected.keys().copied().collect();
        want.sort();
        assert_eq!(listed, want);
        let tags: Vec<_> = store.tags().list().await.unwrap().collect().await;
        assert_eq!(tags.len(), sizes.len());
        store.shutdown().await.unwrap();

        let db = Database::open(&db_path).unwrap();
        assert_eq!(read_rows(&db), expected);
        let names = legacy_tables_exist(&db);
        for gone in ["blobs-0", "inline-data-0", "inline-outboard-0"] {
            assert!(!names.contains(&gone.to_string()), "{gone} still exists");
        }
    }
}
