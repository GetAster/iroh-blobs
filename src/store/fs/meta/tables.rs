//! Table definitions and accessors for the redb database.
use redb::{ReadableTable, TableDefinition, TableError};

use super::row::BlobRow;
use crate::{api::Tag, store::fs::delete_set::FileTransaction, Hash, HashAndFormat};

/// Hash to entry state, with inline data and inline outboard embedded in the
/// row. See [`super::row`] for the encoding.
///
/// A store that has this table can no longer be opened by code that only
/// knows the legacy `blobs-0`, `inline-data-0` and `inline-outboard-0` tables.
/// [`super::migrate`] converts a legacy store when it is opened.
pub(super) const BLOBS_TABLE: TableDefinition<Hash, BlobRow> = TableDefinition::new("blobs-1");

pub(super) const TAGS_TABLE: TableDefinition<Tag, HashAndFormat> = TableDefinition::new("tags-0");

/// A trait similar to [`redb::ReadableTable`] but for all tables that make up
/// the blob store. This can be used in places where either a readonly or
/// mutable table is needed.
pub(super) trait ReadableTables {
    fn blobs(&self) -> &impl ReadableTable<Hash, BlobRow>;
    fn tags(&self) -> &impl ReadableTable<Tag, HashAndFormat>;
}

/// A struct similar to [`redb::Table`] but for all tables that make up the
/// blob store.
pub(super) struct Tables<'a> {
    pub blobs: redb::Table<'a, Hash, BlobRow>,
    pub tags: redb::Table<'a, Tag, HashAndFormat>,
    pub ftx: &'a FileTransaction<'a>,
}

impl<'txn> Tables<'txn> {
    pub fn new(
        tx: &'txn redb::WriteTransaction,
        ds: &'txn FileTransaction<'txn>,
    ) -> std::result::Result<Self, TableError> {
        Ok(Self {
            blobs: tx.open_table(BLOBS_TABLE)?,
            tags: tx.open_table(TAGS_TABLE)?,
            ftx: ds,
        })
    }
}

impl ReadableTables for Tables<'_> {
    fn blobs(&self) -> &impl ReadableTable<Hash, BlobRow> {
        &self.blobs
    }
    fn tags(&self) -> &impl ReadableTable<Tag, HashAndFormat> {
        &self.tags
    }
}

/// A struct similar to [`redb::ReadOnlyTable`] but for all tables that make up
/// the blob store.
#[derive(Debug)]
pub(crate) struct ReadOnlyTables {
    pub blobs: redb::ReadOnlyTable<Hash, BlobRow>,
    pub tags: redb::ReadOnlyTable<Tag, HashAndFormat>,
}

impl ReadOnlyTables {
    pub fn new(tx: &redb::ReadTransaction) -> std::result::Result<Self, TableError> {
        Ok(Self {
            blobs: tx.open_table(BLOBS_TABLE)?,
            tags: tx.open_table(TAGS_TABLE)?,
        })
    }
}

impl ReadableTables for ReadOnlyTables {
    fn blobs(&self) -> &impl ReadableTable<Hash, BlobRow> {
        &self.blobs
    }
    fn tags(&self) -> &impl ReadableTable<Tag, HashAndFormat> {
        &self.tags
    }
}
