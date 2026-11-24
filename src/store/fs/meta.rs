//! The metadata database
#![allow(clippy::result_large_err)]
use std::{
    collections::HashSet,
    io,
    ops::{Bound, Deref, DerefMut},
    path::PathBuf,
    time::SystemTime,
};

use bao_tree::BaoTree;
use bytes::Bytes;
use irpc::channel::mpsc;
use n0_error::{anyerr, e, stack_error, AnyError, StdResultExt};
use redb::{Database, DatabaseError, ReadableDatabase, ReadableTable};
use tokio::pin;

use crate::{
    api::{
        self,
        blobs::{BlobBytesResult, BlobStatus},
        proto::{
            AddProtectedMsg, BlobBytesMsg, BlobBytesRequest, BlobDeleteRequest, BlobStatusManyMsg,
            BlobStatusManyRequest, BlobStatusMsg, BlobStatusRequest, ClearProtectedMsg,
            CreateTagRequest, DeleteBlobsMsg, DeleteTagsRequest, ListBlobsItem, ListBlobsMsg,
            ListRequest, ListTagsItem, ListTagsMsg, ListTagsRequest, RenameTagRequest,
            SetTagRequest, ShutdownMsg, SyncDbMsg,
        },
        tags::TagInfo,
        Tag,
    },
    util::channel::oneshot,
    Hash,
};
mod proto;
mod recovery;
mod run;
pub use proto::*;
mod migrate;
pub(crate) mod row;
pub(crate) mod tables;
use row::{as_slices, to_owned_state};
use run::{LastShutdown, Run};
use tables::{ReadOnlyTables, ReadableTables, Tables, META_TABLE};
use tracing::{debug, error, info, info_span, trace, warn, Span};

use super::{
    boot_id,
    delete_set::DeleteHandle,
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::BatchOptions,
    util::PeekableReceiver,
    BaoFilePart,
};
use crate::store::IROH_BLOCK_SIZE;

/// Error type for message handler functions of the redb actor.
///
/// What can go wrong are various things with redb, as well as io errors related
/// to files other than redb.
#[allow(missing_docs)]
#[non_exhaustive]
#[stack_error(derive, add_meta, from_sources)]
pub enum ActorError {
    #[error("table error: {source}")]
    Table {
        #[error(std_err)]
        source: redb::TableError,
    },
    #[error("database error: {source}")]
    Database {
        #[error(std_err)]
        source: redb::DatabaseError,
    },
    #[error("transaction error: {source}")]
    Transaction {
        #[error(std_err)]
        source: redb::TransactionError,
    },
    #[error("commit error: {source}")]
    Commit {
        #[error(std_err)]
        source: redb::CommitError,
    },
    #[error("storage error: {source}")]
    Storage {
        #[error(std_err)]
        source: redb::StorageError,
    },
    #[error("inconsistent database state: {msg}")]
    Inconsistent { msg: String },
    #[error(transparent)]
    Other { source: AnyError },
}

impl From<ActorError> for io::Error {
    fn from(e: ActorError) -> Self {
        io::Error::other(e)
    }
}

impl ActorError {
    pub(super) fn inconsistent(msg: String) -> Self {
        e!(ActorError::Inconsistent { msg })
    }
}

pub type ActorResult<T> = std::result::Result<T, ActorError>;

#[derive(Debug, Clone)]
pub struct Db {
    sender: tokio::sync::mpsc::Sender<Command>,
}

impl Db {
    pub fn new(sender: tokio::sync::mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    pub async fn snapshot(&self, span: tracing::Span) -> io::Result<ReadOnlyTables> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(Snapshot { tx, span }.into())
            .await
            .map_err(|_| io::Error::other("send snapshot"))?;
        rx.await.map_err(|_| io::Error::other("receive snapshot"))
    }

    pub async fn update_await(&self, hash: Hash, state: EntryState<Bytes>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Update {
                    hash,
                    state,
                    tx: Some(tx),
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))?;
        rx.await
            .map_err(|_e| io::Error::other("receive update"))??;
        Ok(())
    }

    /// Update the entry state for a hash, without awaiting completion.
    pub async fn update(&self, hash: Hash, state: EntryState<Bytes>) -> io::Result<()> {
        self.sender
            .send(
                Update {
                    hash,
                    state,
                    tx: None,
                    span: Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))
    }

    /// Set the entry state and await completion.
    pub async fn set(&self, hash: Hash, entry_state: EntryState<Bytes>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Set {
                    hash,
                    state: entry_state,
                    tx,
                    span: Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))?;
        rx.await.map_err(|_| io::Error::other("receive update"))??;
        Ok(())
    }

    /// Get the entry state for a hash, if any.
    pub async fn get(&self, hash: Hash) -> io::Result<Option<EntryState<Bytes>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Get {
                    hash,
                    tx,
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send get"))?;
        let res = rx.await.map_err(|_| io::Error::other("receive get"))?;
        Ok(res.state?)
    }

    /// Send a command. This exists so the main actor can directly forward commands.
    ///
    /// This will fail only if the database actor is dead. In that case the main
    /// actor should probably also shut down.
    pub async fn send(&self, cmd: Command) -> io::Result<()> {
        self.sender
            .send(cmd)
            .await
            .map_err(|_e| io::Error::other("actor down"))?;
        Ok(())
    }
}

fn handle_get(cmd: Get, tables: &impl ReadableTables) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Get { hash, tx, .. } = cmd;
    let Some(entry) = tables.blobs().get(hash)? else {
        tx.send(GetResult { state: Ok(None) });
        return Ok(());
    };
    let entry = to_owned_state(entry.value());
    tx.send(GetResult {
        state: Ok(Some(entry)),
    });
    Ok(())
}

fn handle_dump(cmd: Dump, tables: &impl ReadableTables) -> ActorResult<()> {
    trace!("{cmd:?}");
    trace!("dumping database");
    for e in tables
        .blobs()
        .iter()
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        let (k, v) = e.map_err(|e| e!(ActorError::Storage, e))?;
        let k = k.value();
        let v = v.value();
        println!("blobs: {} -> {}", k.to_hex(), v.fmt_short());
    }
    for e in tables
        .tags()
        .iter()
        .map_err(|e| e!(ActorError::Storage, e))?
    {
        let (k, v) = e.map_err(|e| e!(ActorError::Storage, e))?;
        let k = k.value();
        let v = v.value();
        println!("tags: {k} -> {v:?}");
    }
    cmd.tx.send(Ok(()));
    Ok(())
}

async fn handle_clear_protected(
    cmd: ClearProtectedMsg,
    protection: &mut Protection,
) -> ActorResult<()> {
    trace!("{cmd:?}");
    // A mark starting is the reset point for delete-time protection AND for
    // the poison an unexpandable claim raised: this mark decides for itself
    // whether its sweep is safe (`GcMarkEvent::TraversalIncomplete`).
    protection.protected.clear();
    protection.sweep_poisoned = false;
    cmd.tx.send(Ok(())).await.ok();
    Ok(())
}

async fn handle_add_protected(
    cmd: AddProtectedMsg,
    protection: &mut Protection,
) -> ActorResult<()> {
    trace!("{cmd:?}");
    protection
        .protected
        .extend(cmd.inner.hashes.iter().copied());
    if cmd.inner.poison_sweep {
        // The claim cannot be expanded, so an in-flight sweep might delete
        // members of the claimed collection nobody can enumerate. Stop it
        // deleting; the next mark resets this and decides for itself.
        protection.sweep_poisoned = true;
    }
    cmd.tx.send(Ok(())).await.ok();
    Ok(())
}

/// The delete-time protection an in-flight sweep's delete phase consults:
/// the hash set joined by writes and tag/claim creation, and the poison flag
/// an unexpandable claim raises. Both reset at every mark.
#[derive(Debug, Default)]
pub(super) struct Protection {
    pub(super) protected: HashSet<Hash>,
    pub(super) sweep_poisoned: bool,
}

fn blob_status(tables: &impl ReadableTables, hash: Hash) -> ActorResult<BlobStatus> {
    Ok(
        match tables
            .blobs()
            .get(hash)
            .map_err(|e| e!(ActorError::Storage, e))?
        {
            Some(entry) => match entry.value() {
                EntryState::Complete { data_location, .. } => match data_location {
                    DataLocation::Inline(data) => BlobStatus::Complete {
                        size: data.len() as u64,
                    },
                    DataLocation::Owned(size) => BlobStatus::Complete { size },
                    DataLocation::External(_, size) => BlobStatus::Complete { size },
                },
                EntryState::Partial { size } => BlobStatus::Partial { size },
            },
            None => BlobStatus::NotFound,
        },
    )
}

async fn handle_get_blob_status(
    msg: BlobStatusMsg,
    tables: &impl ReadableTables,
) -> ActorResult<()> {
    trace!("{msg:?}");
    let BlobStatusMsg {
        inner: BlobStatusRequest { hash },
        tx,
        ..
    } = msg;
    let res = blob_status(tables, hash)?;
    tx.send(res).await.ok();
    Ok(())
}

async fn handle_get_blob_status_many(
    msg: BlobStatusManyMsg,
    tables: &impl ReadableTables,
) -> ActorResult<()> {
    trace!("{msg:?}");
    let BlobStatusManyMsg {
        inner: BlobStatusManyRequest { hashes },
        tx,
        ..
    } = msg;
    let mut out = Vec::with_capacity(hashes.len());
    for hash in hashes {
        out.push(blob_status(tables, hash)?);
    }
    tx.send(out).await.ok();
    Ok(())
}

async fn handle_get_blob_bytes(msg: BlobBytesMsg, tables: &impl ReadableTables) -> ActorResult<()> {
    trace!("{msg:?}");
    let BlobBytesMsg {
        inner: BlobBytesRequest { hashes },
        tx,
        ..
    } = msg;
    let mut out = Vec::with_capacity(hashes.len());
    for hash in hashes {
        if hash == Hash::EMPTY {
            out.push(BlobBytesResult::Complete { data: Bytes::new() });
            continue;
        }

        let res = match tables
            .blobs()
            .get(hash)
            .map_err(|e| e!(ActorError::Storage, e))?
        {
            Some(entry) => match entry.value() {
                EntryState::Complete { data_location, .. } => match data_location {
                    DataLocation::Inline(data) => {
                        if Hash::new(data) != hash {
                            return Err(ActorError::inconsistent(format!(
                                "inline data hash mismatch for {}",
                                hash.to_hex()
                            )));
                        }
                        BlobBytesResult::Complete {
                            data: Bytes::copy_from_slice(data),
                        }
                    }
                    DataLocation::Owned(size) | DataLocation::External(_, size) => {
                        BlobBytesResult::NeedsExport { size }
                    }
                },
                EntryState::Partial { size } => BlobBytesResult::Partial { size },
            },
            None => BlobBytesResult::NotFound,
        };
        out.push(res);
    }
    tx.send(Ok(out)).await.ok();
    Ok(())
}

/// Streams a snapshot without making the metadata actor wait for the reader.
pub async fn list_tags(snapshot: ReadOnlyTables, cmd: ListTagsMsg) {
    let ListTagsMsg { mut tx, inner, .. } = cmd;
    if let Err(cause) = list_tags_impl(snapshot, inner, &mut tx).await {
        tx.send(ListTagsItem::Error(cause)).await.ok();
    }
}

async fn list_tags_impl(
    snapshot: ReadOnlyTables,
    request: ListTagsRequest,
    tx: &mut mpsc::Sender<ListTagsItem>,
) -> api::Result<()> {
    let ListTagsRequest {
        from,
        to,
        raw,
        hash_seq,
    } = request;
    let from = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let to = to.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
    for item in snapshot
        .tags()
        .range((from, to))
        .map_err(api_error_from_storage_error)?
    {
        let (k, v) = item.map_err(api_error_from_storage_error)?;
        let v = v.value();
        if raw && v.format.is_raw() || hash_seq && v.format.is_hash_seq() {
            let info = TagInfo {
                name: k.value(),
                hash: v.hash,
                format: v.format,
            };
            if tx.send(ListTagsItem::Tag(info)).await.is_err() {
                return Ok(());
            }
        }
    }
    tx.send(ListTagsItem::Done).await.ok();
    Ok(())
}

fn handle_update(cmd: Update, protection: &mut Protection, tables: &mut Tables) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Update {
        hash, state, tx, ..
    } = cmd;
    let _ = protection.protected.insert(hash);
    trace!("updating hash {} to {}", hash.to_hex(), state.fmt_short());
    let old_entry_opt = tables.blobs.get(hash)?.map(|e| to_owned_state(e.value()));
    let state = match old_entry_opt {
        Some(old) => {
            let was_partial = old.is_partial();
            let res = EntryState::union(old, state)?;
            if was_partial {
                delete_partial_files(tables, hash, &res);
            }
            res
        }
        None => state,
    };
    tables
        .blobs
        .insert(hash, as_slices(&state))
        .map_err(|e| e!(ActorError::Storage, e))?;
    if let Some(tx) = tx {
        tx.send(Ok(()));
    }
    Ok(())
}

fn handle_set(cmd: Set, protection: &mut Protection, tables: &mut Tables) -> ActorResult<()> {
    trace!("{cmd:?}");
    let Set {
        state, hash, tx, ..
    } = cmd;
    let _ = protection.protected.insert(hash);
    let was_partial = tables
        .blobs
        .insert(hash, as_slices(&state))
        .map_err(|e| e!(ActorError::Storage, e))?
        .is_some_and(|old| old.value().is_partial());
    if was_partial {
        delete_partial_files(tables, hash, &state);
    }
    tx.send(Ok(()));
    Ok(())
}

/// Removes the files of a partial entry that its complete state no longer uses.
///
/// Inlined data and outboards are safe to delete: whether they are inlined depends
/// only on the size, so no concurrent import of the same hash writes those files.
/// Owned data is never deleted, since a re-import can replace that file at any time.
fn delete_partial_files<I>(tables: &Tables, hash: Hash, new: &EntryState<I>) {
    let EntryState::Complete {
        data_location,
        outboard_location,
    } = new
    else {
        return;
    };
    tables
        .ftx
        .delete(hash, [BaoFilePart::Sizes, BaoFilePart::Bitfield]);
    if matches!(data_location, DataLocation::Inline(_)) {
        tables.ftx.delete(hash, [BaoFilePart::Data]);
    }
    if !matches!(outboard_location, OutboardLocation::Owned) {
        tables.ftx.delete(hash, [BaoFilePart::Outboard]);
    }
}

#[derive(Clone, Copy)]
enum TxnNum {
    Read(u64),
    Write(u64),
    TopLevel(u64),
}

impl std::fmt::Debug for TxnNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnNum::Read(n) => write!(f, "r{n}"),
            TxnNum::Write(n) => write!(f, "w{n}"),
            TxnNum::TopLevel(n) => write!(f, "t{n}"),
        }
    }
}

#[derive(Debug)]
pub struct Actor {
    db: redb::Database,
    cmds: PeekableReceiver<Command>,
    ds: DeleteHandle,
    options: BatchOptions,
    protection: Protection,
    /// Number of committed write transactions.
    #[cfg(test)]
    pub(crate) commits: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Actor {
    pub fn new(
        db_path: PathBuf,
        cmds: tokio::sync::mpsc::Receiver<Command>,
        mut ds: DeleteHandle,
        options: BatchOptions,
        syncs_files: bool,
        cache_size: Option<usize>,
    ) -> Result<Self, ActorError> {
        debug!("creating or opening meta database at {}", db_path.display());
        let mut builder = redb::Database::builder();
        if let Some(cache_size) = cache_size {
            builder.set_cache_size(cache_size);
        }
        let db = match builder.create(db_path) {
            Ok(db) => db,
            Err(DatabaseError::UpgradeRequired(v)) => {
                return Err(anyerr!(
                    "migration from redb v{v} no longer supported; \
                     upgrade with an older redb version first"
                )
                .into());
            }
            Err(err) => return Err(err.into()),
        };
        migrate::migrate(&db, migrate::Chunk::default())?;
        let paths = ds.paths().clone();
        let tx = db.begin_write()?;
        let ftx = ds.begin_write();
        let mut tables = Tables::new(&tx, &ftx)?;
        let mut meta = tx.open_table(META_TABLE)?;
        let run = Run {
            boot_id: boot_id::current(),
            syncs_files,
        };
        // Recovery commits together with the new run, so if the process dies
        // during it, the next open still finds the old one.
        match run::record(&mut meta, &run)? {
            LastShutdown::Clean => {}
            LastShutdown::ProcessCrash => info!(
                "the blob store was not shut down cleanly, but the machine kept running, so \
                 nothing that was written got lost"
            ),
            LastShutdown::MaybeOsCrash { synced: true, .. } => info!(
                "the blob store was not shut down cleanly, but its last run synced its files, \
                 so an OS crash cannot have damaged entries"
            ),
            LastShutdown::MaybeOsCrash { synced: false } => {
                warn!(
                    "the blob store was not shut down cleanly, and the machine may have gone \
                     down too, so entries are checked again in case an OS crash lost data"
                );
                report(&recovery::recover(&mut tables, &paths)?);
            }
        }
        // A PowerLoss run must also cover writes inherited from earlier runs.
        // Sync after recovery so its writes and bitfield deletions are included,
        // and before committing the new run so a failed sync leaves the old one.
        if syncs_files {
            super::sync_fs::sync(&paths.data_path)
                .std_context("syncing the data filesystem on open")?;
        }
        drop(meta);
        drop(tables);
        tx.commit()?;
        drop(ftx);
        let cmds = PeekableReceiver::new(cmds);
        Ok(Self {
            db,
            cmds,
            ds,
            options,
            protection: Default::default(),
            #[cfg(test)]
            commits: Default::default(),
        })
    }

    async fn handle_readonly(
        protection: &mut Protection,
        tables: &impl ReadableTables,
        cmd: ReadOnlyCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadOnlyCommand::Get(cmd) => handle_get(cmd, tables),
            ReadOnlyCommand::Dump(cmd) => handle_dump(cmd, tables),
            ReadOnlyCommand::ClearProtected(cmd) => handle_clear_protected(cmd, protection).await,
            ReadOnlyCommand::AddProtected(cmd) => handle_add_protected(cmd, protection).await,
            ReadOnlyCommand::GetBlobStatus(cmd) => handle_get_blob_status(cmd, tables).await,
            ReadOnlyCommand::GetBlobStatusMany(cmd) => {
                handle_get_blob_status_many(cmd, tables).await
            }
            ReadOnlyCommand::GetBlobBytes(cmd) => handle_get_blob_bytes(cmd, tables).await,
        }
    }

    async fn delete(
        protection: &mut Protection,
        tables: &mut Tables<'_>,
        cmd: DeleteBlobsMsg,
    ) -> ActorResult<()> {
        let DeleteBlobsMsg {
            inner: BlobDeleteRequest { hashes, force },
            ..
        } = cmd;
        for hash in hashes {
            if !force && protection.sweep_poisoned {
                // An unexpandable collection claim exists: the sweep that
                // decided this delete cannot know the claim's members, so it
                // must not keep deleting anything.
                trace!("delete {hash}: skip (sweep poisoned by an unexpandable claim)");
                continue;
            }
            if !force && protection.protected.contains(&hash) {
                trace!("delete {hash}: skip (protected)");
                continue;
            }
            if let Some(entry) = tables.blobs.remove(hash)? {
                match entry.value() {
                    EntryState::Complete {
                        data_location,
                        outboard_location,
                    } => {
                        trace!("delete {hash}: currently complete. will be deleted.");
                        match data_location {
                            DataLocation::Inline(_) => {}
                            DataLocation::Owned(_) => {
                                // mark the data for deletion
                                tables.ftx.delete(hash, [BaoFilePart::Data]);
                            }
                            DataLocation::External(_, _) => {}
                        }
                        match outboard_location {
                            OutboardLocation::Inline(_) => {}
                            OutboardLocation::Owned => {
                                // mark the outboard for deletion
                                tables.ftx.delete(hash, [BaoFilePart::Outboard]);
                            }
                            OutboardLocation::NotNeeded => {}
                        }
                    }
                    EntryState::Partial { .. } => {
                        trace!("delete {hash}: currently partial. will be deleted.");
                        tables.ftx.delete(
                            hash,
                            [
                                BaoFilePart::Outboard,
                                BaoFilePart::Data,
                                BaoFilePart::Sizes,
                                BaoFilePart::Bitfield,
                            ],
                        );
                    }
                }
            }
        }
        cmd.tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn set_tag(
        tables: &mut Tables<'_>,
        protection: &mut Protection,
        cmd: SetTagMsg,
    ) -> ActorResult<()> {
        trace!("{cmd:?}");
        let SetTagMsg {
            inner: SetTagRequest { name: tag, value },
            tx,
            ..
        } = cmd;
        // The new tag must protect its hash against a delete decided before
        // the tag existed — a GC sweep whose mark snapshotted the tags table
        // earlier. Deletion consults this set live, and this actor owns both,
        // so inserting here linearizes tag creation against the sweep. The
        // next mark clears the set and re-snapshots the tags, so protection
        // does not outlive the tag.
        let _ = protection.protected.insert(value.hash);
        let res = tables
            .tags
            .insert(tag, value)
            .map_err(api_error_from_storage_error)
            .map(|_| ());
        tx.send(res).await.ok();
        Ok(())
    }

    async fn create_tag(
        tables: &mut Tables<'_>,
        protection: &mut Protection,
        cmd: CreateTagMsg,
    ) -> ActorResult<()> {
        trace!("{cmd:?}");
        let CreateTagMsg {
            inner: CreateTagRequest { value },
            tx,
            ..
        } = cmd;
        // Same linearization as `set_tag`: the auto tag must protect its hash
        // against an in-flight sweep whose mark predates it.
        let _ = protection.protected.insert(value.hash);
        let tag = {
            let tag = Tag::auto(SystemTime::now(), |x| {
                matches!(tables.tags.get(Tag(Bytes::copy_from_slice(x))), Ok(Some(_)))
            });
            tables.tags.insert(tag.clone(), value)?;
            tag
        };
        tx.send(Ok(tag.clone())).await.ok();
        Ok(())
    }

    async fn delete_tags(tables: &mut Tables<'_>, cmd: DeleteTagsMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let DeleteTagsMsg {
            inner: DeleteTagsRequest { from, to },
            tx,
            ..
        } = cmd;
        let from = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
        let to = to.map(Bound::Excluded).unwrap_or(Bound::Unbounded);
        let removing = tables.tags.extract_from_if((from, to), |_, _| true)?;
        // drain the iterator to actually remove the tags
        let mut deleted = 0;
        for res in removing {
            res?;
            deleted += 1;
        }
        tx.send(Ok(deleted)).await.ok();
        Ok(())
    }

    async fn rename_tag(tables: &mut Tables<'_>, cmd: RenameTagMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let RenameTagMsg {
            inner: RenameTagRequest { from, to },
            tx,
            ..
        } = cmd;
        let value = match tables.tags.remove(from)? {
            Some(value) => value.value(),
            None => {
                tx.send(Err(api::Error::io(
                    io::ErrorKind::NotFound,
                    "tag not found",
                )))
                .await
                .ok();
                return Ok(());
            }
        };
        tables.tags.insert(to, value)?;
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_readwrite(
        protection: &mut Protection,
        tables: &mut Tables<'_>,
        cmd: ReadWriteCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        let span = info_span!(
            parent: cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        match cmd {
            ReadWriteCommand::Update(cmd) => handle_update(cmd, protection, tables),
            ReadWriteCommand::Set(cmd) => handle_set(cmd, protection, tables),
            ReadWriteCommand::DeleteBlobw(cmd) => Self::delete(protection, tables, cmd).await,
            ReadWriteCommand::SetTag(cmd) => Self::set_tag(tables, protection, cmd).await,
            ReadWriteCommand::CreateTag(cmd) => Self::create_tag(tables, protection, cmd).await,
            ReadWriteCommand::DeleteTags(cmd) => Self::delete_tags(tables, cmd).await,
            ReadWriteCommand::RenameTag(cmd) => Self::rename_tag(tables, cmd).await,
        }
    }

    async fn handle_non_toplevel(
        protection: &mut Protection,
        tables: &mut Tables<'_>,
        cmd: NonTopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<()> {
        match cmd {
            NonTopLevelCommand::ReadOnly(cmd) => {
                Self::handle_readonly(protection, tables, cmd, op).await
            }
            NonTopLevelCommand::ReadWrite(cmd) => {
                Self::handle_readwrite(protection, tables, cmd, op).await
            }
        }
    }

    async fn sync_db(_db: &mut Database, cmd: SyncDbMsg) -> ActorResult<()> {
        trace!("{cmd:?}");
        let SyncDbMsg { tx, .. } = cmd;
        // nothing to do here, since for a toplevel cmd we are outside a write transaction
        tx.send(Ok(())).await.ok();
        Ok(())
    }

    async fn handle_toplevel(
        db: &mut Database,
        cmd: TopLevelCommand,
        op: TxnNum,
    ) -> ActorResult<Option<ShutdownMsg>> {
        let span = info_span!(
            parent: cmd.parent_span(),
            "tx",
            op = tracing::field::debug(op),
        );
        let _guard = span.enter();
        Ok(match cmd {
            TopLevelCommand::SyncDb(cmd) => {
                Self::sync_db(db, cmd).await?;
                None
            }
            TopLevelCommand::Shutdown(cmd) => {
                trace!("{cmd:?}");
                Self::clear_run(db)?;
                Some(cmd)
            }
            TopLevelCommand::Snapshot(cmd) => {
                trace!("{cmd:?}");
                let txn = db
                    .begin_read()
                    .map_err(|e| e!(ActorError::Transaction, e))?;
                let snapshot = ReadOnlyTables::new(&txn).map_err(|e| e!(ActorError::Table, e))?;
                cmd.tx.send(snapshot).ok();
                None
            }
        })
    }

    /// Removes the run record, so the next open knows the store was shut down cleanly.
    fn clear_run(db: &Database) -> ActorResult<()> {
        let tx = db
            .begin_write()
            .map_err(|e| e!(ActorError::Transaction, e))?;
        let mut meta = tx
            .open_table(META_TABLE)
            .map_err(|e| e!(ActorError::Table, e))?;
        run::clear(&mut meta).map_err(|e| e!(ActorError::Storage, e))?;
        drop(meta);
        tx.commit().map_err(|e| e!(ActorError::Commit, e))?;
        Ok(())
    }

    pub async fn run(self) {
        if let Err(error) = self.run_inner().await {
            // Without this actor every store operation fails, so keep the cause.
            error!("metadata db actor stopped: {error:#}");
        }
    }

    async fn run_inner(mut self) -> ActorResult<()> {
        let mut db = DbWrapper::from(self.db);
        let options = &self.options;
        let mut op = 0u64;
        let shutdown = loop {
            op += 1;
            let Some(cmd) = self.cmds.recv().await else {
                break None;
            };
            match cmd {
                Command::TopLevel(cmd) => {
                    let op = TxnNum::TopLevel(op);
                    if let Some(shutdown) = Self::handle_toplevel(&mut db, cmd, op).await? {
                        break Some(shutdown);
                    }
                }
                Command::ReadOnly(cmd) => {
                    let op = TxnNum::Read(op);
                    self.cmds.push_back(cmd.into()).ok();
                    let tx = db
                        .begin_read()
                        .map_err(|e| e!(ActorError::Transaction, e))?;
                    let tables = ReadOnlyTables::new(&tx).map_err(|e| e!(ActorError::Table, e))?;
                    let timeout = n0_future::time::sleep(self.options.max_read_duration);
                    pin!(timeout);
                    let mut n = 0;
                    while let Some(cmd) = self.cmds.extract(Command::read_only, &mut timeout).await
                    {
                        Self::handle_readonly(&mut self.protection, &tables, cmd, op).await?;
                        n += 1;
                        if n >= options.max_read_batch {
                            break;
                        }
                    }
                }
                Command::ReadWrite(cmd) => {
                    let op = TxnNum::Write(op);
                    self.cmds.push_back(cmd.into()).ok();
                    let ftx = self.ds.begin_write();
                    let tx = db
                        .begin_write()
                        .map_err(|e| e!(ActorError::Transaction, e))?;
                    let mut tables =
                        Tables::new(&tx, &ftx).map_err(|e| e!(ActorError::Table, e))?;
                    let deadline =
                        n0_future::time::Instant::now() + self.options.max_write_duration;
                    let timeout = n0_future::time::sleep_until(deadline);
                    pin!(timeout);
                    let mut n = 0;
                    // `SyncDb` waiters collected in this transaction. Each is
                    // answered only after the commit that covers every write
                    // queued before it.
                    let mut waiters: Vec<SyncDbMsg> = Vec::new();
                    let res = async {
                        loop {
                            let member = if waiters.is_empty() {
                                // Wait for more work until the batch is full or
                                // the timer fires.
                                self.cmds
                                    .extract(Command::write_batch_member, &mut timeout)
                                    .await
                            } else {
                                // Someone is waiting for durability: take what
                                // is already queued, but never wait for more,
                                // so a lone writer commits right away.
                                if n0_future::time::Instant::now() >= deadline {
                                    break;
                                }
                                self.cmds.try_extract(Command::write_batch_member)
                            };
                            match member {
                                Some(WriteBatchMember::Command(cmd)) => {
                                    Self::handle_non_toplevel(
                                        &mut self.protection,
                                        &mut tables,
                                        cmd,
                                        op,
                                    )
                                    .await?;
                                }
                                Some(WriteBatchMember::Sync(cmd)) => {
                                    trace!("{cmd:?}");
                                    waiters.push(cmd);
                                }
                                None => break,
                            }
                            n += 1;
                            if n >= options.max_write_batch {
                                break;
                            }
                        }
                        ActorResult::Ok(())
                    }
                    .await;
                    drop(tables);
                    let res = res.and_then(|()| {
                        tx.commit().map_err(|e| e!(ActorError::Commit, e))?;
                        ftx.commit();
                        #[cfg(test)]
                        self.commits
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(())
                    });
                    match res {
                        Ok(()) => {
                            for SyncDbMsg { tx, .. } in waiters {
                                tx.send(Ok(())).await.ok();
                            }
                        }
                        Err(err) => {
                            for SyncDbMsg { tx, .. } in waiters {
                                let err = api::Error::Io(io::Error::other(err.to_string()));
                                tx.send(Err(err)).await.ok();
                            }
                            return Err(err);
                        }
                    }
                }
            }
        };
        if let Some(shutdown) = shutdown {
            drop(db);
            shutdown.tx.send(()).await.ok();
        }
        Ok(())
    }
}

/// Logs what recovery did.
fn report(recovered: &recovery::Recovered) {
    info!(
        "{} complete and {} partial entries are checked against their files when next loaded",
        recovered.complete, recovered.partial
    );
    if recovered.external > 0 {
        warn!(
            "{} entries with external data are not checked, since the data is not ours",
            recovered.external
        );
    }
}

/// Convert a redb StorageError into an api::Error
///
/// This can't be a From instance because that would require exposing redb::StorageError in the public API.
fn api_error_from_storage_error(e: redb::StorageError) -> api::Error {
    api::Error::Io(io::Error::other(e))
}

#[derive(Debug)]
struct DbWrapper(Option<Database>);

impl Deref for DbWrapper {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("database not open")
    }
}

impl DerefMut for DbWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("database not open")
    }
}

impl From<Database> for DbWrapper {
    fn from(db: Database) -> Self {
        Self(Some(db))
    }
}

impl Drop for DbWrapper {
    fn drop(&mut self) {
        if let Some(db) = self.0.take() {
            debug!("closing database");
            drop(db);
            debug!("database closed");
        }
    }
}

pub(crate) fn raw_outboard_size(size: u64) -> u64 {
    BaoTree::new(size, IROH_BLOCK_SIZE).outboard_size()
}

pub async fn list_blobs(snapshot: ReadOnlyTables, cmd: ListBlobsMsg) {
    let ListBlobsMsg { mut tx, inner, .. } = cmd;
    if let Err(cause) = list_blobs_impl(snapshot, inner, &mut tx).await {
        error!("error listing blobs: {}", cause);
        tx.send(ListBlobsItem::Error(cause)).await.ok();
    }
}

async fn list_blobs_impl(
    snapshot: ReadOnlyTables,
    _cmd: ListRequest,
    tx: &mut mpsc::Sender<ListBlobsItem>,
) -> api::Result<()> {
    for item in snapshot
        .blobs
        .iter()
        .map_err(api_error_from_storage_error)?
    {
        let (k, _) = item.map_err(api_error_from_storage_error)?;
        if tx.send(ListBlobsItem::Hash(k.value())).await.is_err() {
            return Ok(());
        }
    }
    tx.send(ListBlobsItem::Done).await.ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{atomic::Ordering, Arc},
        time::Duration,
    };

    use super::{
        super::{
            delete_set,
            options::{CrashConsistency, Options, PathOptions},
        },
        *,
    };

    fn a_run(boot_id: Option<&str>, syncs_files: bool) -> Run {
        Run {
            boot_id: boot_id.map(str::to_owned),
            syncs_files,
        }
    }

    /// Reads the recorded run from a closed store's database.
    fn recorded_run(db: &std::path::Path) -> testresult::TestResult<Option<Run>> {
        let db = Database::open(db)?;
        let tx = db.begin_read()?;
        Ok(run::read(&tx.open_table(META_TABLE)?))
    }

    /// Records `run` as the open run of a closed store's database.
    fn set_run(db: &std::path::Path, run: &Run) -> testresult::TestResult<()> {
        let db = Database::open(db)?;
        let tx = db.begin_write()?;
        run::write(&mut tx.open_table(META_TABLE)?, run)?;
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn opening_records_the_run_until_a_clean_shutdown() -> testresult::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let paths = Arc::new(PathOptions::new(dir.path()));
        let db = dir.path().join("db");
        let (_protect, deletes) = delete_set::pair(paths);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        // dropped without a shutdown, as in a crash
        drop(Actor::new(
            db.clone(),
            rx,
            deletes,
            BatchOptions::default(),
            false,
            None,
        )?);
        let expected = Run {
            boot_id: boot_id::current(),
            syncs_files: false,
        };
        assert_eq!(recorded_run(&db)?, Some(expected));
        Ok(())
    }

    /// Opens a store at `dir` with `crash_consistency`.
    async fn open(
        dir: &std::path::Path,
        crash_consistency: CrashConsistency,
    ) -> testresult::TestResult<crate::store::fs::FsStore> {
        let mut options = Options::new(dir);
        options.crash_consistency = crash_consistency;
        Ok(crate::store::fs::FsStore::load_with_opts(dir.join("blobs.db"), options).await?)
    }

    /// Simulates an OS crash during an unsynced run, and checks entries after reopening with `reopen`.
    async fn os_crash_recovery(reopen: CrashConsistency) -> testresult::TestResult<()> {
        use bao_tree::ChunkRanges;

        use crate::api::blobs::BlobStatus;

        let dir = tempfile::tempdir()?;
        let paths = PathOptions::new(dir.path());
        let data = |byte: u8, len: usize| (0..len).map(|i| byte ^ (i as u8)).collect::<Vec<_>>();
        // owned data, inline outboard
        let intact = data(1, 100_000);
        let truncated = data(2, 100_000);
        // owned data and outboard
        let no_outboard = data(3, 5_000_000);
        let store = open(dir.path(), CrashConsistency::ProcessCrash).await?;
        let intact_hash = store.add_bytes(intact.clone()).await?.hash;
        let truncated_hash = store.add_bytes(truncated).await?.hash;
        let no_outboard_hash = store.add_bytes(no_outboard).await?.hash;
        store.shutdown().await?;
        drop(store);

        // the machine went down while the store was open, and lost writes
        set_run(
            &dir.path().join("blobs.db"),
            &a_run(Some("another boot"), false),
        )?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(paths.data_path(&truncated_hash))?;
        file.set_len(50_000)?;
        std::fs::File::create(paths.outboard_path(&no_outboard_hash))?;

        let store = open(dir.path(), reopen).await?;
        // loading an entry checks it, an intact one is complete again
        assert_eq!(
            store.get_bytes(intact_hash).await?.as_ref(),
            intact.as_slice()
        );
        assert!(matches!(
            store.status(intact_hash).await?,
            BlobStatus::Complete { size: 100_000 }
        ));
        // a damaged one keeps what its files still hold, and nothing more
        let bitfield = store.observe(truncated_hash).await?;
        assert!(!bitfield.is_complete());
        assert!(bitfield.ranges.contains(&bao_tree::ChunkNum(0)));
        assert!(store.get_bytes(truncated_hash).await.is_err());
        let bitfield = store.observe(no_outboard_hash).await?;
        assert_eq!(bitfield.ranges, ChunkRanges::empty());
        store.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn after_an_os_crash_entries_only_claim_what_their_files_hold(
    ) -> testresult::TestResult<()> {
        os_crash_recovery(CrashConsistency::ProcessCrash).await
    }

    #[tokio::test]
    async fn recovery_depends_on_the_last_run_not_this_one() -> testresult::TestResult<()> {
        os_crash_recovery(CrashConsistency::PowerLoss).await
    }

    #[tokio::test]
    async fn after_an_os_crash_a_synced_run_needs_no_recovery() -> testresult::TestResult<()> {
        use crate::api::blobs::BlobStatus;

        let dir = tempfile::tempdir()?;
        let data = vec![7u8; 100_000];
        let store = open(dir.path(), CrashConsistency::PowerLoss).await?;
        let hash = store.add_bytes(data).await?.hash;
        store.shutdown().await?;
        drop(store);
        set_run(
            &dir.path().join("blobs.db"),
            &a_run(Some("another boot"), true),
        )?;
        let store = open(dir.path(), CrashConsistency::ProcessCrash).await?;
        // recovery would have made it partial until it is loaded
        assert!(matches!(
            store.status(hash).await?,
            BlobStatus::Complete { size: 100_000 }
        ));
        store.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_clean_shutdown_clears_the_run() -> testresult::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(dir.path()).await?;
        store.shutdown().await?;
        drop(store);
        assert_eq!(recorded_run(&dir.path().join("blobs.db"))?, None);
        Ok(())
    }

    #[test]
    fn export_then_reimport_before_metadata_runs() -> testresult::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let paths = Arc::new(PathOptions::new(dir.path()));
        std::fs::create_dir_all(&paths.data_path)?;
        let (protect, mut deletes) = delete_set::pair(paths.clone());
        let db = Database::create(dir.path().join("db"))?;
        let hash = Hash::new(b"content");
        let owned = paths.data_path(&hash);
        let external = dir.path().join("exported");
        std::fs::write(&owned, b"content")?;
        let ftx = deletes.begin_write();
        let tx = db.begin_write()?;
        let mut tables = Tables::new(&tx, &ftx)?;
        tables.blobs.insert(
            hash,
            EntryState::Complete {
                data_location: DataLocation::Owned(7),
                outboard_location: OutboardLocation::NotNeeded,
            },
        )?;
        // Export queues Set; another import writes before the db handles it.
        std::fs::rename(&owned, &external)?;
        protect.protect(hash, [BaoFilePart::Data, BaoFilePart::Outboard]);
        std::fs::write(&owned, b"content")?;
        let mut protected = Protection::default();
        let (reply, _rx) = oneshot::channel();
        handle_set(
            Set {
                hash,
                state: EntryState::Complete {
                    data_location: DataLocation::External(vec![external], 7),
                    outboard_location: OutboardLocation::NotNeeded,
                },
                tx: reply,
                span: Span::none(),
            },
            &mut protected,
            &mut tables,
        )?;
        handle_update(
            Update {
                hash,
                state: EntryState::Complete {
                    data_location: DataLocation::Owned(7),
                    outboard_location: OutboardLocation::NotNeeded,
                },
                tx: None,
                span: Span::none(),
            },
            &mut protected,
            &mut tables,
        )?;
        assert!(matches!(
            tables.blobs.get(hash)?.unwrap().value(),
            EntryState::Complete {
                data_location: DataLocation::Owned(_),
                ..
            }
        ));
        drop(tables);
        tx.commit()?;
        ftx.commit();
        assert!(
            owned.exists(),
            "committed metadata still refers to this file"
        );
        Ok(())
    }

    fn inline_state(data: &[u8]) -> EntryState<Bytes> {
        EntryState::Complete {
            data_location: DataLocation::Inline(Bytes::copy_from_slice(data)),
            outboard_location: OutboardLocation::NotNeeded,
        }
    }

    fn set_msg(i: u32) -> (Command, oneshot::Receiver<ActorResult<()>>) {
        let data = i.to_be_bytes();
        let (tx, rx) = oneshot::channel();
        let cmd = Set {
            hash: Hash::new(data),
            state: inline_state(&data),
            tx,
            span: Span::none(),
        };
        (cmd.into(), rx)
    }

    fn sync_msg() -> (Command, irpc::channel::oneshot::Receiver<api::Result<()>>) {
        let (tx, rx) = irpc::channel::oneshot::channel();
        let msg = SyncDbMsg::from((crate::api::proto::SyncDbRequest, tx));
        (TopLevelCommand::SyncDb(msg).into(), rx)
    }

    fn actor(
        dir: &std::path::Path,
        options: BatchOptions,
    ) -> (Actor, tokio::sync::mpsc::Sender<Command>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        let (_protect, ds) = delete_set::pair(Arc::new(PathOptions::new(dir)));
        let actor = Actor::new(dir.join("meta.redb"), rx, ds, options, false, None).unwrap();
        (actor, tx)
    }

    /// Everything queued before the actor starts, writes interleaved with
    /// syncs, ends up in a single commit, and every sync is answered after it.
    #[tokio::test]
    async fn queued_syncs_share_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (actor, tx) = actor(dir.path(), BatchOptions::default());
        let commits = actor.commits.clone();
        let mut sets = Vec::new();
        let mut syncs = Vec::new();
        for i in 0..40 {
            let (cmd, rx) = set_msg(i);
            tx.send(cmd).await.unwrap();
            sets.push(rx);
            if i % 8 == 7 {
                let (cmd, rx) = sync_msg();
                tx.send(cmd).await.unwrap();
                syncs.push(rx);
            }
        }
        let task = tokio::spawn(actor.run());
        for rx in syncs {
            rx.await.unwrap().unwrap();
            // The reply is only sent after the commit.
            assert_eq!(commits.load(Ordering::Relaxed), 1);
        }
        for rx in sets {
            rx.await.unwrap().unwrap();
        }
        drop(tx);
        task.await.unwrap();
        assert_eq!(commits.load(Ordering::Relaxed), 1);
    }

    /// A lone write followed by a sync commits as soon as the queue is empty,
    /// without waiting for the duration timer.
    #[tokio::test]
    async fn lone_sync_does_not_wait_for_timer() {
        let dir = tempfile::tempdir().unwrap();
        let options = BatchOptions {
            max_write_duration: Duration::from_secs(60),
            max_read_duration: Duration::from_secs(60),
            ..Default::default()
        };
        let (actor, tx) = actor(dir.path(), options);
        let commits = actor.commits.clone();
        let task = tokio::spawn(actor.run());
        for i in 0..3 {
            let (cmd, rx) = set_msg(i);
            tx.send(cmd).await.unwrap();
            rx.await.unwrap().unwrap();
            let (cmd, rx) = sync_msg();
            tx.send(cmd).await.unwrap();
            tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .expect("sync waited for the timer")
                .unwrap()
                .unwrap();
            assert_eq!(commits.load(Ordering::Relaxed), i as u64 + 1);
        }
        drop(tx);
        task.await.unwrap();
    }

    /// The batch size cap still splits a long queue into several commits, and
    /// a sync is answered by the commit that covers it.
    #[tokio::test]
    async fn batch_cap_splits_commits() {
        let dir = tempfile::tempdir().unwrap();
        let options = BatchOptions {
            max_write_batch: 10,
            ..Default::default()
        };
        let (actor, tx) = actor(dir.path(), options);
        let commits = actor.commits.clone();
        for i in 0..25 {
            let (cmd, _rx) = set_msg(i);
            tx.send(cmd).await.unwrap();
        }
        let (cmd, rx) = sync_msg();
        tx.send(cmd).await.unwrap();
        let task = tokio::spawn(actor.run());
        rx.await.unwrap().unwrap();
        assert_eq!(commits.load(Ordering::Relaxed), 3);
        drop(tx);
        task.await.unwrap();
    }
}
