//! Measures the bytes written to the device per small-blob insert.
//!
//! `store` drives a real `FsStore` the way iroh-docs does. Every value goes
//! through `add_bytes(..).temp_tag()`, with one `sync_db` per batch (the
//! source path). The bytes are then imported as bao into a second store (the
//! receiver path). `redb` replays the same insert stream against raw redb
//! tables in alternative key layouts.
//!
//! The written-bytes counter is Linux `/proc/self/io` `write_bytes`, which
//! counts bytes this process caused to be sent to the block layer.
use std::{path::PathBuf, time::Instant};

use anyhow::Result;
use bao_tree::ChunkRanges;
use clap::{Parser, Subcommand, ValueEnum};
use iroh_blobs::{
    store::fs::{options::Options, FsStore},
    Hash,
};
use rand::{rngs::StdRng, RngExt, SeedableRng};
use redb::{Durability, TableDefinition};

#[derive(Parser)]
struct Args {
    /// Directory for the stores; must not exist.
    #[clap(long)]
    dir: PathBuf,
    /// Number of blobs.
    #[clap(long, default_value_t = 1_000_000)]
    count: usize,
    /// Blob size in bytes.
    #[clap(long, default_value_t = 256)]
    size: usize,
    /// Inserts per commit.
    #[clap(long, default_value_t = 128)]
    batch: usize,
    /// Concurrent writer tasks in `store` mode, each doing its own batches
    /// and `sync_db`; `--count` is split between them.
    #[clap(long, default_value_t = 1)]
    writers: usize,
    /// Override `BatchOptions::max_write_batch` (meta commands per commit).
    #[clap(long)]
    max_write_batch: Option<usize>,
    /// Report every this many blobs.
    #[clap(long, default_value_t = 100_000)]
    report: usize,
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Real FsStore: source add_bytes, then receiver bao import.
    Store {
        /// Skip the receiver phase.
        #[clap(long)]
        source_only: bool,
    },
    /// Open the `source` store under `--dir` and time it. With a store made
    /// by the legacy layout this times the migration.
    Open,
    /// Raw redb in the given key layout.
    Redb {
        #[clap(long, value_enum)]
        layout: Layout,
    },
}

#[derive(Clone, Copy, ValueEnum, Debug)]
enum Layout {
    /// Today's tables: hash -> state, hash -> data.
    Two,
    /// One table: hash -> state + data.
    One,
    /// hash -> seq (state), seq -> data, seq ascending per insert.
    Seq,
}

fn write_bytes() -> u64 {
    let io = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    io.lines()
        .find_map(|l| l.strip_prefix("write_bytes: "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

fn dir_size(p: &std::path::Path) -> u64 {
    walkdir::WalkDir::new(p)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

struct Meter {
    label: &'static str,
    start_bytes: u64,
    last_bytes: u64,
    start: Instant,
}

impl Meter {
    fn new(label: &'static str) -> Self {
        let b = write_bytes();
        Self {
            label,
            start_bytes: b,
            last_bytes: b,
            start: Instant::now(),
        }
    }

    fn report(&mut self, done: usize, step: usize, dir: &std::path::Path) {
        let b = write_bytes();
        println!(
            "{} n={done} total_B/blob={:.0} step_B/blob={:.0} disk_B/blob={:.0} t={:.1}s",
            self.label,
            (b - self.start_bytes) as f64 / done as f64,
            (b - self.last_bytes) as f64 / step as f64,
            dir_size(dir) as f64 / done as f64,
            self.start.elapsed().as_secs_f64(),
        );
        self.last_bytes = b;
    }
}

fn values(count: usize, size: usize, seed: u64) -> impl Iterator<Item = bytes::Bytes> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count).map(move |_| {
        let mut v = vec![0u8; size];
        rng.fill(&mut v[..]);
        v.into()
    })
}

async fn open_store(dir: &std::path::Path, args: &Args) -> Result<FsStore> {
    let mut options = Options::new(dir);
    if let Some(n) = args.max_write_batch {
        options.batch.max_write_batch = n;
    }
    FsStore::load_with_opts(dir.join("blobs.db"), options)
        .await
        .map_err(|e| anyhow::anyhow!("{e:#}"))
}

async fn run_store(args: &Args, source_only: bool) -> Result<()> {
    let src_dir = args.dir.join("source");
    let src = open_store(&src_dir, args).await?;
    let mut meter = Meter::new("source");
    let per_writer = args.count / args.writers;
    let mut tasks = Vec::new();
    for w in 0..args.writers {
        let src = src.clone();
        let (size, batch_len, report, writers) = (args.size, args.batch, args.report, args.writers);
        let dir = src_dir.clone();
        tasks.push(tokio::spawn(async move {
            // A single writer reports as it goes, as the original bench did.
            let mut meter = (writers == 1).then(|| Meter::new("source"));
            let mut hashes = Vec::with_capacity(per_writer);
            let mut tags = Vec::new();
            let mut batch = Vec::with_capacity(batch_len);
            let mut since = 0;
            for v in values(per_writer, size, 1 + w as u64) {
                batch.push(v);
                if batch.len() == batch_len {
                    let futs = batch
                        .drain(..)
                        .map(|v| async { src.add_bytes(v).temp_tag().await });
                    for tt in n0_future::join_all(futs).await {
                        let tt = tt?;
                        hashes.push(tt.hash());
                        tags.push(tt);
                    }
                    src.sync_db().await?;
                    // iroh-docs drops the temp tags once the doc entries commit.
                    tags.clear();
                    since += batch_len;
                    if since >= report {
                        if let Some(m) = meter.as_mut() {
                            m.report(hashes.len(), since, &dir);
                        }
                        since = 0;
                    }
                }
            }
            src.sync_db().await?;
            anyhow::Ok(hashes)
        }));
    }
    let mut hashes = Vec::with_capacity(args.count);
    for t in tasks {
        hashes.extend(t.await??);
    }
    meter.report(hashes.len(), hashes.len().min(args.report).max(1), &src_dir);
    if source_only {
        src.shutdown().await?;
        return Ok(());
    }

    let dst_dir = args.dir.join("receiver");
    let dst = open_store(&dst_dir, args).await?;
    let mut meter = Meter::new("receiver");
    let mut since = 0;
    for (i, chunk) in hashes.chunks(args.batch).enumerate() {
        let futs = chunk.iter().map(|h| {
            let src = &src;
            let dst = &dst;
            async move {
                let bao = src.export_bao(*h, ChunkRanges::all()).bao_to_vec().await?;
                dst.import_bao_bytes(*h, ChunkRanges::all(), bao).await?;
                anyhow::Ok(())
            }
        });
        for r in n0_future::join_all(futs).await {
            r?;
        }
        dst.sync_db().await?;
        since += chunk.len();
        if since >= args.report {
            meter.report((i + 1) * args.batch, since, &dst_dir);
            since = 0;
        }
    }
    meter.report(hashes.len(), since.max(1), &dst_dir);
    dst.shutdown().await?;
    src.shutdown().await?;
    Ok(())
}

const STATE: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("state");
const DATA: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("data");
const BY_SEQ: TableDefinition<u64, &[u8]> = TableDefinition::new("by-seq");

fn run_redb(args: &Args, layout: Layout) -> Result<()> {
    std::fs::create_dir_all(&args.dir)?;
    let path = args.dir.join("meta.redb");
    let db = redb::Database::create(&path)?;
    let mut meter = Meter::new("redb");
    // A complete inline entry's encoded state is a few bytes.
    let state = [1u8, 0, 0];
    let mut it = values(args.count, args.size, 1).enumerate().peekable();
    let mut since = 0;
    let mut done = 0;
    while it.peek().is_some() {
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut st = tx.open_table(STATE)?;
            let mut data = tx.open_table(DATA)?;
            let mut by_seq = tx.open_table(BY_SEQ)?;
            for (seq, v) in it.by_ref().take(args.batch) {
                let hash: [u8; 32] = *Hash::new(&v).as_bytes();
                match layout {
                    Layout::Two => {
                        st.insert(&hash, &state[..])?;
                        data.insert(&hash, &v[..])?;
                    }
                    Layout::One => {
                        let mut row = state.to_vec();
                        row.extend_from_slice(&v);
                        st.insert(&hash, &row[..])?;
                    }
                    Layout::Seq => {
                        let mut row = state.to_vec();
                        row.extend_from_slice(&(seq as u64).to_be_bytes());
                        st.insert(&hash, &row[..])?;
                        by_seq.insert(seq as u64, &v[..])?;
                    }
                }
                done += 1;
                since += 1;
            }
        }
        tx.commit()?;
        if since >= args.report {
            meter.report(done, since, &args.dir);
            since = 0;
        }
    }
    meter.report(done, since.max(1), &args.dir);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if matches!(args.cmd, Cmd::Open) {
        let start = Instant::now();
        let store = open_store(&args.dir.join("source"), &args).await?;
        println!("open: {:?}", start.elapsed());
        store.shutdown().await?;
        return Ok(());
    }
    anyhow::ensure!(!args.dir.exists(), "{} exists", args.dir.display());
    match args.cmd {
        Cmd::Open => unreachable!(),
        Cmd::Store { source_only } => run_store(&args, source_only).await,
        Cmd::Redb { layout } => run_redb(&args, layout),
    }
}
