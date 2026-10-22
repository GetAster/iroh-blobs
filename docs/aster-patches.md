# Aster patch ledger: iroh-blobs

This repository is Aster's fork of [n0-computer/iroh-blobs](https://github.com/n0-computer/iroh-blobs). Aster carries the patches listed below on top of upstream releases. Upstream maintainers are welcome to incorporate any of them, in whole or in part, at their discretion.

## Branch and tag model

- `main` is upstream `main` plus one commit: the Aster README preamble and this ledger. Nothing is built from `main`. It is refreshed with `git merge upstream/main` and is never reset or force-pushed. Ledger edits are commits on `main`, never on `upgrade/*`.
- `upgrade/<repo>-v<upstream version>` (for example `upgrade/iroh-v1.4.0`, `upgrade/iroh-blobs-v0.103.1`) is the patched integration line: the upstream release plus the patches in this ledger. A new upstream base always gets a new branch, cut from the upstream release tag; Aster-only fixes and re-pins append to the current branch. Per-minor names (`upgrade/iroh-blobs-v0.103`, `upgrade/salvo-v1.0`, …) are historical. Consumers pin commits on it. Published `upgrade/*` branches and tags are never rewritten; a failed attempt is superseded by new commits and a new version.
- Topic branches carry one change each, are named for the change (for example `feat/read-into`, `security/plumtree-bounds`), are based on the upstream release tag (or, for a patch that builds on another, on that prerequisite patch's commit, which its row names), and are cherry-picked with `git cherry-pick -x` onto the upgrade branch, so every upgrade-branch commit names its source. A topic branch a row names is not rewritten; re-cutting it onto a new upstream base takes a versioned name, `<topic>-v<upstream version>`.
- Version: the upstream version if Aster's registry has never served that version or a higher one in the series; otherwise the next patch above Aster's highest published version. A re-pin without a code change keeps the version and takes the tag suffix `-p<n>`.
- Tags: `aster-<repo>-v<published version>` (re-pins `aster-<repo>-v<published version>-p<n>`), created only after the Aster consumer trial passes against those exact commits, because tags cannot be moved.
- The fork manifest is `iroh-fork-manifest.toml` in the Aster repository (the only copy).
- Aster does not submit patches upstream; upstream maintainers are welcome to take any patch here.

Historical and current tag schemes in this repository:

- `aster-iroh-blobs-v<version>`, for example `aster-iroh-blobs-v0.101.0`, `aster-iroh-blobs-v0.102.0`, `aster-iroh-blobs-v0.103.10`, `aster-iroh-blobs-v0.103.11` (the first on upstream `v0.103.1`), `aster-iroh-blobs-v0.103.12`.
- `aster-iroh-blobs-v<version>-p<n>`, a later commit at the same crate version, for example `aster-iroh-blobs-v0.103.0-p1`…`-p3`, `aster-iroh-blobs-v0.103.1-p1`, `aster-iroh-blobs-v0.103.5-p1` and `-p2`, `aster-iroh-blobs-v0.103.12-p1` (the recorded ones are patch-block re-pins with no source change).

## Current release

| Field | Value |
|---|---|
| Upstream base | `v0.103.1` |
| Integration branch | `upgrade/iroh-blobs-v0.103.1` (cut from `v0.103.1`; `upgrade/iroh-blobs-v0.103` is the historical per-minor line on `v0.103.0`) |
| Tag | `aster-iroh-blobs-v0.103.12-p1` (re-pin to noq 1.4.2) |
| Pinned commit | `63ec9fc4afc9f098c932ebc6443266509e64f420` |
| Crate version | 0.103.12 (workspace spec `iroh-blobs = "0.103.12"`; the registry already served 0.103.11, so the row 26 stall-clock fix publishes 0.103.12) |
| Source repository | `https://forge.emrul.dev/Aster/iroh-blobs.git` |

The fork's own `[patch.crates-io]` (row 5, re-pinned by `63ec9fc4`) pins iroh `91a92aa0` (1.4.0-p2, the noq 1.4.2 re-pin), noq `a3c70b7a` (1.4.2) and iroh-util `c9b91fce` (0.6.3-p4), all matching the manifest. Upstream `v0.103.1` requires iroh-util 0.6.0; `15d47bdb` raises the requirement to 0.6.3 for row 34.

Bootstrap pin line for source-tree builds (consumers install a versioned Aster SDK release instead and must not copy this):

```toml
iroh-blobs = { git = "https://forge.emrul.dev/Aster/iroh-blobs.git", rev = "63ec9fc4afc9f098c932ebc6443266509e64f420" }
```

## Patches

Row numbers are stable patch ids shared across Aster's iroh-family forks (iroh, iroh-blobs, iroh-docs, iroh-gossip, iroh-util, noq), so a commit message that says "row 26" resolves here. This repository's dependency patch block shares number 5 with an unrelated iroh row; it is listed here as 5.

| Id | Topic branch | What | Why / what in Aster depends on it | Commits (topic / upgrade) | Status | Conflict or porting notes |
|---|---|---|---|---|---|---|
| 5 | none recorded (original commit `14a2286e`) | `[patch.crates-io]` entries for the Aster `iroh`, `iroh-base`, `iroh-relay` and `noq` forks (and, since row 34, `iroh-util`), so the crate resolves the same fork stack when built standalone. File: `Cargo.toml`. | Prevents dependency-graph splits when building or testing iroh-blobs outside the Aster repository, which is where fork patches get validated. Aster's own root patch overrides it. | `14a2286e` (original); rc.1 copy `94964e8e`; re-pinned in later waves (see History); on `upgrade/iroh-blobs-v0.103.1`: `f4fb924b`, `6f6b4195` (noq 1.4.1), `63ec9fc4` (noq 1.4.2) | carried | Low conflict risk. Pin explicit fork revs or tags. Drop stale `ed25519-dalek 3.0.0-pre.6` comments. |
| 6 | `fix/iroh-blobs-batch-small-reads` | Adds `Blobs::get_bytes_many_if_complete`, backed by a batched fs-store inline-data read command plus the existing validated export fallback for non-inline complete blobs. Files: `src/api/blobs.rs`, `src/api/proto.rs`, `src/store/fs.rs`, `src/store/fs/meta.rs`, `src/store/fs/meta/proto.rs`, `src/store/mem.rs`, `src/store/readonly_mem.rs`. | `aster_transport_core::CoreDoc::read_entry_contents_if_complete` exposes it to portal-cas; `Manifest::read_snapshot` reads all object/link value blobs with two batch calls instead of one status plus one read per entry. | `a0cbdd15` (rc.1, on `upgrade/iroh-blobs-v0.102`); carried forward since (`cb90ffe4` on `upgrade/iroh-blobs-v0.103`, `6174d23d` on `upgrade/iroh-blobs-v0.103.1`) | carried | Medium conflict risk around the blob store request enum and the fs/meta actor command enums. Preserve response ordering, missing/partial as `None`, inline hash verification, and fallback through `get_bytes` for non-inline complete blobs. Test: `cargo test -p iroh-blobs test_get_bytes_many_if_complete`. |
| 7 | `fix/iroh-blobs-batch-status` | Adds `Blobs::status_many`, with batched store-actor status handling for the fs, mem and readonly-mem stores. Same files as row 6. | The iroh-docs `LiveActor` uses it to batch local completeness checks while scheduling many content downloads from a RemoteInsert burst (iroh-docs row 15). | `e20ff029` (rc.1); `60d098c9` on `upgrade/iroh-blobs-v0.103`, `c7c3e6c8` on `upgrade/iroh-blobs-v0.103.1` | carried | Medium conflict risk, same enums as row 6. Preserve response ordering and exact single-`status` semantics for complete, partial and missing blobs. Test: `cargo test -p iroh-blobs test_status_many`. |
| 8 | `feat/multifetch` (based on `v0.103.0`) | Makes download progress observable. Root fetch in `Split` mode reports through the real sink instead of `Drain`, minus its legacy `Progress` offsets (`RootProgressFilter`), because split mode runs two independent counters and forwarding both made the offset go backwards (e.g. `[384, 1, 2, …]`). The split fan-in is drained after the futures stream ends (previously a fast final child lost its `PartComplete`). `TryProvider` moves after the `local.is_complete()` early return, so an already-resident request is not reported as an attempt. New additive `DownloadProgressItem::BytesTransferred(u64)` gives a per-attempt total of successfully decoded payload bytes (`Progress` is a cumulative offset that includes local bytes and is re-based per child, so it cannot be summed). Provider ordering, failover, resumption and split scheduling are unchanged. Files: `src/api/downloader.rs`, `src/util.rs`. | `aster_transport_core::CoreBlobsClient::run_download` folds the stream into `CoreFetchReport`, which `aster::Blobs::download_hash_from` / `download_hash_to_store_from` / `download_collection_from` expose to portal-sync for topology-aware provider ranking. | `adab99f1`..`02d6a2e3` / `a2e15663`..`7e0da6f7` (released as 0.103.1; earlier multifetch work as 0.103.0); on `upgrade/iroh-blobs-v0.103.1`: `031b0902`, `71588096`, `05683aec` | carried | Medium conflict risk in `execute_get` and `handle_download_split_impl`. Preserve `Progress` monotonicity in split mode; `BytesTransferred` comes from the returned `Stats::payload_bytes_read` on success and from the last raw sink value on failure. The statement order of `pool.get_or_connect` and the local scan is inert (`get_or_connect` is an `async fn`, so nothing happens until awaited); it is left as upstream has it and no overlap is claimed. Six tests in `api::downloader::tests` pin each behaviour. `BytesTransferred` is a transfer metric (the sum of what each attempt decoded), not a content size. Its shape changed in row 35. |
| 23 | `fix/gc-sweep-custody` (tip `64b1631c3e`, a single stack on `v0.103.0`; backfilled) | Four stacked GC fixes: (1) a tag created after the sweep's mark joins the in-flight sweep's delete-time protection (the DeleteBlobs dispatch filters live temp tags in the actor, linearizing tag creation against the sweep); (2) HashSeq custody protects the children too, via `AddProtected`; (3) an unenumerable HashSeq claim poisons the in-flight sweep (`Protection::sweep_poisoned`) instead of letting it delete members nobody can enumerate; (4) every collection root is traversed, novel hash or not. Files: `src/store/gc.rs`, `src/store/fs.rs`, `src/store/fs/meta.rs` (+`/proto`), `src/api/{blobs,proto,tags}.rs`, `src/util/temp_tag.rs`, `src/store/{mem,readonly_mem}.rs`. | portal-sync's retention tags and `gc_run_once` cadence exercise exactly these windows; the K-matrix dedupe-untag and blob-retention scenarios are the downstream regressions. | `64b1631c3e` / `188c4555`..`bf21d4fe`; released as 0.103.2–0.103.5; on `upgrade/iroh-blobs-v0.103.1`: `d3a95af3`, `e9c10b67`, `2c625b8e`, `268f23c5` (`-x` of the 0.103 commits, so they carry their 0.103.2–0.103.5 version bumps) | carried | Medium conflict risk against rows 6 and 7 (shared import lists and command enums in `fs/meta`). The four commits are textually interdependent in `store/gc.rs` and `fs/meta.rs`: port as a unit, in order. The topic branch was reconstructed after release, so its `(cherry picked from …)` trailers point at the upgrade-branch commits (inverted provenance; see History). 12 GC tests (`store::gc::tests`, mem and fs) pin all four behaviours. `cargo fmt --check` reports existing drift in this GC code. |
| 26 | `fix/provider-stall-failover` (tip `47cab4d6`, stacked on `feat/multifetch`) | Adds `DownloadRequest::provider_idle_timeout` (`with_provider_idle_timeout`). `execute_get` only failed over when an attempt errored; a provider that accepts a request and then stops sending never errors, because iroh's default QUIC keep-alive keeps the connection alive, so one stalled provider blocked every provider after it. With the timeout set, each attempt runs under a stall guard whose clock restarts on every payload `Progress` and covers the whole attempt (stream open, first chunk, last chunk). A stalled attempt reports `BytesTransferred` + `ProviderFailed` exactly like a failed one, and the next provider is asked only for `local.missing()`. Default `None` keeps upstream behaviour. File: `src/api/downloader.rs`. | `aster_transport_core` ordered fetch (`download_hash_from` and friends) sets it, so a stalled provider no longer pins portal-sync's fetch until its whole-call timeout, which also made Portal blame the wrong provider (portal-sync fetch-locality request R4). | `47cab4d6` / `2938c882`; released as 0.103.6 at `8fa6fea6`; on `upgrade/iroh-blobs-v0.103.1`: `032d6d98`. Stall-clock fix: topic `fix/stall-guard-local-work` `eae1aada` (based on row 35's `016fb4b3`) / `97d86c31`, released as 0.103.12 at `5e1f4a6f` | carried | Low–medium conflict risk in `execute_get` (shares the attempt-byte counter with row 8, hence stacked). Threaded through the plain path, `split_request`'s root fetch and every split child. Adding a pub field breaks struct-literal construction; every in-tree and Aster caller uses `DownloadRequest::new`. Three tests: the default blocks on a stalled lead, and plain and split modes fail over with a 500 ms idle timeout; disabling the guard fails both fail-over tests. Since 0.103.12 the clock (`StallClock`) runs only while the attempt waits on the provider: `execute_get_sink` stops it while it opens the store import, hands an item to the progress sink and the store, reads a collection root back, and from a blob's last payload byte until the store has committed it, and restarts it when each wait ends. Before, a store still committing the last items (or applying backpressure), or a consumer that stopped draining progress, counted as provider idle, so a provider that had delivered everything could be failed; on a loaded host that made `downloader_idle_timeout_fails_over_from_a_stalled_provider` fail about one lib run in five (holder delivered all 1 MiB, last progress at 800 ms, guard fired at 1302 ms). Tests `stall_clock_stops_during_local_work` and `downloader_idle_timeout_waits_for_a_slow_progress_consumer` fail when local waits do not stop the clock. Under far heavier load (each test's current-thread runtime starved by burners plus parallel suites) the 500 ms tests can still fail because the in-process holder itself delivers nothing for 500 ms or its dial times out; that is the tests' budget, not this row. |
| 27 | `fix/fs-store-tokio-features` (tip `8c0117e0`, on `v0.103.0`) | The `fs-store` feature now enables `tokio/rt-multi-thread`. The fs store builds a multi-threaded runtime (`FsStore::load_with_opts`) and drops it under `block_in_place` (`RtWrapper`), but the crate asked tokio only for `sync` and relied on another dependency to enable the rest. With the 1.3.0-wave pins nothing in the lib graph does, so `cargo check --lib` and `cargo publish`'s package verification fail (E0425/E0599). File: `Cargo.toml`. | The Aster native publish wave verifies each fork crate standalone; 0.103.6 failed there (Aster 0.3.1014 promote job, 2026-10-14). Aster's own workspace enables the feature, so its CI did not see it. | `8c0117e0` / `b28c5260`; released as 0.103.7 at `f04ca2cd`; on `upgrade/iroh-blobs-v0.103.1`: `4277ccab` | carried | Trivial conflict risk (one feature list). Upstream has the same latent gap at `v0.103.0`; drop the row if upstream declares the feature. `cargo check --lib` reproduces it without the patch. |
| 28 | `feat/zstd-transport` (tip `7330ac18`, stacked on `fix/provider-stall-failover`) | First ports upstream's unreleased `Downloader::builder` (`6bd5f2b0`, cherry-pick of upstream `1ff1585a`, #283: `.alpn(..)`, `.pool_options(..)`; `new_with_opts` deprecated), then adds stream transforms. `transport::StreamTransform` wraps both halves of every stream. `TransformedBlobsProtocol` serves the unmodified provider on a derived ALPN with the same `EventSender` (`provider::handle_connection_with_transform`); its `shutdown` leaves the store to the plain handler. `DownloaderBuilder::stream_transform` wraps every downloader attempt, so the ordered walk, local-aware resume, stall guard and progress attribution are unchanged. `DownloaderBuilder::plain_fallback(ttl)` retries a provider on `crate::ALPN` when its handshake fails with TLS alert 120 (`no_application_protocol`), emits the new `DownloadProgressItem::PlainFallback { id, cached }`, and remembers the refusal for `ttl`. Feature `zstd` (off by default; `zstd` 0.14 with `default-features = false`, so zstd-sys) adds `ZstdTransform` on `ZSTD_ALPN` = `/iroh-bytes/4+zstd/1`: level 3 by default, clamped to 1..=19, frame checksums, receiver window capped at 2^23. Every `SendStream::sync` ends a frame. A stream that ends inside a frame, or is not zstd, fails with `InvalidData` and is counted in `ZstdStats::decode_errors`. Observe updates now `sync` after each item (a no-op on plain streams). Files: `src/api/downloader.rs`, `src/transport.rs`, `src/transport/zstd.rs`, `src/provider.rs`, `src/lib.rs`, `Cargo.toml`. | portal-sync request `2026-08-22-iroh-blobs-transport-compression`: compress Portal's text-heavy content fetches without giving up Aster's ordered multi-provider fetch. Measured on the crate's own sources: 4.53x (77.9% saved) at level 3. | `6bd5f2b0` + `7330ac18` / `e9b8e986` + `175f98a7`; released as 0.103.8 at `4971183c`; on `upgrade/iroh-blobs-v0.103.1`: `f82dce8b` + `36dda67d` | carried | The default `Downloader` is byte-for-byte upstream on the wire (same ALPN, same streams, same attempt path). Adding a `DownloadProgressItem` variant breaks exhaustive matches; Aster's `FetchAccumulator` is the known one. Collapse the #283 part when an upstream release contains it. The transform API and the zstd framing (one frame per `sync`) are Aster's own; renaming `ZSTD_ALPN` is a wire break. Tests: 6 codec tests and 8 downloader tests in `transport::zstd::tests` (round trip, partial resume, ordered fail-over attribution, fresh and cached fallback, no fallback, local short-circuit, stall fail-over, garbage and truncated providers that fail over without completing the blob), plus an ignored throughput measurement. Ignoring the transform, removing the fallback, not ending frames on `sync`, or reading a mid-frame end as clean each fail them. |
| 34 | `fix/downloader-lost-connection` (tip `826c6bd2`, stacked on `feat/zstd-transport`) | With `provider_idle_timeout` set, the stall guard also records `conn.stats().udp_rx.datagrams` before the request; if it has not moved after `silence_bound` (4 × the slowest path RTT, at least 1 s, at most the idle timeout) the attempt is `Abandoned::Silent`. A silent attempt, or one whose connection closed by `Reset`/`TimedOut`/`LocallyClosed`, is lost: the downloader retires the connection (`ConnectionPool::retire`, primary or plain-fallback pool) and retries the same provider once on a fresh connection before moving on; each try emits `TryProvider`, the failed one `ProviderFailed`. Requires iroh-util 0.6.3 (iroh-util row 33). The upgrade-branch commit also re-pins the fork's patch block to iroh `a4a9a76b` / noq `e75f3a55` (1.3.2) and iroh-util `6c42d23`. No idle timeout, no change. Files: `src/api/downloader.rs`, `Cargo.toml`, `Cargo.lock`. | Root cause 3 of the Aster restart/reconnect investigation (Aster repository, `docs/_internal/investigations/2026-10-15-restart-reconnect.md`): after a restart on a new port nothing resets the old connection, so the pool handed it out until QUIC's 30 s idle timeout. Loopback probe, new port: blob recovery 31.1–32.0 s → 1.1–1.6 s from kill. A same-port reset is now also retried within the same fetch. | `d38d5053` + `826c6bd2` / `21dce20e` + `e1cb0b07`; released as 0.103.9 at `094bdd2f`; on `upgrade/iroh-blobs-v0.103.1`: `15d47bdb` (the iroh-util 0.6.3 requirement only; the stack re-pin is row 5's `f4fb924b`) + `e5797e59` | carried | **Aster's own; no upstream equivalent (checked 2026-10-15): n0-computer/iroh-blobs #141, #185, #213, #224, #226, #227, #229, #283 and n0-computer/iroh-util #1–#19.** Medium conflict risk in `execute_get` (loop restructured for the retry; rows 26 and 28 touch the same function); `Dialed` gains a `pool` field. Heuristic risk: a healthy connection on which nothing at all arrives for the bound (every stream blocked, keep-alive not yet due) is retired and redialed once. Tests: `downloader_recovers_from_a_provider_restarted_on_a_new_port` (provider killed by runtime shutdown, back on a new port with the same key; fetch 1.01–1.02 s, tried [p, p], failed [p]); the 0.103.8 downloader stalls 10.0 s and fails; without retire + retry it fails at 1 s. The existing stall fail-over tests (plain, split, zstd) still treat the live stalling provider as stalled. lib 129 (3 ignored) + integration 23; clippy `--all-features --all-targets -D warnings` clean; `downloader.rs` fmt clean (existing fmt drift in GC files untouched). |
| 35 | `feat/attempt-transfer-stats` (tip `016fb4b3`, stacked on `fix/downloader-lost-connection`) | `DownloadProgressItem::BytesTransferred(u64)` becomes `BytesTransferred { id, request, bytes, wire_bytes }`: the attempt's provider and request, its payload, and `wire_bytes`, what its streams received before the stream transform decoded them (`None` over plain streams); emitted when either figure is non-zero. `TryProvider` gains `stats: AttemptStats` (`#[serde(skip)]`), the attempt's live counters (`payload_bytes()`, `wire_bytes()`, `transform_tally()`), updated as the attempt runs, so a consumer that stopped reading the stream (a cancelled download) still sees what an unfinished attempt received. `StreamTransform` gains `send_stream_tallied` / `recv_stream_tallied` (defaults ignore the tally), `TransformedConnection::with_tally`, and `transport::StreamTally`; the downloader passes each transformed attempt its own tally. `ZstdSendStream` / `ZstdRecvStream::with_tally` count into it as well as the shared `ZstdStats`, so node-wide counters are unchanged. Files: `src/api/downloader.rs`, `src/transport.rs`, `src/transport/zstd.rs`. | portal-sync request `2026-10-16-aster-transfer-metrics` M3 and per-peer fetched wire bytes (M1): `aster_transport_core`'s `FetchAccumulator` credits payload and zstd wire bytes per provider (`ProviderOutcome::bytes_transferred()` / `wire_bytes()`), exact under split and failover and in a tracker report taken after cancellation; `BlobMeter` credits per-peer `fetched_wire_bytes` / `fetched_plain_bytes`. Before, `BytesTransferred` went to whichever provider was announced last, and the fetch side's one shared zstd transform could not split wire bytes by provider. | `016fb4b3` / `35eb0f03`; released as 0.103.10 at `bcd00bb2`; on `upgrade/iroh-blobs-v0.103.1`: `7e10ccba` | carried | **Aster's own; no upstream equivalent (checked 2026-10-16): upstream `main` (`1ff1585a`) has no `BytesTransferred` (row 8) and no stream transforms (row 28); issues/PRs searched for per-provider or per-attempt download bytes and transform counters: #4 (multi-provider fan-in, open), #147 (stream type abstraction), #225, #232, #250, #270, #271–#290; none adds attempt counters.** Breaking API in a patch release: `BytesTransferred` is a struct variant and `TryProvider` has a third field, so exhaustive matches and struct-literal patterns break; Aster's `FetchAccumulator` and `BlobMeter` are the known consumers. `AttemptStats` reads zero across an rpc boundary. Medium conflict risk in `execute_get` (rows 8, 26, 28, 34) and the zstd stream structs; low in `transport.rs` (default trait methods). Tests: `downloader_split_credits_bytes_per_provider` (three children; root and child 0 from a half holder, children 1–2 from the full one; exact bytes per provider; every `BytesTransferred` names its own attempt), `downloader_credits_a_provider_that_dies_mid_transfer` (throttled provider shut down after part of the blob is stored; both credited, summing to the blob), `downloader_attempt_stats_survive_a_cancelled_download` (download dropped mid-attempt; live payload > 0, ≥ stored, no `BytesTransferred`, no wire bytes), `zstd_wire_bytes_are_attributed_per_provider` (dying zstd provider then holder: wire < payload for each, wire sum = node-wide `ZstdStats::wire_bytes_read`; a plain fallback reports `None`), `zstd_attempt_stats_survive_a_cancelled_download` (live wire < live payload, = node-wide). `downloader_counts_bytes_from_a_partial_provider` also checks per-provider credit and live counters. Ablations: not tallying zstd streams fails both zstd tests; not updating the live counter fails the cancellation tests and the dies-mid-transfer test. lib 134 (3 ignored) + integration 6 with `--all-features`; clippy `--all-targets -D warnings` with and without `--all-features`; touched files fmt clean. |
| 38 | none of its own: carried on `fix/iroh-blobs-batch-small-reads` (original `8707ffa4`, rc.1; backfilled 2026-10-21) | Re-exports `gc_run_once` from `iroh_blobs::store`, next to `GcConfig`, `ProtectCb` and `ProtectOutcome`, so an embedder can run one synchronous GC pass. One line. File: `src/store/mod.rs`. | `aster_transport_core` imports it (`core/src/lib.rs`, `use iroh_blobs::store::{gc_run_once as blobs_gc_run_once, …}`) for the manual GC sweep that portal-sync's retention tests and `gc_run_once` cadence drive (row 23). | `8707ffa4` / `45056b3a` (in 0.103.0); on `upgrade/iroh-blobs-v0.103.1`: `e698ced3` (`-x` of both) | carried | Trivial conflict risk (one `pub use` line). Missing from this ledger until 2026-10-21, though carried since the rc.1 port. Drop the row if upstream exports `gc_run_once`. |

Bookkeeping and documentation commits (not needed at runtime):

| Commits | Files | Purpose | Status |
|---|---|---|---|
| `39ad2947` (rc.1) | `.gitignore` | Ignores local `docs/aster/` internal notes. | Recorded in the rc.1 stack; current status not recorded. Optional. |
| `c23a66ac` (rc.1) | `Cargo.lock` | Resolves the Aster iroh/noq fork crates. Regenerated each wave. | carried in spirit (regenerated, not cherry-picked). |
| `ede454c7` | `README.md` | Aster fork overview and contribution table. | Replaced by the `main` README preamble under the current model; if reused, refresh PR statuses. |

### Upstreamed or dropped

No iroh-blobs patch has been upstreamed or dropped. Not carried onto `upgrade/iroh-blobs-v0.103.1`: the GC docs commits `758d4eeb`, `3e24fdc5`, `23e90ac1` (`docs/upstream-iroh-blobs-hashseq-gc-repro.md`, the upstream HashSeq repro write-up; they stay on `upgrade/iroh-blobs-v0.103`), `.gitignore` `b9f5c5f4`, and the superseded 0.103 re-pin and release commits. Row 28 carries a port of upstream's unreleased `Downloader::builder` (#283, `1ff1585a`); that part collapses when an upstream release contains it.

### Closed security-review branches

These topic branches came from an April 2026 security review. The fixes from that review that held up were merged upstream, so these branches are not carried on the upgrade branch. They are kept for reference.

| Branch |
|---|
| `security/error-handling-cleanup` |
| `security/file-import-safety` |
| `security/protocol-input-validation` |
| `security/store-robustness` |

## Porting to a new upstream release

Order across the iroh family: noq, then iroh, then the satellites in the Aster publisher's order: iroh-util, iroh-gossip, this repository, iroh-docs. Row 34 needs iroh-util's `ConnectionPool::retire`, and iroh-docs pins this fork, so publish iroh-blobs before iroh-docs.

Find the latest upstream release with `git tag --sort=-v:refname | grep -vE 'aster|archive'`; `--sort=-creatordate` lists `v0.97.1` first here. Before touching branches, read the upstream changelog and the commit range from `v0.103.0` to the target (e.g. `git log --oneline v0.101.0..v0.102.0` and `git diff --stat` in the rc.1 port), and decide for each row: keep, drop because upstream absorbed it, or rewrite. Check #283 (row 28) and whether upstream declares `tokio/rt-multi-thread` for `fs-store` (row 27).

Reapply order on the new base:

1. Row 5 (patch block, re-pointed at the new noq/iroh/iroh-util fork revs), then regenerate `Cargo.lock`. If the lock keeps a second, crates.io iroh, name the old one: `cargo update -p iroh@<old> --precise <new>` (1.4.0 wave: `iroh@1.1.0 --precise 1.4.0`).
2. Rows 6 and 7 (batch API).
3. Row 38 (`gc_run_once` export).
4. Row 23 (GC sweep custody), as a unit, in order. Its commits carry the 0.103.2–0.103.5 bumps, so the release commit sets the version explicitly.
5. Row 27.
6. The stacked downloader line, in order: row 8 (`feat/multifetch`) → row 26 (`fix/provider-stall-failover`) → row 28 (`feat/zstd-transport`) → row 34 (`fix/downloader-lost-connection`; first raise the iroh-util requirement to 0.6.3 if the base still asks for less) → row 35 (`feat/attempt-transfer-stats`).

When the real port replays a trial commit whose message already carries `-x` lines naming published commits (every row since 0.103.0), cherry-pick it without `-x`: another trailer would name an unpublished trial commit.

```bash
git fetch --all --tags --prune
git push origin <upstream-tag>          # mirror the upstream tag (e.g. v0.103.1) to the forge
git switch -c upgrade/iroh-blobs-v<upstream version> <upstream-tag>
# cherry-pick -x each row's commits in the order above
# version bump, only if the version rule requires it (Aster's registry has already
# served <upstream version> or higher in the series, as it has 0.103.0..0.103.11):
# set the crate to the next patch above Aster's highest published, then
git commit -am "chore: release <published version>"
git push origin upgrade/iroh-blobs-v<upstream version>
# do not tag yet: tag after the consumer trial (below)
```

Prefer `cherry-pick -x`; if a patch no longer applies, treat the "What" and "Conflict or porting notes" columns as the specification. Published `upgrade/*` branches and tags are never rewritten; a failed attempt is superseded by new commits and a new version.

An Aster-only fix on the current base is a topic branch based on the upstream release tag (or on the commit of the patch it builds on, as rows 26–35 stack), cherry-picked `-x` onto the current upgrade branch, followed by a version-bump commit to the next patch above Aster's highest published version (as 0.103.6 through 0.103.10 were cut). A re-pin without a code change (row 5 only) is a commit on the current upgrade branch with no version bump, tagged `aster-iroh-blobs-v<current version>-p<n>` after the consumer trial (as the `-p1` sweep tags were).

Validate in this repository:

```bash
rustfmt --edition 2021 --check <files the new commits touch>   # not cargo fmt --check: see below
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo check --lib          # catches the row 27 class of standalone feature gaps
```

`cargo fmt --check` does not pass on this line: row 23's GC code carries existing fmt drift. Check only the files a commit touches, and do not reformat upstream-only files or carried patch code during a port.

Then re-pin the iroh-docs fork's own `[patch.crates-io]` to the new iroh-blobs commit and run its full suite (`cargo test --all-features`, 118 tests including `tests/sync.rs`): iroh-docs exercises the whole stack, and must be run whenever iroh-blobs moves.

Gotcha when re-pinning this fork's own patch block: `cargo update -p iroh` alone kept crates.io iroh (1.0.2) and reported the patches as unused. Temporarily requiring `iroh = "1.3.0"` and running `cargo update -p <crate>` moved the transitive dependencies; restoring the original spec keeps that lock. In the 1.4.0 wave `cargo update -p iroh@1.1.0 --precise 1.4.0` did it directly. Update only the crates you name, and revert any unrelated lock churn.

Registry provenance: the Aster publisher's `publish-native-stack.py check` clones the fork at the pinned rev and, for a version the registry already serves, downloads the published `.crate` and diffs its `src` tree against the pin (`published_source_drift`); a mismatch fails the check. It therefore detects divergence between the pin and what the registry serves in `src`; `Cargo.toml` dependency metadata is rewritten during packaging by design and is not compared. To inspect other files by hand, extract the published `.crate` and diff it against the source. Registry versions are immutable; never yank or overwrite a published version.

Aster validation after updating pins, from the Aster repository root:

```bash
cargo fmt --check
cargo clippy --manifest-path bindings/python/rust/Cargo.toml -- -D warnings
cargo test -p aster_transport_core
cargo test -p aster --all-features
uv run maturin develop -m bindings/python/rust/Cargo.toml
uv run pytest tests/python/ -v --timeout=30
```

Tag only after the Aster consumer trial passes against these exact commits (tags cannot be moved):

```bash
git tag -a aster-iroh-blobs-v<published version> -m "Aster iroh-blobs v<published version>" <release commit>
git push origin aster-iroh-blobs-v<published version>
# re-pin only: git tag -a aster-iroh-blobs-v<current version>-p<n> -m "..." <re-pin commit>
```

Update `iroh-fork-manifest.toml` in the Aster repository (the only copy), and this ledger's Current release section on `main` (merge upstream `main` first).

Local checkouts: downstream workspaces with `[patch.crates-io]` path overrides into a local checkout (for example portal-sync) build whatever is checked out. After publishing, switch the local checkout to the released tag (for development, the upgrade branch, which can sit past the tag), and run at least one path-override consumer check (for example `cargo check --locked -p portal-cas` in portal-sync).

Before the current branch model, fork `main` mirrored upstream `main` and was reset to it on each port (`git reset --hard upstream/main`, `git push origin main --force-with-lease`). Under the current model `main` is upstream `main` plus one commit (README preamble and this ledger), refreshed with `git merge upstream/main`, and is never reset or force-pushed.

## History

### noq 1.4.2 re-pin (2026-10-22), current

| Upstream base | Tag | Pinned commit | Branch |
|---|---|---|---|
| `v0.103.1` | `aster-iroh-blobs-v0.103.12-p1` | `63ec9fc4afc9f098c932ebc6443266509e64f420` | `upgrade/iroh-blobs-v0.103.1` |

- **0.103.12-p1** (`63ec9fc4`): re-pin only (row 5), to iroh `91a92aa0` (1.4.0-p2), noq `a3c70b7a` (1.4.2, incremental BBRv3 packet history) and iroh-util `c9b91fce` (0.6.3-p4); the lock follows with no other moves. No source change; the crate stays 0.103.12 and publishes from the 0.103.12 code. Validation (`--locked --all-features`): lib 138 passed (3 ignored), blobs 3, tags 3, doc 17; clippy `--all-features --all-targets -D warnings` clean.

### 1.4.0 wave (2026-10-21)

| Upstream base | Tag | Pinned commit | Branch |
|---|---|---|---|
| `v0.103.1` | `aster-iroh-blobs-v0.103.12` | `5e1f4a6fe47c8c8728c9344e841ea841a0c4982c` | `upgrade/iroh-blobs-v0.103.1` |

- **0.103.12** (`5e1f4a6f`, tag `aster-iroh-blobs-v0.103.12`, 2026-10-22): row 26's stall clock stops while an attempt waits on local work (`97d86c31`, cherry-picked `-x` from `fix/stall-guard-local-work` `eae1aada`), on top of `6f6b4195`, the re-pin of row 5 to iroh `41ec78b1` / noq `13dcb5b7` (1.4.1) / iroh-util `8c7947b6`, which was never tagged on its own. The registry already served 0.103.11, so the version is 0.103.12.
- Validation (macOS, 2026-10-22, `--locked --all-features`): lib 138 passed (3 ignored) + 3 + 3 + doc 17; clippy `--all-features --all-targets -D warnings` and `--all-targets` clean; `downloader.rs` and `remote.rs` rustfmt clean, the same 6 pre-existing diffs elsewhere. Load proof: 50 consecutive full lib runs (two concurrent suite streams plus six busy-loop CPU burners) with the formerly flaky test passing every time; the `6f6b4195` binary failed it 5 of 18 single-stream runs and 2 of 16 runs under the same harness. The topic branch passes clippy and the 35 downloader/zstd tests on its own base. iroh-docs re-pinned to `5e1f4a6f`: 132 passed, 4 ignored.

- **0.103.11** (`1ec44e44`, tag `aster-iroh-blobs-v0.103.11`): a full port onto upstream `v0.103.1` (upstream delta from `v0.103.0`: #270, per-request-type modes in `EventMask`; logging of errors the provider and fs store had dropped; the release commit). `upgrade/iroh-blobs-v0.103.1` was cut from `v0.103.1` and every code row was cherry-picked with `-x` from `upgrade/iroh-blobs-v0.103` in the reapply order above (rows 5 and 34's requirement bump are new commits): row 5 `f4fb924b`, rows 6–7 `6174d23d` `c7c3e6c8`, row 38 `e698ced3`, row 23 `d3a95af3`..`268f23c5`, row 27 `4277ccab`, row 8 `031b0902`..`05683aec`, row 26 `032d6d98`, row 28 `f82dce8b` `36dda67d`, row 34 `15d47bdb` `e5797e59`, row 35 `7e10ccba`, release `1ec44e44`. Upstream absorbed no row; #283 (row 28) is still unreleased. The registry already served 0.103.1…0.103.10, so the version is 0.103.11.
- Stack: iroh `261ad623` (1.4.0), noq `535bd884` (1.4.0), iroh-util `a90ea88d` (0.6.3-p2). The lock had a second, crates.io iroh 1.1.0; `cargo update -p iroh@1.1.0 --precise 1.4.0` left one iroh and one noq in the graph.
- Validation (macOS, 2026-10-21): clippy clean; 136 passed (3 ignored) + 3 + 3 + 17. `cargo fmt --check` reports the same 6 pre-existing diffs in patch code as the 0.103.10 pin `bcd00bb2`.
- Flaky: `downloader_idle_timeout_fails_over_from_a_stalled_provider` (row 26) fails intermittently here and on the 0.103.10 pin alike (it passed in this run). Root cause fixed in 0.103.12.
- Row 38 (`gc_run_once`) backfilled: carried since rc.1, never in this ledger.

### 1.3.0 wave (2026-10-03 onward)

The iroh 1.3.0 wave had no upstream iroh-blobs release that forced a re-port; base stays `v0.103.0`. Releases in this wave, newest first:

| Version | Commit | Tag | Change |
|---|---|---|---|
| 0.103.10 | `bcd00bb2` | `aster-iroh-blobs-v0.103.10` | Row 35 (per-attempt transfer stats). Pin until 0.103.11 (2026-10-21). |
| 0.103.9 | `094bdd2f` | `aster-iroh-blobs-v0.103.9` | Row 34 (retire a silent or lost connection and retry once), plus the re-pin to iroh/noq 1.3.2 and iroh-util 0.6.3. |
| 0.103.8 | `4971183c` | `aster-iroh-blobs-v0.103.8` | Row 28 (zstd stream transport, with the port of upstream #283), 2026-10-15. Committed and tagged locally first; the Aster repository kept 0.103.7 until the branch, tag and registry crate were published. |
| 0.103.7 | `f04ca2cd` | `aster-iroh-blobs-v0.103.7` | Row 27 (fs-store tokio features), 2026-10-14. |
| 0.103.6 | `8fa6fea6` | `aster-iroh-blobs-v0.103.6` | Row 26 (provider stall fail-over), 2026-10-14. Never reached the registry: its package verification failed in the Aster 0.3.1014 promote job (row 27 fixes that). |
| 0.103.5-p1 | `712ce693` | `aster-iroh-blobs-v0.103.5-p1` | Intermediate re-pin on noq 1.3.0, superseded the same day. Do not pin it. |

- Same-day follow-up after noq 1.3.1: the fork's own patch block was re-pinned to iroh `1974e958` and noq `867a3a9a`. Lock and pin changes only; the crate version was unchanged and the publisher skipped it as identical. Validation: iroh-blobs 133 tests, clippy clean; `cargo fmt --check` reported existing drift in the GC patch code (row 23), which these commits do not touch.
- At that point the iroh-docs fork's own patch block still named iroh-blobs `b535aa30`; harmless (the Aster repository's root patch overrides it and the registry dependency is `0.103`). iroh-docs 0.101.5-p2 (2026-10-16) re-pinned it to `bcd00bb2`.

### 1.0.1 wave (2026-07-02)

| Upstream base | Tag | Pinned commit | Branch |
|---|---|---|---|
| `v0.103.0` | `aster-iroh-blobs-v0.103.5-p2` | `b535aa30ae61f82a1b048015817bf4365862ce62` | `upgrade/iroh-blobs-v0.103` |

- No upstream release; pins unchanged from the 1.0 wave until the additions below.
- **2026-08-13, 0.103.1** (tag `aster-iroh-blobs-v0.103.1`): three post-tag commits (row 8): downloader progress observability, its monotonicity follow-up, and a test/contract tightening. Canonical source branch `feat/multifetch`, based on `v0.103.0`. The registry's immutable `iroh-blobs 0.103.0` had been published mid-development and is byte-identical to `1f01a087` (verified by diffing the extracted `.crate` against each rev), so consumers already had the multifetch progress work. The bump to 0.103.1 is hygiene (the delta from that archive is a doc comment plus a test assertion, no behavioural code) and restores one version = one source state. Do not yank 0.103.0.
- **2026-08-13 sweep (`-p1` tags):** the fork's own `[patch.crates-io]`, which had named the GitHub mirror at the 1.0.0-wave iroh (`3c329f0c`) and noq (`ddb8829d`), was moved to Forgejo at this wave's revisions, in dependency order noq → iroh → iroh-blobs → iroh-docs / iroh-gossip. No `src` change, no version change, nothing republished.
- **2026-08-24..26, 0.103.2–0.103.5 (row 23), remediated 2026-08-27:** during the portal-sync cutover campaign the GC sweep-custody stack was committed directly on `upgrade/iroh-blobs-v0.103`, with no topic branch and no `-x` trailers. `fix/gc-sweep-custody` (`64b1631c3e`, all four fixes stacked on `v0.103.0`) was reconstructed afterwards; it compiled and passed its regression tests before push. Its trailers point at the upgrade-branch release commits, the opposite of the normal flow, because the releases existed first. Released tags and registry artifacts were not touched. The upgrade branch then sat past the 0.103.5 tag with post-release GC docs commits.
- Validation: per-fork fmt/clippy (`-D warnings`, `--all-features --tests`)/tests, then downstream `aster_transport_core` (242) and `aster --all-features` (94), PyO3 clippy clean, `maturin develop` and 1117 Python tests.
- Local checkout at the end of the cycle: `aster-iroh-blobs-v0.103.5` (released code); the upgrade branch for development.

### 1.0 wave (2026-06-21)

| Upstream base | Tag | Pinned commit | Branch |
|---|---|---|---|
| `v0.103.0` | `aster-iroh-blobs-v0.103.0` | `60d098c99d6a759b5b1bf0df635b149b50c74775` | `upgrade/iroh-blobs-v0.103` |

- Patch stack carried forward unchanged from rc.1; everything cherry-picked clean except a `Cargo.lock` regeneration. Tag == pinned commit == validated code.
- Validation: per-fork fmt/clippy/tests, then `aster_transport_core` (229) and `aster` (68) Rust tests and `maturin develop` + 1117 Python tests.

### rc.1 port (2026-06-07)

- Before the port Aster pinned `e20ff0292d` (`fix/iroh-blobs-batch-status`); upstream base `v0.102.0` (`099e7cfd`), target the same; stack range `v0.102.0..fix/iroh-blobs-batch-status`. Earlier upgrade branch name: `upgrade/iroh-blobs-v0.101`.
- Ported and published through `e20ff029`: fork `main` mirrored upstream `main` (`099e7cfd`); branch `upgrade/iroh-blobs-v0.102` pushed to `aster-rpc/iroh-blobs`; tag `aster-iroh-blobs-v0.102.0` marks the base rc1 port. The batch read and batch status fixes were also published on `fix/iroh-blobs-batch-small-reads` and `fix/iroh-blobs-batch-status` so later ports can cherry-pick them directly.

| Commit | Purpose |
|---:|---|
| `94964e8e` | Cherry-pick of `14a2286e`: Aster iroh/noq `[patch.crates-io]` entries, updated to explicit rc1 revs. |
| `39ad2947` | Ignore local `docs/aster/` internal notes. |
| `c23a66ac` | `Cargo.lock` resolves the Aster iroh/noq fork crates. |
| `a0cbdd15` | `Blobs::get_bytes_many_if_complete` (row 6). |
| `e20ff029` | `Blobs::status_many` (row 7). |

- Validation: `cargo fmt --check`; `cargo test -p iroh-blobs test_get_bytes_many_if_complete -- --nocapture`; `cargo test -p iroh-blobs test_status_many -- --nocapture`; `cargo test --all-features`; `cargo clippy --all-features --tests -- -D warnings`. Downstream, through the Aster repository and portal-cas: `cargo test -p aster_transport_core`, `cargo test -p aster namespace`, `cargo test -p portal-cas`, `cargo test -p portal-sync-session`, `cargo clippy -p aster_transport_core -p aster -- -D warnings`, `cargo clippy -p portal-cas -p portal-sync-session -- -D warnings`.
- Perf triage (2026-06-07) tested whether H2's remaining small-file wall clock came from per-blob fs-store fsync in iroh-blobs. A no-network `import_bao_bytes` probe disproved it for H2: 500 × 128-byte blobs imported in ~31 ms serial / ~11 ms concurrent, while 500 × 20 KB above-inline blobs took ~3.1 s serial / ~1.8 s concurrent. The H2 files (`fN\n`) are below the inline threshold, so no fs-store patch was added. The measured fix was downstream (Aster commit `e741e31`), reusing a node-level blob `Downloader` instead of constructing a downloader actor and connection pool per hash.
- The rc.1 command sequence was `git cherry-pick -x 14a2286e` onto `upgrade/iroh-blobs-v0.102` from `v0.102.0` (after reviewing `v0.101.0..v0.102.0`), then tag `aster-iroh-blobs-v0.102.0`. The Aster root patch block then pointed at `https://github.com/aster-rpc/iroh-blobs`; it now points at the Forgejo repository.
- Until replacement tags were cut, path-override consumers kept the local checkout on the `fix/iroh-blobs-batch-status` tip; the end-of-cycle checkout tag was `aster-iroh-blobs-v0.102.0`.
