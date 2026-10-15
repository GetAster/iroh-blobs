//! zstd stream transform for the blobs protocol.
//!
//! Each stream is a sequence of complete zstd frames. The sender ends the
//! current frame on every [`SendStream::sync`] (which the blobs protocol calls
//! once per message, right before it finishes the stream), so a receiver can
//! decode everything the sender wrote, and the stream end coincides with a
//! frame end. A stream that ends inside a frame is reported as
//! [`io::ErrorKind::InvalidData`], never as a clean end of stream, so a
//! truncated response cannot be mistaken for a short one.
//!
//! The compression level is local to the sender. The receiver needs no
//! configuration and accepts any frame whose window fits
//! [`MAX_WINDOW_LOG`].
use std::{
    io,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use iroh::endpoint::VarInt;
use zstd::stream::raw::{CParameter, DParameter, Decoder, Encoder, InBuffer, Operation, OutBuffer};

use super::{BoxedRecvStream, BoxedSendStream, StreamTransform};
use crate::util::{RecvStream, SendStream};

/// ALPN for the blobs protocol ([`crate::ALPN`]) over zstd-compressed streams.
///
/// Names the wrapped protocol version (`/iroh-bytes/4`) and the version of this
/// framing (`zstd/1`), so either can change without ambiguity.
pub const ZSTD_ALPN: &[u8] = b"/iroh-bytes/4+zstd/1";

/// Default compression level: zstd's own default, fast enough for interactive
/// reads while still removing most redundancy from text.
pub const DEFAULT_LEVEL: i32 = 3;

/// Highest accepted compression level. Levels above 19 ("ultra") need windows
/// larger than [`MAX_WINDOW_LOG`] allows.
pub const MAX_LEVEL: i32 = 19;

/// Largest frame window a receiver accepts: 2^23 = 8 MiB, which every level
/// up to [`MAX_LEVEL`] fits. Bounds the memory a peer can make a receiver
/// allocate per stream (zstd's own default bound is 128 MiB).
pub const MAX_WINDOW_LOG: u32 = 23;

/// Compressed output is handed to QUIC in chunks of about this size.
const SEND_CHUNK: usize = 64 * 1024;
/// Compressed input is read from QUIC in chunks of at most this size.
const RECV_CHUNK: usize = 64 * 1024;
/// Room reserved for one decode step. zstd's recommended output size.
const DECODE_OUT: usize = 128 * 1024;

/// Byte counters shared by every stream of one [`ZstdTransform`] (and its
/// clones).
#[derive(Debug, Default)]
pub struct ZstdStats {
    wire_bytes_written: AtomicU64,
    plain_bytes_written: AtomicU64,
    wire_bytes_read: AtomicU64,
    plain_bytes_read: AtomicU64,
    decode_errors: AtomicU64,
}

/// A snapshot of [`ZstdStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ZstdStatsSnapshot {
    /// Compressed bytes handed to QUIC.
    pub wire_bytes_written: u64,
    /// Uncompressed protocol bytes accepted from the blobs protocol.
    pub plain_bytes_written: u64,
    /// Compressed bytes read from QUIC.
    pub wire_bytes_read: u64,
    /// Uncompressed protocol bytes handed to the blobs protocol.
    pub plain_bytes_read: u64,
    /// Streams that failed because of malformed or truncated zstd data, as
    /// opposed to transport errors or failed content verification.
    pub decode_errors: u64,
}

impl ZstdStats {
    pub fn snapshot(&self) -> ZstdStatsSnapshot {
        ZstdStatsSnapshot {
            wire_bytes_written: self.wire_bytes_written.load(Ordering::Relaxed),
            plain_bytes_written: self.plain_bytes_written.load(Ordering::Relaxed),
            wire_bytes_read: self.wire_bytes_read.load(Ordering::Relaxed),
            plain_bytes_read: self.plain_bytes_read.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
        }
    }
}

/// zstd [`StreamTransform`]. Serve it on [`ZSTD_ALPN`].
#[derive(Debug, Clone)]
pub struct ZstdTransform {
    level: i32,
    stats: Arc<ZstdStats>,
}

impl Default for ZstdTransform {
    fn default() -> Self {
        Self::new(DEFAULT_LEVEL)
    }
}

impl ZstdTransform {
    /// A transform that compresses at `level`, clamped to `1..=MAX_LEVEL`.
    pub fn new(level: i32) -> Self {
        Self {
            level: level.clamp(1, MAX_LEVEL),
            stats: Default::default(),
        }
    }

    /// The compression level used for sending.
    pub fn level(&self) -> i32 {
        self.level
    }

    /// Counters for every stream this transform (or a clone) has wrapped.
    pub fn stats(&self) -> &Arc<ZstdStats> {
        &self.stats
    }
}

impl StreamTransform for ZstdTransform {
    fn send_stream(&self, stream: iroh::endpoint::SendStream) -> BoxedSendStream {
        BoxedSendStream::new(ZstdSendStream::new(stream, self.level, self.stats.clone()))
    }

    fn recv_stream(&self, stream: iroh::endpoint::RecvStream) -> BoxedRecvStream {
        BoxedRecvStream::new(ZstdRecvStream::new(stream, self.stats.clone()))
    }
}

fn zstd_error(e: io::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("zstd: {e}"))
}

/// Sending half: compresses into zstd frames, one frame per `sync`.
pub struct ZstdSendStream<W = iroh::endpoint::SendStream> {
    inner: W,
    encoder: Encoder<'static>,
    level: i32,
    out: Vec<u8>,
    /// Whether the current frame has input that `sync` has not ended yet.
    frame_open: bool,
    stats: Arc<ZstdStats>,
}

impl<W: SendStream> ZstdSendStream<W> {
    pub fn new(inner: W, level: i32, stats: Arc<ZstdStats>) -> Self {
        Self {
            inner,
            encoder: Self::encoder(level),
            level,
            out: Vec::new(),
            frame_open: false,
            stats,
        }
    }

    fn encoder(level: i32) -> Encoder<'static> {
        // A failure here is an allocation failure in zstd; nothing sensible
        // can continue.
        let mut encoder = Encoder::new(level).expect("zstd: allocate compression context");
        // Frames carry a checksum of their content, so corruption of the
        // compressed stream is reported as a zstd error rather than surfacing
        // later as a content verification failure.
        encoder
            .set_parameter(CParameter::ChecksumFlag(true))
            .expect("zstd: enable frame checksum");
        encoder
    }

    async fn flush_out(&mut self, min: usize) -> io::Result<()> {
        if self.out.len() >= min.max(1) {
            let chunk = Bytes::from(std::mem::take(&mut self.out));
            self.stats
                .wire_bytes_written
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            self.inner.send_bytes(chunk).await?;
        }
        Ok(())
    }

    async fn compress(&mut self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.frame_open = true;
        self.stats
            .plain_bytes_written
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        let mut input = InBuffer::around(buf);
        while input.pos() < buf.len() {
            self.out.reserve(SEND_CHUNK);
            let pos = self.out.len();
            let mut output = OutBuffer::around_pos(&mut self.out, pos);
            self.encoder.run(&mut input, &mut output)?;
            self.flush_out(SEND_CHUNK).await?;
        }
        Ok(())
    }

    /// End the current frame and hand all of it to QUIC.
    async fn end_frame(&mut self) -> io::Result<()> {
        if !self.frame_open {
            return Ok(());
        }
        loop {
            self.out.reserve(SEND_CHUNK);
            let pos = self.out.len();
            let mut output = OutBuffer::around_pos(&mut self.out, pos);
            let remaining = self.encoder.finish(&mut output, true)?;
            if remaining == 0 {
                break;
            }
            self.flush_out(SEND_CHUNK).await?;
        }
        self.flush_out(1).await?;
        self.encoder.reinit()?;
        self.frame_open = false;
        Ok(())
    }
}

impl<W: SendStream> SendStream for ZstdSendStream<W> {
    async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
        self.compress(&bytes).await
    }

    async fn send(&mut self, buf: &[u8]) -> io::Result<()> {
        self.compress(buf).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        self.end_frame().await?;
        self.inner.sync().await
    }

    fn reset(&mut self, code: VarInt) -> io::Result<()> {
        self.inner.reset(code)
    }

    async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
        self.inner.stopped().await
    }

    fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl<W> std::fmt::Debug for ZstdSendStream<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdSendStream")
            .field("level", &self.level)
            .field("frame_open", &self.frame_open)
            .finish_non_exhaustive()
    }
}

/// Receiving half: decodes a sequence of zstd frames.
pub struct ZstdRecvStream<R = iroh::endpoint::RecvStream> {
    inner: R,
    decoder: Decoder<'static>,
    /// Compressed input not yet consumed by the decoder.
    input: Bytes,
    /// Decoded bytes; those before `start` have been handed out.
    output: Vec<u8>,
    start: usize,
    /// The decoder is inside a frame that has not been completed.
    in_frame: bool,
    /// The inner stream has ended.
    eof: bool,
    /// A decode error has been reported; later reads repeat it.
    failed: bool,
    stats: Arc<ZstdStats>,
}

impl<R: RecvStream> ZstdRecvStream<R> {
    pub fn new(inner: R, stats: Arc<ZstdStats>) -> Self {
        let mut decoder = Decoder::new().expect("zstd: allocate decompression context");
        decoder
            .set_parameter(DParameter::WindowLogMax(MAX_WINDOW_LOG))
            .expect("zstd: set window limit");
        Self {
            inner,
            decoder,
            input: Bytes::new(),
            output: Vec::new(),
            start: 0,
            in_frame: false,
            eof: false,
            failed: false,
            stats,
        }
    }

    fn fail(&mut self, e: io::Error) -> io::Error {
        if !self.failed {
            self.failed = true;
            self.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
        }
        e
    }

    /// Decode more output. Returns `Ok(false)` at a clean end of stream, which
    /// requires the stream to end on a frame boundary.
    async fn fill(&mut self) -> io::Result<bool> {
        if self.failed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zstd: stream already failed",
            ));
        }
        loop {
            if self.input.is_empty() {
                if self.eof {
                    if self.in_frame {
                        return Err(self.fail(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "zstd: stream ended inside a frame",
                        )));
                    }
                    return Ok(false);
                }
                let chunk = self.inner.recv_bytes(RECV_CHUNK).await?;
                if chunk.is_empty() {
                    self.eof = true;
                } else {
                    self.stats
                        .wire_bytes_read
                        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    self.input = chunk;
                }
                continue;
            }
            if self.start == self.output.len() {
                self.output.clear();
                self.start = 0;
            }
            self.output.reserve(DECODE_OUT);
            let pos = self.output.len();
            let mut input = InBuffer::around(&self.input[..]);
            let mut output = OutBuffer::around_pos(&mut self.output, pos);
            let res = self.decoder.run(&mut input, &mut output);
            let produced = output.pos() - pos;
            let consumed = input.pos();
            let hint = match res {
                Ok(hint) => hint,
                Err(e) => return Err(self.fail(zstd_error(e))),
            };
            self.input = self.input.slice(consumed..);
            if consumed > 0 || produced > 0 {
                // `hint == 0` means the frame was completed and fully flushed.
                self.in_frame = hint != 0;
            }
            if produced > 0 {
                self.stats
                    .plain_bytes_read
                    .fetch_add(produced as u64, Ordering::Relaxed);
                return Ok(true);
            }
        }
    }

    fn buffered(&self) -> usize {
        self.output.len() - self.start
    }

    fn take(&mut self, n: usize) -> Bytes {
        let bytes = Bytes::copy_from_slice(&self.output[self.start..self.start + n]);
        self.start += n;
        bytes
    }

    /// Fill until `len` bytes are buffered or the stream ends cleanly.
    async fn fill_to(&mut self, len: usize) -> io::Result<()> {
        while self.buffered() < len {
            if !self.fill().await? {
                break;
            }
        }
        Ok(())
    }
}

impl<R: RecvStream> RecvStream for ZstdRecvStream<R> {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        self.fill_to(len).await?;
        let n = len.min(self.buffered());
        Ok(self.take(n))
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        self.fill_to(len).await?;
        if self.buffered() < len {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(self.take(len))
    }

    async fn recv_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let bytes = self.recv_bytes_exact(buf.len()).await?;
        buf.copy_from_slice(&bytes);
        Ok(())
    }

    fn stop(&mut self, code: VarInt) -> io::Result<()> {
        self.inner.stop(code)
    }

    fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl<R> std::fmt::Debug for ZstdRecvStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdRecvStream")
            .field("in_frame", &self.in_frame)
            .field("eof", &self.eof)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc, time::Duration};

    use bao_tree::{ChunkNum, ChunkRanges};
    use bytes::Bytes;
    use iroh::{
        address_lookup::MemoryLookup, endpoint::presets, protocol::Router, Endpoint, EndpointId,
        RelayMode,
    };
    use n0_future::StreamExt;
    use testresult::TestResult;

    use super::*;
    use crate::{
        api::downloader::{DownloadOptions, DownloadProgressItem, Downloader, SplitStrategy},
        protocol::GetRequest,
        store::mem::MemStore,
        transport::{StreamTransform, TransformedBlobsProtocol},
        BlobsProtocol, HashAndFormat,
    };

    /// Text-heavy content: this crate's own sources, each included once.
    fn text_blob() -> Vec<u8> {
        [
            include_str!("../api/downloader.rs"),
            include_str!("../api/remote.rs"),
            include_str!("../provider.rs"),
            include_str!("../get.rs"),
            include_str!("../util/stream.rs"),
            include_str!("../store/fs.rs"),
        ]
        .concat()
        .into_bytes()
    }

    // ---- codec ----

    /// A send stream that collects what it is given.
    #[derive(Default)]
    struct Collect(Vec<u8>);

    impl SendStream for Collect {
        async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
            self.0.extend_from_slice(&bytes);
            Ok(())
        }
        async fn send(&mut self, buf: &[u8]) -> io::Result<()> {
            self.0.extend_from_slice(buf);
            Ok(())
        }
        async fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn reset(&mut self, _code: VarInt) -> io::Result<()> {
            Ok(())
        }
        async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
            Ok(None)
        }
        fn id(&self) -> u64 {
            0
        }
    }

    async fn compress(parts: &[&[&[u8]]]) -> io::Result<Vec<u8>> {
        let mut send = ZstdSendStream::new(Collect::default(), DEFAULT_LEVEL, Default::default());
        for message in parts {
            for part in *message {
                send.send(part).await?;
            }
            send.sync().await?;
        }
        Ok(send.inner.0)
    }

    async fn decompress_all(wire: Vec<u8>) -> (io::Result<Vec<u8>>, Arc<ZstdStats>) {
        let stats = Arc::new(ZstdStats::default());
        let mut recv = ZstdRecvStream::new(Bytes::from(wire), stats.clone());
        let mut out = Vec::new();
        let res = loop {
            match recv.recv_bytes(1000).await {
                Ok(b) if b.is_empty() => break Ok(out),
                Ok(b) => out.extend_from_slice(&b),
                Err(e) => break Err(e),
            }
        };
        (res, stats)
    }

    #[tokio::test]
    async fn every_sync_ends_a_decodable_frame() -> TestResult<()> {
        let text = text_blob();
        let (a, b) = text.split_at(text.len() / 2);
        let wire = compress(&[&[b"request"], &[a, b"", b]]).await?;
        assert!(
            wire.len() < text.len() / 2,
            "{} >= {}",
            wire.len(),
            text.len()
        );
        let (out, stats) = decompress_all(wire.clone()).await;
        let mut expected = b"request".to_vec();
        expected.extend_from_slice(&text);
        assert_eq!(out?, expected);
        let s = stats.snapshot();
        assert_eq!(s.wire_bytes_read, wire.len() as u64);
        assert_eq!(s.plain_bytes_read, expected.len() as u64);
        assert_eq!(s.decode_errors, 0);
        Ok(())
    }

    #[tokio::test]
    async fn sync_without_data_writes_nothing() -> TestResult<()> {
        assert!(compress(&[&[], &[b""]]).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_ending_inside_a_frame_is_invalid_data() -> TestResult<()> {
        let wire = compress(&[&[&text_blob()]]).await?;
        for cut in [1, 4, wire.len() / 2] {
            let (res, stats) = decompress_all(wire[..wire.len() - cut].to_vec()).await;
            let err = res.expect_err("truncated stream decoded");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "cut {cut}: {err}");
            assert_eq!(stats.snapshot().decode_errors, 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn an_unsynced_stream_is_invalid_data() -> TestResult<()> {
        // What a receiver sees if a sender's stream is finished without the
        // final `sync`: whatever zstd had flushed, but no frame end.
        // Incompressible, so zstd emits blocks before the frame is ended.
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let noise: Vec<u8> = (0..512 * 1024)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let mut send = ZstdSendStream::new(Collect::default(), DEFAULT_LEVEL, Default::default());
        send.send(&noise).await?;
        let wire = send.inner.0;
        assert!(!wire.is_empty());
        let (res, _) = decompress_all(wire).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
        Ok(())
    }

    #[tokio::test]
    async fn garbage_is_invalid_data() -> TestResult<()> {
        let (res, stats) = decompress_all(b"definitely not a zstd frame".to_vec()).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(stats.snapshot().decode_errors, 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_frame_window_above_the_limit_is_refused() -> TestResult<()> {
        let mut enc = Encoder::new(3)?;
        enc.set_parameter(CParameter::WindowLog(MAX_WINDOW_LOG + 1))?;
        let mut wire = Vec::with_capacity(1 << 16);
        let mut input = InBuffer::around(b"small payload, large declared window");
        let mut output = OutBuffer::around(&mut wire);
        enc.run(&mut input, &mut output)?;
        while enc.finish(&mut output, true)? != 0 {}
        let (res, _) = decompress_all(wire).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
        Ok(())
    }

    /// Codec throughput on text, one independent frame per message as on the
    /// wire. Run with `cargo test --release --features zstd -- --ignored
    /// --nocapture codec_throughput`.
    #[tokio::test]
    #[ignore = "measurement, not a check"]
    async fn codec_throughput() -> TestResult<()> {
        let text = text_blob();
        const ROUNDS: usize = 64;
        for level in [1, DEFAULT_LEVEL, 5] {
            let started = std::time::Instant::now();
            let mut send = ZstdSendStream::new(Collect::default(), level, Default::default());
            for _ in 0..ROUNDS {
                send.send(&text).await?;
                send.sync().await?;
            }
            let compress = started.elapsed();
            let wire = send.inner.0;
            let started = std::time::Instant::now();
            let (out, _) = decompress_all(wire.clone()).await;
            let decompress = started.elapsed();
            assert_eq!(out?.len(), text.len() * ROUNDS);
            let total = (text.len() * ROUNDS) as f64;
            println!(
                "level {level}: {} B -> {} B per message, ratio {:.2}x, compress {:.0} MB/s, decompress {:.0} MB/s",
                text.len(),
                wire.len() / ROUNDS,
                total / wire.len() as f64,
                total / compress.as_secs_f64() / 1e6,
                total / decompress.as_secs_f64() / 1e6,
            );
        }
        Ok(())
    }

    // ---- downloader over zstd ----

    struct Node {
        router: Router,
        store: MemStore,
        lookup: MemoryLookup,
    }

    impl Node {
        fn id(&self) -> EndpointId {
            self.router.endpoint().id()
        }
        fn knows(&self, other: &Node) {
            self.lookup
                .add_endpoint_info(other.router.endpoint().addr());
        }
        fn knows_router(&self, other: &Router) {
            self.lookup.add_endpoint_info(other.endpoint().addr());
        }
    }

    async fn endpoint(lookup: &MemoryLookup) -> TestResult<Endpoint> {
        Ok(Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .address_lookup(lookup.clone())
            .bind()
            .await?)
    }

    /// A node that serves plain blobs, plus the given transform on [`ZSTD_ALPN`].
    async fn node(transform: Option<Arc<dyn StreamTransform>>) -> TestResult<Node> {
        let store = MemStore::new();
        let lookup = MemoryLookup::new();
        let mut builder = Router::builder(endpoint(&lookup).await?)
            .accept(crate::ALPN, BlobsProtocol::new(&store, None));
        if let Some(transform) = transform {
            builder = builder.accept(
                ZSTD_ALPN,
                TransformedBlobsProtocol::new(&store, None, transform),
            );
        }
        Ok(Node {
            router: builder.spawn(),
            store,
            lookup,
        })
    }

    async fn zstd_node() -> TestResult<(Node, ZstdTransform)> {
        let t = ZstdTransform::default();
        Ok((node(Some(Arc::new(t.clone()))).await?, t))
    }

    fn zstd_downloader(node: &Node, t: &ZstdTransform, fallback: bool) -> Downloader {
        let builder = Downloader::builder(&node.store, node.router.endpoint())
            .alpn(ZSTD_ALPN)
            .stream_transform(Arc::new(t.clone()));
        if fallback {
            builder.plain_fallback(Duration::from_secs(60)).build()
        } else {
            builder.build()
        }
    }

    #[derive(Default, Debug)]
    struct Reported {
        tried: Vec<EndpointId>,
        failed: Vec<EndpointId>,
        completed: usize,
        bytes: u64,
        errors: usize,
        fallbacks: Vec<(EndpointId, bool)>,
    }

    async fn fetch(
        d: &Downloader,
        request: impl crate::api::downloader::SupportedRequest,
        providers: Vec<EndpointId>,
        idle: Option<Duration>,
    ) -> TestResult<Reported> {
        let mut opts = DownloadOptions::new(request, providers, SplitStrategy::None);
        if let Some(idle) = idle {
            opts = opts.with_provider_idle_timeout(idle);
        }
        let mut stream = d.download_with_opts(opts).stream().await?;
        let mut out = Reported::default();
        let collect = async {
            while let Some(item) = stream.next().await {
                match item {
                    DownloadProgressItem::TryProvider { id, .. } => out.tried.push(id),
                    DownloadProgressItem::ProviderFailed { id, .. } => out.failed.push(id),
                    DownloadProgressItem::PartComplete { .. } => out.completed += 1,
                    DownloadProgressItem::BytesTransferred(n) => out.bytes += n,
                    DownloadProgressItem::Error(_) | DownloadProgressItem::DownloadError => {
                        out.errors += 1
                    }
                    DownloadProgressItem::Progress(_) => {}
                    DownloadProgressItem::PlainFallback { id, cached } => {
                        out.fallbacks.push((id, cached))
                    }
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(30), collect).await?;
        Ok(out)
    }

    /// A zstd fetch stores the same bytes under the same hash, moves fewer bytes
    /// on the wire than it decodes, and reports payload bytes as a plain fetch.
    #[tokio::test]
    async fn zstd_round_trip_stores_the_same_content() -> TestResult<()> {
        let (holder, served) = zstd_node().await?;
        let consumer = node(None).await?;
        consumer.knows(&holder);
        let text = text_blob();
        let tt = holder.store.add_slice(&text).await?;
        let client = ZstdTransform::default();
        let d = zstd_downloader(&consumer, &client, false);

        let started = std::time::Instant::now();
        let r = fetch(&d, tt.hash, vec![holder.id()], None).await?;
        let elapsed = started.elapsed();

        assert_eq!(r.errors, 0, "{r:?}");
        assert_eq!(r.tried, vec![holder.id()]);
        assert!(r.failed.is_empty() && r.fallbacks.is_empty(), "{r:?}");
        assert_eq!(r.bytes, text.len() as u64);
        assert_eq!(
            consumer.store.get_bytes(tt.hash).await?.deref(),
            text.as_slice()
        );
        let rx = client.stats().snapshot();
        let tx = served.stats().snapshot();
        assert!(rx.wire_bytes_read > 0 && rx.wire_bytes_read == tx.wire_bytes_written);
        assert_eq!(rx.plain_bytes_read, tx.plain_bytes_written);
        assert!(rx.wire_bytes_read * 2 < rx.plain_bytes_read, "{rx:?}");
        println!(
            "zstd level {}: payload {} B, protocol {} B, wire {} B, ratio {:.2}x ({:.1}% saved), {:?}",
            served.level(),
            text.len(),
            rx.plain_bytes_read,
            rx.wire_bytes_read,
            rx.plain_bytes_read as f64 / rx.wire_bytes_read as f64,
            100.0 * (1.0 - rx.wire_bytes_read as f64 / rx.plain_bytes_read as f64),
            elapsed,
        );
        Ok(())
    }

    /// A partially stored blob resumes over zstd and fetches only what is missing.
    #[tokio::test]
    async fn zstd_resumes_a_partial_blob() -> TestResult<()> {
        let (holder, _) = zstd_node().await?;
        let consumer = node(None).await?;
        consumer.knows(&holder);
        let text = text_blob();
        let tt = holder.store.add_slice(&text).await?;
        let conn = consumer
            .router
            .endpoint()
            .connect(holder.router.endpoint().addr(), crate::ALPN)
            .await?;
        consumer
            .store
            .remote()
            .execute_get(
                conn,
                GetRequest::builder()
                    .root(ChunkRanges::from(..ChunkNum(16)))
                    .build(tt.hash),
            )
            .await?;
        let held = consumer
            .store
            .remote()
            .local(HashAndFormat::raw(tt.hash))
            .await?
            .local_bytes();
        assert!(held > 0 && held < text.len() as u64);

        let client = ZstdTransform::default();
        let r = fetch(
            &zstd_downloader(&consumer, &client, false),
            tt.hash,
            vec![holder.id()],
            None,
        )
        .await?;
        assert_eq!(r.errors, 0, "{r:?}");
        assert_eq!(r.bytes, text.len() as u64 - held);
        assert_eq!(
            consumer.store.get_bytes(tt.hash).await?.deref(),
            text.as_slice()
        );
        Ok(())
    }

    /// Ordered failover keeps provider attribution over zstd.
    #[tokio::test]
    async fn zstd_ordered_failover_keeps_attribution() -> TestResult<()> {
        let (empty, _) = zstd_node().await?;
        let (holder, _) = zstd_node().await?;
        let consumer = node(None).await?;
        consumer.knows(&empty);
        consumer.knows(&holder);
        let tt = holder.store.add_slice(text_blob()).await?;
        let client = ZstdTransform::default();
        let r = fetch(
            &zstd_downloader(&consumer, &client, false),
            tt.hash,
            vec![empty.id(), holder.id()],
            None,
        )
        .await?;
        assert_eq!(r.errors, 0, "{r:?}");
        assert_eq!(r.tried, vec![empty.id(), holder.id()]);
        assert_eq!(r.failed, vec![empty.id()]);
        assert_eq!(r.completed, 1);
        assert!(client.stats().snapshot().wire_bytes_read > 0);
        Ok(())
    }

    /// A provider without the zstd ALPN is fetched from over the plain ALPN,
    /// the fallback is reported, and the refusal is remembered.
    #[tokio::test]
    async fn zstd_falls_back_to_a_plain_provider() -> TestResult<()> {
        let plain = node(None).await?;
        let consumer = node(None).await?;
        consumer.knows(&plain);
        let a = plain.store.add_slice(b"first blob").await?;
        let b = plain.store.add_slice(b"second blob").await?;
        let client = ZstdTransform::default();
        let d = zstd_downloader(&consumer, &client, true);

        let r = fetch(&d, a.hash, vec![plain.id()], None).await?;
        assert_eq!(r.errors, 0, "{r:?}");
        assert_eq!(r.fallbacks, vec![(plain.id(), false)]);
        assert!(r.failed.is_empty());
        assert_eq!(
            consumer.store.get_bytes(a.hash).await?.deref(),
            b"first blob"
        );

        let r = fetch(&d, b.hash, vec![plain.id()], None).await?;
        assert_eq!(r.errors, 0, "{r:?}");
        assert_eq!(r.fallbacks, vec![(plain.id(), true)]);
        assert_eq!(client.stats().snapshot(), ZstdStatsSnapshot::default());
        Ok(())
    }

    /// Without the fallback, a provider that refuses the zstd ALPN is a failed
    /// provider.
    #[tokio::test]
    async fn zstd_without_fallback_fails_a_plain_provider() -> TestResult<()> {
        let plain = node(None).await?;
        let consumer = node(None).await?;
        consumer.knows(&plain);
        let a = plain.store.add_slice(b"blob").await?;
        let client = ZstdTransform::default();
        let r = fetch(
            &zstd_downloader(&consumer, &client, false),
            a.hash,
            vec![plain.id()],
            None,
        )
        .await?;
        assert_eq!(r.failed, vec![plain.id()]);
        assert!(r.fallbacks.is_empty());
        assert!(r.errors > 0);
        assert!(!consumer.store.has(a.hash).await?);
        Ok(())
    }

    /// Content already stored locally is not fetched, and no provider is dialed.
    #[tokio::test]
    async fn zstd_local_content_short_circuits() -> TestResult<()> {
        let consumer = node(None).await?;
        let tt = consumer.store.add_slice(text_blob()).await?;
        let nobody = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let client = ZstdTransform::default();
        let r = fetch(
            &zstd_downloader(&consumer, &client, true),
            tt.hash,
            vec![nobody],
            None,
        )
        .await?;
        assert_eq!(r.errors, 0, "{r:?}");
        assert!(r.tried.is_empty() && r.fallbacks.is_empty(), "{r:?}");
        Ok(())
    }

    /// Accepts a stream on the zstd ALPN and never answers.
    #[derive(Debug, Clone)]
    struct Stall;

    impl iroh::protocol::ProtocolHandler for Stall {
        async fn accept(
            &self,
            conn: iroh::endpoint::Connection,
        ) -> std::result::Result<(), iroh::protocol::AcceptError> {
            if let Ok(_streams) = conn.accept_bi().await {
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }

    /// The stall guard fails over from a stalled zstd provider.
    #[tokio::test]
    async fn zstd_stall_fails_over() -> TestResult<()> {
        let (holder, _) = zstd_node().await?;
        let lookup = MemoryLookup::new();
        let stall = Router::builder(endpoint(&lookup).await?)
            .accept(ZSTD_ALPN, Stall)
            .spawn();
        let consumer = node(None).await?;
        consumer.knows(&holder);
        consumer.knows_router(&stall);
        let tt = holder.store.add_slice(text_blob()).await?;
        let client = ZstdTransform::default();
        let r = fetch(
            &zstd_downloader(&consumer, &client, true),
            tt.hash,
            vec![stall.endpoint().id(), holder.id()],
            Some(Duration::from_millis(500)),
        )
        .await?;
        assert_eq!(r.errors, 0, "{r:?}");
        assert_eq!(r.failed, vec![stall.endpoint().id()]);
        assert_eq!(r.tried, vec![stall.endpoint().id(), holder.id()]);
        assert!(r.fallbacks.is_empty());
        assert_eq!(
            consumer.store.get_bytes(tt.hash).await?.len(),
            text_blob().len()
        );
        Ok(())
    }

    /// Answers every request with bytes that are not zstd.
    #[derive(Debug, Clone)]
    struct Garbage;

    impl iroh::protocol::ProtocolHandler for Garbage {
        async fn accept(
            &self,
            conn: iroh::endpoint::Connection,
        ) -> std::result::Result<(), iroh::protocol::AcceptError> {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                recv.read_to_end(4096).await.ok();
                send.write_all(&[0x5a; 4096]).await.ok();
                send.finish().ok();
            }
            Ok(())
        }
    }

    /// Passes only the first `budget` bytes of a stream, then drops the rest.
    struct Truncate {
        inner: iroh::endpoint::SendStream,
        budget: usize,
    }

    impl SendStream for Truncate {
        async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
            let n = bytes.len().min(self.budget);
            self.budget -= n;
            if n > 0 {
                self.inner.send_bytes(bytes.slice(..n)).await?;
            }
            Ok(())
        }
        async fn send(&mut self, buf: &[u8]) -> io::Result<()> {
            self.send_bytes(Bytes::copy_from_slice(buf)).await
        }
        async fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn reset(&mut self, code: VarInt) -> io::Result<()> {
            SendStream::reset(&mut self.inner, code)
        }
        async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
            SendStream::stopped(&mut self.inner).await
        }
        fn id(&self) -> u64 {
            SendStream::id(&self.inner)
        }
    }

    /// Serves real zstd responses cut off after `budget` wire bytes.
    #[derive(Debug)]
    struct Truncating {
        budget: usize,
    }

    impl StreamTransform for Truncating {
        fn send_stream(&self, stream: iroh::endpoint::SendStream) -> BoxedSendStream {
            BoxedSendStream::new(ZstdSendStream::new(
                Truncate {
                    inner: stream,
                    budget: self.budget,
                },
                DEFAULT_LEVEL,
                Default::default(),
            ))
        }
        fn recv_stream(&self, stream: iroh::endpoint::RecvStream) -> BoxedRecvStream {
            BoxedRecvStream::new(ZstdRecvStream::new(stream, Default::default()))
        }
    }

    /// Malformed or truncated zstd never completes a blob; the next provider
    /// resumes from what verified, and the result is the original content.
    #[tokio::test]
    async fn zstd_bad_streams_fail_over_without_completing() -> TestResult<()> {
        let (holder, _) = zstd_node().await?;
        let text = text_blob();
        let tt = holder.store.add_slice(&text).await?;

        let lookup = MemoryLookup::new();
        let garbage = Router::builder(endpoint(&lookup).await?)
            .accept(ZSTD_ALPN, Garbage)
            .spawn();
        let truncating_store = MemStore::new();
        truncating_store.add_slice(&text).await?;
        let truncating = Router::builder(endpoint(&lookup).await?)
            .accept(
                ZSTD_ALPN,
                TransformedBlobsProtocol::new(
                    &truncating_store,
                    None,
                    Arc::new(Truncating { budget: 20_000 }),
                ),
            )
            .spawn();
        let consumer = node(None).await?;
        consumer.knows(&holder);
        consumer.knows_router(&garbage);
        consumer.knows_router(&truncating);
        let client = ZstdTransform::default();
        let d = zstd_downloader(&consumer, &client, false);

        // Bad providers alone: the fetch fails and the blob is not complete.
        let bad = vec![garbage.endpoint().id(), truncating.endpoint().id()];
        let r = fetch(&d, tt.hash, bad.clone(), None).await?;
        assert_eq!(r.failed, bad, "{r:?}");
        assert!(r.errors > 0);
        assert!(!consumer.store.has(tt.hash).await?);
        assert_eq!(client.stats().snapshot().decode_errors, 2);
        let held = consumer
            .store
            .remote()
            .local(HashAndFormat::raw(tt.hash))
            .await?
            .local_bytes();
        assert!(held < text.len() as u64);

        // With a good provider after them, it resumes and completes.
        let mut providers = bad.clone();
        providers.push(holder.id());
        let r = fetch(&d, tt.hash, providers, None).await?;
        assert_eq!(r.errors, 0, "{r:?}");
        assert_eq!(r.failed, bad);
        assert_eq!(
            consumer.store.get_bytes(tt.hash).await?.deref(),
            text.as_slice()
        );
        Ok(())
    }
}
