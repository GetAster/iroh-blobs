//! Stream transforms: run the ordinary blobs protocol over transformed streams.
//!
//! A [`StreamTransform`] wraps both halves of every QUIC stream of a
//! connection, for example to compress them. It is a transport concern only:
//! after the transform, peers speak the unmodified blobs protocol, write the
//! same verified content into the same store, and use the same hashes.
//!
//! A transformed protocol must be served on its own ALPN, so a peer can tell
//! the plain protocol from the transformed one before the first byte:
//!
//! - providers register a [`TransformedBlobsProtocol`] on the derived ALPN,
//!   next to the plain [`crate::BlobsProtocol`] on [`crate::ALPN`];
//! - the [`Downloader`](crate::api::downloader::Downloader) dials the derived
//!   ALPN when built with
//!   [`DownloaderBuilder::stream_transform`](crate::api::downloader::DownloaderBuilder::stream_transform),
//!   and can fall back to the plain ALPN for providers that refuse it;
//! - one-off fetches wrap a connection in a [`TransformedConnection`] and pass
//!   it to [`Remote::fetch`](crate::api::remote::Remote::fetch).
//!
//! The `zstd` feature provides [`zstd::ZstdTransform`].
use std::{
    fmt::Debug,
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use iroh::{
    endpoint::{Connection, VarInt},
    protocol::{AcceptError, ProtocolHandler},
};

use crate::{
    api::{remote::GetStreamPair, Store},
    get::StreamPair,
    provider::events::EventSender,
    util::{RecvStream, SendStream},
};

#[cfg(feature = "zstd")]
pub mod zstd;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Object-safe form of [`SendStream`], used by [`BoxedSendStream`].
///
/// Implemented for every `Sync` [`SendStream`]; implement [`SendStream`] instead.
pub trait DynSendStream: Send + Sync {
    fn dyn_send_bytes(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>>;
    fn dyn_send<'a>(&'a mut self, buf: &'a [u8]) -> BoxFuture<'a, io::Result<()>>;
    fn dyn_sync(&mut self) -> BoxFuture<'_, io::Result<()>>;
    fn dyn_reset(&mut self, code: VarInt) -> io::Result<()>;
    fn dyn_stopped(&mut self) -> BoxFuture<'_, io::Result<Option<VarInt>>>;
    fn dyn_id(&self) -> u64;
}

impl<T: SendStream + Sync> DynSendStream for T {
    fn dyn_send_bytes(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(self.send_bytes(bytes))
    }
    fn dyn_send<'a>(&'a mut self, buf: &'a [u8]) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(self.send(buf))
    }
    fn dyn_sync(&mut self) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(self.sync())
    }
    fn dyn_reset(&mut self, code: VarInt) -> io::Result<()> {
        self.reset(code)
    }
    fn dyn_stopped(&mut self) -> BoxFuture<'_, io::Result<Option<VarInt>>> {
        Box::pin(self.stopped())
    }
    fn dyn_id(&self) -> u64 {
        self.id()
    }
}

/// Object-safe form of [`RecvStream`], used by [`BoxedRecvStream`].
///
/// Implemented for every `Sync` [`RecvStream`]; implement [`RecvStream`] instead.
pub trait DynRecvStream: Send + Sync {
    fn dyn_recv_bytes(&mut self, len: usize) -> BoxFuture<'_, io::Result<Bytes>>;
    fn dyn_recv_bytes_exact(&mut self, len: usize) -> BoxFuture<'_, io::Result<Bytes>>;
    fn dyn_recv_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxFuture<'a, io::Result<()>>;
    fn dyn_stop(&mut self, code: VarInt) -> io::Result<()>;
    fn dyn_id(&self) -> u64;
}

impl<T: RecvStream + Sync> DynRecvStream for T {
    fn dyn_recv_bytes(&mut self, len: usize) -> BoxFuture<'_, io::Result<Bytes>> {
        Box::pin(self.recv_bytes(len))
    }
    fn dyn_recv_bytes_exact(&mut self, len: usize) -> BoxFuture<'_, io::Result<Bytes>> {
        Box::pin(self.recv_bytes_exact(len))
    }
    fn dyn_recv_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(self.recv_exact(buf))
    }
    fn dyn_stop(&mut self, code: VarInt) -> io::Result<()> {
        self.stop(code)
    }
    fn dyn_id(&self) -> u64 {
        self.id()
    }
}

/// A type-erased [`SendStream`], as produced by a [`StreamTransform`].
pub struct BoxedSendStream(Box<dyn DynSendStream>);

impl BoxedSendStream {
    pub fn new(inner: impl SendStream + Sync + 'static) -> Self {
        Self(Box::new(inner))
    }
}

impl Debug for BoxedSendStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoxedSendStream")
            .field(&self.0.dyn_id())
            .finish()
    }
}

impl SendStream for BoxedSendStream {
    async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
        self.0.dyn_send_bytes(bytes).await
    }
    async fn send(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.dyn_send(buf).await
    }
    async fn sync(&mut self) -> io::Result<()> {
        self.0.dyn_sync().await
    }
    fn reset(&mut self, code: VarInt) -> io::Result<()> {
        self.0.dyn_reset(code)
    }
    async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
        self.0.dyn_stopped().await
    }
    fn id(&self) -> u64 {
        self.0.dyn_id()
    }
}

/// A type-erased [`RecvStream`], as produced by a [`StreamTransform`].
pub struct BoxedRecvStream(Box<dyn DynRecvStream>);

impl BoxedRecvStream {
    pub fn new(inner: impl RecvStream + Sync + 'static) -> Self {
        Self(Box::new(inner))
    }
}

impl Debug for BoxedRecvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoxedRecvStream")
            .field(&self.0.dyn_id())
            .finish()
    }
}

impl RecvStream for BoxedRecvStream {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        self.0.dyn_recv_bytes(len).await
    }
    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        self.0.dyn_recv_bytes_exact(len).await
    }
    async fn recv_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.0.dyn_recv_exact(buf).await
    }
    fn stop(&mut self, code: VarInt) -> io::Result<()> {
        self.0.dyn_stop(code)
    }
    fn id(&self) -> u64 {
        self.0.dyn_id()
    }
}

/// Byte counters for one use of a connection's transformed streams, for
/// example one downloader attempt against one provider.
///
/// A transform's own counters (such as the zstd transform's `ZstdStats`)
/// cover every stream it wraps. A tally covers
/// only the streams it was passed to, through
/// [`StreamTransform::send_stream_tallied`] /
/// [`StreamTransform::recv_stream_tallied`], so traffic can be attributed to
/// one provider while the transform is shared. Transforms that keep no
/// counters leave it at zero.
#[derive(Debug, Default)]
pub struct StreamTally {
    wire_bytes_written: AtomicU64,
    plain_bytes_written: AtomicU64,
    wire_bytes_read: AtomicU64,
    plain_bytes_read: AtomicU64,
}

/// A snapshot of a [`StreamTally`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StreamTallySnapshot {
    /// Transformed bytes handed to QUIC.
    pub wire_bytes_written: u64,
    /// Protocol bytes those were produced from.
    pub plain_bytes_written: u64,
    /// Transformed bytes read from QUIC.
    pub wire_bytes_read: u64,
    /// Protocol bytes those were turned into.
    pub plain_bytes_read: u64,
}

impl StreamTally {
    /// Count `wire` transformed bytes handed to QUIC, made from `plain`
    /// protocol bytes. Either may be zero, for a transform that buffers.
    pub fn record_written(&self, wire: u64, plain: u64) {
        self.wire_bytes_written.fetch_add(wire, Ordering::Relaxed);
        self.plain_bytes_written.fetch_add(plain, Ordering::Relaxed);
    }

    /// Count `wire` transformed bytes read from QUIC and `plain` protocol
    /// bytes produced. Either may be zero, for a transform that buffers.
    pub fn record_read(&self, wire: u64, plain: u64) {
        self.wire_bytes_read.fetch_add(wire, Ordering::Relaxed);
        self.plain_bytes_read.fetch_add(plain, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StreamTallySnapshot {
        StreamTallySnapshot {
            wire_bytes_written: self.wire_bytes_written.load(Ordering::Relaxed),
            plain_bytes_written: self.plain_bytes_written.load(Ordering::Relaxed),
            wire_bytes_read: self.wire_bytes_read.load(Ordering::Relaxed),
            plain_bytes_read: self.plain_bytes_read.load(Ordering::Relaxed),
        }
    }
}

/// Wraps both halves of each QUIC stream of a blobs connection.
///
/// Both peers of a connection must use the same transform, which is why a
/// transform is always paired with its own ALPN.
///
/// # Stream end
///
/// The blobs protocol calls [`SendStream::sync`] once it has written everything
/// it intends to write for a message (the request on the getter side, the
/// response on the provider side), then drops the stream, which finishes it.
/// A transform that buffers (a compressor) must make everything written so far
/// decodable by the peer when `sync` returns, because no further call is made
/// before the stream is finished. A receiver that sees the stream end in the
/// middle of a transformed unit must fail with an error rather than report a
/// clean end of stream.
pub trait StreamTransform: Debug + Send + Sync + 'static {
    /// Wrap the sending half of a stream.
    fn send_stream(&self, stream: iroh::endpoint::SendStream) -> BoxedSendStream;
    /// Wrap the receiving half of a stream.
    fn recv_stream(&self, stream: iroh::endpoint::RecvStream) -> BoxedRecvStream;

    /// Wrap the sending half of a stream, also counting its bytes into
    /// `tally`. The default ignores the tally.
    fn send_stream_tallied(
        &self,
        stream: iroh::endpoint::SendStream,
        tally: Arc<StreamTally>,
    ) -> BoxedSendStream {
        let _ = tally;
        self.send_stream(stream)
    }

    /// Wrap the receiving half of a stream, also counting its bytes into
    /// `tally`. The default ignores the tally.
    fn recv_stream_tallied(
        &self,
        stream: iroh::endpoint::RecvStream,
        tally: Arc<StreamTally>,
    ) -> BoxedRecvStream {
        let _ = tally;
        self.recv_stream(stream)
    }
}

/// A connection whose streams are wrapped by a [`StreamTransform`].
///
/// Implements [`GetStreamPair`], so it can be passed wherever a plain
/// [`Connection`] is accepted for a get request.
#[derive(Debug, Clone)]
pub struct TransformedConnection {
    connection: Connection,
    transform: Arc<dyn StreamTransform>,
    tally: Option<Arc<StreamTally>>,
}

impl TransformedConnection {
    pub fn new(connection: Connection, transform: Arc<dyn StreamTransform>) -> Self {
        Self {
            connection,
            transform,
            tally: None,
        }
    }

    /// Also count the bytes of the streams opened through this value into
    /// `tally`, besides the transform's own counters.
    pub fn with_tally(mut self, tally: Arc<StreamTally>) -> Self {
        self.tally = Some(tally);
        self
    }
}

impl GetStreamPair for TransformedConnection {
    async fn open_stream_pair(self) -> io::Result<StreamPair<impl RecvStream, impl SendStream>> {
        let connection_id = self.connection.stable_id() as u64;
        let (send, recv) = self.connection.open_bi().await?;
        let (recv, send) = match self.tally {
            None => (
                self.transform.recv_stream(recv),
                self.transform.send_stream(send),
            ),
            Some(tally) => (
                self.transform.recv_stream_tallied(recv, tally.clone()),
                self.transform.send_stream_tallied(send, tally),
            ),
        };
        Ok(StreamPair::new(connection_id, recv, send))
    }
}

/// A protocol handler that serves the blobs protocol over transformed streams.
///
/// Register it on the transform's own ALPN, beside the plain
/// [`crate::BlobsProtocol`]. Requests are handled by the same provider code as
/// the plain protocol, with the same [`EventSender`] semantics.
///
/// Unlike [`crate::BlobsProtocol`], shutting this handler down does not shut
/// the store down: the plain handler owns the store's lifecycle.
#[derive(Debug, Clone)]
pub struct TransformedBlobsProtocol {
    store: Store,
    events: EventSender,
    transform: Arc<dyn StreamTransform>,
}

impl TransformedBlobsProtocol {
    pub fn new(
        store: &Store,
        events: Option<EventSender>,
        transform: Arc<dyn StreamTransform>,
    ) -> Self {
        Self {
            store: store.clone(),
            events: events.unwrap_or(EventSender::DEFAULT),
            transform,
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }
}

impl ProtocolHandler for TransformedBlobsProtocol {
    async fn accept(&self, conn: Connection) -> std::result::Result<(), AcceptError> {
        crate::provider::handle_connection_with_transform(
            conn,
            self.store.clone(),
            self.events.clone(),
            self.transform.clone(),
        )
        .await;
        Ok(())
    }
}
