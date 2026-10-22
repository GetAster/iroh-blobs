//! API for downloads from multiple nodes.
use std::{
    collections::HashMap,
    fmt::Debug,
    future::{Future, IntoFuture},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use genawaiter::sync::Gen;
use iroh::{Endpoint, EndpointId};
use irpc::{
    channel::{mpsc, oneshot},
    rpc_requests,
};
use n0_error::{anyerr, Result};
use n0_future::{
    future, stream,
    task::{JoinError, JoinSet},
    time::{Duration, Instant},
    BufferedStreamExt, Stream, StreamExt,
};
use rand::seq::SliceRandom;
use serde::{de::Error, Deserialize, Serialize};
use tracing::instrument::Instrument;

use super::Store;
use crate::{
    protocol::{GetManyRequest, GetRequest},
    transport::{StreamTally, StreamTallySnapshot, StreamTransform, TransformedConnection},
    util::{
        connection_pool::{ConnectionPool, ConnectionRef, PoolConnectError},
        sink::{IrpcSenderRefSink, Sink, TokioMpscSenderSink},
    },
    BlobFormat, Hash, HashAndFormat,
};

#[derive(Debug, Clone)]
pub struct Downloader {
    client: irpc::Client<SwarmProtocol>,
}

#[rpc_requests(message = SwarmMsg, alias = "Msg", rpc_feature = "rpc")]
#[derive(Debug, Serialize, Deserialize)]
enum SwarmProtocol {
    #[rpc(tx = mpsc::Sender<DownloadProgressItem>)]
    Download(DownloadRequest),
    #[rpc(tx = oneshot::Sender<()>)]
    WaitIdle(WaitIdleRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WaitIdleRequest;

struct DownloaderActor {
    store: Store,
    pool: Dialer,
    tasks: JoinSet<()>,
    idle_waiters: Vec<irpc::channel::oneshot::Sender<()>>,
}

/// Live byte counters of one provider attempt.
///
/// Handed out with the attempt's [`DownloadProgressItem::TryProvider`] and
/// updated by the attempt as it runs. A consumer that stops reading the
/// progress stream before the attempt's
/// [`DownloadProgressItem::BytesTransferred`] (for example because it
/// cancelled the download) can still read what the attempt received.
///
/// Clones share the counters. Not carried across an rpc boundary: there it
/// reads zero.
#[derive(Debug, Clone, Default)]
pub struct AttemptStats(Arc<AttemptStatsInner>);

#[derive(Debug, Default)]
struct AttemptStatsInner {
    payload_bytes: AtomicU64,
    /// The attempt connected and runs over the downloader's stream transform.
    transformed: AtomicBool,
    tally: Arc<StreamTally>,
}

impl AttemptStats {
    /// Payload bytes the attempt has decoded so far: the figure its
    /// [`DownloadProgressItem::BytesTransferred`] reports once it ends.
    pub fn payload_bytes(&self) -> u64 {
        self.0.payload_bytes.load(Ordering::Relaxed)
    }

    /// Bytes the attempt's streams have received from the network so far,
    /// before the stream transform decoded them. `None` while the attempt runs
    /// over plain streams: no transform, a plain fallback, or not connected.
    pub fn wire_bytes(&self) -> Option<u64> {
        self.transform_tally().map(|t| t.wire_bytes_read)
    }

    /// All of the attempt's transformed-stream counters, in both directions.
    /// `None` like [`Self::wire_bytes`].
    pub fn transform_tally(&self) -> Option<StreamTallySnapshot> {
        self.0
            .transformed
            .load(Ordering::Relaxed)
            .then(|| self.0.tally.snapshot())
    }

    fn set_payload_bytes(&self, n: u64) {
        self.0.payload_bytes.store(n, Ordering::Relaxed);
    }

    /// Mark the attempt as transformed and return the tally for its streams.
    fn tally_for_transform(&self) -> Arc<StreamTally> {
        self.0.transformed.store(true, Ordering::Relaxed);
        self.0.tally.clone()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DownloadProgressItem {
    #[serde(skip)]
    Error(n0_error::AnyError),
    /// An attempt to fetch `request` from provider `id` starts.
    TryProvider {
        id: EndpointId,
        request: Arc<GetRequest>,
        /// The attempt's live counters.
        #[serde(skip)]
        stats: AttemptStats,
    },
    ProviderFailed {
        id: EndpointId,
        request: Arc<GetRequest>,
    },
    PartComplete {
        request: Arc<GetRequest>,
    },
    Progress(u64),
    /// What one attempt read from provider `id` for `request`, emitted once
    /// when that attempt ends (before its [`Self::PartComplete`] or
    /// [`Self::ProviderFailed`]), and only when `bytes` or `wire_bytes` is
    /// non-zero. The attempt's [`AttemptStats`] hold the same final figures.
    ///
    /// `bytes` is payload. `wire_bytes` is what the attempt's streams received
    /// from the network before the downloader's stream transform decoded it
    /// (compressed bytes, for zstd), or `None` when the attempt ran over plain
    /// streams. A transform that keeps no counters reports `Some(0)`.
    ///
    /// The provider and request identify the attempt, so a consumer can credit
    /// bytes per provider also when split children run concurrently.
    ///
    /// Unlike [`Self::Progress`], which is a cumulative offset that includes
    /// bytes already resident locally, these values are additive: summing them
    /// over a download gives the payload pulled off the wire.
    ///
    /// Precisely, each value is the payload **successfully decoded** by that
    /// attempt. A partial transfer that fails part-way is counted up to its
    /// last valid chunk; a chunk that fails verification is not counted,
    /// because the decode error propagates before its progress update is sent.
    /// The sum is therefore per-attempt, not per-byte-of-content: ordinary
    /// failover resumes from what is already stored and so counts each byte
    /// once, but a range that was decoded and then failed to import locally is
    /// re-fetched and counted again.
    BytesTransferred {
        id: EndpointId,
        request: Arc<GetRequest>,
        bytes: u64,
        wire_bytes: Option<u64>,
    },
    DownloadError,
    /// The provider refused the downloader's ALPN, and the attempt continues
    /// over the plain blobs ALPN ([`crate::ALPN`]) with plain streams.
    ///
    /// Only emitted by a downloader built with
    /// [`DownloaderBuilder::plain_fallback`], right after the
    /// [`Self::TryProvider`] it qualifies. `cached` is true when the refusal
    /// was remembered from an earlier attempt, so no dial on the downloader's
    /// ALPN was made this time. This says nothing about whether the provider
    /// has the content: the attempt's outcome is still reported by
    /// [`Self::PartComplete`] or [`Self::ProviderFailed`].
    PlainFallback {
        id: EndpointId,
        cached: bool,
    },
}

impl DownloaderActor {
    fn new(store: Store, pool: Dialer) -> Self {
        Self {
            store,
            pool,
            tasks: JoinSet::new(),
            idle_waiters: Vec::new(),
        }
    }

    async fn run(mut self, mut rx: tokio::sync::mpsc::Receiver<SwarmMsg>) {
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else { break };
                    match msg {
                        SwarmMsg::Download(request) => {
                            self.spawn(handle_download(
                                self.store.clone(),
                                self.pool.clone(),
                                request,
                            ));
                        }
                        SwarmMsg::WaitIdle(WaitIdleMsg { tx, .. }) => {
                            if self.tasks.is_empty() {
                                tx.send(()).await.ok();
                            } else {
                                self.idle_waiters.push(tx);
                            }
                        }
                    }
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Self::log_task_result(res);
                    if self.tasks.is_empty() {
                        for tx in self.idle_waiters.drain(..) {
                            tx.send(()).await.ok();
                        }
                    }
                }
            }
        }
        while let Some(res) = self.tasks.join_next().await {
            Self::log_task_result(res);
        }
    }

    fn log_task_result(res: std::result::Result<(), JoinError>) {
        match res {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => tracing::trace!("download task cancelled: {e}"),
            Err(e) => tracing::error!("download task failed: {e}"),
        }
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        let span = tracing::Span::current();
        self.tasks.spawn(fut.instrument(span));
    }
}

async fn handle_download(store: Store, pool: Dialer, msg: DownloadMsg) {
    let DownloadMsg { inner, mut tx, .. } = msg;
    if let Err(cause) = handle_download_impl(store, pool, inner, &mut tx).await {
        tx.send(DownloadProgressItem::Error(cause)).await.ok();
    }
}

async fn handle_download_impl(
    store: Store,
    pool: Dialer,
    request: DownloadRequest,
    tx: &mut mpsc::Sender<DownloadProgressItem>,
) -> Result<()> {
    match request.strategy {
        SplitStrategy::Split => handle_download_split_impl(store, pool, request, tx).await?,
        SplitStrategy::None => match request.request {
            FiniteRequest::Get(get) => {
                let sink = IrpcSenderRefSink(tx);
                execute_get(
                    &pool,
                    Arc::new(get),
                    &request.providers,
                    &store,
                    request.provider_idle_timeout,
                    sink,
                )
                .await?;
            }
            FiniteRequest::GetMany(_) => {
                handle_download_split_impl(store, pool, request, tx).await?
            }
        },
    }
    Ok(())
}

/// Forwards every progress item from the split path's root fetch except the
/// legacy cumulative [`DownloadProgressItem::Progress`] offset.
///
/// Split mode runs two independent `Progress` counters: the root fetch's, and
/// the child aggregator's, which sums the latest per-child offset and knows
/// nothing about the root. Forwarding both makes the offset jump backwards the
/// moment the first child reports — a 384-byte root followed by `Progress(1)`.
/// Dropping the root's offsets keeps the counter monotonic and matches what
/// consumers saw before the root was reported at all. Provider attribution and
/// the additive `BytesTransferred` totals still flow through, so no accounting
/// is lost.
struct RootProgressFilter<'a>(&'a mut mpsc::Sender<DownloadProgressItem>);

impl Sink<DownloadProgressItem> for RootProgressFilter<'_> {
    type Error = irpc::channel::SendError;

    async fn send(&mut self, value: DownloadProgressItem) -> std::result::Result<(), Self::Error> {
        if matches!(value, DownloadProgressItem::Progress(_)) {
            return Ok(());
        }
        self.0.send(value).await
    }
}

async fn handle_download_split_impl(
    store: Store,
    pool: Dialer,
    request: DownloadRequest,
    tx: &mut mpsc::Sender<DownloadProgressItem>,
) -> Result<()> {
    let providers = request.providers;
    let idle_timeout = request.provider_idle_timeout;
    // The root fetch is a real provider attempt: report it like any other,
    // instead of dropping its events into a `Drain`. Its legacy `Progress`
    // offsets are the exception — see [`RootProgressFilter`].
    let requests = split_request(
        &request.request,
        &providers,
        &pool,
        &store,
        idle_timeout,
        RootProgressFilter(&mut *tx),
    )
    .await?;
    let (progress_tx, progress_rx) = tokio::sync::mpsc::channel(32);
    let mut futs = stream::iter(requests.into_iter().enumerate())
        .map(|(id, request)| {
            let pool = pool.clone();
            let providers = providers.clone();
            let store = store.clone();
            let progress_tx = progress_tx.clone();
            async move {
                let hash = request.hash;
                let (tx, rx) = tokio::sync::mpsc::channel::<(usize, DownloadProgressItem)>(16);
                progress_tx.send(rx).await.ok();
                let sink = TokioMpscSenderSink(tx).with_map(move |x| (id, x));
                let res = execute_get(
                    &pool,
                    Arc::new(request),
                    &providers,
                    &store,
                    idle_timeout,
                    sink,
                )
                .await;
                (hash, res)
            }
        })
        .buffered_unordered(32);
    let mut progress_stream = {
        let mut offsets = HashMap::new();
        let mut total = 0;
        into_stream(progress_rx)
            .flat_map(into_stream)
            .map(move |(id, item)| match item {
                DownloadProgressItem::Progress(offset) => {
                    total += offset;
                    if let Some(prev) = offsets.insert(id, offset) {
                        total -= prev;
                    }
                    DownloadProgressItem::Progress(total)
                }
                x => x,
            })
    };
    let mut closed = false;
    loop {
        tokio::select! {
            Some(item) = progress_stream.next() => {
                tx.send(item).await?;
            },
            res = futs.next() => {
                match res {
                    Some((_hash, Ok(()))) => {
                    }
                    Some((_hash, Err(_e))) => {
                        tx.send(DownloadProgressItem::DownloadError).await?;
                    }
                    None => break,
                }
            }
            _ = tx.closed() => {
                // The sender has been closed, we should stop processing.
                closed = true;
                break;
            }
        }
    }
    if closed {
        return Ok(());
    }
    // Every child has finished, but its last events can still be sitting in the
    // fan-in. Drop the futures stream and the producer handle so the fan-in can
    // terminate, then forward what is left: otherwise a final `TryProvider`,
    // `BytesTransferred`, `ProviderFailed` or `PartComplete` is lost whenever a
    // child completes fast enough to end `futs` first.
    drop(futs);
    drop(progress_tx);
    while let Some(item) = progress_stream.next().await {
        tx.send(item).await?;
    }
    Ok(())
}

fn into_stream<T>(mut recv: tokio::sync::mpsc::Receiver<T>) -> impl Stream<Item = T> {
    Gen::new(|co| async move {
        while let Some(item) = recv.recv().await {
            co.yield_(item).await;
        }
    })
}

#[derive(Debug, Serialize, Deserialize, derive_more::From)]
pub enum FiniteRequest {
    Get(GetRequest),
    GetMany(GetManyRequest),
}

pub trait SupportedRequest {
    fn into_request(self) -> FiniteRequest;
}

impl<I: Into<Hash>, T: IntoIterator<Item = I>> SupportedRequest for T {
    fn into_request(self) -> FiniteRequest {
        let hashes = self.into_iter().map(Into::into).collect::<GetManyRequest>();
        FiniteRequest::GetMany(hashes)
    }
}

impl SupportedRequest for GetRequest {
    fn into_request(self) -> FiniteRequest {
        self.into()
    }
}

impl SupportedRequest for GetManyRequest {
    fn into_request(self) -> FiniteRequest {
        self.into()
    }
}

impl SupportedRequest for Hash {
    fn into_request(self) -> FiniteRequest {
        GetRequest::blob(self).into()
    }
}

impl SupportedRequest for HashAndFormat {
    fn into_request(self) -> FiniteRequest {
        (match self.format {
            BlobFormat::Raw => GetRequest::blob(self.hash),
            BlobFormat::HashSeq => GetRequest::all(self.hash),
        })
        .into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddProviderRequest {
    pub hash: Hash,
    pub providers: Vec<EndpointId>,
}

#[derive(Debug)]
pub struct DownloadRequest {
    pub request: FiniteRequest,
    pub providers: Arc<dyn ContentDiscovery>,
    pub strategy: SplitStrategy,
    /// Abandon a provider attempt that delivers no payload for this long and
    /// move on to the next provider, which is asked only for what is still
    /// missing. `None` waits on a provider indefinitely.
    ///
    /// A provider that accepts a request and then stops sending never fails:
    /// QUIC keep-alive holds its connection open, so without this one stalled
    /// provider blocks every provider after it in the list.
    pub provider_idle_timeout: Option<Duration>,
}

impl DownloadRequest {
    pub fn new(
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
        strategy: SplitStrategy,
    ) -> Self {
        Self {
            request: request.into_request(),
            providers: Arc::new(providers),
            strategy,
            provider_idle_timeout: None,
        }
    }

    /// Set [`Self::provider_idle_timeout`].
    pub fn with_provider_idle_timeout(mut self, idle: Duration) -> Self {
        self.provider_idle_timeout = Some(idle);
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SplitStrategy {
    None,
    Split,
}

impl Serialize for DownloadRequest {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom(
            "cannot serialize DownloadRequest",
        ))
    }
}

// Implement Deserialize to always fail
impl<'de> Deserialize<'de> for DownloadRequest {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(D::Error::custom("cannot deserialize DownloadRequest"))
    }
}

pub type DownloadOptions = DownloadRequest;

pub struct DownloadProgress {
    fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgressItem>>>,
}

impl DownloadProgress {
    fn new(fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgressItem>>>) -> Self {
        Self { fut }
    }

    pub async fn stream(self) -> irpc::Result<impl Stream<Item = DownloadProgressItem> + Unpin> {
        let rx = self.fut.await?;
        Ok(Box::pin(rx.into_stream().map(|item| match item {
            Ok(item) => item,
            Err(e) => DownloadProgressItem::Error(e.into()),
        })))
    }

    async fn complete(self) -> Result<()> {
        let rx = self.fut.await?;
        let stream = rx.into_stream();
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            match item? {
                DownloadProgressItem::Error(e) => Err(e)?,
                DownloadProgressItem::DownloadError => {
                    n0_error::bail_any!("Download error");
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl IntoFuture for DownloadProgress {
    type Output = Result<()>;
    type IntoFuture = future::Boxed<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.complete())
    }
}

/// Builder for a [`Downloader`], created with [`Downloader::builder`].
#[derive(Debug)]
pub struct DownloaderBuilder {
    store: Store,
    endpoint: Endpoint,
    alpn: Vec<u8>,
    pool_options: crate::util::connection_pool::Options,
    transform: Option<Arc<dyn StreamTransform>>,
    plain_fallback: Option<Duration>,
}

impl DownloaderBuilder {
    /// Sets the ALPN used to connect to providers. Defaults to [`crate::ALPN`].
    ///
    /// This is an advanced option: providers must accept the same ALPN,
    /// e.g. via `Router::accept(alpn, blobs)`.
    pub fn alpn(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.alpn = alpn.into();
        self
    }

    /// Sets the options for the connection pool used to connect to providers.
    ///
    /// With [`Self::plain_fallback`], the fallback pool uses the same options.
    pub fn pool_options(mut self, pool_options: crate::util::connection_pool::Options) -> Self {
        self.pool_options = pool_options;
        self
    }

    /// Wraps every stream to a provider with `transform`.
    ///
    /// Providers must serve the blobs protocol with the same transform on the
    /// ALPN set by [`Self::alpn`], e.g. with a
    /// [`TransformedBlobsProtocol`](crate::transport::TransformedBlobsProtocol).
    /// The request semantics, the local-aware resume, the provider walk and the
    /// progress events are unchanged; `Progress` and `BytesTransferred` count
    /// payload after the transform is undone. Each attempt's streams are also
    /// tallied on their own ([`StreamTransform::recv_stream_tallied`]), which
    /// `BytesTransferred::wire_bytes` and [`AttemptStats`] report per provider.
    pub fn stream_transform(mut self, transform: Arc<dyn StreamTransform>) -> Self {
        self.transform = Some(transform);
        self
    }

    /// Falls back to the plain blobs protocol for providers that refuse the
    /// ALPN set by [`Self::alpn`].
    ///
    /// When the TLS handshake fails because the provider serves none of the
    /// offered ALPNs (alert 120, `no_application_protocol`), the same provider
    /// is retried on [`crate::ALPN`] with plain streams, and
    /// [`DownloadProgressItem::PlainFallback`] is emitted. The refusal is
    /// remembered for `ttl`, during which that provider is dialed on the plain
    /// ALPN directly. Any other connect failure is reported as before.
    ///
    /// Without this, a refusing provider is reported as
    /// [`DownloadProgressItem::ProviderFailed`].
    pub fn plain_fallback(mut self, ttl: Duration) -> Self {
        self.plain_fallback = Some(ttl);
        self
    }

    /// Spawns the downloader.
    pub fn build(self) -> Downloader {
        let (tx, rx) = tokio::sync::mpsc::channel::<SwarmMsg>(32);
        let fallback = self.plain_fallback.map(|ttl| PlainFallback {
            pool: ConnectionPool::new(
                self.endpoint.clone(),
                crate::ALPN,
                self.pool_options.clone(),
            ),
            ttl,
            refused: Default::default(),
        });
        let dialer = Dialer {
            primary: ConnectionPool::new(self.endpoint, &self.alpn, self.pool_options),
            transform: self.transform,
            fallback,
        };
        let actor = DownloaderActor::new(self.store, dialer);
        n0_future::task::spawn(actor.run(rx));
        Downloader { client: tx.into() }
    }
}

/// Connects to providers for the downloader: on the downloader's ALPN, with
/// its optional stream transform, and with the optional plain fallback.
#[derive(Debug, Clone)]
struct Dialer {
    primary: ConnectionPool,
    transform: Option<Arc<dyn StreamTransform>>,
    fallback: Option<PlainFallback>,
}

#[derive(Debug, Clone)]
struct PlainFallback {
    pool: ConnectionPool,
    ttl: Duration,
    /// Providers that refused the primary ALPN, until the given instant.
    refused: Arc<std::sync::Mutex<HashMap<EndpointId, Instant>>>,
}

impl PlainFallback {
    fn is_refused(&self, id: EndpointId) -> bool {
        let mut refused = self.refused.lock().expect("poisoned");
        let now = Instant::now();
        refused.retain(|_, until| *until > now);
        refused.contains_key(&id)
    }

    fn remember_refused(&self, id: EndpointId) {
        let until = Instant::now() + self.ttl;
        self.refused.lock().expect("poisoned").insert(id, until);
    }
}

/// A connection handed out by [`Dialer::connect`].
struct Dialed {
    conn: ConnectionRef,
    /// The pool that handed out `conn`.
    pool: ConnectionPool,
    /// The transform to apply to this connection's streams.
    transform: Option<Arc<dyn StreamTransform>>,
    /// `Some(cached)` when this is a plain fallback connection.
    fallback: Option<bool>,
}

impl Dialer {
    async fn connect(&self, id: EndpointId) -> std::result::Result<Dialed, PoolConnectError> {
        let Some(fallback) = &self.fallback else {
            return Ok(Dialed {
                conn: self.primary.get_or_connect(id).await?,
                pool: self.primary.clone(),
                transform: self.transform.clone(),
                fallback: None,
            });
        };
        if fallback.is_refused(id) {
            return Ok(Dialed {
                conn: fallback.pool.get_or_connect(id).await?,
                pool: fallback.pool.clone(),
                transform: None,
                fallback: Some(true),
            });
        }
        match self.primary.get_or_connect(id).await {
            Ok(conn) => Ok(Dialed {
                conn,
                pool: self.primary.clone(),
                transform: self.transform.clone(),
                fallback: None,
            }),
            Err(cause) if refused_alpn(&cause) => {
                tracing::debug!(%id, "provider refused the downloader ALPN; falling back to plain");
                fallback.remember_refused(id);
                Ok(Dialed {
                    conn: fallback.pool.get_or_connect(id).await?,
                    pool: fallback.pool.clone(),
                    transform: None,
                    fallback: Some(false),
                })
            }
            Err(cause) => Err(cause),
        }
    }
}

impl Dialed {
    /// Stops the pool from handing out this connection again, so the next
    /// connect to the provider dials anew.
    async fn retire(&self) {
        // Fails only once the pool has shut down, and then there is nothing to retire.
        self.pool.retire(&self.conn).await.ok();
    }
}

/// Whether the connection died under an attempt: the peer reset it (a restart
/// on the same address), it timed out, or this side closed it, for example
/// because a concurrent attempt retired it.
fn connection_lost(conn: &iroh::endpoint::Connection) -> bool {
    use iroh::endpoint::ConnectionError;
    matches!(
        conn.close_reason(),
        Some(ConnectionError::Reset | ConnectionError::TimedOut | ConnectionError::LocallyClosed)
    )
}

/// Whether a connect failed because the peer serves none of the offered ALPNs
/// (TLS alert 120, `no_application_protocol`).
fn refused_alpn(err: &PoolConnectError) -> bool {
    use iroh::endpoint::{ConnectError, ConnectingError, ConnectionError, TransportErrorCode};
    const NO_APPLICATION_PROTOCOL: u8 = 120;
    let PoolConnectError::ConnectError { source, .. } = err else {
        return false;
    };
    let conn_err = match source.as_ref() {
        ConnectError::Connection { source, .. } => source,
        ConnectError::Connecting {
            source: ConnectingError::ConnectionError { source, .. },
            ..
        } => source,
        _ => return false,
    };
    matches!(
        conn_err,
        ConnectionError::ConnectionClosed(close)
            if close.error_code == TransportErrorCode::crypto(NO_APPLICATION_PROTOCOL)
    )
}

impl Downloader {
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        Self::builder(store, endpoint).build()
    }

    /// Creates a builder for a downloader with non-default options.
    pub fn builder(store: &Store, endpoint: &Endpoint) -> DownloaderBuilder {
        DownloaderBuilder {
            store: store.clone(),
            endpoint: endpoint.clone(),
            alpn: crate::ALPN.to_vec(),
            pool_options: Default::default(),
            transform: None,
            plain_fallback: None,
        }
    }

    #[deprecated(note = "use Downloader::builder")]
    pub fn new_with_opts(
        store: &Store,
        endpoint: &Endpoint,
        pool_options: crate::util::connection_pool::Options,
    ) -> Self {
        Self::builder(store, endpoint)
            .pool_options(pool_options)
            .build()
    }

    pub fn download(
        &self,
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
    ) -> DownloadProgress {
        let request = request.into_request();
        let providers = Arc::new(providers);
        self.download_with_opts(DownloadOptions {
            request,
            providers,
            strategy: SplitStrategy::None,
            provider_idle_timeout: None,
        })
    }

    pub fn download_with_opts(&self, options: DownloadOptions) -> DownloadProgress {
        let fut = self.client.server_streaming(options, 32);
        DownloadProgress::new(Box::pin(fut))
    }

    /// Wait until the downloader has no in-flight download tasks.
    ///
    /// This is mostly useful for tests, where you want to confirm that all
    /// previously-issued downloads have settled (whether they completed
    /// successfully or errored out). Note that the downloader is not
    /// guaranteed to become idle if it is being interacted with concurrently;
    /// in that case this might wait forever. Also note that once you get the
    /// callback, the downloader is not guaranteed to still be idle — all this
    /// tells you is that there was a point in time, between the call and the
    /// response, where it was idle.
    pub async fn wait_idle(&self) -> irpc::Result<()> {
        self.client.rpc(WaitIdleRequest).await?;
        Ok(())
    }
}

/// Split a request into multiple requests that can be run in parallel.
async fn split_request<'a>(
    request: &'a FiniteRequest,
    providers: &Arc<dyn ContentDiscovery>,
    pool: &Dialer,
    store: &Store,
    idle_timeout: Option<Duration>,
    progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
) -> Result<Box<dyn Iterator<Item = GetRequest> + Send + 'a>> {
    Ok(match request {
        FiniteRequest::Get(req) => {
            let Some(_first) = req.ranges.iter_infinite().next() else {
                return Ok(Box::new(std::iter::empty()));
            };
            let first = GetRequest::blob(req.hash);
            execute_get(
                pool,
                Arc::new(first),
                providers,
                store,
                idle_timeout,
                progress,
            )
            .await?;
            let size = store.observe(req.hash).await?.size();
            n0_error::ensure_any!(size % 32 == 0, "Size is not a multiple of 32");
            let n = size / 32;
            Box::new(
                req.ranges
                    .iter_infinite()
                    .take(n as usize + 1)
                    .enumerate()
                    .filter_map(|(i, ranges)| {
                        if i != 0 && !ranges.is_empty() {
                            Some(
                                GetRequest::builder()
                                    .offset(i as u64, ranges.clone())
                                    .build(req.hash),
                            )
                        } else {
                            None
                        }
                    }),
            )
        }
        FiniteRequest::GetMany(req) => Box::new(
            req.hashes
                .iter()
                .enumerate()
                .map(|(i, hash)| GetRequest::blob_ranges(*hash, req.ranges[i as u64].clone())),
        ),
    })
}

/// Emit the final counters of one provider attempt, skipping the zero case.
async fn send_bytes_transferred(
    mut progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
    id: EndpointId,
    request: &Arc<GetRequest>,
    stats: &AttemptStats,
) -> Result<()> {
    let bytes = stats.payload_bytes();
    let wire_bytes = stats.wire_bytes();
    if bytes > 0 || wire_bytes.unwrap_or(0) > 0 {
        progress
            .send(DownloadProgressItem::BytesTransferred {
                id,
                request: request.clone(),
                bytes,
                wire_bytes,
            })
            .await?;
    }
    Ok(())
}

/// Execute a get request sequentially for multiple providers.
///
/// It will try each provider in order
/// until it finds one that can fulfill the request. When trying a new provider,
/// it takes the progress from the previous providers into account, so e.g.
/// if the first provider had the first 10% of the data, it will only ask the next
/// provider for the remaining 90%.
///
/// This is fully sequential, so there will only be one request in flight at a time.
///
/// If the request is not complete after trying all providers, it will return an error.
/// If the provider stream never ends, it will try indefinitely.
///
/// With `idle_timeout`, an attempt that delivers no payload for that long is
/// reported as [`DownloadProgressItem::ProviderFailed`] and the next provider
/// is tried; without it, a provider that stalls blocks the walk.
///
/// With `idle_timeout`, an attempt whose connection is lost is also retried
/// once on a fresh connection to the same provider before moving on, and the
/// pool stops handing out the lost connection. Lost means the connection
/// closed by reset, idle timeout or locally, or the provider did not
/// acknowledge anything within [`silence_bound`] of the request (it died
/// without closing, for example to restart on another address). Each try is
/// reported with its own [`DownloadProgressItem::TryProvider`].
async fn execute_get(
    pool: &Dialer,
    request: Arc<GetRequest>,
    providers: &Arc<dyn ContentDiscovery>,
    store: &Store,
    idle_timeout: Option<Duration>,
    mut progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
) -> Result<()> {
    let remote = store.remote();
    let mut providers = providers.find_providers(request.content());
    // A provider to try again on a fresh connection, and the last one retried.
    let mut retry: Option<EndpointId> = None;
    let mut retried: Option<EndpointId> = None;
    loop {
        let provider = match retry.take() {
            Some(provider) => provider,
            None => match providers.next().await {
                Some(provider) => provider,
                None => break,
            },
        };
        let conn = pool.connect(provider);
        let local = remote.local_for_request(request.clone()).await?;
        if local.is_complete() {
            return Ok(());
        }
        // Emitted only once we know there is something left to fetch, so
        // `TryProvider` means an actual transfer attempt rather than a no-op on
        // already-resident content.
        let attempt = AttemptStats::default();
        progress
            .send(DownloadProgressItem::TryProvider {
                id: provider,
                request: request.clone(),
                stats: attempt.clone(),
            })
            .await?;
        let local_bytes = local.local_bytes();
        let Ok(dialed) = conn.await else {
            progress
                .send(DownloadProgressItem::ProviderFailed {
                    id: provider,
                    request: request.clone(),
                })
                .await?;
            continue;
        };
        if let Some(cached) = dialed.fallback {
            progress
                .send(DownloadProgressItem::PlainFallback {
                    id: provider,
                    cached,
                })
                .await?;
        }
        let conn: &iroh::endpoint::Connection = &dialed.conn;
        // Datagrams received before the request goes out. The provider must
        // acknowledge the request, so if this has not moved within the
        // silence bound, nothing is answering on this connection.
        let received = conn.stats().udp_rx.datagrams;
        // The live per-attempt payload counter lets a *failed* attempt still
        // report the bytes it did transfer. On success the exact figure from
        // the returned stats replaces the last progress update.
        let attempt_counter = attempt.clone();
        // The attempt's latest payload progress, read by the stall guard.
        let clock = Arc::new(StallClock::new());
        let progress_clock = clock.clone();
        let on_progress = move |x| {
            attempt_counter.set_payload_bytes(x);
            progress_clock.touch();
            DownloadProgressItem::Progress(x + local_bytes)
        };
        let outcome = match &dialed.transform {
            None => {
                let get = remote.execute_get_sink(
                    conn.clone(),
                    local.missing(),
                    (&mut progress).with_map(on_progress),
                    Some(&clock),
                );
                guard_attempt(get, idle_timeout, &clock, conn, received).await
            }
            Some(transform) => {
                let get = remote.execute_get_sink(
                    TransformedConnection::new(conn.clone(), transform.clone())
                        .with_tally(attempt.tally_for_transform()),
                    local.missing(),
                    (&mut progress).with_map(on_progress),
                    Some(&clock),
                );
                guard_attempt(get, idle_timeout, &clock, conn, received).await
            }
        };
        let silent = matches!(outcome, Err(Abandoned::Silent));
        match outcome
            .map_err(|_| ())
            .and_then(|res| res.map_err(|_cause| ()))
        {
            Ok(stats) => {
                attempt.set_payload_bytes(stats.payload_bytes_read);
                send_bytes_transferred(&mut progress, provider, &request, &attempt).await?;
                progress
                    .send(DownloadProgressItem::PartComplete {
                        request: request.clone(),
                    })
                    .await?;
                return Ok(());
            }
            Err(()) => {
                let lost = idle_timeout.is_some() && (silent || connection_lost(conn));
                if lost {
                    tracing::debug!(%provider, silent, "connection to provider lost; retiring it");
                    dialed.retire().await;
                    if retried != Some(provider) {
                        retried = Some(provider);
                        retry = Some(provider);
                    }
                }
                send_bytes_transferred(&mut progress, provider, &request, &attempt).await?;
                progress
                    .send(DownloadProgressItem::ProviderFailed {
                        id: provider,
                        request: request.clone(),
                    })
                    .await?;
                continue;
            }
        }
    }
    Err(anyerr!("Unable to download {}", request.hash))
}

/// Why the stall guard abandoned a provider attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Abandoned {
    /// No payload progress for the idle timeout.
    Stalled,
    /// The connection received nothing within [`silence_bound`] of the
    /// request: the provider is gone, not slow.
    Silent,
}

/// When a provider attempt last made payload progress, for the stall guard.
///
/// The clock runs only while the attempt waits on the provider. While the
/// attempt waits on local work instead (the store importing what arrived, or
/// the progress consumer taking an update), it is stopped, and it restarts
/// when that work ends: a slow disk or a busy consumer is not a stalled
/// provider. That includes the end of an attempt, after the provider has
/// delivered every byte and the store is still committing them.
#[derive(Debug)]
pub(crate) struct StallClock {
    started: Instant,
    /// Milliseconds from `started` to the latest progress.
    last_ms: AtomicU64,
    /// Local waits in progress; while non-zero the clock is stopped.
    local: AtomicUsize,
}

impl StallClock {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            last_ms: AtomicU64::new(0),
            local: AtomicUsize::new(0),
        }
    }

    /// Record progress now.
    fn touch(&self) {
        let now = self.started.elapsed().as_millis() as u64;
        self.last_ms.store(now, Ordering::SeqCst);
    }

    /// Stop the clock until the returned guard drops.
    pub(crate) fn local(&self) -> LocalWait<'_> {
        self.local.fetch_add(1, Ordering::SeqCst);
        LocalWait(self)
    }

    /// When the attempt counts as stalled, or `None` while it waits on local
    /// work.
    fn stalled_at(&self, idle: Duration) -> Option<Instant> {
        if self.local.load(Ordering::SeqCst) > 0 {
            return None;
        }
        let last = Duration::from_millis(self.last_ms.load(Ordering::SeqCst));
        Some(self.started + last + idle)
    }
}

/// A local wait in progress: see [`StallClock::local`].
#[derive(Debug)]
pub(crate) struct LocalWait<'a>(&'a StallClock);

impl Drop for LocalWait<'_> {
    fn drop(&mut self) {
        // Restart the clock before releasing it, so the guard never reads a
        // time from before the local work with no local work in progress.
        self.0.touch();
        self.0.local.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The least time to wait for the provider to acknowledge a request.
const MIN_SILENCE: Duration = Duration::from_secs(1);

/// How long a connection may receive nothing after a request before the
/// provider counts as gone: four round trips on its slowest path, at least
/// [`MIN_SILENCE`], at most the idle timeout.
///
/// A live QUIC peer acknowledges the request within a round trip plus its ACK
/// delay even when the provider is slow to answer it.
fn silence_bound(conn: &iroh::endpoint::Connection, idle: Duration) -> Duration {
    let rtt = conn
        .paths()
        .iter()
        .map(|path| path.rtt())
        .max()
        .unwrap_or_default();
    (rtt * 4).max(MIN_SILENCE).min(idle)
}

/// Run one provider attempt, under the stall guard if `idle_timeout` is set.
async fn guard_attempt<T>(
    attempt: impl Future<Output = T>,
    idle_timeout: Option<Duration>,
    clock: &StallClock,
    conn: &iroh::endpoint::Connection,
    received: u64,
) -> std::result::Result<T, Abandoned> {
    match idle_timeout {
        None => Ok(attempt.await),
        Some(idle) => stall_guard(attempt, clock, idle, conn, received).await,
    }
}

/// Run one provider attempt, abandoning it once the provider has made no
/// payload progress for `idle` on `clock`, or once the connection has
/// received no datagram since `received` was read for [`silence_bound`].
///
/// The clock covers the whole attempt, including opening the stream and
/// waiting for the first chunk, so a provider that accepts and never answers
/// is caught too. It does not cover local work (see [`StallClock`]).
async fn stall_guard<T>(
    attempt: impl Future<Output = T>,
    clock: &StallClock,
    idle: Duration,
    conn: &iroh::endpoint::Connection,
    received: u64,
) -> std::result::Result<T, Abandoned> {
    let silence = async {
        n0_future::time::sleep_until(clock.started + silence_bound(conn, idle)).await;
        if conn.stats().udp_rx.datagrams == received {
            return;
        }
        future::pending::<()>().await
    };
    tokio::select! {
        biased;
        res = attempt => Ok(res),
        () = silence => {
            tracing::debug!("provider acknowledged nothing since the request; connection lost");
            Err(Abandoned::Silent)
        }
        () = stalled(clock, idle) => {
            tracing::debug!(?idle, "provider attempt stalled; trying the next provider");
            Err(Abandoned::Stalled)
        }
    }
}

/// Resolve once `clock` shows no progress for `idle` outside local work.
async fn stalled(clock: &StallClock, idle: Duration) {
    loop {
        let now = Instant::now();
        let wait = match clock.stalled_at(idle) {
            Some(at) if now >= at => return,
            Some(at) => at - now,
            // Stopped for local work: when it ends the clock restarts, so the
            // attempt cannot stall sooner than `idle` from now.
            None => idle,
        };
        n0_future::time::sleep(wait).await;
    }
}

/// Trait for pluggable content discovery strategies.
pub trait ContentDiscovery: Debug + Send + Sync + 'static {
    fn find_providers(&self, hash: HashAndFormat) -> n0_future::stream::Boxed<EndpointId>;
}

impl<C, I> ContentDiscovery for C
where
    C: Debug + Clone + IntoIterator<Item = I> + Send + Sync + 'static,
    C::IntoIter: Send + Sync + 'static,
    I: Into<EndpointId> + Send + Sync + 'static,
{
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let providers = self.clone();
        n0_future::stream::iter(providers.into_iter().map(Into::into)).boxed()
    }
}

#[derive(derive_more::Debug)]
pub struct Shuffled {
    nodes: Vec<EndpointId>,
}

impl Shuffled {
    pub fn new(nodes: Vec<EndpointId>) -> Self {
        Self { nodes }
    }
}

impl ContentDiscovery for Shuffled {
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let mut nodes = self.nodes.clone();
        nodes.shuffle(&mut rand::rng());
        n0_future::stream::iter(nodes).boxed()
    }
}

#[cfg(test)]
#[cfg(feature = "fs-store")]
mod tests {
    use std::{collections::HashMap, ops::Deref};

    use bao_tree::{ChunkNum, ChunkRanges};
    use iroh::{endpoint::presets, protocol::Router, Endpoint, EndpointId, RelayMode};
    use n0_future::StreamExt;
    use testresult::TestResult;

    use crate::{
        api::{
            blobs::AddBytesOptions,
            downloader::{
                DownloadOptions, DownloadProgressItem, Downloader, Shuffled, SplitStrategy,
            },
        },
        hashseq::HashSeq,
        net_protocol::BlobsProtocol,
        protocol::{GetManyRequest, GetRequest},
        store::mem::MemStore,
        tests::{node_test_setup_fs, node_test_setup_mem},
        Hash, HashAndFormat,
    };

    #[tokio::test]
    #[ignore = "todo"]
    async fn downloader_get_many_smoke() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice("hello world").await?;
        let tt2 = store2.add_slice("hello world 2").await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetManyRequest::builder()
            .hash(tt1.hash, ChunkRanges::all())
            .hash(tt2.hash, ChunkRanges::all())
            .build();
        let mut progress = swarm
            .download(request, Shuffled::new(vec![node1_id, node2_id]))
            .stream()
            .await?;
        while progress.next().await.is_some() {}
        assert_eq!(store3.get_bytes(tt1.hash).await?.deref(), b"hello world");
        assert_eq!(store3.get_bytes(tt2.hash).await?.deref(), b"hello world 2");
        Ok(())
    }

    #[tokio::test]
    async fn downloader_get_smoke() -> TestResult<()> {
        // tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetRequest::builder()
            .root(ChunkRanges::all())
            .next(ChunkRanges::all())
            .next(ChunkRanges::all())
            .build(root.hash);
        if true {
            let mut progress = swarm
                .download_with_opts(DownloadOptions::new(
                    request,
                    [node1_id, node2_id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?;
            while progress.next().await.is_some() {}
        }
        if false {
            let conn = r3.endpoint().connect(node1_addr, crate::ALPN).await?;
            let remote = store3.remote();
            let _rh = remote
                .execute_get(
                    conn.clone(),
                    GetRequest::builder()
                        .root(ChunkRanges::all())
                        .build(root.hash),
                )
                .await?;
            let h1 = remote.execute_get(
                conn.clone(),
                GetRequest::builder()
                    .child(0, ChunkRanges::all())
                    .build(root.hash),
            );
            let h2 = remote.execute_get(
                conn.clone(),
                GetRequest::builder()
                    .child(1, ChunkRanges::all())
                    .build(root.hash),
            );
            h1.await?;
            h2.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn downloader_get_all() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetRequest::all(root.hash);
        let mut progress = swarm
            .download_with_opts(DownloadOptions::new(
                request,
                [node1_id, node2_id],
                SplitStrategy::Split,
            ))
            .stream()
            .await?;
        while progress.next().await.is_some() {}
        Ok(())
    }

    #[tokio::test]
    async fn downloader_custom_alpn() -> TestResult<()> {
        const CUSTOM_ALPN: &[u8] = b"/custom-blobs/1";
        // provider that only accepts the custom ALPN
        let store1 = MemStore::new();
        let ep1 = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Default)
            .bind()
            .await?;
        let r1 = Router::builder(ep1)
            .accept(CUSTOM_ALPN, BlobsProtocol::new(&store1, None))
            .spawn();
        let tt = store1.add_slice("hello world").await?;
        let (r2, store2, sp2) = node_test_setup_mem().await?;
        sp2.add_endpoint_info(r1.endpoint().addr());
        let providers = [r1.endpoint().id()];

        let timeout = std::time::Duration::from_secs(10);
        let default = Downloader::new(&store2, r2.endpoint());
        let res = tokio::time::timeout(
            timeout,
            default.download(GetRequest::blob(tt.hash), providers),
        )
        .await?;
        assert!(res.is_err());

        let custom = Downloader::builder(&store2, r2.endpoint())
            .alpn(CUSTOM_ALPN)
            .build();
        tokio::time::timeout(
            timeout,
            custom.download(GetRequest::blob(tt.hash), providers),
        )
        .await??;
        assert_eq!(store2.get_bytes(tt.hash).await?.deref(), b"hello world");
        Ok(())
    }

    /// Invariant: `DownloaderActor` must reap each `handle_download` task
    /// from its `JoinSet` as that task finishes, not only on shutdown.
    /// If steady-state reaping is skipped, every completed download
    /// retains its tokio task header for the lifetime of the actor and
    /// the heap grows linearly with download volume — invisible at the
    /// API surface, since downloads still report progress and finish.
    ///
    /// We submit many downloads with an empty provider list so each
    /// `handle_download` finishes in microseconds with no I/O, then
    /// assert the actor reaches an idle state via [`Downloader::wait_idle`].
    /// If the `tasks.join_next()` arm is removed from
    /// `DownloaderActor::run`'s `select!`, the JoinSet stays full of
    /// completed-but-not-joined tasks, `tasks.is_empty()` never becomes
    /// true, and the timeout below fires.
    ///
    /// The complementary `idle_waiters` notification path is not
    /// exercised here: by the time this test calls `wait_idle`, the
    /// actor has already drained the JoinSet during the per-stream
    /// awaits, so `wait_idle`'s fast path answers directly.
    #[tokio::test]
    async fn downloader_drains_completed_tasks() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r, store, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let swarm = Downloader::new(&store, r.endpoint());

        let n = 1_000;
        let bogus_hash = Hash::new(b"this hash is not stored anywhere");
        let mut streams = Vec::with_capacity(n);
        for _ in 0..n {
            streams.push(
                swarm
                    .download(GetRequest::all(bogus_hash), Shuffled::new(vec![]))
                    .stream()
                    .await?,
            );
        }
        for mut s in streams {
            while s.next().await.is_some() {}
        }

        tokio::time::timeout(std::time::Duration::from_secs(5), swarm.wait_idle())
            .await
            .map_err(|_| {
                "wait_idle did not resolve within 5s — DownloaderActor JoinSet not draining"
            })??;

        Ok(())
    }

    /// What a download reported, folded out of its progress stream.
    #[derive(Default, Debug)]
    struct Reported {
        tried: Vec<EndpointId>,
        failed: Vec<EndpointId>,
        completed: usize,
        bytes: u64,
        /// Payload per provider, from `BytesTransferred`.
        by_provider: HashMap<EndpointId, u64>,
        /// Requests whose `BytesTransferred` named a provider other than the
        /// one their `TryProvider` announced.
        misattributed: usize,
        errors: usize,
        fallbacks: Vec<(EndpointId, bool)>,
        /// Live counters of every attempt, by provider.
        attempts: Vec<(EndpointId, super::AttemptStats)>,
    }

    impl Reported {
        fn bytes_from(&self, id: EndpointId) -> u64 {
            self.by_provider.get(&id).copied().unwrap_or(0)
        }
    }

    async fn collect(
        mut stream: impl n0_future::Stream<Item = DownloadProgressItem> + Unpin,
    ) -> Reported {
        let mut out = Reported::default();
        let mut active = HashMap::new();
        while let Some(item) = stream.next().await {
            match item {
                DownloadProgressItem::TryProvider { id, request, stats } => {
                    out.tried.push(id);
                    active.insert(request, id);
                    out.attempts.push((id, stats));
                }
                DownloadProgressItem::ProviderFailed { id, .. } => out.failed.push(id),
                DownloadProgressItem::PartComplete { .. } => out.completed += 1,
                DownloadProgressItem::BytesTransferred {
                    id, request, bytes, ..
                } => {
                    out.bytes += bytes;
                    *out.by_provider.entry(id).or_default() += bytes;
                    if active.get(&request) != Some(&id) {
                        out.misattributed += 1;
                    }
                }
                DownloadProgressItem::Error(_) | DownloadProgressItem::DownloadError => {
                    out.errors += 1
                }
                DownloadProgressItem::Progress(_) => {}
                DownloadProgressItem::PlainFallback { id, cached } => {
                    out.fallbacks.push((id, cached))
                }
            }
        }
        out
    }

    /// Build a HashSeq of `children` in `store` and return (root hash, total
    /// payload bytes of root + children).
    async fn add_collection(
        store: &crate::store::fs::FsStore,
        children: &[Vec<u8>],
    ) -> TestResult<(Hash, u64)> {
        let mut hashes = Vec::new();
        let mut total = 0u64;
        for child in children {
            hashes.push(store.add_slice(child.clone()).await?.hash);
            total += child.len() as u64;
        }
        let hs = hashes.into_iter().collect::<HashSeq>();
        let root_bytes: bytes::Bytes = hs.into();
        total += root_bytes.len() as u64;
        let root = store
            .add_bytes_with_opts(AddBytesOptions {
                data: root_bytes,
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        Ok((root.hash, total))
    }

    /// `Progress` is a cumulative offset that consumers render as a bar, so it
    /// must never go backwards.
    ///
    /// In `Split` mode the root fetch and the child aggregation are separate
    /// counters — the aggregator sums the latest per-child offset and knows
    /// nothing about the root — so forwarding both emits the root's total and
    /// then restarts from the first child's offset.
    #[tokio::test]
    async fn downloader_split_progress_is_monotonic() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, sp2) = node_test_setup_fs(testdir.path().join("b")).await?;
        // Twelve one-byte children: the root is 12 * 32 bytes of hashes, far
        // larger than any child, so a root offset reaching the child counter
        // shows up as a backwards jump.
        let children = (0..12u8).map(|i| vec![i; 1]).collect::<Vec<_>>();
        let (root, _) = add_collection(&store1, &children).await?;
        let addr1 = r1.endpoint().addr();
        sp2.add_endpoint_info(addr1.clone());
        let swarm = Downloader::new(&store2, r2.endpoint());

        let mut stream = swarm
            .download_with_opts(DownloadOptions::new(
                GetRequest::all(root),
                [addr1.id],
                SplitStrategy::Split,
            ))
            .stream()
            .await?;
        let mut offsets = Vec::new();
        while let Some(item) = stream.next().await {
            if let DownloadProgressItem::Progress(offset) = item {
                offsets.push(offset);
            }
        }

        let mut high = 0;
        for (i, offset) in offsets.iter().enumerate() {
            assert!(
                *offset >= high,
                "progress went backwards at index {i}: {offsets:?}"
            );
            high = *offset;
        }
        // Monotonicity alone would also hold if progress stopped being reported
        // at all, so pin that it is still emitted and still lands on the child
        // aggregate total (the root is reported through `BytesTransferred`, not
        // through this counter).
        let children_total: u64 = children.iter().map(|c| c.len() as u64).sum();
        assert_eq!(
            offsets.last().copied(),
            Some(children_total),
            "final offset should be the child total: {offsets:?}"
        );
        Ok(())
    }

    /// Split downloads must report the provider that served the HashSeq root
    /// (it used to be swallowed by a `Drain` sink), and the summed
    /// `BytesTransferred` must equal the exact payload of root + children.
    #[tokio::test]
    async fn downloader_split_reports_root_and_exact_bytes() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, sp2) = node_test_setup_fs(testdir.path().join("b")).await?;
        let children = vec![vec![1u8; 100_000], vec![2u8; 4096]];
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        let addr1 = r1.endpoint().addr();
        sp2.add_endpoint_info(addr1.clone());
        let swarm = Downloader::new(&store2, r2.endpoint());

        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    GetRequest::all(root),
                    [addr1.id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        // root + both children, each attributed to the one provider.
        assert_eq!(reported.tried, vec![addr1.id; 3]);
        assert!(reported.failed.is_empty());
        assert_eq!(reported.completed, 3);
        assert_eq!(reported.bytes, expected_bytes);
        Ok(())
    }

    /// A child that is already resident must neither be re-fetched nor reported
    /// as a provider attempt.
    #[tokio::test]
    async fn downloader_split_skips_resident_child() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, sp2) = node_test_setup_fs(testdir.path().join("b")).await?;
        let resident = vec![1u8; 100_000];
        let missing = vec![2u8; 4096];
        let children = vec![resident.clone(), missing.clone()];
        let (root, all_bytes) = add_collection(&store1, &children).await?;
        // The consumer already holds the big child.
        store2.add_slice(resident.clone()).await?;
        let addr1 = r1.endpoint().addr();
        sp2.add_endpoint_info(addr1.clone());
        let swarm = Downloader::new(&store2, r2.endpoint());

        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    GetRequest::all(root),
                    [addr1.id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        // Root + the missing child only: the resident child never dials.
        assert_eq!(reported.tried, vec![addr1.id; 2]);
        assert_eq!(reported.completed, 2);
        assert_eq!(reported.bytes, all_bytes - resident.len() as u64);
        Ok(())
    }

    /// Every completed child must be reported, including the last one to
    /// finish. The fan-in used to be abandoned as soon as the futures stream
    /// ended, dropping whatever was still buffered; small children make that
    /// race likely, so this repeats with a fresh consumer each round.
    #[tokio::test]
    async fn downloader_split_reports_every_child_completion() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let children = (0..12u8).map(|i| vec![i; 1024]).collect::<Vec<_>>();
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        let addr1 = r1.endpoint().addr();

        for round in 0..3 {
            let (r2, store2, _, sp2) =
                node_test_setup_fs(testdir.path().join(format!("consumer-{round}"))).await?;
            sp2.add_endpoint_info(addr1.clone());
            let swarm = Downloader::new(&store2, r2.endpoint());
            let reported = collect(
                swarm
                    .download_with_opts(DownloadOptions::new(
                        GetRequest::all(root),
                        [addr1.id],
                        SplitStrategy::Split,
                    ))
                    .stream()
                    .await?,
            )
            .await;
            assert_eq!(reported.errors, 0, "round {round}");
            assert_eq!(reported.completed, children.len() + 1, "round {round}");
            assert_eq!(reported.tried.len(), children.len() + 1, "round {round}");
            assert_eq!(reported.bytes, expected_bytes, "round {round}");
            r2.shutdown().await?;
        }
        Ok(())
    }

    /// A provider holding only part of a blob transfers what it has and then
    /// fails. Those bytes must still be counted, and the next provider must be
    /// asked only for the remainder.
    #[tokio::test]
    async fn downloader_counts_bytes_from_a_partial_provider() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r2, store2, _, sp2) = node_test_setup_fs(testdir.path().join("partial")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let data = vec![42u8; 1024 * 1024];
        let tt = store1.add_slice(data.clone()).await?;
        let holder = r1.endpoint().addr();
        let partial = r2.endpoint().addr();
        sp2.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(partial.clone());

        // `partial` fetches only the first 8 chunks, leaving an incomplete blob.
        let conn = r2.endpoint().connect(holder.clone(), crate::ALPN).await?;
        store2
            .remote()
            .execute_get(
                conn,
                GetRequest::builder()
                    .root(ChunkRanges::from(..ChunkNum(8)))
                    .build(tt.hash),
            )
            .await?;
        let held = store2
            .remote()
            .local(HashAndFormat::raw(tt.hash))
            .await?
            .local_bytes();
        assert!(held > 0 && held < data.len() as u64, "held {held} bytes");

        let swarm = Downloader::new(&store3, r3.endpoint());
        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    tt.hash,
                    [partial.id, holder.id],
                    SplitStrategy::None,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        assert_eq!(reported.failed, vec![partial.id]);
        assert_eq!(reported.tried, vec![partial.id, holder.id]);
        assert_eq!(store3.get_bytes(tt.hash).await?.deref(), data.as_slice());
        assert_eq!(
            reported.bytes,
            data.len() as u64,
            "partial provider {held}, total {}",
            data.len()
        );
        // Each provider is credited with what its own attempt delivered, and
        // the live counters end on the same figures, with no wire bytes for
        // plain attempts.
        assert_eq!(reported.misattributed, 0);
        assert_eq!(
            reported.bytes_from(partial.id) + reported.bytes_from(holder.id),
            data.len() as u64
        );
        let live = reported
            .attempts
            .iter()
            .map(|(id, stats)| (*id, stats.payload_bytes(), stats.wire_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(
            live,
            vec![
                (partial.id, reported.bytes_from(partial.id), None),
                (holder.id, reported.bytes_from(holder.id), None)
            ]
        );
        Ok(())
    }

    /// Split children run concurrently against different providers; every
    /// `BytesTransferred` names the provider and request of its own attempt,
    /// so bytes are credited per provider without relying on event order.
    #[tokio::test]
    async fn downloader_split_credits_bytes_per_provider() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("full")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("half")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let children = vec![vec![1u8; 100_000], vec![2u8; 30_000], vec![3u8; 7_000]];
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        // `half` has the root and the first child only.
        let half_root = store2
            .add_bytes_with_opts(AddBytesOptions {
                data: store1.get_bytes(root).await?,
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        assert_eq!(half_root.hash, root);
        store2.add_slice(children[0].clone()).await?;
        let root_len = expected_bytes - children.iter().map(|c| c.len() as u64).sum::<u64>();
        let full = r1.endpoint().addr();
        let half = r2.endpoint().addr();
        sp3.add_endpoint_info(full.clone());
        sp3.add_endpoint_info(half.clone());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    GetRequest::all(root),
                    [half.id, full.id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        assert_eq!(reported.misattributed, 0);
        // `half` serves the root and child 0 and fails children 1 and 2,
        // which `full` serves.
        assert_eq!(reported.failed, vec![half.id; 2]);
        assert_eq!(reported.completed, 4);
        assert_eq!(
            reported.bytes_from(half.id),
            root_len + children[0].len() as u64
        );
        assert_eq!(
            reported.bytes_from(full.id),
            (children[1].len() + children[2].len()) as u64
        );
        assert_eq!(reported.bytes, expected_bytes);
        Ok(())
    }

    /// A provider whose blobs handler lets the first `allow` chunk groups
    /// through and then holds every further one, with the connection alive.
    async fn throttled_node(
        data: &[u8],
        allow: usize,
    ) -> TestResult<(iroh::protocol::Router, Hash, tokio::task::JoinHandle<()>)> {
        use crate::provider::events::{EventMask, EventSender, ProviderMessage, ThrottleMode};
        let store = MemStore::new();
        let tt = store.add_slice(data).await?;
        let (events, mut messages) = EventSender::channel(
            8,
            EventMask {
                throttle: ThrottleMode::Intercept,
                ..EventMask::DEFAULT
            },
        );
        let gate = tokio::spawn(async move {
            let mut held = Vec::new();
            let mut allowed = 0;
            while let Some(message) = messages.recv().await {
                if let ProviderMessage::Throttle(message) = message {
                    if allowed < allow {
                        allowed += 1;
                        message.tx.send(Ok(())).await.ok();
                    } else {
                        held.push(message);
                    }
                }
            }
        });
        let ep = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let router = iroh::protocol::Router::builder(ep)
            .accept(crate::ALPN, BlobsProtocol::new(&store, Some(events)))
            .spawn();
        Ok((router, tt.hash, gate))
    }

    /// A download dropped while its attempt is mid-transfer never reports the
    /// attempt's `BytesTransferred`; the live counters handed out with
    /// `TryProvider` still hold what it received.
    #[tokio::test]
    async fn downloader_attempt_stats_survive_a_cancelled_download() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let data = vec![42u8; 1024 * 1024];
        let (stall, hash, gate) = throttled_node(&data, 4).await?;
        sp3.add_endpoint_info(stall.endpoint().addr());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let mut stream = swarm
            .download(hash, vec![stall.endpoint().id()])
            .stream()
            .await?;
        let stats = loop {
            match tokio::time::timeout(BOUND, stream.next()).await? {
                Some(DownloadProgressItem::TryProvider { stats, .. }) => break stats,
                Some(_) => {}
                None => panic!("the download ended"),
            }
        };
        tokio::time::timeout(BOUND, async {
            let mut last = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let now = stats.payload_bytes();
                if now > 0 && now == last {
                    return;
                }
                last = now;
            }
        })
        .await?;
        let mut ended = false;
        while let Ok(item) =
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await
        {
            match item {
                Some(DownloadProgressItem::BytesTransferred { .. }) | None => ended = true,
                Some(_) => {}
            }
            if ended {
                break;
            }
        }
        assert!(!ended, "the attempt ended instead of stalling");
        drop(stream);

        let payload = stats.payload_bytes();
        assert!(payload > 0 && payload < data.len() as u64, "{payload}");
        assert_eq!(stats.wire_bytes(), None);
        // What was decoded, of which the store may not have everything yet.
        let local = store3
            .remote()
            .local(HashAndFormat::raw(hash))
            .await?
            .local_bytes();
        assert!(local <= payload, "live count {payload}, stored {local}");
        gate.abort();
        Ok(())
    }

    /// The first provider dies after delivering part of the blob; the next one
    /// delivers the rest. Both are credited with their own bytes, which add up
    /// to the blob.
    #[tokio::test]
    async fn downloader_credits_a_provider_that_dies_mid_transfer() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let data = (0..1024 * 1024)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        store1.add_slice(data.clone()).await?;
        let (dying, hash, gate) = throttled_node(&data, 4).await?;
        let holder = r1.endpoint().addr();
        sp3.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(dying.endpoint().addr());
        let dying_id = dying.endpoint().id();
        let swarm = Downloader::new(&store3, r3.endpoint());

        let mut stream = swarm
            .download(hash, vec![dying_id, holder.id])
            .stream()
            .await?;
        let killer = {
            let dying = dying.clone();
            let store3 = store3.clone();
            tokio::spawn(async move {
                loop {
                    let held = store3
                        .remote()
                        .local(HashAndFormat::raw(hash))
                        .await
                        .map(|l| l.local_bytes())
                        .unwrap_or(0);
                    if held > 0 {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                dying.shutdown().await.ok();
            })
        };
        let reported = tokio::time::timeout(BOUND, collect(&mut stream)).await?;
        killer.await?;
        gate.abort();

        assert_eq!(reported.errors, 0);
        assert_eq!(reported.misattributed, 0);
        assert_eq!(reported.tried, vec![dying_id, holder.id]);
        assert_eq!(reported.failed, vec![dying_id]);
        let (from_dying, from_holder) = (
            reported.bytes_from(dying_id),
            reported.bytes_from(holder.id),
        );
        assert!(from_dying > 0 && from_holder > 0, "{reported:?}");
        assert_eq!(from_dying + from_holder, data.len() as u64);
        assert_eq!(reported.bytes, data.len() as u64);
        assert_eq!(store3.get_bytes(hash).await?.deref(), data.as_slice());
        Ok(())
    }

    /// Failover across an ordered provider list is reported per request: the
    /// provider that cannot serve is marked failed, the next one completes, and
    /// the byte total still covers the content exactly once.
    #[tokio::test]
    async fn downloader_split_reports_failover() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r2, _store2, _, _) = node_test_setup_fs(testdir.path().join("empty")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let children = vec![vec![7u8; 8192], vec![9u8; 8192]];
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        let holder = r1.endpoint().addr();
        let empty = r2.endpoint().addr();
        sp3.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(empty.clone());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    GetRequest::all(root),
                    [empty.id, holder.id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        // Root + 2 children, each walking the list from the top.
        assert_eq!(reported.failed, vec![empty.id; 3]);
        assert_eq!(reported.completed, 3);
        assert_eq!(reported.bytes, expected_bytes);
        Ok(())
    }

    /// A provider that accepts a request and never answers, holding the
    /// connection open the way a stuck peer does: QUIC keep-alive means the
    /// connection itself never times out.
    #[derive(Debug, Clone)]
    struct StallingProvider;

    impl iroh::protocol::ProtocolHandler for StallingProvider {
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

    async fn stalling_node() -> TestResult<iroh::protocol::Router> {
        let ep = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Default)
            .bind()
            .await?;
        Ok(iroh::protocol::Router::builder(ep)
            .accept(crate::ALPN, StallingProvider)
            .spawn())
    }

    const IDLE: std::time::Duration = std::time::Duration::from_millis(500);
    /// Generous bound on a whole download that should fail over promptly.
    const BOUND: std::time::Duration = std::time::Duration::from_secs(30);

    /// The default keeps upstream behaviour: a stalled provider at the head of
    /// the list blocks the providers behind it.
    #[tokio::test]
    async fn downloader_waits_on_a_stalled_provider_by_default() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let stall = stalling_node().await?;
        let tt = store1.add_slice(vec![42u8; 64 * 1024]).await?;
        let holder = r1.endpoint().addr();
        let stalled = stall.endpoint().addr();
        sp3.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(stalled.clone());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let stream = swarm
            .download_with_opts(DownloadOptions::new(
                tt.hash,
                [stalled.id, holder.id],
                SplitStrategy::None,
            ))
            .stream()
            .await?;
        let res = tokio::time::timeout(IDLE * 6, collect(stream)).await;
        assert!(res.is_err(), "download finished despite a stalled lead");
        assert!(!store3.has(tt.hash).await?);
        Ok(())
    }

    /// With an idle timeout, the stalled provider is reported failed and the
    /// next one serves the whole blob.
    #[tokio::test]
    async fn downloader_idle_timeout_fails_over_from_a_stalled_provider() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let stall = stalling_node().await?;
        let data = vec![42u8; 1024 * 1024];
        let tt = store1.add_slice(data.clone()).await?;
        let holder = r1.endpoint().addr();
        let stalled = stall.endpoint().addr();
        sp3.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(stalled.clone());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let stream = swarm
            .download_with_opts(
                DownloadOptions::new(tt.hash, [stalled.id, holder.id], SplitStrategy::None)
                    .with_provider_idle_timeout(IDLE),
            )
            .stream()
            .await?;
        let reported = tokio::time::timeout(BOUND, collect(stream)).await?;

        assert_eq!(reported.errors, 0);
        assert_eq!(reported.tried, vec![stalled.id, holder.id]);
        assert_eq!(reported.failed, vec![stalled.id]);
        assert_eq!(reported.completed, 1);
        assert_eq!(reported.bytes, data.len() as u64);
        assert_eq!(store3.get_bytes(tt.hash).await?.deref(), data.as_slice());
        Ok(())
    }

    /// A consumer that stops taking progress is local work, not a stalled
    /// provider: the attempt waits for it instead of failing the provider.
    #[tokio::test]
    async fn downloader_idle_timeout_waits_for_a_slow_progress_consumer() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let data = vec![42u8; 1024 * 1024];
        let tt = store1.add_slice(data.clone()).await?;
        let holder = r1.endpoint().addr();
        sp3.add_endpoint_info(holder.clone());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let mut stream = swarm
            .download_with_opts(
                DownloadOptions::new(tt.hash, [holder.id], SplitStrategy::None)
                    .with_provider_idle_timeout(IDLE),
            )
            .stream()
            .await?;
        // Take items up to the first payload progress, then stop reading for
        // well over the idle timeout while the provider keeps sending.
        let mut head = Vec::new();
        loop {
            let item = stream.next().await.ok_or("download ended early")?;
            let payload = matches!(item, DownloadProgressItem::Progress(_));
            head.push(item);
            if payload {
                break;
            }
        }
        tokio::time::sleep(IDLE * 4).await;
        let rest = n0_future::stream::iter(head).chain(stream);
        let reported = tokio::time::timeout(BOUND, collect(rest)).await?;

        assert_eq!(reported.errors, 0);
        assert_eq!(reported.tried, vec![holder.id]);
        assert!(reported.failed.is_empty(), "failed {:?}", reported.failed);
        assert_eq!(reported.completed, 1);
        assert_eq!(reported.bytes, data.len() as u64);
        assert_eq!(store3.get_bytes(tt.hash).await?.deref(), data.as_slice());
        Ok(())
    }

    /// Local work stops the stall clock, however long it takes, and the clock
    /// restarts from where the work ended.
    #[tokio::test]
    async fn stall_clock_stops_during_local_work() {
        const IDLE: std::time::Duration = std::time::Duration::from_millis(100);
        let clock = super::StallClock::new();
        let local = clock.local();
        let held = tokio::time::timeout(IDLE * 3, super::stalled(&clock, IDLE)).await;
        assert!(held.is_err(), "stalled while waiting on local work");
        let released = n0_future::time::Instant::now();
        drop(local);
        super::stalled(&clock, IDLE).await;
        // The clock keeps milliseconds, so allow for one lost to rounding.
        assert!(released.elapsed() + std::time::Duration::from_millis(1) >= IDLE);
    }

    /// Split mode threads the timeout through the root fetch and every child.
    #[tokio::test]
    async fn downloader_split_idle_timeout_fails_over_from_a_stalled_provider() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let stall = stalling_node().await?;
        let children = vec![vec![7u8; 8192], vec![9u8; 8192]];
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        let holder = r1.endpoint().addr();
        let stalled = stall.endpoint().addr();
        sp3.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(stalled.clone());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let stream = swarm
            .download_with_opts(
                DownloadOptions::new(
                    GetRequest::all(root),
                    [stalled.id, holder.id],
                    SplitStrategy::Split,
                )
                .with_provider_idle_timeout(IDLE),
            )
            .stream()
            .await?;
        let reported = tokio::time::timeout(BOUND, collect(stream)).await?;

        assert_eq!(reported.errors, 0);
        // Root + 2 children, each walking the list from the top.
        assert_eq!(reported.failed, vec![stalled.id; 3]);
        assert_eq!(reported.completed, 3);
        assert_eq!(reported.bytes, expected_bytes);
        Ok(())
    }

    /// Blob payloads served by [`restartable_provider`].
    fn restart_blobs() -> [Vec<u8>; 2] {
        [vec![1u8; 64 * 1024], vec![2u8; 64 * 1024]]
    }

    /// A loopback-only endpoint with relays off, like a restart probe.
    fn loopback_endpoint() -> TestResult<iroh::endpoint::Builder> {
        Ok(Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?)
    }

    /// Serve [`restart_blobs`] as `key` on a fresh loopback port.
    async fn restartable_provider(
        key: iroh::SecretKey,
    ) -> TestResult<(iroh::EndpointAddr, Router)> {
        let store = MemStore::new();
        for blob in restart_blobs() {
            store.add_slice(blob).await?;
        }
        let ep = loopback_endpoint()?.secret_key(key).bind().await?;
        let addr = iroh::EndpointAddr::new(ep.id()).with_ip_addr(ep.bound_sockets()[0]);
        let blobs = BlobsProtocol::new(&store, None);
        let router = Router::builder(ep).accept(crate::ALPN, blobs).spawn();
        // The store lives as long as the router's protocol handler.
        Ok((addr, router))
    }

    /// A provider that dies without CONNECTION_CLOSE and comes back with the
    /// same key on a new port. Nothing answers the old port, so the pooled
    /// connection stays open until QUIC's 30 s idle timeout. The next fetch
    /// must find the connection lost, retire it, and fetch over a fresh one
    /// within a few seconds, not stall for the provider idle timeout.
    #[tokio::test]
    async fn downloader_recovers_from_a_provider_restarted_on_a_new_port() -> TestResult<()> {
        use crate::util::connection_pool::Options;
        /// Aster's provider idle timeout.
        const PROVIDER_IDLE: std::time::Duration = std::time::Duration::from_secs(10);
        const PROMPTLY: std::time::Duration = std::time::Duration::from_secs(5);

        let key = iroh::SecretKey::generate();
        let [blob_a, blob_b] = restart_blobs().map(|b| Hash::new(&b));

        // The old provider runs in its own runtime, so it can vanish without
        // sending CONNECTION_CLOSE: shutting the runtime down drops every task
        // without polling it again, like kill -9.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
        let old = {
            let key = key.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("runtime");
                let router = rt.block_on(async move {
                    let (addr, router) = restartable_provider(key).await.expect("provider");
                    ready_tx.send(addr).expect("ready");
                    kill_rx.await.ok();
                    router
                });
                rt.shutdown_timeout(std::time::Duration::from_secs(2));
                drop(router);
            })
        };
        let old_addr = ready_rx.await?;

        let lookup = iroh::address_lookup::MemoryLookup::new();
        lookup.add_endpoint_info(old_addr.clone());
        let ep = loopback_endpoint()?
            .address_lookup(lookup.clone())
            .bind()
            .await?;
        let store = MemStore::new();
        let swarm = Downloader::builder(&store, &ep)
            .pool_options(Options {
                idle_timeout: std::time::Duration::from_secs(120),
                ..Default::default()
            })
            .build();
        let fetch = |hash: Hash| {
            swarm.download_with_opts(
                DownloadOptions::new(hash, [old_addr.id], SplitStrategy::None)
                    .with_provider_idle_timeout(PROVIDER_IDLE),
            )
        };

        // Fetch one blob, which leaves a pooled connection to the provider.
        let first = collect(fetch(blob_a).stream().await?).await;
        assert_eq!(first.errors, 0, "first fetch: {first:?}");
        assert!(store.has(blob_a).await?);

        kill_tx.send(()).ok();
        tokio::task::spawn_blocking(move || old.join().expect("old provider")).await?;

        // Same key, new port.
        let (new_addr, _new) = restartable_provider(key).await?;
        assert_ne!(new_addr, old_addr, "restarted on the same port");
        lookup.add_endpoint_info(new_addr);

        let start = std::time::Instant::now();
        let stream = fetch(blob_b).stream().await?;
        let second = tokio::time::timeout(PROVIDER_IDLE * 3, collect(stream)).await?;
        let elapsed = start.elapsed();
        println!("fetch after restart: {elapsed:?} {second:?}");
        assert_eq!(second.errors, 0, "fetch after restart failed: {second:?}");
        assert!(store.has(blob_b).await?);
        assert!(
            elapsed < PROMPTLY,
            "fetch after restart took {elapsed:?}, waiting on the dead pooled connection"
        );
        // One try on the dead pooled connection, one on a fresh one.
        assert_eq!(second.tried, vec![old_addr.id; 2]);
        assert_eq!(second.failed, vec![old_addr.id]);

        // The fresh connection is pooled and reused.
        let start = std::time::Instant::now();
        let third = collect(fetch(blob_a).stream().await?).await;
        assert_eq!(third.errors, 0);
        assert!(start.elapsed() < PROMPTLY);
        Ok(())
    }
}
