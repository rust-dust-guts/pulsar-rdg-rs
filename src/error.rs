//! Error types
use std::{
    fmt, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use crate::{message::proto::ServerError, producer::SendFuture};

#[derive(Debug)]
pub enum Error {
    Connection(ConnectionError),
    Consumer(ConsumerError),
    Producer(ProducerError),
    ServiceDiscovery(ServiceDiscoveryError),
    Authentication(AuthenticationError),
    Custom(String),
    Executor,
    #[cfg(feature = "admin-api")]
    Admin(AdminError),
}

impl From<ConnectionError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        Error::Connection(err)
    }
}

impl From<ConsumerError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConsumerError) -> Self {
        Error::Consumer(err)
    }
}

impl From<ProducerError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ProducerError) -> Self {
        Error::Producer(err)
    }
}

impl From<ServiceDiscoveryError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ServiceDiscoveryError) -> Self {
        Error::ServiceDiscovery(err)
    }
}

impl fmt::Display for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Connection(e) => write!(f, "Connection error: {e}"),
            Error::Consumer(e) => write!(f, "consumer error: {e}"),
            Error::Producer(e) => write!(f, "producer error: {e}"),
            Error::ServiceDiscovery(e) => write!(f, "service discovery error: {e}"),
            Error::Authentication(e) => write!(f, "authentication error: {e}"),
            Error::Custom(e) => write!(f, "error: {e}"),
            Error::Executor => write!(f, "could not spawn task"),
            #[cfg(feature = "admin-api")]
            Error::Admin(e) => write!(f, "admin error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Connection(e) => e.source(),
            Error::Consumer(e) => e.source(),
            Error::Producer(e) => e.source(),
            Error::ServiceDiscovery(e) => e.source(),
            Error::Authentication(e) => e.source(),
            Error::Custom(_) => None,
            Error::Executor => None,
            #[cfg(feature = "admin-api")]
            Error::Admin(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),
    SlowDown,
    Disconnected,
    PulsarError(Option<crate::message::proto::ServerError>, Option<String>),
    Unexpected(String),
    /// The broker lacks a protocol capability the requested operation needs.
    ///
    /// Not retriable: retrying against the same broker version cannot succeed.
    NotSupported(String),
    Decoding(String),
    Encoding(String),
    SocketAddr(String),
    UnexpectedResponse(String),
    #[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
    Tls(native_tls::Error),
    #[cfg(all(
        any(
            feature = "tokio-rustls-runtime-aws-lc-rs",
            feature = "tokio-rustls-runtime-ring",
            feature = "async-std-rustls-runtime-aws-lc-rs",
            feature = "async-std-rustls-runtime-ring",
        ),
        not(any(feature = "tokio-runtime", feature = "async-std-runtime"))
    ))]
    Tls(rustls::Error),
    #[cfg(any(
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring",
    ))]
    DnsName(rustls::pki_types::InvalidDnsNameError),
    Authentication(AuthenticationError),
    NotFound,
    Canceled,
    Shutdown,
}

impl ConnectionError {
    pub fn establish_retryable(&self) -> bool {
        match self {
            ConnectionError::Io(e) => {
                e.kind() == io::ErrorKind::ConnectionRefused || e.kind() == io::ErrorKind::TimedOut
            }
            ConnectionError::Authentication(AuthenticationError::Retriable(_)) => true,
            _ => false,
        }
    }
}

impl From<io::Error> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

#[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
impl From<native_tls::Error> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: native_tls::Error) -> Self {
        ConnectionError::Tls(err)
    }
}

#[cfg(all(
    any(
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring",
    ),
    not(any(feature = "tokio-runtime", feature = "async-std-runtime"))
))]
impl From<rustls::Error> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: rustls::Error) -> Self {
        ConnectionError::Tls(err)
    }
}

#[cfg(any(
    feature = "tokio-rustls-runtime-aws-lc-rs",
    feature = "tokio-rustls-runtime-ring",
    feature = "async-std-rustls-runtime-aws-lc-rs",
    feature = "async-std-rustls-runtime-ring",
))]
impl From<rustls::pki_types::InvalidDnsNameError> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: rustls::pki_types::InvalidDnsNameError) -> Self {
        ConnectionError::DnsName(err)
    }
}

impl From<AuthenticationError> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: AuthenticationError) -> Self {
        ConnectionError::Authentication(err)
    }
}

impl<T> From<async_channel::SendError<T>> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(_err: async_channel::SendError<T>) -> Self {
        ConnectionError::Disconnected
    }
}

impl<T> From<async_channel::TrySendError<T>> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: async_channel::TrySendError<T>) -> Self {
        match err {
            async_channel::TrySendError::Full(_) => ConnectionError::SlowDown,
            async_channel::TrySendError::Closed(_) => ConnectionError::Disconnected,
        }
    }
}

impl fmt::Display for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "{e}"),
            ConnectionError::SlowDown => write!(f, "SlowDown"),
            ConnectionError::Disconnected => write!(f, "Disconnected"),
            ConnectionError::PulsarError(e, s) => {
                write!(f, "Server error ({:?}): {}", e, s.as_deref().unwrap_or(""))
            }
            ConnectionError::Unexpected(e) => write!(f, "{e}"),
            ConnectionError::NotSupported(e) => write!(f, "not supported by the broker: {e}"),
            ConnectionError::Decoding(e) => write!(f, "Error decoding message: {e}"),
            ConnectionError::Encoding(e) => write!(f, "Error encoding message: {e}"),
            ConnectionError::SocketAddr(e) => write!(f, "Error obtaining socket address: {e}"),
            ConnectionError::Tls(e) => write!(f, "Error connecting TLS stream: {e}"),
            #[cfg(any(
                feature = "tokio-rustls-runtime-aws-lc-rs",
                feature = "tokio-rustls-runtime-ring",
                feature = "async-std-rustls-runtime-aws-lc-rs",
                feature = "async-std-rustls-runtime-ring",
            ))]
            ConnectionError::DnsName(e) => write!(f, "Error resolving hostname: {e}"),
            ConnectionError::Authentication(e) => write!(f, "Authentication error: {e}"),
            ConnectionError::UnexpectedResponse(e) => {
                write!(f, "Unexpected response from pulsar: {e}")
            }
            ConnectionError::NotFound => write!(f, "error looking up URL"),
            ConnectionError::Canceled => write!(f, "canceled request"),
            ConnectionError::Shutdown => write!(f, "The connection was shut down"),
        }
    }
}

impl std::error::Error for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectionError::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ConsumerError {
    Connection(ConnectionError),
    MissingPayload(String),
    Io(io::Error),
    ChannelFull,
    Closed,
    BuildError,
    /// The broker delivered a chunk of a chunked message.
    ///
    /// Chunk reassembly is not implemented. Returning the chunk to the
    /// application would hand it a truncated payload that looks like a complete
    /// message, so the message is rejected instead. Republish the data below the
    /// broker's `maxMessageSize`, or consume it with a client that supports
    /// chunking.
    UnsupportedChunkedMessage {
        /// `MessageMetadata.uuid` grouping the chunks, when the broker sent one.
        uuid: Option<String>,
        /// Total number of chunks the original message was split into.
        num_chunks: i32,
    },
}

impl From<ConnectionError> for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        ConsumerError::Connection(err)
    }
}

impl From<io::Error> for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: io::Error) -> Self {
        ConsumerError::Io(err)
    }
}

impl From<futures::channel::mpsc::SendError> for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: futures::channel::mpsc::SendError) -> Self {
        if err.is_full() {
            ConsumerError::ChannelFull
        } else {
            ConsumerError::Closed
        }
    }
}

impl fmt::Display for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConsumerError::Connection(e) => write!(f, "Connection error: {e}"),
            ConsumerError::MissingPayload(s) => write!(f, "Missing payload: {s}"),
            ConsumerError::Io(s) => write!(f, "Decompression error: {s}"),
            ConsumerError::ChannelFull => write!(
                f,
                "cannot send message to the consumer engine: the channel is full"
            ),
            ConsumerError::Closed => write!(
                f,
                "cannot send message to the consumer engine: the channel is closed"
            ),
            ConsumerError::BuildError => write!(f, "Error while building the consumer."),
            ConsumerError::UnsupportedChunkedMessage { uuid, num_chunks } => write!(
                f,
                "received chunk of a {num_chunks}-chunk message (uuid = {}), but chunked \
                 message reassembly is not supported by this client",
                uuid.as_deref().unwrap_or("<none>")
            ),
        }
    }
}

impl std::error::Error for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConsumerError::Connection(e) => Some(e),
            _ => None,
        }
    }
}

pub enum ProducerError {
    Connection(ConnectionError),
    Custom(String),
    Io(io::Error),
    PartialSend(Vec<Result<SendFuture, Error>>),
    /// Indiciates the error was part of sending a batch, and thus shared across the batch
    Batch(Arc<Error>),
    /// Indicates this producer has lost exclusive access to the topic. Client can decided whether
    /// to recreate or not
    Fenced,
    /// Indicates the producer is closed or dropped
    Closed,
}

impl From<ConnectionError> for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        ProducerError::Connection(err)
    }
}

impl From<io::Error> for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: io::Error) -> Self {
        ProducerError::Io(err)
    }
}

impl fmt::Display for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProducerError::Connection(e) => write!(f, "Connection error: {e}"),
            ProducerError::Io(e) => write!(f, "Compression error: {e}"),
            ProducerError::Custom(s) => write!(f, "Custom error: {s}"),
            ProducerError::Batch(e) => write!(f, "Batch error: {e}"),
            ProducerError::PartialSend(e) => {
                let (successes, failures) = e.iter().fold((0, 0), |(s, f), r| match r {
                    Ok(_) => (s + 1, f),
                    Err(_) => (s, f + 1),
                });
                write!(
                    f,
                    "Partial send error - {successes} successful, {failures} failed"
                )?;

                if failures > 0 {
                    let first_error = e
                        .iter()
                        .find(|r| r.is_err())
                        .unwrap()
                        .as_ref()
                        .map(drop)
                        .unwrap_err();
                    write!(f, "first error: {first_error}")?;
                }
                Ok(())
            }
            ProducerError::Fenced => write!(f, "Producer is fenced"),
            ProducerError::Closed => write!(f, "Producer is closed or dropped"),
        }
    }
}

impl fmt::Debug for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProducerError::Connection(e) => write!(f, "Connection({e:?})"),
            ProducerError::Custom(msg) => write!(f, "Custom({msg:?})"),
            ProducerError::Io(e) => write!(f, "Connection({e:?})"),
            ProducerError::Batch(e) => write!(f, "Connection({e:?})"),
            ProducerError::PartialSend(parts) => {
                write!(f, "PartialSend(")?;
                for (i, part) in parts.iter().enumerate() {
                    match part {
                        Ok(_) => write!(f, "Ok(SendFuture)")?,
                        Err(e) => write!(f, "Err({e:?})")?,
                    }
                    if i < (parts.len() - 1) {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            ProducerError::Fenced => write!(f, "Producer is fenced"),
            ProducerError::Closed => write!(f, "Producer is closed or dropped"),
        }
    }
}

impl std::error::Error for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProducerError::Connection(e) => Some(e),
            ProducerError::Io(e) => Some(e),
            ProducerError::Batch(e) => Some(e.as_ref()),
            ProducerError::PartialSend(parts) => parts
                .iter()
                .find(|r| r.is_err())
                .map(|r| r.as_ref().map(drop).unwrap_err() as _),
            ProducerError::Custom(_) => None,
            ProducerError::Fenced => None,
            ProducerError::Closed => None,
        }
    }
}

#[derive(Debug)]
pub enum ServiceDiscoveryError {
    Connection(ConnectionError),
    Query(Option<crate::message::proto::ServerError>, Option<String>),
    NotFound,
    DnsLookupError,
    Canceled,
    Shutdown,
    Dummy,
}

impl From<ConnectionError> for ServiceDiscoveryError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        ServiceDiscoveryError::Connection(err)
    }
}

impl fmt::Display for ServiceDiscoveryError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServiceDiscoveryError::Connection(e) => write!(f, "Connection error: {e}"),
            ServiceDiscoveryError::Query(e, s) => {
                write!(f, "Query error ({:?}): {}", e, s.as_deref().unwrap_or(""))
            }
            ServiceDiscoveryError::NotFound => write!(f, "cannot find topic"),
            ServiceDiscoveryError::DnsLookupError => write!(f, "cannot lookup broker address"),
            ServiceDiscoveryError::Canceled => write!(f, "canceled request"),
            ServiceDiscoveryError::Shutdown => write!(f, "service discovery engine not responding"),
            ServiceDiscoveryError::Dummy => write!(f, "placeholder error"),
        }
    }
}

impl std::error::Error for ServiceDiscoveryError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ServiceDiscoveryError::Connection(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum AuthenticationError {
    Custom(String),
    Retriable(String),
}

impl fmt::Display for AuthenticationError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationError::Custom(m) => write!(f, "{m}"),
            AuthenticationError::Retriable(m) => write!(f, "{m} (retriable)"),
        }
    }
}

impl std::error::Error for AuthenticationError {}

#[cfg(feature = "admin-api")]
#[derive(Debug)]
pub enum AdminError {
    /// The HTTP request to the Pulsar admin API failed
    Request(reqwest::Error),
    /// The requested tenant, namespace, topic, cluster or policy does not exist
    /// (HTTP 404).
    NotFound(String),
    /// The resource already exists, or is in a state that conflicts with the
    /// request (HTTP 409).
    Conflict(String),
    /// The request was rejected as malformed or semantically invalid
    /// (HTTP 400/422).
    BadRequest(String),
    /// Authentication failed or the role lacks permission (HTTP 401/403).
    NotAuthorized(String),
    /// A precondition on the request was not met (HTTP 412).
    PreconditionFailed(String),
    /// The operation is not permitted on this resource (HTTP 405).
    NotAllowed(String),
    /// The broker does not implement this operation (HTTP 501).
    NotSupported(String),
    /// The broker is up but cannot serve the request yet (HTTP 503).
    ServerUnavailable(String),
    /// The Pulsar admin API returned a non-2xx status not covered above
    Http { status: u16, body: String },
    /// A successful response body could not be deserialized
    Decode(String),
    /// The Pulsar admin API returned schema JSON this client could not parse
    SchemaDecode(String),
    /// The Pulsar admin API returned an unknown schema type
    InvalidSchemaType(String),
    /// The topic string could not be parsed
    InvalidTopic(String),
    /// TLS configuration failed (e.g. certificate chain could not be parsed)
    TlsConfig(String),
}

#[cfg(feature = "admin-api")]
impl AdminError {
    /// Maps an HTTP status and response body onto the most specific variant.
    ///
    /// Pulsar reports failures as `{"reason": "..."}`; the reason is extracted so
    /// the error message is the broker's own explanation rather than raw JSON.
    pub(crate) fn from_status(status: u16, body: String) -> Self {
        let reason = Self::reason(&body);
        match status {
            400 | 422 => AdminError::BadRequest(reason),
            401 | 403 => AdminError::NotAuthorized(reason),
            404 => AdminError::NotFound(reason),
            405 => AdminError::NotAllowed(reason),
            409 => AdminError::Conflict(reason),
            412 => AdminError::PreconditionFailed(reason),
            501 => AdminError::NotSupported(reason),
            503 => AdminError::ServerUnavailable(reason),
            _ => AdminError::Http { status, body },
        }
    }

    fn reason(body: &str) -> String {
        #[derive(serde::Deserialize)]
        struct Reason {
            reason: String,
        }
        match serde_json::from_str::<Reason>(body) {
            Ok(r) if !r.reason.is_empty() => r.reason,
            _ if body.trim().is_empty() => "<no message>".to_string(),
            _ => body.trim().to_string(),
        }
    }

    /// Whether retrying the same request could plausibly succeed.
    ///
    /// Only transient server-side conditions are retriable; a 4xx describes the
    /// request itself and will fail identically on retry.
    pub fn is_retriable(&self) -> bool {
        match self {
            AdminError::ServerUnavailable(_) => true,
            AdminError::Request(e) => e.is_timeout() || e.is_connect(),
            AdminError::Http { status, .. } => *status >= 500,
            _ => false,
        }
    }
}

#[cfg(feature = "admin-api")]
impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AdminError::Request(e) => write!(f, "HTTP request failed: {e}"),
            AdminError::NotFound(m) => write!(f, "not found: {m}"),
            AdminError::Conflict(m) => write!(f, "conflict: {m}"),
            AdminError::BadRequest(m) => write!(f, "bad request: {m}"),
            AdminError::NotAuthorized(m) => write!(f, "not authorized: {m}"),
            AdminError::PreconditionFailed(m) => write!(f, "precondition failed: {m}"),
            AdminError::NotAllowed(m) => write!(f, "not allowed: {m}"),
            AdminError::NotSupported(m) => write!(f, "not supported by the broker: {m}"),
            AdminError::ServerUnavailable(m) => write!(f, "broker unavailable: {m}"),
            AdminError::Decode(m) => write!(f, "could not decode admin response: {m}"),
            AdminError::Http { status, body } => {
                write!(f, "admin API returned HTTP {status}: {body}")
            }
            AdminError::SchemaDecode(msg) => write!(f, "failed to decode schema response: {msg}"),
            AdminError::InvalidSchemaType(schema_type) => {
                write!(
                    f,
                    "invalid schema type returned by admin API: {schema_type}"
                )
            }
            AdminError::InvalidTopic(t) => write!(f, "invalid topic URL: {t}"),
            AdminError::TlsConfig(msg) => write!(f, "TLS configuration error: {msg}"),
        }
    }
}

#[cfg(feature = "admin-api")]
impl std::error::Error for AdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AdminError::Request(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(feature = "admin-api")]
impl From<AdminError> for Error {
    fn from(err: AdminError) -> Self {
        Error::Admin(err)
    }
}

#[derive(Clone)]
pub(crate) struct SharedError {
    error_set: Arc<AtomicBool>,
    error: Arc<Mutex<Option<ConnectionError>>>,
}

impl SharedError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new() -> SharedError {
        SharedError {
            error_set: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn is_set(&self) -> bool {
        self.error_set.load(Ordering::Relaxed)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn remove(&self) -> Option<ConnectionError> {
        let mut lock = self.error.lock().unwrap();
        let error = lock.take();
        self.error_set.store(false, Ordering::Release);
        error
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn set(&self, error: ConnectionError) {
        let mut lock = self.error.lock().unwrap();
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
pub(crate) fn server_error(i: i32) -> Option<ServerError> {
    match i {
        0 => Some(ServerError::UnknownError),
        1 => Some(ServerError::MetadataError),
        2 => Some(ServerError::PersistenceError),
        3 => Some(ServerError::AuthenticationError),
        4 => Some(ServerError::AuthorizationError),
        5 => Some(ServerError::ConsumerBusy),
        6 => Some(ServerError::ServiceNotReady),
        7 => Some(ServerError::ProducerBlockedQuotaExceededError),
        8 => Some(ServerError::ProducerBlockedQuotaExceededException),
        9 => Some(ServerError::ChecksumError),
        10 => Some(ServerError::UnsupportedVersionError),
        11 => Some(ServerError::TopicNotFound),
        12 => Some(ServerError::SubscriptionNotFound),
        13 => Some(ServerError::ConsumerNotFound),
        14 => Some(ServerError::TooManyRequests),
        15 => Some(ServerError::TopicTerminatedError),
        16 => Some(ServerError::ProducerBusy),
        17 => Some(ServerError::InvalidTopicName),
        18 => Some(ServerError::IncompatibleSchema),
        19 => Some(ServerError::ConsumerAssignError),
        20 => Some(ServerError::TransactionCoordinatorNotFound),
        21 => Some(ServerError::InvalidTxnStatus),
        22 => Some(ServerError::NotAllowedError),
        23 => Some(ServerError::TransactionConflict),
        24 => Some(ServerError::TransactionNotFound),
        25 => Some(ServerError::ProducerFenced),
        _ => None,
    }
}

#[cfg(all(test, feature = "admin-api"))]
mod admin_error_tests {
    use super::AdminError;

    /// Every status the mapper names, plus the fallthrough.
    ///
    /// This taxonomy is what nearly every error-path assertion in the suite keys
    /// off, so a wrong mapping would quietly re-classify failures across the whole
    /// admin client.
    #[test]
    fn statuses_map_to_their_variants() {
        let body = |m: &str| format!(r#"{{"reason":"{m}"}}"#);
        /// A status and the predicate its mapped variant must satisfy.
        type Case = (u16, fn(&AdminError) -> bool);
        let cases: Vec<Case> = vec![
            (400, |e| matches!(e, AdminError::BadRequest(_))),
            (422, |e| matches!(e, AdminError::BadRequest(_))),
            (401, |e| matches!(e, AdminError::NotAuthorized(_))),
            (403, |e| matches!(e, AdminError::NotAuthorized(_))),
            (404, |e| matches!(e, AdminError::NotFound(_))),
            (405, |e| matches!(e, AdminError::NotAllowed(_))),
            (409, |e| matches!(e, AdminError::Conflict(_))),
            (412, |e| matches!(e, AdminError::PreconditionFailed(_))),
            (501, |e| matches!(e, AdminError::NotSupported(_))),
            (503, |e| matches!(e, AdminError::ServerUnavailable(_))),
            // Not named by the mapper, so they must fall through with the status
            // preserved rather than being forced into a neighbouring variant.
            (402, |e| matches!(e, AdminError::Http { status: 402, .. })),
            (415, |e| matches!(e, AdminError::Http { status: 415, .. })),
            (500, |e| matches!(e, AdminError::Http { status: 500, .. })),
            (502, |e| matches!(e, AdminError::Http { status: 502, .. })),
            (307, |e| matches!(e, AdminError::Http { status: 307, .. })),
        ];
        for (status, is_expected) in cases {
            let error = AdminError::from_status(status, body("boom"));
            assert!(
                is_expected(&error),
                "HTTP {status} mapped to the wrong variant: {error:?}"
            );
        }
    }

    /// The broker's own explanation is what surfaces, whatever the body looks like.
    #[test]
    fn the_reason_is_extracted_from_every_body_shape() {
        let cases = [
            (
                r#"{"reason":"Namespace does not exist"}"#,
                "Namespace does not exist",
            ),
            // Not JSON at all — Jetty's HTML page, which must survive verbatim so
            // route-miss detection can still see it.
            (
                "<html><title>Error 404 Not Found</title></html>",
                "<html><title>Error 404 Not Found</title></html>",
            ),
            // JSON without a `reason` key.
            (r#"{"other":"x"}"#, r#"{"other":"x"}"#),
            // Malformed JSON.
            ("{not json", "{not json"),
            // Empty, which must not produce an empty message.
            ("", "<no message>"),
            ("   ", "<no message>"),
            // Present but empty `reason` falls back to the raw body.
            (r#"{"reason":""}"#, r#"{"reason":""}"#),
        ];
        for (body, expected) in cases {
            let error = AdminError::from_status(404, body.to_string());
            let AdminError::NotFound(message) = &error else {
                panic!("expected NotFound for {body:?}, got {error:?}");
            };
            assert_eq!(message, expected, "wrong reason extracted from {body:?}");
        }
    }

    /// Only transient server-side conditions may be retried.
    #[test]
    fn only_transient_failures_are_retriable() {
        let body = || r#"{"reason":"x"}"#.to_string();
        assert!(AdminError::from_status(503, body()).is_retriable());
        assert!(AdminError::from_status(500, body()).is_retriable());
        assert!(AdminError::from_status(502, body()).is_retriable());

        // A 4xx describes the request, so it fails identically on retry.
        for status in [400, 401, 403, 404, 405, 409, 412, 422, 415] {
            assert!(
                !AdminError::from_status(status, body()).is_retriable(),
                "HTTP {status} must not be retriable"
            );
        }
        // 501 is a permanent statement about the broker, not a transient outage.
        assert!(!AdminError::from_status(501, body()).is_retriable());

        for error in [
            AdminError::Decode("x".into()),
            AdminError::InvalidTopic("x".into()),
            AdminError::TlsConfig("x".into()),
            AdminError::SchemaDecode("x".into()),
            AdminError::InvalidSchemaType("x".into()),
        ] {
            assert!(!error.is_retriable(), "{error:?} must not be retriable");
        }
    }
}
