//! Message publication
use std::{
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap, HashMap, HashSet, VecDeque},
    io::Write,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

use futures::{
    channel::{mpsc, oneshot},
    future::{self, try_join_all, Either},
    task::{Context, Poll},
    Future, SinkExt, StreamExt,
};
use rand::Rng;

use crate::{
    client::SerializeMessage,
    compression::Compression,
    connection::{Connection, SerialId},
    error::{ConnectionError, ProducerError},
    executor::Executor,
    message::{
        proto::{self, CommandSendReceipt, EncryptionKeys, Schema},
        BatchedMessage,
    },
    proto::CommandSuccess,
    retry_op::retry_create_producer,
    routing_policy::{HashingScheme, RoutingPolicy},
    BrokerAddress, Error, Pulsar,
};

type ProducerId = u64;
type ProducerName = String;

/// returned by [Producer::send]
///
/// it contains a channel on which we can await to get the message receipt.
/// Depending on the producer's configuration (batching, flow control, etc)and
/// the server's load, the send receipt could come much later after sending it
pub struct SendFuture(pub(crate) oneshot::Receiver<Result<CommandSendReceipt, Error>>);

impl Future for SendFuture {
    type Output = Result<CommandSendReceipt, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => Poll::Ready(Err(ProducerError::Custom(
                "producer unexpectedly disconnected".into(),
            )
            .into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A message's partition key.
///
/// Mirrors Java's `TypedMessageBuilder::key` / `keyBytes`, which are two spellings
/// of the same wire field: the key always travels as text in
/// `MessageMetadata.partition_key`, and a binary key is base64-encoded with
/// `partition_key_b64_encoded` set so the consumer can recover the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionKey {
    /// A text key, sent and hashed as-is.
    Text(String),
    /// A binary key.
    ///
    /// Sent base64-encoded, and **hashed in that encoded form** — Java's routers
    /// hash `msg.getKey()`, which for `keyBytes` is already the encoded string. A
    /// Rust producer that hashed the raw bytes would route the same key to a
    /// different partition than every other client.
    Bytes(Vec<u8>),
    /// An explicitly null key, as Java's `key(null)` / `keyBytes(null)`.
    ///
    /// Distinct from `partition_key: None`, which means no key was set at all: this
    /// sets `null_partition_key` on the wire, which a consumer can observe.
    Null,
}

impl PartitionKey {
    /// The text form the broker stores and the routers hash.
    ///
    /// `None` for [`PartitionKey::Null`], which carries no key to route on.
    pub fn routing_key(&self) -> Option<Cow<'_, str>> {
        match self {
            PartitionKey::Text(key) => Some(Cow::Borrowed(key)),
            PartitionKey::Bytes(key) => Some(Cow::Owned(BASE64.encode(key))),
            PartitionKey::Null => None,
        }
    }
}

impl PartitionKey {
    /// Rebuilds a key from the metadata of a message that was received.
    ///
    /// Used by the retry-letter and dead-letter paths, which re-publish a consumed
    /// message and must not silently turn a binary key back into text.
    pub(crate) fn from_metadata(
        key: Option<String>,
        b64_encoded: Option<bool>,
        null_key: Option<bool>,
    ) -> Option<Self> {
        match (key, b64_encoded) {
            // A key the producer sent as bytes; recover them. If it somehow is not
            // valid base64, keep the text rather than losing the key entirely.
            (Some(key), Some(true)) => Some(
                BASE64
                    .decode(key.as_bytes())
                    .map(PartitionKey::Bytes)
                    .unwrap_or(PartitionKey::Text(key)),
            ),
            (Some(key), _) => Some(PartitionKey::Text(key)),
            (None, _) if null_key.unwrap_or(false) => Some(PartitionKey::Null),
            (None, _) => None,
        }
    }
}

impl From<String> for PartitionKey {
    fn from(key: String) -> Self {
        PartitionKey::Text(key)
    }
}

impl From<&str> for PartitionKey {
    fn from(key: &str) -> Self {
        PartitionKey::Text(key.to_string())
    }
}

impl From<Vec<u8>> for PartitionKey {
    fn from(key: Vec<u8>) -> Self {
        PartitionKey::Bytes(key)
    }
}

/// message data that will be sent on a topic
///
/// generated from the [SerializeMessage] trait or [MessageBuilder]
///
/// this is actually a subset of the fields of a message, because batching,
/// compression and encryption should be handled by the producer
#[derive(Debug, Clone, Default)]
pub struct Message {
    /// Serialized data.
    ///
    /// `None` sends a protocol **null value**, which a Java consumer sees as a
    /// null rather than as an empty payload. `Some(vec![])` is an empty value,
    /// and the two are distinguishable on the wire.
    pub payload: Option<Vec<u8>>,
    /// user defined properties
    pub properties: HashMap<String, String>,
    /// Key deciding the message's partition. Text or binary; see [`PartitionKey`].
    pub partition_key: ::std::option::Option<PartitionKey>,
    /// key to decide partition for the message
    pub ordering_key: ::std::option::Option<Vec<u8>>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: ::std::option::Option<u64>,
    /// current version of the schema
    pub schema_version: ::std::option::Option<Vec<u8>>,
    /// UTC Unix timestamp in milliseconds, time at which the message should be
    /// delivered to consumers
    pub deliver_at_time: ::std::option::Option<i64>,
}

/// internal message type carrying options that must be defined
/// by the producer
#[derive(Debug, Clone, Default)]
pub(crate) struct ProducerMessage {
    pub payload: Vec<u8>,
    /// The value is absent, not empty. A null value travels as an empty payload
    /// plus this flag, so `payload` stays a plain `Vec<u8>` internally.
    pub null_value: ::std::option::Option<bool>,
    /// The key in `partition_key` is base64-encoded binary.
    pub partition_key_b64_encoded: ::std::option::Option<bool>,
    /// The key was explicitly set to null, as distinct from never set.
    pub null_partition_key: ::std::option::Option<bool>,
    pub properties: HashMap<String, String>,
    ///key to decide partition for the msg
    pub partition_key: ::std::option::Option<String>,
    ///key to decide partition for the msg
    pub ordering_key: ::std::option::Option<Vec<u8>>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    pub compression: ::std::option::Option<i32>,
    pub uncompressed_size: ::std::option::Option<u32>,
    /// Removed below checksum field from Metadata as
    /// it should be part of send-command which keeps checksum of header + payload
    ///optional sfixed64 checksum = 10;
    ///differentiate single and batch message metadata
    pub num_messages_in_batch: ::std::option::Option<i32>,
    pub event_time: ::std::option::Option<u64>,
    /// Contains encryption key name, encrypted key and metadata to describe the key
    pub encryption_keys: ::std::vec::Vec<EncryptionKeys>,
    /// Algorithm used to encrypt data key
    pub encryption_algo: ::std::option::Option<String>,
    /// Additional parameters required by encryption
    pub encryption_param: ::std::option::Option<Vec<u8>>,
    pub schema_version: ::std::option::Option<Vec<u8>>,
    /// UTC Unix timestamp in milliseconds, time at which the message should be
    /// delivered to consumers
    pub deliver_at_time: ::std::option::Option<i64>,
}

impl From<Message> for ProducerMessage {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(m: Message) -> Self {
        // The wire carries a null value as an empty payload plus a flag, and a
        // binary key as base64 text plus a flag — so both collapse here.
        let null_value = m.payload.is_none().then_some(true);
        let (partition_key, partition_key_b64_encoded, null_partition_key) = match m.partition_key {
            Some(PartitionKey::Text(key)) => (Some(key), None, None),
            Some(PartitionKey::Bytes(key)) => (Some(BASE64.encode(key)), Some(true), None),
            Some(PartitionKey::Null) => (None, None, Some(true)),
            None => (None, None, None),
        };
        ProducerMessage {
            payload: m.payload.unwrap_or_default(),
            null_value,
            partition_key,
            partition_key_b64_encoded,
            null_partition_key,
            properties: m.properties,
            ordering_key: m.ordering_key,
            replicate_to: m.replicate_to,
            event_time: m.event_time,
            schema_version: m.schema_version,
            deliver_at_time: m.deliver_at_time,
            ..Default::default()
        }
    }
}

/// Configuration options for producers
#[derive(Clone, Default)]
pub struct ProducerOptions {
    /// end to end message encryption (not implemented yet)
    pub encrypted: Option<bool>,
    /// user defined properties added to all messages
    pub metadata: BTreeMap<String, String>,
    /// schema used to encode this producer's messages
    pub schema: Option<Schema>,
    /// batch message size
    pub batch_size: Option<u32>,
    /// batch size in bytes treshold (only relevant when batch_size active).
    /// batch is sent when batch size in bytes is reached
    pub batch_byte_size: Option<usize>,
    /// the batch will be sent if this timeout is reached after the 1st message is added into the
    /// batch even if it does not reach the size or byte size limit.
    pub batch_timeout: Option<Duration>,
    /// algorithm used to compress the messages
    pub compression: Option<Compression>,
    /// producer access mode: shared = 0, exclusive = 1, waitforexclusive =2,
    /// exclusivewithoutfencing =3
    pub access_mode: Option<i32>,
    /// Whether to block if the internal pending queue, whose size is configured by
    /// [`crate::client::PulsarBuilder::with_outbound_channel_size`] is full, when awaiting
    /// [`Producer::send_non_blocking`]. (default: false)
    pub block_queue_if_full: bool,
    pub routing_policy: Option<RoutingPolicy>,
    /// hash function used to map a message's partition key to a partition
    ///
    /// Defaults to [`HashingScheme::JavaStringHash`], matching the Java client,
    /// so that the same key routes to the same partition across clients. Only
    /// change this if every other producer on the topic uses the same scheme.
    pub hashing_scheme: HashingScheme,
}

impl ProducerOptions {
    fn enabled_batching(&self) -> bool {
        match self.batch_size {
            Some(batch_size) => batch_size > 1,
            None => self.batch_byte_size.is_some() || self.batch_timeout.is_some(),
        }
    }
}

/// Wrapper structure that manges multiple producers at once, creating them as needed
/// ```rust,no_run
/// use pulsar::{Pulsar, TokioExecutor};
///
/// # async fn test() -> Result<(), pulsar::Error> {
/// # let addr = "pulsar://127.0.0.1:6650";
/// # let topic = "topic";
/// # let message = "data".to_owned();
/// let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
/// let mut producer = pulsar.producer().with_name("name").build_multi_topic();
/// let send_1 = producer.send_non_blocking(topic, &message).await?;
/// let send_2 = producer.send_non_blocking(topic, &message).await?;
/// send_1.await?;
/// send_2.await?;
/// # Ok(())
/// # }
/// ```
pub struct MultiTopicProducer<Exe: Executor> {
    client: Pulsar<Exe>,
    producers: BTreeMap<String, Producer<Exe>>,
    options: ProducerOptions,
    name: Option<String>,
}

impl<Exe: Executor> MultiTopicProducer<Exe> {
    /// producer options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ProducerOptions {
        &self.options
    }

    /// list topics currently handled by this producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topics(&self) -> Vec<String> {
        self.producers.keys().cloned().collect()
    }

    /// stops the producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close_producer<S: Into<String>>(&mut self, topic: S) -> Result<(), Error> {
        let partitions = self.client.lookup_partitioned_topic(topic).await?;
        for (topic, _) in partitions {
            self.producers.remove(&topic);
        }
        Ok(())
    }

    /// sends one message on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_non_blocking"]
    pub async fn send<T: SerializeMessage + Sized, S: Into<String>>(
        &mut self,
        topic: S,
        message: T,
    ) -> Result<SendFuture, Error> {
        let fut = self.send_non_blocking(topic, message).await?;
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(fut.await);
        Ok(SendFuture(rx))
    }

    /// sends one message on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_non_blocking<T: SerializeMessage + Sized, S: Into<String>>(
        &mut self,
        topic: S,
        message: T,
    ) -> Result<SendFuture, Error> {
        let message = T::serialize_message(message)?;
        let topic = topic.into();
        let producer = match self.producers.entry(topic) {
            Entry::Vacant(entry) => {
                let mut builder = self
                    .client
                    .producer()
                    .with_topic(entry.key())
                    .with_options(self.options.clone());
                if let Some(name) = &self.name {
                    builder = builder.with_name(name);
                }
                let producer = builder.build().await?;
                entry.insert(producer)
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };

        producer.send_non_blocking(message).await
    }

    /// sends a list of messages on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_all_non_blocking"]
    pub async fn send_all<'a, 'b, T, S, I>(
        &mut self,
        topic: S,
        messages: I,
    ) -> Result<Vec<SendFuture>, Error>
    where
        'b: 'a,
        T: 'b + SerializeMessage + Sized,
        I: IntoIterator<Item = T>,
        S: Into<String>,
    {
        let topic: String = topic.into();
        let mut futs = vec![];
        for message in messages {
            #[allow(deprecated)]
            let fut = self.send(&topic, message).await?;
            futs.push(fut);
        }
        Ok(futs)
    }

    /// sends a list of messages on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_all_non_blocking<'a, 'b, T, S, I>(
        &mut self,
        topic: S,
        messages: I,
    ) -> Result<Vec<SendFuture>, Error>
    where
        'b: 'a,
        T: 'b + SerializeMessage + Sized,
        I: IntoIterator<Item = T>,
        S: Into<String>,
    {
        let topic = topic.into();
        let mut sends = Vec::new();
        for msg in messages {
            sends.push(self.send_non_blocking(&topic, msg).await);
        }
        // TODO determine whether to keep this approach or go with the partial send, but more mem
        // friendly lazy approach. serialize all messages before sending to avoid a partial
        // send
        if sends.iter().all(|s| s.is_ok()) {
            Ok(sends.into_iter().map(|s| s.unwrap()).collect())
        } else {
            Err(ProducerError::PartialSend(sends).into())
        }
    }
}

/// a producer for a single topic
pub struct Producer<Exe: Executor> {
    inner: ProducerInner<Exe>,
}

impl<Exe: Executor> Producer<Exe> {
    /// creates a producer builder from a client instance
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn builder(pulsar: &Pulsar<Exe>) -> ProducerBuilder<Exe> {
        ProducerBuilder::new(pulsar)
    }

    /// this producer's topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> &str {
        match &self.inner {
            ProducerInner::Single(p) => p.topic(),
            ProducerInner::Partitioned(p) => &p.topic,
        }
    }

    /// list of partitions for this producer's topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn partitions(&self) -> Option<Vec<String>> {
        match &self.inner {
            ProducerInner::Single(_) => None,
            ProducerInner::Partitioned(p) => {
                Some(p.producers.iter().map(|p| p.topic().to_owned()).collect())
            }
        }
    }

    /// configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ProducerOptions {
        match &self.inner {
            ProducerInner::Single(p) => p.options(),
            ProducerInner::Partitioned(p) => &p.options,
        }
    }

    /// creates a message builder
    ///
    /// the created message will ber sent by this producer in [MessageBuilder::send]
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn create_message<'a>(&'a mut self) -> MessageBuilder<'a, (), Exe> {
        MessageBuilder::new(self)
    }

    /// test that the broker connections are still valid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&self) -> Result<(), Error> {
        match &self.inner {
            ProducerInner::Single(p) => p.check_connection().await,
            ProducerInner::Partitioned(p) => {
                try_join_all(p.producers.iter().map(|p| p.check_connection()))
                    .await
                    .map(drop)
            }
        }
    }

    /// Sends a message
    ///
    /// this function returns a `SendFuture` because the receipt can come long after
    /// this function was called, for various reasons:
    /// - the message was sent successfully but Pulsar did not send the receipt yet
    /// - the producer is batching messages, so this function must return immediately, and the
    ///   receipt will come when the batched messages are actually sent
    ///
    /// If [`ProducerOptions::block_queue_if_full`] is false (by default) and the internal pending
    /// queue is full, which means the send rate is too fast,
    /// [`crate::error::ConnectionError::SlowDown`] will be returned. You should handle the error
    /// like:
    ///
    /// ```rust,no_run
    /// use pulsar::error::{ConnectionError, Error, ProducerError};
    ///
    /// # async fn run(mut producer: pulsar::Producer<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// match producer.send_non_blocking("msg").await {
    ///     Ok(future) => { /* handle the send future */ }
    ///     Err(Error::Producer(ProducerError::Connection(ConnectionError::SlowDown))) => {
    ///         /* wait for a while and resent */
    ///     }
    ///     Err(e) => { /* handle other errors */ }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Usage:
    ///
    /// ```rust,no_run
    /// # async fn run(mut producer: pulsar::Producer<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let f1 = producer.send_non_blocking("hello").await?;
    /// let f2 = producer.send_non_blocking("world").await?;
    /// let receipt1 = f1.await?;
    /// let receipt2 = f2.await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_non_blocking<T: SerializeMessage + Sized>(
        &mut self,
        message: T,
    ) -> Result<SendFuture, Error> {
        let serialized_message = T::serialize_message(message)?;
        match &mut self.inner {
            ProducerInner::Single(p) => p.send(serialized_message).await,
            ProducerInner::Partitioned(p) => {
                p.refresh_partitions().await;
                p.choose_partition(&serialized_message)
                    .send(serialized_message)
                    .await
            }
        }
    }

    /// Sends a message
    ///
    /// this function is similar to send_non_blocking then waits the returned `SendFuture`
    /// for the receipt.
    ///
    /// It returns the returned receipt in another `SendFuture` to be backward compatible.
    ///
    /// It is deprecated, and users should instread use send_non_blocking. Users should await the
    /// returned `SendFuture` if blocking is needed.
    ///
    /// Usage:
    ///
    /// ```rust,no_run
    /// # async fn run(mut producer: pulsar::Producer<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let f1 = producer.send_non_blocking("hello").await?;
    /// let f2 = producer.send_non_blocking("world").await?;
    /// let receipt1 = f1.await?;
    /// let receipt2 = f2.await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_non_blocking"]
    pub async fn send<T: SerializeMessage + Sized>(
        &mut self,
        message: T,
    ) -> Result<SendFuture, Error> {
        let fut = self.send_non_blocking(message).await?;
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(fut.await);
        Ok(SendFuture(rx))
    }

    /// sends a list of messages
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_all<T, I>(&mut self, messages: I) -> Result<Vec<SendFuture>, Error>
    where
        T: SerializeMessage,
        I: IntoIterator<Item = T>,
    {
        if let ProducerInner::Partitioned(p) = &mut self.inner {
            p.refresh_partitions().await;
        }
        let mut sends = Vec::new();
        for message in messages {
            let serialized_message = T::serialize_message(message)?;
            let producer = match &mut self.inner {
                ProducerInner::Single(p) => p,
                ProducerInner::Partitioned(p) => p.choose_partition(&serialized_message),
            };

            sends.push(producer.send(serialized_message).await);
        }
        if sends.iter().all(|s| s.is_ok()) {
            Ok(sends.into_iter().map(|s| s.unwrap()).collect())
        } else {
            Err(ProducerError::PartialSend(sends).into())
        }
    }

    /// sends the current batch of messages
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_batch(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            ProducerInner::Single(p) => p.send_batch().await,
            ProducerInner::Partitioned(p) => {
                try_join_all(p.producers.iter_mut().map(|p| p.send_batch()))
                    .await
                    .map(drop)
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            ProducerInner::Single(producer) => producer.close().await,
            ProducerInner::Partitioned(p) => {
                try_join_all(p.producers.iter_mut().map(|p| p.close()))
                    .await
                    .map(drop)
            }
        }
    }
}

enum ProducerInner<Exe: Executor> {
    Single(TopicProducer<Exe>),
    Partitioned(PartitionedProducer<Exe>),
}

/// How often a producer re-checks its topic's partition count by default.
///
/// Matches Java's `autoUpdatePartitionsIntervalSeconds`.
const DEFAULT_PARTITION_REFRESH: Duration = Duration::from_secs(60);

/// Partition index encoded in a resolved partition name, e.g. `3` for
/// `persistent://public/default/orders-partition-3`.
///
/// Deliberately strict about the suffix: `orders-partition-archive` is an
/// ordinary topic, not partition "archive".
fn partition_index(topic: &str) -> Option<usize> {
    let (prefix, index) = topic.rsplit_once("-partition-")?;
    if prefix.is_empty() {
        return None;
    }
    index.parse().ok()
}

struct PartitionedProducer<Exe: Executor> {
    // Guaranteed to be non-empty
    producers: Vec<TopicProducer<Exe>>,
    last_used_producer_index: usize,
    topic: String,
    options: ProducerOptions,
    /// Everything a partition re-check needs, plus when the last one ran.
    ///
    /// `None` disables the check. Time is measured with [`Instant`] rather than
    /// [`Executor::interval`] on purpose: the two runtimes disagree on whether an
    /// interval fires immediately, and elapsed-since is the same under both.
    partition_refresh: Option<Duration>,
    last_partition_check: Instant,
    pulsar: Pulsar<Exe>,
    name: Option<String>,
}

impl<Exe: Executor> PartitionedProducer<Exe> {
    /// Starts producers for partitions added since the last check, at most once
    /// per configured interval.
    ///
    /// A partitioned topic can be grown at any time, and a producer that never
    /// re-checks keeps routing over the original set. The new partitions get no
    /// traffic, and — worse — a keyed message lands on a different partition than
    /// a client that did notice, so per-key ordering breaks across the fleet. That
    /// is the same failure `HashingScheme` exists to prevent.
    ///
    /// Failures are logged and swallowed. A partition re-check is background work
    /// that a send merely happens to drive, so a transient lookup error must not
    /// fail that send; Java takes the same view, its
    /// `partitionsAutoUpdateTimerTask` catching everything and rescheduling.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn refresh_partitions(&mut self) {
        match self.partition_refresh {
            Some(interval) if self.last_partition_check.elapsed() >= interval => {}
            _ => return,
        }
        self.last_partition_check = Instant::now();

        let found = match self.pulsar.lookup_partitioned_topic(&self.topic).await {
            Ok(found) => found,
            Err(e) => {
                warn!(
                    "could not re-check the partitions of {}, keeping the current {}: {}",
                    self.topic,
                    self.producers.len(),
                    e
                );
                return;
            }
        };

        // Match on name rather than on count: it needs no assumption about which
        // indices are new, and Pulsar only ever appends partitions.
        let known: HashSet<&str> = self.producers.iter().map(|p| p.topic()).collect();
        let added: Vec<_> = found
            .into_iter()
            .filter(|(topic, _)| !known.contains(topic.as_str()))
            .collect();
        if added.is_empty() {
            return;
        }

        let created = try_join_all(added.into_iter().map(|(topic, addr)| {
            TopicProducer::new(
                self.pulsar.clone(),
                addr,
                topic,
                self.name.clone(),
                self.options.clone(),
            )
        }))
        .await;

        match created {
            Ok(mut created) => {
                info!(
                    "{} grew from {} to {} partitions",
                    self.topic,
                    self.producers.len(),
                    self.producers.len() + created.len()
                );
                self.producers.append(&mut created);
                // Keyed routing indexes this vector by partition number, so the
                // order has to be the partition order rather than creation order.
                self.producers
                    .sort_by_key(|p| partition_index(p.topic()).unwrap_or(usize::MAX));
            }
            Err(e) => warn!(
                "could not start producers for the new partitions of {}, keeping the current {}: {}",
                self.topic,
                self.producers.len(),
                e
            ),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn get_next_round_robin_producer(&mut self) -> &mut TopicProducer<Exe> {
        let amount_of_producers = self.producers.len();
        self.last_used_producer_index += 1;
        if self.last_used_producer_index >= amount_of_producers {
            self.last_used_producer_index = 0;
        }
        self.producers
            .get_mut(self.last_used_producer_index)
            .unwrap()
    }

    /// Routes a keyed message to the partition owning its key hash.
    ///
    /// A partition key always supersedes the routing policy, matching
    /// `RoundRobinPartitionMessageRouterImpl` and
    /// `SinglePartitionMessageRouterImpl` in the Java client.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn route_by_key(&mut self, partition_key: &str) -> &mut TopicProducer<Exe> {
        // `producers` is non-empty by construction: `build` errors on zero
        // partitions and takes the `Single` path for exactly one.
        let partition_count = NonZeroUsize::new(self.producers.len())
            .expect("a PartitionedProducer always has at least one partition");
        let index = RoutingPolicy::compute_partition_index_for_key(
            partition_key,
            partition_count,
            self.options.hashing_scheme,
        );
        self.producers.get_mut(index).unwrap()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn choose_partition(&mut self, message: &Message) -> &mut TopicProducer<Exe> {
        // A partition key supersedes the routing policy, so that the same key
        // always lands on the same partition regardless of how this producer is
        // configured. `Custom` is the exception: the user's router decides
        // everything, keyed or not, as in Java's `CustomPartition` mode.
        //
        // This applies to `None` too, which behaves as `RoundRobin` — that is the
        // default path, so a key being ignored there would break ordering for
        // every producer that never sets a policy.
        // `routing_key` is the *wire* form: a binary key hashes base64-encoded,
        // matching Java, whose routers hash `msg.getKey()` — already the encoded
        // string for `keyBytes`. Hashing the raw bytes would send the same key to a
        // different partition than every other client.
        let hash_routed_key = match &self.options.routing_policy {
            Some(RoutingPolicy::Custom(_)) => None,
            _ => message
                .partition_key
                .as_ref()
                .and_then(PartitionKey::routing_key),
        };
        if let Some(partition_key) = hash_routed_key {
            return self.route_by_key(&partition_key);
        }

        match &self.options.routing_policy {
            Some(RoutingPolicy::Single) => self
                .producers
                .get_mut(self.last_used_producer_index)
                .unwrap(),
            Some(RoutingPolicy::Custom(policy)) => {
                let amount_of_producers = self.producers.len();
                self.producers
                    .get_mut(policy.route(message, amount_of_producers))
                    .unwrap()
            }
            Some(RoutingPolicy::RoundRobin) | None => self.get_next_round_robin_producer(),
        }
    }
}

/// a producer is used to publish messages on a topic
struct TopicProducer<Exe: Executor> {
    client: Pulsar<Exe>,
    connection: Arc<Connection<Exe>>,
    id: ProducerId,
    name: ProducerName,
    topic: String,
    batch: Option<Batch>,
    sequence_id: SerialId,
    compression: Option<Compression>,
    options: ProducerOptions,
    /// Schema version returned by the broker in `CommandProducerSuccess`.
    ///
    /// * **Non-batched path** — used as a default when the message does not
    ///   already have a `schema_version` set (`send_raw`).  Updated in-place
    ///   on reconnection via `send_message`.
    /// * **Batched path** — a clone is passed by value to the spawned
    ///   `message_send_loop`, which owns its own independent copy.  That copy
    ///   is updated on reconnection within the loop; this field is **not**
    ///   kept in sync and may go stale for batched producers.
    ///
    /// Per-message `schema_version` is not supported by the Pulsar protocol
    /// for batched messages (`SingleMessageMetadata` has no such field).
    schema_version: Option<Vec<u8>>,
}

impl<Exe: Executor> TopicProducer<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn new<S: Into<String>>(
        client: Pulsar<Exe>,
        addr: BrokerAddress,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> Result<Self, Error> {
        static PRODUCER_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

        let topic = topic.into();
        let producer_id = PRODUCER_ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
        let sequence_id = SerialId::new();

        let topic = topic.clone();
        let compression = options.compression.clone();
        let mut connection = client.manager.get_connection(&addr).await?;

        let (producer_name, schema_version) = retry_create_producer(
            &client,
            &mut connection,
            addr,
            &topic,
            producer_id,
            name,
            &options,
        )
        .await?;

        if !options.enabled_batching() {
            return Ok(TopicProducer {
                client,
                connection,
                id: producer_id,
                name: producer_name,
                topic,
                sequence_id,
                compression,
                options,
                batch: None,
                schema_version,
            });
        }
        let executor = client.executor.clone();
        let batch_storage = BatchStorage {
            max_size: options.batch_size,
            max_byte_size: options.batch_byte_size,
            timeout: options.batch_timeout,
            size: 0,
            storage: match options.batch_size {
                Some(batch_size) => VecDeque::with_capacity(batch_size as usize),
                None => VecDeque::new(),
            },
        };
        // the message should be received quickly, so a small buffer is okay
        let (msg_sender, msg_receiver) = mpsc::channel::<BatchItem>(10);
        let executor_clone = executor.clone();
        let (batch_sender, batch_receiver) = mpsc::channel::<Vec<BatchItem>>(1);
        let (close_sender, close_receiver) =
            oneshot::channel::<Result<CommandSuccess, ConnectionError>>();

        let _ = executor.spawn(Box::pin(batch_process_loop(
            producer_id,
            batch_storage,
            msg_receiver,
            batch_sender,
            executor_clone,
        )));
        let _ = executor.spawn(Box::pin(message_send_loop(
            batch_receiver,
            close_sender,
            client.clone(),
            connection.clone(),
            topic.clone(),
            producer_id,
            producer_name.clone(),
            sequence_id.clone(),
            options.clone(),
            schema_version.clone(),
        )));

        Ok(TopicProducer {
            client,
            connection,
            id: producer_id,
            name: producer_name,
            topic,
            batch: Some(Batch {
                msg_sender,
                close_receiver,
            }),
            sequence_id,
            compression,
            options,
            schema_version,
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn topic(&self) -> &str {
        &self.topic
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn options(&self) -> &ProducerOptions {
        &self.options
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn check_connection(&self) -> Result<(), Error> {
        self.connection.sender().send_ping().await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send(&mut self, message: Message) -> Result<SendFuture, Error> {
        self.send_raw(message.into()).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_batch(&mut self) -> Result<(), Error> {
        match &mut self.batch.as_mut().map(|batch| &mut batch.msg_sender) {
            Some(msg_sender) => {
                let (tx, rx) = oneshot::channel::<()>();
                let item = BatchItem::Flush(tx);
                let _ = msg_sender.send(item).await;
                let _ = rx.await; // ignore any error
                Ok(())
            }
            None if self.options.enabled_batching() => Err(ProducerError::Closed.into()),
            _ => Err(ProducerError::Custom("not a batching producer".to_string()).into()),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn send_raw(
        &mut self,
        mut message: ProducerMessage,
    ) -> Result<SendFuture, Error> {
        let (tx, rx) = oneshot::channel();
        match &mut self.batch.as_mut().map(|batch| &mut batch.msg_sender) {
            Some(msg_sender) => {
                // Per-message schema_version is not supported by the Pulsar
                // protocol for batched messages. The schema_version is set
                // on the batch envelope in message_send_loop instead. Any
                // user-provided schema_version on individual messages is
                // structurally dropped here.
                let properties = message
                    .properties
                    .into_iter()
                    .map(|(key, value)| proto::KeyValue { key, value })
                    .collect();
                let batched = BatchedMessage {
                    metadata: proto::SingleMessageMetadata {
                        properties,
                        partition_key: message.partition_key,
                        partition_key_b64_encoded: message.partition_key_b64_encoded,
                        null_partition_key: message.null_partition_key,
                        null_value: message.null_value,
                        ordering_key: message.ordering_key,
                        payload_size: message.payload.len() as i32,
                        event_time: message.event_time,
                        ..Default::default()
                    },
                    payload: message.payload,
                };
                let item = BatchItem::SingleMessage(tx, batched);
                msg_sender.send(item).await.map_err(|e| {
                    Error::Producer(ProducerError::Custom(format!(
                        "failed to send message to batch_process_loop: {e}"
                    )))
                })?;
            }
            None if self.options.enabled_batching() => {
                return Err(ProducerError::Closed.into());
            }
            _ => {
                // If the user didn't set a schema_version on the message,
                // use the one returned by the broker in
                // CommandProducerSuccess.
                if message.schema_version.is_none() {
                    message.schema_version = self.schema_version.clone();
                }
                let compressed_message = compress_message(message, &self.compression)?;
                let fut = send_message(
                    &self.client,
                    &self.topic,
                    &mut self.connection,
                    compressed_message,
                    self.id,
                    &self.name,
                    &self.sequence_id,
                    &self.options,
                    &mut self.schema_version,
                )
                .await?;
                self.client
                    .executor
                    .spawn(Box::pin(async move {
                        let _ = tx.send(fut.await);
                    }))
                    .map_err(|_| Error::Executor)?;
            }
        };
        Ok(SendFuture(rx))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn close(&mut self) -> Result<(), Error> {
        match self.batch.take() {
            None => {
                self.connection.sender().close_producer(self.id).await?;
            }
            Some(mut batch) if self.options.enabled_batching() => {
                batch.msg_sender.close_channel();
                let close_receiver = &mut batch.close_receiver;
                return match close_receiver.await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(Error::Producer(ProducerError::Connection(e))),
                    Err(_) => Err(Error::Producer(ProducerError::Closed)),
                };
            }
            _ => {
                warn!(
                    "close called multiple times on producer {} for topic {}",
                    self.id, self.topic
                );
            }
        };
        Ok(())
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn compress_message(
    mut message: ProducerMessage,
    compression: &Option<Compression>,
) -> Result<ProducerMessage, Error> {
    let compressed_message = match compression {
        None | Some(Compression::None) => message,
        #[cfg(feature = "lz4")]
        Some(Compression::Lz4(compression)) => {
            let compressed_payload: Vec<u8> =
                lz4::block::compress(&message.payload[..], Some(compression.mode), false)
                    .map_err(ProducerError::Io)?;

            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Lz4.into());
            message
        }
        #[cfg(feature = "flate2")]
        Some(Compression::Zlib(compression)) => {
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), compression.level);
            e.write_all(&message.payload[..])
                .map_err(ProducerError::Io)?;
            let compressed_payload = e.finish().map_err(ProducerError::Io)?;

            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Zlib.into());
            message
        }
        #[cfg(feature = "zstd")]
        Some(Compression::Zstd(compression)) => {
            let compressed_payload = zstd::encode_all(&message.payload[..], compression.level)
                .map_err(ProducerError::Io)?;
            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Zstd.into());
            message
        }
        #[cfg(feature = "snap")]
        Some(Compression::Snappy(..)) => {
            let mut compressed_payload = Vec::new();
            {
                let mut encoder = snap::write::FrameEncoder::new(&mut compressed_payload);
                encoder
                    .write_all(&message.payload[..])
                    .map_err(ProducerError::Io)?;
                encoder.flush().map_err(ProducerError::Io)?;
            }

            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Snappy.into());
            message
        }
    };
    Ok(compressed_message)
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn send_message<Exe>(
    client: &Pulsar<Exe>,
    topic: &String,
    connection: &mut Arc<Connection<Exe>>,
    message: ProducerMessage,
    producer_id: ProducerId,
    producer_name: &ProducerName,
    sequence_id: &SerialId,
    options: &ProducerOptions,
    schema_version: &mut Option<Vec<u8>>,
) -> Result<impl Future<Output = Result<CommandSendReceipt, Error>>, Error>
where
    Exe: Executor,
{
    loop {
        // If a previous send timed out waiting for a receipt, the connection
        // is poisoned (error flag set) but the underlying TCP channel may
        // still be open.  Detect this early and fall through to reconnection
        // instead of sending into a black hole that will time out again.
        if !connection.is_valid() {
            warn!(
                "send_message: connection {} is no longer valid, reconnecting producer for topic: {}",
                connection.id(),
                topic
            );
            // fall through to reconnection below
        } else {
            match connection
                .sender()
                .send(
                    producer_id,
                    producer_name.clone(),
                    sequence_id.get(),
                    message.clone(),
                    options.block_queue_if_full,
                )
                .await
            {
                Ok(fut) => {
                    let fut = async move {
                        let res = fut.await;
                        res.map_err(|e| {
                            error!("wait send receipt got error: {:?}", e);
                            Error::Producer(ProducerError::Connection(e))
                        })
                    };
                    return Ok(fut);
                }
                Err(ConnectionError::Disconnected) => {}
                Err(ConnectionError::Io(e)) => {
                    if e.kind() != std::io::ErrorKind::TimedOut {
                        error!("send_message got io error: {:?}", e);
                        return Err(ProducerError::Connection(ConnectionError::Io(e)).into());
                    }
                }
                Err(e) => {
                    error!("send_message got error: {:?}", e);
                    return Err(ProducerError::Connection(e).into());
                }
            }
        }

        error!(
            "send_message: connection {} disconnected, reconnecting producer for topic: {}",
            connection.id(),
            &topic
        );

        if let Err(e) = connection.sender().close_producer(producer_id).await {
            error!(
                "could not close producer {:?}({}) for topic {}: {:?}",
                producer_name, producer_id, &topic, e
            );
        }

        let broker_address = client.lookup_topic(topic).await?;

        let (_producer_name, new_schema_version) = retry_create_producer(
            client,
            connection,
            broker_address,
            topic,
            producer_id,
            Some(producer_name.clone()),
            options,
        )
        .await?;

        // Update the producer-level schema_version for future messages.
        // The in-flight message keeps its original schema_version: it was
        // set before entering send_message and matches the schema used to
        // serialize its payload.
        *schema_version = new_schema_version;
    }
}

impl<Exe: Executor> std::ops::Drop for TopicProducer<Exe> {
    fn drop(&mut self) {
        let conn = self.connection.clone();
        let id = self.id;
        let name = self.name.clone();
        let topic = self.topic.clone();
        if let Some(mut batch) = self.batch.take() {
            batch.msg_sender.close_channel();
        }
        let _ = self.client.executor.spawn(Box::pin(async move {
            if let Err(e) = conn.sender().close_producer(id).await {
                error!(
                    "could not close producer {:?}({}) for topic {}: {:?}",
                    name, id, topic, e
                );
            }
        }));
    }
}

/// Helper structure to prepare a producer
///
/// generated from [Pulsar::producer]
#[derive(Clone)]
pub struct ProducerBuilder<Exe: Executor> {
    pulsar: Pulsar<Exe>,
    topic: Option<String>,
    name: Option<String>,
    producer_options: Option<ProducerOptions>,
    partition_refresh: Option<Duration>,
}

impl<Exe: Executor> ProducerBuilder<Exe> {
    /// creates a new ProducerBuilder from a client
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(pulsar: &Pulsar<Exe>) -> Self {
        ProducerBuilder {
            pulsar: pulsar.clone(),
            topic: None,
            name: None,
            producer_options: None,
            partition_refresh: Some(DEFAULT_PARTITION_REFRESH),
        }
    }

    /// sets the producer's topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// How often to re-check whether the topic has gained partitions.
    ///
    /// A partitioned topic can be grown at any time, and a producer that never
    /// re-checks keeps routing over the partitions it started with: the new ones
    /// receive nothing, and keyed messages land somewhere other than where a
    /// client that did notice would put them.
    ///
    /// The check runs on the next send after the interval has elapsed, so an idle
    /// producer does no work; it costs one topic lookup. Java's equivalent pair is
    /// `autoUpdatePartitions` and `autoUpdatePartitionsInterval`, and this shares
    /// its default of 60 seconds. Non-partitioned topics never re-check, since
    /// they cannot gain partitions.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_partition_refresh(mut self, interval: Duration) -> Self {
        self.partition_refresh = Some(interval);
        self
    }

    /// Never re-check the topic's partition count.
    ///
    /// See [`with_partition_refresh`][Self::with_partition_refresh] for what this
    /// gives up.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn without_partition_refresh(mut self) -> Self {
        self.partition_refresh = None;
        self
    }

    /// sets the producer's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = Some(name.into());
        self
    }

    /// configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_options(mut self, options: ProducerOptions) -> Self {
        self.producer_options = Some(options);
        self
    }

    /// creates a new producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build(self) -> Result<Producer<Exe>, Error> {
        let ProducerBuilder {
            pulsar,
            topic,
            name,
            producer_options,
            partition_refresh,
        } = self;
        let topic = topic.ok_or_else(|| Error::Custom("topic not set".to_string()))?;
        let options = producer_options.unwrap_or_default();

        let partitions = pulsar.lookup_partitioned_topic(&topic).await?;

        // `lookup_partitioned_topic` echoes the topic back unchanged when it is not
        // partitioned, and returns `-partition-N` names when it is. That, rather
        // than the count, is what decides the two cases below: a one-partition
        // topic is still partitioned and can still grow.
        let is_partitioned = partitions.first().is_some_and(|(first, _)| first != &topic);

        let mut producers: Vec<TopicProducer<Exe>> =
            try_join_all(partitions.into_iter().map(|(topic, addr)| {
                let name = name.clone();
                let options = options.clone();
                let pulsar = pulsar.clone();
                async move {
                    let producer = TopicProducer::new(pulsar, addr, topic, name, options).await?;
                    Ok::<TopicProducer<Exe>, Error>(producer)
                }
            }))
            .await?;

        // Keyed routing indexes this vector by partition number, so sort by that
        // and not by `prod.id`, which is a process-wide producer counter that only
        // happens to agree with partition order.
        producers.sort_by_key(|prod| partition_index(prod.topic()).unwrap_or(usize::MAX));

        if producers.is_empty() {
            return Err(Error::Custom(format!(
                "Unexpected error: Partition lookup returned no topics for {topic}"
            )));
        }

        let producer = if is_partitioned {
            let len = producers.len();
            ProducerInner::Partitioned(PartitionedProducer {
                producers,
                last_used_producer_index: rand::thread_rng().gen_range(0..len),
                topic,
                partition_refresh,
                last_partition_check: Instant::now(),
                pulsar,
                name,
                options,
            })
        } else {
            ProducerInner::Single(producers.into_iter().next().unwrap())
        };

        Ok(Producer { inner: producer })
    }

    /// creates a new [MultiTopicProducer]
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn build_multi_topic(self) -> MultiTopicProducer<Exe> {
        MultiTopicProducer {
            client: self.pulsar,
            producers: Default::default(),
            options: self.producer_options.unwrap_or_default(),
            name: self.name,
        }
    }
}

struct BatchStorage {
    max_size: Option<u32>,
    max_byte_size: Option<usize>,
    timeout: Option<Duration>,
    size: usize,
    storage: VecDeque<BatchItem>,
}

impl BatchStorage {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn push_back(&mut self, item: BatchItem) {
        if let BatchItem::SingleMessage(_, batched_msg) = &item {
            self.size += batched_msg.metadata.payload_size as usize;
        }
        self.storage.push_back(item);
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get_messages(&mut self) -> Vec<BatchItem> {
        self.size = 0;
        self.storage.drain(..).collect()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn ready_to_flush(&self) -> bool {
        if let Some(max_size) = self.max_size {
            if self.storage.len() >= max_size as usize {
                return true;
            }
        }
        if let Some(max_byte_size) = self.max_byte_size {
            if self.size >= max_byte_size {
                return true;
            }
        }
        matches!(self.storage.back(), Some(BatchItem::Flush(_)))
    }
}

enum BatchItem {
    SingleMessage(
        oneshot::Sender<Result<CommandSendReceipt, Error>>,
        BatchedMessage,
    ),
    Flush(oneshot::Sender<()>),
}

struct Batch {
    // sends a message or trigger a flush
    msg_sender: mpsc::Sender<BatchItem>,
    // receives the notification when `bath_process_loop` is closed
    close_receiver: oneshot::Receiver<Result<CommandSuccess, ConnectionError>>,
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn batch_process_loop(
    producer_id: ProducerId,
    mut batch_storage: BatchStorage,
    mut msg_receiver: mpsc::Receiver<BatchItem>,
    mut batch_sender: mpsc::Sender<Vec<BatchItem>>,
    executor: impl Executor,
) {
    let mut recv_future = msg_receiver.next();
    let mut timer_future: Pin<Box<dyn Future<Output = ()> + Send + 'static>> =
        Box::pin(future::pending());

    let flush = async |batch_sender: &mut mpsc::Sender<Vec<BatchItem>>,
                       messages: Vec<BatchItem>| {
        if !messages.is_empty() {
            let _ = batch_sender.send(messages).await;
        }
    };

    loop {
        match future::select(recv_future, timer_future).await {
            Either::Left((Some(batch_item), previous_timer_future)) => {
                batch_storage.push_back(batch_item);
                if batch_storage.ready_to_flush() {
                    flush(&mut batch_sender, batch_storage.get_messages()).await;
                    timer_future = Box::pin(future::pending());
                } else {
                    timer_future = match batch_storage.timeout {
                        Some(timeout) if batch_storage.storage.len() == 1 => {
                            Box::pin(executor.delay(timeout))
                        }
                        _ => previous_timer_future,
                    };
                }
                recv_future = msg_receiver.next();
            }
            Either::Left((None, _)) => {
                let count = batch_storage.storage.len();
                if count > 0 {
                    warn!("producer {}'s batch_process_loop exits when there are {} messages not flushed",
                        producer_id, count);
                    for item in batch_storage.get_messages() {
                        if let BatchItem::SingleMessage(tx, _) = item {
                            let _ = tx.send(Err(Error::Producer(ProducerError::Closed)));
                        }
                    }
                } else {
                    info!("producer {producer_id}'s batch_process_loop: channel closed, exiting");
                }
                break;
            }
            Either::Right((_, previous_recv_future)) => {
                if batch_storage.timeout.is_some() {
                    flush(&mut batch_sender, batch_storage.get_messages()).await;
                }
                timer_future = Box::pin(future::pending());
                recv_future = previous_recv_future;
            }
        }
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn message_send_loop<Exe>(
    mut msg_receiver: mpsc::Receiver<Vec<BatchItem>>,
    close_sender: oneshot::Sender<Result<CommandSuccess, ConnectionError>>,
    client: Pulsar<Exe>,
    mut connection: Arc<Connection<Exe>>,
    topic: String,
    producer_id: ProducerId,
    producer_name: ProducerName,
    sequence_id: SerialId,
    options: ProducerOptions,
    mut schema_version: Option<Vec<u8>>,
) where
    Exe: Executor,
{
    loop {
        match msg_receiver.next().await {
            Some(mut batch_items) => {
                if batch_items.is_empty() {
                    error!(
                        "producer {}'s message_send_loop received an empty batch unexpectedly",
                        producer_id
                    );
                    continue;
                }
                let mut payload: Vec<u8> = Vec::new();
                let mut receipts = Vec::new();

                let flush_tx = {
                    if let Some(BatchItem::Flush(_)) = batch_items.last() {
                        if let BatchItem::Flush(tx) = batch_items.pop().unwrap() {
                            Some(tx)
                        } else {
                            unreachable!()
                        }
                    } else {
                        None
                    }
                };
                let counter = batch_items.len();
                for item in batch_items {
                    if let BatchItem::SingleMessage(tx, batched_msg) = item {
                        receipts.push(tx);
                        batched_msg.serialize(&mut payload);
                    } else {
                        error!(
                            "producer {}'s message_send_loop received a Flush item unexpectedly",
                            producer_id
                        );
                    }
                }
                if counter == 0 {
                    if let Some(flush_tx) = flush_tx {
                        let _ = flush_tx.send(());
                    }
                    continue;
                }

                let message = ProducerMessage {
                    payload,
                    num_messages_in_batch: Some(counter as i32),
                    schema_version: schema_version.clone(),
                    ..Default::default()
                };

                trace!("sending a batched message of size {}", counter);

                let send = async || {
                    let compressed_message = compress_message(message, &options.compression)?;
                    send_message(
                        &client,
                        &topic,
                        &mut connection,
                        compressed_message,
                        producer_id,
                        &producer_name,
                        &sequence_id,
                        &options,
                        &mut schema_version,
                    )
                    .await?
                    .await
                };
                match send().await {
                    Ok(receipt) => {
                        for (batch_index, tx) in receipts.into_iter().enumerate() {
                            let mut receipt = receipt.clone();
                            if let Some(msg_id) = &mut receipt.message_id {
                                msg_id.batch_index = Some(batch_index as i32);
                                msg_id.batch_size = Some(counter as i32);
                            }
                            let _ = tx.send(Ok(receipt));
                        }
                        if let Some(flush_tx) = flush_tx {
                            let _ = flush_tx.send(());
                        }
                    }
                    Err(e) => {
                        let error = Arc::new(e);
                        for tx in receipts {
                            let _ =
                                tx.send(Err(Error::Producer(ProducerError::Batch(error.clone()))));
                        }
                    }
                };
            }
            None => {
                debug!("producer {producer_id} message_send_loop: channel closed, exiting");
                let close_result = connection.sender().close_producer(producer_id).await;
                let _ = close_sender.send(close_result).inspect_err(|e| {
                    warn!(
                        "{producer_id} could not notify the message_send_loop is closed: {:?}, the producer might be dropped without closing",
                        e
                    );
                });
                break;
            }
        }
    }
}

/// Helper structure to prepare a message
///
/// generated with [Producer::create_message]
pub struct MessageBuilder<'a, T, Exe: Executor> {
    producer: &'a mut Producer<Exe>,
    properties: HashMap<String, String>,
    partition_key: Option<PartitionKey>,
    ordering_key: Option<Vec<u8>>,
    deliver_at_time: Option<i64>,
    event_time: Option<u64>,
    content: T,
}

impl<'a, Exe: Executor> MessageBuilder<'a, (), Exe> {
    /// creates a message builder from an existing producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(producer: &'a mut Producer<Exe>) -> Self {
        MessageBuilder {
            producer,
            properties: HashMap::new(),
            partition_key: None,
            ordering_key: None,
            deliver_at_time: None,
            event_time: None,
            content: (),
        }
    }
}

impl<'a, T, Exe: Executor> MessageBuilder<'a, T, Exe> {
    /// sets the message's content
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_content<C>(self, content: C) -> MessageBuilder<'a, C, Exe> {
        MessageBuilder {
            producer: self.producer,
            properties: self.properties,
            partition_key: self.partition_key,
            ordering_key: self.ordering_key,
            deliver_at_time: self.deliver_at_time,
            event_time: self.event_time,
            content,
        }
    }

    /// Sets the message's partition key.
    ///
    /// Accepts a `String`, `&str` or `Vec<u8>`; a byte key is sent base64-encoded
    /// and hashed in that form, matching Java's `keyBytes`. See [`PartitionKey`].
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_partition_key<K: Into<PartitionKey>>(mut self, partition_key: K) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// sets the message's ordering key for key_shared subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_ordering_key<S: Into<Vec<u8>>>(mut self, ordering_key: S) -> Self {
        self.ordering_key = Some(ordering_key.into());
        self
    }

    /// sets the message's partition key
    ///
    /// this is the same as `with_partition_key`, this method is added for
    /// more consistency with other clients
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_key<K: Into<PartitionKey>>(mut self, partition_key: K) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// sets a user defined property
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_property<S1: Into<String>, S2: Into<String>>(mut self, key: S1, value: S2) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// delivers the message at this date
    /// Note: The delayed and scheduled message attributes are only applied to shared subscription.
    /// With other subscription types, the messages will still be delivered immediately.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn deliver_at(mut self, date: SystemTime) -> Result<Self, std::time::SystemTimeError> {
        self.deliver_at_time = Some(date.duration_since(UNIX_EPOCH)?.as_millis() as i64);
        Ok(self)
    }

    /// delays message deliver with this duration
    /// Note: The delayed and scheduled message attributes are only applied to shared subscription.
    /// With other subscription types, the messages will still be delivered immediately.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn delay(mut self, delay: Duration) -> Result<Self, std::time::SystemTimeError> {
        let date = SystemTime::now() + delay;
        self.deliver_at_time = Some(date.duration_since(UNIX_EPOCH)?.as_millis() as i64);
        Ok(self)
    }

    // set the event time for a given message
    // By default, messages don't have an event time associated, while the publish
    // time will be be always present.
    // Set the event time to explicitly declare the time
    // that the event "happened", as opposed to when the message is being published.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn event_time(mut self, event_time: u64) -> Self {
        self.event_time = Some(event_time);
        self
    }
}

impl<T: SerializeMessage + Sized, Exe: Executor> MessageBuilder<'_, T, Exe> {
    /// sends the message through the producer that created it
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_non_blocking"]
    pub async fn send(self) -> Result<SendFuture, Error> {
        let fut = self.send_non_blocking().await?;
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(fut.await);
        Ok(SendFuture(rx))
    }

    /// sends the message through the producer that created it
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_non_blocking(self) -> Result<SendFuture, Error> {
        let MessageBuilder {
            producer,
            properties,
            partition_key,
            ordering_key,
            content,
            deliver_at_time,
            event_time,
        } = self;

        let mut message = T::serialize_message(content)?;
        message.properties = properties;
        message.partition_key = partition_key;
        message.ordering_key = ordering_key;
        message.event_time = event_time;
        message.deliver_at_time = deliver_at_time;
        producer.send_non_blocking(message).await
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use log::LevelFilter;

    use super::*;
    use crate::{
        routing_policy::CustomRoutingPolicy, test_utils, tests::TEST_LOGGER, TokioExecutor,
    };

    /// Keyed routing indexes the producer vector by partition number, so an
    /// index parsed from the wrong shape of name would silently misroute.
    #[test]
    fn partition_index_reads_only_real_partition_suffixes() {
        assert_eq!(
            partition_index("persistent://public/default/orders-partition-3"),
            Some(3)
        );
        assert_eq!(partition_index("orders-partition-0"), Some(0));
        assert_eq!(partition_index("orders-partition-12"), Some(12));

        for topic in [
            "orders",
            "orders-partition-",
            "orders-partition-archive",
            "orders-partition-3x",
            "orders-partition--1",
            "-partition-0",
        ] {
            assert_eq!(partition_index(topic), None, "{topic} is not a partition");
        }
    }

    #[test]
    fn send_future_errors_when_sender_dropped() {
        let (tx, rx) = futures::channel::oneshot::channel::<Result<CommandSendReceipt, Error>>();
        // Drop the sender immediately to simulate an unexpected disconnect:
        drop(tx);

        let fut = SendFuture(rx);
        let err = block_on(fut).expect_err("expected an error when sender is dropped");

        // It should be mapped to a ProducerError::Custom inside Error::Producer
        match err {
            Error::Producer(ProducerError::Custom(msg)) => {
                assert!(
                    msg.contains("unexpectedly disconnected"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("unexpected error variant: {:?}", other),
        }
    }

    /// A null value is distinct from an empty one, on the wire.
    ///
    /// Java draws the same line between `value(null)` and `value(new byte[0])`, and a
    /// Java consumer reads `null_value` to tell them apart. Modelling the payload as
    /// a plain `Vec<u8>` made the two indistinguishable.
    #[test]
    fn a_null_value_is_not_an_empty_value() {
        let null: ProducerMessage = Message {
            payload: None,
            ..Default::default()
        }
        .into();
        assert_eq!(null.null_value, Some(true));
        assert!(null.payload.is_empty(), "a null value carries no bytes");

        let empty: ProducerMessage = Message {
            payload: Some(Vec::new()),
            ..Default::default()
        }
        .into();
        assert_eq!(empty.null_value, None, "an empty value is not a null value");
        assert!(empty.payload.is_empty());
    }

    /// A binary key travels base64-encoded with the flag set.
    #[test]
    fn a_binary_key_is_base64_encoded_on_the_wire() {
        let m: ProducerMessage = Message {
            partition_key: Some(PartitionKey::Bytes(vec![0, 1, 255])),
            ..Default::default()
        }
        .into();
        assert_eq!(m.partition_key.as_deref(), Some("AAH/"));
        assert_eq!(m.partition_key_b64_encoded, Some(true));
        assert_eq!(m.null_partition_key, None);
    }

    /// An explicitly null key is distinct from no key at all.
    #[test]
    fn an_explicitly_null_key_is_not_an_absent_key() {
        let null: ProducerMessage = Message {
            partition_key: Some(PartitionKey::Null),
            ..Default::default()
        }
        .into();
        assert_eq!(null.null_partition_key, Some(true));
        assert_eq!(null.partition_key, None);

        let absent: ProducerMessage = Message {
            partition_key: None,
            ..Default::default()
        }
        .into();
        assert_eq!(absent.null_partition_key, None);
        assert_eq!(absent.partition_key, None);
    }

    /// A binary key hashes in its **encoded** form.
    ///
    /// Java's routers hash `msg.getKey()`, which for `keyBytes` is already the
    /// base64 string. Hashing the raw bytes would put the same key on a different
    /// partition than every other client — the exact class of bug `HashingScheme`
    /// was introduced to fix.
    #[test]
    fn a_binary_key_hashes_as_its_base64_text() {
        let raw = vec![0u8, 1, 255];
        let key = PartitionKey::Bytes(raw.clone());
        assert_eq!(key.routing_key().as_deref(), Some("AAH/"));

        let partitions = NonZeroUsize::new(7).unwrap();
        for scheme in [HashingScheme::JavaStringHash, HashingScheme::Murmur3_32Hash] {
            let encoded = RoutingPolicy::compute_partition_index_for_key(
                key.routing_key().unwrap().as_ref(),
                partitions,
                scheme,
            );
            // What the raw bytes would have hashed to, had we not encoded first.
            let raw_as_text = String::from_utf8_lossy(&raw).to_string();
            let unencoded =
                RoutingPolicy::compute_partition_index_for_key(&raw_as_text, partitions, scheme);
            assert_ne!(
                encoded, unencoded,
                "{scheme:?}: this key hashes the same either way, so it cannot show \
                 that the encoded form is used — pick a different fixture"
            );
        }
    }

    /// A round-trip through message metadata preserves the key's form.
    #[test]
    fn a_key_survives_a_round_trip_through_metadata() {
        for original in [
            PartitionKey::Text("plain".to_string()),
            PartitionKey::Bytes(vec![0, 1, 255]),
            PartitionKey::Null,
        ] {
            let sent: ProducerMessage = Message {
                partition_key: Some(original.clone()),
                ..Default::default()
            }
            .into();
            assert_eq!(
                PartitionKey::from_metadata(
                    sent.partition_key,
                    sent.partition_key_b64_encoded,
                    sent.null_partition_key,
                ),
                Some(original.clone()),
                "{original:?} did not survive the round trip"
            );
        }
        assert_eq!(PartitionKey::from_metadata(None, None, None), None);
    }

    #[test]
    fn message_converts_into_producer_message() {
        let mut props = HashMap::new();
        props.insert("a".to_string(), "1".to_string());
        props.insert("b".to_string(), "2".to_string());

        let m = Message {
            payload: Some(b"hello".to_vec()),
            properties: props.clone(),
            partition_key: Some("key".into()),
            ordering_key: Some(vec![1, 2, 3]),
            replicate_to: vec!["r1".into(), "r2".into()],
            event_time: Some(42),
            schema_version: Some(vec![9, 9]),
            deliver_at_time: Some(123456789),
        };

        let pm: ProducerMessage = m.clone().into();

        assert_eq!(pm.payload, m.payload.clone().unwrap());
        assert!(
            pm.null_value.is_none(),
            "a present payload is not a null value"
        );
        assert_eq!(pm.properties, m.properties);
        assert_eq!(pm.partition_key.as_deref(), Some("key"));
        assert!(
            pm.partition_key_b64_encoded.is_none(),
            "a text key is not base64"
        );
        assert!(pm.null_partition_key.is_none());
        assert_eq!(pm.ordering_key, m.ordering_key);
        assert_eq!(pm.replicate_to, m.replicate_to);
        assert_eq!(pm.event_time, m.event_time);
        assert_eq!(pm.schema_version, m.schema_version);
        assert_eq!(pm.deliver_at_time, m.deliver_at_time);

        // And defaults that the producer fills later:
        assert!(pm.num_messages_in_batch.is_none());
        assert!(pm.compression.is_none());
        assert!(pm.uncompressed_size.is_none());
    }

    #[tokio::test]
    async fn block_if_queue_full() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .with_outbound_channel_size(3)
            .build()
            .await
            .unwrap();
        let mut producer = pulsar
            .producer()
            .with_topic(format!("block_queue_if_full_{}", rand::random::<u16>()))
            .build()
            .await
            .unwrap();
        let mut send_results = Vec::with_capacity(10);
        for i in 0..10 {
            send_results.push(producer.send_non_blocking(format!("msg-{i}")).await);
        }
        let mut failed_indexes = vec![];
        for (i, result) in send_results.into_iter().enumerate() {
            match result {
                Ok(_) => {}
                Err(Error::Producer(ProducerError::Connection(ConnectionError::SlowDown))) => {
                    failed_indexes.push(i);
                }
                Err(e) => panic!("failed to send {}: {}", i, e),
            }
        }
        info!("Messages failed due to SlowDown: {:?}", &failed_indexes);
        assert!(!failed_indexes.is_empty());

        let mut producer = pulsar
            .producer()
            .with_topic(format!("block_queue_if_full_{}", rand::random::<u16>()))
            .with_options(ProducerOptions {
                block_queue_if_full: true,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let mut send_results = Vec::with_capacity(10);
        for i in 0..10 {
            send_results.push(producer.send_non_blocking(format!("msg-{i}")).await);
        }
        for (i, result) in send_results.into_iter().enumerate() {
            match result {
                Ok(_) => {}
                Err(e) => panic!("failed to send {}: {}", i, e),
            }
        }
    }

    #[tokio::test]
    async fn move_producer_to_spawned_task() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .with_outbound_channel_size(3)
            .build()
            .await
            .unwrap();
        let mut producer = pulsar
            .producer()
            .with_topic(format!("topic_{}", rand::random::<u16>()))
            .build()
            .await
            .unwrap();
        let (sender, receiver) = oneshot::channel();
        let _ = pulsar.executor.spawn(Box::pin(async move {
            sender.send(producer.close().await).unwrap();
        }));
        assert!(receiver.await.is_ok());
    }

    #[tokio::test]
    async fn test_round_robin_routing_policy() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("topic_{}", rand::random::<u16>());
        let options = ProducerOptions {
            routing_policy: Some(RoutingPolicy::RoundRobin),
            ..Default::default()
        };
        let partition_count = 3;
        test_utils::create_partitioned_topic("public", "default", &topic, partition_count).await;

        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_options(options)
            .build()
            .await
            .unwrap();

        // test round robin without key
        let message = "test".to_string();
        let mut producer_id = 0;
        for _ in 1..100 {
            let send_receipt = producer
                .send_non_blocking(&message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id != producer_id);
            producer_id = send_receipt.producer_id;
        }

        // test round robin with key
        let key = "test";
        let message = Message {
            payload: Some("test".into()),
            partition_key: Some(key.into()),
            ..Default::default()
        };
        let CommandSendReceipt { producer_id, .. } = producer
            .send_non_blocking(message)
            .await
            .unwrap()
            .await
            .unwrap();
        for _ in 1..100 {
            let message = Message {
                payload: Some("test".into()),
                partition_key: Some(key.into()),
                ..Default::default()
            };

            let send_receipt = producer
                .send_non_blocking(message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id == producer_id);
        }
    }

    #[tokio::test]
    async fn test_single_routing_policy() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("topic_{}", rand::random::<u16>());
        let options = ProducerOptions {
            routing_policy: Some(RoutingPolicy::Single),
            ..Default::default()
        };
        let partition_count = 3;
        test_utils::create_partitioned_topic("public", "default", &topic, partition_count).await;

        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_options(options)
            .build()
            .await
            .unwrap();

        let key = "test";
        let message = Message {
            payload: Some("test".into()),
            partition_key: Some(key.into()),
            ..Default::default()
        };

        let CommandSendReceipt { producer_id, .. } = producer
            .send_non_blocking(message)
            .await
            .unwrap()
            .await
            .unwrap();
        for _ in 1..100 {
            let message = Message {
                payload: Some("test".into()),
                partition_key: Some(key.into()),
                ..Default::default()
            };

            let send_receipt = producer
                .send_non_blocking(message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id == producer_id);
        }
    }

    /// Produces a keyed message per golden vector and asserts the broker
    /// received it on the partition the Java client would have chosen.
    ///
    /// The unit tests in [`crate::routing_policy`] pin the hash arithmetic; this
    /// pins the wiring — that `choose_partition` actually consults the key, uses
    /// the configured scheme, and maps the index to the right partition topic.
    /// Together they are what stops a Rust producer from silently interleaving
    /// keys against a Java producer on the same topic.
    async fn assert_keys_land_on_java_partitions(
        routing_policy: Option<RoutingPolicy>,
        hashing_scheme: HashingScheme,
    ) {
        use std::collections::{BTreeMap, BTreeSet};

        use futures::TryStreamExt;

        use crate::{
            consumer::InitialPosition,
            routing_policy::java_vectors::{PARTITION_COUNTS, VECTORS},
            Consumer, ConsumerOptions, SubType,
        };

        // Index of the partition count under test within the golden table.
        // Must be a partition count for which clearing Murmur's sign bit can
        // actually change the chosen partition. Masking subtracts exactly 2^31, so
        // for any power-of-two count (2, 4, 8, 16, 64) `raw % n == masked % n` and
        // the test could not detect a missing mask at all. 7 is coprime to 2^31.
        const COUNT_INDEX: usize = 4;
        let partition_count = PARTITION_COUNTS[COUNT_INDEX];
        assert!(
            !(1u64 << 31).is_multiple_of(u64::from(partition_count)),
            "partition count {partition_count} cannot distinguish masked from unmasked hashes"
        );

        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("test_key_placement_{}", rand::random::<u32>());
        test_utils::create_partitioned_topic("public", "default", &topic, partition_count).await;

        let expected_partition =
            |v: &crate::routing_policy::java_vectors::HashVector| match hashing_scheme {
                HashingScheme::JavaStringHash => v.jsh_partitions[COUNT_INDEX],
                HashingScheme::Murmur3_32Hash => v.m3_partitions[COUNT_INDEX],
            };

        // Pick keys deliberately rather than taking a prefix of the table, which
        // would be all ASCII. The interesting classes are:
        //   * ASCII, including every tail length mod 4 (Murmur's tail handling)
        //   * BMP multi-byte UTF-8
        //   * non-BMP, where Java's UTF-16 `hashCode` sees two surrogate halves
        //   * keys whose raw Murmur hash has bit 31 set, which are exactly the
        //     ones the missing mask used to misroute
        let non_bmp = |k: &str| k.chars().any(|c| c as u32 > 0xFFFF);
        let non_ascii_bmp = |k: &str| !k.is_ascii() && !non_bmp(k);
        let bit31_set =
            |k: &str| murmur3::murmur3_32(&mut k.as_bytes(), 0).unwrap() & (1 << 31) != 0;

        let mut keys: Vec<&'static str> = Vec::new();
        let push = |keys: &mut Vec<&'static str>, k: &'static str| {
            if !k.is_empty() && !keys.contains(&k) {
                keys.push(k);
            }
        };
        // An empty key is not a key as far as the broker is concerned, so it is
        // covered by the unit vectors only.
        for v in VECTORS.iter().filter(|v| non_bmp(v.key)) {
            push(&mut keys, v.key);
        }
        for v in VECTORS.iter().filter(|v| non_ascii_bmp(v.key)) {
            push(&mut keys, v.key);
        }
        for v in VECTORS.iter().filter(|v| bit31_set(v.key)).take(8) {
            push(&mut keys, v.key);
        }
        for v in VECTORS.iter().take(12) {
            push(&mut keys, v.key);
        }

        assert!(
            keys.iter().any(|k| non_bmp(k))
                && keys.iter().any(|k| non_ascii_bmp(k))
                && keys.iter().any(|k| bit31_set(k)),
            "key selection must cover non-BMP, BMP-non-ASCII and bit-31-set keys"
        );

        let mut want: BTreeMap<&str, u32> = BTreeMap::new();
        for v in VECTORS.iter().filter(|v| keys.contains(&v.key)) {
            want.insert(v.key, expected_partition(v));
        }
        assert!(
            want.values().collect::<BTreeSet<_>>().len() > 1,
            "test would be vacuous if every key routed to one partition"
        );

        let mut producer = pulsar
            .producer()
            .with_topic(&topic)
            .with_options(ProducerOptions {
                routing_policy,
                hashing_scheme,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        for key in &keys {
            producer
                .create_message()
                .with_content(key.to_string())
                .with_partition_key(*key)
                .send_non_blocking()
                .await
                .unwrap()
                .await
                .unwrap();
        }
        producer.close().await.unwrap();

        // Read each partition topic directly and record which keys arrived there.
        let mut got: BTreeMap<&str, u32> = BTreeMap::new();
        for partition in 0..partition_count {
            let partition_topic =
                format!("persistent://public/default/{topic}-partition-{partition}");
            let mut consumer: Consumer<String, _> = pulsar
                .consumer()
                .with_topic(&partition_topic)
                .with_subscription(format!("verify_{partition}"))
                .with_subscription_type(SubType::Exclusive)
                .with_options(
                    ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
                )
                .build()
                .await
                .unwrap();

            // Drain until the partition goes quiet; every message was acked by the
            // producer before we subscribed, so nothing is still in flight. A
            // consumer error must surface rather than read as "partition empty",
            // otherwise a broken consumer is indistinguishable from a routing
            // mismatch.
            loop {
                let msg = match tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    consumer.try_next(),
                )
                .await
                {
                    // Timed out or stream ended: this partition has no more messages.
                    Err(_) | Ok(Ok(None)) => break,
                    Ok(Ok(Some(msg))) => msg,
                    Ok(Err(e)) => panic!("consumer error on partition {partition}: {e}"),
                };

                let key = msg.key().expect("produced messages all carry a key");
                let key = keys
                    .iter()
                    .find(|k| **k == key)
                    .unwrap_or_else(|| panic!("unexpected key {key:?} on partition {partition}"));
                assert!(
                    got.insert(key, partition).is_none(),
                    "key {key:?} delivered more than once"
                );
                consumer.ack(&msg).await.unwrap();
            }
            consumer.close().await.unwrap();
        }

        assert_eq!(
            got, want,
            "{hashing_scheme:?}: keys landed on different partitions than the Java client would pick"
        );
    }

    #[tokio::test]
    async fn keyed_messages_land_on_java_partitions_java_string_hash() {
        assert_keys_land_on_java_partitions(
            Some(RoutingPolicy::RoundRobin),
            HashingScheme::JavaStringHash,
        )
        .await;
    }

    #[tokio::test]
    async fn keyed_messages_land_on_java_partitions_murmur3() {
        assert_keys_land_on_java_partitions(
            Some(RoutingPolicy::RoundRobin),
            HashingScheme::Murmur3_32Hash,
        )
        .await;
    }

    /// `RoutingPolicy::Single` must still hash-route keyed messages, matching
    /// `SinglePartitionMessageRouterImpl`. Before this was fixed every keyed
    /// message went to whichever partition the producer happened to be pinned to.
    #[tokio::test]
    async fn keyed_messages_land_on_java_partitions_single_policy() {
        assert_keys_land_on_java_partitions(
            Some(RoutingPolicy::Single),
            HashingScheme::JavaStringHash,
        )
        .await;
    }

    /// The default path: no routing policy configured at all. This is what most
    /// producers use, so it is the case that matters most.
    #[tokio::test]
    async fn keyed_messages_land_on_java_partitions_default_policy() {
        assert_keys_land_on_java_partitions(None, HashingScheme::default()).await;
    }

    struct TestCustomRoutingPolicy {}

    impl CustomRoutingPolicy for TestCustomRoutingPolicy {
        fn route(&self, _message: &Message, _num_producers: usize) -> usize {
            1
        }
    }

    /// A null value and a binary key survive a publish/consume round trip.
    ///
    /// This is the Phase 0 exit criterion: before this, `producer::Message` modelled
    /// the payload as `Vec<u8>` and the key as `Option<String>`, so neither could be
    /// expressed at all. The assertions are on the *metadata flags*, because that is
    /// what a Java consumer reads to tell a null value from an empty one.
    #[tokio::test]
    #[cfg_attr(not(feature = "admin-api"), ignore)]
    async fn null_values_and_binary_keys_round_trip() {
        use crate::{
            consumer::{ConsumerOptions, InitialPosition},
            message::proto::command_subscribe::SubType,
            Consumer,
        };

        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!(
            "persistent://public/default/nullkey-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let mut consumer: Consumer<Vec<u8>, _> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription("round-trip")
            .with_subscription_type(SubType::Exclusive)
            .with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
            )
            .build()
            .await
            .unwrap();

        let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
        let binary_key = vec![0u8, 1, 255, 0x7f];

        // 1: a null value. 2: an empty value, which must NOT look null.
        // 3: a binary key. 4: an explicitly null key.
        for message in [
            Message {
                payload: None,
                ..Default::default()
            },
            Message {
                payload: Some(Vec::new()),
                ..Default::default()
            },
            Message {
                payload: Some(b"keyed".to_vec()),
                partition_key: Some(PartitionKey::Bytes(binary_key.clone())),
                ..Default::default()
            },
            Message {
                payload: Some(b"nullkey".to_vec()),
                partition_key: Some(PartitionKey::Null),
                ..Default::default()
            },
        ] {
            producer
                .send_non_blocking(message)
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let mut received = Vec::new();
        while received.len() < 4 {
            let msg = consumer.next().await.unwrap().unwrap();
            consumer.ack(&msg).await.unwrap();
            received.push(msg);
        }

        let null_value = received[0].metadata();
        assert_eq!(
            null_value.null_value,
            Some(true),
            "a null value did not set null_value"
        );

        let empty_value = received[1].metadata();
        assert_ne!(
            empty_value.null_value,
            Some(true),
            "an empty value was published as a null one — the two must stay distinct"
        );

        let keyed = received[2].metadata();
        assert_eq!(
            keyed.partition_key_b64_encoded,
            Some(true),
            "a binary key did not set partition_key_b64_encoded"
        );
        assert_eq!(
            keyed.partition_key.as_deref(),
            Some(BASE64.encode(&binary_key).as_str()),
            "the binary key was not base64-encoded on the wire"
        );
        // And it decodes back to exactly the bytes that were sent.
        assert_eq!(
            PartitionKey::from_metadata(
                keyed.partition_key.clone(),
                keyed.partition_key_b64_encoded,
                keyed.null_partition_key,
            ),
            Some(PartitionKey::Bytes(binary_key)),
            "the binary key did not survive the round trip"
        );

        let null_key = received[3].metadata();
        assert_eq!(
            null_key.null_partition_key,
            Some(true),
            "an explicitly null key did not set null_partition_key"
        );
        assert_eq!(null_key.partition_key, None);

        producer.close().await.unwrap();
    }
    #[tokio::test]
    async fn test_custom_routing_policy() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("topic_{}", rand::random::<u16>());
        let options = ProducerOptions {
            routing_policy: Some(RoutingPolicy::Custom(Arc::new(TestCustomRoutingPolicy {}))),
            ..Default::default()
        };
        let partition_count = 3;
        test_utils::create_partitioned_topic("public", "default", &topic, partition_count).await;

        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_options(options)
            .build()
            .await
            .unwrap();

        let key = "test";
        let message = Message {
            payload: Some("test".into()),
            partition_key: Some(key.into()),
            ..Default::default()
        };

        let CommandSendReceipt { producer_id, .. } = producer
            .send_non_blocking(message)
            .await
            .unwrap()
            .await
            .unwrap();
        for _ in 1..100 {
            let message = Message {
                payload: Some("test".into()),
                partition_key: Some(key.into()),
                ..Default::default()
            };

            let send_receipt = producer
                .send_non_blocking(message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id == producer_id);
        }
    }

    /// A producer picks up partitions added after it was built.
    ///
    /// Without this, a producer keeps routing over the partitions it started with:
    /// the new ones receive nothing, and a keyed message lands somewhere other
    /// than where a Java client — which auto-updates by default — would put it, so
    /// per-key ordering breaks across a mixed fleet.
    ///
    /// Starts at **one** partition on purpose. That was the case with no coverage
    /// at all: a one-partition topic used to build a `Single` producer, which has
    /// no partition set to grow.
    #[tokio::test]
    #[cfg_attr(not(feature = "admin-api"), ignore)]
    async fn a_producer_picks_up_partitions_added_after_it_was_built() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let admin = pulsar.admin(test_utils::admin_url()).unwrap();

        let topic = format!("persistent://public/default/grow-{}", rand::random::<u32>());
        admin
            .topics()
            .create_partitioned_topic(&topic, 1)
            .await
            .unwrap();

        let mut producer = pulsar
            .producer()
            .with_topic(&topic)
            .with_partition_refresh(Duration::from_millis(1))
            .build()
            .await
            .unwrap();

        assert_eq!(
            producer.partitions(),
            Some(vec![format!("{topic}-partition-0")]),
            "a one-partition topic is still partitioned, and can still grow"
        );

        admin
            .topics()
            .update_partitioned_topic(&topic, 4)
            .await
            .unwrap();

        // The refresh runs on the next send, not on a timer, so it takes one send
        // to notice. Send twice: the first triggers the re-check, the second is
        // routed over the grown set.
        for _ in 0..2 {
            producer
                .send_non_blocking("x")
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let mut partitions = producer.partitions().expect("still partitioned");
        partitions.sort();
        assert_eq!(
            partitions,
            (0..4)
                .map(|n| format!("{topic}-partition-{n}"))
                .collect::<Vec<_>>(),
            "the producer should have caught up with the topic"
        );

        producer.close().await.unwrap();
        admin
            .topics()
            .delete_partitioned_topic(&topic, true)
            .await
            .unwrap();
    }

    /// Opting out means opting out: no lookup, no new partitions.
    ///
    /// This is the negative control for the test above — it fails if the refresh
    /// ignores its configuration and runs unconditionally.
    #[tokio::test]
    #[cfg_attr(not(feature = "admin-api"), ignore)]
    async fn a_producer_that_opted_out_keeps_its_original_partitions() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let pulsar: Pulsar<_> = Pulsar::builder(test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let admin = pulsar.admin(test_utils::admin_url()).unwrap();

        let topic = format!(
            "persistent://public/default/nogrow-{}",
            rand::random::<u32>()
        );
        admin
            .topics()
            .create_partitioned_topic(&topic, 1)
            .await
            .unwrap();

        let mut producer = pulsar
            .producer()
            .with_topic(&topic)
            .without_partition_refresh()
            .build()
            .await
            .unwrap();

        admin
            .topics()
            .update_partitioned_topic(&topic, 4)
            .await
            .unwrap();

        for _ in 0..2 {
            producer
                .send_non_blocking("x")
                .await
                .unwrap()
                .await
                .unwrap();
        }

        assert_eq!(
            producer.partitions(),
            Some(vec![format!("{topic}-partition-0")]),
            "a producer that opted out must not re-check"
        );

        producer.close().await.unwrap();
        admin
            .topics()
            .delete_partitioned_topic(&topic, true)
            .await
            .unwrap();
    }
}
