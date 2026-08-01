use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::{
    channel::mpsc::SendError,
    task::{Context, Poll},
    Future, SinkExt, Stream,
};
use url::Url;

use crate::{
    client::DeserializeMessage,
    consumer::{ConsumerOptions, DeadLetterPolicy, EngineMessage, Message, TopicConsumer},
    error::Error,
    executor::Executor,
    message::proto::{command_subscribe::SubType, MessageIdData},
};

/// A client that acknowledges messages systematically
pub struct Reader<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer: TopicConsumer<T, Exe>,
    pub(crate) state: Option<State<T>>,
}

impl<T: DeserializeMessage + 'static, Exe: Executor> Unpin for Reader<T, Exe> {}

pub enum State<T: DeserializeMessage> {
    PollingConsumer,
    PollingAck(
        Message<T>,
        Pin<Box<dyn Future<Output = Result<(), SendError>> + Send + Sync>>,
    ),
}

impl<T: DeserializeMessage + 'static, Exe: Executor> Stream for Reader<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.state.take().unwrap() {
            State::PollingConsumer => match Pin::new(&mut this.consumer).poll_next(cx) {
                Poll::Pending => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Pending
                }

                Poll::Ready(None) => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Ready(None)
                }

                Poll::Ready(Some(Ok(msg))) => {
                    let mut acker = this.consumer.acker();
                    let message_id = msg.message_id().clone();
                    this.state = Some(State::PollingAck(
                        msg,
                        Box::pin(
                            async move { acker.send(EngineMessage::Ack(message_id, false)).await },
                        ),
                    ));
                    Pin::new(this).poll_next(cx)
                }

                Poll::Ready(Some(Err(e))) => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Ready(Some(Err(e)))
                }
            },
            State::PollingAck(msg, mut ack_fut) => match ack_fut.as_mut().poll(cx) {
                Poll::Pending => {
                    this.state = Some(State::PollingAck(msg, ack_fut));
                    Poll::Pending
                }

                Poll::Ready(res) => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Ready(Some(
                        res.map_err(|err| Error::Consumer(err.into())).map(|()| msg),
                    ))
                }
            },
        }
    }
}

impl<T: DeserializeMessage, Exe: Executor> Reader<T, Exe> {
    // this is totally useless as calling ConsumerBuilder::new(&pulsar_client)
    // does just the same
    /*
    /// creates a [ReaderBuilder] from a client instance
    pub fn builder(pulsar: &Pulsar<Exe>) -> ConsumerBuilder<Exe> {
        ConsumerBuilder::new(pulsar)
    }
    */

    /// test that the connections to the Pulsar brokers are still valid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        self.consumer.check_connection().await
    }

    /// returns topic this reader is subscribed on
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> String {
        self.consumer.topic()
    }

    /// returns a list of broker URLs this reader is connected to
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connections(&mut self) -> Result<Url, Error> {
        Ok(self.consumer.connection().await?.url().clone())
    }

    /// returns the consumer's configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ConsumerOptions {
        &self.consumer.config.options
    }

    // is this necessary?
    /// returns the consumer's dead letter policy options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn dead_letter_policy(&self) -> Option<&DeadLetterPolicy> {
        self.consumer.dead_letter_policy.as_ref()
    }

    /// returns the readers's subscription name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn subscription(&self) -> &str {
        &self.consumer.config.subscription
    }

    /// returns the reader's subscription type
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn sub_type(&self) -> SubType {
        self.consumer.config.sub_type
    }

    /// returns the reader's batch size
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn batch_size(&self) -> Option<u32> {
        self.consumer.config.batch_size
    }

    /// returns the reader's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn reader_name(&self) -> Option<&str> {
        self.consumer.config.consumer_name.as_deref()
    }

    /// returns the reader's id
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn reader_id(&self) -> u64 {
        self.consumer.consumer_id
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        self.consumer.seek(message_id, timestamp).await
    }

    /// returns the date of the last message reception
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.consumer.last_message_received()
    }

    /// Whether the topic holds a message this reader has not returned yet.
    ///
    /// The canonical use is draining a topic and stopping at the end, which the
    /// stream alone cannot express — it blocks identically whether the topic is
    /// merely quiet or fully read:
    ///
    /// ```rust,no_run
    /// # async fn run(mut reader: pulsar::Reader<String, pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// use futures::StreamExt;
    /// while reader.has_message_available().await? {
    ///     if let Some(msg) = reader.next().await {
    ///         println!("{:?}", msg?.deserialize());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Costs one round trip to the broker per call.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn has_message_available(&mut self) -> Result<bool, Error> {
        self.consumer.has_message_available().await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<MessageIdData, Error> {
        self.consumer.get_last_message_id().await
    }

    /// returns the current number of messages received
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.consumer.messages_received()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use tokio::time::timeout;

    use crate::{
        consumer::{DeadLetterPolicy, InitialPosition, Message},
        producer,
        proto::MessageIdData,
        reader::Reader,
        ConsumerOptions, DeserializeMessage, Error, Executor, Payload, Pulsar, SerializeMessage,
        SubType, TokioExecutor,
    };

    #[derive(Serialize, Deserialize)]
    struct TestData {
        data: String,
    }

    impl SerializeMessage for &TestData {
        fn serialize_message(input: Self) -> Result<producer::Message, Error> {
            let payload = serde_json::to_vec(&input).map_err(|e| Error::Custom(e.to_string()))?;
            Ok(producer::Message {
                payload: Some(payload),
                ..Default::default()
            })
        }
    }

    impl DeserializeMessage for TestData {
        type Output = Result<TestData, serde_json::Error>;

        fn deserialize_message(payload: &Payload) -> Self::Output {
            serde_json::from_slice(&payload.data)
        }
    }

    #[tokio::test]
    async fn reader() {
        let addr = crate::test_utils::broker_url();
        let topic = format!("test_reader_{}", rand::random::<u16>());
        let dead_letter_policy = DeadLetterPolicy {
            max_redeliver_count: 1,
            dead_letter_topic: format!("{}_dead_letter", &topic),
        };
        let client: Pulsar<_> = Pulsar::builder(&addr, TokioExecutor).build().await.unwrap();
        let mut reader: Reader<TestData, _> = client
            .reader()
            .with_topic(&topic)
            .with_consumer_name("test_reader")
            .with_subscription("test_reader_subscription")
            .with_dead_letter_policy(dead_letter_policy)
            .with_options(ConsumerOptions::default())
            .into_reader()
            .await
            .unwrap();
        assert!(reader.check_connection().await.is_ok());
        assert_eq!(reader.topic(), topic);

        let url = reader.connections().await.unwrap();
        assert_eq!(url.as_str(), addr);

        let option = reader.options();
        assert_eq!(option.initial_position, InitialPosition::Latest);

        let policy = reader.dead_letter_policy().unwrap();
        assert_eq!(policy.max_redeliver_count, 1);
        assert_eq!(policy.dead_letter_topic, format!("{}_dead_letter", &topic));
        assert_eq!(reader.subscription(), "test_reader_subscription");
        assert_eq!(reader.sub_type(), SubType::Exclusive);
        assert_eq!(reader.batch_size(), None);
        assert_eq!(reader.reader_name().unwrap(), "test_reader");
        // No assertion on `reader_id()`'s value: it comes from a process-wide
        // counter that starts at zero, so the previous `> 0` check only held when
        // another test had created a consumer first and failed when this test ran
        // alone. Zero is a valid consumer id, and there is nothing else about it
        // worth asserting here.

        let message = TestData {
            data: "test_reader_data".to_string(),
        };
        let message_count = 10;
        let mut lastest_message_id: [u64; 2] = [0, 0];
        for index in 0..message_count {
            let receipt = client.send(&topic, &message).await.unwrap().await.unwrap();
            let message_id = receipt.message_id.unwrap();
            println!(
                "producer sends done, message_id: {}:{}",
                message_id.ledger_id, message_id.entry_id
            );
            if index == message_count - 1 {
                lastest_message_id[0] = message_id.ledger_id;
                lastest_message_id[1] = message_id.entry_id;
            }
        }

        let mut seek_message_id: Option<MessageIdData> = None;
        let messages = reader_messages(&mut reader, message_count, 5000).await;
        assert!(messages.len() <= message_count);
        for (i, data) in messages.into_iter().enumerate() {
            let value = data.deserialize().unwrap();
            assert_eq!(value.data, "test_reader_data".to_string());
            if i <= message_count / 2 {
                seek_message_id = Some(data.message_id.id.clone());
            }
        }
        let time = reader.last_message_received().unwrap();
        assert!(time <= chrono::Utc::now());

        let last_message_id_data = reader.get_last_message_id().await.unwrap();
        println!("last message id: {:?}", last_message_id_data);
        assert_eq!(last_message_id_data.ledger_id, lastest_message_id[0]);
        assert_eq!(last_message_id_data.entry_id, lastest_message_id[1]);

        let received = reader.messages_received();
        assert!(received <= message_count as u64);

        // seek to half message
        reader.seek(seek_message_id, None).await.unwrap();
        let seek_message = reader_messages(&mut reader, message_count / 2, 5000).await;
        assert!(seek_message.len() <= message_count / 2);
        crate::test_utils::delete_topic("public", "default", &topic).await;
    }

    async fn reader_messages(
        reader: &mut Reader<TestData, impl Executor>,
        max_num_messages: usize,
        receive_timeout_ms: u64,
    ) -> Vec<Message<TestData>> {
        let mut messages = Vec::new();
        loop {
            match timeout(Duration::from_millis(receive_timeout_ms), reader.next()).await {
                Ok(Some(msg)) => {
                    let msg = msg.unwrap();
                    messages.push(msg);
                    if messages.len() >= max_num_messages {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    info!("timed out waiting for reading messages: {}", e);
                    break;
                }
            }
        }
        messages
    }

    /// `start_message_id` names a resume cursor, so by default reading begins
    /// *after* it — the message itself is not redelivered.
    ///
    /// Before this, the start message came back too, so a caller storing "the
    /// last id I processed" and restarting from it processed that message twice.
    /// `start_message_id_inclusive` is the opt-in to the old behaviour, and is
    /// Java's `startMessageIdInclusive()`.
    async fn assert_start_message_boundary(inclusive: bool, want: &[&str]) {
        assert_start_message_boundary_on(
            inclusive,
            want,
            &format!(
                "persistent://public/default/startid-{}",
                rand::random::<u32>()
            ),
        )
        .await;
    }

    async fn assert_start_message_boundary_on(inclusive: bool, want: &[&str], topic: &str) {
        let client: Pulsar<_> = Pulsar::builder(crate::test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let mut ids = Vec::new();
        for i in 0..5u32 {
            let receipt = client
                .send(topic, format!("m{i}"))
                .await
                .unwrap()
                .await
                .unwrap();
            ids.push(receipt.message_id.unwrap());
        }

        let mut reader: Reader<String, _> = client
            .consumer()
            .with_topic(topic)
            .with_consumer_name("start-id")
            .with_options(ConsumerOptions {
                start_message_id: Some(ids[2].clone()),
                start_message_id_inclusive: inclusive,
                ..Default::default()
            })
            .into_reader()
            .await
            .unwrap();

        let mut got: Vec<String> = Vec::new();
        while got.len() < want.len() + 1 {
            match tokio::time::timeout(Duration::from_secs(2), reader.next()).await {
                Ok(Some(Ok(m))) => got.push(m.deserialize().unwrap()),
                _ => break,
            }
        }
        assert_eq!(got, want, "inclusive = {inclusive}");

        crate::test_utils::delete_topic("public", "default", topic.rsplit('/').next().unwrap())
            .await;
    }

    #[tokio::test]
    async fn reading_from_a_start_id_skips_that_message_by_default() {
        assert_start_message_boundary(false, &["m3", "m4"]).await;
    }

    #[tokio::test]
    async fn reading_from_a_start_id_can_include_it() {
        assert_start_message_boundary(true, &["m2", "m3", "m4"]).await;
    }

    /// The drain-to-end loop: `has_message_available` must go false exactly when
    /// the backlog runs out, and true again when something new is published.
    ///
    /// The empty topic is covered separately by
    /// `has_message_available_is_false_on_an_empty_topic`.
    #[tokio::test]
    async fn has_message_available_tracks_the_end_of_the_topic() {
        let client: Pulsar<_> = Pulsar::builder(crate::test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!(
            "persistent://public/default/hasmsg-{}",
            rand::random::<u32>()
        );
        // One message, so the drain below has exactly one to find.
        client
            .send(&topic, "seed".to_string())
            .await
            .unwrap()
            .await
            .unwrap();

        let mut reader: Reader<String, _> = client
            .consumer()
            .with_topic(&topic)
            .with_consumer_name("hasmsg")
            .with_options(ConsumerOptions {
                initial_position: crate::consumer::InitialPosition::Earliest,
                ..Default::default()
            })
            .into_reader()
            .await
            .unwrap();

        assert!(
            reader.has_message_available().await.unwrap(),
            "one message was published and none read"
        );

        let mut drained = 0;
        while reader.has_message_available().await.unwrap() {
            match tokio::time::timeout(Duration::from_secs(5), reader.next()).await {
                Ok(Some(Ok(_))) => drained += 1,
                other => panic!("expected a message while one was reported available: {other:?}"),
            }
        }
        assert_eq!(drained, 1, "should have drained exactly what was published");

        // Publishing again must flip it back.
        client
            .send(&topic, "more".to_string())
            .await
            .unwrap()
            .await
            .unwrap();
        assert!(
            reader.has_message_available().await.unwrap(),
            "a newly published message must be reported available"
        );

        crate::test_utils::delete_topic("public", "default", topic.rsplit('/').next().unwrap())
            .await;
    }

    /// A reader that has read nothing on a topic with nothing to read.
    // `cfg`, not `cfg_attr(..., ignore)`: the body uses the admin client, so
    // without the feature this must not be *compiled*, not merely skipped at run
    // time. `ignore` alone left `cargo test` failing to build.
    #[cfg(feature = "admin-api")]
    #[tokio::test]
    async fn has_message_available_is_false_on_an_empty_topic() {
        use crate::admin::AdminClient;

        let client: Pulsar<_> = Pulsar::builder(crate::test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!(
            "persistent://public/default/hasmsg-empty-{}",
            rand::random::<u32>()
        );
        let admin: AdminClient = client.admin(crate::test_utils::admin_url()).unwrap();
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        let mut reader: Reader<String, _> = client
            .consumer()
            .with_topic(&topic)
            .with_consumer_name("hasmsg-empty")
            .with_options(ConsumerOptions {
                initial_position: crate::consumer::InitialPosition::Earliest,
                ..Default::default()
            })
            .into_reader()
            .await
            .unwrap();

        assert!(
            !reader.has_message_available().await.unwrap(),
            "an empty topic has nothing available"
        );

        crate::test_utils::delete_topic("public", "default", topic.rsplit('/').next().unwrap())
            .await;
    }

    /// Regression: the filter used to be gated on the topic string starting with
    /// `persistent://`, so an unqualified name — which Pulsar accepts and expands
    /// to the persistent domain itself — skipped the filter entirely and silently
    /// redelivered the start message.
    #[tokio::test]
    async fn a_start_id_is_exclusive_for_an_unqualified_topic_name_too() {
        let topic = format!("startid-short-{}", rand::random::<u32>());
        assert_start_message_boundary_on(false, &["m3", "m4"], &topic).await;
    }

    /// Regression: a partly-read batch still has messages available.
    ///
    /// Every message in a batch shares one entry, so once the first is delivered
    /// the broker's last id already equals the reader's position and a
    /// position-only comparison reports the topic drained — losing the rest of the
    /// batch from a `while has_message_available()` loop.
    #[tokio::test]
    async fn has_message_available_sees_the_rest_of_a_partly_read_batch() {
        use crate::producer::ProducerOptions;

        let client: Pulsar<_> = Pulsar::builder(crate::test_utils::broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!(
            "persistent://public/default/batched-hasmsg-{}",
            rand::random::<u32>()
        );

        const BATCH: usize = 10;
        let mut producer = client
            .producer()
            .with_topic(&topic)
            .with_options(ProducerOptions {
                batch_size: Some(BATCH as u32),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        for i in 0..BATCH {
            producer.send_non_blocking(format!("m{i}")).await.unwrap();
        }
        producer.send_batch().await.unwrap();

        let mut reader: Reader<String, _> = client
            .consumer()
            .with_topic(&topic)
            .with_consumer_name("batched-hasmsg")
            .with_options(ConsumerOptions {
                initial_position: crate::consumer::InitialPosition::Earliest,
                ..Default::default()
            })
            .into_reader()
            .await
            .unwrap();

        // Drain by asking first, exactly as the documented loop does.
        let mut drained = 0;
        while reader.has_message_available().await.unwrap() {
            match tokio::time::timeout(Duration::from_secs(5), reader.next()).await {
                Ok(Some(Ok(_))) => drained += 1,
                other => panic!("a message was reported available but none arrived: {other:?}"),
            }
            if drained > BATCH {
                break;
            }
        }
        assert_eq!(
            drained, BATCH,
            "the whole batch should be drained, not just its first message"
        );

        producer.close().await.unwrap();
        crate::test_utils::delete_topic("public", "default", topic.rsplit('/').next().unwrap())
            .await;
    }
}
