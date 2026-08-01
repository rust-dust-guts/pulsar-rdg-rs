use std::sync::Arc;

use futures::{future::try_join_all, FutureExt};
use url::Url;

use crate::{
    connection_manager::{BrokerAddress, ConnectionManager},
    error::{ConnectionError, ServiceDiscoveryError},
    executor::Executor,
    message::proto::{
        command_lookup_topic_response, command_partitioned_topic_metadata_response,
        CommandLookupTopicResponse,
    },
};

/// Look up broker addresses for topics and partitioned topics
///
/// The ServiceDiscovery object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ServiceDiscovery<Exe: Executor> {
    manager: Arc<ConnectionManager<Exe>>,
}

/// Whether `topic` names an individual partition of a partitioned topic, i.e.
/// ends in `-partition-<digits>`.
///
/// Deliberately strict: a substring test would also match ordinary topics that
/// merely contain the word, such as `orders-partition-archive`.
fn is_topic_partition_name(topic: &str) -> bool {
    match topic.rsplit_once("-partition-") {
        Some((prefix, index)) => {
            !prefix.is_empty() && !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

impl<Exe: Executor> ServiceDiscovery<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_manager(manager: Arc<ConnectionManager<Exe>>) -> Self {
        ServiceDiscovery { manager }
    }

    /// get the broker address for a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<BrokerAddress, ServiceDiscoveryError> {
        let topic = topic.into();
        let mut proxied_query = false;
        let mut conn = self.manager.get_base_connection().await?;
        let base_url = self.manager.url.clone();
        let mut is_authoritative = false;
        let mut broker_address = self.manager.get_base_address();

        let mut current_retries = 0u32;
        let start = std::time::Instant::now();
        let operation_retry_options = self.manager.operation_retry_options.clone();

        loop {
            let response = match conn
                .sender()
                .lookup_topic(
                    topic.to_string(),
                    is_authoritative,
                    self.manager.listener_name.clone(),
                )
                .await
            {
                Ok(res) => res,
                Err(ConnectionError::Disconnected) => {
                    error!("tried to lookup a topic but connection was closed, reconnecting...");
                    conn = self.manager.get_connection(&broker_address).await?;
                    conn.sender()
                        .lookup_topic(
                            topic.to_string(),
                            is_authoritative,
                            self.manager.listener_name.clone(),
                        )
                        .await?
                }
                Err(e) => {
                    error!("tried to lookup a topic but error occrured: {:?}", e);
                    return Err(e.into());
                }
            };

            if response.response.is_none()
                || response.response
                    == Some(command_lookup_topic_response::LookupType::Failed as i32)
            {
                let error = response.error.and_then(crate::error::server_error);
                if matches!(
                    error,
                    Some(
                        crate::message::proto::ServerError::ServiceNotReady
                            | crate::message::proto::ServerError::MetadataError,
                    )
                ) {
                    // `max_retries` defaults to `None`, meaning unlimited, so the
                    // deadline is what stops a permanently-failing lookup. Without
                    // it a condition the broker will never resolve — a listener
                    // name it does not have, say, which it reports as
                    // `ServiceNotReady` — retried silently for ever instead of
                    // surfacing. Java does not retry a failed lookup at all;
                    // `BinaryProtoLookupService.findBroker` fails the future and
                    // lets the caller's operation timeout govern.
                    let elapsed = start.elapsed();
                    let within_deadline = elapsed < operation_retry_options.operation_timeout;
                    let within_retry_budget = operation_retry_options
                        .max_retries
                        .is_none_or(|max| max > current_retries);

                    if within_retry_budget && within_deadline {
                        error!("lookup({}) failed with {:?}, retrying request after {}ms (max_retries = {:?})", topic, error, operation_retry_options.retry_delay.as_millis(), operation_retry_options.max_retries);
                        current_retries += 1;
                        self.manager
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;
                        continue;
                    } else if !within_deadline {
                        error!(
                            "lookup({}) gave up after {:?}, past the {:?} operation timeout",
                            topic, elapsed, operation_retry_options.operation_timeout
                        );
                    } else {
                        error!("lookup({}) reached max retries", topic);
                    }
                }

                error!(
                    "tried to lookup a topic but error occured[{:?}]: {:?}",
                    line!(),
                    error
                );
                return Err(ServiceDiscoveryError::Query(
                    error,
                    response.message.clone(),
                ));
            }

            if current_retries > 0 {
                let dur = (std::time::Instant::now() - start).as_secs();
                log::info!(
                    "lookup({}) success after {} retries over {} seconds",
                    topic,
                    current_retries + 1,
                    dur
                );
            }
            let LookupResponse {
                broker_url,
                broker_url_tls,
                proxy,
                redirect,
                authoritative,
            } = convert_lookup_response(&response)?;
            is_authoritative = authoritative;

            // Use broker url with the same schema of url in setting
            let (broker_url_maybe_none, broker_port) = match base_url.scheme() {
                "pulsar+ssl" => (&broker_url_tls, 6651),
                "pulsar" => (&broker_url, 6650),
                other => {
                    error!("invalid scheme: {}", other);
                    return Err(ServiceDiscoveryError::NotFound);
                }
            };

            let (connection_url, broker_url) = if let Some(u) = broker_url_maybe_none {
                (
                    u.clone(),
                    format!(
                        "{}:{}",
                        u.host_str().unwrap(),
                        u.port().unwrap_or(broker_port)
                    ),
                )
            } else {
                return Err(ServiceDiscoveryError::NotFound);
            };

            // if going through a proxy, we use the base URL
            let url = if proxied_query || proxy {
                base_url.clone()
            } else {
                connection_url.clone()
            };

            broker_address = BrokerAddress {
                url,
                broker_url,
                proxy: proxied_query || proxy,
            };

            // if the response indicated a redirect, do another query
            // to the target broker
            if redirect {
                conn = self.manager.get_connection(&broker_address).await?;
                proxied_query = broker_address.proxy;
                continue;
            } else {
                let res = self
                    .manager
                    .get_connection(&broker_address)
                    .await
                    .map(|_| broker_address)
                    .map_err(ServiceDiscoveryError::Connection);
                break res;
            }
        }
    }

    /// get the number of partitions for a partitioned topic
    ///
    /// The lookup auto-creates the topic if the broker's auto-creation policy
    /// allows it. Use
    /// [`lookup_partitioned_topic_number_with_options`][Self::lookup_partitioned_topic_number_with_options]
    /// to look up metadata without that side effect.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_partitioned_topic_number<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<u32, ServiceDiscoveryError> {
        self.lookup_partitioned_topic_number_with_options(topic, true)
            .await
    }

    /// get the number of partitions for a partitioned topic, controlling whether
    /// the lookup may auto-create the topic
    ///
    /// With `metadata_auto_creation_enabled == false` (PIP-344) the broker does
    /// not create a missing topic and instead reports it as absent, surfacing here
    /// as `Query(Some(ServerError::TopicNotFound), _)`. Zero is *not* used for
    /// "missing", because zero is also the correct answer for an existing
    /// non-partitioned topic.
    ///
    /// Brokers that predate PIP-344 cannot honour the request, so the call fails
    /// with [`ConnectionError::NotSupported`] rather than creating the topic
    /// anyway.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_partitioned_topic_number_with_options<S: Into<String>>(
        &self,
        topic: S,
        metadata_auto_creation_enabled: bool,
    ) -> Result<u32, ServiceDiscoveryError> {
        let topic = topic.into();

        // Fast path: a partition of a partitioned topic is itself never partitioned,
        // so answering 0 locally saves one lookup per partition (13 -> 1 for a
        // 12-partition topic) and the matching metadata-store reads.
        //
        // Two constraints on when it may be taken:
        //
        // * It must match only a real partition suffix (`-partition-<digits>` at
        //   the end). A `contains` test also swallows ordinary topics such as
        //   `orders-partition-archive`.
        // * It must be skipped when auto-creation is disabled. The caller is then
        //   asking whether the topic exists, and answering 0 from the name alone
        //   would claim it does — and would also skip the broker-capability check
        //   this method promises to make.
        if metadata_auto_creation_enabled && is_topic_partition_name(&topic) {
            return Ok(0);
        }

        let mut connection = self.manager.get_base_connection().await?;
        let mut current_retries = 0u32;
        let start = std::time::Instant::now();
        let operation_retry_options = self.manager.operation_retry_options.clone();

        let response = loop {
            let response = match connection
                .sender()
                .lookup_partitioned_topic(&topic, metadata_auto_creation_enabled)
                .await
            {
                Ok(res) => res,
                Err(ConnectionError::Disconnected) => {
                    error!("tried to lookup a topic but connection was closed, reconnecting...");
                    connection = self.manager.get_base_connection().await?;
                    connection
                        .sender()
                        .lookup_partitioned_topic(&topic, metadata_auto_creation_enabled)
                        .await?
                }
                Err(e) => return Err(e.into()),
            };

            if response.response.is_none()
                || response.response
                    == Some(command_partitioned_topic_metadata_response::LookupType::Failed as i32)
            {
                let error = response.error.and_then(crate::error::server_error);
                if error == Some(crate::message::proto::ServerError::ServiceNotReady) {
                    if operation_retry_options.max_retries.is_none()
                        || operation_retry_options.max_retries.unwrap() > current_retries
                    {
                        error!("lookup_partitioned_topic_number({}) answered ServiceNotReady, retrying request after {}ms (max_retries = {:?})",
                    topic, operation_retry_options.retry_delay.as_millis(),
                    operation_retry_options.max_retries);

                        current_retries += 1;
                        self.manager
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;
                        continue;
                    } else {
                        error!(
                            "lookup_partitioned_topic_number({}) reached max retries",
                            topic
                        );
                    }
                }
                return Err(ServiceDiscoveryError::Query(
                    error,
                    response.message.clone(),
                ));
            }

            break response;
        };

        if current_retries > 0 {
            let dur = (std::time::Instant::now() - start).as_secs();
            log::info!(
                "lookup_partitioned_topic_number({}) success after {} retries over {} seconds",
                topic,
                current_retries + 1,
                dur
            );
        }

        match response.partitions {
            Some(partitions) => Ok(partitions),
            None => Err(ServiceDiscoveryError::Query(
                response.error.and_then(crate::error::server_error),
                response.message,
            )),
        }
    }

    /// Lookup a topic, returning a list of the partitions (if partitioned) and addresses
    /// associated with that topic.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<Vec<(String, BrokerAddress)>, ServiceDiscoveryError> {
        let topic = topic.into();
        let partitions = self.lookup_partitioned_topic_number(&topic).await?;

        trace!("Partitions for topic {}: {}", &topic, &partitions);
        let topics = match partitions {
            0 => vec![topic],
            _ => (0..partitions)
                .map(|n| format!("{}-partition-{}", &topic, n))
                .collect(),
        };
        try_join_all(topics.into_iter().map(|topic| {
            self.lookup_topic(topic.clone())
                .map(move |address_res| match address_res {
                    Err(e) => Err(e),
                    Ok(address) => Ok((topic, address)),
                })
        }))
        .await
    }
}

struct LookupResponse {
    pub broker_url: Option<Url>,
    pub broker_url_tls: Option<Url>,
    pub proxy: bool,
    pub redirect: bool,
    pub authoritative: bool,
}

/// extracts information from a lookup response
#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn convert_lookup_response(
    response: &CommandLookupTopicResponse,
) -> Result<LookupResponse, ServiceDiscoveryError> {
    let proxy = response.proxy_through_service_url.unwrap_or(false);
    let authoritative = response.authoritative.unwrap_or(false);
    let redirect =
        response.response == Some(command_lookup_topic_response::LookupType::Redirect as i32);

    let broker_url = match response.broker_service_url.as_ref() {
        Some(_u) => Some(
            Url::parse(&response.broker_service_url.clone().unwrap()).map_err(|e| {
                error!("error parsing URL: {:?}", e);
                ServiceDiscoveryError::NotFound
            })?,
        ),
        None => None,
    };

    let broker_url_tls = match response.broker_service_url_tls.as_ref() {
        Some(u) => Some(Url::parse(u).map_err(|e| {
            error!("error parsing URL: {:?}", e);
            ServiceDiscoveryError::NotFound
        })?),
        None => None,
    };

    Ok(LookupResponse {
        broker_url,
        broker_url_tls,
        proxy,
        redirect,
        authoritative,
    })
}

#[cfg(test)]
mod tests {
    use super::is_topic_partition_name;

    /// Real partition suffixes must be recognised so the fast path still works.
    #[test]
    fn recognises_real_partition_names() {
        for topic in [
            "persistent://public/default/orders-partition-0",
            "orders-partition-12",
            "a-partition-999",
        ] {
            assert!(
                is_topic_partition_name(topic),
                "{topic} is a partition name"
            );
        }
    }

    /// Regression: a `contains("-partition-")` test also matched ordinary topics,
    /// which then short-circuited to `Ok(0)` — reporting a nonexistent topic as an
    /// existing non-partitioned one and skipping the PIP-344 capability check.
    #[test]
    fn rejects_ordinary_topics_containing_the_word_partition() {
        for topic in [
            "orders-partition-archive",
            "orders-partition-",
            "orders-partition-3x",
            "orders-partition-0-backup",
            "-partition-0",
            "partition-0",
            "orders",
        ] {
            assert!(
                !is_topic_partition_name(topic),
                "{topic} must not be treated as a partition name"
            );
        }
    }

    /// A configured listener name resolves, and an unconfigured one is refused.
    ///
    /// The refusal half is the real assertion, and it doubles as this test's own
    /// negative control: the broker fails a lookup naming a listener it does not
    /// have (`NamespaceService.resolveBrokerServiceLookupResult`), so if the
    /// client stopped putting the name on `CommandLookupTopic` the lookup would
    /// succeed and this test would fail. Nothing else in the suite would notice.
    ///
    /// `scripts/start_test_broker.sh` configures the "external" listener.
    #[tokio::test]
    #[cfg_attr(not(feature = "admin-api"), ignore)]
    async fn lookups_resolve_against_the_named_listener() {
        use crate::{
            client::Pulsar, connection_manager::OperationRetryOptions, executor::TokioExecutor,
            test_utils::broker_url,
        };

        let topic = "persistent://public/default/listener-name-lookup";

        let configured: Pulsar<_> = Pulsar::builder(broker_url(), TokioExecutor)
            .with_listener_name("external")
            .build()
            .await
            .unwrap();
        let via_listener = configured.lookup_topic(topic).await.unwrap();

        // The listener advertises the address the broker already serves on, so
        // resolving through it must land on the same broker as the default path.
        let plain: Pulsar<_> = Pulsar::builder(broker_url(), TokioExecutor)
            .build()
            .await
            .unwrap();
        assert_eq!(
            via_listener.broker_url,
            plain.lookup_topic(topic).await.unwrap().broker_url
        );

        // The broker usually reports a missing listener as `ServiceNotReady`, and
        // `max_retries: Some(0)` surfaces that immediately instead of retrying to
        // the operation timeout.
        //
        // "Usually" is deliberate: the rejection is not guaranteed. Depending on
        // the path the lookup takes, an unknown listener can still come back
        // `Connect` — which is why the retry-deadline test next door drives a stub
        // rather than this condition. This assertion holds because the topic here
        // already exists, so the lookup resolves against an owned bundle; if it
        // ever starts flaking, that is the reason, and a stub is the fix.
        let unconfigured: Pulsar<_> = Pulsar::builder(broker_url(), TokioExecutor)
            .with_listener_name("no-such-listener")
            .with_operation_retry_options(OperationRetryOptions {
                max_retries: Some(0),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let err = unconfigured
            .lookup_topic(topic)
            .await
            .expect_err("the broker has no 'no-such-listener' listener");
        let message = err.to_string();
        assert!(
            message.contains("no-such-listener"),
            "the broker should name the listener it is missing, got: {message}"
        );
    }

    /// A broker that answers every lookup with `ServiceNotReady`.
    ///
    /// A stub rather than the real broker: the obvious way to provoke a
    /// permanently failing lookup is a listener name the broker does not have,
    /// but Pulsar does not reject those deterministically — depending on which
    /// path the lookup takes, an unknown listener can still come back `Connect`.
    /// A test built on that premise fails at random, which is exactly what this
    /// one did. The condition under test is the retry loop, so the broker's
    /// behaviour is worth controlling outright.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn stub_broker_that_never_becomes_ready() -> u16 {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::Framed;

        use crate::message::{
            proto::{
                base_command::Type as CommandType, command_lookup_topic_response::LookupType,
                BaseCommand, CommandConnected, CommandLookupTopicResponse, ServerError,
            },
            Codec, Message,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut framed = Framed::new(socket, Codec);
                    while let Some(Ok(msg)) = framed.next().await {
                        let reply = if msg.command.connect.is_some() {
                            BaseCommand {
                                r#type: CommandType::Connected as i32,
                                connected: Some(CommandConnected::default()),
                                ..Default::default()
                            }
                        } else if let Some(lookup) = msg.command.lookup_topic.as_ref() {
                            BaseCommand {
                                r#type: CommandType::LookupResponse as i32,
                                lookup_topic_response: Some(CommandLookupTopicResponse {
                                    request_id: lookup.request_id,
                                    response: Some(LookupType::Failed as i32),
                                    error: Some(ServerError::ServiceNotReady as i32),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }
                        } else {
                            continue;
                        };
                        if framed
                            .send(Message {
                                command: reply,
                                payload: None,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });

        port
    }

    /// A lookup the broker will never satisfy has to give up, not retry for ever.
    ///
    /// `max_retries` defaults to `None` — unlimited — so before the deadline was
    /// honoured this retried every 5 seconds for as long as the broker kept
    /// answering `ServiceNotReady`, which is to say indefinitely.
    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn a_lookup_that_can_never_succeed_gives_up_at_the_operation_timeout() {
        use std::time::{Duration, Instant};

        use crate::{
            client::Pulsar, connection_manager::OperationRetryOptions,
            error::ServiceDiscoveryError, executor::TokioExecutor,
        };

        let port = stub_broker_that_never_becomes_ready().await;
        let timeout = Duration::from_secs(2);
        let pulsar: Pulsar<_> =
            Pulsar::builder(format!("pulsar://127.0.0.1:{port}"), TokioExecutor)
                .with_operation_retry_options(OperationRetryOptions {
                    operation_timeout: timeout,
                    retry_delay: Duration::from_millis(100),
                    // Unlimited, as by default: the deadline is what must stop this.
                    max_retries: None,
                })
                .build()
                .await
                .unwrap();

        let started = Instant::now();
        let err = pulsar
            .lookup_topic("persistent://public/default/never-resolves")
            .await
            .expect_err("the stub answers every lookup with ServiceNotReady");
        let elapsed = started.elapsed();

        assert!(
            elapsed >= timeout,
            "gave up after {elapsed:?}, before the {timeout:?} deadline"
        );
        assert!(
            elapsed < timeout * 5,
            "took {elapsed:?}, far past the {timeout:?} deadline"
        );
        // The specific error matters: giving up must surface what the broker kept
        // reporting, not some unrelated failure that would also end the loop.
        assert!(
            matches!(
                err,
                crate::error::Error::ServiceDiscovery(ServiceDiscoveryError::Query(
                    Some(crate::message::proto::ServerError::ServiceNotReady),
                    _
                ))
            ),
            "expected the broker's ServiceNotReady to surface, got: {err:?}"
        );
    }
}
