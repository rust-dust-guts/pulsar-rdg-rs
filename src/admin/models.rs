//! Typed request and response bodies for the Pulsar Admin REST API.
//!
//! Field names mirror the Java classes in
//! `org.apache.pulsar.common.policies.data` so the JSON matches the broker
//! exactly. Optional fields are `Option<_>` and skipped when serializing, because
//! the broker rejects some policy bodies that carry explicit nulls, and because
//! omitting a field is how "leave unchanged" is expressed.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// How a proxy routes a client connection to a broker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyProtocol {
    SNI,
}

/// Connection details for a Pulsar cluster.
///
/// Mirrors `ClusterData`. `service_url` (HTTP) and `broker_service_url` (binary)
/// are the two that matter for a minimal cluster definition.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_url_tls: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_service_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_service_url_tls: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_service_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication_plugin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication_parameters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_protocol: Option<ProxyProtocol>,
    /// Ordered, so round-trips preserve the operator's declared order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_cluster_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_client_tls_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_allow_insecure_connection: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_client_trust_certs_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_client_key_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_client_certificate_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listener_name: Option<String>,
}

impl ClusterData {
    /// A cluster reachable at the given HTTP admin/service URL.
    pub fn with_service_url(service_url: impl Into<String>) -> Self {
        ClusterData {
            service_url: Some(service_url.into()),
            ..Default::default()
        }
    }
}

/// Whether a cluster is being migrated, and where to.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterUrl {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_url_tls: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_service_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_service_url_tls: Option<String>,
}

/// Cluster-level migration state, as returned by the `migrate` endpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterPolicies {
    #[serde(default)]
    pub migrated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrated_cluster_url: Option<ClusterUrl>,
}

/// Administrative roles and cluster allow-list for a tenant.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantInfo {
    /// Roles permitted to administer the tenant.
    #[serde(default)]
    pub admin_roles: BTreeSet<String>,
    /// Clusters the tenant may create namespaces in.
    #[serde(default)]
    pub allowed_clusters: BTreeSet<String>,
}

impl TenantInfo {
    /// A tenant allowed on `clusters`, with no admin roles.
    pub fn with_clusters<I, S>(clusters: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        TenantInfo {
            admin_roles: BTreeSet::new(),
            allowed_clusters: clusters.into_iter().map(Into::into).collect(),
        }
    }
}

/// A named group of brokers that share a failure domain.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureDomain {
    #[serde(default)]
    pub brokers: BTreeSet<String>,
}

/// Identity and service URL of a broker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_url: Option<String>,
}

/// Rack placement for a bookie.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookieInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// One entry of the bookie rack-placement listing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBookieInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// All bookies known to the cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookiesClusterInfo {
    #[serde(default)]
    pub bookies: Vec<RawBookieInfo>,
}

/// Rate limits shared by the namespaces in a resource group.
///
/// The wire names are plural (`publishRateInMsgs`) even though the Java accessors
/// are singular; verified against a live broker, which silently ignores a body
/// whose field names do not match. `-1` means unlimited and is what the broker
/// reports for a limit that was never set.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceGroup {
    #[serde(rename = "publishRateInMsgs", skip_serializing_if = "Option::is_none")]
    pub publish_rate_in_msgs: Option<i32>,
    #[serde(rename = "publishRateInBytes", skip_serializing_if = "Option::is_none")]
    pub publish_rate_in_bytes: Option<i64>,
    #[serde(rename = "dispatchRateInMsgs", skip_serializing_if = "Option::is_none")]
    pub dispatch_rate_in_msgs: Option<i32>,
    #[serde(
        rename = "dispatchRateInBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub dispatch_rate_in_bytes: Option<i64>,
}

/// Per-bundle resource quota.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuota {
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub bandwidth_in: f64,
    #[serde(default)]
    pub bandwidth_out: f64,
    #[serde(default)]
    pub memory: f64,
    /// Whether the quota was set explicitly or derived by the load manager.
    #[serde(default)]
    pub dynamic: bool,
}

/// Which brokers a namespace may be placed on.
///
/// Unlike most policy types this one is **snake_case** on the wire; a camelCase
/// body is rejected with HTTP 400. Verified against a live broker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceIsolationData {
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub primary: Vec<String>,
    #[serde(default)]
    pub secondary: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_failover_policy: Option<AutoFailoverPolicyData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unload_scope: Option<String>,
}

/// Failover behaviour for a namespace isolation policy. Snake_case on the wire,
/// like its containing [`NamespaceIsolationData`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoFailoverPolicyData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_type: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

/// A broker's isolation-policy assignment.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerNamespaceIsolationData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_name: Option<String>,
    #[serde(default)]
    pub policy_name: Option<String>,
    #[serde(default)]
    pub is_primary: bool,
    #[serde(default)]
    pub namespace_regex: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A standalone broker answers `load-report` with 204, so the live test can
    /// only assert "absent or decodes". This pins the shape against the field names
    /// in Java's `LocalBrokerData`, including `lastStats`, which no rename rule
    /// produces from `bundle_stats`.
    #[test]
    fn load_manager_report_decodes_the_java_shape() {
        let report: LoadManagerReport = serde_json::from_str(
            r#"{"brokerId":"broker-1:8080","webServiceUrl":"http://broker-1:8080",
                "pulsarServiceUrl":"pulsar://broker-1:6650",
                "brokerVersionString":"5.0.0",
                "cpu":{"usage":12.5,"limit":800.0},
                "memory":{"usage":1024.0,"limit":4096.0},
                "directMemory":{"usage":16.0,"limit":2048.0},
                "bandwidthIn":{"usage":1.5,"limit":100.0},
                "bandwidthOut":{"usage":2.5,"limit":100.0},
                "msgThroughputIn":10.0,"msgThroughputOut":20.0,
                "msgRateIn":1.0,"msgRateOut":2.0,
                "lastUpdate":1700000000000,
                "numTopics":7,"numBundles":3,"numConsumers":4,"numProducers":5,
                "bundles":["public/default/0x00000000_0xffffffff"],
                "lastStats":{"public/default/0x00000000_0xffffffff":
                    {"msgRateIn":1.0,"msgThroughputIn":10.0,"msgRateOut":2.0,
                     "msgThroughputOut":20.0,"consumerCount":4,"producerCount":5,
                     "topics":7,"cacheSize":128}}}"#,
        )
        .unwrap();

        assert_eq!(report.broker_id.as_deref(), Some("broker-1:8080"));
        assert_eq!(report.cpu.limit, 800.0);
        assert_eq!(report.direct_memory.usage, 16.0);
        assert_eq!(report.num_topics, 7);
        assert_eq!(report.bundles.len(), 1);
        let bundle = report
            .bundle_stats
            .get("public/default/0x00000000_0xffffffff")
            .expect("`lastStats` did not decode into bundle_stats");
        assert_eq!(bundle.consumer_count, 4);
        assert_eq!(bundle.cache_size, 128);
    }

    /// A TLS-only lookup carries its endpoints in `brokerUrlTls` / `httpUrlTls`.
    /// Without those fields the response decoded "successfully" while dropping the
    /// only broker URL a TLS client could use.
    #[test]
    fn tls_lookup_endpoints_decode() {
        let data: TopicLookupResult = serde_json::from_str(
            r#"{"brokerId":"b1","brokerUrlTls":"pulsar+ssl://b1:6651",
                "httpUrlTls":"https://b1:8443","nativeUrl":"pulsar://b1:6650"}"#,
        )
        .unwrap();
        assert_eq!(
            data.broker_url_tls.as_deref(),
            Some("pulsar+ssl://b1:6651"),
            "the TLS broker URL was dropped: {data:?}"
        );
        assert_eq!(data.http_url_tls.as_deref(), Some("https://b1:8443"));
    }

    /// The function and connector configs are accepted by Pulsar's object mapper
    /// even when a field name is wrong — unknown properties are ignored — so a
    /// rename regression silently drops the setting instead of failing. Names come
    /// from `FunctionConfig` and `SinkConfig`.
    #[test]
    fn connector_subscription_fields_use_their_real_wire_names() {
        let function = FunctionConfig {
            subscription_name: Some("s".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_string(&function).unwrap();
        assert!(json.contains(r#""subName":"s""#), "{json}");
        assert!(!json.contains("subscriptionName"), "{json}");
        assert_eq!(
            serde_json::from_str::<FunctionConfig>(&json).unwrap(),
            function
        );

        let sink = SinkConfig {
            subscription_name: Some("s".to_string()),
            ..Default::default()
        };
        let json = serde_json::to_string(&sink).unwrap();
        assert!(json.contains(r#""sourceSubscriptionName":"s""#), "{json}");
        assert_eq!(serde_json::from_str::<SinkConfig>(&json).unwrap(), sink);
    }

    /// Per-instance counters are nested under `metrics`; reading them flat gave
    /// zeroes for every instance while still reporting a successful decode.
    #[test]
    fn function_stats_instances_are_nested_under_metrics() {
        let stats: FunctionStats = serde_json::from_str(
            r#"{"receivedTotal":10,"processedSuccessfullyTotal":9,"lastInvocation":123,
                "1min":{"receivedTotal":4},
                "instances":[{"instanceId":0,
                              "metrics":{"receivedTotal":10,"processedSuccessfullyTotal":9,
                                         "1min":{"receivedTotal":4},"lastInvocation":123,
                                         "userMetrics":{"m":1.5}}}]}"#,
        )
        .unwrap();
        assert_eq!(stats.received_total, 10);
        assert_eq!(stats.one_min.received_total, 4);
        assert_eq!(stats.last_invocation, Some(123));
        let instance = &stats.instances[0];
        assert_eq!(
            instance.metrics.received_total, 10,
            "per-instance counters decoded as zero: {instance:?}"
        );
        assert_eq!(instance.metrics.one_min.received_total, 4);
        assert_eq!(instance.metrics.user_metrics.get("m"), Some(&1.5));
    }

    /// `byteValue` is a Java `byte[]`, i.e. base64 text — not a JSON number array.
    #[test]
    fn function_state_byte_value_is_base64() {
        let state = FunctionState {
            key: Some("k".to_string()),
            byte_value: Some(vec![0, 1, 255]),
            ..Default::default()
        };
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains(r#""byteValue":"AAH/""#), "{json}");
        assert_eq!(serde_json::from_str::<FunctionState>(&json).unwrap(), state);
    }

    /// Transaction stats are almost entirely `#[serde(default)]`, so a wrong wire
    /// name decodes to zero/`None`/empty rather than failing. These fixtures use the
    /// exact field names from the Java classes in
    /// `org.apache.pulsar.common.policies.data`, so a rename regression fails here
    /// instead of silently returning empty stats.
    #[test]
    fn transaction_stats_wire_names_are_pinned() {
        let coordinator: TransactionCoordinatorStats = serde_json::from_str(
            r#"{"state":"Ready","leastSigBits":7,"lowWaterMark":3,"ongoingTxnSize":2,
                "recoverStartTime":10,"recoverEndTime":20}"#,
        )
        .unwrap();
        assert_eq!(coordinator.state.as_deref(), Some("Ready"));
        assert_eq!(coordinator.least_sig_bits, 7);
        assert_eq!(coordinator.low_water_mark, 3);
        assert_eq!(coordinator.ongoing_txn_size, 2);
        assert_eq!(coordinator.recover_start_time, 10);
        assert_eq!(coordinator.recover_end_time, 20);

        let metadata: TransactionMetadata = serde_json::from_str(
            r#"{"txnId":"1:2","status":"OPEN","openTimestamp":5,"timeoutAt":6,"owner":"o",
                "producedPartitions":{"persistent://a/b/c":{"startPosition":"1:0","aborted":false}},
                "ackedPartitions":{"persistent://a/b/c":{"sub":{"cumulativeAckPosition":"1:1"}}}}"#,
        )
        .unwrap();
        assert_eq!(metadata.txn_id.as_deref(), Some("1:2"));
        assert_eq!(metadata.status.as_deref(), Some("OPEN"));
        assert_eq!(metadata.open_timestamp, 5);
        assert_eq!(metadata.timeout_at, 6);
        assert_eq!(metadata.owner.as_deref(), Some("o"));
        // Reach into the nested values too — every one of these is `serde(default)`,
        // so a wrong inner name decodes to an empty map that a length check on the
        // outer map would still accept.
        assert_eq!(
            metadata.produced_partitions["persistent://a/b/c"]
                .start_position
                .as_deref(),
            Some("1:0")
        );
        assert!(!metadata.produced_partitions["persistent://a/b/c"].aborted);
        assert_eq!(
            metadata.acked_partitions["persistent://a/b/c"]["sub"]
                .cumulative_ack_position
                .as_deref(),
            Some("1:1")
        );

        let buffer: TransactionBufferStats = serde_json::from_str(
            r#"{"state":"Ready","maxReadPosition":"1:2","lastSnapshotTimestamps":99,
                "lowWaterMarks":{"0":4},"ongoingTxnSize":1,"totalAbortedTransactions":8,
                "snapshotType":"Segment",
                "segmentsStats":{"segmentsSize":2,"currentSegmentCapacity":10,
                                 "unsealedAbortTxnIDSize":3,"lastTookSnapshotSegmentTimestamp":77,
                                 "segmentStats":[{"lastTxnID":"1:2","persistentPosition":"3:4"}]}}"#,
        )
        .unwrap();
        assert_eq!(buffer.state.as_deref(), Some("Ready"));
        assert_eq!(buffer.max_read_position.as_deref(), Some("1:2"));
        assert_eq!(buffer.snapshot_type.as_deref(), Some("Segment"));
        assert_eq!(buffer.ongoing_txn_size, 1);
        assert_eq!(buffer.last_snapshot_timestamps, 99);
        assert_eq!(buffer.low_water_marks.get("0"), Some(&4));
        assert_eq!(buffer.total_aborted_transactions, 8);
        let segments = buffer.segments_stats.expect("segmentsStats must decode");
        assert_eq!(segments.segments_size, 2);
        assert_eq!(segments.unsealed_abort_txn_id_size, 3);
        assert_eq!(segments.current_segment_capacity, 10);
        assert_eq!(segments.last_took_snapshot_segment_timestamp, 77);
        // Every field, not just the vector length: the previous fixture invented
        // the same wrong keys as the model, so both agreed on a shape the broker
        // never sends and the assertion proved nothing.
        assert_eq!(
            segments.segment_stats,
            vec![SegmentStats {
                last_txn_id: Some("1:2".to_string()),
                persistent_position: Some("3:4".to_string()),
            }]
        );

        let pending: TransactionPendingAckStats =
            serde_json::from_str(r#"{"state":"Ready","lowWaterMarks":{"0":2},"ongoingTxnSize":1}"#)
                .unwrap();
        assert_eq!(pending.state.as_deref(), Some("Ready"));
        assert_eq!(pending.low_water_marks.get("0"), Some(&2));
        assert_eq!(pending.ongoing_txn_size, 1);

        let internal: TransactionCoordinatorInternalStats = serde_json::from_str(
            r#"{"transactionLogStats":{"managedLedgerName":"ml","managedLedgerInternalStats":{}}}"#,
        )
        .unwrap();
        let log = internal
            .transaction_log_stats
            .expect("transactionLogStats must decode");
        assert_eq!(log.managed_ledger_name.as_deref(), Some("ml"));
        assert!(log.managed_ledger_internal_stats.is_some());

        let buf_internal: TransactionBufferInternalStats = serde_json::from_str(
            r#"{"snapshotType":"Segment",
                "singleSnapshotSystemTopicInternalStats":{"managedLedgerName":"a"},
                "segmentInternalStats":{"managedLedgerName":"b"},
                "segmentIndexInternalStats":{"managedLedgerName":"c"}}"#,
        )
        .unwrap();
        assert!(buf_internal.segment_internal_stats.is_some());
        assert!(buf_internal.segment_index_internal_stats.is_some());
    }

    /// The broker's routes end in `.../{mostSigBits}/{leastSigBits}` — two segments.
    #[test]
    fn txn_id_renders_two_path_segments() {
        assert_eq!(
            TxnId {
                most_sig_bits: 1,
                least_sig_bits: 2
            }
            .as_segments(),
            ["1".to_string(), "2".to_string()]
        );
    }

    /// Every field is `#[serde(default)]`, so a wrong wire name would decode to
    /// `0.0` instead of failing — and the proxy's rates really are `0.0` in
    /// practice, because it calculates them from a one-shot task scheduled 60s
    /// after startup. Only a unit test on the exact names can catch that.
    ///
    /// Names taken from `org.apache.pulsar.proxy.stats.TopicStats`.
    #[test]
    fn proxy_topic_stats_field_names_are_pinned() {
        let decoded: ProxyTopicStats = serde_json::from_str(
            r#"{"msgRateIn":1.5,"msgByteIn":2.5,"msgRateOut":3.5,"msgByteOut":4.5}"#,
        )
        .unwrap();
        assert_eq!(
            decoded,
            ProxyTopicStats {
                msg_rate_in: 1.5,
                msg_byte_in: 2.5,
                msg_rate_out: 3.5,
                msg_byte_out: 4.5,
            }
        );
    }

    /// Names taken from `org.apache.pulsar.proxy.stats.ConnectionStats`. The
    /// addresses are Netty `SocketAddress` renderings, hence the leading `/`.
    #[test]
    fn proxy_connection_stats_field_names_are_pinned() {
        let decoded: ProxyConnectionStats = serde_json::from_str(
            r#"{"requestRate":1.5,"byteRate":2.5,"clientAddress":"/127.0.0.1:54321",
                "brokerAddress":"/127.0.0.1:6650"}"#,
        )
        .unwrap();
        assert_eq!(
            decoded,
            ProxyConnectionStats {
                request_rate: 1.5,
                byte_rate: 2.5,
                client_address: Some("/127.0.0.1:54321".to_string()),
                broker_address: Some("/127.0.0.1:6650".to_string()),
            }
        );

        // Either address is null while a connection is still being set up.
        let partial: ProxyConnectionStats =
            serde_json::from_str(r#"{"requestRate":0.0,"byteRate":0.0}"#).unwrap();
        assert_eq!(partial, ProxyConnectionStats::default());
    }

    /// The phase is `SCREAMING_SNAKE_CASE` on the wire and the target is camelCase.
    #[test]
    fn migration_state_wire_format_is_pinned() {
        let decoded: MigrationState =
            serde_json::from_str(r#"{"phase":"COPYING","targetUrl":"oxia://h:6648/pulsar"}"#)
                .unwrap();
        assert_eq!(decoded.phase, MigrationPhase::Copying);
        assert_eq!(decoded.target_url.as_deref(), Some("oxia://h:6648/pulsar"));
        assert_eq!(
            serde_json::from_str::<MigrationState>(r#"{"phase":"NOT_STARTED"}"#)
                .unwrap()
                .phase,
            MigrationPhase::NotStarted
        );
    }

    /// Optional fields must be omitted rather than serialized as null: the broker
    /// rejects some policy bodies containing explicit nulls.
    #[test]
    fn cluster_data_omits_unset_fields() {
        let json = serde_json::to_string(&ClusterData::with_service_url("http://h:8080")).unwrap();
        assert_eq!(json, r#"{"serviceUrl":"http://h:8080"}"#);
    }

    /// Field names must be camelCase to match the broker's JSON.
    #[test]
    fn cluster_data_uses_camel_case() {
        let data = ClusterData {
            broker_service_url: Some("pulsar://h:6650".to_string()),
            broker_client_tls_enabled: Some(true),
            ..Default::default()
        };
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"brokerServiceUrl\""), "{json}");
        assert!(json.contains("\"brokerClientTlsEnabled\""), "{json}");
    }

    /// Round-tripping must preserve every field that was set.
    #[test]
    fn cluster_data_round_trips() {
        let data = ClusterData {
            service_url: Some("http://h:8080".to_string()),
            broker_service_url: Some("pulsar://h:6650".to_string()),
            peer_cluster_names: Some(vec!["a".to_string(), "b".to_string()]),
            proxy_protocol: Some(ProxyProtocol::SNI),
            ..Default::default()
        };
        let json = serde_json::to_string(&data).unwrap();
        assert_eq!(serde_json::from_str::<ClusterData>(&json).unwrap(), data);
    }

    /// The broker sends fields this client does not model; they must be ignored
    /// rather than failing the whole response.
    #[test]
    fn unknown_fields_are_ignored() {
        let json = r#"{"serviceUrl":"http://h:8080","someFutureField":123}"#;
        let data: ClusterData = serde_json::from_str(json).unwrap();
        assert_eq!(data.service_url.as_deref(), Some("http://h:8080"));
    }

    /// An absent collection must decode as empty, not fail.
    #[test]
    fn tenant_info_defaults_collections() {
        let info: TenantInfo = serde_json::from_str("{}").unwrap();
        assert!(info.admin_roles.is_empty());
        assert!(info.allowed_clusters.is_empty());

        let info = TenantInfo::with_clusters(["standalone"]);
        let json = serde_json::to_string(&info).unwrap();
        assert_eq!(
            json,
            r#"{"adminRoles":[],"allowedClusters":["standalone"]}"#
        );
    }

    #[test]
    fn resource_quota_round_trips() {
        let quota = ResourceQuota {
            msg_rate_in: 1.5,
            msg_rate_out: 2.5,
            bandwidth_in: 100.0,
            bandwidth_out: 200.0,
            memory: 64.0,
            dynamic: true,
        };
        let json = serde_json::to_string(&quota).unwrap();
        assert!(json.contains("\"msgRateIn\":1.5"), "{json}");
        assert_eq!(serde_json::from_str::<ResourceQuota>(&json).unwrap(), quota);
    }

    /// Regression: these names are plural on the wire. With the singular spelling
    /// the broker accepted the request with 204 and silently ignored every field,
    /// so a wrong name here fails no HTTP status — only a read-back catches it.
    #[test]
    fn resource_group_uses_plural_wire_names() {
        let group = ResourceGroup {
            publish_rate_in_msgs: Some(100),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&group).unwrap(),
            r#"{"publishRateInMsgs":100}"#
        );

        let all = ResourceGroup {
            publish_rate_in_msgs: Some(1),
            publish_rate_in_bytes: Some(2),
            dispatch_rate_in_msgs: Some(3),
            dispatch_rate_in_bytes: Some(4),
        };
        let json = serde_json::to_string(&all).unwrap();
        for name in [
            "publishRateInMsgs",
            "publishRateInBytes",
            "dispatchRateInMsgs",
            "dispatchRateInBytes",
        ] {
            assert!(json.contains(name), "{name} missing from {json}");
        }
        assert_eq!(serde_json::from_str::<ResourceGroup>(&json).unwrap(), all);
    }

    /// Regression: this policy type is snake_case; a camelCase body is rejected
    /// with HTTP 400 by the broker.
    #[test]
    fn namespace_isolation_data_is_snake_case() {
        let data = NamespaceIsolationData {
            namespaces: vec!["public/x.*".to_string()],
            primary: vec![".*".to_string()],
            secondary: vec![],
            auto_failover_policy: Some(AutoFailoverPolicyData {
                policy_type: Some("min_available".to_string()),
                parameters: [("min_limit".to_string(), "1".to_string())]
                    .into_iter()
                    .collect(),
            }),
            unload_scope: None,
        };
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"auto_failover_policy\""), "{json}");
        assert!(json.contains("\"policy_type\""), "{json}");
        assert!(!json.contains("autoFailoverPolicy"), "{json}");
        assert_eq!(
            serde_json::from_str::<NamespaceIsolationData>(&json).unwrap(),
            data
        );
    }
}

// ------------------------------------------------------- namespace policies
//
// Every shape below was verified against a live Pulsar 5.0.0-M1 broker by
// setting a value and reading it back. Unlike `NamespaceIsolationData`, these are
// all camelCase.

/// How long a namespace keeps acknowledged messages.
///
/// `-1` means unlimited for either dimension; `0` disables retention.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicies {
    pub retention_time_in_minutes: i32,
    /// Explicitly renamed: the broker spells this `retentionSizeInMB` with an
    /// uppercase acronym, which serde's `camelCase` would render `retentionSizeInMb`.
    /// A mismatch here is silent — the broker treats the field as unset (0) and then
    /// rejects the body for mixing a zero with a non-zero limit.
    #[serde(rename = "retentionSizeInMB")]
    pub retention_size_in_mb: i64,
}

/// BookKeeper ensemble sizing and mark-delete throttling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistencePolicies {
    pub bookkeeper_ensemble: i32,
    pub bookkeeper_write_quorum: i32,
    pub bookkeeper_ack_quorum: i32,
    pub managed_ledger_max_mark_delete_rate: f64,
}

/// Dispatch throttling. Used for topic, subscription and replicator rates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DispatchRate {
    pub dispatch_throttling_rate_in_msg: i32,
    pub dispatch_throttling_rate_in_byte: i64,
    #[serde(default)]
    pub relative_to_publish_rate: bool,
    pub rate_period_in_second: i32,
}

/// Publish throttling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishRate {
    pub publish_throttling_rate_in_msg: i32,
    pub publish_throttling_rate_in_byte: i64,
}

/// Per-consumer subscribe throttling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeRate {
    pub subscribe_throttling_rate_per_consumer: i32,
    pub rate_period_in_second: i32,
}

/// When the broker may delete an inactive topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InactiveTopicPolicies {
    /// `delete_when_no_subscriptions` or `delete_when_subscriptions_caught_up`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inactive_topic_delete_mode: Option<String>,
    pub max_inactive_duration_seconds: i32,
    pub delete_while_inactive: bool,
}

/// Delayed-delivery tracking for a namespace or topic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelayedDeliveryPolicies {
    pub active: bool,
    /// Granularity of the delayed-message tracker, in milliseconds.
    pub tick_time: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_delivery_delay_in_millis: Option<i64>,
}

/// Whether and how the broker auto-creates topics in a namespace.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoTopicCreationOverride {
    pub allow_auto_topic_creation: bool,
    /// `partitioned` or `non-partitioned`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_num_partitions: Option<i32>,
}

/// Whether the broker auto-creates subscriptions in a namespace.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoSubscriptionCreationOverride {
    pub allow_auto_subscription_creation: bool,
}

/// What the broker does when a backlog quota is exceeded.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BacklogQuota {
    /// Size limit in bytes; `-1` for unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_size: Option<i64>,
    /// Time limit in seconds; `-1` for unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_time: Option<i32>,
    /// `producer_request_hold`, `producer_exception` or `consumer_backlog_eviction`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
}

/// Which backlog dimension a quota constrains.
///
/// Sent as the `backlogQuotaType` query parameter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BacklogQuotaType {
    /// Total stored size — the default when the parameter is omitted.
    #[default]
    DestinationStorage,
    /// Age of the oldest unacknowledged message.
    MessageAge,
}

impl BacklogQuotaType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            BacklogQuotaType::DestinationStorage => "destination_storage",
            BacklogQuotaType::MessageAge => "message_age",
        }
    }
}

/// Which BookKeeper groups a namespace's ledgers prefer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookieAffinityGroupData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookkeeper_affinity_group_primary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookkeeper_affinity_group_secondary: Option<String>,
}

/// Broker-side entry filters applied to a namespace or topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryFilters {
    /// Comma-separated filter names. The broker rejects an empty value; use the
    /// remove operation instead of clearing it.
    pub entry_filter_names: String,
}

/// Namespace bundle boundaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundlesData {
    #[serde(default)]
    pub boundaries: Vec<String>,
    #[serde(rename = "numBundles", default)]
    pub num_bundles: i32,
}

// ------------------------------------------------------------ enumerations

/// How subscription names are authorized within a namespace.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionAuthMode {
    /// No restriction.
    #[default]
    None,
    /// A subscription name must be prefixed with the consumer's role.
    Prefix,
}

/// Rule applied when a producer registers a new schema version.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaCompatibilityStrategy {
    /// Inherit from the broker default.
    #[default]
    Undefined,
    AlwaysIncompatible,
    AlwaysCompatible,
    Backward,
    Forward,
    Full,
    BackwardTransitive,
    ForwardTransitive,
    FullTransitive,
}

/// Where a reader looks first for an offloaded message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OffloadedReadPriority {
    /// Prefer BookKeeper while the entry is still there.
    #[default]
    #[serde(rename = "bookkeeper-first")]
    BookkeeperFirst,
    /// Prefer tiered storage even when BookKeeper still holds the entry.
    #[serde(rename = "tiered-storage-first")]
    TieredStorageFirst,
}

/// What a role may do on a namespace or topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthAction {
    Produce,
    Consume,
    Functions,
    Sources,
    Sinks,
    Packages,
}

// ------------------------------------------------------- offload policies

/// Tiered-storage offload configuration.
///
/// Field names are the Java field names verbatim (Jackson applies no renaming
/// here). The `managedLedgerOffload*` group is the driver-agnostic view; the
/// `s3*`, `gcs*` and `fileSystem*` groups configure specific drivers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OffloadPolicies {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offloaders_directory: Option<String>,
    /// `aws-s3`, `google-cloud-storage`, `filesystem`, `azureblob`, ...
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_driver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_max_threads: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_read_threads: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_prefetch_rounds: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_threshold_in_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_threshold_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_deletion_lag_in_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offloaded_read_priority: Option<OffloadedReadPriority>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub managed_ledger_extra_configurations: BTreeMap<String, String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_ledger_offload_service_endpoint: Option<String>,
    /// Singular `Byte`, matching the Java field; the `s3`/`gcs` variants are plural.
    #[serde(
        rename = "managedLedgerOffloadMaxBlockSizeInBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub managed_ledger_offload_max_block_size_in_bytes: Option<i32>,
    #[serde(
        rename = "managedLedgerOffloadReadBufferSizeInBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub managed_ledger_offload_read_buffer_size_in_bytes: Option<i32>,

    #[serde(
        rename = "s3ManagedLedgerOffloadRegion",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_region: Option<String>,
    #[serde(
        rename = "s3ManagedLedgerOffloadBucket",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_bucket: Option<String>,
    #[serde(
        rename = "s3ManagedLedgerOffloadServiceEndpoint",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_service_endpoint: Option<String>,
    #[serde(
        rename = "s3ManagedLedgerOffloadMaxBlockSizeInBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_max_block_size_in_bytes: Option<i32>,
    #[serde(
        rename = "s3ManagedLedgerOffloadReadBufferSizeInBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_read_buffer_size_in_bytes: Option<i32>,
    #[serde(
        rename = "s3ManagedLedgerOffloadCredentialId",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_credential_id: Option<String>,
    #[serde(
        rename = "s3ManagedLedgerOffloadCredentialSecret",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_credential_secret: Option<String>,
    #[serde(
        rename = "s3ManagedLedgerOffloadRole",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_role: Option<String>,
    #[serde(
        rename = "s3ManagedLedgerOffloadRoleSessionName",
        skip_serializing_if = "Option::is_none"
    )]
    pub s3_managed_ledger_offload_role_session_name: Option<String>,

    #[serde(
        rename = "gcsManagedLedgerOffloadRegion",
        skip_serializing_if = "Option::is_none"
    )]
    pub gcs_managed_ledger_offload_region: Option<String>,
    #[serde(
        rename = "gcsManagedLedgerOffloadBucket",
        skip_serializing_if = "Option::is_none"
    )]
    pub gcs_managed_ledger_offload_bucket: Option<String>,
    #[serde(
        rename = "gcsManagedLedgerOffloadMaxBlockSizeInBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub gcs_managed_ledger_offload_max_block_size_in_bytes: Option<i32>,
    #[serde(
        rename = "gcsManagedLedgerOffloadReadBufferSizeInBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub gcs_managed_ledger_offload_read_buffer_size_in_bytes: Option<i32>,
    #[serde(
        rename = "gcsManagedLedgerOffloadServiceAccountKeyFile",
        skip_serializing_if = "Option::is_none"
    )]
    pub gcs_managed_ledger_offload_service_account_key_file: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_system_profile_path: Option<String>,
    /// Uppercase `URI`, matching the Java field.
    #[serde(rename = "fileSystemURI", skip_serializing_if = "Option::is_none")]
    pub file_system_uri: Option<String>,
}

/// Automatic split/merge thresholds for a Pulsar 5.0 scalable topic.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoScalePolicyOverride {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Plural on the wire. The singular spellings are accepted with HTTP 204 and
    /// then silently discarded, so only a read-back catches them — see
    /// `scalable_topic_auto_scale_policy_round_trip`.
    #[serde(rename = "maxSegments", skip_serializing_if = "Option::is_none")]
    pub max_segments: Option<i32>,
    #[serde(rename = "minSegments", skip_serializing_if = "Option::is_none")]
    pub min_segments: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_dag_depth: Option<i32>,
    #[serde(
        rename = "splitCooldownSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub split_cooldown_seconds: Option<i64>,
    #[serde(
        rename = "mergeCooldownSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub merge_cooldown_seconds: Option<i64>,
    #[serde(rename = "mergeWindowSeconds", skip_serializing_if = "Option::is_none")]
    pub merge_window_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_msg_rate_in_threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_bytes_rate_in_threshold: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_msg_rate_out_threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_bytes_rate_out_threshold: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_msg_rate_in_threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_bytes_rate_in_threshold: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_msg_rate_out_threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_bytes_rate_out_threshold: Option<i64>,
}

/// Per-role authorization recorded on a namespace.
///
/// The JSON keys are `namespace_auth`, `destination_auth` and
/// `subscription_auth_roles`, which match neither the Java getter names nor a
/// simple case conversion; verified against a live broker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthPolicies {
    /// Role -> actions granted on the namespace.
    #[serde(default, rename = "namespace_auth")]
    pub namespace_auth: BTreeMap<String, BTreeSet<AuthAction>>,
    /// Topic -> role -> actions granted on that topic.
    #[serde(default, rename = "destination_auth")]
    pub destination_auth: BTreeMap<String, BTreeMap<String, BTreeSet<AuthAction>>>,
    /// Subscription -> roles allowed to use it.
    #[serde(default, rename = "subscription_auth_roles")]
    pub subscription_auth_roles: BTreeMap<String, BTreeSet<String>>,
}

/// The full policy set of a namespace.
///
/// Field naming is **mixed** on the wire — mostly `snake_case` but with a
/// camelCase minority (`clusterDispatchRate`, `deduplicationEnabled`, ...). These
/// are the Java field names verbatim, since Jackson applies no renaming, and each
/// one is spelled explicitly below rather than derived by a case convention.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Policies {
    #[serde(default)]
    pub auth_policies: AuthPolicies,
    #[serde(default)]
    pub replication_clusters: BTreeSet<String>,
    #[serde(default)]
    pub allowed_clusters: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundles: Option<BundlesData>,
    /// Keyed by backlog quota type, e.g. `destination_storage`.
    #[serde(default)]
    pub backlog_quota_map: BTreeMap<String, BacklogQuota>,
    #[serde(default, rename = "clusterDispatchRate")]
    pub cluster_dispatch_rate: BTreeMap<String, DispatchRate>,
    #[serde(default, rename = "topicDispatchRate")]
    pub topic_dispatch_rate: BTreeMap<String, DispatchRate>,
    #[serde(default, rename = "subscriptionDispatchRate")]
    pub subscription_dispatch_rate: BTreeMap<String, DispatchRate>,
    #[serde(default, rename = "replicatorDispatchRate")]
    pub replicator_dispatch_rate: BTreeMap<String, DispatchRate>,
    #[serde(default, rename = "clusterSubscribeRate")]
    pub cluster_subscribe_rate: BTreeMap<String, SubscribeRate>,
    #[serde(default, rename = "publishMaxMessageRate")]
    pub publish_max_message_rate: BTreeMap<String, PublishRate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistencePolicies>,
    #[serde(
        default,
        rename = "deduplicationEnabled",
        skip_serializing_if = "Option::is_none"
    )]
    pub deduplication_enabled: Option<bool>,
    #[serde(
        default,
        rename = "autoTopicCreationOverride",
        skip_serializing_if = "Option::is_none"
    )]
    pub auto_topic_creation_override: Option<AutoTopicCreationOverride>,
    #[serde(
        default,
        rename = "autoSubscriptionCreationOverride",
        skip_serializing_if = "Option::is_none"
    )]
    pub auto_subscription_creation_override: Option<AutoSubscriptionCreationOverride>,
    #[serde(
        default,
        rename = "scalableTopicAutoScalePolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub scalable_topic_auto_scale_policy: Option<AutoScalePolicyOverride>,
    #[serde(default)]
    pub latency_stats_sample_rate: BTreeMap<String, i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_ttl_in_seconds: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_expiration_time_minutes: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_policies: Option<RetentionPolicies>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub encryption_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delayed_delivery_policies: Option<DelayedDeliveryPolicies>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactive_topic_policies: Option<InactiveTopicPolicies>,
    #[serde(default)]
    pub subscription_auth_mode: SubscriptionAuthMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_producers_per_topic: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_consumers_per_topic: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_consumers_per_subscription: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unacked_messages_per_consumer: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unacked_messages_per_subscription: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_subscriptions_per_topic: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_threshold: Option<i64>,
    #[serde(default)]
    pub offload_threshold: i64,
    #[serde(default)]
    pub offload_threshold_in_seconds: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offload_deletion_lag_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_topics_per_namespace: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_auto_update_compatibility_strategy: Option<String>,
    #[serde(default)]
    pub schema_compatibility_strategy: SchemaCompatibilityStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_allow_auto_update_schema: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_allow_auto_update_schema_with_replicator: Option<bool>,
    #[serde(default)]
    pub schema_validation_enforced: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offload_policies: Option<OffloadPolicies>,
    #[serde(
        default,
        rename = "deduplicationSnapshotIntervalSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub deduplication_snapshot_interval_seconds: Option<i32>,
    #[serde(default)]
    pub subscription_types_enabled: BTreeSet<String>,
    #[serde(default)]
    pub allowed_topic_property_keys_for_metrics: BTreeSet<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_group_name: Option<String>,
    #[serde(default)]
    pub migrated: bool,
    #[serde(
        default,
        rename = "dispatcherPauseOnAckStatePersistentEnabled",
        skip_serializing_if = "Option::is_none"
    )]
    pub dispatcher_pause_on_ack_state_persistent_enabled: Option<bool>,
    #[serde(
        default,
        rename = "entryFilters",
        skip_serializing_if = "Option::is_none"
    )]
    pub entry_filters: Option<EntryFilters>,
}

// -------------------------------------------------------------- topic data

/// Partition count and properties of a (possibly partitioned) topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionedTopicMetadata {
    /// `0` for a non-partitioned topic.
    #[serde(default)]
    pub partitions: i32,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

/// A message position within a topic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageIdData {
    pub ledger_id: i64,
    pub entry_id: i64,
    #[serde(default)]
    pub partition_index: i32,
    #[serde(default)]
    pub batch_index: i32,
    #[serde(default)]
    pub batch_size: i32,
}

impl MessageIdData {
    /// The oldest message available on the topic — Java's `MessageId.earliest`.
    ///
    /// A subscription created here replays the whole retained backlog.
    pub fn earliest() -> Self {
        Self {
            ledger_id: -1,
            entry_id: -1,
            partition_index: -1,
            ..Default::default()
        }
    }

    /// The next message published to the topic — Java's `MessageId.latest`.
    ///
    /// Spelled `Long.MAX_VALUE:Long.MAX_VALUE`, not `-1:-1`; `-1:-1` is
    /// [`earliest`][Self::earliest].
    pub fn latest() -> Self {
        Self {
            ledger_id: i64::MAX,
            entry_id: i64::MAX,
            partition_index: -1,
            ..Default::default()
        }
    }
}

/// Progress of a long-running broker operation such as compaction or offload.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LongRunningProcessStatus {
    /// `NOT_RUN`, `RUNNING`, `SUCCESS` or `ERROR`.
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub last_error: String,
}

/// Offload progress, plus the first message not yet offloaded.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OffloadProcessStatus {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub last_error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_unoffloaded_message: Option<MessageIdData>,
}

/// One connected producer.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublisherStats {
    #[serde(default)]
    pub producer_id: Option<i64>,
    #[serde(default)]
    pub producer_name: Option<String>,
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_throughput_in: f64,
    #[serde(default)]
    pub average_msg_size: f64,
    #[serde(default)]
    pub chunked_message_rate: f64,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub connected_since: Option<String>,
    #[serde(default)]
    pub client_version: Option<String>,
    #[serde(default)]
    pub access_mode: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// One connected consumer.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerStats {
    #[serde(default)]
    pub consumer_name: Option<String>,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub msg_throughput_out: f64,
    #[serde(default)]
    pub bytes_out_counter: i64,
    #[serde(default)]
    pub msg_out_counter: i64,
    #[serde(default)]
    pub msg_rate_redeliver: f64,
    #[serde(default)]
    pub message_ack_rate: f64,
    #[serde(default)]
    pub chunked_message_rate: f64,
    #[serde(default)]
    pub available_permits: i32,
    #[serde(default)]
    pub unacked_messages: i32,
    #[serde(default)]
    pub avg_messages_per_entry: i32,
    #[serde(default)]
    pub blocked_consumer_on_unacked_msgs: bool,
    #[serde(default)]
    pub read_position_when_joining: Option<String>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub connected_since: Option<String>,
    #[serde(default)]
    pub client_version: Option<String>,
    #[serde(default)]
    pub last_acked_timestamp: i64,
    #[serde(default)]
    pub last_consumed_timestamp: i64,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// Runtime statistics for one subscription.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionStats {
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub msg_throughput_out: f64,
    #[serde(default)]
    pub bytes_out_counter: i64,
    #[serde(default)]
    pub msg_out_counter: i64,
    #[serde(default)]
    pub msg_rate_redeliver: f64,
    #[serde(default)]
    pub message_ack_rate: f64,
    #[serde(default)]
    pub chunked_message_rate: f64,
    #[serde(default)]
    pub msg_backlog: i64,
    #[serde(default)]
    pub backlog_size: i64,
    #[serde(default)]
    pub earliest_msg_publish_time_in_backlog: i64,
    #[serde(default)]
    pub msg_backlog_no_delayed: i64,
    #[serde(default)]
    pub blocked_subscription_on_unacked_msgs: bool,
    #[serde(default)]
    pub msg_delayed: i64,
    #[serde(default)]
    pub msg_in_replay: i64,
    #[serde(default)]
    pub unacked_messages: i64,
    /// `Exclusive`, `Shared`, `Failover` or `Key_Shared`.
    #[serde(default, rename = "type")]
    pub subscription_type: Option<String>,
    #[serde(default)]
    pub msg_rate_expired: f64,
    #[serde(default)]
    pub msg_expired: i64,
    #[serde(default)]
    pub total_msg_expired: i64,
    #[serde(default)]
    pub last_expire_timestamp: i64,
    #[serde(default)]
    pub last_consumed_flow_timestamp: i64,
    #[serde(default)]
    pub last_consumed_timestamp: i64,
    #[serde(default)]
    pub last_acked_timestamp: i64,
    #[serde(default)]
    pub last_mark_delete_advanced_timestamp: i64,
    #[serde(default)]
    pub consumers: Vec<ConsumerStats>,
    #[serde(default, rename = "isDurable")]
    pub is_durable: bool,
    #[serde(default, rename = "isReplicated")]
    pub is_replicated: bool,
    #[serde(default)]
    pub allow_out_of_order_delivery: bool,
    #[serde(default)]
    pub draining_hashes_count: i32,
    #[serde(default)]
    pub draining_hashes_cleared_total: i64,
    #[serde(default)]
    pub draining_hashes_unacked_messages: i32,
    #[serde(default)]
    pub non_contiguous_deleted_messages_ranges: i32,
    #[serde(default)]
    pub delayed_message_index_size_in_bytes: i64,
    #[serde(default)]
    pub subscription_properties: BTreeMap<String, String>,
    #[serde(default)]
    pub filter_processed_msg_count: i64,
    #[serde(default)]
    pub filter_accepted_msg_count: i64,
    #[serde(default)]
    pub filter_rejected_msg_count: i64,
    #[serde(default)]
    pub filter_rescheduled_msg_count: i64,
}

/// Geo-replication statistics for one remote cluster.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatorStats {
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_throughput_in: f64,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub msg_throughput_out: f64,
    #[serde(default)]
    pub msg_rate_expired: f64,
    #[serde(default)]
    pub replication_backlog: i64,
    #[serde(default)]
    pub connected: bool,
    #[serde(default)]
    pub replication_delay_in_seconds: i64,
    #[serde(default)]
    pub inbound_connection: Option<String>,
    #[serde(default)]
    pub outbound_connection: Option<String>,
}

/// Compaction progress reported inside [`TopicStats`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionStats {
    #[serde(default)]
    pub last_compaction_removed_event_count: i64,
    #[serde(default)]
    pub last_compaction_succeed_timestamp: i64,
    #[serde(default)]
    pub last_compaction_failed_timestamp: i64,
    #[serde(default)]
    pub last_compaction_duration_time_in_mills: i64,
}

/// Runtime statistics for a topic.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicStats {
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_throughput_in: f64,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub msg_throughput_out: f64,
    #[serde(default)]
    pub bytes_in_counter: i64,
    #[serde(default)]
    pub msg_in_counter: i64,
    #[serde(default)]
    pub bytes_out_counter: i64,
    #[serde(default)]
    pub msg_out_counter: i64,
    #[serde(default)]
    pub bytes_out_internal_counter: i64,
    #[serde(default)]
    pub average_msg_size: f64,
    #[serde(default)]
    pub msg_chunk_published: bool,
    #[serde(default)]
    pub storage_size: i64,
    #[serde(default)]
    pub backlog_size: i64,
    #[serde(default)]
    pub backlog_quota_limit_size: i64,
    #[serde(default)]
    pub backlog_quota_limit_time: i64,
    #[serde(default)]
    pub oldest_backlog_message_age_seconds: i64,
    #[serde(default)]
    pub publish_rate_limited_times: i64,
    #[serde(default)]
    pub earliest_msg_publish_time_in_backlogs: i64,
    #[serde(default)]
    pub offloaded_storage_size: i64,
    #[serde(default)]
    pub last_offload_ledger_id: i64,
    #[serde(default)]
    pub last_offload_success_time_stamp: i64,
    #[serde(default)]
    pub last_offload_failure_time_stamp: i64,
    #[serde(default)]
    pub ongoing_txn_count: i64,
    #[serde(default)]
    pub aborted_txn_count: i64,
    #[serde(default)]
    pub committed_txn_count: i64,
    #[serde(default)]
    pub publishers: Vec<PublisherStats>,
    #[serde(default)]
    pub waiting_publishers: i32,
    #[serde(default)]
    pub subscriptions: BTreeMap<String, SubscriptionStats>,
    #[serde(default)]
    pub replication: BTreeMap<String, ReplicatorStats>,
    #[serde(default)]
    pub deduplication_status: Option<String>,
    #[serde(default)]
    pub non_contiguous_deleted_messages_ranges: i32,
    #[serde(default)]
    pub delayed_message_index_size_in_bytes: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionStats>,
    #[serde(default)]
    pub owner_broker: Option<String>,
    #[serde(default)]
    pub topic_creation_time_stamp: i64,
    #[serde(default)]
    pub last_publish_time_stamp: i64,
}

/// Aggregated statistics for a partitioned topic.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionedTopicStats {
    /// Metadata for the partitioned topic as a whole.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<PartitionedTopicMetadata>,
    /// Per-partition statistics, keyed by partition topic name. Present only when
    /// `perPartition` was requested.
    #[serde(default)]
    pub partitions: BTreeMap<String, TopicStats>,
    /// The aggregate across all partitions.
    #[serde(flatten)]
    pub aggregate: TopicStats,
}

/// One BookKeeper ledger backing a topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LedgerInfo {
    #[serde(default)]
    pub ledger_id: i64,
    #[serde(default)]
    pub entries: i64,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub offloaded: bool,
    #[serde(default)]
    pub under_replicated: bool,
}

/// Internal state of one subscription cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CursorInfo {
    #[serde(default)]
    pub mark_delete_position: Option<String>,
    #[serde(default)]
    pub read_position: Option<String>,
    #[serde(default)]
    pub waiting_read_op: bool,
    #[serde(default)]
    pub pending_read_ops: i32,
    #[serde(default)]
    pub messages_consumed_counter: i64,
    #[serde(default)]
    pub cursor_ledger: i64,
    #[serde(default)]
    pub cursor_ledger_last_entry: i64,
    #[serde(default)]
    pub individually_deleted_messages: Option<String>,
    #[serde(default)]
    pub last_ledger_switch_timestamp: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub number_of_entries_since_first_not_acked_message: i64,
    #[serde(default)]
    pub total_non_contiguous_deleted_messages_range: i32,
    #[serde(default)]
    pub subscription_have_pending_read: bool,
    #[serde(default)]
    pub subscription_have_pending_replay_read: bool,
    #[serde(default)]
    pub properties: BTreeMap<String, i64>,
}

/// Managed-ledger internals for a topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentTopicInternalStats {
    #[serde(default)]
    pub entries_added_counter: i64,
    #[serde(default)]
    pub number_of_entries: i64,
    #[serde(default)]
    pub total_size: i64,
    #[serde(default)]
    pub current_ledger_entries: i64,
    #[serde(default)]
    pub current_ledger_size: i64,
    #[serde(default)]
    pub last_ledger_created_timestamp: Option<String>,
    #[serde(default)]
    pub waiting_cursors_count: i32,
    #[serde(default)]
    pub pending_add_entries_count: i32,
    #[serde(default)]
    pub last_confirmed_entry: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub ledgers: Vec<LedgerInfo>,
    #[serde(default)]
    pub cursors: BTreeMap<String, CursorInfo>,
    #[serde(default)]
    pub schema_ledgers: Vec<LedgerInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compacted_ledger: Option<LedgerInfo>,
}

/// Aggregated internal stats for a partitioned topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionedTopicInternalStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<PartitionedTopicMetadata>,
    #[serde(default)]
    pub partitions: BTreeMap<String, PersistentTopicInternalStats>,
}

/// Result of analysing a subscription's backlog.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzeSubscriptionBacklogResult {
    #[serde(default)]
    pub entries: i64,
    #[serde(default)]
    pub messages: i64,
    #[serde(default)]
    pub filter_rejected_entries: i64,
    #[serde(default)]
    pub filter_accepted_entries: i64,
    #[serde(default)]
    pub filter_rescheduled_entries: i64,
    #[serde(default)]
    pub filter_rejected_messages: i64,
    #[serde(default)]
    pub filter_accepted_messages: i64,
    #[serde(default)]
    pub filter_rescheduled_messages: i64,
    #[serde(default)]
    pub aborted: bool,
}

/// Options controlling how much work `get_stats` does.
///
/// The extra computations are opt-in because each one costs a scan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GetStatsOptions {
    /// Count the backlog exactly instead of estimating from ledger metadata.
    pub get_precise_backlog: bool,
    /// Include each subscription's backlog size in bytes.
    pub subscription_backlog_size: bool,
    /// Include the publish time of the oldest message still in the backlog.
    pub get_earliest_time_in_backlog: bool,
    /// Omit the `publishers` array.
    pub exclude_publishers: bool,
    /// Omit the per-subscription `consumers` arrays.
    pub exclude_consumers: bool,
}

impl GetStatsOptions {
    pub(crate) fn to_query(self) -> Vec<(&'static str, String)> {
        vec![
            ("getPreciseBacklog", self.get_precise_backlog.to_string()),
            (
                "subscriptionBacklogSize",
                self.subscription_backlog_size.to_string(),
            ),
            (
                "getEarliestTimeInBacklog",
                self.get_earliest_time_in_backlog.to_string(),
            ),
            ("excludePublishers", self.exclude_publishers.to_string()),
            ("excludeConsumers", self.exclude_consumers.to_string()),
        ]
    }
}

/// A message read back through the admin API by `peek` or `examine`.
///
/// The broker returns the payload as the HTTP body and the metadata as
/// `X-Pulsar-*` headers, so this is assembled rather than deserialized.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeekedMessage {
    /// Position, parsed from the `X-Pulsar-Message-ID` header when present.
    pub message_id: Option<String>,
    pub publish_time: Option<String>,
    pub event_time: Option<String>,
    pub producer_name: Option<String>,
    pub partition_key: Option<String>,
    /// Application properties.
    ///
    /// The broker sends these as a **single** `X-Pulsar-PROPERTY` header holding a
    /// JSON object, not as one header per key.
    pub properties: BTreeMap<String, String>,
    /// Number of logical messages in this entry, from `X-Pulsar-num-batch-message`.
    ///
    /// `None` for an unbatched entry. When set, [`payload`][Self::payload] is the
    /// raw batch envelope — Pulsar-framed, one length-prefixed
    /// `SingleMessageMetadata` per message — not a single application payload.
    pub num_messages_in_batch: Option<i32>,
    /// Whether the entry's value is null (`X-Pulsar-null-value`).
    pub null_value: bool,
    /// Raw payload bytes.
    ///
    /// See [`num_messages_in_batch`][Self::num_messages_in_batch] before treating
    /// this as an application payload.
    pub payload: Vec<u8>,
}

// -------------------------------------------------------------- schemas

/// Whether a schema is compatible with what a topic already carries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IsCompatibilityResponse {
    /// Java's field is `isCompatibility`, but Lombok's generated getter makes
    /// Jackson publish it as `compatibility` — verified against a live broker.
    #[serde(default, rename = "compatibility")]
    pub is_compatible: bool,
    #[serde(default)]
    pub schema_compatibility_strategy: Option<String>,
}

/// A schema registered against a topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Monotonic version, assigned by the broker.
    #[serde(default)]
    pub version: i64,
    /// `STRING`, `JSON`, `AVRO`, `PROTOBUF`, `KEY_VALUE`, `NONE`, ...
    #[serde(rename = "type")]
    pub schema_type: String,
    /// When the version was registered, in epoch milliseconds.
    #[serde(default)]
    pub timestamp: i64,
    /// The schema definition. Empty for schemaless types such as `STRING`.
    #[serde(default)]
    pub data: String,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// Request body for registering a schema version.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostSchemaPayload {
    /// `STRING`, `JSON`, `AVRO`, ...
    #[serde(rename = "type")]
    pub schema_type: String,
    /// The definition; empty for schemaless types.
    #[serde(default)]
    pub schema: String,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// Where a schema version is stored in BookKeeper.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaLedgerEntry {
    #[serde(default)]
    pub ledger_id: i64,
    #[serde(default)]
    pub entry_id: i64,
    #[serde(default)]
    pub version: i64,
}

/// Storage layout of a topic's schema history.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<SchemaLedgerEntry>,
    #[serde(default)]
    pub index: Vec<SchemaLedgerEntry>,
}

/// Broker chosen to serve a topic, as returned by the HTTP lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicLookupResult {
    #[serde(default)]
    pub broker_id: Option<String>,
    #[serde(default)]
    pub broker_url: Option<String>,
    /// The binary-protocol TLS endpoint. On a TLS-only cluster this is the only
    /// usable broker URL, so omitting it silently dropped the whole answer.
    #[serde(default)]
    pub broker_url_tls: Option<String>,
    #[serde(default)]
    pub http_url: Option<String>,
    /// The web-service HTTPS endpoint.
    #[serde(default)]
    pub http_url_tls: Option<String>,
    #[serde(default)]
    pub native_url: Option<String>,
    /// A compatibility alias the broker still serializes alongside
    /// [`broker_url_tls`][Self::broker_url_tls].
    #[serde(default)]
    pub broker_url_ssl: Option<String>,
}

/// Runtime statistics for a non-persistent topic.
///
/// Non-persistent topics have no storage, so the persistent fields
/// (`storageSize`, `backlogSize`, offload counters) are absent.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonPersistentTopicStats {
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_throughput_in: f64,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub msg_throughput_out: f64,
    #[serde(default)]
    pub msg_in_counter: i64,
    #[serde(default)]
    pub msg_out_counter: i64,
    #[serde(default)]
    pub average_msg_size: f64,
    /// Messages dropped because no consumer could take them.
    #[serde(default)]
    pub msg_drop_rate: f64,
    #[serde(default)]
    pub publishers: Vec<PublisherStats>,
    #[serde(default)]
    pub subscriptions: BTreeMap<String, SubscriptionStats>,
    #[serde(default)]
    pub replication: BTreeMap<String, ReplicatorStats>,
    #[serde(default)]
    pub owner_broker: Option<String>,
}

// ---------------------------------------------------------- transactions

/// A transaction id, split into its two 64-bit halves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TxnId {
    pub most_sig_bits: i64,
    pub least_sig_bits: i64,
}

impl TxnId {
    /// The two path segments the admin API expects.
    ///
    /// The broker's routes end in `.../{mostSigBits}/{leastSigBits}` — two
    /// separate segments, not one combined `most:least` token.
    pub(crate) fn as_segments(self) -> [String; 2] {
        [
            self.most_sig_bits.to_string(),
            self.least_sig_bits.to_string(),
        ]
    }
}

/// One transaction coordinator and the broker hosting it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionCoordinatorInfo {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub broker_service_url: Option<String>,
}

/// Runtime state of one transaction coordinator.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionCoordinatorStats {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default, rename = "leastSigBits")]
    pub least_sig_bits: i64,
    #[serde(default)]
    pub low_water_mark: i64,
    #[serde(default)]
    pub ongoing_txn_size: i64,
    #[serde(default)]
    pub recover_start_time: i64,
    #[serde(default)]
    pub recover_end_time: i64,
}

/// A transaction's position inside one topic's transaction buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionInBufferStats {
    #[serde(default)]
    pub start_position: Option<String>,
    #[serde(default)]
    pub aborted: bool,
}

/// A transaction's cumulative acknowledgement position on one subscription.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionInPendingAckStats {
    #[serde(default)]
    pub cumulative_ack_position: Option<String>,
}

/// Everything the coordinator knows about one transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionMetadata {
    #[serde(default)]
    pub txn_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub open_timestamp: i64,
    #[serde(default)]
    pub timeout_at: i64,
    #[serde(default)]
    pub owner: Option<String>,
    /// Topic -> the transaction's buffer position on it.
    #[serde(default)]
    pub produced_partitions: BTreeMap<String, TransactionInBufferStats>,
    /// Topic -> subscription -> pending-ack position.
    #[serde(default)]
    pub acked_partitions: BTreeMap<String, BTreeMap<String, TransactionInPendingAckStats>>,
}

/// Aborted/ongoing segment counters inside a transaction buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentsStats {
    #[serde(default, rename = "segmentsSize")]
    pub segments_size: i64,
    #[serde(default)]
    pub current_segment_capacity: i64,
    /// `ID` is capitalised on the wire, so camelCase alone gets this wrong.
    #[serde(default, rename = "unsealedAbortTxnIDSize")]
    pub unsealed_abort_txn_id_size: i64,
    #[serde(default)]
    pub segment_stats: Vec<SegmentStats>,
    #[serde(default)]
    pub last_took_snapshot_segment_timestamp: i64,
}

/// One aborted-transaction segment inside a transaction buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentStats {
    /// `ID` is capitalised on the wire, so camelCase alone gets this wrong.
    #[serde(default, rename = "lastTxnID")]
    pub last_txn_id: Option<String>,
    #[serde(default)]
    pub persistent_position: Option<String>,
}

/// Transaction-buffer state for one topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionBufferStats {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub max_read_position: Option<String>,
    #[serde(default, rename = "lastSnapshotTimestamps")]
    pub last_snapshot_timestamps: i64,
    /// Coordinator id -> low water mark.
    #[serde(default, rename = "lowWaterMarks")]
    pub low_water_marks: BTreeMap<String, i64>,
    #[serde(default)]
    pub ongoing_txn_size: i64,
    #[serde(default)]
    pub recover_start_time: i64,
    #[serde(default)]
    pub recover_end_time: i64,
    #[serde(default, rename = "totalAbortedTransactions")]
    pub total_aborted_transactions: i64,
    #[serde(default)]
    pub snapshot_type: Option<String>,
    #[serde(
        default,
        rename = "segmentsStats",
        skip_serializing_if = "Option::is_none"
    )]
    pub segments_stats: Option<SegmentsStats>,
}

/// Pending-acknowledgement state for one subscription.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionPendingAckStats {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default, rename = "lowWaterMarks")]
    pub low_water_marks: BTreeMap<String, i64>,
    #[serde(default)]
    pub ongoing_txn_size: i64,
    #[serde(default)]
    pub recover_start_time: i64,
    #[serde(default)]
    pub recover_end_time: i64,
}

/// Managed-ledger internals of a transaction log.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionLogStats {
    #[serde(default)]
    pub managed_ledger_name: Option<String>,
    #[serde(
        default,
        rename = "managedLedgerInternalStats",
        skip_serializing_if = "Option::is_none"
    )]
    pub managed_ledger_internal_stats: Option<PersistentTopicInternalStats>,
}

/// Managed-ledger internals of a transaction-buffer snapshot system topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSystemTopicInternalStats {
    #[serde(default)]
    pub managed_ledger_name: Option<String>,
    #[serde(
        default,
        rename = "managedLedgerInternalStats",
        skip_serializing_if = "Option::is_none"
    )]
    pub managed_ledger_internal_stats: Option<PersistentTopicInternalStats>,
}

/// Internal state of one transaction coordinator.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionCoordinatorInternalStats {
    #[serde(
        default,
        rename = "transactionLogStats",
        skip_serializing_if = "Option::is_none"
    )]
    pub transaction_log_stats: Option<TransactionLogStats>,
}

/// Internal state of one subscription's pending-ack store.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionPendingAckInternalStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_ack_log_stats: Option<TransactionLogStats>,
}

/// Internal state of one topic's transaction buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionBufferInternalStats {
    #[serde(default)]
    pub snapshot_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub single_snapshot_system_topic_internal_stats: Option<SnapshotSystemTopicInternalStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_internal_stats: Option<SnapshotSystemTopicInternalStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_index_internal_stats: Option<SnapshotSystemTopicInternalStats>,
}

// ------------------------------------------------------- scalable topics

/// An inclusive hash range owned by a segment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashRange {
    #[serde(default)]
    pub start: u32,
    #[serde(default)]
    pub end: u32,
}

/// One hash-range segment of a scalable topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalableSegmentInfo {
    #[serde(default)]
    pub segment_id: i64,
    #[serde(default)]
    pub hash_range: HashRange,
    /// `ACTIVE` or `SEALED`.
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub parent_ids: Vec<i64>,
    #[serde(default)]
    pub child_ids: Vec<i64>,
    #[serde(default)]
    pub created_at_epoch: i64,
    #[serde(default)]
    pub sealed_at_epoch: i64,
    #[serde(default)]
    pub created_at_ms: i64,
    #[serde(default)]
    pub sealed_at_ms: i64,
    /// A legacy segment wraps an unmigrated `persistent://` topic.
    #[serde(default)]
    pub legacy: bool,
    /// No children yet, so it is at the frontier of the DAG.
    #[serde(default)]
    pub leaf: bool,
    #[serde(default)]
    pub root: bool,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub sealed: bool,
}

/// The segment DAG of a scalable topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalableTopicMetadata {
    /// DAG generation number, bumped on every split or merge.
    #[serde(default)]
    pub epoch: i64,
    #[serde(default)]
    pub next_segment_id: i64,
    /// Keyed by segment id rendered as a string.
    #[serde(default)]
    pub segments: BTreeMap<String, ScalableSegmentInfo>,
}

/// Name and state of one segment, as reported in stats.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalableSegmentStats {
    /// The `segment://…` topic backing this segment.
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
}

/// Aggregate statistics for a scalable topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalableTopicStats {
    #[serde(default)]
    pub epoch: i64,
    #[serde(default)]
    pub total_segments: i32,
    #[serde(default)]
    pub active_segments: i32,
    #[serde(default)]
    pub sealed_segments: i32,
    #[serde(default)]
    pub segments: BTreeMap<String, ScalableSegmentStats>,
    #[serde(default)]
    pub subscriptions: BTreeMap<String, ScalableSubscriptionStats>,
}

/// Consumer occupancy of one scalable-topic subscription.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalableSubscriptionStats {
    #[serde(default)]
    pub consumer_count: i32,
}

/// How a scalable-topic subscription consumes its segments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScalableSubscriptionType {
    /// Controller-assigned exclusive segments, key-ordered within each.
    #[default]
    Stream,
    /// Externally tracked read positions, for stream-processing frameworks.
    ///
    /// Defined by PIP-460 but **not yet served by Pulsar 5.0.0-M1**, which answers
    /// HTTP 404 for it. Verified against a live broker.
    Checkpoint,
}

impl ScalableSubscriptionType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ScalableSubscriptionType::Stream => "STREAM",
            ScalableSubscriptionType::Checkpoint => "CHECKPOINT",
        }
    }
}

// ------------------------------------------------ functions and connectors

/// How a function's messages are routed to its output topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_pending_messages: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_pending_messages_across_partitions: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_thread_local_producers: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_builder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_type: Option<String>,
}

/// How a function or sink consumes one input topic.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serde_class_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_regex_pattern: Option<bool>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub schema_properties: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub consumer_properties: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receiver_queue_size: Option<i32>,
}

/// CPU, memory and disk reserved for one function or connector instance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
    /// Bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram: Option<i64>,
    /// Bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk: Option<i64>,
}

/// Definition of a Pulsar Function.
///
/// Only the fields a caller realistically sets are modelled; the broker echoes
/// back many computed defaults, which are ignored on decode.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
    /// `JAVA`, `PYTHON` or `GO`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics_pattern: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_specs: BTreeMap<String, ConsumerConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter_topic: Option<String>,
    /// Serialized as `subName`. Pulsar's object mapper ignores unknown properties,
    /// so `subscriptionName` was accepted and silently discarded.
    #[serde(rename = "subName", skip_serializing_if = "Option::is_none")]
    pub subscription_name: Option<String>,
    /// `ATLEAST_ONCE`, `ATMOST_ONCE` or `EFFECTIVELY_ONCE`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_guarantees: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retain_ordering: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retain_key_ordering: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_ack: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_message_retries: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jar: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub py: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub go: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub user_config: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_config: Option<ProducerConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_flags: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_runtime_options: Option<String>,
}

/// Definition of a sink connector.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
    /// Built-in connector name, e.g. `jdbc-postgres`. Mutually exclusive with `archive`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics_pattern: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_specs: BTreeMap<String, ConsumerConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub topic_to_serde_class_name: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub topic_to_schema_type: BTreeMap<String, String>,
    /// Serialized as `sourceSubscriptionName`, the sink's own spelling.
    #[serde(
        rename = "sourceSubscriptionName",
        skip_serializing_if = "Option::is_none"
    )]
    pub subscription_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_guarantees: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retain_ordering: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_ack: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_message_retries: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dead_letter_topic: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configs: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
}

/// Definition of a source connector.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
    /// Built-in connector name, e.g. `data-generator`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serde_class_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_guarantees: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configs: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_config: Option<ProducerConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_source_config: Option<serde_json::Value>,
}

/// One instance of a running function.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionInstanceStatus {
    #[serde(default)]
    pub running: bool,
    /// The most recent user-code exceptions, newest last.
    #[serde(default)]
    pub latest_user_exceptions: Vec<ExceptionInformation>,
    /// The most recent framework exceptions, newest last.
    #[serde(default)]
    pub latest_system_exceptions: Vec<ExceptionInformation>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub num_restarts: i64,
    #[serde(default)]
    pub num_received: i64,
    #[serde(default)]
    pub num_successfully_processed: i64,
    #[serde(default)]
    pub num_user_exceptions: i64,
    #[serde(default)]
    pub num_system_exceptions: i64,
    #[serde(default)]
    pub average_latency: f64,
    #[serde(default)]
    pub last_invocation_time: i64,
    #[serde(default)]
    pub worker_id: Option<String>,
}

/// One recorded exception from a function instance.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExceptionInformation {
    #[serde(default)]
    pub exception_string: Option<String>,
    #[serde(default)]
    pub timestamp_ms: i64,
}

/// Wrapper the broker puts around each instance's status.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionInstanceStatusEntry {
    #[serde(default)]
    pub instance_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<FunctionInstanceStatus>,
}

/// Aggregate status of a function across its instances.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionStatus {
    #[serde(default)]
    pub num_instances: i32,
    #[serde(default)]
    pub num_running: i32,
    #[serde(default)]
    pub instances: Vec<FunctionInstanceStatusEntry>,
}

/// Throughput and latency counters for one function instance.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionInstanceStats {
    #[serde(default)]
    pub instance_id: i32,
    /// The counters, which the broker nests under `metrics` rather than emitting
    /// flat. Reading them flat decoded every counter as zero.
    #[serde(default)]
    pub metrics: FunctionInstanceStatsData,
}

/// The counters the broker nests under an instance's `metrics` key.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionInstanceStatsData {
    #[serde(default, rename = "receivedTotal")]
    pub received_total: i64,
    #[serde(default, rename = "processedSuccessfullyTotal")]
    pub processed_successfully_total: i64,
    #[serde(default, rename = "systemExceptionsTotal")]
    pub system_exceptions_total: i64,
    #[serde(default, rename = "userExceptionsTotal")]
    pub user_exceptions_total: i64,
    #[serde(default, rename = "avgProcessLatency")]
    pub avg_process_latency: Option<f64>,
    /// Spelled `1min` on the wire, which no rename rule produces.
    #[serde(default, rename = "1min")]
    pub one_min: FunctionInstanceStatsDataBase,
    #[serde(default, rename = "lastInvocation")]
    pub last_invocation: Option<i64>,
    #[serde(default)]
    pub user_metrics: BTreeMap<String, f64>,
}

/// The rolling one-minute window of a function's counters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionInstanceStatsDataBase {
    #[serde(default, rename = "receivedTotal")]
    pub received_total: i64,
    #[serde(default, rename = "processedSuccessfullyTotal")]
    pub processed_successfully_total: i64,
    #[serde(default, rename = "systemExceptionsTotal")]
    pub system_exceptions_total: i64,
    #[serde(default, rename = "userExceptionsTotal")]
    pub user_exceptions_total: i64,
    #[serde(default, rename = "avgProcessLatency")]
    pub avg_process_latency: Option<f64>,
}

/// Aggregate statistics for a function across its instances.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionStats {
    #[serde(default, rename = "receivedTotal")]
    pub received_total: i64,
    #[serde(default, rename = "processedSuccessfullyTotal")]
    pub processed_successfully_total: i64,
    #[serde(default, rename = "systemExceptionsTotal")]
    pub system_exceptions_total: i64,
    #[serde(default, rename = "userExceptionsTotal")]
    pub user_exceptions_total: i64,
    #[serde(default, rename = "avgProcessLatency")]
    pub avg_process_latency: Option<f64>,
    /// Spelled `1min` on the wire.
    #[serde(default, rename = "1min")]
    pub one_min: FunctionInstanceStatsDataBase,
    #[serde(default, rename = "lastInvocation")]
    pub last_invocation: Option<i64>,
    #[serde(default)]
    pub instances: Vec<FunctionInstanceStats>,
}

/// One key of a function's state store.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionState {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub string_value: Option<String>,
    /// Java's field is `byte[]`, which Jackson renders as a **base64 string**.
    /// Plain serde would emit and expect a JSON array of numbers, so binary state
    /// would fail to decode and would be written in the wrong shape.
    #[serde(default, with = "base64_bytes")]
    pub byte_value: Option<Vec<u8>>,
    #[serde(default)]
    pub number_value: Option<i64>,
    #[serde(default)]
    pub version: Option<i64>,
}

/// Options for a function, sink or source update.
///
/// Mirrors Java's `UpdateOptions`, sent as a `updateOptions` multipart part.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateOptions {
    /// Re-read the caller's credentials and store them with the function, so a
    /// rotated token takes effect without recreating it.
    #[serde(default)]
    pub update_auth_data: bool,
}

/// A built-in connector or function shipped with the broker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorDefinition {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub source_class: Option<String>,
    #[serde(default)]
    pub sink_class: Option<String>,
    #[serde(default)]
    pub source_config_class: Option<String>,
    #[serde(default)]
    pub sink_config_class: Option<String>,
}

/// A built-in function shipped with the broker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionDefinition {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub function_class: Option<String>,
}

/// Aggregate status of a sink or source connector.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorStatus {
    #[serde(default)]
    pub num_instances: i32,
    #[serde(default)]
    pub num_running: i32,
    #[serde(default)]
    pub instances: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------- packages

/// Metadata attached to a package version.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackageMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
    /// Set by the broker on upload; ignored on update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_time: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modification_time: Option<i64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

/// Which kind of package a name refers to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PackageType {
    #[default]
    Function,
    Sink,
    Source,
}

impl PackageType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            PackageType::Function => "function",
            PackageType::Sink => "sink",
            PackageType::Source => "source",
        }
    }
}

// ------------------------------------------------------------------ worker

/// One function worker in the cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerInfo {
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub worker_hostname: Option<String>,
    #[serde(default)]
    pub port: i32,
}

/// Per-instance metrics reported by a function worker.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerFunctionInstanceStats {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<serde_json::Value>,
}

// ------------------------------------------------------- load and metrics

/// A single resource's usage against its limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceUsage {
    #[serde(default)]
    pub usage: f64,
    #[serde(default)]
    pub limit: f64,
}

/// Aggregate traffic over one namespace bundle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceBundleStats {
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_throughput_in: f64,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub msg_throughput_out: f64,
    #[serde(default)]
    pub consumer_count: i32,
    #[serde(default)]
    pub producer_count: i32,
    #[serde(default)]
    pub topics: i64,
    #[serde(default)]
    pub cache_size: i64,
}

/// A broker's load report, as the load manager publishes it.
///
/// Mirrors Java's `LoadManagerReport` / `LocalBrokerData`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadManagerReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_service_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_service_url_tls: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pulsar_service_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pulsar_service_url_tls: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_version_string: Option<String>,
    #[serde(default)]
    pub cpu: ResourceUsage,
    #[serde(default)]
    pub memory: ResourceUsage,
    #[serde(default)]
    pub direct_memory: ResourceUsage,
    #[serde(default)]
    pub bandwidth_in: ResourceUsage,
    #[serde(default)]
    pub bandwidth_out: ResourceUsage,
    #[serde(default)]
    pub msg_throughput_in: f64,
    #[serde(default)]
    pub msg_throughput_out: f64,
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub last_update: i64,
    #[serde(default)]
    pub num_topics: i64,
    #[serde(default)]
    pub num_bundles: i32,
    #[serde(default)]
    pub num_consumers: i32,
    #[serde(default)]
    pub num_producers: i32,
    /// Per-bundle stats, spelled `lastStats` on the wire.
    #[serde(default, rename = "lastStats")]
    pub bundle_stats: BTreeMap<String, NamespaceBundleStats>,
    #[serde(default)]
    pub bundles: BTreeSet<String>,
}

/// One dimensioned metrics sample.
///
/// Mirrors Java's `org.apache.pulsar.common.stats.Metrics`: `dimensions` labels the
/// sample and `metrics` holds the values, which are numbers or strings depending on
/// the metric.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    #[serde(default)]
    pub dimensions: BTreeMap<String, String>,
    #[serde(default)]
    pub metrics: BTreeMap<String, serde_json::Value>,
}

/// Whether a position is present in a subscription's pending-ack store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionInPendingAckState {
    PendingAck,
    MarkDelete,
    NotInPendingAck,
    /// The store has not finished initializing, so nothing can be said yet.
    #[default]
    PendingAckNotReady,
    InvalidPosition,
}

/// A position's status inside a subscription's pending-ack store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionInPendingAckStats {
    #[serde(default)]
    pub state: PositionInPendingAckState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<i32>,
}

// ------------------------------------------------------ metadata migration

/// How far a metadata-store migration has progressed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MigrationPhase {
    #[default]
    NotStarted,
    Preparation,
    Copying,
    Completed,
    Failed,
}

/// State of a metadata-store migration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationState {
    #[serde(default)]
    pub phase: MigrationPhase,
    /// The store being migrated to; absent before a migration starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
}

// ------------------------------------------------------------- proxy stats

/// Rates for a single client connection through a Pulsar proxy.
///
/// Mirrors `org.apache.pulsar.proxy.stats.ConnectionStats`. The addresses are
/// Netty's `SocketAddress` renderings, so they carry a leading `/`
/// (`/127.0.0.1:54321`), and either can be absent while a connection is still
/// being set up.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConnectionStats {
    #[serde(default)]
    pub request_rate: f64,
    #[serde(default)]
    pub byte_rate: f64,
    /// Remote address of the client that connected to the proxy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_address: Option<String>,
    /// Remote address of the broker the proxy forwards to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_address: Option<String>,
}

/// Per-topic rates as observed by a Pulsar proxy.
///
/// Mirrors `org.apache.pulsar.proxy.stats.TopicStats`. This is the proxy's own
/// view of traffic passing through it and is unrelated to the broker's
/// [`TopicStats`]; only these four fields are serialized.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyTopicStats {
    #[serde(default)]
    pub msg_rate_in: f64,
    #[serde(default)]
    pub msg_byte_in: f64,
    #[serde(default)]
    pub msg_rate_out: f64,
    #[serde(default)]
    pub msg_byte_out: f64,
}

/// Serde adapter for a Java `byte[]`, which Jackson renders as a base64 string.
mod base64_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => STANDARD.encode(bytes).serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        let Some(text) = Option::<String>::deserialize(deserializer)? else {
            return Ok(None);
        };
        STANDARD
            .decode(text.as_bytes())
            .map(Some)
            .map_err(serde::de::Error::custom)
    }
}

// ------------------------------------------------- namespace bulk operations

/// Where each topic in a bundle falls on the hash ring.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicHashPositions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle: Option<String>,
    /// Topic name -> its position on the bundle's ring.
    #[serde(default)]
    pub topic_hash_positions: BTreeMap<String, i64>,
}

/// One entry of a bulk topic-permission grant.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantTopicPermissionOptions {
    pub topic: String,
    pub role: String,
    /// `produce`, `consume`, `functions`, `sources` or `sinks`.
    #[serde(default)]
    pub actions: BTreeSet<String>,
}

/// One entry of a bulk topic-permission revocation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeTopicPermissionOptions {
    pub topic: String,
    pub role: String,
}
