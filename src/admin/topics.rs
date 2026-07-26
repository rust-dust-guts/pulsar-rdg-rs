//! Topic administration — `/admin/v2/{domain}/{tenant}/{namespace}/{topic}`.
//!
//! Mirrors the data-plane half of `org.apache.pulsar.client.admin.Topics`:
//! lifecycle, partitions, subscriptions, cursors, stats and maintenance actions.
//!
//! The *policy* half of the Java `Topics` interface is deprecated in favour of
//! `TopicPolicies`, so those operations live on
//! [`TopicPolicies`][crate::admin::topic_policies::TopicPolicies] instead of being
//! duplicated here.

use std::collections::BTreeMap;

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{
            AnalyzeSubscriptionBacklogResult, GetStatsOptions, LongRunningProcessStatus,
            MessageIdData, OffloadProcessStatus, PartitionedTopicInternalStats,
            PartitionedTopicMetadata, PartitionedTopicStats, PeekedMessage,
            PersistentTopicInternalStats, TopicStats,
        },
        parse_topic_path, split_namespace, AdminClient,
    },
    Error,
};

/// Where to start when examining a message by position.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MessagePosition {
    /// Count forward from the oldest message.
    #[default]
    Earliest,
    /// Count backward from the newest message.
    Latest,
}

impl MessagePosition {
    fn as_str(self) -> &'static str {
        match self {
            MessagePosition::Earliest => "earliest",
            MessagePosition::Latest => "latest",
        }
    }
}

/// Handle for the `topics` group of admin operations.
///
/// Obtained from [`AdminClient::topics`].
pub struct Topics<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Topics<'_> {
    fn topic_url(&self, topic: &str, extra: &[&str]) -> Result<String, Error> {
        let segments = parse_topic_path(topic)?;
        let mut all: Vec<&str> = segments.iter().map(String::as_str).collect();
        all.extend_from_slice(extra);
        Ok(self.client.url(&all))
    }

    fn sub_url(&self, topic: &str, subscription: &str, extra: &[&str]) -> Result<String, Error> {
        let encoded = encode_segment(subscription);
        let mut all: Vec<&str> = vec!["subscription", &encoded];
        all.extend_from_slice(extra);
        self.topic_url(topic, &all)
    }

    fn ns_url(&self, domain: &str, namespace: &str, extra: &[&str]) -> Result<String, Error> {
        let (tenant, ns) = split_namespace(namespace)?;
        let (tenant, ns) = (encode_segment(tenant), encode_segment(ns));
        let mut all: Vec<&str> = vec![domain, &tenant, &ns];
        all.extend_from_slice(extra);
        Ok(self.client.url(&all))
    }

    // ----------------------------------------------------------- listing

    /// Lists the **persistent** topics in a namespace.
    ///
    /// Deliberately narrower than Java's `getList`, which unions the persistent and
    /// non-persistent domains when no domain is given. Use
    /// [`get_non_persistent_list`][Self::get_non_persistent_list] for the other
    /// domain, and concatenate if you want Java's combined answer.
    pub async fn get_list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        let url = self.ns_url("persistent", namespace, &[])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the non-persistent topics in a namespace.
    pub async fn get_non_persistent_list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        let url = self.ns_url("non-persistent", namespace, &[])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the **persistent** partitioned topics in a namespace.
    ///
    /// As with [`get_list`][Self::get_list], this is one domain rather than Java's
    /// union of both.
    pub async fn get_partitioned_topic_list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        let url = self.ns_url("persistent", namespace, &["partitioned"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the topics owned by one namespace bundle.
    pub async fn get_list_in_bundle(
        &self,
        namespace: &str,
        bundle: &str,
    ) -> Result<Vec<String>, Error> {
        let encoded = encode_segment(bundle);
        let url = self.ns_url("non-persistent", namespace, &[&encoded])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    // --------------------------------------------------------- lifecycle

    /// Creates a non-partitioned topic.
    pub async fn create_non_partitioned_topic(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &[])?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Creates a partitioned topic with `num_partitions` partitions.
    pub async fn create_partitioned_topic(
        &self,
        topic: &str,
        num_partitions: i32,
    ) -> Result<(), Error> {
        let url = self.topic_url(topic, &["partitions"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&num_partitions))
            .await
    }

    /// Increases a partitioned topic's partition count.
    ///
    /// The count can only grow; Pulsar cannot reduce partitions.
    pub async fn update_partitioned_topic(
        &self,
        topic: &str,
        num_partitions: i32,
    ) -> Result<(), Error> {
        let url = self.topic_url(topic, &["partitions"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&num_partitions))
            .await
    }

    /// Creates any partitions that are missing from a partitioned topic.
    pub async fn create_missed_partitions(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["createMissedPartitions"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Gets a topic's partition count and properties.
    pub async fn get_partitioned_topic_metadata(
        &self,
        topic: &str,
    ) -> Result<PartitionedTopicMetadata, Error> {
        let url = self.topic_url(topic, &["partitions"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Deletes a non-partitioned topic.
    pub async fn delete(&self, topic: &str, force: bool) -> Result<(), Error> {
        let url = self.topic_url(topic, &[])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Deletes a partitioned topic and all of its partitions.
    pub async fn delete_partitioned_topic(&self, topic: &str, force: bool) -> Result<(), Error> {
        let url = self.topic_url(topic, &["partitions"])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Releases the topic so the load manager can reassign it.
    pub async fn unload(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["unload"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Permanently seals the topic against further writes.
    pub async fn terminate_topic(&self, topic: &str) -> Result<MessageIdData, Error> {
        let url = self.topic_url(topic, &["terminate"])?;
        self.client
            .send_json(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Seals every partition of a partitioned topic.
    pub async fn terminate_partitioned_topic(
        &self,
        topic: &str,
    ) -> Result<BTreeMap<String, MessageIdData>, Error> {
        let url = self.topic_url(topic, &["terminate", "partitions"])?;
        self.client
            .send_json(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Deletes all data in the topic, keeping the topic itself.
    pub async fn truncate(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["truncate"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    // -------------------------------------------------------- properties

    /// Gets the topic's free-form properties.
    pub async fn get_properties(&self, topic: &str) -> Result<BTreeMap<String, String>, Error> {
        let url = self.topic_url(topic, &["properties"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Merges `properties` into the topic's properties.
    pub async fn update_properties(
        &self,
        topic: &str,
        properties: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        let url = self.topic_url(topic, &["properties"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(properties))
            .await
    }

    /// Removes one property.
    pub async fn remove_properties(&self, topic: &str, key: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["properties"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[("key", key.to_string())], NO_BODY)
            .await
    }

    // ------------------------------------------------------- permissions

    /// Gets the actions each role may perform on the topic.
    pub async fn get_permissions(
        &self,
        topic: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, Error> {
        let url = self.topic_url(topic, &["permissions"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Grants a role actions on the topic.
    pub async fn grant_permission(
        &self,
        topic: &str,
        role: &str,
        actions: &[String],
    ) -> Result<(), Error> {
        let encoded = encode_segment(role);
        let url = self.topic_url(topic, &["permissions", &encoded])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(actions))
            .await
    }

    /// Revokes all of a role's topic permissions.
    pub async fn revoke_permissions(&self, topic: &str, role: &str) -> Result<(), Error> {
        let encoded = encode_segment(role);
        let url = self.topic_url(topic, &["permissions", &encoded])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    // ------------------------------------------------------------ stats

    /// Gets runtime statistics for the topic.
    pub async fn get_stats(
        &self,
        topic: &str,
        options: GetStatsOptions,
    ) -> Result<TopicStats, Error> {
        let url = self.topic_url(topic, &["stats"])?;
        self.client
            .send_json(Method::GET, &url, &options.to_query(), NO_BODY)
            .await
    }

    /// Gets managed-ledger internals for the topic.
    pub async fn get_internal_stats(
        &self,
        topic: &str,
    ) -> Result<PersistentTopicInternalStats, Error> {
        let url = self.topic_url(topic, &["internalStats"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets aggregated statistics for a partitioned topic.
    ///
    /// With `per_partition` the response also carries each partition separately.
    pub async fn get_partitioned_stats(
        &self,
        topic: &str,
        per_partition: bool,
        options: GetStatsOptions,
    ) -> Result<PartitionedTopicStats, Error> {
        let url = self.topic_url(topic, &["partitioned-stats"])?;
        let mut query = options.to_query();
        query.push(("perPartition", per_partition.to_string()));
        self.client
            .send_json(Method::GET, &url, &query, NO_BODY)
            .await
    }

    /// Gets aggregated internal statistics for a partitioned topic.
    pub async fn get_partitioned_internal_stats(
        &self,
        topic: &str,
    ) -> Result<PartitionedTopicInternalStats, Error> {
        let url = self.topic_url(topic, &["partitioned-internalStats"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    // ---------------------------------------------------- subscriptions

    /// Lists the topic's subscription names.
    pub async fn get_subscriptions(&self, topic: &str) -> Result<Vec<String>, Error> {
        let url = self.topic_url(topic, &["subscriptions"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a subscription positioned at the newest message.
    /// Creates a subscription starting at `position`.
    ///
    /// The position is explicit because the two sensible choices behave very
    /// differently: [`MessageIdData::latest`] starts with the next message, while
    /// [`MessageIdData::earliest`] replays the entire retained backlog. Java's
    /// `createSubscription` likewise requires the caller to pass a `MessageId`.
    pub async fn create_subscription(
        &self,
        topic: &str,
        subscription: &str,
        position: &MessageIdData,
    ) -> Result<(), Error> {
        let url = self.sub_url(topic, subscription, &[])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(position))
            .await
    }

    /// Deletes a subscription.
    pub async fn delete_subscription(
        &self,
        topic: &str,
        subscription: &str,
        force: bool,
    ) -> Result<(), Error> {
        let url = self.sub_url(topic, subscription, &[])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets a subscription's free-form properties.
    pub async fn get_subscription_properties(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<BTreeMap<String, String>, Error> {
        let url = self.sub_url(topic, subscription, &["properties"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Replaces a subscription's properties.
    pub async fn update_subscription_properties(
        &self,
        topic: &str,
        subscription: &str,
        properties: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        let url = self.sub_url(topic, subscription, &["properties"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(properties))
            .await
    }

    /// Acknowledges every message in a subscription's backlog.
    pub async fn skip_all_messages(&self, topic: &str, subscription: &str) -> Result<(), Error> {
        let url = self.sub_url(topic, subscription, &["skip_all"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Acknowledges the next `num_messages` of a subscription's backlog.
    pub async fn skip_messages(
        &self,
        topic: &str,
        subscription: &str,
        num_messages: i64,
    ) -> Result<(), Error> {
        let count = num_messages.to_string();
        let url = self.sub_url(topic, subscription, &["skip", &count])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Expires messages older than `expire_time_seconds` on one subscription.
    pub async fn expire_messages(
        &self,
        topic: &str,
        subscription: &str,
        expire_time_seconds: i64,
    ) -> Result<(), Error> {
        let secs = expire_time_seconds.to_string();
        let url = self.sub_url(topic, subscription, &["expireMessages", &secs])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Expires messages older than `expire_time_seconds` on every subscription.
    pub async fn expire_messages_for_all_subscriptions(
        &self,
        topic: &str,
        expire_time_seconds: i64,
    ) -> Result<(), Error> {
        let secs = expire_time_seconds.to_string();
        let url = self.topic_url(topic, &["all_subscription", "expireMessages", &secs])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Moves a subscription's cursor to the given publish time, in milliseconds.
    pub async fn reset_cursor(
        &self,
        topic: &str,
        subscription: &str,
        timestamp_ms: i64,
    ) -> Result<(), Error> {
        let ts = timestamp_ms.to_string();
        let url = self.sub_url(topic, subscription, &["resetcursor", &ts])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Moves a subscription's cursor to a specific message.
    pub async fn reset_cursor_to_message_id(
        &self,
        topic: &str,
        subscription: &str,
        message_id: &MessageIdData,
    ) -> Result<(), Error> {
        let url = self.sub_url(topic, subscription, &["resetcursor"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(message_id))
            .await
    }

    /// Estimates how much of a subscription's backlog an entry filter would keep.
    pub async fn analyze_subscription_backlog(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<AnalyzeSubscriptionBacklogResult, Error> {
        let url = self.sub_url(topic, subscription, &["analyzeBacklog"])?;
        self.client
            .send_json(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Gets whether a subscription is replicated across clusters.
    pub async fn get_replicated_subscription_status(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<BTreeMap<String, bool>, Error> {
        let url = self.sub_url(topic, subscription, &["replicatedSubscriptionStatus"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Enables or disables cross-cluster replication for a subscription.
    pub async fn set_replicated_subscription_status(
        &self,
        topic: &str,
        subscription: &str,
        enabled: bool,
    ) -> Result<(), Error> {
        let url = self.sub_url(topic, subscription, &["replicatedSubscriptionStatus"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&enabled))
            .await
    }

    // ------------------------------------------------------ maintenance

    /// Starts compaction, keeping only the newest value per key.
    pub async fn trigger_compaction(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["compaction"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Gets the status of the most recent compaction.
    pub async fn compaction_status(&self, topic: &str) -> Result<LongRunningProcessStatus, Error> {
        let url = self.topic_url(topic, &["compaction"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Discards ledgers that every subscription has already consumed.
    pub async fn trim_topic(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["trim"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Starts offloading messages up to `message_id` to tiered storage.
    pub async fn trigger_offload(
        &self,
        topic: &str,
        message_id: &MessageIdData,
    ) -> Result<(), Error> {
        let url = self.topic_url(topic, &["offload"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(message_id))
            .await
    }

    /// Gets the status of the most recent offload.
    pub async fn offload_status(&self, topic: &str) -> Result<OffloadProcessStatus, Error> {
        let url = self.topic_url(topic, &["offload"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    // -------------------------------------------------------- messages

    /// Gets the id of the newest message in the topic.
    pub async fn get_last_message_id(&self, topic: &str) -> Result<MessageIdData, Error> {
        let url = self.topic_url(topic, &["lastMessageId"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Finds the first message published at or after `timestamp_ms`.
    pub async fn get_message_id_by_timestamp(
        &self,
        topic: &str,
        timestamp_ms: i64,
    ) -> Result<MessageIdData, Error> {
        let ts = timestamp_ms.to_string();
        let url = self.topic_url(topic, &["messageid", &ts])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Reads the messages stored in one managed-ledger entry.
    ///
    /// An entry holds a batch, so this returns every message in it. Unlike
    /// [`peek_messages`][Self::peek_messages] it addresses storage directly and
    /// needs no subscription.
    pub async fn get_messages_by_id(
        &self,
        topic: &str,
        ledger_id: i64,
        entry_id: i64,
    ) -> Result<Vec<PeekedMessage>, Error> {
        let (ledger, entry) = (ledger_id.to_string(), entry_id.to_string());
        let url = self.topic_url(topic, &["ledger", &ledger, "entry", &entry])?;
        self.client
            .send_message(Method::GET, &url, &[])
            .await
            .map(|message| vec![message])
    }

    /// The first message in the entry at `ledger_id:entry_id`.
    pub async fn get_message_by_id(
        &self,
        topic: &str,
        ledger_id: i64,
        entry_id: i64,
    ) -> Result<PeekedMessage, Error> {
        self.get_messages_by_id(topic, ledger_id, entry_id)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                Error::Admin(crate::error::AdminError::NotFound(format!(
                    "no message at {ledger_id}:{entry_id} on {topic}"
                )))
            })
    }

    /// Finds the message at a logical `index` within the topic.
    ///
    /// Requires `brokerEntryMetadataInterceptors` to include the index
    /// interceptor; without it the broker answers 412.
    pub async fn get_message_id_by_index(
        &self,
        topic: &str,
        index: i64,
    ) -> Result<MessageIdData, Error> {
        let url = self.topic_url(topic, &["getMessageIdByIndex"])?;
        self.client
            .send_json(Method::GET, &url, &[("index", index.to_string())], NO_BODY)
            .await
    }

    /// Bytes still to be consumed from `message_id` onwards.
    ///
    /// The verb is PUT and the message id travels in the body, not the path.
    pub async fn get_backlog_size_by_message_id(
        &self,
        topic: &str,
        message_id: &MessageIdData,
    ) -> Result<i64, Error> {
        let url = self.topic_url(topic, &["backlogSize"])?;
        self.client
            .send_json(Method::PUT, &url, &[], Some(message_id))
            .await
    }

    /// The topic's managed-ledger metadata, as a raw JSON document.
    ///
    /// Returned as text, as Java's `getInternalInfo` does: this is a dump of
    /// ledger bookkeeping whose shape tracks the storage layer rather than the
    /// admin API. [`get_internal_stats`][Self::get_internal_stats] is the typed,
    /// stable view.
    pub async fn get_internal_info(&self, topic: &str) -> Result<String, Error> {
        let url = self.topic_url(topic, &["internal-info"])?;
        self.client.send_text(Method::GET, &url, &[]).await
    }

    /// Reads messages from a subscription's backlog without acknowledging them.
    pub async fn peek_messages(
        &self,
        topic: &str,
        subscription: &str,
        num_messages: i32,
    ) -> Result<Vec<PeekedMessage>, Error> {
        let mut out = Vec::new();
        // The endpoint returns one message per request, indexed from 1.
        for position in 1..=num_messages {
            let pos = position.to_string();
            let url = self.sub_url(topic, subscription, &["position", &pos])?;
            match self.client.send_message(Method::GET, &url, &[]).await {
                Ok(msg) => out.push(msg),
                // Fewer messages in the backlog than requested is not an error.
                Err(Error::Admin(crate::error::AdminError::NotFound(_))) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Reads one message by its position in the topic, without a subscription.
    pub async fn examine_message(
        &self,
        topic: &str,
        initial_position: MessagePosition,
        message_position: i64,
    ) -> Result<PeekedMessage, Error> {
        let url = self.topic_url(topic, &["examinemessage"])?;
        self.client
            .send_message(
                Method::GET,
                &url,
                &[
                    ("initialPosition", initial_position.as_str().to_string()),
                    ("messagePosition", message_position.to_string()),
                ],
            )
            .await
    }

    // ---------------------------------------------------- shadow topics

    /// Gets the topic's shadow topics.
    pub async fn get_shadow_topics(&self, topic: &str) -> Result<Option<Vec<String>>, Error> {
        let url = self.topic_url(topic, &["shadowTopics"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets the topic's shadow topics.
    pub async fn set_shadow_topics(&self, topic: &str, shadows: &[String]) -> Result<(), Error> {
        let url = self.topic_url(topic, &["shadowTopics"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(shadows))
            .await
    }

    /// Removes the topic's shadow topics.
    pub async fn remove_shadow_topics(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["shadowTopics"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
}
