//! Topic-level policy overrides — `/admin/v2/{domain}/{tenant}/{ns}/{topic}/...`.
//!
//! Mirrors `org.apache.pulsar.client.admin.TopicPolicies`. A topic policy
//! overrides the namespace policy of the same name; the namespace-level
//! equivalents live in [`Namespaces`][crate::admin::namespaces::Namespaces].
//!
//! The broker only stores topic policies when `topicLevelPoliciesEnabled=true`,
//! and the topic must already exist.
//!
//! # Reading effective vs overridden values
//!
//! Every getter takes an `applied` flag, matching the Java client:
//!
//! * `applied = false` returns only a value set *on this topic*, or `None`.
//! * `applied = true` returns the value actually in force, falling back to the
//!   namespace policy and then the broker default.

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        models::{
            AutoSubscriptionCreationOverride, BacklogQuota, BacklogQuotaType,
            DelayedDeliveryPolicies, DispatchRate, EntryFilters, InactiveTopicPolicies,
            OffloadPolicies, PersistencePolicies, PublishRate, RetentionPolicies,
            SchemaCompatibilityStrategy, SubscribeRate,
        },
        parse_topic_path, AdminClient,
    },
    Error,
};

/// Handle for the topic-policy group of admin operations.
///
/// Obtained from [`AdminClient::topic_policies`] for the cluster-local policy set,
/// or [`AdminClient::topic_policies_global`] for the geo-replicated one.
pub struct TopicPolicies<'a> {
    pub(crate) client: &'a AdminClient,
    /// Selects the geo-replicated policy set rather than the cluster-local one.
    ///
    /// Mirrors Java's `PulsarAdmin.topicPolicies(boolean isGlobal)`: the flag is a
    /// property of the handle, so it applies to every operation reached through it.
    pub(crate) is_global: bool,
}

impl TopicPolicies<'_> {
    fn policy_url(&self, topic: &str, policy: &str) -> Result<String, Error> {
        parse_topic_path(topic).map(|segments| {
            let mut all: Vec<&str> = segments.iter().map(String::as_str).collect();
            all.push(policy);
            let url = self.client.url(&all);
            // Appended here rather than at each of the ~90 call sites. Methods that
            // add their own query parameters still work: reqwest appends to an
            // existing query string with `&`.
            if self.is_global {
                format!("{url}?isGlobal=true")
            } else {
                url
            }
        })
    }

    /// Removes every topic-level policy override at once.
    pub async fn delete_topic_policies(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "policies")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Gets the clusters this topic replicates to.
    pub async fn get_replication_clusters(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<Vec<String>>, Error> {
        let url = self.policy_url(topic, "replication")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Removes the topic-level replication override.
    pub async fn remove_replication_clusters(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "replication")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Gets the subscription types this topic permits.
    pub async fn get_subscription_types_enabled(
        &self,
        topic: &str,
    ) -> Result<Option<Vec<String>>, Error> {
        let url = self.policy_url(topic, "subscriptionTypesEnabled")?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Restricts which subscription types this topic permits.
    pub async fn set_subscription_types_enabled(
        &self,
        topic: &str,
        types: &[String],
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscriptionTypesEnabled")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(types))
            .await
    }

    /// Removes the topic-level subscription-type restriction.
    pub async fn remove_subscription_types_enabled(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscriptionTypesEnabled")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Acknowledged-message retention.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_retention(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<RetentionPolicies>, Error> {
        let url = self.policy_url(topic, "retention")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Acknowledged-message retention.
    pub async fn set_retention(&self, topic: &str, value: &RetentionPolicies) -> Result<(), Error> {
        let url = self.policy_url(topic, "retention")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Acknowledged-message retention.
    pub async fn remove_retention(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "retention")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// BookKeeper ensemble sizing.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_persistence(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<PersistencePolicies>, Error> {
        let url = self.policy_url(topic, "persistence")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: BookKeeper ensemble sizing.
    pub async fn set_persistence(
        &self,
        topic: &str,
        value: &PersistencePolicies,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "persistence")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: BookKeeper ensemble sizing.
    pub async fn remove_persistence(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "persistence")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Dispatch throttling.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_dispatch_rate(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<DispatchRate>, Error> {
        let url = self.policy_url(topic, "dispatchRate")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Dispatch throttling.
    pub async fn set_dispatch_rate(&self, topic: &str, value: &DispatchRate) -> Result<(), Error> {
        let url = self.policy_url(topic, "dispatchRate")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Dispatch throttling.
    pub async fn remove_dispatch_rate(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "dispatchRate")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Per-subscription dispatch throttling.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_subscription_dispatch_rate(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<DispatchRate>, Error> {
        let url = self.policy_url(topic, "subscriptionDispatchRate")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Per-subscription dispatch throttling.
    pub async fn set_subscription_dispatch_rate(
        &self,
        topic: &str,
        value: &DispatchRate,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscriptionDispatchRate")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Per-subscription dispatch throttling.
    pub async fn remove_subscription_dispatch_rate(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscriptionDispatchRate")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Geo-replication dispatch throttling.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_replicator_dispatch_rate(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<DispatchRate>, Error> {
        let url = self.policy_url(topic, "replicatorDispatchRate")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Geo-replication dispatch throttling.
    pub async fn set_replicator_dispatch_rate(
        &self,
        topic: &str,
        value: &DispatchRate,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "replicatorDispatchRate")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Geo-replication dispatch throttling.
    pub async fn remove_replicator_dispatch_rate(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "replicatorDispatchRate")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Publish throttling.
    pub async fn get_publish_rate(&self, topic: &str) -> Result<Option<PublishRate>, Error> {
        let url = self.policy_url(topic, "publishRate")?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets, for this topic: Publish throttling.
    pub async fn set_publish_rate(&self, topic: &str, value: &PublishRate) -> Result<(), Error> {
        let url = self.policy_url(topic, "publishRate")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Publish throttling.
    pub async fn remove_publish_rate(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "publishRate")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Subscribe throttling.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_subscribe_rate(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<SubscribeRate>, Error> {
        let url = self.policy_url(topic, "subscribeRate")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Subscribe throttling.
    pub async fn set_subscribe_rate(
        &self,
        topic: &str,
        value: &SubscribeRate,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscribeRate")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Subscribe throttling.
    pub async fn remove_subscribe_rate(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscribeRate")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// When the broker deletes this topic while inactive.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_inactive_topic_policies(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<InactiveTopicPolicies>, Error> {
        let url = self.policy_url(topic, "inactiveTopicPolicies")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: When the broker deletes this topic while inactive.
    pub async fn set_inactive_topic_policies(
        &self,
        topic: &str,
        value: &InactiveTopicPolicies,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "inactiveTopicPolicies")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: When the broker deletes this topic while inactive.
    pub async fn remove_inactive_topic_policies(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "inactiveTopicPolicies")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Delayed-delivery tracking.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_delayed_delivery_policy(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<DelayedDeliveryPolicies>, Error> {
        let url = self.policy_url(topic, "delayedDelivery")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Delayed-delivery tracking.
    pub async fn set_delayed_delivery_policy(
        &self,
        topic: &str,
        value: &DelayedDeliveryPolicies,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "delayedDelivery")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Delayed-delivery tracking.
    pub async fn remove_delayed_delivery_policy(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "delayedDelivery")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Tiered-storage offload configuration.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_offload_policies(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<OffloadPolicies>, Error> {
        let url = self.policy_url(topic, "offloadPolicies")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Tiered-storage offload configuration.
    pub async fn set_offload_policies(
        &self,
        topic: &str,
        value: &OffloadPolicies,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "offloadPolicies")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Tiered-storage offload configuration.
    pub async fn remove_offload_policies(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "offloadPolicies")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Whether subscriptions are auto-created.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_auto_subscription_creation(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<AutoSubscriptionCreationOverride>, Error> {
        let url = self.policy_url(topic, "autoSubscriptionCreation")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Whether subscriptions are auto-created.
    pub async fn set_auto_subscription_creation(
        &self,
        topic: &str,
        value: &AutoSubscriptionCreationOverride,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "autoSubscriptionCreation")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Whether subscriptions are auto-created.
    pub async fn remove_auto_subscription_creation(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "autoSubscriptionCreation")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Broker-side entry filters.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_entry_filters(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<EntryFilters>, Error> {
        let url = self.policy_url(topic, "entryFilters")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Broker-side entry filters.
    pub async fn set_entry_filters(&self, topic: &str, value: &EntryFilters) -> Result<(), Error> {
        let url = self.policy_url(topic, "entryFilters")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the topic-level override for: Broker-side entry filters.
    pub async fn remove_entry_filters(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "entryFilters")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Schema evolution rule.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_schema_compatibility_strategy(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<SchemaCompatibilityStrategy>, Error> {
        let url = self.policy_url(topic, "schemaCompatibilityStrategy")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Schema evolution rule.
    pub async fn set_schema_compatibility_strategy(
        &self,
        topic: &str,
        value: SchemaCompatibilityStrategy,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "schemaCompatibilityStrategy")?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Schema evolution rule.
    pub async fn remove_schema_compatibility_strategy(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "schemaCompatibilityStrategy")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Unacknowledged-message cap per consumer before dispatch pauses.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_max_unacked_messages_on_consumer(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "maxUnackedMessagesOnConsumer")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Unacknowledged-message cap per consumer before dispatch pauses.
    pub async fn set_max_unacked_messages_on_consumer(
        &self,
        topic: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxUnackedMessagesOnConsumer")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Unacknowledged-message cap per consumer before dispatch pauses.
    pub async fn remove_max_unacked_messages_on_consumer(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxUnackedMessagesOnConsumer")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Unacknowledged-message cap per subscription before dispatch pauses.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_max_unacked_messages_on_subscription(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "maxUnackedMessagesOnSubscription")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Unacknowledged-message cap per subscription before dispatch pauses.
    pub async fn set_max_unacked_messages_on_subscription(
        &self,
        topic: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxUnackedMessagesOnSubscription")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Unacknowledged-message cap per subscription before dispatch pauses.
    pub async fn remove_max_unacked_messages_on_subscription(
        &self,
        topic: &str,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxUnackedMessagesOnSubscription")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Producer cap for this topic.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_max_producers(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "maxProducers")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Producer cap for this topic.
    pub async fn set_max_producers(&self, topic: &str, value: i32) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxProducers")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Producer cap for this topic.
    pub async fn remove_max_producers(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxProducers")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Consumer cap for this topic.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_max_consumers(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "maxConsumers")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Consumer cap for this topic.
    pub async fn set_max_consumers(&self, topic: &str, value: i32) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxConsumers")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Consumer cap for this topic.
    pub async fn remove_max_consumers(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxConsumers")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Consumer cap per subscription.
    pub async fn get_max_consumers_per_subscription(
        &self,
        topic: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "maxConsumersPerSubscription")?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets, for this topic: Consumer cap per subscription.
    pub async fn set_max_consumers_per_subscription(
        &self,
        topic: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxConsumersPerSubscription")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Consumer cap per subscription.
    pub async fn remove_max_consumers_per_subscription(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxConsumersPerSubscription")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Subscription cap for this topic.
    pub async fn get_max_subscriptions_per_topic(&self, topic: &str) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "maxSubscriptionsPerTopic")?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets, for this topic: Subscription cap for this topic.
    pub async fn set_max_subscriptions_per_topic(
        &self,
        topic: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxSubscriptionsPerTopic")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Subscription cap for this topic.
    pub async fn remove_max_subscriptions_per_topic(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxSubscriptionsPerTopic")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Largest accepted message, in bytes.
    pub async fn get_max_message_size(&self, topic: &str) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "maxMessageSize")?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets, for this topic: Largest accepted message, in bytes.
    pub async fn set_max_message_size(&self, topic: &str, value: i32) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxMessageSize")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Largest accepted message, in bytes.
    pub async fn remove_max_message_size(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "maxMessageSize")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Backlog bytes after which compaction is triggered.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_compaction_threshold(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<i64>, Error> {
        let url = self.policy_url(topic, "compactionThreshold")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Backlog bytes after which compaction is triggered.
    pub async fn set_compaction_threshold(&self, topic: &str, value: i64) -> Result<(), Error> {
        let url = self.policy_url(topic, "compactionThreshold")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Backlog bytes after which compaction is triggered.
    pub async fn remove_compaction_threshold(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "compactionThreshold")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Entries between deduplication cursor snapshots.
    pub async fn get_deduplication_snapshot_interval(
        &self,
        topic: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "deduplicationSnapshotInterval")?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets, for this topic: Entries between deduplication cursor snapshots.
    pub async fn set_deduplication_snapshot_interval(
        &self,
        topic: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "deduplicationSnapshotInterval")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Entries between deduplication cursor snapshots.
    pub async fn remove_deduplication_snapshot_interval(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "deduplicationSnapshotInterval")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Minutes an inactive subscription survives.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_subscription_expiration_time(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "subscriptionExpirationTime")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Minutes an inactive subscription survives.
    ///
    /// Unlike the other scalar policies this one travels as a query parameter
    /// rather than a JSON body; a body is accepted with 204 and then ignored.
    pub async fn set_subscription_expiration_time(
        &self,
        topic: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscriptionExpirationTime")?;
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("subscriptionExpirationTime", value.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Removes the topic-level override for: Minutes an inactive subscription survives.
    pub async fn remove_subscription_expiration_time(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "subscriptionExpirationTime")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Whether the broker deduplicates by producer sequence id.
    ///
    /// With `applied = true` this falls back to the namespace policy and then
    /// the broker default; with `false` it reports only a topic-level override.
    pub async fn get_deduplication_status(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<bool>, Error> {
        let url = self.policy_url(topic, "deduplicationEnabled")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: Whether the broker deduplicates by producer sequence id.
    pub async fn set_deduplication_status(&self, topic: &str, value: bool) -> Result<(), Error> {
        let url = self.policy_url(topic, "deduplicationEnabled")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the topic-level override for: Whether the broker deduplicates by producer sequence id.
    pub async fn remove_deduplication_status(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "deduplicationEnabled")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    // --------------------------------------------------------- backlog quota

    /// Gets this topic's backlog quotas, keyed by quota type.
    ///
    /// Read from `backlogQuotaMap`; the setter and remover use `backlogQuota`.
    pub async fn get_backlog_quota_map(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<std::collections::BTreeMap<String, BacklogQuota>, Error> {
        let url = self.policy_url(topic, "backlogQuotaMap")?;
        Ok(self
            .client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await?
            .unwrap_or_default())
    }

    /// Sets, for this topic, the backlog quota of one type.
    pub async fn set_backlog_quota(
        &self,
        topic: &str,
        quota: &BacklogQuota,
        quota_type: BacklogQuotaType,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "backlogQuota")?;
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("backlogQuotaType", quota_type.as_str().to_string())],
                Some(quota),
            )
            .await
    }

    /// Removes this topic's override for the backlog quota of one type.
    pub async fn remove_backlog_quota(
        &self,
        topic: &str,
        quota_type: BacklogQuotaType,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "backlogQuota")?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("backlogQuotaType", quota_type.as_str().to_string())],
                NO_BODY,
            )
            .await
    }

    // ----------------------------------------------------------- message TTL

    /// Gets this topic's message TTL, in seconds.
    pub async fn get_message_ttl(&self, topic: &str, applied: bool) -> Result<Option<i32>, Error> {
        let url = self.policy_url(topic, "messageTTL")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Sets, for this topic: message TTL in seconds.
    ///
    /// The value is a **query parameter**, not a body.
    pub async fn set_message_ttl(&self, topic: &str, seconds: i32) -> Result<(), Error> {
        let url = self.policy_url(topic, "messageTTL")?;
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("messageTTL", seconds.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Removes this topic's message-TTL override.
    pub async fn remove_message_ttl(&self, topic: &str) -> Result<(), Error> {
        let url = self.policy_url(topic, "messageTTL")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    // ------------------------------------------------------ dispatcher pause

    /// Gets whether dispatch pauses until acknowledgement state is persisted.
    pub async fn get_dispatcher_pause_on_ack_state_persistent(
        &self,
        topic: &str,
        applied: bool,
    ) -> Result<Option<bool>, Error> {
        let url = self.policy_url(topic, "dispatcherPauseOnAckStatePersistent")?;
        self.client
            .send_json_opt(
                Method::GET,
                &url,
                &[("applied", applied.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Enables, for this topic: dispatch pauses until acknowledgement state is
    /// persisted.
    ///
    /// Takes no value — as at namespace level, POST enables and DELETE clears.
    pub async fn set_dispatcher_pause_on_ack_state_persistent(
        &self,
        topic: &str,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "dispatcherPauseOnAckStatePersistent")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(""))
            .await
    }

    /// Removes this topic's dispatcher-pause override.
    pub async fn remove_dispatcher_pause_on_ack_state_persistent(
        &self,
        topic: &str,
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "dispatcherPauseOnAckStatePersistent")?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Sets the clusters this topic replicates to.
    ///
    /// The getter and remover existed without this, so a topic-level replication
    /// override could be read and cleared but never established.
    pub async fn set_replication_clusters(
        &self,
        topic: &str,
        clusters: &[String],
    ) -> Result<(), Error> {
        let url = self.policy_url(topic, "replication")?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(clusters))
            .await
    }
}
