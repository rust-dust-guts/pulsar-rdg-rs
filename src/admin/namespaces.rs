//! Namespace administration — `/admin/v2/namespaces`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Namespaces`. Every policy shape was
//! verified against a live broker; see `models` for the wire details.

use std::collections::BTreeMap;

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{
            AutoScalePolicyOverride, AutoSubscriptionCreationOverride, AutoTopicCreationOverride,
            BacklogQuota, BacklogQuotaType, BookieAffinityGroupData, BundlesData,
            DelayedDeliveryPolicies, DispatchRate, EntryFilters, GrantTopicPermissionOptions,
            InactiveTopicPolicies, OffloadPolicies, PersistencePolicies, Policies, PublishRate,
            RetentionPolicies, RevokeTopicPermissionOptions, SchemaCompatibilityStrategy,
            SubscribeRate, SubscriptionAuthMode, TopicHashPositions,
        },
        split_namespace, AdminClient,
    },
    Error,
};

/// Handle for the `namespaces` group of admin operations.
///
/// Obtained from [`AdminClient::namespaces`]. Grouping mirrors the Java admin
/// client's separate interfaces and keeps same-named operations on different
/// resource kinds (a namespace retention policy vs a topic one) distinct.
pub struct Namespaces<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Namespaces<'_> {
    // Note: not every policy is removable. Verified against a live broker, these
    // have a setter and a getter but no DELETE route — the broker routes an
    // unmatched DELETE to its delete-bundle handler and answers 412 "Invalid
    // bundle range": deduplicationSnapshotInterval, offloadThreshold,
    // offloadThresholdInSeconds, encryptionRequired, schemaValidationEnforced,
    // isAllowAutoUpdateSchema, subscriptionAuthMode and
    // schemaCompatibilityStrategy. No `remove_*` is generated for those.

    /// Builds `/admin/v2/namespaces/{tenant}/{namespace}/...`.
    fn ns_url(&self, namespace: &str, segments: &[&str]) -> Result<String, Error> {
        let (tenant, ns) = split_namespace(namespace)?;
        let (tenant, ns) = (encode_segment(tenant), encode_segment(ns));
        let mut all = vec!["namespaces", &tenant[..], &ns[..]];
        all.extend_from_slice(segments);
        Ok(self.client.url(&all))
    }

    /// Lists the namespaces of a tenant, as `tenant/namespace` strings.
    pub async fn get_namespaces(&self, tenant: &str) -> Result<Vec<String>, Error> {
        let url = self.client.url(&["namespaces", &encode_segment(tenant)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a namespace with broker defaults.
    pub async fn create_namespace(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &[])?;
        // An empty policy object means "use defaults"; the endpoint requires a body.
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&serde_json::Map::new()))
            .await
    }

    /// Creates a namespace split into `num_bundles` bundles.
    pub async fn create_namespace_with_bundles(
        &self,
        namespace: &str,
        num_bundles: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &[])?;
        let body = serde_json::json!({ "bundles": { "numBundles": num_bundles } });
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&body))
            .await
    }

    /// Deletes a namespace.
    ///
    /// Unless `force` is set the namespace must be empty; forced deletion also
    /// requires `forceDeleteNamespaceAllowed=true` on the broker.
    pub async fn delete_namespace(&self, namespace: &str, force: bool) -> Result<(), Error> {
        let url = self.ns_url(namespace, &[])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets the namespace's full policy set.
    pub async fn get_policies(&self, namespace: &str) -> Result<Policies, Error> {
        let url = self.ns_url(namespace, &[])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the topics in a namespace.
    pub async fn get_namespace_topics(&self, namespace: &str) -> Result<Vec<String>, Error> {
        let url = self.ns_url(namespace, &["topics"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the namespace's bundle boundaries.
    pub async fn get_bundles(&self, namespace: &str) -> Result<BundlesData, Error> {
        let url = self.ns_url(namespace, &["bundles"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    // ------------------------------------------------------------ permissions

    /// Gets the actions each role may perform, keyed by role.
    pub async fn get_permissions(
        &self,
        namespace: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, Error> {
        let url = self.ns_url(namespace, &["permissions"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Grants a role a set of actions (`produce`, `consume`, `functions`, ...).
    pub async fn grant_permission_on_namespace(
        &self,
        namespace: &str,
        role: &str,
        actions: &[String],
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["permissions", &encode_segment(role)])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(actions))
            .await
    }

    /// Revokes all of a role's namespace permissions.
    pub async fn revoke_permissions_on_namespace(
        &self,
        namespace: &str,
        role: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["permissions", &encode_segment(role)])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Grants roles access to a specific subscription.
    pub async fn grant_permission_on_subscription(
        &self,
        namespace: &str,
        subscription: &str,
        roles: &[String],
    ) -> Result<(), Error> {
        let url = self.ns_url(
            namespace,
            &["permissions", "subscription", &encode_segment(subscription)],
        )?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(roles))
            .await
    }

    /// Revokes one role's access to a subscription.
    pub async fn revoke_permission_on_subscription(
        &self,
        namespace: &str,
        subscription: &str,
        role: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(
            namespace,
            &[
                "permissions",
                &encode_segment(subscription),
                &encode_segment(role),
            ],
        )?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    // -------------------------------------------------------------- clusters

    /// Gets the clusters this namespace replicates to.
    pub async fn get_namespace_replication_clusters(
        &self,
        namespace: &str,
    ) -> Result<Vec<String>, Error> {
        let url = self.ns_url(namespace, &["replication"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Sets the clusters this namespace replicates to.
    ///
    /// `compare_topic_partitions` asks the broker to verify that every topic has a
    /// matching partition count in the new clusters and to refuse the change if not.
    pub async fn set_namespace_replication_clusters(
        &self,
        namespace: &str,
        clusters: &[String],
        compare_topic_partitions: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["replication"])?;
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[(
                    "compareTopicPartitions",
                    compare_topic_partitions.to_string(),
                )],
                Some(clusters),
            )
            .await
    }

    // --------------------------------------------------------- backlog quota

    /// Gets the namespace's backlog quotas, keyed by quota type.
    pub async fn get_backlog_quota_map(
        &self,
        namespace: &str,
    ) -> Result<BTreeMap<String, BacklogQuota>, Error> {
        let url = self.ns_url(namespace, &["backlogQuotaMap"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Sets a backlog quota for one dimension.
    pub async fn set_backlog_quota(
        &self,
        namespace: &str,
        quota: &BacklogQuota,
        quota_type: BacklogQuotaType,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["backlogQuota"])?;
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("backlogQuotaType", quota_type.as_str().to_string())],
                Some(quota),
            )
            .await
    }

    /// Removes a backlog quota for one dimension.
    pub async fn remove_backlog_quota(
        &self,
        namespace: &str,
        quota_type: BacklogQuotaType,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["backlogQuota"])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("backlogQuotaType", quota_type.as_str().to_string())],
                NO_BODY,
            )
            .await
    }

    // -------------------------------------------------------------- actions

    /// Unloads the namespace, releasing every bundle for reassignment.
    pub async fn unload_namespace(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["unload"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Unloads one bundle.
    pub async fn unload_namespace_bundle(
        &self,
        namespace: &str,
        bundle: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &[&encode_segment(bundle), "unload"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Splits one bundle in two.
    pub async fn split_namespace_bundle(
        &self,
        namespace: &str,
        bundle: &str,
        unload_split_bundles: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &[&encode_segment(bundle), "split"])?;
        self.client
            .send_empty(
                Method::PUT,
                &url,
                &[("unload", unload_split_bundles.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Clears the backlog of every subscription in the namespace.
    pub async fn clear_namespace_backlog(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["clearBacklog"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Clears the backlog of one subscription across the namespace.
    pub async fn clear_namespace_backlog_for_subscription(
        &self,
        namespace: &str,
        subscription: &str,
    ) -> Result<(), Error> {
        // The subscription follows the verb: `clearBacklog/{sub}`. Reversing the two
        // makes the broker read the subscription as a bundle range.
        let url = self.ns_url(namespace, &["clearBacklog", &encode_segment(subscription)])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Clears the backlog of every subscription in one bundle.
    pub async fn clear_namespace_bundle_backlog(
        &self,
        namespace: &str,
        bundle: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &[&encode_segment(bundle), "clearBacklog"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Clears the backlog of one subscription within one bundle.
    pub async fn clear_namespace_bundle_backlog_for_subscription(
        &self,
        namespace: &str,
        bundle: &str,
        subscription: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(
            namespace,
            &[
                &encode_segment(bundle),
                "clearBacklog",
                &encode_segment(subscription),
            ],
        )?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Deletes one subscription from every topic in one bundle.
    pub async fn unsubscribe_namespace_bundle(
        &self,
        namespace: &str,
        bundle: &str,
        subscription: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(
            namespace,
            &[
                &encode_segment(bundle),
                "unsubscribe",
                &encode_segment(subscription),
            ],
        )?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Deletes one subscription from every topic in the namespace.
    pub async fn unsubscribe_namespace(
        &self,
        namespace: &str,
        subscription: &str,
    ) -> Result<(), Error> {
        // As with clearBacklog, the subscription follows the verb.
        let url = self.ns_url(namespace, &["unsubscribe", &encode_segment(subscription)])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    // ------------------------------------------------------------ properties

    /// Gets the namespace's free-form properties.
    pub async fn get_namespace_properties(
        &self,
        namespace: &str,
    ) -> Result<BTreeMap<String, String>, Error> {
        let url = self.ns_url(namespace, &["properties"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Replaces the namespace's properties.
    pub async fn set_namespace_properties(
        &self,
        namespace: &str,
        properties: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["properties"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(properties))
            .await
    }

    /// Removes every property.
    pub async fn clear_namespace_properties(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["properties"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Sets one property.
    pub async fn set_namespace_property(
        &self,
        namespace: &str,
        key: &str,
        value: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(
            namespace,
            &["property", &encode_segment(key), &encode_segment(value)],
        )?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Removes one property, returning its previous value if it had one.
    pub async fn remove_namespace_property(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<String>, Error> {
        let url = self.ns_url(namespace, &["property", &encode_segment(key)])?;
        // This endpoint answers with the bare previous value rather than JSON, so
        // it cannot go through the JSON decoder.
        let value = self.client.send_text(Method::DELETE, &url, &[]).await?;
        Ok(if value.is_empty() { None } else { Some(value) })
    }

    /// Seconds before an unacknowledged message expires. `0` disables expiry.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_message_ttl(&self, namespace: &str) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["messageTTL"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Seconds before an unacknowledged message expires. `0` disables expiry.
    pub async fn set_message_ttl(&self, namespace: &str, value: i32) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["messageTTL"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Seconds before an unacknowledged message expires. `0` disables expiry.
    pub async fn remove_message_ttl(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["messageTTL"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Minutes an inactive subscription survives. `0` disables expiry.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_subscription_expiration_time(
        &self,
        namespace: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["subscriptionExpirationTime"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Minutes an inactive subscription survives. `0` disables expiry.
    pub async fn set_subscription_expiration_time(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscriptionExpirationTime"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Minutes an inactive subscription survives. `0` disables expiry.
    pub async fn remove_subscription_expiration_time(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscriptionExpirationTime"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Producer cap per topic. `0` means unlimited.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_max_producers_per_topic(&self, namespace: &str) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["maxProducersPerTopic"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Producer cap per topic. `0` means unlimited.
    pub async fn set_max_producers_per_topic(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxProducersPerTopic"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Producer cap per topic. `0` means unlimited.
    pub async fn remove_max_producers_per_topic(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxProducersPerTopic"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Consumer cap per topic. `0` means unlimited.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_max_consumers_per_topic(&self, namespace: &str) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["maxConsumersPerTopic"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Consumer cap per topic. `0` means unlimited.
    pub async fn set_max_consumers_per_topic(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxConsumersPerTopic"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Consumer cap per topic. `0` means unlimited.
    pub async fn remove_max_consumers_per_topic(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxConsumersPerTopic"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Consumer cap per subscription. `0` means unlimited.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_max_consumers_per_subscription(
        &self,
        namespace: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["maxConsumersPerSubscription"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Consumer cap per subscription. `0` means unlimited.
    pub async fn set_max_consumers_per_subscription(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxConsumersPerSubscription"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Consumer cap per subscription. `0` means unlimited.
    pub async fn remove_max_consumers_per_subscription(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxConsumersPerSubscription"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Unacknowledged-message cap per consumer before dispatch pauses.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_max_unacked_messages_per_consumer(
        &self,
        namespace: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["maxUnackedMessagesPerConsumer"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Unacknowledged-message cap per consumer before dispatch pauses.
    pub async fn set_max_unacked_messages_per_consumer(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxUnackedMessagesPerConsumer"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Unacknowledged-message cap per consumer before dispatch pauses.
    pub async fn remove_max_unacked_messages_per_consumer(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxUnackedMessagesPerConsumer"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Unacknowledged-message cap per subscription before dispatch pauses.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_max_unacked_messages_per_subscription(
        &self,
        namespace: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["maxUnackedMessagesPerSubscription"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Unacknowledged-message cap per subscription before dispatch pauses.
    pub async fn set_max_unacked_messages_per_subscription(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxUnackedMessagesPerSubscription"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Unacknowledged-message cap per subscription before dispatch pauses.
    pub async fn remove_max_unacked_messages_per_subscription(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxUnackedMessagesPerSubscription"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Subscription cap per topic. `0` means unlimited.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_max_subscriptions_per_topic(
        &self,
        namespace: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["maxSubscriptionsPerTopic"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Subscription cap per topic. `0` means unlimited.
    pub async fn set_max_subscriptions_per_topic(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxSubscriptionsPerTopic"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Subscription cap per topic. `0` means unlimited.
    pub async fn remove_max_subscriptions_per_topic(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxSubscriptionsPerTopic"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Topic cap for the namespace. `0` means unlimited.
    ///
    /// After a removal this reports `Some(0)` rather than `None`, unlike the other
    /// scalar policies.
    pub async fn get_max_topics_per_namespace(
        &self,
        namespace: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["maxTopicsPerNamespace"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Topic cap for the namespace. `0` means unlimited.
    pub async fn set_max_topics_per_namespace(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxTopicsPerNamespace"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Topic cap for the namespace. `0` means unlimited.
    pub async fn remove_max_topics_per_namespace(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["maxTopicsPerNamespace"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Entries between deduplication cursor snapshots.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_deduplication_snapshot_interval(
        &self,
        namespace: &str,
    ) -> Result<Option<i32>, Error> {
        let url = self.ns_url(namespace, &["deduplicationSnapshotInterval"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Entries between deduplication cursor snapshots.
    pub async fn set_deduplication_snapshot_interval(
        &self,
        namespace: &str,
        value: i32,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["deduplicationSnapshotInterval"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the deduplication-snapshot interval override.
    ///
    /// There is no DELETE route for this policy — the broker would route it to the
    /// delete-bundle handler and answer 412. Java posts a null instead, which is
    /// what clears it.
    pub async fn remove_deduplication_snapshot_interval(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["deduplicationSnapshotInterval"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&serde_json::Value::Null))
            .await
    }

    /// Backlog bytes after which compaction is triggered.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_compaction_threshold(&self, namespace: &str) -> Result<Option<i64>, Error> {
        let url = self.ns_url(namespace, &["compactionThreshold"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Backlog bytes after which compaction is triggered.
    pub async fn set_compaction_threshold(&self, namespace: &str, value: i64) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["compactionThreshold"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Backlog bytes after which compaction is triggered.
    pub async fn remove_compaction_threshold(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["compactionThreshold"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Backlog bytes after which offloading starts. `-1` disables it.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_offload_threshold(&self, namespace: &str) -> Result<Option<i64>, Error> {
        let url = self.ns_url(namespace, &["offloadThreshold"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Backlog bytes after which offloading starts. `-1` disables it.
    pub async fn set_offload_threshold(&self, namespace: &str, value: i64) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["offloadThreshold"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&value))
            .await
    }

    /// Backlog age in seconds after which offloading starts.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_offload_threshold_in_seconds(
        &self,
        namespace: &str,
    ) -> Result<Option<i64>, Error> {
        let url = self.ns_url(namespace, &["offloadThresholdInSeconds"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Backlog age in seconds after which offloading starts.
    pub async fn set_offload_threshold_in_seconds(
        &self,
        namespace: &str,
        value: i64,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["offloadThresholdInSeconds"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&value))
            .await
    }

    /// Milliseconds to wait before deleting offloaded ledgers from BookKeeper.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_offload_deletion_lag(&self, namespace: &str) -> Result<Option<i64>, Error> {
        let url = self.ns_url(namespace, &["offloadDeletionLagMs"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Milliseconds to wait before deleting offloaded ledgers from BookKeeper.
    pub async fn set_offload_deletion_lag(&self, namespace: &str, value: i64) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["offloadDeletionLagMs"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Milliseconds to wait before deleting offloaded ledgers from BookKeeper.
    pub async fn remove_offload_deletion_lag(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["offloadDeletionLagMs"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Whether the broker deduplicates messages by producer sequence id.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_deduplication_status(&self, namespace: &str) -> Result<Option<bool>, Error> {
        let url = self.ns_url(namespace, &["deduplication"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Whether the broker deduplicates messages by producer sequence id.
    pub async fn set_deduplication_status(
        &self,
        namespace: &str,
        value: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["deduplication"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Removes the namespace-level override for: Whether the broker deduplicates messages by producer sequence id.
    pub async fn remove_deduplication_status(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["deduplication"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Whether producers must encrypt payloads.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_encryption_required_status(
        &self,
        namespace: &str,
    ) -> Result<Option<bool>, Error> {
        let url = self.ns_url(namespace, &["encryptionRequired"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Whether producers must encrypt payloads.
    pub async fn set_encryption_required_status(
        &self,
        namespace: &str,
        value: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["encryptionRequired"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Whether producers without a matching schema are rejected.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_schema_validation_enforced(
        &self,
        namespace: &str,
    ) -> Result<Option<bool>, Error> {
        let url = self.ns_url(namespace, &["schemaValidationEnforced"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Whether producers without a matching schema are rejected.
    pub async fn set_schema_validation_enforced(
        &self,
        namespace: &str,
        value: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["schemaValidationEnforced"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Whether producers may register new schema versions.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_is_allow_auto_update_schema(
        &self,
        namespace: &str,
    ) -> Result<Option<bool>, Error> {
        let url = self.ns_url(namespace, &["isAllowAutoUpdateSchema"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Whether producers may register new schema versions.
    pub async fn set_is_allow_auto_update_schema(
        &self,
        namespace: &str,
        value: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["isAllowAutoUpdateSchema"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Whether dispatch pauses until acknowledgement state is persisted.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_dispatcher_pause_on_ack_state_persistent(
        &self,
        namespace: &str,
    ) -> Result<Option<bool>, Error> {
        let url = self.ns_url(namespace, &["dispatcherPauseOnAckStatePersistent"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Whether dispatch pauses until acknowledgement state is persisted.
    /// Takes no value: the broker ignores the body and POST unconditionally
    /// enables the setting. Use
    /// [`remove_dispatcher_pause_on_ack_state_persistent`][Self::remove_dispatcher_pause_on_ack_state_persistent]
    /// to turn it off — passing `false` here would have read back as `true`.
    pub async fn set_dispatcher_pause_on_ack_state_persistent(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["dispatcherPauseOnAckStatePersistent"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(""))
            .await
    }

    /// Removes the namespace-level override for: Whether dispatch pauses until acknowledgement state is persisted.
    pub async fn remove_dispatcher_pause_on_ack_state_persistent(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["dispatcherPauseOnAckStatePersistent"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Anti-affinity group; the load manager keeps grouped namespaces apart.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_namespace_anti_affinity_group(
        &self,
        namespace: &str,
    ) -> Result<Option<String>, Error> {
        let url = self.ns_url(namespace, &["antiAffinity"])?;
        // Plain text in both directions, like the setter. This used to decode as
        // JSON and appeared to work only because the setter was storing the name
        // *with quotes* — which happens to be valid JSON for the same string.
        let value = self.client.send_text(Method::GET, &url, &[]).await?;
        Ok(if value.is_empty() { None } else { Some(value) })
    }

    /// Sets: Anti-affinity group; the load manager keeps grouped namespaces apart.
    pub async fn set_namespace_anti_affinity_group(
        &self,
        namespace: &str,
        value: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["antiAffinity"])?;
        // Sent as raw text: the broker binds this entity onto a plain `String`, so a
        // JSON-encoded body would store the group name *with its quotes*. The
        // getter echoes the stored text back verbatim, which makes a get/set
        // round-trip look right while
        // [`get_anti_affinity_namespaces`][Self::get_anti_affinity_namespaces]
        // finds nothing.
        self.client
            .send_raw_text(Method::POST, &url, &[], value)
            .await
    }

    /// Removes the namespace-level override for: Anti-affinity group; the load manager keeps grouped namespaces apart.
    pub async fn remove_namespace_anti_affinity_group(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["antiAffinity"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Resource group whose rate limits this namespace shares.
    ///
    /// This endpoint answers with the bare group name rather than JSON, so it
    /// cannot go through the JSON decoder.
    pub async fn get_namespace_resource_group(
        &self,
        namespace: &str,
    ) -> Result<Option<String>, Error> {
        let url = self.ns_url(namespace, &["resourcegroup"])?;
        let value = self.client.send_text(Method::GET, &url, &[]).await?;
        Ok(if value.is_empty() { None } else { Some(value) })
    }

    /// Sets: Resource group whose rate limits this namespace shares.
    ///
    /// The group name travels as a path segment; sent as a JSON body the broker
    /// answers 405 Method Not Allowed.
    pub async fn set_namespace_resource_group(
        &self,
        namespace: &str,
        value: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["resourcegroup", &encode_segment(value)])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Removes the namespace-level override for: Resource group whose rate limits this namespace shares.
    pub async fn remove_namespace_resource_group(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["resourcegroup"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// `None` or `Prefix` — how subscription names are authorized.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_subscription_auth_mode(
        &self,
        namespace: &str,
    ) -> Result<Option<SubscriptionAuthMode>, Error> {
        let url = self.ns_url(namespace, &["subscriptionAuthMode"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: `None` or `Prefix` — how subscription names are authorized.
    pub async fn set_subscription_auth_mode(
        &self,
        namespace: &str,
        value: SubscriptionAuthMode,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscriptionAuthMode"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&value))
            .await
    }

    /// Schema evolution rule, e.g. `FULL`, `BACKWARD`, `FORWARD`, `ALWAYS_COMPATIBLE`.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_schema_compatibility_strategy(
        &self,
        namespace: &str,
    ) -> Result<Option<SchemaCompatibilityStrategy>, Error> {
        let url = self.ns_url(namespace, &["schemaCompatibilityStrategy"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Schema evolution rule, e.g. `FULL`, `BACKWARD`, `FORWARD`, `ALWAYS_COMPATIBLE`.
    pub async fn set_schema_compatibility_strategy(
        &self,
        namespace: &str,
        value: SchemaCompatibilityStrategy,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["schemaCompatibilityStrategy"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(&value))
            .await
    }

    /// Acknowledged-message retention.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_retention(&self, namespace: &str) -> Result<Option<RetentionPolicies>, Error> {
        let url = self.ns_url(namespace, &["retention"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Acknowledged-message retention.
    pub async fn set_retention(
        &self,
        namespace: &str,
        value: &RetentionPolicies,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["retention"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Acknowledged-message retention.
    pub async fn remove_retention(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["retention"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// BookKeeper ensemble sizing.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_persistence(
        &self,
        namespace: &str,
    ) -> Result<Option<PersistencePolicies>, Error> {
        let url = self.ns_url(namespace, &["persistence"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: BookKeeper ensemble sizing.
    pub async fn set_persistence(
        &self,
        namespace: &str,
        value: &PersistencePolicies,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["persistence"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: BookKeeper ensemble sizing.
    pub async fn remove_persistence(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["persistence"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Topic dispatch throttling.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_dispatch_rate(&self, namespace: &str) -> Result<Option<DispatchRate>, Error> {
        let url = self.ns_url(namespace, &["dispatchRate"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Topic dispatch throttling.
    pub async fn set_dispatch_rate(
        &self,
        namespace: &str,
        value: &DispatchRate,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["dispatchRate"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Topic dispatch throttling.
    pub async fn remove_dispatch_rate(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["dispatchRate"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Per-subscription dispatch throttling.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_subscription_dispatch_rate(
        &self,
        namespace: &str,
    ) -> Result<Option<DispatchRate>, Error> {
        let url = self.ns_url(namespace, &["subscriptionDispatchRate"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Per-subscription dispatch throttling.
    pub async fn set_subscription_dispatch_rate(
        &self,
        namespace: &str,
        value: &DispatchRate,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscriptionDispatchRate"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Per-subscription dispatch throttling.
    pub async fn remove_subscription_dispatch_rate(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscriptionDispatchRate"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Geo-replication dispatch throttling.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_replicator_dispatch_rate(
        &self,
        namespace: &str,
    ) -> Result<Option<DispatchRate>, Error> {
        let url = self.ns_url(namespace, &["replicatorDispatchRate"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Geo-replication dispatch throttling.
    pub async fn set_replicator_dispatch_rate(
        &self,
        namespace: &str,
        value: &DispatchRate,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["replicatorDispatchRate"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Geo-replication dispatch throttling.
    pub async fn remove_replicator_dispatch_rate(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["replicatorDispatchRate"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Publish throttling.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_publish_rate(&self, namespace: &str) -> Result<Option<PublishRate>, Error> {
        let url = self.ns_url(namespace, &["publishRate"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Publish throttling.
    pub async fn set_publish_rate(
        &self,
        namespace: &str,
        value: &PublishRate,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["publishRate"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Publish throttling.
    pub async fn remove_publish_rate(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["publishRate"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Subscribe throttling.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_subscribe_rate(
        &self,
        namespace: &str,
    ) -> Result<Option<SubscribeRate>, Error> {
        let url = self.ns_url(namespace, &["subscribeRate"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Subscribe throttling.
    pub async fn set_subscribe_rate(
        &self,
        namespace: &str,
        value: &SubscribeRate,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscribeRate"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Subscribe throttling.
    pub async fn remove_subscribe_rate(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscribeRate"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// When the broker deletes inactive topics.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_inactive_topic_policies(
        &self,
        namespace: &str,
    ) -> Result<Option<InactiveTopicPolicies>, Error> {
        let url = self.ns_url(namespace, &["inactiveTopicPolicies"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: When the broker deletes inactive topics.
    pub async fn set_inactive_topic_policies(
        &self,
        namespace: &str,
        value: &InactiveTopicPolicies,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["inactiveTopicPolicies"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: When the broker deletes inactive topics.
    pub async fn remove_inactive_topic_policies(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["inactiveTopicPolicies"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Delayed-delivery tracking.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_delayed_delivery_messages(
        &self,
        namespace: &str,
    ) -> Result<Option<DelayedDeliveryPolicies>, Error> {
        let url = self.ns_url(namespace, &["delayedDelivery"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Delayed-delivery tracking.
    pub async fn set_delayed_delivery_messages(
        &self,
        namespace: &str,
        value: &DelayedDeliveryPolicies,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["delayedDelivery"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Delayed-delivery tracking.
    pub async fn remove_delayed_delivery_messages(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["delayedDelivery"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Whether and how topics are auto-created.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_auto_topic_creation(
        &self,
        namespace: &str,
    ) -> Result<Option<AutoTopicCreationOverride>, Error> {
        let url = self.ns_url(namespace, &["autoTopicCreation"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Whether and how topics are auto-created.
    pub async fn set_auto_topic_creation(
        &self,
        namespace: &str,
        value: &AutoTopicCreationOverride,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["autoTopicCreation"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Whether and how topics are auto-created.
    pub async fn remove_auto_topic_creation(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["autoTopicCreation"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Whether subscriptions are auto-created.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_auto_subscription_creation(
        &self,
        namespace: &str,
    ) -> Result<Option<AutoSubscriptionCreationOverride>, Error> {
        let url = self.ns_url(namespace, &["autoSubscriptionCreation"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Whether subscriptions are auto-created.
    pub async fn set_auto_subscription_creation(
        &self,
        namespace: &str,
        value: &AutoSubscriptionCreationOverride,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["autoSubscriptionCreation"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Whether subscriptions are auto-created.
    pub async fn remove_auto_subscription_creation(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["autoSubscriptionCreation"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Broker-side entry filters.
    ///
    /// `None` means no namespace-level override is set.
    pub async fn get_namespace_entry_filters(
        &self,
        namespace: &str,
    ) -> Result<Option<EntryFilters>, Error> {
        let url = self.ns_url(namespace, &["entryFilters"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets: Broker-side entry filters.
    pub async fn set_namespace_entry_filters(
        &self,
        namespace: &str,
        value: &EntryFilters,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["entryFilters"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(value))
            .await
    }

    /// Removes the namespace-level override for: Broker-side entry filters.
    pub async fn remove_namespace_entry_filters(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["entryFilters"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
    /// Gets the namespace's BookKeeper affinity groups.
    pub async fn get_bookie_affinity_group(
        &self,
        namespace: &str,
    ) -> Result<Option<BookieAffinityGroupData>, Error> {
        let url = self.ns_url(namespace, &["persistence", "bookieAffinity"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets the namespace's BookKeeper affinity groups.
    pub async fn set_bookie_affinity_group(
        &self,
        namespace: &str,
        data: &BookieAffinityGroupData,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["persistence", "bookieAffinity"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(data))
            .await
    }

    /// Clears the namespace's BookKeeper affinity groups.
    pub async fn delete_bookie_affinity_group(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["persistence", "bookieAffinity"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Gets the subscription types producers may use, or `None` for no override.
    pub async fn get_subscription_types_enabled(
        &self,
        namespace: &str,
    ) -> Result<Option<Vec<String>>, Error> {
        let url = self.ns_url(namespace, &["subscriptionTypesEnabled"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Restricts which subscription types may be used.
    pub async fn set_subscription_types_enabled(
        &self,
        namespace: &str,
        types: &[String],
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscriptionTypesEnabled"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(types))
            .await
    }

    /// Removes the subscription-type restriction.
    pub async fn remove_subscription_types_enabled(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["subscriptionTypesEnabled"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Gets the namespace's tiered-storage offload configuration.
    pub async fn get_offload_policies(
        &self,
        namespace: &str,
    ) -> Result<Option<OffloadPolicies>, Error> {
        let url = self.ns_url(namespace, &["offloadPolicies"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    // ------------------------------------------ scalable-topic auto-scaling

    /// Gets the namespace-wide split/merge policy for scalable topics.
    ///
    /// The per-topic override lives on
    /// [`scalable_topics()`][crate::admin::AdminClient::scalable_topics].
    pub async fn get_scalable_topic_auto_scale_policy(
        &self,
        namespace: &str,
    ) -> Result<Option<AutoScalePolicyOverride>, Error> {
        let url = self.ns_url(namespace, &["scalableTopicAutoScalePolicy"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets the namespace-wide split/merge policy for scalable topics.
    pub async fn set_scalable_topic_auto_scale_policy(
        &self,
        namespace: &str,
        policy: &AutoScalePolicyOverride,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["scalableTopicAutoScalePolicy"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(policy))
            .await
    }

    /// Removes the namespace-wide scalable-topic auto-scale policy.
    pub async fn remove_scalable_topic_auto_scale_policy(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["scalableTopicAutoScalePolicy"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Sets the namespace's tiered-storage offload configuration.
    pub async fn set_offload_policies(
        &self,
        namespace: &str,
        policies: &OffloadPolicies,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["offloadPolicies"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(policies))
            .await
    }

    /// Removes the namespace's offload configuration.
    ///
    /// The verb is DELETE even though the path segment reads like a command; POST
    /// matches no route on this path and the broker answers 405.
    pub async fn remove_offload_policies(&self, namespace: &str) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["removeOffloadPolicies"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    // ------------------------------------------------- remaining Java parity

    /// Deletes one namespace bundle.
    ///
    /// `bundle` is a range such as `0x00000000_0x80000000`.
    pub async fn delete_namespace_bundle(
        &self,
        namespace: &str,
        bundle: &str,
        force: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &[&encode_segment(bundle)])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Topic name -> its position on the bundle's hash ring.
    ///
    /// `topics` narrows the answer; an empty slice asks about every topic in the
    /// bundle.
    pub async fn get_topic_hash_positions(
        &self,
        namespace: &str,
        bundle: &str,
        topics: &[String],
    ) -> Result<TopicHashPositions, Error> {
        let url = self.ns_url(namespace, &[&encode_segment(bundle), "topicHashPositions"])?;
        let query: Vec<(&str, String)> = topics.iter().map(|t| ("topics", t.clone())).collect();
        self.client
            .send_json(Method::GET, &url, &query, NO_BODY)
            .await
    }

    // `getReplicationConfigVersion` is deliberately absent. Java's client still
    // offers it, but `configversion` appears nowhere in the broker's admin
    // resources: the path falls through to the DELETE-only delete-bundle route, so
    // a live broker answers 405 with a Jetty error page. Implementing it would add
    // a method that can only ever fail.

    /// Marks the namespace as migrated, or not.
    pub async fn update_migration_state(
        &self,
        namespace: &str,
        migrated: bool,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["migration"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(&migrated))
            .await
    }

    /// The roles permitted on each subscription, keyed by subscription.
    pub async fn get_permission_on_subscription(
        &self,
        namespace: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, Error> {
        let url = self.ns_url(namespace, &["permissions", "subscription"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Reads one namespace property.
    pub async fn get_namespace_property(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<String>, Error> {
        let url = self.ns_url(namespace, &["property", &encode_segment(key)])?;
        let value = self.client.send_text(Method::GET, &url, &[]).await?;
        Ok(if value.is_empty() { None } else { Some(value) })
    }

    /// Lists the namespaces in an anti-affinity group.
    ///
    /// Addressed by **cluster**, not by namespace, so it does not use `ns_url`.
    pub async fn get_anti_affinity_namespaces(
        &self,
        tenant: &str,
        cluster: &str,
        group: &str,
    ) -> Result<Vec<String>, Error> {
        let url = self.client.url(&[
            "namespaces",
            &encode_segment(cluster),
            "antiAffinity",
            &encode_segment(group),
        ]);
        self.client
            .send_json(
                Method::GET,
                &url,
                &[("tenant", tenant.to_string())],
                NO_BODY,
            )
            .await
    }

    // --------------------------------------------------------- allowed clusters

    /// Gets the clusters this namespace may be assigned to.
    ///
    /// Distinct from
    /// [`get_namespace_replication_clusters`][Self::get_namespace_replication_clusters]:
    /// this is the permitted set, that one is where data actually replicates.
    pub async fn get_namespace_allowed_clusters(
        &self,
        namespace: &str,
    ) -> Result<Vec<String>, Error> {
        let url = self.ns_url(namespace, &["allowedClusters"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Sets the clusters this namespace may be assigned to.
    pub async fn set_namespace_allowed_clusters(
        &self,
        namespace: &str,
        clusters: &[String],
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["allowedClusters"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(clusters))
            .await
    }

    // ------------------------------------------- metric topic-property keys

    /// Topic property keys the broker may attach to metrics as labels.
    pub async fn get_allowed_topic_property_keys_for_metrics(
        &self,
        namespace: &str,
    ) -> Result<Vec<String>, Error> {
        let url = self.ns_url(namespace, &["allowedTopicPropertyKeysForMetrics"])?;
        Ok(self
            .client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await?
            .unwrap_or_default())
    }

    /// Sets the topic property keys the broker may use as metric labels.
    pub async fn set_allowed_topic_property_keys_for_metrics(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["allowedTopicPropertyKeysForMetrics"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(keys))
            .await
    }

    /// Removes the metric topic-property key allow-list.
    pub async fn remove_allowed_topic_property_keys_for_metrics(
        &self,
        namespace: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["allowedTopicPropertyKeysForMetrics"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    // ---------------------------------------------- bulk topic permissions

    /// Grants topic permissions in bulk.
    ///
    /// Cluster-scoped rather than namespace-scoped: each entry names its own topic,
    /// so this posts to `/admin/v2/namespaces/grantPermissionsOnTopics`.
    pub async fn grant_permission_on_topics(
        &self,
        options: &[GrantTopicPermissionOptions],
    ) -> Result<(), Error> {
        let url = self.client.url(&["namespaces", "grantPermissionsOnTopics"]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(options))
            .await
    }

    /// Revokes topic permissions in bulk.
    pub async fn revoke_permission_on_topics(
        &self,
        options: &[RevokeTopicPermissionOptions],
    ) -> Result<(), Error> {
        let url = self
            .client
            .url(&["namespaces", "revokePermissionsOnTopics"]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(options))
            .await
    }

    // ------------------------------------------------------------- deprecated

    /// Gets the namespace's schema auto-update compatibility strategy.
    ///
    /// Deprecated in Java in favour of
    /// [`get_schema_compatibility_strategy`][Self::get_schema_compatibility_strategy];
    /// kept for parity with older brokers.
    #[deprecated(note = "use get_schema_compatibility_strategy instead, as Java does")]
    pub async fn get_schema_auto_update_compatibility_strategy(
        &self,
        namespace: &str,
    ) -> Result<Option<String>, Error> {
        let url = self.ns_url(namespace, &["schemaAutoUpdateCompatibilityStrategy"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets the namespace's schema auto-update compatibility strategy.
    ///
    /// The verb is PUT, not POST, unlike most policy setters.
    #[deprecated(note = "use set_schema_compatibility_strategy instead, as Java does")]
    pub async fn set_schema_auto_update_compatibility_strategy(
        &self,
        namespace: &str,
        strategy: &str,
    ) -> Result<(), Error> {
        let url = self.ns_url(namespace, &["schemaAutoUpdateCompatibilityStrategy"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(strategy))
            .await
    }
}
