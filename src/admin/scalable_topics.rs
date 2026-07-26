//! Scalable topic administration — `/admin/v2/scalable` and `/admin/v2/segments`.
//!
//! Mirrors `org.apache.pulsar.client.admin.ScalableTopics` (PIP-460). A scalable
//! topic lives in the `topic://` domain and is a DAG of hash-range segments the
//! broker splits and merges, so its partition count is invisible and dynamic.
//!
//! Requires a broker that advertises `supports_scalable_topics`; check
//! [`Pulsar::broker_features`][crate::Pulsar::broker_features] first.
//!
//! This is the admin surface only. Producing to and consuming from `topic://`
//! needs the scalable-topic client protocol, which is not implemented yet.

use std::collections::BTreeMap;

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{
            AutoScalePolicyOverride, ScalableSubscriptionType, ScalableTopicMetadata,
            ScalableTopicStats,
        },
        parse_topic, split_namespace, AdminClient,
    },
    Error,
};

/// Handle for the `scalable_topics` group of admin operations.
///
/// Obtained from [`AdminClient::scalable_topics`].
pub struct ScalableTopics<'a> {
    pub(crate) client: &'a AdminClient,
}

impl ScalableTopics<'_> {
    /// Builds `/admin/v2/scalable/{tenant}/{namespace}/{topic}/...`.
    ///
    /// Scalable topic paths carry no `persistent`/`non-persistent` domain; a
    /// `topic://` prefix on the input is accepted and stripped.
    fn scalable_url(&self, topic: &str, extra: &[&str]) -> Result<String, Error> {
        let topic = topic.strip_prefix("topic://").unwrap_or(topic);
        let (_, tenant, namespace, name) = parse_topic(topic)?;
        let (tenant, namespace, name) = (
            encode_segment(tenant),
            encode_segment(namespace),
            encode_segment(name),
        );
        let mut all: Vec<&str> = vec!["scalable", &tenant, &namespace, &name];
        all.extend_from_slice(extra);
        Ok(self.client.url(&all))
    }

    /// Builds a `/segments/{tenant}/{namespace}/{topic}/{descriptor}` URL.
    ///
    /// A segment topic is `segment://tenant/ns/topic/<start>-<end>-<id>`, and the
    /// broker routes on the parent topic and the descriptor as **separate** path
    /// segments. Percent-encoding the whole `topic/descriptor` tail into one
    /// segment matches no route, so the request falls through to Jetty's HTML 404.
    fn segment_url(&self, segment_topic: &str, extra: &[&str]) -> Result<String, Error> {
        let topic = segment_topic
            .strip_prefix("segment://")
            .unwrap_or(segment_topic);
        let (_, tenant, namespace, name) = parse_topic(topic)?;
        let (parent, descriptor) = name.rsplit_once('/').ok_or_else(|| {
            Error::Admin(crate::error::AdminError::InvalidTopic(format!(
                "expected a segment topic like segment://tenant/ns/topic/0000-ffff-0, got: \
                 {segment_topic}"
            )))
        })?;
        let (tenant, namespace, parent, descriptor) = (
            encode_segment(tenant),
            encode_segment(namespace),
            encode_segment(parent),
            encode_segment(descriptor),
        );
        let mut all: Vec<&str> = vec!["segments", &tenant, &namespace, &parent, &descriptor];
        all.extend_from_slice(extra);
        Ok(self.client.url(&all))
    }

    /// Lists the scalable topics in a namespace, as `topic://…` names.
    pub async fn list_scalable_topics(&self, namespace: &str) -> Result<Vec<String>, Error> {
        let (tenant, ns) = split_namespace(namespace)?;
        let url = self
            .client
            .url(&["scalable", &encode_segment(tenant), &encode_segment(ns)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the scalable topics whose properties contain every given pair.
    pub async fn list_scalable_topics_by_properties(
        &self,
        namespace: &str,
        filters: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, Error> {
        let (tenant, ns) = split_namespace(namespace)?;
        let url = self
            .client
            .url(&["scalable", &encode_segment(tenant), &encode_segment(ns)]);
        // Repeated `property=key=value` pairs, matching the Java client.
        let query: Vec<(&str, String)> = filters
            .iter()
            .map(|(k, v)| ("property", format!("{k}={v}")))
            .collect();
        self.client
            .send_json(Method::GET, &url, &query, NO_BODY)
            .await
    }

    /// Creates a scalable topic with `num_initial_segments` segments.
    pub async fn create_scalable_topic(
        &self,
        topic: &str,
        num_initial_segments: i32,
    ) -> Result<(), Error> {
        let url = self.scalable_url(topic, &[])?;
        self.client
            .send_empty(
                Method::PUT,
                &url,
                &[("numInitialSegments", num_initial_segments.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Creates a scalable topic carrying `properties`.
    pub async fn create_scalable_topic_with_properties(
        &self,
        topic: &str,
        num_initial_segments: i32,
        properties: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        let url = self.scalable_url(topic, &[])?;
        self.client
            .send_empty(
                Method::PUT,
                &url,
                &[("numInitialSegments", num_initial_segments.to_string())],
                Some(properties),
            )
            .await
    }

    /// Converts an existing regular topic into a scalable one.
    pub async fn migrate_to_scalable(&self, topic: &str, force: bool) -> Result<(), Error> {
        let url = self.scalable_url(topic, &["migrate"])?;
        self.client
            .send_empty(Method::POST, &url, &[("force", force.to_string())], NO_BODY)
            .await
    }

    /// Deletes a scalable topic.
    pub async fn delete_scalable_topic(&self, topic: &str, force: bool) -> Result<(), Error> {
        let url = self.scalable_url(topic, &[])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets the topic's segment DAG.
    pub async fn get_metadata(&self, topic: &str) -> Result<ScalableTopicMetadata, Error> {
        let url = self.scalable_url(topic, &[])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets segment and subscription statistics.
    pub async fn get_stats(&self, topic: &str) -> Result<ScalableTopicStats, Error> {
        let url = self.scalable_url(topic, &["stats"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the topic's auto split/merge policy, or `None` if unset.
    pub async fn get_auto_scale_policy(
        &self,
        topic: &str,
    ) -> Result<Option<AutoScalePolicyOverride>, Error> {
        let url = self.scalable_url(topic, &["autoScalePolicy"])?;
        self.client
            .send_json_opt(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets the topic's auto split/merge policy.
    pub async fn set_auto_scale_policy(
        &self,
        topic: &str,
        policy: &AutoScalePolicyOverride,
    ) -> Result<(), Error> {
        let url = self.scalable_url(topic, &["autoScalePolicy"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(policy))
            .await
    }

    /// Removes the topic's auto split/merge policy override.
    pub async fn remove_auto_scale_policy(&self, topic: &str) -> Result<(), Error> {
        let url = self.scalable_url(topic, &["autoScalePolicy"])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Creates a subscription on a scalable topic.
    pub async fn create_subscription(
        &self,
        topic: &str,
        subscription: &str,
        subscription_type: ScalableSubscriptionType,
    ) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.scalable_url(topic, &["subscriptions", &encoded])?;
        self.client
            .send_empty(
                Method::PUT,
                &url,
                &[("type", subscription_type.as_str().to_string())],
                NO_BODY,
            )
            .await
    }

    /// Deletes a subscription from a scalable topic.
    pub async fn delete_subscription(&self, topic: &str, subscription: &str) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.scalable_url(topic, &["subscriptions", &encoded])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Moves a subscription to the given publish time, in milliseconds.
    pub async fn seek_subscription(
        &self,
        topic: &str,
        subscription: &str,
        timestamp_ms: i64,
    ) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.scalable_url(topic, &["subscriptions", &encoded, "seek"])?;
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("timestamp", timestamp_ms.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Acknowledges a subscription's whole backlog.
    pub async fn clear_backlog(&self, topic: &str, subscription: &str) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.scalable_url(topic, &["subscriptions", &encoded, "skip-all"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Splits one segment into two children, halving its hash range.
    pub async fn split_segment(&self, topic: &str, segment_id: i64) -> Result<(), Error> {
        let id = segment_id.to_string();
        let url = self.scalable_url(topic, &["split", &id])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Merges two adjacent segments into one child.
    pub async fn merge_segments(
        &self,
        topic: &str,
        segment_id_1: i64,
        segment_id_2: i64,
    ) -> Result<(), Error> {
        let (a, b) = (segment_id_1.to_string(), segment_id_2.to_string());
        let url = self.scalable_url(topic, &["merge", &a, &b])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    // ------------------------------------------------------- segments

    /// Seals one segment against further writes.
    pub async fn terminate_segment(&self, segment_topic: &str) -> Result<(), Error> {
        let url = self.segment_url(segment_topic, &["terminate"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Deletes one segment.
    pub async fn delete_segment(&self, segment_topic: &str, force: bool) -> Result<(), Error> {
        let url = self.segment_url(segment_topic, &[])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Creates a segment explicitly, with the given subscriptions.
    pub async fn create_segment(
        &self,
        segment_topic: &str,
        subscriptions: &[String],
    ) -> Result<(), Error> {
        let url = self.segment_url(segment_topic, &[])?;
        self.client
            .send_empty(Method::PUT, &url, &[], Some(subscriptions))
            .await
    }

    /// Creates a subscription on one segment.
    pub async fn create_segment_subscription(
        &self,
        segment_topic: &str,
        subscription: &str,
    ) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.segment_url(segment_topic, &["subscription", &encoded])?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Deletes a subscription from one segment.
    pub async fn delete_segment_subscription(
        &self,
        segment_topic: &str,
        subscription: &str,
    ) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.segment_url(segment_topic, &["subscription", &encoded])?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Seeks a segment subscription to a timestamp, in epoch milliseconds.
    pub async fn seek_segment_subscription(
        &self,
        segment_topic: &str,
        subscription: &str,
        timestamp_ms: i64,
    ) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.segment_url(segment_topic, &["subscription", &encoded, "seek"])?;
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("timestamp", timestamp_ms.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Gets a segment subscription's backlog.
    pub async fn get_segment_subscription_backlog(
        &self,
        segment_topic: &str,
        subscription: &str,
    ) -> Result<i64, Error> {
        let encoded = encode_segment(subscription);
        let url = self.segment_url(segment_topic, &["subscription", &encoded, "backlog"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Clears a segment subscription's backlog.
    pub async fn clear_segment_subscription_backlog(
        &self,
        segment_topic: &str,
        subscription: &str,
    ) -> Result<(), Error> {
        let encoded = encode_segment(subscription);
        let url = self.segment_url(segment_topic, &["subscription", &encoded, "skip-all"])?;
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }
}
