//! Non-persistent topic administration — `/admin/v2/non-persistent`.
//!
//! Mirrors `org.apache.pulsar.client.admin.NonPersistentTopics`. Non-persistent
//! topics keep nothing on disk, so they have their own stats shape and no
//! backlog, cursor or offload operations.
//!
//! Listing is also available through
//! [`Topics::get_non_persistent_list`][crate::admin::topics::Topics::get_non_persistent_list].

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{NonPersistentTopicStats, PartitionedTopicMetadata, PersistentTopicInternalStats},
        parse_topic, split_namespace, AdminClient,
    },
    Error,
};

/// Handle for the `non_persistent_topics` group of admin operations.
///
/// Obtained from [`AdminClient::non_persistent_topics`].
pub struct NonPersistentTopics<'a> {
    pub(crate) client: &'a AdminClient,
}

impl NonPersistentTopics<'_> {
    fn topic_url(&self, topic: &str, extra: &[&str]) -> Result<String, Error> {
        let (scheme, tenant, namespace, name) = parse_topic(topic)?;
        // A bare `tenant/ns/topic` means the non-persistent domain here, but an
        // explicit `persistent://` is a mistake worth reporting: silently rewriting
        // it would operate on a different topic than the caller named.
        if scheme == "persistent" && topic.starts_with("persistent://") {
            return Err(Error::Admin(crate::error::AdminError::InvalidTopic(
                format!("{topic} is a persistent topic; use `topics()` for those"),
            )));
        }
        let (tenant, namespace, name) = (
            encode_segment(tenant),
            encode_segment(namespace),
            encode_segment(name),
        );
        let mut all: Vec<&str> = vec!["non-persistent", &tenant, &namespace, &name];
        all.extend_from_slice(extra);
        Ok(self.client.url(&all))
    }

    /// Lists the non-persistent topics in a namespace.
    pub async fn get_list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        let (tenant, ns) = split_namespace(namespace)?;
        let url = self.client.url(&[
            "non-persistent",
            &encode_segment(tenant),
            &encode_segment(ns),
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the non-persistent topics owned by one namespace bundle.
    pub async fn get_list_in_bundle(
        &self,
        namespace: &str,
        bundle: &str,
    ) -> Result<Vec<String>, Error> {
        let (tenant, ns) = split_namespace(namespace)?;
        let url = self.client.url(&[
            "non-persistent",
            &encode_segment(tenant),
            &encode_segment(ns),
            &encode_segment(bundle),
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a partitioned non-persistent topic.
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

    /// Gets a non-persistent topic's partition count.
    pub async fn get_partitioned_topic_metadata(
        &self,
        topic: &str,
    ) -> Result<PartitionedTopicMetadata, Error> {
        let url = self.topic_url(topic, &["partitions"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets runtime statistics for a non-persistent topic.
    pub async fn get_stats(&self, topic: &str) -> Result<NonPersistentTopicStats, Error> {
        let url = self.topic_url(topic, &["stats"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets internal statistics for a non-persistent topic.
    pub async fn get_internal_stats(
        &self,
        topic: &str,
    ) -> Result<PersistentTopicInternalStats, Error> {
        let url = self.topic_url(topic, &["internalStats"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Releases the topic so the load manager can reassign it.
    pub async fn unload(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_url(topic, &["unload"])?;
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }
}
