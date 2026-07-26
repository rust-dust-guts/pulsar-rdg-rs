//! Topic lookup over HTTP — `/lookup/v2`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Lookup`. Producers and consumers use
//! the binary protocol for lookup; this is the administrative view, useful for
//! answering "which broker owns this topic?" without opening a connection.

use reqwest::Method;

use crate::{
    admin::{encode_segment, models::TopicLookupResult, parse_topic, AdminClient},
    Error,
};

/// Handle for the `lookup` group of admin operations.
///
/// Obtained from [`AdminClient::lookup`].
pub struct Lookup<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Lookup<'_> {
    /// Builds `/lookup/v2/topic/{domain}/{tenant}/{namespace}/{topic}/...`.
    ///
    /// This is the one group not rooted at `/admin/v2`.
    fn lookup_url(&self, topic: &str, extra: &[&str]) -> Result<String, Error> {
        let (scheme, tenant, namespace, name) = parse_topic(topic)?;
        let (tenant, namespace, name) = (
            encode_segment(tenant),
            encode_segment(namespace),
            encode_segment(name),
        );
        let mut url = format!(
            "{}/lookup/v2/topic/{scheme}/{tenant}/{namespace}/{name}",
            self.client.admin_url()
        );
        for segment in extra {
            url.push('/');
            url.push_str(segment);
        }
        Ok(url)
    }

    /// Finds the broker currently serving a topic.
    pub async fn lookup_topic(&self, topic: &str) -> Result<TopicLookupResult, Error> {
        let url = self.lookup_url(topic, &[])?;
        self.client
            .send_json(Method::GET, &url, &[], None::<&()>)
            .await
    }

    /// Gets the namespace bundle a topic hashes into, e.g.
    /// `0xc0000000_0xffffffff`.
    pub async fn get_bundle_range(&self, topic: &str) -> Result<String, Error> {
        let url = self.lookup_url(topic, &["bundle"])?;
        self.client.send_text(Method::GET, &url, &[]).await
    }

    /// Looks up the owning broker of every partition of a partitioned topic.
    ///
    /// Keyed by partition topic name. There is no single broker for a partitioned
    /// topic, so — as in Java — this reads the partition count and then looks each
    /// partition up individually. A topic with no partitions is an error, not an
    /// empty map, because a non-partitioned topic should use
    /// [`lookup_topic`][Self::lookup_topic].
    pub async fn lookup_partitioned_topic(
        &self,
        topic: &str,
    ) -> Result<std::collections::BTreeMap<String, TopicLookupResult>, Error> {
        let metadata = crate::admin::topics::Topics {
            client: self.client,
        }
        .get_partitioned_topic_metadata(topic)
        .await?;
        if metadata.partitions <= 0 {
            return Err(Error::Admin(crate::error::AdminError::BadRequest(format!(
                "{topic} is not a partitioned topic"
            ))));
        }

        let mut out = std::collections::BTreeMap::new();
        for partition in 0..metadata.partitions {
            let name = format!("{topic}-partition-{partition}");
            out.insert(name.clone(), self.lookup_topic(&name).await?);
        }
        Ok(out)
    }
}
