//! Broker statistics dumps — `/admin/v2/broker-stats`.
//!
//! Mirrors `org.apache.pulsar.client.admin.BrokerStats`. Most of these return
//! large diagnostic documents whose shape varies by broker version and enabled
//! features, so they stay raw JSON text — the Java client does the same. The load
//! report is the exception: Java types it as `LoadManagerReport`, so it is modelled
//! here too.

use reqwest::Method;

use crate::{
    admin::{encode_segment, models::LoadManagerReport, AdminClient},
    Error,
};

/// Handle for the `broker_stats` group of admin operations.
///
/// Obtained from [`AdminClient::broker_stats`].
pub struct BrokerStats<'a> {
    pub(crate) client: &'a AdminClient,
}

impl BrokerStats<'_> {
    fn stats_url(&self, segments: &[&str]) -> String {
        let mut all = vec!["broker-stats"];
        all.extend_from_slice(segments);
        self.client.url(&all)
    }

    /// Prometheus-style broker metrics, as a JSON document.
    pub async fn get_metrics(&self) -> Result<String, Error> {
        self.client
            .send_text(Method::GET, &self.stats_url(&["metrics"]), &[])
            .await
    }

    /// Per-topic statistics for every topic this broker owns.
    pub async fn get_topics(&self) -> Result<String, Error> {
        self.client
            .send_text(Method::GET, &self.stats_url(&["topics"]), &[])
            .await
    }

    /// JVM MBean dump.
    pub async fn get_mbeans(&self) -> Result<String, Error> {
        self.client
            .send_text(Method::GET, &self.stats_url(&["mbeans"]), &[])
            .await
    }

    /// Pending BookKeeper operation counts, per topic.
    pub async fn get_pending_bookie_ops_stats(&self) -> Result<String, Error> {
        self.client
            .send_text(Method::GET, &self.stats_url(&["bookieops"]), &[])
            .await
    }

    /// Netty allocator statistics for `allocator_name`.
    pub async fn get_allocator_stats(&self, allocator_name: &str) -> Result<String, Error> {
        let encoded = encode_segment(allocator_name);
        let url = self.stats_url(&["allocator-stats", &encoded]);
        self.client.send_text(Method::GET, &url, &[]).await
    }

    /// The broker's load report.
    ///
    /// Returns `None` when the broker has not produced one yet — a standalone
    /// broker answers 204 with no body.
    pub async fn get_load_report(&self) -> Result<Option<LoadManagerReport>, Error> {
        self.client
            .send_json(
                Method::GET,
                &self.stats_url(&["load-report"]),
                &[],
                crate::admin::clusters::NO_BODY,
            )
            .await
    }
}
