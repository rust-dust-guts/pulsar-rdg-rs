//! Pulsar proxy statistics — `/proxy-stats`.
//!
//! Mirrors `org.apache.pulsar.client.admin.ProxyStats`. These endpoints are served
//! by a **Pulsar proxy**, not a broker, so `admin_url` must point at a proxy;
//! against a broker they answer 404.
//!
//! Java's `ProxyStats` returns the two stats documents as raw `String`s even though
//! the proxy serves `List<ConnectionStats>` and `Map<String, TopicStats>`; these
//! are deserialized into those shapes instead.

use std::collections::HashMap;

use reqwest::Method;

use crate::{
    admin::{
        models::{ProxyConnectionStats, ProxyTopicStats},
        AdminClient,
    },
    Error,
};

/// Handle for the `proxy_stats` group of admin operations.
///
/// Obtained from [`AdminClient::proxy_stats`].
pub struct ProxyStats<'a> {
    pub(crate) client: &'a AdminClient,
}

impl ProxyStats<'_> {
    fn proxy_url(&self, segment: &str) -> String {
        format!("{}/proxy-stats/{segment}", self.client.admin_url())
    }

    /// Per-connection statistics for the proxy, one entry per live client
    /// connection. Empty when nothing is connected.
    pub async fn get_connections(&self) -> Result<Vec<ProxyConnectionStats>, Error> {
        self.client
            .send_json(
                Method::GET,
                &self.proxy_url("connections"),
                &[],
                crate::admin::clusters::NO_BODY,
            )
            .await
    }

    /// Per-topic statistics as seen by the proxy.
    ///
    /// Requires the proxy to have been **started** with `proxyLogLevel=2`; it reads
    /// the configured level, not the running one, so
    /// [`set_log_level`][Self::set_log_level] cannot enable it. Otherwise the proxy
    /// answers 412 "Proxy doesn't have logging level 2".
    pub async fn get_topics(&self) -> Result<HashMap<String, ProxyTopicStats>, Error> {
        self.client
            .send_json(
                Method::GET,
                &self.proxy_url("topics"),
                &[],
                crate::admin::clusters::NO_BODY,
            )
            .await
    }

    /// Gets the proxy's current log level.
    pub async fn get_log_level(&self) -> Result<i32, Error> {
        let url = self.proxy_url("logging");
        self.client
            .send_json(Method::GET, &url, &[], crate::admin::clusters::NO_BODY)
            .await
    }

    /// Sets the proxy's log level.
    ///
    /// `0` disables per-connection logging, `1` logs frame-level events and `2`
    /// additionally logs message payloads.
    ///
    /// In-memory only: the proxy does not persist it, and
    /// [`get_topics`][Self::get_topics] checks the *configured* level rather than
    /// this one, so raising the level here does not unlock topic stats.
    pub async fn set_log_level(&self, level: i32) -> Result<(), Error> {
        let url = self.proxy_url(&format!("logging/{level}"));
        self.client
            .send_empty(Method::POST, &url, &[], crate::admin::clusters::NO_BODY)
            .await
    }
}
