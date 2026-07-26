//! Broker administration — `/admin/v2/brokers`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Brokers`.

use std::collections::BTreeMap;

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::{
    admin::{clusters::NO_BODY, encode_segment, models::BrokerInfo, AdminClient},
    Error,
};

/// Metadata-store and state-storage coordinates shared by the cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalConfigurationData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_store_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration_metadata_store_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledgers_root_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookkeeper_metadata_service_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_storage_service_url: Option<String>,
}

/// How completely a broker owns a namespace bundle.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceOwnershipStatus {
    #[serde(default)]
    pub broker_assignment: Option<String>,
    #[serde(default)]
    pub is_controlled: bool,
    #[serde(default)]
    pub is_active: bool,
}

/// Handle for the `brokers` group of admin operations.
///
/// Obtained from [`AdminClient::brokers`]. Grouping mirrors the Java admin
/// client's separate interfaces and keeps same-named operations on different
/// resource kinds (a namespace retention policy vs a topic one) distinct.
pub struct Brokers<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Brokers<'_> {
    fn brokers_url(&self, segments: &[&str]) -> String {
        let mut all = vec!["brokers"];
        all.extend_from_slice(segments);
        self.client.url(&all)
    }

    /// Lists brokers currently active in the cluster the client is connected to.
    pub async fn get_active_brokers(&self) -> Result<Vec<String>, Error> {
        self.client
            .send_json(Method::GET, &self.brokers_url(&[]), &[], NO_BODY)
            .await
    }

    /// Lists brokers currently active in a named cluster.
    pub async fn get_active_brokers_in_cluster(&self, cluster: &str) -> Result<Vec<String>, Error> {
        let url = self.brokers_url(&[&encode_segment(cluster)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the broker currently elected leader.
    pub async fn get_leader_broker(&self) -> Result<BrokerInfo, Error> {
        let url = self.brokers_url(&["leaderBroker"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the namespaces a broker owns, keyed by namespace bundle.
    pub async fn get_owned_namespaces(
        &self,
        cluster: &str,
        broker_id: &str,
    ) -> Result<BTreeMap<String, NamespaceOwnershipStatus>, Error> {
        let url = self.brokers_url(&[
            &encode_segment(cluster),
            &encode_segment(broker_id),
            "ownedNamespaces",
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the configuration keys that can be changed at runtime.
    pub async fn get_dynamic_configuration_names(&self) -> Result<Vec<String>, Error> {
        let url = self.brokers_url(&["configuration"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the dynamic configuration values that have been overridden.
    pub async fn get_all_dynamic_configurations(&self) -> Result<BTreeMap<String, String>, Error> {
        let url = self.brokers_url(&["configuration", "values"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the broker's full effective runtime configuration.
    pub async fn get_runtime_configurations(&self) -> Result<BTreeMap<String, String>, Error> {
        let url = self.brokers_url(&["configuration", "runtime"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Overrides one dynamic configuration value across the cluster.
    ///
    /// Both name and value travel as path segments, matching the Java client.
    pub async fn update_dynamic_configuration(&self, name: &str, value: &str) -> Result<(), Error> {
        let url = self.brokers_url(&[
            "configuration",
            &encode_segment(name),
            &encode_segment(value),
        ]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Removes a dynamic configuration override, restoring the static value.
    pub async fn delete_dynamic_configuration(&self, name: &str) -> Result<(), Error> {
        let url = self.brokers_url(&["configuration", &encode_segment(name)]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Gets the cluster's metadata-store coordinates.
    pub async fn get_internal_configuration_data(
        &self,
    ) -> Result<InternalConfigurationData, Error> {
        let url = self.brokers_url(&["internal-configuration"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the broker's version string.
    pub async fn get_broker_version(&self) -> Result<String, Error> {
        // This endpoint answers with a bare version string, not JSON, so it cannot
        // go through `send_json`.
        let url = self.brokers_url(&["version"]);
        self.client.send_text(Method::GET, &url, &[]).await
    }

    /// Runs the broker's health check. `Ok(())` means healthy.
    ///
    /// A 2xx is not on its own a pass: the broker answers 200 with a body saying
    /// what went wrong, so Java checks that the trimmed body is exactly `ok`.
    pub async fn healthcheck(&self) -> Result<(), Error> {
        let url = self.brokers_url(&["health"]);
        Self::require_ok(self.client.send_text(Method::GET, &url, &[]).await?)
    }

    /// Runs the health check against a specific broker.
    pub async fn healthcheck_broker(&self, broker_id: &str) -> Result<(), Error> {
        let url = self.brokers_url(&["health"]);
        Self::require_ok(
            self.client
                .send_text(Method::GET, &url, &[("brokerId", broker_id.to_string())])
                .await?,
        )
    }

    fn require_ok(body: String) -> Result<(), Error> {
        if body.trim().eq_ignore_ascii_case("ok") {
            return Ok(());
        }
        Err(Error::Admin(crate::error::AdminError::ServerUnavailable(
            format!("broker health check reported: {}", body.trim()),
        )))
    }

    /// Shuts this broker down gracefully, unloading its topics first.
    ///
    /// `max_concurrent_unload_per_sec` throttles the unload rate (0 means no
    /// limit); `forced_terminate_topic` terminates topics rather than waiting for
    /// them to drain. The broker stops serving, so this is deliberately the only
    /// destructive operation in this group.
    pub async fn shutdown_broker_gracefully(
        &self,
        max_concurrent_unload_per_sec: i32,
        forced_terminate_topic: bool,
    ) -> Result<(), Error> {
        let url = self.brokers_url(&["shutdown"]);
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[
                    (
                        "maxConcurrentUnloadPerSec",
                        max_concurrent_unload_per_sec.to_string(),
                    ),
                    ("forcedTerminateTopic", forced_terminate_topic.to_string()),
                ],
                Some(""),
            )
            .await
    }

    /// Asks the broker to re-evaluate backlog quotas immediately.
    pub async fn backlog_quota_check(&self) -> Result<(), Error> {
        let url = self.brokers_url(&["backlog-quota-check"]);
        self.client
            .send_empty(Method::GET, &url, &[], NO_BODY)
            .await
    }
}
