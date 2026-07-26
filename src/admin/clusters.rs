//! Cluster administration — `/admin/v2/clusters`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Clusters`.

use reqwest::Method;

use crate::{
    admin::{
        encode_segment,
        models::{
            BrokerNamespaceIsolationData, ClusterData, ClusterPolicies, ClusterUrl, FailureDomain,
            NamespaceIsolationData,
        },
        AdminClient,
    },
    Error,
};

use std::collections::BTreeMap;

/// Handle for the `clusters` group of admin operations.
///
/// Obtained from [`AdminClient::clusters`]. Grouping mirrors the Java admin
/// client's separate interfaces and keeps same-named operations on different
/// resource kinds (a namespace retention policy vs a topic one) distinct.
pub struct Clusters<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Clusters<'_> {
    fn clusters_url(&self, segments: &[&str]) -> String {
        let mut all = vec!["clusters"];
        all.extend_from_slice(segments);
        self.client.url(&all)
    }

    /// Lists the names of all known clusters.
    pub async fn get_clusters(&self) -> Result<Vec<String>, Error> {
        self.client
            .send_json(Method::GET, &self.clusters_url(&[]), &[], NO_BODY)
            .await
    }

    /// Gets a cluster's connection details.
    pub async fn get_cluster(&self, cluster: &str) -> Result<ClusterData, Error> {
        let url = self.clusters_url(&[&encode_segment(cluster)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a cluster. Fails with
    /// [`AdminError::Conflict`][crate::error::AdminError::Conflict] if it exists.
    pub async fn create_cluster(&self, cluster: &str, data: &ClusterData) -> Result<(), Error> {
        let url = self.clusters_url(&[&encode_segment(cluster)]);
        self.client
            .send_empty(Method::PUT, &url, &[], Some(data))
            .await
    }

    /// Replaces a cluster's connection details.
    pub async fn update_cluster(&self, cluster: &str, data: &ClusterData) -> Result<(), Error> {
        let url = self.clusters_url(&[&encode_segment(cluster)]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(data))
            .await
    }

    /// Deletes a cluster.
    pub async fn delete_cluster(&self, cluster: &str) -> Result<(), Error> {
        let url = self.clusters_url(&[&encode_segment(cluster)]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Gets the clusters that may serve as peers for `cluster`.
    pub async fn get_peer_cluster_names(&self, cluster: &str) -> Result<Vec<String>, Error> {
        let url = self.clusters_url(&[&encode_segment(cluster), "peers"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Replaces the peer cluster list. Order is preserved.
    pub async fn update_peer_cluster_names(
        &self,
        cluster: &str,
        peers: &[String],
    ) -> Result<(), Error> {
        let url = self.clusters_url(&[&encode_segment(cluster), "peers"]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(peers))
            .await
    }

    /// Gets the cluster's migration state, or `None` if none is configured.
    ///
    /// The broker answers 404 "Cluster does not exist" until a migration has been
    /// set, even for a cluster that does exist, so absence is reported as `None`
    /// rather than as an error.
    pub async fn get_cluster_migration(
        &self,
        cluster: &str,
    ) -> Result<Option<ClusterPolicies>, Error> {
        let url = self.clusters_url(&[&encode_segment(cluster), "migrate"]);
        self.client
            .send_json_absent_on_404(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Marks the cluster as migrated (or not) and records where it moved to.
    pub async fn update_cluster_migration(
        &self,
        cluster: &str,
        migrated: bool,
        url_data: &ClusterUrl,
    ) -> Result<(), Error> {
        let url = self.clusters_url(&[&encode_segment(cluster), "migrate"]);
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("migrated", migrated.to_string())],
                Some(url_data),
            )
            .await
    }

    /// Gets every namespace isolation policy defined on the cluster, by name.
    pub async fn get_namespace_isolation_policies(
        &self,
        cluster: &str,
    ) -> Result<BTreeMap<String, NamespaceIsolationData>, Error> {
        let url = self.clusters_url(&[&encode_segment(cluster), "namespaceIsolationPolicies"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets one namespace isolation policy.
    pub async fn get_namespace_isolation_policy(
        &self,
        cluster: &str,
        policy: &str,
    ) -> Result<NamespaceIsolationData, Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "namespaceIsolationPolicies",
            &encode_segment(policy),
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates or replaces a namespace isolation policy.
    pub async fn set_namespace_isolation_policy(
        &self,
        cluster: &str,
        policy: &str,
        data: &NamespaceIsolationData,
    ) -> Result<(), Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "namespaceIsolationPolicies",
            &encode_segment(policy),
        ]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(data))
            .await
    }

    /// Deletes a namespace isolation policy.
    pub async fn delete_namespace_isolation_policy(
        &self,
        cluster: &str,
        policy: &str,
    ) -> Result<(), Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "namespaceIsolationPolicies",
            &encode_segment(policy),
        ]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Lists brokers together with the isolation policies that select them.
    pub async fn get_brokers_with_namespace_isolation_policy(
        &self,
        cluster: &str,
    ) -> Result<Vec<BrokerNamespaceIsolationData>, Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "namespaceIsolationPolicies",
            "brokers",
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the isolation-policy assignment for one broker.
    pub async fn get_broker_with_namespace_isolation_policy(
        &self,
        cluster: &str,
        broker: &str,
    ) -> Result<BrokerNamespaceIsolationData, Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "namespaceIsolationPolicies",
            "brokers",
            &encode_segment(broker),
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets every failure domain on the cluster, by name.
    pub async fn get_failure_domains(
        &self,
        cluster: &str,
    ) -> Result<BTreeMap<String, FailureDomain>, Error> {
        let url = self.clusters_url(&[&encode_segment(cluster), "failureDomains"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets one failure domain.
    pub async fn get_failure_domain(
        &self,
        cluster: &str,
        domain: &str,
    ) -> Result<FailureDomain, Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "failureDomains",
            &encode_segment(domain),
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates or replaces a failure domain.
    pub async fn set_failure_domain(
        &self,
        cluster: &str,
        domain: &str,
        data: &FailureDomain,
    ) -> Result<(), Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "failureDomains",
            &encode_segment(domain),
        ]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(data))
            .await
    }

    /// Deletes a failure domain.
    pub async fn delete_failure_domain(&self, cluster: &str, domain: &str) -> Result<(), Error> {
        let url = self.clusters_url(&[
            &encode_segment(cluster),
            "failureDomains",
            &encode_segment(domain),
        ]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
}

/// Explicit "no request body", so call sites read unambiguously and the generic
/// body parameter can still be inferred.
pub(crate) const NO_BODY: Option<&()> = None;
