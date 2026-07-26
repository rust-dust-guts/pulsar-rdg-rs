//! Function-worker cluster inspection — `/admin/v2/worker` and `/worker-stats`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Worker`. Requires the broker to run a
//! functions worker.

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        models::{Metrics, WorkerFunctionInstanceStats, WorkerInfo},
        AdminClient,
    },
    Error,
};

/// Handle for the `worker` group of admin operations.
///
/// Obtained from [`AdminClient::worker`].
pub struct Worker<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Worker<'_> {
    /// Lists the workers in the function cluster.
    pub async fn get_cluster(&self) -> Result<Vec<WorkerInfo>, Error> {
        let url = self.client.url(&["worker", "cluster"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets the worker currently elected leader.
    pub async fn get_cluster_leader(&self) -> Result<WorkerInfo, Error> {
        let url = self.client.url(&["worker", "cluster", "leader"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the function assignments held by each worker.
    pub async fn get_assignments(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Vec<String>>, Error> {
        let url = self.client.url(&["worker", "assignments"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Redistributes function instances across the workers.
    pub async fn rebalance(&self) -> Result<(), Error> {
        let url = self.client.url(&["worker", "rebalance"]);
        self.client
            .send_empty(Method::PUT, &url, &[], NO_BODY)
            .await
    }

    /// Per-instance metrics for every function this worker runs.
    pub async fn get_functions_stats(&self) -> Result<Vec<WorkerFunctionInstanceStats>, Error> {
        let url = self.client.url(&["worker-stats", "functionsmetrics"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Worker-level metrics.
    pub async fn get_metrics(&self) -> Result<Vec<Metrics>, Error> {
        let url = self.client.url(&["worker-stats", "metrics"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }
}
