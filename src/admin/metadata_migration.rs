//! Metadata-store migration control — `/admin/v2/metadata/migration`.
//!
//! Mirrors `org.apache.pulsar.client.admin.MetadataMigration`, used when moving a
//! cluster's metadata between stores — for example ZooKeeper to Oxia.
//!
//! A cluster that has never migrated reports [`MigrationPhase::NotStarted`].
//! Starting a migration is a cluster-wide, one-way operation.

use reqwest::Method;

use crate::{
    admin::{clusters::NO_BODY, models::MigrationState, AdminClient},
    Error,
};

/// Handle for the `metadata_migration` group of admin operations.
///
/// Obtained from [`AdminClient::metadata_migration`].
pub struct MetadataMigration<'a> {
    pub(crate) client: &'a AdminClient,
}

impl MetadataMigration<'_> {
    /// Gets the current migration phase and target store.
    pub async fn status(&self) -> Result<MigrationState, Error> {
        let url = self.client.url(&["metadata", "migration", "status"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Starts migrating the cluster's metadata to `target_url`.
    ///
    /// `target_url` is a metadata-store URL such as
    /// `oxia://oxia-service:6648/pulsar`. The target travels as the `target` query
    /// parameter, matching the Java client.
    ///
    /// This is cluster-wide and not reversible through this API.
    pub async fn start(&self, target_url: &str) -> Result<(), Error> {
        let url = self.client.url(&["metadata", "migration", "start"]);
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("target", target_url.to_string())],
                NO_BODY,
            )
            .await
    }
}
