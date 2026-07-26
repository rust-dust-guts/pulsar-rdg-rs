//! Tenant administration — `/admin/v2/tenants`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Tenants`.

use reqwest::Method;

use crate::{
    admin::{clusters::NO_BODY, encode_segment, models::TenantInfo, AdminClient},
    Error,
};

/// Handle for the `tenants` group of admin operations.
///
/// Obtained from [`AdminClient::tenants`]. Grouping mirrors the Java admin
/// client's separate interfaces and keeps same-named operations on different
/// resource kinds (a namespace retention policy vs a topic one) distinct.
pub struct Tenants<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Tenants<'_> {
    fn tenants_url(&self, segments: &[&str]) -> String {
        let mut all = vec!["tenants"];
        all.extend_from_slice(segments);
        self.client.url(&all)
    }

    /// Lists the names of all tenants.
    pub async fn get_tenants(&self) -> Result<Vec<String>, Error> {
        self.client
            .send_json(Method::GET, &self.tenants_url(&[]), &[], NO_BODY)
            .await
    }

    /// Gets a tenant's admin roles and allowed clusters.
    pub async fn get_tenant_info(&self, tenant: &str) -> Result<TenantInfo, Error> {
        let url = self.tenants_url(&[&encode_segment(tenant)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a tenant. Every cluster in `info.allowed_clusters` must exist.
    pub async fn create_tenant(&self, tenant: &str, info: &TenantInfo) -> Result<(), Error> {
        let url = self.tenants_url(&[&encode_segment(tenant)]);
        self.client
            .send_empty(Method::PUT, &url, &[], Some(info))
            .await
    }

    /// Replaces a tenant's admin roles and allowed clusters.
    pub async fn update_tenant(&self, tenant: &str, info: &TenantInfo) -> Result<(), Error> {
        let url = self.tenants_url(&[&encode_segment(tenant)]);
        self.client
            .send_empty(Method::POST, &url, &[], Some(info))
            .await
    }

    /// Deletes a tenant.
    ///
    /// The tenant must have no namespaces unless `force` is set, in which case the
    /// broker deletes them too.
    pub async fn delete_tenant(&self, tenant: &str, force: bool) -> Result<(), Error> {
        let url = self.tenants_url(&[&encode_segment(tenant)]);
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }
}
