//! Resource quota administration — `/admin/v2/resource-quotas`.
//!
//! Mirrors `org.apache.pulsar.client.admin.ResourceQuotas`.

use reqwest::Method;

use crate::{
    admin::{clusters::NO_BODY, encode_segment, models::ResourceQuota, AdminClient},
    Error,
};

/// Handle for the `resource_quotas` group of admin operations.
///
/// Obtained from [`AdminClient::resource_quotas`]. Grouping mirrors the Java admin
/// client's separate interfaces and keeps same-named operations on different
/// resource kinds (a namespace retention policy vs a topic one) distinct.
pub struct ResourceQuotas<'a> {
    pub(crate) client: &'a AdminClient,
}

impl ResourceQuotas<'_> {
    fn quotas_url(&self, segments: &[&str]) -> String {
        let mut all = vec!["resource-quotas"];
        all.extend_from_slice(segments);
        self.client.url(&all)
    }

    /// Gets the quota applied to bundles with no explicit override.
    pub async fn get_default_resource_quota(&self) -> Result<ResourceQuota, Error> {
        self.client
            .send_json(Method::GET, &self.quotas_url(&[]), &[], NO_BODY)
            .await
    }

    /// Sets the default bundle quota.
    pub async fn set_default_resource_quota(&self, quota: &ResourceQuota) -> Result<(), Error> {
        self.client
            .send_empty(Method::POST, &self.quotas_url(&[]), &[], Some(quota))
            .await
    }

    /// Gets the quota for one namespace bundle.
    ///
    /// `namespace` is `tenant/namespace`; `bundle` is a hash range such as
    /// `0x00000000_0xffffffff`.
    pub async fn get_namespace_bundle_resource_quota(
        &self,
        namespace: &str,
        bundle: &str,
    ) -> Result<ResourceQuota, Error> {
        let url = self.bundle_url(namespace, bundle)?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Sets the quota for one namespace bundle.
    pub async fn set_namespace_bundle_resource_quota(
        &self,
        namespace: &str,
        bundle: &str,
        quota: &ResourceQuota,
    ) -> Result<(), Error> {
        let url = self.bundle_url(namespace, bundle)?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(quota))
            .await
    }

    /// Removes a bundle's quota override, restoring the default.
    pub async fn reset_namespace_bundle_resource_quota(
        &self,
        namespace: &str,
        bundle: &str,
    ) -> Result<(), Error> {
        let url = self.bundle_url(namespace, bundle)?;
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    fn bundle_url(&self, namespace: &str, bundle: &str) -> Result<String, Error> {
        let (tenant, ns) = crate::admin::split_namespace(namespace)?;
        Ok(self.quotas_url(&[
            &encode_segment(tenant),
            &encode_segment(ns),
            &encode_segment(bundle),
        ]))
    }
}
