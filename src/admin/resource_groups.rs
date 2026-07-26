//! Resource group administration — `/admin/v2/resourcegroups`.
//!
//! Mirrors `org.apache.pulsar.client.admin.ResourceGroups`.

use reqwest::Method;

use crate::{
    admin::{clusters::NO_BODY, encode_segment, models::ResourceGroup, AdminClient},
    Error,
};

/// Handle for the `resource_groups` group of admin operations.
///
/// Obtained from [`AdminClient::resource_groups`]. Grouping mirrors the Java admin
/// client's separate interfaces and keeps same-named operations on different
/// resource kinds (a namespace retention policy vs a topic one) distinct.
pub struct ResourceGroups<'a> {
    pub(crate) client: &'a AdminClient,
}

impl ResourceGroups<'_> {
    fn resource_groups_url(&self, segments: &[&str]) -> String {
        let mut all = vec!["resourcegroups"];
        all.extend_from_slice(segments);
        self.client.url(&all)
    }

    /// Lists the names of all resource groups.
    pub async fn get_resource_groups(&self) -> Result<Vec<String>, Error> {
        self.client
            .send_json(Method::GET, &self.resource_groups_url(&[]), &[], NO_BODY)
            .await
    }

    /// Gets one resource group's limits.
    pub async fn get_resource_group(&self, name: &str) -> Result<ResourceGroup, Error> {
        let url = self.resource_groups_url(&[&encode_segment(name)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a resource group.
    pub async fn create_resource_group(
        &self,
        name: &str,
        group: &ResourceGroup,
    ) -> Result<(), Error> {
        let url = self.resource_groups_url(&[&encode_segment(name)]);
        self.client
            .send_empty(Method::PUT, &url, &[], Some(group))
            .await
    }

    /// Replaces a resource group's limits.
    ///
    /// The broker uses `PUT` for both create and update here, unlike clusters and
    /// tenants which use `POST` to update.
    pub async fn update_resource_group(
        &self,
        name: &str,
        group: &ResourceGroup,
    ) -> Result<(), Error> {
        let url = self.resource_groups_url(&[&encode_segment(name)]);
        self.client
            .send_empty(Method::PUT, &url, &[], Some(group))
            .await
    }

    /// Deletes a resource group. It must not be referenced by any namespace.
    pub async fn delete_resource_group(&self, name: &str) -> Result<(), Error> {
        let url = self.resource_groups_url(&[&encode_segment(name)]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
}
