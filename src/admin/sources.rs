//! Source connector management — `/admin/v3/source`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Sources`. Requires a functions worker on
//! the broker. A connector is either **built in** (set `source_type` to its name,
//! e.g. `data-generator`) or supplied as a NAR archive.

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{ConnectorDefinition, ConnectorStatus, SourceConfig, UpdateOptions},
        AdminClient,
    },
    Error,
};

/// Handle for the `sources` group of admin operations.
///
/// Obtained from [`AdminClient::sources`].
pub struct Sources<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Sources<'_> {
    fn base_url(&self, segments: &[&str]) -> String {
        let mut url = format!("{}/admin/v3/source", self.client.admin_url());
        for segment in segments {
            url.push('/');
            url.push_str(segment);
        }
        url
    }

    fn named(&self, tenant: &str, namespace: &str, name: &str, extra: &[&str]) -> String {
        let (t, ns, n) = (
            encode_segment(tenant),
            encode_segment(namespace),
            encode_segment(name),
        );
        let mut all: Vec<&str> = vec![&t, &ns, &n];
        all.extend_from_slice(extra);
        self.base_url(&all)
    }

    /// Lists the sources in a namespace.
    pub async fn list_sources(&self, tenant: &str, namespace: &str) -> Result<Vec<String>, Error> {
        let url = self.base_url(&[&encode_segment(tenant), &encode_segment(namespace)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets a source's configuration.
    pub async fn get_source(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<SourceConfig, Error> {
        let url = self.named(tenant, namespace, name, &[]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a source from a local NAR archive.
    pub async fn create_source(
        &self,
        config: &SourceConfig,
        filename: &str,
        archive: Vec<u8>,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        self.client
            .send_multipart(
                Method::POST,
                &url,
                &[],
                &[("sourceConfig", Self::json(config)?)],
                &[],
                Some(("data", filename.to_string(), archive)),
            )
            .await
    }

    /// Creates a source whose archive the *worker* fetches, or a built-in one.
    ///
    /// For a built-in connector set `source_type` on the config and pass an empty
    /// URL; the worker resolves the name against its own connector directory.
    pub async fn create_source_with_url(
        &self,
        config: &SourceConfig,
        archive_url: &str,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        let text: Vec<(&str, String)> = if archive_url.is_empty() {
            Vec::new()
        } else {
            vec![("url", archive_url.to_string())]
        };
        self.client
            .send_multipart(
                Method::POST,
                &url,
                &[],
                &[("sourceConfig", Self::json(config)?)],
                &text,
                None,
            )
            .await
    }

    /// Replaces a source's configuration and archive.
    pub async fn update_source(
        &self,
        config: &SourceConfig,
        filename: &str,
        archive: Vec<u8>,
        options: Option<&UpdateOptions>,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        self.client
            .send_multipart(
                Method::PUT,
                &url,
                &[],
                &Self::config_parts("sourceConfig", config, options)?,
                &[],
                Some(("data", filename.to_string(), archive)),
            )
            .await
    }

    /// Replaces a source using an archive URL the worker fetches.
    pub async fn update_source_with_url(
        &self,
        config: &SourceConfig,
        archive_url: &str,
        options: Option<&UpdateOptions>,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        let text: Vec<(&str, String)> = if archive_url.is_empty() {
            Vec::new()
        } else {
            vec![("url", archive_url.to_string())]
        };
        self.client
            .send_multipart(
                Method::PUT,
                &url,
                &[],
                &Self::config_parts("sourceConfig", config, options)?,
                &text,
                None,
            )
            .await
    }

    /// Deletes a source.
    pub async fn delete_source(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<(), Error> {
        let url = self.named(tenant, namespace, name, &[]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }

    /// Gets a source's aggregate status.
    pub async fn get_source_status(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<ConnectorStatus, Error> {
        let url = self.named(tenant, namespace, name, &["status"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets one instance's status.
    pub async fn get_source_instance_status(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        instance_id: i32,
    ) -> Result<serde_json::Value, Error> {
        let id = instance_id.to_string();
        let url = self.named(tenant, namespace, name, &[&id, "status"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Restarts every instance.
    pub async fn restart_source(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<(), Error> {
        let url = self.named(tenant, namespace, name, &["restart"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Restarts one instance.
    pub async fn restart_source_instance(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        instance_id: i32,
    ) -> Result<(), Error> {
        let id = instance_id.to_string();
        let url = self.named(tenant, namespace, name, &[&id, "restart"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Stops every instance.
    pub async fn stop_source(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<(), Error> {
        let url = self.named(tenant, namespace, name, &["stop"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Stops one instance.
    pub async fn stop_source_instance(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        instance_id: i32,
    ) -> Result<(), Error> {
        let id = instance_id.to_string();
        let url = self.named(tenant, namespace, name, &[&id, "stop"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Starts every stopped instance.
    pub async fn start_source(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<(), Error> {
        let url = self.named(tenant, namespace, name, &["start"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Starts one stopped instance.
    pub async fn start_source_instance(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        instance_id: i32,
    ) -> Result<(), Error> {
        let id = instance_id.to_string();
        let url = self.named(tenant, namespace, name, &[&id, "start"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Lists the sources built into the broker.
    pub async fn get_built_in_sources(&self) -> Result<Vec<ConnectorDefinition>, Error> {
        let url = self.base_url(&["builtinsources"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Reloads the built-in connector list from disk.
    pub async fn reload_built_in_sources(&self) -> Result<(), Error> {
        let url = self.base_url(&["reloadBuiltInSources"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// The JSON parts of an update: the config, plus `updateOptions` when given.
    fn config_parts(
        name: &'static str,
        config: &SourceConfig,
        options: Option<&UpdateOptions>,
    ) -> Result<Vec<(&'static str, String)>, Error> {
        let mut parts = vec![(name, Self::json(config)?)];
        if let Some(options) = options {
            parts.push((
                "updateOptions",
                serde_json::to_string(options).map_err(|e| {
                    Error::Custom(format!("could not serialize UpdateOptions: {e}"))
                })?,
            ));
        }
        Ok(parts)
    }

    fn json(config: &SourceConfig) -> Result<String, Error> {
        serde_json::to_string(config)
            .map_err(|e| Error::Custom(format!("could not serialize SourceConfig: {e}")))
    }

    /// The URL a config addresses, from its own tenant/namespace/name.
    fn config_url(&self, config: &SourceConfig) -> Result<String, Error> {
        let missing = |what: &str| Error::Custom(format!("SourceConfig is missing its {what}"));
        Ok(self.named(
            config.tenant.as_deref().ok_or_else(|| missing("tenant"))?,
            config
                .namespace
                .as_deref()
                .ok_or_else(|| missing("namespace"))?,
            config.name.as_deref().ok_or_else(|| missing("name"))?,
            &[],
        ))
    }
}
