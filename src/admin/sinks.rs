//! Sink connector management — `/admin/v3/sink`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Sinks`. Requires a functions worker on
//! the broker. A connector is either **built in** (set `sink_type` to its name,
//! e.g. `jdbc-postgres`) or supplied as a NAR archive.

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{ConnectorDefinition, ConnectorStatus, SinkConfig, UpdateOptions},
        AdminClient,
    },
    Error,
};

/// Handle for the `sinks` group of admin operations.
///
/// Obtained from [`AdminClient::sinks`].
pub struct Sinks<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Sinks<'_> {
    fn base_url(&self, segments: &[&str]) -> String {
        let mut url = format!("{}/admin/v3/sink", self.client.admin_url());
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

    /// Lists the sinks in a namespace.
    pub async fn list_sinks(&self, tenant: &str, namespace: &str) -> Result<Vec<String>, Error> {
        let url = self.base_url(&[&encode_segment(tenant), &encode_segment(namespace)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets a sink's configuration.
    pub async fn get_sink(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<SinkConfig, Error> {
        let url = self.named(tenant, namespace, name, &[]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a sink from a local NAR archive.
    pub async fn create_sink(
        &self,
        config: &SinkConfig,
        filename: &str,
        archive: Vec<u8>,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        self.client
            .send_multipart(
                Method::POST,
                &url,
                &[],
                &[("sinkConfig", Self::json(config)?)],
                &[],
                Some(("data", filename.to_string(), archive)),
            )
            .await
    }

    /// Creates a sink whose archive the *worker* fetches, or a built-in one.
    ///
    /// For a built-in connector set `sink_type` on the config and pass an empty
    /// URL; the worker resolves the name against its own connector directory.
    pub async fn create_sink_with_url(
        &self,
        config: &SinkConfig,
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
                &[("sinkConfig", Self::json(config)?)],
                &text,
                None,
            )
            .await
    }

    /// Replaces a sink's configuration and archive.
    pub async fn update_sink(
        &self,
        config: &SinkConfig,
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
                &Self::config_parts("sinkConfig", config, options)?,
                &[],
                Some(("data", filename.to_string(), archive)),
            )
            .await
    }

    /// Replaces a sink using an archive URL the worker fetches.
    pub async fn update_sink_with_url(
        &self,
        config: &SinkConfig,
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
                &Self::config_parts("sinkConfig", config, options)?,
                &text,
                None,
            )
            .await
    }

    /// Deletes a sink.
    pub async fn delete_sink(
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

    /// Gets a sink's aggregate status.
    pub async fn get_sink_status(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<ConnectorStatus, Error> {
        let url = self.named(tenant, namespace, name, &["status"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets one instance's status.
    pub async fn get_sink_instance_status(
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
    pub async fn restart_sink(
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
    pub async fn restart_sink_instance(
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
    pub async fn stop_sink(&self, tenant: &str, namespace: &str, name: &str) -> Result<(), Error> {
        let url = self.named(tenant, namespace, name, &["stop"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Stops one instance.
    pub async fn stop_sink_instance(
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
    pub async fn start_sink(&self, tenant: &str, namespace: &str, name: &str) -> Result<(), Error> {
        let url = self.named(tenant, namespace, name, &["start"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// Starts one stopped instance.
    pub async fn start_sink_instance(
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

    /// Lists the sinks built into the broker.
    pub async fn get_built_in_sinks(&self) -> Result<Vec<ConnectorDefinition>, Error> {
        let url = self.base_url(&["builtinsinks"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Reloads the built-in connector list from disk.
    pub async fn reload_built_in_sinks(&self) -> Result<(), Error> {
        let url = self.base_url(&["reloadBuiltInSinks"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// The JSON parts of an update: the config, plus `updateOptions` when given.
    fn config_parts(
        name: &'static str,
        config: &SinkConfig,
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

    fn json(config: &SinkConfig) -> Result<String, Error> {
        serde_json::to_string(config)
            .map_err(|e| Error::Custom(format!("could not serialize SinkConfig: {e}")))
    }

    /// The URL a config addresses, from its own tenant/namespace/name.
    fn config_url(&self, config: &SinkConfig) -> Result<String, Error> {
        let missing = |what: &str| Error::Custom(format!("SinkConfig is missing its {what}"));
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
