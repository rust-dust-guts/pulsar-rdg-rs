//! Pulsar Function management — `/admin/v3/functions`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Functions`. Requires the broker to run
//! a functions worker; a broker started with `--no-functions-worker` answers 404
//! for every endpoint here.
//!
//! Creating a function uploads its package. Three sources are supported, matching
//! the Java client: a local file, a URL the *worker* can reach, or a `function://`
//! package-repository reference set in `FunctionConfig::jar`/`py`/`go`.

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{
            ConnectorDefinition, FunctionConfig, FunctionDefinition, FunctionState, FunctionStats,
            FunctionStatus, UpdateOptions,
        },
        AdminClient,
    },
    Error,
};

/// Handle for the `functions` group of admin operations.
///
/// Obtained from [`AdminClient::functions`].
pub struct Functions<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Functions<'_> {
    /// Functions live under `/admin/v3`, like sinks, sources and transactions.
    fn fn_url(&self, segments: &[&str]) -> String {
        let mut url = format!("{}/admin/v3/functions", self.client.admin_url());
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
        self.fn_url(&all)
    }

    /// Lists the functions in a namespace.
    pub async fn get_functions(&self, tenant: &str, namespace: &str) -> Result<Vec<String>, Error> {
        let url = self.fn_url(&[&encode_segment(tenant), &encode_segment(namespace)]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets a function's configuration.
    pub async fn get_function(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<FunctionConfig, Error> {
        let url = self.named(tenant, namespace, name, &[]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Creates a function from a local package file.
    ///
    /// `package` is the archive contents; `filename` is used only for the form part
    /// and should carry the right extension (`.jar`, `.py`, `.go`).
    pub async fn create_function(
        &self,
        config: &FunctionConfig,
        filename: &str,
        package: Vec<u8>,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        let json = serde_json::to_string(config)
            .map_err(|e| Error::Custom(format!("could not serialize FunctionConfig: {e}")))?;
        self.client
            .send_multipart(
                Method::POST,
                &url,
                &[],
                &[("functionConfig", json)],
                &[],
                Some(("data", filename.to_string(), package)),
            )
            .await
    }

    /// Creates a function from a package the *worker* can fetch.
    ///
    /// Accepts `http(s)://`, `file://` and `function://` URLs. The worker resolves
    /// them, so a `file://` path must exist on the worker, not on this host.
    pub async fn create_function_with_url(
        &self,
        config: &FunctionConfig,
        package_url: &str,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        let json = serde_json::to_string(config)
            .map_err(|e| Error::Custom(format!("could not serialize FunctionConfig: {e}")))?;
        self.client
            .send_multipart(
                Method::POST,
                &url,
                &[],
                &[("functionConfig", json)],
                &[("url", package_url.to_string())],
                None,
            )
            .await
    }

    /// Replaces a function's configuration and package.
    pub async fn update_function(
        &self,
        config: &FunctionConfig,
        filename: &str,
        package: Vec<u8>,
        options: Option<&UpdateOptions>,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        let json = serde_json::to_string(config)
            .map_err(|e| Error::Custom(format!("could not serialize FunctionConfig: {e}")))?;
        let mut json_parts = vec![("functionConfig", json)];
        if let Some(options) = options {
            json_parts.push((
                "updateOptions",
                serde_json::to_string(options).map_err(|e| {
                    Error::Custom(format!("could not serialize UpdateOptions: {e}"))
                })?,
            ));
        }
        self.client
            .send_multipart(
                Method::PUT,
                &url,
                &[],
                &json_parts,
                &[],
                Some(("data", filename.to_string(), package)),
            )
            .await
    }

    /// Replaces a function using a package URL the worker fetches.
    pub async fn update_function_with_url(
        &self,
        config: &FunctionConfig,
        package_url: &str,
        options: Option<&UpdateOptions>,
    ) -> Result<(), Error> {
        let url = self.config_url(config)?;
        let json = serde_json::to_string(config)
            .map_err(|e| Error::Custom(format!("could not serialize FunctionConfig: {e}")))?;
        let mut json_parts = vec![("functionConfig", json)];
        if let Some(options) = options {
            json_parts.push((
                "updateOptions",
                serde_json::to_string(options).map_err(|e| {
                    Error::Custom(format!("could not serialize UpdateOptions: {e}"))
                })?,
            ));
        }
        self.client
            .send_multipart(
                Method::PUT,
                &url,
                &[],
                &json_parts,
                &[("url", package_url.to_string())],
                None,
            )
            .await
    }

    /// Deletes a function.
    pub async fn delete_function(
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

    /// Gets a function's aggregate status.
    pub async fn get_function_status(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<FunctionStatus, Error> {
        let url = self.named(tenant, namespace, name, &["status"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets one instance's status.
    pub async fn get_function_instance_status(
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

    /// Gets a function's aggregate statistics.
    pub async fn get_function_stats(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
    ) -> Result<FunctionStats, Error> {
        let url = self.named(tenant, namespace, name, &["stats"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets one instance's statistics.
    pub async fn get_function_instance_stats(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        instance_id: i32,
    ) -> Result<serde_json::Value, Error> {
        let id = instance_id.to_string();
        let url = self.named(tenant, namespace, name, &[&id, "stats"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Restarts every instance of a function.
    pub async fn restart_function(
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
    pub async fn restart_function_instance(
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

    /// Stops every instance of a function.
    pub async fn stop_function(
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
    pub async fn stop_function_instance(
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

    /// Starts every stopped instance of a function.
    pub async fn start_function(
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
    pub async fn start_function_instance(
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

    /// Reads one key from a function's state store.
    pub async fn get_function_state(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        key: &str,
    ) -> Result<FunctionState, Error> {
        let key = encode_segment(key);
        let url = self.named(tenant, namespace, name, &["state", &key]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Writes one key into a function's state store.
    ///
    /// The worker binds this from a **multipart form part named `state`** holding
    /// the JSON, not from a plain JSON request body — a JSON body is rejected
    /// before any state operation runs.
    pub async fn put_function_state(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        state: &FunctionState,
    ) -> Result<(), Error> {
        let key = encode_segment(state.key.as_deref().unwrap_or_default());
        let url = self.named(tenant, namespace, name, &["state", &key]);
        let json = serde_json::to_string(state)
            .map_err(|e| Error::Custom(format!("could not serialize FunctionState: {e}")))?;
        self.client
            .send_multipart(Method::POST, &url, &[], &[("state", json)], &[], None)
            .await
    }

    /// Sends a value to a function and returns whatever it produced.
    ///
    /// Either `value` or `file` supplies the input — Java sends them as the `data`
    /// and `dataStream` parts respectively. `topic` selects which input topic to
    /// deliver on, for a function with more than one.
    pub async fn trigger_function(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        value: Option<&str>,
        topic: Option<&str>,
        file: Option<(String, Vec<u8>)>,
    ) -> Result<String, Error> {
        let url = self.named(tenant, namespace, name, &["trigger"]);
        let mut text_parts = Vec::new();
        if let Some(value) = value {
            text_parts.push(("data", value.to_string()));
        }
        if let Some(topic) = topic {
            text_parts.push(("topic", topic.to_string()));
        }
        self.client
            .send_multipart_text(
                Method::POST,
                &url,
                &[],
                &[],
                &text_parts,
                file.map(|(filename, bytes)| ("dataStream", filename, bytes)),
            )
            .await
    }

    /// Uploads a package to the worker's package store at `path`.
    ///
    /// `path` is the worker-side location a later
    /// [`create_function_with_url`][Self::create_function_with_url] can point at.
    pub async fn upload_function(
        &self,
        path: &str,
        filename: &str,
        package: Vec<u8>,
    ) -> Result<(), Error> {
        let url = self.fn_url(&["upload"]);
        self.client
            .send_multipart(
                Method::POST,
                &url,
                &[],
                &[],
                &[("path", path.to_string())],
                Some(("data", filename.to_string(), package)),
            )
            .await
    }

    /// Downloads a function's package by name.
    ///
    /// `transform_function` asks for the transform function attached to a sink or
    /// source rather than the function's own package.
    pub async fn download_function(
        &self,
        tenant: &str,
        namespace: &str,
        name: &str,
        transform_function: bool,
    ) -> Result<Vec<u8>, Error> {
        let url = self.named(tenant, namespace, name, &["download"]);
        self.client
            .send_bytes(
                Method::GET,
                &url,
                &[("transform-function", transform_function.to_string())],
            )
            .await
    }

    /// Downloads a package from the worker's package store by path.
    pub async fn download_function_by_path(&self, path: &str) -> Result<Vec<u8>, Error> {
        let url = self.fn_url(&["download"]);
        self.client
            .send_bytes(Method::GET, &url, &[("path", path.to_string())])
            .await
    }

    /// Lists every built-in connector, source and sink alike.
    ///
    /// [`sinks().get_built_in_sinks()`][crate::admin::sinks::Sinks::get_built_in_sinks]
    /// and the source equivalent are the filtered views of this list.
    pub async fn get_connectors_list(&self) -> Result<Vec<ConnectorDefinition>, Error> {
        let url = self.fn_url(&["connectors"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the functions built into the broker.
    pub async fn get_built_in_functions(&self) -> Result<Vec<FunctionDefinition>, Error> {
        let url = self.fn_url(&["builtins"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Reloads the built-in function list from disk.
    pub async fn reload_built_in_functions(&self) -> Result<(), Error> {
        let url = self.fn_url(&["builtins", "reload"]);
        self.client
            .send_empty(Method::POST, &url, &[], NO_BODY)
            .await
    }

    /// The URL a config addresses, from its own tenant/namespace/name.
    fn config_url(&self, config: &FunctionConfig) -> Result<String, Error> {
        let missing = |what: &str| Error::Custom(format!("FunctionConfig is missing its {what}"));
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
