//! Pulsar Admin REST API client.
//!
//! Enabled by the `admin-api` feature flag. Requires a tokio runtime.

use std::{collections::HashMap, fmt::Write as _, sync::Arc, time::Duration};

use futures::lock::Mutex;

use crate::{
    authentication::Authentication,
    connection_manager::TlsOptions,
    error::{AdminError, Error},
    message::proto::{self, Schema},
};

pub mod bookies;
pub mod broker_stats;
pub mod brokers;
pub mod clusters;
pub mod functions;
pub mod lookup;
pub mod metadata_migration;
pub mod models;
pub mod namespaces;
pub mod non_persistent_topics;
pub mod packages;
pub mod proxy_stats;
pub mod resource_groups;
pub mod resource_quotas;
pub mod scalable_topics;
pub mod schemas;
pub mod sinks;
pub mod sources;
pub mod tenants;
pub mod topic_policies;
pub mod topics;
pub mod transactions;
pub mod worker;

/// Broker-backed tests for the endpoint groups above. The unit tests for URL and
/// payload handling live in the `tests` module at the bottom of this file.
#[cfg(test)]
mod integration_tests;

// The async-std tests live in `tests/async_std_admin.rs` rather than here: this
// crate's unit-test tree is Tokio-only, so an in-crate module could not be built
// with `--no-default-features` and would have silently linked Tokio.

/// Parses a Pulsar topic URL into (scheme, tenant, namespace, topic_name).
/// Accepts `persistent://` and `non-persistent://` prefixes, or a bare
/// `tenant/namespace/topic` string which defaults to `persistent://`.
pub(crate) fn parse_topic(topic: &str) -> Result<(&str, &str, &str, &str), Error> {
    let invalid = || {
        Error::Admin(AdminError::InvalidTopic(format!(
            "expected tenant/namespace/topic or a fully-qualified topic URL, got: {topic}"
        )))
    };

    let (scheme, rest) = if let Some(rest) = topic.strip_prefix("persistent://") {
        ("persistent", rest)
    } else if let Some(rest) = topic.strip_prefix("non-persistent://") {
        ("non-persistent", rest)
    } else {
        ("persistent", topic)
    };

    let mut parts = rest.splitn(3, '/');
    let tenant = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
    let namespace = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
    let name = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;

    Ok((scheme, tenant, namespace, name))
}

/// Percent-encodes one path segment.
///
/// Every byte outside the unreserved set is escaped, including `/`, so a name
/// containing a slash cannot inject extra path segments into the request.
pub(crate) fn encode_segment(name: &str) -> String {
    let mut encoded = String::with_capacity(name.len());
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            _ => write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail"),
        }
    }
    encoded
}

fn encode_topic_local_name(name: &str) -> String {
    encode_segment(name)
}

/// Splits a topic into the encoded path segments the admin API expects:
/// `[domain, tenant, namespace, topic]`, e.g.
/// `["persistent", "public", "default", "my-topic"]`.
pub(crate) fn parse_topic_path(topic: &str) -> Result<Vec<String>, Error> {
    let (scheme, tenant, namespace, name) = parse_topic(topic)?;
    Ok(vec![
        scheme.to_string(),
        encode_segment(tenant),
        encode_segment(namespace),
        encode_segment(name),
    ])
}

/// Splits a `tenant/namespace` string into its two parts.
pub(crate) fn split_namespace(namespace: &str) -> Result<(&str, &str), Error> {
    let invalid = || {
        Error::Admin(AdminError::InvalidTopic(format!(
            "expected tenant/namespace, got: {namespace}"
        )))
    };
    let (tenant, ns) = namespace.split_once('/').ok_or_else(invalid)?;
    if tenant.is_empty() || ns.is_empty() || ns.contains('/') {
        return Err(invalid());
    }
    Ok((tenant, ns))
}

#[derive(serde::Deserialize)]
struct AdminSchemaResponse {
    #[serde(rename = "type")]
    schema_type: String,
    #[serde(default)]
    data: String,
    #[serde(default)]
    properties: HashMap<String, String>,
}

fn schema_type_from_admin(schema_type: &str) -> Result<proto::schema::Type, Error> {
    match schema_type.to_ascii_uppercase().as_str() {
        "NONE" => Ok(proto::schema::Type::None),
        "STRING" => Ok(proto::schema::Type::String),
        "JSON" => Ok(proto::schema::Type::Json),
        "PROTOBUF" => Ok(proto::schema::Type::Protobuf),
        "AVRO" => Ok(proto::schema::Type::Avro),
        "BOOL" | "BOOLEAN" => Ok(proto::schema::Type::Bool),
        "INT8" => Ok(proto::schema::Type::Int8),
        "INT16" => Ok(proto::schema::Type::Int16),
        "INT32" => Ok(proto::schema::Type::Int32),
        "INT64" => Ok(proto::schema::Type::Int64),
        "FLOAT" => Ok(proto::schema::Type::Float),
        "DOUBLE" => Ok(proto::schema::Type::Double),
        "DATE" => Ok(proto::schema::Type::Date),
        "TIME" => Ok(proto::schema::Type::Time),
        "TIMESTAMP" => Ok(proto::schema::Type::Timestamp),
        "KEYVALUE" | "KEY_VALUE" => Ok(proto::schema::Type::KeyValue),
        "INSTANT" => Ok(proto::schema::Type::Instant),
        "LOCALDATE" | "LOCAL_DATE" => Ok(proto::schema::Type::LocalDate),
        "LOCALTIME" | "LOCAL_TIME" => Ok(proto::schema::Type::LocalTime),
        "LOCALDATETIME" | "LOCAL_DATE_TIME" => Ok(proto::schema::Type::LocalDateTime),
        "PROTOBUFNATIVE" | "PROTOBUF_NATIVE" => Ok(proto::schema::Type::ProtobufNative),
        _ => Err(AdminError::InvalidSchemaType(schema_type.to_string()).into()),
    }
}

fn key_value_schema_part_bytes(value: &serde_json::Value) -> Result<Vec<u8>, Error> {
    if value.as_str() == Some("") {
        return Ok(Vec::new());
    }
    serde_json::to_vec(value).map_err(|e| AdminError::SchemaDecode(e.to_string()).into())
}

fn append_key_value_schema_part(schema_data: &mut Vec<u8>, part: &[u8]) -> Result<(), Error> {
    let len = u32::try_from(part.len()).map_err(|_| {
        AdminError::SchemaDecode("KEY_VALUE schema part is too large to encode".to_string())
    })?;
    schema_data.extend_from_slice(&len.to_be_bytes());
    schema_data.extend_from_slice(part);
    Ok(())
}

fn key_value_schema_data_from_admin(data: &str) -> Result<Vec<u8>, Error> {
    let value: serde_json::Value =
        serde_json::from_str(data).map_err(|e| AdminError::SchemaDecode(e.to_string()))?;
    let object = value.as_object().ok_or_else(|| {
        AdminError::SchemaDecode("KEY_VALUE schema data must be a JSON object".to_string())
    })?;
    let key = object
        .get("key")
        .ok_or_else(|| AdminError::SchemaDecode("KEY_VALUE schema data missing key".to_string()))?;
    let value = object.get("value").ok_or_else(|| {
        AdminError::SchemaDecode("KEY_VALUE schema data missing value".to_string())
    })?;
    let key = key_value_schema_part_bytes(key)?;
    let value = key_value_schema_part_bytes(value)?;
    let mut schema_data = Vec::with_capacity(8 + key.len() + value.len());
    append_key_value_schema_part(&mut schema_data, &key)?;
    append_key_value_schema_part(&mut schema_data, &value)?;
    Ok(schema_data)
}

fn parse_schema_response(body: &str) -> Result<Schema, Error> {
    let response: AdminSchemaResponse =
        serde_json::from_str(body).map_err(|e| AdminError::SchemaDecode(e.to_string()))?;
    let schema_type = schema_type_from_admin(&response.schema_type)?;
    let schema_data = if schema_type == proto::schema::Type::KeyValue {
        key_value_schema_data_from_admin(&response.data)?
    } else {
        response.data.into_bytes()
    };
    Ok(Schema {
        r#type: schema_type as i32,
        schema_data,
        properties: response
            .properties
            .into_iter()
            .map(|(key, value)| proto::KeyValue { key, value })
            .collect(),
        ..Default::default()
    })
}

/// Client for the Pulsar Admin REST API.
///
/// Obtain an instance via [`Pulsar::admin()`][crate::Pulsar::admin].
///
/// # Example
///
/// Operations are grouped by resource kind, mirroring the Java admin client's
/// separate interfaces: [`clusters`][Self::clusters], [`tenants`][Self::tenants],
/// [`namespaces`][Self::namespaces], [`brokers`][Self::brokers],
/// [`bookies`][Self::bookies], [`resource_groups`][Self::resource_groups] and
/// [`resource_quotas`][Self::resource_quotas].
///
/// # Example
///
/// ```rust,no_run
/// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
/// use pulsar::admin::models::{RetentionPolicies, TenantInfo};
///
/// let admin = pulsar.admin("http://localhost:8080")?;
///
/// admin
///     .tenants()
///     .create_tenant("my-tenant", &TenantInfo::with_clusters(["standalone"]))
///     .await?;
///
/// admin.namespaces().create_namespace("my-tenant/my-ns").await?;
///
/// admin
///     .namespaces()
///     .set_retention(
///         "my-tenant/my-ns",
///         &RetentionPolicies { retention_time_in_minutes: 60, retention_size_in_mb: 512 },
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct AdminClient {
    client: reqwest::Client,
    admin_url: String,
    auth: Option<Arc<Mutex<Box<dyn Authentication>>>>,
}

/// Tuning for [`AdminClient`], passed to
/// [`Pulsar::admin_with_options`][crate::Pulsar::admin_with_options].
#[derive(Clone, Debug)]
pub struct AdminOptions {
    /// How long any single admin request may take, including its redirects.
    ///
    /// Defaults to 60 seconds, which is Java's `requestTimeoutMs`. It was 30 —
    /// the value this client originally hard-coded — but a broker servicing
    /// concurrent admin work can take longer than that on operations that touch
    /// topic policies, and half of Java's allowance is not a useful place to give
    /// up.
    pub timeout: Duration,
}

impl Default for AdminOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
        }
    }
}

/// One hop of a redirect chain.
#[derive(Clone, Copy)]
struct Hop<'a> {
    /// Where to send this hop.
    url: &'a str,
    /// False after a 301/302/303, which must be re-issued as a bodyless GET.
    keep_body: bool,
    /// True only for the first request: a redirect `Location` carries its own
    /// query, so re-appending the caller's would duplicate every parameter.
    apply_query: bool,
}

impl AdminClient {
    /// Creates a new `AdminClient`.
    ///
    /// Reuses the TLS and authentication configuration already present on the
    /// [`Pulsar`][crate::Pulsar] client. Called internally by
    /// [`Pulsar::admin()`][crate::Pulsar::admin].
    pub(crate) fn new(
        admin_url: String,
        tls_options: &TlsOptions,
        auth: Option<Arc<Mutex<Box<dyn Authentication>>>>,
    ) -> Result<Self, Error> {
        Self::with_options(admin_url, tls_options, auth, &AdminOptions::default())
    }

    pub(crate) fn with_options(
        admin_url: String,
        tls_options: &TlsOptions,
        auth: Option<Arc<Mutex<Box<dyn Authentication>>>>,
        options: &AdminOptions,
    ) -> Result<Self, Error> {
        let mut builder = reqwest::ClientBuilder::new()
            .timeout(options.timeout)
            // Redirects are followed by hand (see `send_with_redirects`). reqwest
            // strips `Authorization` when a redirect crosses to another host or
            // port — which is exactly what Pulsar's 307 to the broker that owns a
            // resource does — and it cannot replay a streaming multipart body,
            // because such a body is not cloneable.
            .redirect(reqwest::redirect::Policy::none())
            .danger_accept_invalid_certs(tls_options.allow_insecure_connection);

        builder = builder.danger_accept_invalid_hostnames(
            tls_options.allow_insecure_connection || !tls_options.tls_hostname_verification_enabled,
        );

        if let Some(pem_bytes) = &tls_options.certificate_chain {
            let certs = pem::parse_many(pem_bytes).map_err(|e| {
                Error::Admin(AdminError::TlsConfig(format!(
                    "failed to parse certificate chain: {e}"
                )))
            })?;
            for cert in certs.iter().rev() {
                let reqwest_cert = reqwest::Certificate::from_der(cert.contents())
                    .map_err(|e| Error::Admin(AdminError::Request(e)))?;
                builder = builder.add_root_certificate(reqwest_cert);
            }
        }

        Ok(AdminClient {
            client: builder
                .build()
                .map_err(|e| Error::Admin(AdminError::Request(e)))?,
            admin_url: admin_url.trim_end_matches('/').to_string(),
            auth,
        })
    }

    /// Runs `request` on a tokio reactor, whatever runtime the caller is using.
    ///
    /// `reqwest` needs tokio. Under any other executor — `async-std`, say — its
    /// futures panic with "no reactor running", which is why the admin client used
    /// to be unusable outside tokio. When there is already an ambient tokio runtime
    /// the future runs inline; otherwise it is spawned onto a small shared runtime
    /// owned by this process, and the resulting `JoinHandle` is awaited from
    /// whichever executor the caller is on.
    async fn on_tokio<F>(request: F) -> Result<reqwest::Response, Error>
    where
        F: std::future::Future<Output = Result<reqwest::Response, Error>> + Send + 'static,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            return request.await;
        }

        static BRIDGE: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        let runtime = BRIDGE.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .thread_name("pulsar-admin-http")
                .build()
                .expect("could not start the admin client's HTTP runtime")
        });
        runtime
            .spawn(request)
            .await
            .map_err(|e| Error::Custom(format!("the admin HTTP task failed: {e}")))?
    }

    async fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, Error> {
        let Some(auth) = &self.auth else {
            return Ok(req);
        };
        let mut auth = auth.lock().await;
        let method = auth.auth_method_name();
        let data = auth.auth_data().await.map_err(Error::Authentication)?;
        let data_str = String::from_utf8(data)
            .map_err(|e| Error::Custom(format!("auth data is not valid UTF-8: {e}")))?;
        Ok(match method.as_str() {
            "token" => req.bearer_auth(data_str),
            "basic" => match data_str.split_once(':') {
                Some((user, pass)) => req.basic_auth(user, Some(pass)),
                None => req.basic_auth(&data_str, None::<&str>),
            },
            // Sending the request unauthenticated would surface as a confusing 401
            // from the broker rather than as the configuration problem it is.
            other => {
                return Err(Error::Admin(AdminError::NotSupported(format!(
                    "the admin client cannot send {other:?} authentication over HTTP; \
                     only \"token\" and \"basic\" are mapped"
                ))))
            }
        })
    }

    /// The configured admin base URL, without a trailing slash.
    pub(crate) fn admin_url(&self) -> &str {
        &self.admin_url
    }

    fn topic_policy_url(&self, topic: &str, policy: &str) -> Result<String, Error> {
        let (scheme, tenant, namespace, name) = parse_topic(topic)?;
        Ok(format!(
            "{}/admin/v2/{}/{}/{}/{}/{policy}",
            self.admin_url, scheme, tenant, namespace, name
        ))
    }

    fn schema_url(&self, topic: &str, version: Option<u64>) -> Result<String, Error> {
        let (_, tenant, namespace, name) = parse_topic(topic)?;
        let name = encode_topic_local_name(name);
        let url = format!(
            "{}/admin/v2/schemas/{}/{}/{}/schema",
            self.admin_url, tenant, namespace, name
        );
        Ok(match version {
            Some(version) => format!("{url}/{version}"),
            None => url,
        })
    }

    async fn check_response(&self, resp: reqwest::Response) -> Result<(), Error> {
        if resp.status().is_success() {
            return Ok(());
        }
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(Error::Admin(AdminError::from_status(status, body)))
    }

    /// Builds an `/admin/v2`-rooted URL from already-encoded path segments.
    pub(crate) fn url(&self, segments: &[&str]) -> String {
        let mut url = format!("{}/admin/v2", self.admin_url);
        for segment in segments {
            url.push('/');
            url.push_str(segment);
        }
        url
    }

    /// Sends a request and discards the (expected empty) response body.
    pub(crate) async fn send_empty(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json: Option<&(impl serde::Serialize + ?Sized)>,
    ) -> Result<(), Error> {
        let resp = self.send(method, url, query, json).await?;
        self.check_response(resp).await
    }

    /// Sends `body` as the raw request entity, without JSON encoding it.
    ///
    /// A few endpoints bind the entity straight onto a Java `String` parameter
    /// rather than parsing it, so they take the text literally. Sending JSON there
    /// stores the value *with its quotes* — and because the same endpoint echoes
    /// the raw stored text back, a naive get/set round-trip still looks correct.
    /// Only a lookup that matches on the value exposes it.
    pub(crate) async fn send_raw_text(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        body: &str,
    ) -> Result<(), Error> {
        let owned = body.to_string();
        let resp = self
            .send_with_redirects(url, |hop| {
                let mut req = if hop.keep_body {
                    self.client
                        .request(method.clone(), hop.url)
                        .header(reqwest::header::CONTENT_TYPE, "application/json")
                        .body(owned.clone())
                } else {
                    self.client.get(hop.url)
                };
                if hop.apply_query && !query.is_empty() {
                    req = req.query(query);
                }
                Ok(req)
            })
            .await?;
        self.check_response(resp).await
    }

    /// Sends a request and deserializes a JSON response body.
    pub(crate) async fn send_json<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json: Option<&(impl serde::Serialize + ?Sized)>,
    ) -> Result<T, Error> {
        let resp = self.send(method, url, query, json).await?;
        let resp = self.require_success(resp).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        Self::decode(&body)
    }

    /// Like [`Self::send_json_opt`], but for the few endpoints whose contract
    /// really does spell "absent" as HTTP 404.
    ///
    /// Deliberately narrow. Policy getters answer an unset value with HTTP 200 and
    /// an empty body and reserve 404 for a tenant/namespace/topic that does not
    /// exist, so mapping 404 to `None` there would hide a missing resource. Only
    /// these use it:
    ///
    /// * `clusters/{cluster}/migrate` — answers 404 `Cluster does not exist` even
    ///   for a cluster that exists, when no migration is configured.
    /// * `bookies/racks-info/{bookie}` — 404 when the bookie has no rack assigned.
    /// * `schemas/.../schema` — 404 when the topic carries no schema.
    pub(crate) async fn send_json_absent_on_404<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json: Option<&(impl serde::Serialize + ?Sized)>,
    ) -> Result<Option<T>, Error> {
        let resp = self.send(method, url, query, json).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = self.require_success(resp).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        Self::decode(&body)
    }

    /// Like [`Self::send_json`], for reads whose "not set" answer is an empty body.
    ///
    /// A policy with no override comes back as HTTP 200 with an empty body, which
    /// [`Self::decode`] turns into `None`. A 404 is deliberately **not** mapped to
    /// `None`: the broker uses it for a missing tenant, namespace or topic, and
    /// swallowing it would report a nonexistent resource as one with no override.
    pub(crate) async fn send_json_opt<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json: Option<&(impl serde::Serialize + ?Sized)>,
    ) -> Result<Option<T>, Error> {
        let resp = self.send(method, url, query, json).await?;
        let resp = self.require_success(resp).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        // Several policy endpoints answer 200 with an empty body to mean "unset".
        if body.trim().is_empty() {
            return Ok(None);
        }
        Self::decode(&body).map(Some)
    }

    /// Sends a request and returns the raw response body.
    ///
    /// A few endpoints (notably `brokers/version`) answer with a bare string
    /// rather than JSON, which `send_json` cannot parse.
    pub(crate) async fn send_text(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
    ) -> Result<String, Error> {
        let resp = self
            .send(method, url, query, crate::admin::clusters::NO_BODY)
            .await?;
        let resp = self.require_success(resp).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        // Some of these endpoints answer with a JSON string (`"rg1"`) and others
        // with bare text (`0xc0000000_0xffffffff`) or a whole JSON document.
        // Decoding a JSON string properly also unescapes it; stripping quotes by
        // hand corrupted any value that legitimately began or ended with one.
        Ok(match serde_json::from_str::<String>(body.trim()) {
            Ok(decoded) => decoded,
            Err(_) => body.trim().to_string(),
        })
    }

    /// Sends a request whose response is a message: payload in the body, metadata
    /// in `X-Pulsar-*` headers.
    ///
    /// Used by `peek` and `examine`, which do not return JSON.
    pub(crate) async fn send_message(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
    ) -> Result<crate::admin::models::PeekedMessage, Error> {
        use crate::admin::models::PeekedMessage;

        let resp = self
            .send(method, url, query, crate::admin::clusters::NO_BODY)
            .await?;
        let resp = self.require_success(resp).await?;

        let mut message = PeekedMessage::default();
        for (name, value) in resp.headers() {
            let Ok(value) = value.to_str() else { continue };
            let name = name.as_str().to_ascii_lowercase();
            match name.as_str() {
                "x-pulsar-message-id" => message.message_id = Some(value.to_string()),
                "x-pulsar-publish-time" => message.publish_time = Some(value.to_string()),
                "x-pulsar-event-time" => message.event_time = Some(value.to_string()),
                "x-pulsar-producer-name" => message.producer_name = Some(value.to_string()),
                "x-pulsar-partition-key" => message.partition_key = Some(value.to_string()),
                // One header carrying a JSON object — not one header per key. The
                // per-key spelling `X-Pulsar-PROPERTY-<name>` is used only for the
                // chunk counters below, so matching on that prefix both loses every
                // real property and misreports the chunk counters as properties.
                "x-pulsar-property" => {
                    if let Ok(map) =
                        serde_json::from_str::<std::collections::BTreeMap<String, String>>(value)
                    {
                        message.properties.extend(map);
                    }
                }
                "x-pulsar-num-batch-message" => {
                    message.num_messages_in_batch = value.parse().ok();
                }
                "x-pulsar-null-value" => message.null_value = value == "true",
                _ => {}
            }
        }
        message.payload = resp
            .bytes()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?
            .to_vec();
        Ok(message)
    }

    /// Sends a `multipart/form-data` request.
    ///
    /// Function, sink, source and package management upload a config document and
    /// optionally a package archive as separate form parts. `json_parts` carries
    /// `(name, json)` pairs; `file_part` carries `(name, filename, bytes)`.
    async fn send_multipart_response(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json_parts: &[(&str, String)],
        text_parts: &[(&str, String)],
        file_part: Option<(&str, String, Vec<u8>)>,
    ) -> Result<reqwest::Response, Error> {
        // The form is rebuilt for every attempt. reqwest turns a form into a
        // streaming body, which is not cloneable, so a redirect cannot replay it —
        // uploads that land on a worker that does not own the function would
        // otherwise return the raw redirect instead of following it.
        let build_form = || -> Result<reqwest::multipart::Form, Error> {
            let mut form = reqwest::multipart::Form::new();
            for (name, json) in json_parts {
                let part = reqwest::multipart::Part::text(json.clone())
                    .mime_str("application/json")
                    .map_err(|e| Error::Admin(AdminError::Request(e)))?;
                form = form.part(name.to_string(), part);
            }
            for (name, text) in text_parts {
                let part = reqwest::multipart::Part::text(text.clone())
                    .mime_str("text/plain")
                    .map_err(|e| Error::Admin(AdminError::Request(e)))?;
                form = form.part(name.to_string(), part);
            }
            if let Some((name, filename, bytes)) = &file_part {
                let part = reqwest::multipart::Part::bytes(bytes.clone())
                    .file_name(filename.clone())
                    .mime_str("application/octet-stream")
                    .map_err(|e| Error::Admin(AdminError::Request(e)))?;
                form = form.part(name.to_string(), part);
            }
            Ok(form)
        };

        let resp = self
            .send_with_redirects(url, |hop| {
                let mut req = if hop.keep_body {
                    self.client
                        .request(method.clone(), hop.url)
                        .multipart(build_form()?)
                } else {
                    self.client.get(hop.url)
                };
                if hop.apply_query && !query.is_empty() {
                    req = req.query(query);
                }
                Ok(req)
            })
            .await?;
        self.require_success(resp).await
    }

    /// [`Self::send_multipart`] for requests whose response body matters.
    pub(crate) async fn send_multipart_text(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json_parts: &[(&str, String)],
        text_parts: &[(&str, String)],
        file_part: Option<(&str, String, Vec<u8>)>,
    ) -> Result<String, Error> {
        let resp = self
            .send_multipart_response(method, url, query, json_parts, text_parts, file_part)
            .await?;
        resp.text()
            .await
            .map(|body| body.trim().to_string())
            .map_err(|e| Error::Admin(AdminError::Request(e)))
    }

    /// Discards the response body; see [`Self::send_multipart_text`] to keep it.
    pub(crate) async fn send_multipart(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json_parts: &[(&str, String)],
        text_parts: &[(&str, String)],
        file_part: Option<(&str, String, Vec<u8>)>,
    ) -> Result<(), Error> {
        self.send_multipart_response(method, url, query, json_parts, text_parts, file_part)
            .await
            .map(|_| ())
    }

    /// Sends a request and returns the raw response body bytes.
    ///
    /// Used for package download, whose body is an opaque archive.
    pub(crate) async fn send_bytes(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<u8>, Error> {
        let resp = self
            .send(method, url, query, crate::admin::clusters::NO_BODY)
            .await?;
        let resp = self.require_success(resp).await?;
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| Error::Admin(AdminError::Request(e)))
    }

    fn decode<T: serde::de::DeserializeOwned>(body: &str) -> Result<T, Error> {
        // A 204 or a policy read with no override yields an empty body; `null` is
        // the JSON spelling serde understands for that.
        let text = if body.trim().is_empty() { "null" } else { body };
        serde_json::from_str(text).map_err(|e| {
            Error::Admin(AdminError::Decode(format!(
                "{e}; body was: {}",
                body.chars().take(512).collect::<String>()
            )))
        })
    }

    async fn require_success(&self, resp: reqwest::Response) -> Result<reqwest::Response, Error> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(Error::Admin(AdminError::from_status(status, body)))
    }

    /// Sends a request, following redirects explicitly.
    ///
    /// `request_for` must build a *fresh* request each time: a redirect is a new
    /// request to a new URL, and authentication is applied to each hop. Automatic
    /// redirects are disabled on the client precisely so this can happen — see the
    /// note in [`AdminClient::new`].
    ///
    /// The semantics match Java's `AsyncHttpConnector`, which also disables its
    /// library's redirect handling and implements this by hand: 307 and 308 replay
    /// the method and body verbatim, while 301, 302 and 303 re-issue a non-GET as a
    /// bodyless GET, as HTTP requires.
    async fn send_with_redirects<F>(
        &self,
        url: &str,
        mut request_for: F,
    ) -> Result<reqwest::Response, Error>
    where
        F: FnMut(Hop<'_>) -> Result<reqwest::RequestBuilder, Error>,
    {
        /// Matches the default reqwest would have applied.
        const MAX_REDIRECTS: usize = 10;

        let mut target = url.to_string();
        let mut hop = Hop {
            url,
            keep_body: true,
            apply_query: true,
        };

        for _ in 0..=MAX_REDIRECTS {
            let req = self
                .apply_auth(request_for(Hop {
                    url: &target,
                    ..hop
                })?)
                .await?;
            let resp = Self::on_tokio(async move {
                req.send()
                    .await
                    .map_err(|e| Error::Admin(AdminError::Request(e)))
            })
            .await?;

            let status = resp.status().as_u16();
            let keep_body = match status {
                307 | 308 => true,
                // HTTP says re-issue these as GET without the original body.
                301..=303 => false,
                _ => return Ok(resp),
            };

            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            // A redirect with no usable Location is not something to retry; hand it
            // back so the status mapper reports it.
            let Some(location) = location else {
                return Ok(resp);
            };
            // Pulsar sends an absolute URL, but Location may legally be relative.
            target = reqwest::Url::parse(&target)
                .and_then(|base| base.join(&location))
                .map_err(|e| {
                    Error::Admin(AdminError::Decode(format!(
                        "broker redirected to an unusable location {location:?}: {e}"
                    )))
                })?
                .to_string();
            // The caller's query belongs to the original URL only; `Location`
            // carries its own, so re-appending would duplicate every parameter.
            hop = Hop {
                url,
                keep_body,
                apply_query: false,
            };
        }
        Err(Error::Admin(AdminError::Decode(format!(
            "gave up after {MAX_REDIRECTS} redirects starting from {url}"
        ))))
    }

    async fn send(
        &self,
        method: reqwest::Method,
        url: &str,
        query: &[(&str, String)],
        json: Option<&(impl serde::Serialize + ?Sized)>,
    ) -> Result<reqwest::Response, Error> {
        self.send_with_redirects(url, |hop| {
            let mut req = if hop.keep_body {
                self.client.request(method.clone(), hop.url)
            } else {
                self.client.get(hop.url)
            };
            if hop.apply_query && !query.is_empty() {
                req = req.query(query);
            }
            if hop.keep_body {
                if let Some(body) = json {
                    req = req.json(body);
                }
            }
            Ok(req)
        })
        .await
    }

    /// Cluster administration.
    pub fn clusters(&self) -> clusters::Clusters<'_> {
        clusters::Clusters { client: self }
    }

    /// Tenant administration.
    pub fn tenants(&self) -> tenants::Tenants<'_> {
        tenants::Tenants { client: self }
    }

    /// Namespace administration and policies.
    pub fn namespaces(&self) -> namespaces::Namespaces<'_> {
        namespaces::Namespaces { client: self }
    }

    /// Broker inspection and dynamic configuration.
    pub fn brokers(&self) -> brokers::Brokers<'_> {
        brokers::Brokers { client: self }
    }

    /// Bookie rack placement.
    pub fn bookies(&self) -> bookies::Bookies<'_> {
        bookies::Bookies { client: self }
    }

    /// Resource group administration.
    pub fn resource_groups(&self) -> resource_groups::ResourceGroups<'_> {
        resource_groups::ResourceGroups { client: self }
    }

    /// Namespace-bundle resource quotas.
    pub fn resource_quotas(&self) -> resource_quotas::ResourceQuotas<'_> {
        resource_quotas::ResourceQuotas { client: self }
    }

    /// Topic-level policy overrides.
    pub fn topic_policies(&self) -> topic_policies::TopicPolicies<'_> {
        topic_policies::TopicPolicies {
            client: self,
            is_global: false,
        }
    }

    /// Topic-level policy overrides in the **geo-replicated** policy set.
    ///
    /// Java spells this `topicPolicies(true)`. Global policies live in a separate
    /// store from the cluster-local ones this client's
    /// [`topic_policies`][Self::topic_policies] reaches, and replicate to every
    /// cluster in the namespace: a value set here is invisible to a local read, and
    /// vice versa.
    pub fn topic_policies_global(&self) -> topic_policies::TopicPolicies<'_> {
        topic_policies::TopicPolicies {
            client: self,
            is_global: true,
        }
    }

    /// Topic lifecycle, subscriptions, cursors and statistics.
    pub fn topics(&self) -> topics::Topics<'_> {
        topics::Topics { client: self }
    }

    /// Non-persistent topic administration.
    pub fn non_persistent_topics(&self) -> non_persistent_topics::NonPersistentTopics<'_> {
        non_persistent_topics::NonPersistentTopics { client: self }
    }

    /// Schema registry administration.
    pub fn schemas(&self) -> schemas::Schemas<'_> {
        schemas::Schemas { client: self }
    }

    /// Broker diagnostic dumps.
    pub fn broker_stats(&self) -> broker_stats::BrokerStats<'_> {
        broker_stats::BrokerStats { client: self }
    }

    /// Topic lookup over HTTP.
    pub fn lookup(&self) -> lookup::Lookup<'_> {
        lookup::Lookup { client: self }
    }

    /// Transaction coordinator observability.
    pub fn transactions(&self) -> transactions::Transactions<'_> {
        transactions::Transactions { client: self }
    }

    /// Pulsar 5.0 scalable topic (`topic://`) administration.
    pub fn scalable_topics(&self) -> scalable_topics::ScalableTopics<'_> {
        scalable_topics::ScalableTopics { client: self }
    }

    /// Pulsar Function management.
    pub fn functions(&self) -> functions::Functions<'_> {
        functions::Functions { client: self }
    }

    /// Sink connector management.
    pub fn sinks(&self) -> sinks::Sinks<'_> {
        sinks::Sinks { client: self }
    }

    /// Source connector management.
    pub fn sources(&self) -> sources::Sources<'_> {
        sources::Sources { client: self }
    }

    /// Package repository management.
    pub fn packages(&self) -> packages::Packages<'_> {
        packages::Packages { client: self }
    }

    /// Function-worker cluster inspection.
    pub fn worker(&self) -> worker::Worker<'_> {
        worker::Worker { client: self }
    }

    /// Pulsar proxy statistics.
    pub fn proxy_stats(&self) -> proxy_stats::ProxyStats<'_> {
        proxy_stats::ProxyStats { client: self }
    }

    /// Metadata-store migration control.
    pub fn metadata_migration(&self) -> metadata_migration::MetadataMigration<'_> {
        metadata_migration::MetadataMigration { client: self }
    }

    /// Sets the maximum number of unacknowledged messages allowed per consumer
    /// on a topic.
    ///
    /// This is a persistent broker-side topic policy. The topic must already
    /// exist when this is called (subscribe a consumer first, then call this).
    /// Requires `topicLevelPoliciesEnabled=true` in the broker configuration.
    pub async fn set_max_unacked_messages_on_consumer(
        &self,
        topic: &str,
        max_unacked: u32,
    ) -> Result<(), Error> {
        let url = self.topic_policy_url(topic, "maxUnackedMessagesOnConsumer")?;
        let body = max_unacked.to_string();
        let resp = self
            .send_with_redirects(&url, |hop| {
                Ok(if hop.keep_body {
                    self.client
                        .post(hop.url)
                        .header("Content-Type", "application/json")
                        .body(body.clone())
                } else {
                    self.client.get(hop.url)
                })
            })
            .await?;
        self.check_response(resp).await
    }

    /// Removes the per-topic max unacked messages override, reverting to the
    /// broker or namespace default.
    ///
    /// To disable the limit without removing the topic-level override, call
    /// [`set_max_unacked_messages_on_consumer`][Self::set_max_unacked_messages_on_consumer]
    /// with a value of `0` (unlimited).
    pub async fn remove_max_unacked_messages_on_consumer(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_policy_url(topic, "maxUnackedMessagesOnConsumer")?;
        let resp = self
            .send_with_redirects(&url, |hop| {
                Ok(if hop.keep_body {
                    self.client.delete(hop.url)
                } else {
                    self.client.get(hop.url)
                })
            })
            .await?;
        self.check_response(resp).await
    }

    async fn get_schema_with_version(
        &self,
        topic: &str,
        version: Option<u64>,
    ) -> Result<Option<Schema>, Error> {
        let url = self.schema_url(topic, version)?;
        let resp = self
            .send_with_redirects(&url, |hop| Ok(self.client.get(hop.url)))
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        // Map through the same taxonomy as everything else; this used to return a
        // generic `Http` for 401/403/409/412/503.
        let resp = self.require_success(resp).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        parse_schema_response(&body).map(Some)
    }

    /// Gets the latest schema registered for a topic through the Pulsar Admin
    /// HTTP API.
    pub async fn get_schema(&self, topic: &str) -> Result<Option<Schema>, Error> {
        self.get_schema_with_version(topic, None).await
    }

    /// Gets a specific schema version registered for a topic through the Pulsar
    /// Admin HTTP API.
    pub async fn get_schema_at_version(
        &self,
        topic: &str,
        version: u64,
    ) -> Result<Option<Schema>, Error> {
        self.get_schema_with_version(topic, Some(version)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topic_persistent() {
        let (scheme, tenant, ns, name) =
            parse_topic("persistent://my-tenant/my-namespace/my-topic").unwrap();
        assert_eq!(scheme, "persistent");
        assert_eq!(tenant, "my-tenant");
        assert_eq!(ns, "my-namespace");
        assert_eq!(name, "my-topic");
    }

    #[test]
    fn test_parse_topic_non_persistent() {
        let (scheme, tenant, ns, name) = parse_topic("non-persistent://tenant/ns/topic").unwrap();
        assert_eq!(scheme, "non-persistent");
        assert_eq!(tenant, "tenant");
        assert_eq!(ns, "ns");
        assert_eq!(name, "topic");
    }

    #[test]
    fn test_parse_topic_bare() {
        // No prefix defaults to persistent://
        let (scheme, tenant, ns, name) = parse_topic("tenant/ns/topic").unwrap();
        assert_eq!(scheme, "persistent");
        assert_eq!(tenant, "tenant");
        assert_eq!(ns, "ns");
        assert_eq!(name, "topic");
    }

    #[test]
    fn test_parse_topic_missing_parts() {
        assert!(parse_topic("").is_err());
        assert!(parse_topic("tenant").is_err());
        assert!(parse_topic("tenant/ns").is_err());
        // trailing slash = empty topic name
        assert!(parse_topic("tenant/ns/").is_err());
        assert!(parse_topic("persistent://").is_err());
        assert!(parse_topic("persistent://tenant").is_err());
        assert!(parse_topic("persistent://tenant/ns").is_err());
        assert!(parse_topic("persistent://tenant/ns/").is_err());
    }

    #[test]
    fn test_topic_policy_url() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .topic_policy_url(
                    "persistent://public/default/my-topic",
                    "maxUnackedMessagesOnConsumer"
                )
                .unwrap(),
            "http://localhost:8080/admin/v2/persistent/public/default/my-topic/maxUnackedMessagesOnConsumer"
        );
    }

    #[test]
    fn test_schema_url_latest() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .schema_url("persistent://public/default/my-topic", None)
                .unwrap(),
            "http://localhost:8080/admin/v2/schemas/public/default/my-topic/schema"
        );
    }

    #[test]
    fn test_schema_url_version() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .schema_url("public/default/my-topic", Some(7))
                .unwrap(),
            "http://localhost:8080/admin/v2/schemas/public/default/my-topic/schema/7"
        );
    }

    #[test]
    fn test_schema_url_encodes_local_name() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .schema_url("persistent://public/default/topic?key#frag ment", None)
                .unwrap(),
            "http://localhost:8080/admin/v2/schemas/public/default/topic%3Fkey%23frag%20ment/schema"
        );
    }

    #[test]
    fn test_parse_schema_response() {
        let body = r#"{
            "version": 3,
            "type": "JSON",
            "timestamp": 1234,
            "data": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[]}",
            "properties": {"k": "v"}
        }"#;

        let schema = parse_schema_response(body).unwrap();

        assert_eq!(
            schema.r#type,
            crate::message::proto::schema::Type::Json as i32
        );
        assert_eq!(
            schema.schema_data,
            b"{\"type\":\"record\",\"name\":\"User\",\"fields\":[]}"
        );
        assert_eq!(schema.properties.len(), 1);
        assert_eq!(schema.properties[0].key, "k");
        assert_eq!(schema.properties[0].value, "v");
    }

    #[test]
    fn test_parse_key_value_schema_response_converts_data() {
        let body = r#"{
            "version": 1,
            "type": "KEY_VALUE",
            "timestamp": 1234,
            "data": "{\"key\":{\"type\":\"record\",\"name\":\"Key\",\"fields\":[]},\"value\":\"\"}",
            "properties": {"kv.encoding.type": "SEPARATED"}
        }"#;

        let schema = parse_schema_response(body).unwrap();

        assert_eq!(
            schema.r#type,
            crate::message::proto::schema::Type::KeyValue as i32
        );
        let key_schema = br#"{"fields":[],"name":"Key","type":"record"}"#;
        let mut expected = Vec::new();
        expected.extend_from_slice(&(key_schema.len() as u32).to_be_bytes());
        expected.extend_from_slice(key_schema);
        expected.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(schema.schema_data, expected);
        assert_eq!(schema.properties[0].key, "kv.encoding.type");
        assert_eq!(schema.properties[0].value, "SEPARATED");
    }

    #[test]
    fn test_admin_url_trailing_slash_stripped() {
        // Trailing slash on admin_url should be normalized away
        let tls = TlsOptions::default();
        let client = AdminClient::new("http://localhost:8080/".to_string(), &tls, None).unwrap();
        assert_eq!(client.admin_url, "http://localhost:8080");
    }
}
