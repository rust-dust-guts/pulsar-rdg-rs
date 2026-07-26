//! Schema registry administration — `/admin/v2/schemas`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Schemas`.

use reqwest::Method;
use serde::Deserialize;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{IsCompatibilityResponse, PostSchemaPayload, SchemaInfo, SchemaMetadata},
        parse_topic, AdminClient,
    },
    Error,
};

/// The `schemas` endpoint wraps its listing in an object rather than returning a
/// bare array.
#[derive(Deserialize)]
struct AllSchemasResponse {
    #[serde(default, rename = "getSchemaResponses")]
    get_schema_responses: Vec<SchemaInfo>,
}

/// Handle for the `schemas` group of admin operations.
///
/// Obtained from [`AdminClient::schemas`].
pub struct Schemas<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Schemas<'_> {
    /// Builds `/admin/v2/schemas/{tenant}/{namespace}/{topic}/...`.
    ///
    /// The schema endpoints omit the `persistent`/`non-persistent` domain that
    /// every other topic path carries.
    fn schema_url(&self, topic: &str, extra: &[&str]) -> Result<String, Error> {
        let (_, tenant, namespace, name) = parse_topic(topic)?;
        let (tenant, namespace, name) = (
            encode_segment(tenant),
            encode_segment(namespace),
            encode_segment(name),
        );
        let mut all: Vec<&str> = vec!["schemas", &tenant, &namespace, &name];
        all.extend_from_slice(extra);
        Ok(self.client.url(&all))
    }

    /// Gets the topic's latest schema, or `None` if it has none.
    pub async fn get_schema_info(&self, topic: &str) -> Result<Option<SchemaInfo>, Error> {
        let url = self.schema_url(topic, &["schema"])?;
        self.client
            .send_json_absent_on_404(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Gets one specific schema version, or `None` if it does not exist.
    pub async fn get_schema_info_at_version(
        &self,
        topic: &str,
        version: i64,
    ) -> Result<Option<SchemaInfo>, Error> {
        let version = version.to_string();
        let url = self.schema_url(topic, &["schema", &version])?;
        self.client
            .send_json_absent_on_404(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Gets every schema version registered for the topic, oldest first.
    pub async fn get_all_schemas(&self, topic: &str) -> Result<Vec<SchemaInfo>, Error> {
        let url = self.schema_url(topic, &["schemas"])?;
        let resp: AllSchemasResponse = self
            .client
            .send_json(Method::GET, &url, &[], NO_BODY)
            .await?;
        Ok(resp.get_schema_responses)
    }

    /// Gets where the topic's schema history is stored.
    pub async fn get_schema_metadata(&self, topic: &str) -> Result<SchemaMetadata, Error> {
        let url = self.schema_url(topic, &["metadata"])?;
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Registers a new schema version.
    ///
    /// Fails if the payload is incompatible with the topic's configured
    /// compatibility strategy.
    pub async fn create_schema(
        &self,
        topic: &str,
        payload: &PostSchemaPayload,
    ) -> Result<(), Error> {
        let url = self.schema_url(topic, &["schema"])?;
        self.client
            .send_empty(Method::POST, &url, &[], Some(payload))
            .await
    }

    /// Deletes the topic's schema.
    ///
    /// `force` deletes even when the schema is still in use.
    pub async fn delete_schema(&self, topic: &str, force: bool) -> Result<(), Error> {
        let url = self.schema_url(topic, &["schema"])?;
        self.client
            .send_empty(
                Method::DELETE,
                &url,
                &[("force", force.to_string())],
                NO_BODY,
            )
            .await
    }

    /// Tests whether `payload` would be accepted for this topic.
    ///
    /// A compatible schema answers 200; an incompatible one is reported by the
    /// broker as an error rather than as `is_compatible: false`.
    pub async fn test_compatibility(
        &self,
        topic: &str,
        payload: &PostSchemaPayload,
    ) -> Result<IsCompatibilityResponse, Error> {
        let url = self.schema_url(topic, &["compatibility"])?;
        self.client
            .send_json(Method::POST, &url, &[], Some(payload))
            .await
    }

    /// Gets the version a schema was registered under, if it is registered.
    ///
    /// The payload is the same body [`create_schema`][Self::create_schema] takes, so a
    /// caller can ask "which version is this exact schema?" without scanning
    /// every version.
    pub async fn get_version_by_schema(
        &self,
        topic: &str,
        payload: &PostSchemaPayload,
    ) -> Result<i64, Error> {
        let url = self.schema_url(topic, &["version"])?;
        #[derive(serde::Deserialize)]
        struct VersionResponse {
            version: i64,
        }
        let response: VersionResponse = self
            .client
            .send_json(Method::POST, &url, &[], Some(payload))
            .await?;
        Ok(response.version)
    }
}
