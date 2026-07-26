//! Package repository management — `/admin/v3/packages`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Packages`. The repository stores
//! function and connector archives so they can be referenced by name instead of
//! re-uploaded, e.g. `function://public/default/my-fn@v1`.
//!
//! Requires `enablePackagesManagement=true` on the broker; otherwise every
//! endpoint answers 503.

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{PackageMetadata, PackageType},
        AdminClient,
    },
    Error,
};

/// A parsed package name: `{type}://{tenant}/{namespace}/{name}@{version}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageName {
    pub package_type: PackageType,
    pub tenant: String,
    pub namespace: String,
    pub name: String,
    pub version: String,
}

impl PackageName {
    /// Parses `function://tenant/namespace/name@version`.
    ///
    /// The version defaults to `latest` when `@version` is omitted, matching the
    /// Java client.
    pub fn parse(package_name: &str) -> Result<Self, Error> {
        let invalid = |why: &str| {
            Error::Admin(crate::error::AdminError::InvalidTopic(format!(
                "invalid package name {package_name:?}: {why}"
            )))
        };
        let (scheme, rest) = package_name
            .split_once("://")
            .ok_or_else(|| invalid("expected {type}://{tenant}/{namespace}/{name}"))?;
        // Java compares the type case-insensitively (`PackageType.getEnum` uses
        // `equalsIgnoreCase`), so `FUNCTION://` is a valid name there.
        let package_type = match scheme.to_ascii_lowercase().as_str() {
            "function" => PackageType::Function,
            "sink" => PackageType::Sink,
            "source" => PackageType::Source,
            other => return Err(invalid(&format!("unknown package type {other:?}"))),
        };
        // Exactly one version separator: Java splits on every `@` and rejects the
        // name unless that yields two parts, so `name@v1@extra` is invalid rather
        // than parsing as version `v1@extra`.
        let mut at_parts = rest.split('@');
        let path = at_parts.next().unwrap_or_default();
        let version = match (at_parts.next(), at_parts.next()) {
            (_, Some(_)) => return Err(invalid("expected at most one '@' before the version")),
            (Some(v), None) if !v.is_empty() => v.to_string(),
            _ => "latest".to_string(),
        };
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
            return Err(invalid("expected exactly tenant/namespace/name"));
        }
        Ok(PackageName {
            package_type,
            tenant: parts[0].to_string(),
            namespace: parts[1].to_string(),
            name: parts[2].to_string(),
            version,
        })
    }

    /// The REST path segments this name maps to, without the version.
    fn segments(&self) -> Vec<String> {
        vec![
            self.package_type.as_str().to_string(),
            encode_segment(&self.tenant),
            encode_segment(&self.namespace),
            encode_segment(&self.name),
        ]
    }
}

/// Handle for the `packages` group of admin operations.
///
/// Obtained from [`AdminClient::packages`].
pub struct Packages<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Packages<'_> {
    fn packages_url(&self, segments: &[&str]) -> String {
        let mut url = format!("{}/admin/v3/packages", self.client.admin_url());
        for segment in segments {
            url.push('/');
            url.push_str(segment);
        }
        url
    }

    /// `/packages/{type}/{tenant}/{namespace}/{name}/{version}/...`
    fn versioned(&self, name: &PackageName, extra: &[&str]) -> String {
        let owned = name.segments();
        let version = encode_segment(&name.version);
        let mut all: Vec<&str> = owned.iter().map(String::as_str).collect();
        all.push(&version);
        all.extend_from_slice(extra);
        self.packages_url(&all)
    }

    /// Lists the package names of one type in a namespace.
    pub async fn list_packages(
        &self,
        package_type: PackageType,
        namespace: &str,
    ) -> Result<Vec<String>, Error> {
        let (tenant, ns) = crate::admin::split_namespace(namespace)?;
        let url = self.packages_url(&[
            package_type.as_str(),
            &encode_segment(tenant),
            &encode_segment(ns),
        ]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Lists the versions of one package.
    pub async fn list_package_versions(&self, package_name: &str) -> Result<Vec<String>, Error> {
        let name = PackageName::parse(package_name)?;
        let owned = name.segments();
        let all: Vec<&str> = owned.iter().map(String::as_str).collect();
        let url = self.packages_url(&all);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets a package version's metadata.
    pub async fn get_metadata(&self, package_name: &str) -> Result<PackageMetadata, Error> {
        let name = PackageName::parse(package_name)?;
        let url = self.versioned(&name, &["metadata"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Replaces a package version's metadata.
    pub async fn update_metadata(
        &self,
        package_name: &str,
        metadata: &PackageMetadata,
    ) -> Result<(), Error> {
        let name = PackageName::parse(package_name)?;
        let url = self.versioned(&name, &["metadata"]);
        self.client
            .send_empty(Method::PUT, &url, &[], Some(metadata))
            .await
    }

    /// Uploads a package version.
    pub async fn upload(
        &self,
        package_name: &str,
        metadata: &PackageMetadata,
        filename: &str,
        contents: Vec<u8>,
    ) -> Result<(), Error> {
        let name = PackageName::parse(package_name)?;
        let url = self.versioned(&name, &[]);
        let json = serde_json::to_string(metadata)
            .map_err(|e| Error::Custom(format!("could not serialize PackageMetadata: {e}")))?;
        self.client
            .send_multipart(
                Method::POST,
                &url,
                &[],
                &[("metadata", json)],
                &[],
                Some(("file", filename.to_string(), contents)),
            )
            .await
    }

    /// Downloads a package version's contents.
    pub async fn download(&self, package_name: &str) -> Result<Vec<u8>, Error> {
        let name = PackageName::parse(package_name)?;
        let url = self.versioned(&name, &[]);
        self.client.send_bytes(Method::GET, &url, &[]).await
    }

    /// Deletes a package version.
    pub async fn delete(&self, package_name: &str) -> Result<(), Error> {
        let name = PackageName::parse(package_name)?;
        let url = self.versioned(&name, &[]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both boundaries of Java's parser: the type is compared case-insensitively,
    /// and exactly one `@` is allowed.
    #[test]
    fn package_name_matches_the_reference_parser_at_both_boundaries() {
        let upper = PackageName::parse("FUNCTION://tenant/ns/name@v1")
            .expect("Java's PackageType.getEnum is case-insensitive, so this is valid");
        assert_eq!(upper.package_type, PackageType::Function);
        assert_eq!(upper.version, "v1");

        assert!(
            PackageName::parse("function://tenant/ns/name@v1@extra").is_err(),
            "Java splits on every '@' and requires exactly two parts"
        );
    }

    /// Package names must parse into the exact REST segments, including the
    /// implicit `latest` version.
    #[test]
    fn package_names_parse() {
        let name = PackageName::parse("function://public/default/my-fn@v1").unwrap();
        assert_eq!(name.package_type, PackageType::Function);
        assert_eq!(name.tenant, "public");
        assert_eq!(name.namespace, "default");
        assert_eq!(name.name, "my-fn");
        assert_eq!(name.version, "v1");

        // An omitted version means `latest`, as in the Java client.
        let name = PackageName::parse("sink://t/ns/s").unwrap();
        assert_eq!(name.package_type, PackageType::Sink);
        assert_eq!(name.version, "latest");

        assert_eq!(
            PackageName::parse("source://t/ns/s@2")
                .unwrap()
                .package_type,
            PackageType::Source
        );
    }

    #[test]
    fn malformed_package_names_are_rejected() {
        for bad in [
            "",
            "my-fn",
            "function://",
            "function://only-tenant",
            "function://t/ns",
            "function://t/ns/n/extra",
            "unknown://t/ns/n",
            "function:///ns/n",
        ] {
            assert!(
                PackageName::parse(bad).is_err(),
                "{bad:?} should not parse as a package name"
            );
        }
    }
}
