#[cfg(test)]
use crate::{client::Pulsar, executor::TokioExecutor};

/// Binary-protocol URL of the broker the integration tests run against.
///
/// Override with `PULSAR_BROKER_URL` to point the suite at a broker on a
/// non-default port; defaults to the standalone address CI publishes. Never has
/// a trailing slash, which the URL parser would treat as an empty path.
#[cfg(test)]
pub(crate) fn broker_url() -> String {
    std::env::var("PULSAR_BROKER_URL")
        .unwrap_or_else(|_| "pulsar://127.0.0.1:6650".to_string())
        .trim_end_matches('/')
        .to_string()
}

/// Admin REST base URL of the Pulsar **proxy**, when one is in the topology.
///
/// `proxy-stats` is served by a proxy rather than a broker, so those tests skip
/// when this is unset. `scripts/start_test_broker.sh` exports it.
///
/// Gated on `admin-api` alongside its only callers, or it is dead code in the
/// feature sets that exclude them.
#[cfg(all(test, feature = "admin-api"))]
pub(crate) fn proxy_admin_url() -> Option<String> {
    std::env::var("PULSAR_PROXY_ADMIN_URL")
        .ok()
        .map(|u| u.trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty())
}

/// Binary-protocol URL of the Pulsar proxy, when one is in the topology.
#[cfg(all(test, feature = "admin-api"))]
pub(crate) fn proxy_broker_url() -> Option<String> {
    std::env::var("PULSAR_PROXY_URL")
        .ok()
        .map(|u| u.trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty())
}

/// Admin REST base URL of the broker the integration tests run against.
///
/// Override with `PULSAR_ADMIN_URL`; defaults to the standalone address CI
/// publishes. Never has a trailing slash.
#[cfg(test)]
pub(crate) fn admin_url() -> String {
    std::env::var("PULSAR_ADMIN_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
        .trim_end_matches('/')
        .to_string()
}

/// Wrapper for the Tokio executor
#[cfg(any(
    feature = "tokio-runtime",
    feature = "tokio-rustls-runtime-aws-lc-rs",
    feature = "tokio-rustls-runtime-ring"
))]
#[cfg(test)]
pub async fn new_pulsar() -> Pulsar<TokioExecutor> {
    use log::LevelFilter;

    use crate::tests::TEST_LOGGER;

    let _result = log::set_logger(&TEST_LOGGER);
    log::set_max_level(LevelFilter::Debug);

    Pulsar::builder(broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap()
}

#[cfg(test)]
pub(crate) async fn create_partitioned_topic(
    tenant: &str,
    namespace: &str,
    topic_name: &str,
    num_partitions: u32,
) {
    use reqwest::Client;

    let create_partitioned_topic_url = format!(
        "{}/admin/v2/persistent/{tenant}/{namespace}/{topic_name}/partitions",
        admin_url()
    );
    let client = Client::new();
    let response = client
        .put(create_partitioned_topic_url)
        .json(&num_partitions.to_string())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
}
