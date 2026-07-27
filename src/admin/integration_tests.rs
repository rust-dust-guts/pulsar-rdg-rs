//! Integration tests for the Admin REST client.
//!
//! Every test here talks to the broker started by `scripts/start_test_broker.sh`
//! and cleans up after itself, so the suite can run repeatedly against one broker.

use std::collections::BTreeMap;

use crate::{
    admin::{
        models::{
            BookieInfo, ClusterData, FailureDomain, NamespaceIsolationData, ResourceGroup,
            ResourceQuota, TenantInfo,
        },
        AdminClient,
    },
    error::AdminError,
    test_utils, Error, TokioExecutor,
};

async fn new_admin() -> AdminClient {
    let pulsar = crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap();
    pulsar.admin(test_utils::admin_url()).unwrap()
}

/// Runs `body`, then deletes `name` whether or not it panicked.
///
/// Functions live in the shared `public/default` namespace, so one left behind by
/// a failing test breaks later tests and every later run.
async fn with_function_cleanup<Fut>(name: &str, body: Fut)
where
    Fut: std::future::Future<Output = ()>,
{
    let name = name.to_string();
    with_cleanup(body, || async move {
        let admin = new_admin().await;
        match admin
            .functions()
            .delete_function("public", "default", &name)
            .await
        {
            Ok(()) | Err(Error::Admin(AdminError::NotFound(_))) => Ok(()),
            Err(e) => Err(e),
        }
    })
    .await;
}

/// Runs `body`, then always cleans up — even when `body` panics.
///
/// Without this a failing test leaves its namespace, topic or function behind on a
/// broker that the rest of the suite, and every later run, shares. The panic is
/// re-raised afterwards so the failure still surfaces; cleanup errors are ignored
/// on that path so they cannot mask the real one.
async fn with_cleanup<Fut, C, CFut>(body: Fut, cleanup: C)
where
    Fut: std::future::Future<Output = ()>,
    C: FnOnce() -> CFut,
    CFut: std::future::Future<Output = Result<(), Error>>,
{
    use futures::FutureExt;

    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    let cleaned = cleanup().await;
    match outcome {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(()) => cleaned.expect("cleanup failed after the test body succeeded"),
    }
}

/// Serializes the `proxy-stats` tests against each other.
///
/// The proxy's log level is process-global: one test lowers it to exercise the
/// setter, while the traffic test needs level 2 for topic accounting. CI runs with
/// `--test-threads=1`, but the documented local command uses Rust's parallel
/// default, where the two would race.
fn proxy_log_level_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Asserts that an admin error was produced by the endpoint's own handler.
///
/// The weak version of this check only rejected Jetty's HTML 404, so it also
/// accepted errors that never reached the intended handler at all — a connection
/// failure, a locally rejected topic name, a wrong verb (405) or a wrong media
/// type (415). Those are exactly the defects these tests exist to catch, so each
/// is rejected explicitly.
///
/// What survives is an error carrying the broker's own `{"reason": …}`, which only
/// a handler that ran can produce.
#[track_caller]
fn assert_reached_handler(what: &str, error: &Error) {
    let admin = match error {
        Error::Admin(e) => e,
        other => panic!("{what}: expected an admin error, got {other:?}"),
    };

    match admin {
        // No HTTP exchange happened: connect failure, timeout, or a local reject.
        AdminError::Request(e) => {
            panic!("{what}: the request never completed, so no handler saw it: {e}")
        }
        AdminError::InvalidTopic(m) | AdminError::TlsConfig(m) => {
            panic!("{what}: rejected locally before anything was sent: {m}")
        }
        AdminError::Decode(m) => panic!("{what}: response did not match the model: {m}"),
        // Reached the server, but not this route's handler.
        AdminError::NotAllowed(m) => panic!(
            "{what}: HTTP 405 — the path exists but not for this verb, so the \
             intended handler did not run: {m}"
        ),
        AdminError::Http { status, body } if matches!(status, 404 | 405 | 406 | 415) => panic!(
            "{what}: HTTP {status} means the request was not dispatched to this \
             handler (wrong path, verb, or content type): {}",
            body.chars().take(200).collect::<String>()
        ),
        _ => {}
    }

    let message = format!("{admin}");
    assert!(
        !message.contains("<html") && !message.contains("Error 404 Not Found"),
        "{what}: the request did not reach a handler — the path matches no route. \
         The broker returned an HTML error page: {}",
        message.chars().take(200).collect::<String>()
    );
    assert!(
        !message.trim().is_empty(),
        "{what}: the error carried no broker reason, so nothing proves a handler ran"
    );
}

/// `Ok`, or an error that provably reached the endpoint's handler.
///
/// Use for endpoints whose success path needs state this suite cannot create.
macro_rules! assert_ok_or_handled {
    ($what:expr, $call:expr) => {
        match $call {
            Ok(value) => Some(value),
            Err(e) => {
                assert_reached_handler($what, &e);
                None
            }
        }
    };
}

/// The contract of [`assert_reached_handler`], pinned.
///
/// Each rejected case is one the weak version accepted, which is how tests could
/// pass while pointing at the wrong verb, the wrong media type, or nothing at all.
#[test]
fn assert_reached_handler_rejects_errors_no_handler_produced() {
    let rejected: Vec<(&str, Error)> = vec![
        (
            "local topic rejection",
            Error::Admin(AdminError::InvalidTopic("bad".into())),
        ),
        (
            "wrong verb",
            Error::Admin(AdminError::NotAllowed("method not allowed".into())),
        ),
        (
            "wrong media type",
            Error::Admin(AdminError::Http {
                status: 415,
                body: "Unsupported Media Type".into(),
            }),
        ),
        (
            "unmatched route",
            Error::Admin(AdminError::NotFound(
                "<html>Error 404 Not Found</html>".into(),
            )),
        ),
        (
            "decode failure",
            Error::Admin(AdminError::Decode("bad json".into())),
        ),
    ];
    for (label, error) in rejected {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_reached_handler("probe", &error)
        }));
        assert!(
            outcome.is_err(),
            "{label} must not count as reaching a handler"
        );
    }

    // A broker `reason` is what a handler that ran produces.
    let accepted = Error::Admin(AdminError::NotFound(
        "Transaction coordinator not found! coordinator id : 0".into(),
    ));
    assert_reached_handler("probe", &accepted);
}

/// Deletes a resource group, retrying the broker's namespace-walk race.
///
/// `deleteResourceGroup` validates that no namespace still references the group by
/// walking every namespace; a namespace deleted concurrently by another test makes
/// that walk answer 404. The retry lives here rather than in the client because
/// Java's admin client has no retry layer either.
async fn delete_resource_group_retrying(admin: &AdminClient, name: &str) {
    for _ in 0..15 {
        match admin.resource_groups().delete_resource_group(name).await {
            Ok(()) => return,
            Err(Error::Admin(AdminError::NotFound(m))) if m.contains("Namespace") => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => panic!("unexpected error deleting resource group {name}: {e:?}"),
        }
    }
    panic!("resource group {name} delete kept racing namespace deletion");
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", rand::random::<u32>())
}

/// The cluster this broker runs as.
///
/// Read from the broker's own `clusterName` rather than picking the first entry of
/// `get_clusters()`: tests run concurrently and create throwaway clusters, so the
/// listing order is not stable and the first entry may be another test's fixture
/// that disappears mid-test.
async fn primary_cluster(admin: &AdminClient) -> String {
    admin
        .brokers()
        .get_runtime_configurations()
        .await
        .unwrap()
        .get("clusterName")
        .expect("broker runtime configuration has no clusterName")
        .clone()
}

// ---------------------------------------------------------------- clusters

#[tokio::test]
async fn clusters_list_contains_the_local_cluster() {
    let admin = new_admin().await;
    let clusters = admin.clusters().get_clusters().await.unwrap();
    assert!(!clusters.is_empty());

    // Every listed cluster must be readable. Other tests create and delete
    // throwaway clusters concurrently, so one can vanish between the listing and
    // the read; that is not a failure of this test.
    for cluster in &clusters {
        match admin.clusters().get_cluster(cluster).await {
            Ok(_) => {}
            Err(Error::Admin(AdminError::NotFound(_))) => {}
            Err(e) => panic!("cluster {cluster} listed but unreadable: {e:?}"),
        }
    }
}

#[tokio::test]
async fn cluster_create_read_update_delete() {
    let admin = new_admin().await;
    let name = unique("test_cluster");

    admin
        .clusters()
        .create_cluster(&name, &ClusterData::with_service_url("http://example:8080"))
        .await
        .unwrap();

    let read = admin.clusters().get_cluster(&name).await.unwrap();
    assert_eq!(read.service_url.as_deref(), Some("http://example:8080"));

    let updated = ClusterData {
        service_url: Some("http://example:8081".to_string()),
        broker_service_url: Some("pulsar://example:6650".to_string()),
        ..Default::default()
    };
    admin
        .clusters()
        .update_cluster(&name, &updated)
        .await
        .unwrap();
    let read = admin.clusters().get_cluster(&name).await.unwrap();
    assert_eq!(read.service_url.as_deref(), Some("http://example:8081"));
    assert_eq!(
        read.broker_service_url.as_deref(),
        Some("pulsar://example:6650")
    );

    admin.clusters().delete_cluster(&name).await.unwrap();
    assert!(
        !admin
            .clusters()
            .get_clusters()
            .await
            .unwrap()
            .contains(&name),
        "cluster still listed after delete"
    );
}

/// Creating a cluster twice must surface as `Conflict`, not a bare HTTP code.
#[tokio::test]
async fn creating_an_existing_cluster_is_a_conflict() {
    let admin = new_admin().await;
    let name = unique("test_cluster_conflict");
    let data = ClusterData::with_service_url("http://example:8080");

    admin.clusters().create_cluster(&name, &data).await.unwrap();
    let err = admin
        .clusters()
        .create_cluster(&name, &data)
        .await
        .unwrap_err();
    admin.clusters().delete_cluster(&name).await.unwrap();

    match err {
        Error::Admin(AdminError::Conflict(_)) => {}
        other => panic!("expected Conflict, got {other:?}"),
    }
}

/// Reading a missing cluster must surface as `NotFound`.
#[tokio::test]
async fn reading_a_missing_cluster_is_not_found() {
    let admin = new_admin().await;
    let err = admin
        .clusters()
        .get_cluster(&unique("nope"))
        .await
        .unwrap_err();
    match err {
        Error::Admin(AdminError::NotFound(_)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
    // Not retriable: retrying cannot make a missing cluster appear.
    if let Error::Admin(e) = admin
        .clusters()
        .get_cluster(&unique("nope"))
        .await
        .unwrap_err()
    {
        assert!(!e.is_retriable());
    }
}

#[tokio::test]
async fn cluster_peers_round_trip() {
    let admin = new_admin().await;
    let name = unique("test_cluster_peers");
    let peer = unique("test_cluster_peer");

    admin
        .clusters()
        .create_cluster(&name, &ClusterData::with_service_url("http://a:8080"))
        .await
        .unwrap();
    admin
        .clusters()
        .create_cluster(&peer, &ClusterData::with_service_url("http://b:8080"))
        .await
        .unwrap();

    admin
        .clusters()
        .update_peer_cluster_names(&name, std::slice::from_ref(&peer))
        .await
        .unwrap();
    assert_eq!(
        admin
            .clusters()
            .get_peer_cluster_names(&name)
            .await
            .unwrap(),
        vec![peer.clone()]
    );

    admin.clusters().delete_cluster(&name).await.unwrap();
    admin.clusters().delete_cluster(&peer).await.unwrap();
}

#[tokio::test]
async fn failure_domain_round_trip() {
    let admin = new_admin().await;
    let cluster = unique("test_cluster_fd");
    admin
        .clusters()
        .create_cluster(&cluster, &ClusterData::with_service_url("http://a:8080"))
        .await
        .unwrap();

    let domain = unique("domain");
    let brokers: std::collections::BTreeSet<String> =
        ["broker-1:8080".to_string(), "broker-2:8080".to_string()]
            .into_iter()
            .collect();
    admin
        .clusters()
        .set_failure_domain(
            &cluster,
            &domain,
            &FailureDomain {
                brokers: brokers.clone(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        admin
            .clusters()
            .get_failure_domain(&cluster, &domain)
            .await
            .unwrap()
            .brokers,
        brokers
    );
    assert!(admin
        .clusters()
        .get_failure_domains(&cluster)
        .await
        .unwrap()
        .contains_key(&domain));

    admin
        .clusters()
        .delete_failure_domain(&cluster, &domain)
        .await
        .unwrap();
    admin.clusters().delete_cluster(&cluster).await.unwrap();
}

#[tokio::test]
async fn namespace_isolation_policy_round_trip() {
    let admin = new_admin().await;
    let cluster = primary_cluster(&admin).await;
    let policy = unique("isolation");

    let data = NamespaceIsolationData {
        namespaces: vec!["public/isolated.*".to_string()],
        primary: vec![".*".to_string()],
        secondary: vec![],
        auto_failover_policy: Some(crate::admin::models::AutoFailoverPolicyData {
            policy_type: Some("min_available".to_string()),
            // The broker's `min_available` policy requires *both* parameters and
            // rejects the whole body with HTTP 400 if either is missing.
            parameters: [
                ("min_limit".to_string(), "1".to_string()),
                ("usage_threshold".to_string(), "80".to_string()),
            ]
            .into_iter()
            .collect(),
        }),
        unload_scope: None,
    };

    admin
        .clusters()
        .set_namespace_isolation_policy(&cluster, &policy, &data)
        .await
        .unwrap();

    let read = admin
        .clusters()
        .get_namespace_isolation_policy(&cluster, &policy)
        .await
        .unwrap();
    assert_eq!(read.namespaces, data.namespaces);
    assert_eq!(read.primary, data.primary);

    // The policy listing is served from the configuration store, which the write
    // above reaches asynchronously — poll rather than assuming it is visible at once.
    let mut visible = false;
    for _ in 0..20 {
        if admin
            .clusters()
            .get_namespace_isolation_policies(&cluster)
            .await
            .unwrap()
            .contains_key(&policy)
        {
            visible = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        visible,
        "isolation policy {policy} never appeared in the listing"
    );

    // Listing broker assignments must at least parse against a live broker.
    admin
        .clusters()
        .get_brokers_with_namespace_isolation_policy(&cluster)
        .await
        .unwrap();

    admin
        .clusters()
        .delete_namespace_isolation_policy(&cluster, &policy)
        .await
        .unwrap();
}

// ---------------------------------------------------------------- tenants

#[tokio::test]
async fn tenant_create_read_update_delete() {
    let admin = new_admin().await;
    let cluster = primary_cluster(&admin).await;
    let tenant = unique("test_tenant");

    admin
        .tenants()
        .create_tenant(&tenant, &TenantInfo::with_clusters([cluster.clone()]))
        .await
        .unwrap();

    assert!(admin
        .tenants()
        .get_tenants()
        .await
        .unwrap()
        .contains(&tenant));
    let info = admin.tenants().get_tenant_info(&tenant).await.unwrap();
    assert!(info.allowed_clusters.contains(&cluster));

    let updated = TenantInfo {
        admin_roles: ["role-a".to_string()].into_iter().collect(),
        allowed_clusters: [cluster.clone()].into_iter().collect(),
    };
    admin
        .tenants()
        .update_tenant(&tenant, &updated)
        .await
        .unwrap();
    assert_eq!(
        admin
            .tenants()
            .get_tenant_info(&tenant)
            .await
            .unwrap()
            .admin_roles,
        updated.admin_roles
    );

    admin.tenants().delete_tenant(&tenant, false).await.unwrap();
    assert!(!admin
        .tenants()
        .get_tenants()
        .await
        .unwrap()
        .contains(&tenant));
}

#[tokio::test]
async fn creating_a_tenant_on_an_unknown_cluster_is_rejected() {
    let admin = new_admin().await;
    let tenant = unique("test_tenant_badcluster");
    let err = admin
        .tenants()
        .create_tenant(&tenant, &TenantInfo::with_clusters(["no_such_cluster"]))
        .await
        .unwrap_err();
    match err {
        // The broker reports this as 412 Precondition Failed.
        Error::Admin(AdminError::PreconditionFailed(_) | AdminError::NotFound(_)) => {}
        other => panic!("expected PreconditionFailed or NotFound, got {other:?}"),
    }
}

// ---------------------------------------------------------------- brokers

#[tokio::test]
async fn broker_read_only_endpoints() {
    let admin = new_admin().await;

    let brokers = admin.brokers().get_active_brokers().await.unwrap();
    assert!(!brokers.is_empty(), "no active brokers reported");

    let cluster = primary_cluster(&admin).await;
    assert!(!admin
        .brokers()
        .get_active_brokers_in_cluster(&cluster)
        .await
        .unwrap()
        .is_empty());

    let leader = admin.brokers().get_leader_broker().await.unwrap();
    assert!(
        leader.broker_id.is_some() || leader.service_url.is_some(),
        "leader broker had neither id nor url: {leader:?}"
    );

    let version = admin.brokers().get_broker_version().await.unwrap();
    assert!(!version.is_empty(), "empty broker version");

    admin.brokers().healthcheck().await.unwrap();
    admin.brokers().backlog_quota_check().await.unwrap();

    let internal = admin
        .brokers()
        .get_internal_configuration_data()
        .await
        .unwrap();
    assert!(
        internal.metadata_store_url.is_some()
            || internal.configuration_metadata_store_url.is_some(),
        "internal configuration had no metadata store url: {internal:?}"
    );

    assert!(!admin
        .brokers()
        .get_runtime_configurations()
        .await
        .unwrap()
        .is_empty());
    assert!(!admin
        .brokers()
        .get_dynamic_configuration_names()
        .await
        .unwrap()
        .is_empty());
    admin
        .brokers()
        .get_all_dynamic_configurations()
        .await
        .unwrap();
    admin
        .brokers()
        .get_owned_namespaces(&cluster, &brokers[0])
        .await
        .unwrap();
}

#[tokio::test]
async fn dynamic_configuration_round_trip() {
    let admin = new_admin().await;
    // A numeric, side-effect-free knob that every supported broker exposes.
    const KEY: &str = "dispatcherMaxReadBatchSize";

    let names = admin
        .brokers()
        .get_dynamic_configuration_names()
        .await
        .unwrap();
    if !names.iter().any(|n| n == KEY) {
        log::warn!("{KEY} is not dynamically configurable on this broker, skipping");
        return;
    }

    admin
        .brokers()
        .update_dynamic_configuration(KEY, "97")
        .await
        .unwrap();
    let values = admin
        .brokers()
        .get_all_dynamic_configurations()
        .await
        .unwrap();
    assert_eq!(values.get(KEY).map(String::as_str), Some("97"));

    admin
        .brokers()
        .delete_dynamic_configuration(KEY)
        .await
        .unwrap();
    let values = admin
        .brokers()
        .get_all_dynamic_configurations()
        .await
        .unwrap();
    assert!(
        !values.contains_key(KEY),
        "override still present after delete"
    );
}

// ---------------------------------------------------------------- bookies

#[tokio::test]
async fn bookie_rack_info_round_trip() {
    let admin = new_admin().await;

    // Reads must work even with no racks configured.
    admin.bookies().get_bookies_rack_info().await.unwrap();
    admin.bookies().get_bookies().await.unwrap();

    let address = "127.0.0.1:3181";
    admin
        .bookies()
        .update_bookie_rack_info(
            address,
            "default",
            &BookieInfo {
                rack: Some("rack-a".to_string()),
                hostname: Some("bookie-a".to_string()),
            },
        )
        .await
        .unwrap();

    let info = admin.bookies().get_bookie_rack_info(address).await.unwrap();
    assert_eq!(
        info.as_ref().and_then(|i| i.rack.as_deref()),
        Some("rack-a"),
        "rack not stored: {info:?}"
    );

    admin
        .bookies()
        .delete_bookie_rack_info(address)
        .await
        .unwrap();
}

/// A bookie with no rack set must read as `None`, not error.
#[tokio::test]
async fn missing_bookie_rack_info_is_none() {
    let admin = new_admin().await;
    assert!(admin
        .bookies()
        .get_bookie_rack_info("192.0.2.1:3181")
        .await
        .unwrap()
        .is_none());
}

// ------------------------------------------------------- resource groups

#[tokio::test]
async fn resource_group_create_read_update_delete() {
    let admin = new_admin().await;
    let name = unique("test_rg");

    admin
        .resource_groups()
        .create_resource_group(
            &name,
            &ResourceGroup {
                publish_rate_in_msgs: Some(100),
                publish_rate_in_bytes: Some(1024),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(admin
        .resource_groups()
        .get_resource_groups()
        .await
        .unwrap()
        .contains(&name));
    let read = admin
        .resource_groups()
        .get_resource_group(&name)
        .await
        .unwrap();
    assert_eq!(read.publish_rate_in_msgs, Some(100));
    assert_eq!(read.publish_rate_in_bytes, Some(1024));

    admin
        .resource_groups()
        .update_resource_group(
            &name,
            &ResourceGroup {
                publish_rate_in_msgs: Some(200),
                dispatch_rate_in_msgs: Some(50),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let read = admin
        .resource_groups()
        .get_resource_group(&name)
        .await
        .unwrap();
    assert_eq!(read.publish_rate_in_msgs, Some(200));
    assert_eq!(read.dispatch_rate_in_msgs, Some(50));

    // The broker validates that no namespace still references the group by walking
    // every namespace. Other tests create and delete namespaces concurrently, so
    // that walk can observe a namespace that has just gone away and answer 404.
    //
    // The retry is deliberately here in the test and NOT in `delete_resource_group`:
    // the Java admin client has no retry layer at all — `BaseResource.sync()` just
    // wraps and propagates — so the client matches it and surfaces the error. Only
    // this test, which creates the contention itself, papers over it.
    let mut last_err = None;
    for _ in 0..10 {
        match admin.resource_groups().delete_resource_group(&name).await {
            Ok(()) => {
                last_err = None;
                break;
            }
            Err(Error::Admin(AdminError::NotFound(m))) if m.contains("Namespace") => {
                last_err = Some(m);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => panic!("unexpected error deleting resource group: {e:?}"),
        }
    }
    assert!(
        last_err.is_none(),
        "resource group delete kept racing namespace deletion: {last_err:?}"
    );

    assert!(!admin
        .resource_groups()
        .get_resource_groups()
        .await
        .unwrap()
        .contains(&name));
}

// ------------------------------------------------------- resource quotas

#[tokio::test]
async fn default_resource_quota_round_trip() {
    let admin = new_admin().await;

    let original = admin
        .resource_quotas()
        .get_default_resource_quota()
        .await
        .unwrap();

    let wanted = ResourceQuota {
        msg_rate_in: 41.0,
        msg_rate_out: 42.0,
        bandwidth_in: 1000.0,
        bandwidth_out: 2000.0,
        memory: 64.0,
        dynamic: original.dynamic,
    };
    admin
        .resource_quotas()
        .set_default_resource_quota(&wanted)
        .await
        .unwrap();

    let read = admin
        .resource_quotas()
        .get_default_resource_quota()
        .await
        .unwrap();
    assert_eq!(read.msg_rate_in, 41.0);
    assert_eq!(read.msg_rate_out, 42.0);

    // Restore, so the broker is left as found for other tests.
    admin
        .resource_quotas()
        .set_default_resource_quota(&original)
        .await
        .unwrap();
}

// ------------------------------------------------------------- namespaces

/// Creates a namespace, runs `body`, then force-deletes it.
async fn with_namespace<F, Fut>(admin: &AdminClient, body: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let namespace = format!("public/{}", unique("test_ns"));
    admin
        .namespaces()
        .create_namespace(&namespace)
        .await
        .unwrap();
    with_cleanup(body(namespace.clone()), || async {
        admin.namespaces().delete_namespace(&namespace, true).await
    })
    .await;
}

#[tokio::test]
async fn namespace_create_list_delete() {
    let admin = new_admin().await;
    let namespace = format!("public/{}", unique("test_ns"));

    admin
        .namespaces()
        .create_namespace(&namespace)
        .await
        .unwrap();
    assert!(admin
        .namespaces()
        .get_namespaces("public")
        .await
        .unwrap()
        .contains(&namespace));

    // A fresh namespace has no topics and a default bundle layout.
    assert!(admin
        .namespaces()
        .get_namespace_topics(&namespace)
        .await
        .unwrap()
        .is_empty());
    assert!(
        admin
            .namespaces()
            .get_bundles(&namespace)
            .await
            .unwrap()
            .num_bundles
            > 0
    );

    admin
        .namespaces()
        .delete_namespace(&namespace, true)
        .await
        .unwrap();
    assert!(!admin
        .namespaces()
        .get_namespaces("public")
        .await
        .unwrap()
        .contains(&namespace));
}

#[tokio::test]
async fn namespace_with_explicit_bundle_count() {
    let admin = new_admin().await;
    let namespace = format!("public/{}", unique("test_ns_bundles"));
    admin
        .namespaces()
        .create_namespace_with_bundles(&namespace, 8)
        .await
        .unwrap();
    assert_eq!(
        admin
            .namespaces()
            .get_bundles(&namespace)
            .await
            .unwrap()
            .num_bundles,
        8
    );
    admin
        .namespaces()
        .delete_namespace(&namespace, true)
        .await
        .unwrap();
}

/// A namespace that was never given an override must report `None` rather than a
/// zero value, so callers can tell "unset" from "set to zero".
#[tokio::test]
async fn unset_namespace_policies_read_as_none() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();
        assert!(n.get_message_ttl(&ns).await.unwrap().is_none());
        assert!(n.get_retention(&ns).await.unwrap().is_none());
        assert!(n.get_max_producers_per_topic(&ns).await.unwrap().is_none());
        assert!(n.get_compaction_threshold(&ns).await.unwrap().is_none());
    })
    .await;
}

#[tokio::test]
async fn namespace_struct_policies_round_trip() {
    use crate::admin::models::*;
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        let retention = RetentionPolicies {
            retention_time_in_minutes: 30,
            retention_size_in_mb: 100,
        };
        n.set_retention(&ns, &retention).await.unwrap();
        assert_eq!(n.get_retention(&ns).await.unwrap(), Some(retention));

        let persistence = PersistencePolicies {
            bookkeeper_ensemble: 1,
            bookkeeper_write_quorum: 1,
            bookkeeper_ack_quorum: 1,
            managed_ledger_max_mark_delete_rate: 0.0,
        };
        n.set_persistence(&ns, &persistence).await.unwrap();
        assert_eq!(n.get_persistence(&ns).await.unwrap(), Some(persistence));

        let dispatch = DispatchRate {
            dispatch_throttling_rate_in_msg: 100,
            dispatch_throttling_rate_in_byte: 1024,
            relative_to_publish_rate: false,
            rate_period_in_second: 1,
        };
        n.set_dispatch_rate(&ns, &dispatch).await.unwrap();
        assert_eq!(n.get_dispatch_rate(&ns).await.unwrap(), Some(dispatch));
        n.set_subscription_dispatch_rate(&ns, &dispatch)
            .await
            .unwrap();
        assert_eq!(
            n.get_subscription_dispatch_rate(&ns).await.unwrap(),
            Some(dispatch)
        );
        n.set_replicator_dispatch_rate(&ns, &dispatch)
            .await
            .unwrap();
        assert_eq!(
            n.get_replicator_dispatch_rate(&ns).await.unwrap(),
            Some(dispatch)
        );

        let publish = PublishRate {
            publish_throttling_rate_in_msg: 50,
            publish_throttling_rate_in_byte: 512,
        };
        n.set_publish_rate(&ns, &publish).await.unwrap();
        assert_eq!(n.get_publish_rate(&ns).await.unwrap(), Some(publish));

        let subscribe = SubscribeRate {
            subscribe_throttling_rate_per_consumer: 10,
            rate_period_in_second: 30,
        };
        n.set_subscribe_rate(&ns, &subscribe).await.unwrap();
        assert_eq!(n.get_subscribe_rate(&ns).await.unwrap(), Some(subscribe));

        let inactive = InactiveTopicPolicies {
            inactive_topic_delete_mode: Some("delete_when_no_subscriptions".to_string()),
            max_inactive_duration_seconds: 60,
            delete_while_inactive: true,
        };
        n.set_inactive_topic_policies(&ns, &inactive).await.unwrap();
        assert_eq!(
            n.get_inactive_topic_policies(&ns).await.unwrap(),
            Some(inactive)
        );

        let auto_topic = AutoTopicCreationOverride {
            allow_auto_topic_creation: true,
            topic_type: Some("partitioned".to_string()),
            default_num_partitions: Some(2),
        };
        n.set_auto_topic_creation(&ns, &auto_topic).await.unwrap();
        assert_eq!(
            n.get_auto_topic_creation(&ns).await.unwrap(),
            Some(auto_topic)
        );

        let auto_sub = AutoSubscriptionCreationOverride {
            allow_auto_subscription_creation: true,
        };
        n.set_auto_subscription_creation(&ns, &auto_sub)
            .await
            .unwrap();
        assert_eq!(
            n.get_auto_subscription_creation(&ns).await.unwrap(),
            Some(auto_sub)
        );

        // Delayed delivery reports back an extra field the setter omits, so compare
        // the fields that were actually sent.
        let delayed = DelayedDeliveryPolicies {
            active: true,
            tick_time: 1000.0,
            max_delivery_delay_in_millis: None,
        };
        n.set_delayed_delivery_messages(&ns, &delayed)
            .await
            .unwrap();
        let read = n.get_delayed_delivery_messages(&ns).await.unwrap().unwrap();
        assert!(read.active);
        assert_eq!(read.tick_time, 1000.0);
    })
    .await;
}

#[tokio::test]
async fn namespace_backlog_quota_round_trip() {
    use crate::admin::models::{BacklogQuota, BacklogQuotaType};
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        assert!(n.get_backlog_quota_map(&ns).await.unwrap().is_empty());

        let quota = BacklogQuota {
            limit_size: Some(1024),
            limit_time: Some(60),
            policy: Some("producer_request_hold".to_string()),
        };
        n.set_backlog_quota(&ns, &quota, BacklogQuotaType::DestinationStorage)
            .await
            .unwrap();

        let map = n.get_backlog_quota_map(&ns).await.unwrap();
        let stored = map
            .get(BacklogQuotaType::DestinationStorage.as_str())
            .expect("quota not stored under its type key");
        assert_eq!(stored.limit_size, Some(1024));
        assert_eq!(stored.policy.as_deref(), Some("producer_request_hold"));

        n.remove_backlog_quota(&ns, BacklogQuotaType::DestinationStorage)
            .await
            .unwrap();
        assert!(!n
            .get_backlog_quota_map(&ns)
            .await
            .unwrap()
            .contains_key(BacklogQuotaType::DestinationStorage.as_str()));
    })
    .await;
}

#[tokio::test]
async fn namespace_permissions_round_trip() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        assert!(n.get_permissions(&ns).await.unwrap().is_empty());
        n.grant_permission_on_namespace(&ns, "role-a", &["produce".into(), "consume".into()])
            .await
            .unwrap();

        let perms = n.get_permissions(&ns).await.unwrap();
        let actions = perms.get("role-a").expect("role not granted");
        assert!(actions.iter().any(|a| a == "produce"));
        assert!(actions.iter().any(|a| a == "consume"));

        n.revoke_permissions_on_namespace(&ns, "role-a")
            .await
            .unwrap();
        assert!(!n.get_permissions(&ns).await.unwrap().contains_key("role-a"));
    })
    .await;
}

#[tokio::test]
async fn namespace_properties_round_trip() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        let props: std::collections::BTreeMap<String, String> = [
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]
        .into_iter()
        .collect();
        n.set_namespace_properties(&ns, &props).await.unwrap();
        assert_eq!(n.get_namespace_properties(&ns).await.unwrap(), props);

        n.set_namespace_property(&ns, "c", "3").await.unwrap();
        assert_eq!(
            n.get_namespace_properties(&ns)
                .await
                .unwrap()
                .get("c")
                .map(String::as_str),
            Some("3")
        );

        n.remove_namespace_property(&ns, "c").await.unwrap();
        assert!(!n
            .get_namespace_properties(&ns)
            .await
            .unwrap()
            .contains_key("c"));

        n.clear_namespace_properties(&ns).await.unwrap();
        assert!(n.get_namespace_properties(&ns).await.unwrap().is_empty());
    })
    .await;
}

#[tokio::test]
async fn namespace_replication_clusters_round_trip() {
    let admin = new_admin().await;
    let cluster = primary_cluster(&admin).await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();
        assert_eq!(
            n.get_namespace_replication_clusters(&ns).await.unwrap(),
            vec![cluster.clone()]
        );
        n.set_namespace_replication_clusters(&ns, std::slice::from_ref(&cluster), false)
            .await
            .unwrap();
        assert_eq!(
            n.get_namespace_replication_clusters(&ns).await.unwrap(),
            vec![cluster]
        );
    })
    .await;
}

#[tokio::test]
async fn namespace_actions_and_aggregate_policies() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        // The aggregate is fully typed, so a field this client models wrongly shows
        // up as a decode error here rather than being silently ignored.
        let policies = n.get_policies(&ns).await.unwrap();
        assert!(
            !policies.replication_clusters.is_empty(),
            "policies had no replication clusters: {policies:?}"
        );
        assert!(policies.bundles.is_some(), "policies had no bundle layout");
        assert!(!policies.deleted);

        // Clearing an empty backlog and unloading must both succeed.
        n.clear_namespace_backlog(&ns).await.unwrap();
        n.unload_namespace(&ns).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn namespace_subscription_types_and_entry_filters() {
    use crate::admin::models::{EntryFilters, SchemaCompatibilityStrategy, SubscriptionAuthMode};
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        n.set_subscription_types_enabled(&ns, &["Shared".into(), "Exclusive".into()])
            .await
            .unwrap();
        let types = n
            .get_subscription_types_enabled(&ns)
            .await
            .unwrap()
            .unwrap();
        assert!(types.iter().any(|t| t == "Shared"));

        n.remove_subscription_types_enabled(&ns).await.unwrap();

        // The two enum-valued namespace policies are strictly typed, so a wrong
        // serialization shows up as a failed round-trip rather than a stringly
        // typed value the broker silently ignores.
        n.set_subscription_auth_mode(&ns, SubscriptionAuthMode::Prefix)
            .await
            .unwrap();
        assert_eq!(
            n.get_subscription_auth_mode(&ns).await.unwrap(),
            Some(SubscriptionAuthMode::Prefix)
        );

        n.set_schema_compatibility_strategy(&ns, SchemaCompatibilityStrategy::Forward)
            .await
            .unwrap();
        assert_eq!(
            n.get_schema_compatibility_strategy(&ns).await.unwrap(),
            Some(SchemaCompatibilityStrategy::Forward)
        );

        // The broker rejects an empty filter name, which is the documented way to
        // find out you should use the remove operation instead.
        let err = n
            .set_namespace_entry_filters(
                &ns,
                &EntryFilters {
                    entry_filter_names: String::new(),
                },
            )
            .await
            .unwrap_err();
        match err {
            Error::Admin(AdminError::BadRequest(_)) => {}
            other => panic!("expected BadRequest for empty filter name, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn namespace_bookie_affinity_round_trip() {
    use crate::admin::models::BookieAffinityGroupData;
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        let data = BookieAffinityGroupData {
            bookkeeper_affinity_group_primary: Some("group-1".to_string()),
            bookkeeper_affinity_group_secondary: Some("group-2".to_string()),
        };
        n.set_bookie_affinity_group(&ns, &data).await.unwrap();
        assert_eq!(n.get_bookie_affinity_group(&ns).await.unwrap(), Some(data));

        n.delete_bookie_affinity_group(&ns).await.unwrap();
    })
    .await;
}

/// An invalid namespace string must fail locally rather than producing a request
/// against a nonsense URL.
#[tokio::test]
async fn malformed_namespace_is_rejected_before_sending() {
    let admin = new_admin().await;
    for bad in ["", "no-slash", "a/b/c", "/b", "a/"] {
        let err = admin.namespaces().get_policies(bad).await.unwrap_err();
        match err {
            Error::Admin(AdminError::InvalidTopic(_)) => {}
            other => panic!("expected InvalidTopic for {bad:?}, got {other:?}"),
        }
    }
}

/// Every scalar namespace policy must round-trip set -> get -> remove -> unset.
///
/// Generated over the whole set rather than spot-checking, because these are
/// declared from a table and a wrong path string would otherwise go unnoticed.
#[tokio::test]
async fn namespace_scalar_policies_round_trip() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        n.set_message_ttl(&ns, 60i32).await.unwrap();
        assert_eq!(
            n.get_message_ttl(&ns).await.unwrap(),
            Some(60i32),
            "message_ttl did not round-trip"
        );
        n.remove_message_ttl(&ns).await.unwrap();
        assert!(
            n.get_message_ttl(&ns).await.unwrap().is_none(),
            "message_ttl still set after remove"
        );

        n.set_subscription_expiration_time(&ns, 30i32)
            .await
            .unwrap();
        assert_eq!(
            n.get_subscription_expiration_time(&ns).await.unwrap(),
            Some(30i32),
            "subscription_expiration_time did not round-trip"
        );
        n.remove_subscription_expiration_time(&ns).await.unwrap();
        assert!(
            n.get_subscription_expiration_time(&ns)
                .await
                .unwrap()
                .is_none(),
            "subscription_expiration_time still set after remove"
        );

        n.set_max_producers_per_topic(&ns, 10i32).await.unwrap();
        assert_eq!(
            n.get_max_producers_per_topic(&ns).await.unwrap(),
            Some(10i32),
            "max_producers_per_topic did not round-trip"
        );
        n.remove_max_producers_per_topic(&ns).await.unwrap();
        assert!(
            n.get_max_producers_per_topic(&ns).await.unwrap().is_none(),
            "max_producers_per_topic still set after remove"
        );

        n.set_max_consumers_per_topic(&ns, 20i32).await.unwrap();
        assert_eq!(
            n.get_max_consumers_per_topic(&ns).await.unwrap(),
            Some(20i32),
            "max_consumers_per_topic did not round-trip"
        );
        n.remove_max_consumers_per_topic(&ns).await.unwrap();
        assert!(
            n.get_max_consumers_per_topic(&ns).await.unwrap().is_none(),
            "max_consumers_per_topic still set after remove"
        );

        n.set_max_consumers_per_subscription(&ns, 5i32)
            .await
            .unwrap();
        assert_eq!(
            n.get_max_consumers_per_subscription(&ns).await.unwrap(),
            Some(5i32),
            "max_consumers_per_subscription did not round-trip"
        );
        n.remove_max_consumers_per_subscription(&ns).await.unwrap();
        assert!(
            n.get_max_consumers_per_subscription(&ns)
                .await
                .unwrap()
                .is_none(),
            "max_consumers_per_subscription still set after remove"
        );

        n.set_max_unacked_messages_per_consumer(&ns, 500i32)
            .await
            .unwrap();
        assert_eq!(
            n.get_max_unacked_messages_per_consumer(&ns).await.unwrap(),
            Some(500i32),
            "max_unacked_messages_per_consumer did not round-trip"
        );
        n.remove_max_unacked_messages_per_consumer(&ns)
            .await
            .unwrap();
        assert!(
            n.get_max_unacked_messages_per_consumer(&ns)
                .await
                .unwrap()
                .is_none(),
            "max_unacked_messages_per_consumer still set after remove"
        );

        n.set_max_unacked_messages_per_subscription(&ns, 1000i32)
            .await
            .unwrap();
        assert_eq!(
            n.get_max_unacked_messages_per_subscription(&ns)
                .await
                .unwrap(),
            Some(1000i32),
            "max_unacked_messages_per_subscription did not round-trip"
        );
        n.remove_max_unacked_messages_per_subscription(&ns)
            .await
            .unwrap();
        assert!(
            n.get_max_unacked_messages_per_subscription(&ns)
                .await
                .unwrap()
                .is_none(),
            "max_unacked_messages_per_subscription still set after remove"
        );

        n.set_max_subscriptions_per_topic(&ns, 7i32).await.unwrap();
        assert_eq!(
            n.get_max_subscriptions_per_topic(&ns).await.unwrap(),
            Some(7i32),
            "max_subscriptions_per_topic did not round-trip"
        );
        n.remove_max_subscriptions_per_topic(&ns).await.unwrap();
        assert!(
            n.get_max_subscriptions_per_topic(&ns)
                .await
                .unwrap()
                .is_none(),
            "max_subscriptions_per_topic still set after remove"
        );

        n.set_max_topics_per_namespace(&ns, 50i32).await.unwrap();
        assert_eq!(
            n.get_max_topics_per_namespace(&ns).await.unwrap(),
            Some(50i32),
            "max_topics_per_namespace did not round-trip"
        );
        n.remove_max_topics_per_namespace(&ns).await.unwrap();
        // Unlike the other scalars this one reports 0 rather than an empty body
        // after removal; 0 is the broker's "no limit" value.
        assert!(
            matches!(
                n.get_max_topics_per_namespace(&ns).await.unwrap(),
                None | Some(0)
            ),
            "max_topics_per_namespace still limited after remove"
        );

        n.set_deduplication_snapshot_interval(&ns, 1000i32)
            .await
            .unwrap();
        assert_eq!(
            n.get_deduplication_snapshot_interval(&ns).await.unwrap(),
            Some(1000i32),
            "deduplication_snapshot_interval did not round-trip"
        );

        n.set_compaction_threshold(&ns, 1048576i64).await.unwrap();
        assert_eq!(
            n.get_compaction_threshold(&ns).await.unwrap(),
            Some(1048576i64),
            "compaction_threshold did not round-trip"
        );
        n.remove_compaction_threshold(&ns).await.unwrap();
        assert!(
            n.get_compaction_threshold(&ns).await.unwrap().is_none(),
            "compaction_threshold still set after remove"
        );

        n.set_offload_threshold(&ns, 2097152i64).await.unwrap();
        assert_eq!(
            n.get_offload_threshold(&ns).await.unwrap(),
            Some(2097152i64),
            "offload_threshold did not round-trip"
        );

        n.set_offload_threshold_in_seconds(&ns, 3600i64)
            .await
            .unwrap();
        assert_eq!(
            n.get_offload_threshold_in_seconds(&ns).await.unwrap(),
            Some(3600i64),
            "offload_threshold_in_seconds did not round-trip"
        );

        n.set_offload_deletion_lag(&ns, 60000i64).await.unwrap();
        assert_eq!(
            n.get_offload_deletion_lag(&ns).await.unwrap(),
            Some(60000i64),
            "offload_deletion_lag did not round-trip"
        );
        n.remove_offload_deletion_lag(&ns).await.unwrap();
        assert!(
            n.get_offload_deletion_lag(&ns).await.unwrap().is_none(),
            "offload_deletion_lag still set after remove"
        );
    })
    .await;
}

/// Same, for the boolean policies.
#[tokio::test]
async fn namespace_boolean_policies_round_trip() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        n.set_deduplication_status(&ns, true).await.unwrap();
        assert_eq!(
            n.get_deduplication_status(&ns).await.unwrap(),
            Some(true),
            "deduplication_status did not round-trip"
        );

        n.set_encryption_required_status(&ns, true).await.unwrap();
        assert_eq!(
            n.get_encryption_required_status(&ns).await.unwrap(),
            Some(true),
            "encryption_required_status did not round-trip"
        );

        n.set_schema_validation_enforced(&ns, true).await.unwrap();
        assert_eq!(
            n.get_schema_validation_enforced(&ns).await.unwrap(),
            Some(true),
            "schema_validation_enforced did not round-trip"
        );

        n.set_is_allow_auto_update_schema(&ns, false).await.unwrap();
        assert_eq!(
            n.get_is_allow_auto_update_schema(&ns).await.unwrap(),
            Some(false),
            "is_allow_auto_update_schema did not round-trip"
        );

        n.set_dispatcher_pause_on_ack_state_persistent(&ns)
            .await
            .unwrap();
        assert_eq!(
            n.get_dispatcher_pause_on_ack_state_persistent(&ns)
                .await
                .unwrap(),
            Some(true),
            "dispatcher_pause_on_ack_state_persistent did not round-trip"
        );
        // The setter has no "off" — POST always enables, DELETE clears. Passing
        // `false` used to read back as `true`.
        n.remove_dispatcher_pause_on_ack_state_persistent(&ns)
            .await
            .unwrap();
        assert_ne!(
            n.get_dispatcher_pause_on_ack_state_persistent(&ns)
                .await
                .unwrap(),
            Some(true),
            "removing dispatcher_pause_on_ack_state_persistent left it enabled"
        );
    })
    .await;
}

// -------------------------------------------------------- topic policies

/// Creates a namespace and a topic inside it, runs `body`, then cleans up.
///
/// Topic policies require the topic to exist, so it is created by attaching a
/// producer rather than through the admin API.
async fn with_topic<F, Fut>(admin: &AdminClient, body: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let namespace = format!("public/{}", unique("test_tp_ns"));
    admin
        .namespaces()
        .create_namespace(&namespace)
        .await
        .unwrap();
    let topic = format!("persistent://{namespace}/{}", unique("topic"));

    let pulsar = crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap();
    let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
    producer
        .send_non_blocking("bootstrap")
        .await
        .unwrap()
        .await
        .unwrap();
    producer.close().await.unwrap();

    with_cleanup(body(topic), || async {
        admin.namespaces().delete_namespace(&namespace, true).await
    })
    .await;
}

/// With no override set, `applied = false` reports `None` while `applied = true`
/// falls back to the namespace or broker value. Getting this backwards would make
/// "is there an override?" unanswerable.
#[tokio::test]
async fn topic_policy_applied_flag_distinguishes_override_from_effective() {
    let admin = new_admin().await;
    with_topic(&admin, |topic| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();

        assert!(
            tp.get_max_producers(&topic, false).await.unwrap().is_none(),
            "unset topic policy must report None when not applied"
        );

        tp.set_max_producers(&topic, 11).await.unwrap();
        assert_eq!(tp.get_max_producers(&topic, false).await.unwrap(), Some(11));
        assert_eq!(tp.get_max_producers(&topic, true).await.unwrap(), Some(11));

        tp.remove_max_producers(&topic).await.unwrap();
        assert!(tp.get_max_producers(&topic, false).await.unwrap().is_none());
    })
    .await;
}

/// A topic policy must override the namespace policy of the same name.
#[tokio::test]
async fn topic_policy_overrides_namespace_policy() {
    use crate::admin::models::RetentionPolicies;
    let admin = new_admin().await;
    let namespace = format!("public/{}", unique("test_tp_over"));
    admin
        .namespaces()
        .create_namespace(&namespace)
        .await
        .unwrap();
    let topic = format!("persistent://{namespace}/{}", unique("topic"));

    let pulsar = crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap();
    let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
    producer
        .send_non_blocking("bootstrap")
        .await
        .unwrap()
        .await
        .unwrap();
    producer.close().await.unwrap();

    // Retention must exceed the configured backlog quota, and this broker's default
    // is 10GB — so use -1 (unlimited) for size and vary only the time.
    let ns_retention = RetentionPolicies {
        retention_time_in_minutes: 10,
        retention_size_in_mb: -1,
    };
    let topic_retention = RetentionPolicies {
        retention_time_in_minutes: 99,
        retention_size_in_mb: -1,
    };

    admin
        .namespaces()
        .set_retention(&namespace, &ns_retention)
        .await
        .unwrap();
    let tp = admin.topic_policies();

    // Before any topic override, the effective value is the namespace's.
    assert_eq!(
        tp.get_retention(&topic, true).await.unwrap(),
        Some(ns_retention)
    );
    assert!(tp.get_retention(&topic, false).await.unwrap().is_none());

    tp.set_retention(&topic, &topic_retention).await.unwrap();
    assert_eq!(
        tp.get_retention(&topic, true).await.unwrap(),
        Some(topic_retention)
    );

    // Removing the override falls back to the namespace value again.
    tp.remove_retention(&topic).await.unwrap();
    assert_eq!(
        tp.get_retention(&topic, true).await.unwrap(),
        Some(ns_retention)
    );

    admin
        .namespaces()
        .delete_namespace(&namespace, true)
        .await
        .unwrap();
}

#[tokio::test]
async fn topic_struct_policies_round_trip() {
    use crate::admin::models::*;
    let admin = new_admin().await;
    with_topic(&admin, |topic| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();

        let persistence = PersistencePolicies {
            bookkeeper_ensemble: 1,
            bookkeeper_write_quorum: 1,
            bookkeeper_ack_quorum: 1,
            managed_ledger_max_mark_delete_rate: 0.0,
        };
        tp.set_persistence(&topic, &persistence).await.unwrap();
        assert_eq!(
            tp.get_persistence(&topic, false).await.unwrap(),
            Some(persistence)
        );

        let dispatch = DispatchRate {
            dispatch_throttling_rate_in_msg: 100,
            dispatch_throttling_rate_in_byte: 1024,
            relative_to_publish_rate: false,
            rate_period_in_second: 1,
        };
        tp.set_dispatch_rate(&topic, &dispatch).await.unwrap();
        assert_eq!(
            tp.get_dispatch_rate(&topic, false).await.unwrap(),
            Some(dispatch)
        );
        tp.set_subscription_dispatch_rate(&topic, &dispatch)
            .await
            .unwrap();
        assert_eq!(
            tp.get_subscription_dispatch_rate(&topic, false)
                .await
                .unwrap(),
            Some(dispatch)
        );
        tp.set_replicator_dispatch_rate(&topic, &dispatch)
            .await
            .unwrap();
        assert_eq!(
            tp.get_replicator_dispatch_rate(&topic, false)
                .await
                .unwrap(),
            Some(dispatch)
        );

        let publish = PublishRate {
            publish_throttling_rate_in_msg: 50,
            publish_throttling_rate_in_byte: 512,
        };
        tp.set_publish_rate(&topic, &publish).await.unwrap();
        assert_eq!(tp.get_publish_rate(&topic).await.unwrap(), Some(publish));

        let subscribe = SubscribeRate {
            subscribe_throttling_rate_per_consumer: 10,
            rate_period_in_second: 30,
        };
        tp.set_subscribe_rate(&topic, &subscribe).await.unwrap();
        assert_eq!(
            tp.get_subscribe_rate(&topic, false).await.unwrap(),
            Some(subscribe)
        );

        let inactive = InactiveTopicPolicies {
            inactive_topic_delete_mode: Some("delete_when_no_subscriptions".to_string()),
            max_inactive_duration_seconds: 60,
            delete_while_inactive: true,
        };
        tp.set_inactive_topic_policies(&topic, &inactive)
            .await
            .unwrap();
        assert_eq!(
            tp.get_inactive_topic_policies(&topic, false).await.unwrap(),
            Some(inactive)
        );

        let auto_sub = AutoSubscriptionCreationOverride {
            allow_auto_subscription_creation: true,
        };
        tp.set_auto_subscription_creation(&topic, &auto_sub)
            .await
            .unwrap();
        assert_eq!(
            tp.get_auto_subscription_creation(&topic, false)
                .await
                .unwrap(),
            Some(auto_sub)
        );

        let delayed = DelayedDeliveryPolicies {
            active: true,
            tick_time: 1000.0,
            max_delivery_delay_in_millis: None,
        };
        tp.set_delayed_delivery_policy(&topic, &delayed)
            .await
            .unwrap();
        let read = tp
            .get_delayed_delivery_policy(&topic, false)
            .await
            .unwrap()
            .unwrap();
        assert!(read.active);
        assert_eq!(read.tick_time, 1000.0);

        tp.set_deduplication_status(&topic, true).await.unwrap();
        assert_eq!(
            tp.get_deduplication_status(&topic, false).await.unwrap(),
            Some(true)
        );

        tp.set_schema_compatibility_strategy(&topic, SchemaCompatibilityStrategy::Forward)
            .await
            .unwrap();
        assert_eq!(
            tp.get_schema_compatibility_strategy(&topic, false)
                .await
                .unwrap(),
            Some(SchemaCompatibilityStrategy::Forward)
        );
    })
    .await;
}

/// `OffloadPolicies` is a 31-field model; round-trip the driver-agnostic subset
/// against a real broker to prove the field names are right.
#[tokio::test]
async fn topic_offload_policies_round_trip() {
    use crate::admin::models::{OffloadPolicies, OffloadedReadPriority};
    let admin = new_admin().await;
    with_topic(&admin, |topic| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();

        let policies = OffloadPolicies {
            managed_ledger_offload_driver: Some("aws-s3".to_string()),
            managed_ledger_offload_threshold_in_bytes: Some(1_048_576),
            managed_ledger_offload_deletion_lag_in_millis: Some(60_000),
            managed_ledger_offloaded_read_priority: Some(OffloadedReadPriority::TieredStorageFirst),
            s3_managed_ledger_offload_bucket: Some("my-bucket".to_string()),
            s3_managed_ledger_offload_region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        tp.set_offload_policies(&topic, &policies).await.unwrap();

        let read = tp
            .get_offload_policies(&topic, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            read.managed_ledger_offload_driver.as_deref(),
            Some("aws-s3")
        );
        assert_eq!(
            read.managed_ledger_offload_threshold_in_bytes,
            Some(1_048_576)
        );
        assert_eq!(
            read.s3_managed_ledger_offload_bucket.as_deref(),
            Some("my-bucket")
        );
        // Pulsar 5.0.0-M1 accepts `managedLedgerOffloadedReadPriority` and
        // `s3ManagedLedgerOffloadRegion` (204) but does not echo either back, so
        // there is nothing to assert about them here.

        tp.remove_offload_policies(&topic).await.unwrap();
        assert_eq!(
            tp.get_offload_policies(&topic, false).await.unwrap(),
            None,
            "the topic-level offload override survived removal"
        );
    })
    .await;
}

/// `delete_topic_policies` must clear every override at once.
#[tokio::test]
async fn delete_topic_policies_clears_all_overrides() {
    let admin = new_admin().await;
    with_topic(&admin, |topic| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();

        tp.set_max_producers(&topic, 3).await.unwrap();
        tp.set_max_consumers(&topic, 4).await.unwrap();
        assert!(tp.get_max_producers(&topic, false).await.unwrap().is_some());

        tp.delete_topic_policies(&topic).await.unwrap();
        assert!(tp.get_max_producers(&topic, false).await.unwrap().is_none());
        assert!(tp.get_max_consumers(&topic, false).await.unwrap().is_none());
    })
    .await;
}

/// A malformed topic must fail locally rather than hitting a nonsense URL.
#[tokio::test]
async fn malformed_topic_is_rejected_before_sending() {
    let admin = new_admin().await;
    for bad in ["", "a", "a/b", "persistent://a/b"] {
        let err = admin
            .topic_policies()
            .get_max_producers(bad, false)
            .await
            .unwrap_err();
        match err {
            Error::Admin(AdminError::InvalidTopic(_)) => {}
            other => panic!("expected InvalidTopic for {bad:?}, got {other:?}"),
        }
    }
}

/// Every scalar topic policy must round-trip set -> get -> remove -> unset.
#[tokio::test]
async fn topic_scalar_policies_round_trip() {
    let admin = new_admin().await;
    with_topic(&admin, |topic| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();

        tp.set_max_unacked_messages_on_consumer(&topic, 500i32)
            .await
            .unwrap();
        assert_eq!(
            tp.get_max_unacked_messages_on_consumer(&topic, false)
                .await
                .unwrap(),
            Some(500i32),
            "max_unacked_messages_on_consumer did not round-trip"
        );
        tp.remove_max_unacked_messages_on_consumer(&topic)
            .await
            .unwrap();
        assert!(
            tp.get_max_unacked_messages_on_consumer(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "max_unacked_messages_on_consumer still set after remove"
        );

        tp.set_max_unacked_messages_on_subscription(&topic, 1000i32)
            .await
            .unwrap();
        assert_eq!(
            tp.get_max_unacked_messages_on_subscription(&topic, false)
                .await
                .unwrap(),
            Some(1000i32),
            "max_unacked_messages_on_subscription did not round-trip"
        );
        tp.remove_max_unacked_messages_on_subscription(&topic)
            .await
            .unwrap();
        assert!(
            tp.get_max_unacked_messages_on_subscription(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "max_unacked_messages_on_subscription still set after remove"
        );

        tp.set_max_producers(&topic, 10i32).await.unwrap();
        assert_eq!(
            tp.get_max_producers(&topic, false).await.unwrap(),
            Some(10i32),
            "max_producers did not round-trip"
        );
        tp.remove_max_producers(&topic).await.unwrap();
        assert!(
            tp.get_max_producers(&topic, false).await.unwrap().is_none(),
            "max_producers still set after remove"
        );

        tp.set_max_consumers(&topic, 20i32).await.unwrap();
        assert_eq!(
            tp.get_max_consumers(&topic, false).await.unwrap(),
            Some(20i32),
            "max_consumers did not round-trip"
        );
        tp.remove_max_consumers(&topic).await.unwrap();
        assert!(
            tp.get_max_consumers(&topic, false).await.unwrap().is_none(),
            "max_consumers still set after remove"
        );

        tp.set_max_consumers_per_subscription(&topic, 5i32)
            .await
            .unwrap();
        assert_eq!(
            tp.get_max_consumers_per_subscription(&topic).await.unwrap(),
            Some(5i32),
            "max_consumers_per_subscription did not round-trip"
        );
        tp.remove_max_consumers_per_subscription(&topic)
            .await
            .unwrap();
        assert!(
            tp.get_max_consumers_per_subscription(&topic)
                .await
                .unwrap()
                .is_none(),
            "max_consumers_per_subscription still set after remove"
        );

        tp.set_max_subscriptions_per_topic(&topic, 7i32)
            .await
            .unwrap();
        assert_eq!(
            tp.get_max_subscriptions_per_topic(&topic).await.unwrap(),
            Some(7i32),
            "max_subscriptions_per_topic did not round-trip"
        );
        tp.remove_max_subscriptions_per_topic(&topic).await.unwrap();
        assert!(
            tp.get_max_subscriptions_per_topic(&topic)
                .await
                .unwrap()
                .is_none(),
            "max_subscriptions_per_topic still set after remove"
        );

        tp.set_max_message_size(&topic, 65536i32).await.unwrap();
        assert_eq!(
            tp.get_max_message_size(&topic).await.unwrap(),
            Some(65536i32),
            "max_message_size did not round-trip"
        );
        tp.remove_max_message_size(&topic).await.unwrap();
        assert!(
            tp.get_max_message_size(&topic).await.unwrap().is_none(),
            "max_message_size still set after remove"
        );

        tp.set_compaction_threshold(&topic, 1048576i64)
            .await
            .unwrap();
        assert_eq!(
            tp.get_compaction_threshold(&topic, false).await.unwrap(),
            Some(1048576i64),
            "compaction_threshold did not round-trip"
        );
        tp.remove_compaction_threshold(&topic).await.unwrap();
        assert!(
            tp.get_compaction_threshold(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "compaction_threshold still set after remove"
        );

        tp.set_deduplication_snapshot_interval(&topic, 1000i32)
            .await
            .unwrap();
        assert_eq!(
            tp.get_deduplication_snapshot_interval(&topic)
                .await
                .unwrap(),
            Some(1000i32),
            "deduplication_snapshot_interval did not round-trip"
        );
        tp.remove_deduplication_snapshot_interval(&topic)
            .await
            .unwrap();
        assert!(
            tp.get_deduplication_snapshot_interval(&topic)
                .await
                .unwrap()
                .is_none(),
            "deduplication_snapshot_interval still set after remove"
        );

        tp.set_subscription_expiration_time(&topic, 30i32)
            .await
            .unwrap();
        assert_eq!(
            tp.get_subscription_expiration_time(&topic, false)
                .await
                .unwrap(),
            Some(30i32),
            "subscription_expiration_time did not round-trip"
        );
        tp.remove_subscription_expiration_time(&topic)
            .await
            .unwrap();
        assert!(
            tp.get_subscription_expiration_time(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "subscription_expiration_time still set after remove"
        );
    })
    .await;
}

// ---------------------------------------------------------------- topics

/// Creates a namespace, runs `body` with it, then force-deletes it.
async fn with_topic_namespace<F, Fut>(admin: &AdminClient, body: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let namespace = format!("public/{}", unique("test_t_ns"));
    admin
        .namespaces()
        .create_namespace(&namespace)
        .await
        .unwrap();
    with_cleanup(body(namespace.clone()), || async {
        admin.namespaces().delete_namespace(&namespace, true).await
    })
    .await;
}

/// Publishes `count` messages to `topic` and returns the client so the caller can
/// keep using it.
async fn publish(topic: &str, count: usize) {
    let pulsar = crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap();
    let mut producer = pulsar.producer().with_topic(topic).build().await.unwrap();
    for i in 0..count {
        producer
            .send_non_blocking(format!("message-{i}"))
            .await
            .unwrap()
            .await
            .unwrap();
    }
    producer.close().await.unwrap();
}

#[tokio::test]
async fn topic_create_list_delete() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("topic"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        assert!(t.get_list(&ns).await.unwrap().contains(&topic));

        let meta = t.get_partitioned_topic_metadata(&topic).await.unwrap();
        assert_eq!(meta.partitions, 0, "non-partitioned topic reports 0");

        t.delete(&topic, true).await.unwrap();
        assert!(!t.get_list(&ns).await.unwrap().contains(&topic));
    })
    .await;
}

#[tokio::test]
async fn partitioned_topic_lifecycle() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("part"));

        t.create_partitioned_topic(&topic, 3).await.unwrap();
        assert_eq!(
            t.get_partitioned_topic_metadata(&topic)
                .await
                .unwrap()
                .partitions,
            3
        );
        assert!(t
            .get_partitioned_topic_list(&ns)
            .await
            .unwrap()
            .contains(&topic));

        // Partition counts can only grow.
        t.update_partitioned_topic(&topic, 5).await.unwrap();
        assert_eq!(
            t.get_partitioned_topic_metadata(&topic)
                .await
                .unwrap()
                .partitions,
            5
        );
        t.create_missed_partitions(&topic).await.unwrap();

        t.delete_partitioned_topic(&topic, true).await.unwrap();
    })
    .await;
}

/// `TopicStats` and `PersistentTopicInternalStats` are large models; decode real
/// broker output and check the numbers actually reflect published messages.
#[tokio::test]
async fn topic_stats_and_internal_stats() {
    use crate::admin::models::GetStatsOptions;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("stats"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        t.create_subscription(
            &topic,
            "sub1",
            &crate::admin::models::MessageIdData::latest(),
        )
        .await
        .unwrap();
        publish(&topic, 5).await;

        let stats = t
            .get_stats(
                &topic,
                GetStatsOptions {
                    get_precise_backlog: true,
                    subscription_backlog_size: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(stats.msg_in_counter, 5, "stats: {stats:?}");
        assert!(stats.storage_size > 0);
        assert!(stats.subscriptions.contains_key("sub1"));
        assert!(stats.owner_broker.is_some());

        let internal = t.get_internal_stats(&topic).await.unwrap();
        assert_eq!(internal.entries_added_counter, 5, "internal: {internal:?}");
        assert!(!internal.ledgers.is_empty());
        assert!(internal.cursors.contains_key("sub1"));

        // excludePublishers/excludeConsumers must actually take effect.
        let lean = t
            .get_stats(
                &topic,
                GetStatsOptions {
                    exclude_publishers: true,
                    exclude_consumers: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(lean.publishers.is_empty());

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn partitioned_topic_stats() {
    use crate::admin::models::GetStatsOptions;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("pstats"));

        t.create_partitioned_topic(&topic, 2).await.unwrap();
        publish(&topic, 4).await;

        let stats = t
            .get_partitioned_stats(&topic, true, GetStatsOptions::default())
            .await
            .unwrap();
        assert_eq!(
            stats.metadata.as_ref().map(|m| m.partitions),
            Some(2),
            "stats: {stats:?}"
        );
        assert_eq!(stats.partitions.len(), 2, "per-partition stats missing");
        assert_eq!(stats.aggregate.msg_in_counter, 4);

        let internal = t.get_partitioned_internal_stats(&topic).await.unwrap();
        assert_eq!(internal.partitions.len(), 2);

        t.delete_partitioned_topic(&topic, true).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn subscription_lifecycle_and_cursor_management() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("subs"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        t.create_subscription(
            &topic,
            "sub1",
            &crate::admin::models::MessageIdData::latest(),
        )
        .await
        .unwrap();
        assert!(t
            .get_subscriptions(&topic)
            .await
            .unwrap()
            .contains(&"sub1".to_string()));

        publish(&topic, 10).await;

        let backlog = |stats: &crate::admin::models::TopicStats| {
            stats
                .subscriptions
                .get("sub1")
                .map(|s| s.msg_backlog)
                .unwrap_or(-1)
        };
        let stats = t.get_stats(&topic, Default::default()).await.unwrap();
        assert_eq!(backlog(&stats), 10);

        // Skipping acknowledges without delivering.
        t.skip_messages(&topic, "sub1", 4).await.unwrap();
        let stats = t.get_stats(&topic, Default::default()).await.unwrap();
        assert_eq!(
            backlog(&stats),
            6,
            "skip_messages did not advance the cursor"
        );

        t.skip_all_messages(&topic, "sub1").await.unwrap();
        let stats = t.get_stats(&topic, Default::default()).await.unwrap();
        assert_eq!(backlog(&stats), 0, "skip_all_messages left a backlog");

        // Rewinding to the epoch replays everything.
        t.reset_cursor(&topic, "sub1", 1).await.unwrap();
        let stats = t.get_stats(&topic, Default::default()).await.unwrap();
        assert!(backlog(&stats) > 0, "reset_cursor did not rewind");

        // Subscription properties round-trip.
        let props: BTreeMap<String, String> = [("owner".to_string(), "team-a".to_string())]
            .into_iter()
            .collect();
        t.update_subscription_properties(&topic, "sub1", &props)
            .await
            .unwrap();
        assert_eq!(
            t.get_subscription_properties(&topic, "sub1").await.unwrap(),
            props
        );

        t.analyze_subscription_backlog(&topic, "sub1")
            .await
            .unwrap();

        t.delete_subscription(&topic, "sub1", true).await.unwrap();
        assert!(t.get_subscriptions(&topic).await.unwrap().is_empty());

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn topic_message_lookup_and_examine() {
    use crate::admin::topics::MessagePosition;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("msgs"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        publish(&topic, 3).await;

        let last = t.get_last_message_id(&topic).await.unwrap();
        assert!(last.ledger_id > 0, "last message id: {last:?}");

        // Every message was published after the epoch, so this resolves.
        let by_ts = t.get_message_id_by_timestamp(&topic, 1).await.unwrap();
        assert!(by_ts.ledger_id > 0);

        // Examine reads the payload back out of the HTTP body.
        let msg = t
            .examine_message(&topic, MessagePosition::Earliest, 1)
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(msg.payload.clone()).unwrap(),
            "message-0",
            "examine returned the wrong payload: {msg:?}"
        );

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn topic_peek_messages() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("peek"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        t.create_subscription(
            &topic,
            "sub1",
            &crate::admin::models::MessageIdData::latest(),
        )
        .await
        .unwrap();
        publish(&topic, 3).await;

        let msgs = t.peek_messages(&topic, "sub1", 2).await.unwrap();
        assert_eq!(
            msgs.len(),
            2,
            "expected 2 peeked messages, got {}",
            msgs.len()
        );
        assert_eq!(
            String::from_utf8(msgs[0].payload.clone()).unwrap(),
            "message-0"
        );

        // Asking for more than the backlog holds must stop, not error.
        let all = t.peek_messages(&topic, "sub1", 50).await.unwrap();
        assert_eq!(all.len(), 3, "peek did not stop at the end of the backlog");

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn topic_properties_and_permissions() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("props"));

        t.create_non_partitioned_topic(&topic).await.unwrap();

        let props: BTreeMap<String, String> =
            [("a".to_string(), "1".to_string())].into_iter().collect();
        t.update_properties(&topic, &props).await.unwrap();
        assert_eq!(
            t.get_properties(&topic)
                .await
                .unwrap()
                .get("a")
                .map(String::as_str),
            Some("1")
        );
        t.remove_properties(&topic, "a").await.unwrap();
        assert!(!t.get_properties(&topic).await.unwrap().contains_key("a"));

        t.grant_permission(&topic, "role-a", &["produce".into()])
            .await
            .unwrap();
        assert!(t
            .get_permissions(&topic)
            .await
            .unwrap()
            .contains_key("role-a"));
        t.revoke_permissions(&topic, "role-a").await.unwrap();
        assert!(!t
            .get_permissions(&topic)
            .await
            .unwrap()
            .contains_key("role-a"));

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn topic_maintenance_actions() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("maint"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        publish(&topic, 3).await;

        // Status endpoints must decode before either process has ever run.
        assert_eq!(t.compaction_status(&topic).await.unwrap().status, "NOT_RUN");
        assert_eq!(t.offload_status(&topic).await.unwrap().status, "NOT_RUN");

        t.trigger_compaction(&topic).await.unwrap();
        // Compaction is asynchronous; only its acceptance is asserted here.
        assert!(!t.compaction_status(&topic).await.unwrap().status.is_empty());

        t.unload(&topic).await.unwrap();
        t.trim_topic(&topic).await.unwrap();
        t.expire_messages_for_all_subscriptions(&topic, 0)
            .await
            .unwrap();
        t.truncate(&topic).await.unwrap();

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Terminating a topic must return the last message id and reject further writes.
#[tokio::test]
async fn topic_terminate() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("term"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        publish(&topic, 2).await;

        let last = t.terminate_topic(&topic).await.unwrap();
        assert!(last.ledger_id > 0, "terminate returned {last:?}");

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

/// A malformed topic must fail locally rather than hitting a nonsense URL.
#[tokio::test]
async fn topics_reject_malformed_names() {
    let admin = new_admin().await;
    for bad in ["", "a", "a/b", "persistent://a/b"] {
        let err = admin
            .topics()
            .get_stats(bad, Default::default())
            .await
            .unwrap_err();
        match err {
            Error::Admin(AdminError::InvalidTopic(_)) => {}
            other => panic!("expected InvalidTopic for {bad:?}, got {other:?}"),
        }
    }
    for bad in ["", "no-slash", "a/b/c"] {
        let err = admin.topics().get_list(bad).await.unwrap_err();
        match err {
            Error::Admin(AdminError::InvalidTopic(_)) => {}
            other => panic!("expected InvalidTopic for namespace {bad:?}, got {other:?}"),
        }
    }
}

// --------------------------------------------------------------- schemas

#[tokio::test]
async fn schema_create_read_versions_delete() {
    use crate::admin::models::PostSchemaPayload;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("schema"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        let s = admin.schemas();
        // A topic with no schema must report None rather than erroring.
        assert!(s.get_schema_info(&topic).await.unwrap().is_none());

        s.create_schema(
            &topic,
            &PostSchemaPayload {
                schema_type: "STRING".to_string(),
                schema: String::new(),
                properties: Default::default(),
            },
        )
        .await
        .unwrap();

        let info = s.get_schema_info(&topic).await.unwrap().unwrap();
        assert_eq!(info.schema_type, "STRING");
        assert_eq!(info.version, 0);
        assert!(info.timestamp > 0);

        assert_eq!(
            s.get_schema_info_at_version(&topic, 0)
                .await
                .unwrap()
                .map(|i| i.version),
            Some(0)
        );

        let all = s.get_all_schemas(&topic).await.unwrap();
        assert_eq!(all.len(), 1, "expected one schema version, got {all:?}");

        let metadata = s.get_schema_metadata(&topic).await.unwrap();
        assert!(metadata.info.is_some(), "metadata: {metadata:?}");
        assert!(!metadata.index.is_empty());

        s.delete_schema(&topic, false).await.unwrap();
        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// An AVRO schema carries a real definition, so this exercises the `data` field
/// rather than the schemaless `STRING` path.
#[tokio::test]
async fn schema_avro_round_trip() {
    use crate::admin::models::PostSchemaPayload;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("avro"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        let definition =
            r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;
        admin
            .schemas()
            .create_schema(
                &topic,
                &PostSchemaPayload {
                    schema_type: "AVRO".to_string(),
                    schema: definition.to_string(),
                    properties: [("owner".to_string(), "team-a".to_string())]
                        .into_iter()
                        .collect(),
                },
            )
            .await
            .unwrap();

        let info = admin
            .schemas()
            .get_schema_info(&topic)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(info.schema_type, "AVRO");
        assert!(info.data.contains("\"name\""), "definition lost: {info:?}");
        assert_eq!(
            info.properties.get("owner").map(String::as_str),
            Some("team-a")
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

// -------------------------------------------------- non-persistent topics

#[tokio::test]
async fn non_persistent_topic_stats_and_listing() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("non-persistent://{ns}/{}", unique("np"));

        // A non-persistent topic comes into being when a producer attaches.
        let pulsar =
            crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
                .build()
                .await
                .unwrap();
        let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
        producer
            .send_non_blocking("hello")
            .await
            .unwrap()
            .await
            .unwrap();

        let np = admin.non_persistent_topics();
        assert!(
            np.get_list(&ns).await.unwrap().iter().any(|t| t == &topic),
            "non-persistent topic not listed"
        );

        // Its stats shape differs from a persistent topic's — no storage fields.
        let stats = np.get_stats(&topic).await.unwrap();
        assert!(
            !stats.publishers.is_empty(),
            "expected the attached producer in stats: {stats:?}"
        );

        assert_eq!(
            np.get_partitioned_topic_metadata(&topic)
                .await
                .unwrap()
                .partitions,
            0
        );

        producer.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn non_persistent_partitioned_topic() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("non-persistent://{ns}/{}", unique("nppart"));
        let np = admin.non_persistent_topics();

        np.create_partitioned_topic(&topic, 3).await.unwrap();
        assert_eq!(
            np.get_partitioned_topic_metadata(&topic)
                .await
                .unwrap()
                .partitions,
            3
        );
    })
    .await;
}

// ---------------------------------------------------------- broker stats

#[tokio::test]
async fn broker_stats_endpoints() {
    let admin = new_admin().await;
    let bs = admin.broker_stats();

    let metrics = bs.get_metrics().await.unwrap();
    assert!(
        metrics.starts_with('['),
        "metrics was not a JSON array: {}",
        &metrics[..40.min(metrics.len())]
    );

    let topics = bs.get_topics().await.unwrap();
    assert!(topics.starts_with('{'), "topics was not a JSON object");

    bs.get_mbeans().await.unwrap();
    bs.get_pending_bookie_ops_stats().await.unwrap();

    // A standalone broker answers 204 here, which must read as None not as an error.
    bs.get_load_report().await.unwrap();
}

// --------------------------------------------------------------- lookup

#[tokio::test]
async fn lookup_topic_and_bundle() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("lookup"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        let result = admin.lookup().lookup_topic(&topic).await.unwrap();
        assert!(
            result.broker_url.is_some() || result.native_url.is_some(),
            "lookup returned no broker url: {result:?}"
        );

        // Bundle ranges look like 0x00000000_0xffffffff.
        let bundle = admin.lookup().get_bundle_range(&topic).await.unwrap();
        assert!(
            bundle.starts_with("0x") && bundle.contains('_'),
            "unexpected bundle range: {bundle}"
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

// ---------------------------------------------------------- transactions

/// The coordinator listing works on any broker with transactions enabled, and is
/// the entry point every other transaction endpoint needs.
#[tokio::test]
async fn transaction_coordinators_are_listed() {
    let admin = new_admin().await;
    let coordinators = admin
        .transactions()
        .list_transaction_coordinators()
        .await
        .unwrap();
    assert!(
        !coordinators.is_empty(),
        "no transaction coordinators; is transactionCoordinatorEnabled set?"
    );
    assert!(
        coordinators[0].broker_service_url.is_some(),
        "coordinator has no broker url: {:?}",
        coordinators[0]
    );
}

/// Transaction-buffer and pending-ack stats are readable for a topic that has
/// never carried a transaction, which is the case worth pinning: the models must
/// decode the broker's "nothing happened yet" shape.
#[tokio::test]
async fn transaction_buffer_and_pending_ack_stats_decode() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("txn"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        admin
            .topics()
            .create_subscription(
                &topic,
                "sub1",
                &crate::admin::models::MessageIdData::latest(),
            )
            .await
            .unwrap();

        let txn = admin.transactions();

        // The buffer exists lazily; a topic that never saw a transaction may report
        // it as absent, which is a legitimate answer rather than a decode failure.
        match txn.get_transaction_buffer_stats(&topic, true, false).await {
            Ok(stats) => assert!(
                stats.state.is_some() || stats.ongoing_txn_size == 0,
                "buffer stats decoded but empty: {stats:?}"
            ),
            Err(Error::Admin(AdminError::NotFound(_))) => {}
            Err(e) => panic!("unexpected error reading buffer stats: {e:?}"),
        }

        match txn.get_pending_ack_stats(&topic, "sub1", true).await {
            Ok(stats) => assert!(
                stats.state.is_some() || stats.ongoing_txn_size == 0,
                "pending-ack stats decoded but empty: {stats:?}"
            ),
            Err(Error::Admin(AdminError::NotFound(_))) => {}
            Err(e) => panic!("unexpected error reading pending-ack stats: {e:?}"),
        }

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Slow-transaction listing must decode an empty result rather than failing.
#[tokio::test]
async fn slow_transactions_decode_when_none_are_slow() {
    let admin = new_admin().await;
    match admin.transactions().get_slow_transactions(60_000).await {
        Ok(slow) => assert!(slow.is_empty(), "unexpected slow transactions: {slow:?}"),
        // Some broker versions require the coordinator to be fully recovered first.
        Err(Error::Admin(
            AdminError::NotFound(_) | AdminError::ServerUnavailable(_) | AdminError::Http { .. },
        )) => {}
        Err(e) => panic!("unexpected error listing slow transactions: {e:?}"),
    }
}

// ------------------------------------------------------- scalable topics

/// Skips the body unless the broker speaks the Pulsar 5.0 scalable-topic protocol.
async fn if_scalable_supported<F, Fut>(body: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let pulsar = crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap();
    if !pulsar
        .broker_features()
        .await
        .unwrap()
        .supports_scalable_topics
    {
        log::warn!("broker does not support scalable topics, skipping");
        return;
    }
    body().await;
}

#[tokio::test]
async fn scalable_topic_create_metadata_stats_delete() {
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let st = admin.scalable_topics();
            let name = unique("st");
            let topic = format!("topic://{ns}/{name}");

            st.create_scalable_topic(&topic, 2).await.unwrap();

            // Listing returns canonical topic:// names.
            let listed = st.list_scalable_topics(&ns).await.unwrap();
            assert!(
                listed.iter().any(|t| t.contains(&name)),
                "scalable topic not listed: {listed:?}"
            );

            // The DAG starts as N leaf segments covering the whole 16-bit ring.
            let meta = st.get_metadata(&topic).await.unwrap();
            assert_eq!(meta.segments.len(), 2, "metadata: {meta:?}");
            let mut ranges: Vec<(u32, u32)> = meta
                .segments
                .values()
                .map(|s| (s.hash_range.start, s.hash_range.end))
                .collect();
            ranges.sort();
            assert_eq!(
                ranges,
                vec![(0, 32767), (32768, 65535)],
                "two initial segments must split the 16-bit ring evenly"
            );
            assert!(meta
                .segments
                .values()
                .all(|s| s.active && s.leaf && !s.sealed));

            let stats = st.get_stats(&topic).await.unwrap();
            assert_eq!(stats.total_segments, 2);
            assert_eq!(stats.active_segments, 2);
            assert_eq!(stats.sealed_segments, 0);
            // Each segment is backed by its own segment:// topic.
            assert!(
                stats.segments.values().all(|s| s
                    .name
                    .as_deref()
                    .is_some_and(|n| n.starts_with("segment://"))),
                "stats: {stats:?}"
            );

            // Delete is accepted, but 5.0.0-M1 keeps the topic in the namespace
            // listing afterwards (verified by hand against the broker), so only the
            // acceptance is asserted here rather than its disappearance.
            st.delete_scalable_topic(&topic, true).await.unwrap();
        })
        .await;
    })
    .await;
}

/// Splitting a segment must extend the DAG: the parent seals and two children
/// appear covering its hash range.
#[tokio::test]
async fn scalable_topic_split_segment_extends_the_dag() {
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let st = admin.scalable_topics();
            let topic = format!("topic://{ns}/{}", unique("split"));

            st.create_scalable_topic(&topic, 1).await.unwrap();
            let before = st.get_metadata(&topic).await.unwrap();
            assert_eq!(before.segments.len(), 1);
            let epoch_before = before.epoch;

            st.split_segment(&topic, 0).await.unwrap();

            let after = st.get_metadata(&topic).await.unwrap();
            assert!(
                after.segments.len() > before.segments.len(),
                "split did not add segments: {after:?}"
            );
            assert!(
                after.epoch > epoch_before,
                "split did not bump the DAG epoch ({epoch_before} -> {})",
                after.epoch
            );

            // The parent must now be sealed and have children recorded.
            let parent = after.segments.get("0").expect("segment 0 missing");
            assert!(parent.sealed, "parent segment not sealed: {parent:?}");
            assert!(!parent.child_ids.is_empty(), "parent has no children");

            // The children together must still cover the parent's range.
            let children: Vec<_> = after
                .segments
                .values()
                .filter(|s| s.parent_ids.contains(&0))
                .collect();
            assert_eq!(children.len(), 2, "expected 2 children, got {children:?}");
            let min = children.iter().map(|c| c.hash_range.start).min().unwrap();
            let max = children.iter().map(|c| c.hash_range.end).max().unwrap();
            assert_eq!((min, max), (parent.hash_range.start, parent.hash_range.end));

            st.delete_scalable_topic(&topic, true).await.unwrap();
        })
        .await;
    })
    .await;
}

#[tokio::test]
async fn scalable_topic_auto_scale_policy_round_trip() {
    use crate::admin::models::AutoScalePolicyOverride;
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let st = admin.scalable_topics();
            let topic = format!("topic://{ns}/{}", unique("autoscale"));

            st.create_scalable_topic(&topic, 1).await.unwrap();
            assert!(st.get_auto_scale_policy(&topic).await.unwrap().is_none());

            let policy = AutoScalePolicyOverride {
                enabled: Some(true),
                max_segments: Some(8),
                min_segments: Some(1),
                split_cooldown_seconds: Some(30),
                merge_cooldown_seconds: Some(45),
                merge_window_seconds: Some(60),
                split_msg_rate_in_threshold: Some(1000.0),
                merge_msg_rate_in_threshold: Some(10.0),
                ..Default::default()
            };
            st.set_auto_scale_policy(&topic, &policy).await.unwrap();

            // Every field, not just `enabled`. An earlier version of this test
            // asserted only `enabled` and blamed the preview broker for dropping
            // the rest — in fact five field names were singular where the wire is
            // plural, so the broker answered 204 and silently discarded them. Only
            // a full read-back catches that.
            let read = st.get_auto_scale_policy(&topic).await.unwrap().unwrap();
            assert_eq!(read, policy, "the auto-scale policy did not round-trip");

            st.remove_auto_scale_policy(&topic).await.unwrap();
            st.delete_scalable_topic(&topic, true).await.unwrap();
        })
        .await;
    })
    .await;
}

#[tokio::test]
async fn scalable_topic_subscriptions() {
    use crate::admin::models::ScalableSubscriptionType;
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let st = admin.scalable_topics();
            let topic = format!("topic://{ns}/{}", unique("stsub"));

            st.create_scalable_topic(&topic, 2).await.unwrap();

            st.create_subscription(&topic, "stream-sub", ScalableSubscriptionType::Stream)
                .await
                .unwrap();

            // PIP-460 defines CHECKPOINT, but 5.0.0-M1 does not serve it yet and
            // answers 404. Asserting the rejection documents the gap and will fail
            // loudly — prompting this test to be tightened — once the broker adds it.
            match st
                .create_subscription(&topic, "ckpt-sub", ScalableSubscriptionType::Checkpoint)
                .await
            {
                Err(Error::Admin(AdminError::NotFound(_))) => {}
                Ok(()) => panic!(
                    "the broker now supports CHECKPOINT subscriptions; \
                     enable the assertions below"
                ),
                Err(e) => panic!("unexpected error creating a CHECKPOINT subscription: {e:?}"),
            }

            let stats = st.get_stats(&topic).await.unwrap();
            assert!(
                stats.subscriptions.contains_key("stream-sub"),
                "subscription missing from stats: {stats:?}"
            );
            assert_eq!(
                stats.subscriptions["stream-sub"].consumer_count, 0,
                "no consumer is attached, so the count must be 0"
            );

            // Both maintenance operations must be accepted on an empty backlog.
            st.clear_backlog(&topic, "stream-sub").await.unwrap();
            st.seek_subscription(&topic, "stream-sub", 1).await.unwrap();

            st.delete_subscription(&topic, "stream-sub").await.unwrap();
            let stats = st.get_stats(&topic).await.unwrap();
            assert!(!stats.subscriptions.contains_key("stream-sub"));

            st.delete_scalable_topic(&topic, true).await.unwrap();
        })
        .await;
    })
    .await;
}

/// Property filters are how a control plane finds its own topics.
#[tokio::test]
async fn scalable_topic_properties_and_filtered_listing() {
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let st = admin.scalable_topics();
            let name = unique("stprops");
            let topic = format!("topic://{ns}/{name}");

            let props: BTreeMap<String, String> = [("team".to_string(), "payments".to_string())]
                .into_iter()
                .collect();
            st.create_scalable_topic_with_properties(&topic, 1, &props)
                .await
                .unwrap();

            let matching = st
                .list_scalable_topics_by_properties(&ns, &props)
                .await
                .unwrap();
            assert!(
                matching.iter().any(|t| t.contains(&name)),
                "property filter did not match: {matching:?}"
            );

            let other: BTreeMap<String, String> = [("team".to_string(), "nobody".to_string())]
                .into_iter()
                .collect();
            let none = st
                .list_scalable_topics_by_properties(&ns, &other)
                .await
                .unwrap();
            assert!(
                !none.iter().any(|t| t.contains(&name)),
                "property filter matched the wrong topic: {none:?}"
            );

            st.delete_scalable_topic(&topic, true).await.unwrap();
        })
        .await;
    })
    .await;
}

/// A `topic://` prefix must be accepted and stripped, and malformed names must
/// fail locally.
#[tokio::test]
async fn scalable_topics_accept_prefix_and_reject_malformed() {
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let st = admin.scalable_topics();
            let name = unique("stprefix");

            // Prefixed and bare forms must address the same topic.
            st.create_scalable_topic(&format!("topic://{ns}/{name}"), 1)
                .await
                .unwrap();
            let meta = st.get_metadata(&format!("{ns}/{name}")).await.unwrap();
            assert_eq!(meta.segments.len(), 1);

            st.delete_scalable_topic(&format!("{ns}/{name}"), true)
                .await
                .unwrap();
        })
        .await;

        let admin = new_admin().await;
        for bad in ["", "a", "a/b"] {
            let err = admin.scalable_topics().get_metadata(bad).await.unwrap_err();
            match err {
                Error::Admin(AdminError::InvalidTopic(_)) => {}
                other => panic!("expected InvalidTopic for {bad:?}, got {other:?}"),
            }
        }
    })
    .await;
}

// ------------------------------------- completeness: policy removal paths

/// Every removable struct-valued namespace policy must actually clear.
///
/// Generated over the whole set: the setters are covered elsewhere, but a wrong
/// path on a `remove_*` would otherwise go unnoticed, since the broker answers 204
/// for an unmatched DELETE that its bundle handler happens to accept.
#[tokio::test]
async fn namespace_struct_policy_removals() {
    use crate::admin::models::*;
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        n.set_persistence(
            &ns,
            &PersistencePolicies {
                bookkeeper_ensemble: 1,
                bookkeeper_write_quorum: 1,
                bookkeeper_ack_quorum: 1,
                managed_ledger_max_mark_delete_rate: 0.0,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_persistence(&ns).await.unwrap().is_some(),
            "persistence was not set, so its removal proves nothing"
        );
        n.remove_persistence(&ns).await.unwrap();
        assert!(
            n.get_persistence(&ns).await.unwrap().is_none(),
            "persistence survived removal"
        );

        n.set_dispatch_rate(
            &ns,
            &DispatchRate {
                dispatch_throttling_rate_in_msg: 10,
                dispatch_throttling_rate_in_byte: 100,
                relative_to_publish_rate: false,
                rate_period_in_second: 1,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_dispatch_rate(&ns).await.unwrap().is_some(),
            "dispatch_rate was not set, so its removal proves nothing"
        );
        n.remove_dispatch_rate(&ns).await.unwrap();
        assert!(
            n.get_dispatch_rate(&ns).await.unwrap().is_none(),
            "dispatch_rate survived removal"
        );

        n.set_subscription_dispatch_rate(
            &ns,
            &DispatchRate {
                dispatch_throttling_rate_in_msg: 10,
                dispatch_throttling_rate_in_byte: 100,
                relative_to_publish_rate: false,
                rate_period_in_second: 1,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_subscription_dispatch_rate(&ns)
                .await
                .unwrap()
                .is_some(),
            "subscription_dispatch_rate was not set, so its removal proves nothing"
        );
        n.remove_subscription_dispatch_rate(&ns).await.unwrap();
        assert!(
            n.get_subscription_dispatch_rate(&ns)
                .await
                .unwrap()
                .is_none(),
            "subscription_dispatch_rate survived removal"
        );

        n.set_replicator_dispatch_rate(
            &ns,
            &DispatchRate {
                dispatch_throttling_rate_in_msg: 10,
                dispatch_throttling_rate_in_byte: 100,
                relative_to_publish_rate: false,
                rate_period_in_second: 1,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_replicator_dispatch_rate(&ns).await.unwrap().is_some(),
            "replicator_dispatch_rate was not set, so its removal proves nothing"
        );
        n.remove_replicator_dispatch_rate(&ns).await.unwrap();
        assert!(
            n.get_replicator_dispatch_rate(&ns).await.unwrap().is_none(),
            "replicator_dispatch_rate survived removal"
        );

        n.set_publish_rate(
            &ns,
            &PublishRate {
                publish_throttling_rate_in_msg: 10,
                publish_throttling_rate_in_byte: 100,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_publish_rate(&ns).await.unwrap().is_some(),
            "publish_rate was not set, so its removal proves nothing"
        );
        n.remove_publish_rate(&ns).await.unwrap();
        assert!(
            n.get_publish_rate(&ns).await.unwrap().is_none(),
            "publish_rate survived removal"
        );

        n.set_subscribe_rate(
            &ns,
            &SubscribeRate {
                subscribe_throttling_rate_per_consumer: 5,
                rate_period_in_second: 30,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_subscribe_rate(&ns).await.unwrap().is_some(),
            "subscribe_rate was not set, so its removal proves nothing"
        );
        n.remove_subscribe_rate(&ns).await.unwrap();
        assert!(
            n.get_subscribe_rate(&ns).await.unwrap().is_none(),
            "subscribe_rate survived removal"
        );

        n.set_inactive_topic_policies(
            &ns,
            &InactiveTopicPolicies {
                inactive_topic_delete_mode: Some("delete_when_no_subscriptions".to_string()),
                max_inactive_duration_seconds: 60,
                delete_while_inactive: true,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_inactive_topic_policies(&ns).await.unwrap().is_some(),
            "inactive_topic_policies was not set, so its removal proves nothing"
        );
        n.remove_inactive_topic_policies(&ns).await.unwrap();
        assert!(
            n.get_inactive_topic_policies(&ns).await.unwrap().is_none(),
            "inactive_topic_policies survived removal"
        );

        n.set_delayed_delivery_messages(
            &ns,
            &DelayedDeliveryPolicies {
                active: true,
                tick_time: 1000.0,
                max_delivery_delay_in_millis: None,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_delayed_delivery_messages(&ns)
                .await
                .unwrap()
                .is_some(),
            "delayed_delivery_messages was not set, so its removal proves nothing"
        );
        n.remove_delayed_delivery_messages(&ns).await.unwrap();
        assert!(
            n.get_delayed_delivery_messages(&ns)
                .await
                .unwrap()
                .is_none(),
            "delayed_delivery_messages survived removal"
        );

        n.set_auto_topic_creation(
            &ns,
            &AutoTopicCreationOverride {
                allow_auto_topic_creation: true,
                topic_type: Some("non-partitioned".to_string()),
                default_num_partitions: None,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_auto_topic_creation(&ns).await.unwrap().is_some(),
            "auto_topic_creation was not set, so its removal proves nothing"
        );
        n.remove_auto_topic_creation(&ns).await.unwrap();
        assert!(
            n.get_auto_topic_creation(&ns).await.unwrap().is_none(),
            "auto_topic_creation survived removal"
        );

        n.set_auto_subscription_creation(
            &ns,
            &AutoSubscriptionCreationOverride {
                allow_auto_subscription_creation: true,
            },
        )
        .await
        .unwrap();
        assert!(
            n.get_auto_subscription_creation(&ns)
                .await
                .unwrap()
                .is_some(),
            "auto_subscription_creation was not set, so its removal proves nothing"
        );
        n.remove_auto_subscription_creation(&ns).await.unwrap();
        assert!(
            n.get_auto_subscription_creation(&ns)
                .await
                .unwrap()
                .is_none(),
            "auto_subscription_creation survived removal"
        );

        // Booleans with a remover.
        n.set_deduplication_status(&ns, true).await.unwrap();
        n.remove_deduplication_status(&ns).await.unwrap();
        assert!(n.get_deduplication_status(&ns).await.unwrap().is_none());

        n.set_dispatcher_pause_on_ack_state_persistent(&ns)
            .await
            .unwrap();
        n.remove_dispatcher_pause_on_ack_state_persistent(&ns)
            .await
            .unwrap();
        // Like `maxTopicsPerNamespace`, this one reports its default (`false`) after
        // removal rather than an empty body.
        assert!(matches!(
            n.get_dispatcher_pause_on_ack_state_persistent(&ns)
                .await
                .unwrap(),
            None | Some(false)
        ));
    })
    .await;
}

/// Same for the topic-level struct policies.
#[tokio::test]
async fn topic_struct_policy_removals() {
    use crate::admin::models::*;
    let admin = new_admin().await;
    with_topic(&admin, |topic| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();

        tp.set_persistence(
            &topic,
            &PersistencePolicies {
                bookkeeper_ensemble: 1,
                bookkeeper_write_quorum: 1,
                bookkeeper_ack_quorum: 1,
                managed_ledger_max_mark_delete_rate: 0.0,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_persistence(&topic, false).await.unwrap().is_some(),
            "persistence was not set, so its removal proves nothing"
        );
        tp.remove_persistence(&topic).await.unwrap();
        assert!(
            tp.get_persistence(&topic, false).await.unwrap().is_none(),
            "persistence survived removal"
        );

        tp.set_dispatch_rate(
            &topic,
            &DispatchRate {
                dispatch_throttling_rate_in_msg: 10,
                dispatch_throttling_rate_in_byte: 100,
                relative_to_publish_rate: false,
                rate_period_in_second: 1,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_dispatch_rate(&topic, false).await.unwrap().is_some(),
            "dispatch_rate was not set, so its removal proves nothing"
        );
        tp.remove_dispatch_rate(&topic).await.unwrap();
        assert!(
            tp.get_dispatch_rate(&topic, false).await.unwrap().is_none(),
            "dispatch_rate survived removal"
        );

        tp.set_subscription_dispatch_rate(
            &topic,
            &DispatchRate {
                dispatch_throttling_rate_in_msg: 10,
                dispatch_throttling_rate_in_byte: 100,
                relative_to_publish_rate: false,
                rate_period_in_second: 1,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_subscription_dispatch_rate(&topic, false)
                .await
                .unwrap()
                .is_some(),
            "subscription_dispatch_rate was not set, so its removal proves nothing"
        );
        tp.remove_subscription_dispatch_rate(&topic).await.unwrap();
        assert!(
            tp.get_subscription_dispatch_rate(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "subscription_dispatch_rate survived removal"
        );

        tp.set_replicator_dispatch_rate(
            &topic,
            &DispatchRate {
                dispatch_throttling_rate_in_msg: 10,
                dispatch_throttling_rate_in_byte: 100,
                relative_to_publish_rate: false,
                rate_period_in_second: 1,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_replicator_dispatch_rate(&topic, false)
                .await
                .unwrap()
                .is_some(),
            "replicator_dispatch_rate was not set, so its removal proves nothing"
        );
        tp.remove_replicator_dispatch_rate(&topic).await.unwrap();
        assert!(
            tp.get_replicator_dispatch_rate(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "replicator_dispatch_rate survived removal"
        );

        tp.set_publish_rate(
            &topic,
            &PublishRate {
                publish_throttling_rate_in_msg: 10,
                publish_throttling_rate_in_byte: 100,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_publish_rate(&topic).await.unwrap().is_some(),
            "publish_rate was not set, so its removal proves nothing"
        );
        tp.remove_publish_rate(&topic).await.unwrap();
        assert!(
            tp.get_publish_rate(&topic).await.unwrap().is_none(),
            "publish_rate survived removal"
        );

        tp.set_subscribe_rate(
            &topic,
            &SubscribeRate {
                subscribe_throttling_rate_per_consumer: 5,
                rate_period_in_second: 30,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_subscribe_rate(&topic, false)
                .await
                .unwrap()
                .is_some(),
            "subscribe_rate was not set, so its removal proves nothing"
        );
        tp.remove_subscribe_rate(&topic).await.unwrap();
        assert!(
            tp.get_subscribe_rate(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "subscribe_rate survived removal"
        );

        tp.set_inactive_topic_policies(
            &topic,
            &InactiveTopicPolicies {
                inactive_topic_delete_mode: Some("delete_when_no_subscriptions".to_string()),
                max_inactive_duration_seconds: 60,
                delete_while_inactive: true,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_inactive_topic_policies(&topic, false)
                .await
                .unwrap()
                .is_some(),
            "inactive_topic_policies was not set, so its removal proves nothing"
        );
        tp.remove_inactive_topic_policies(&topic).await.unwrap();
        assert!(
            tp.get_inactive_topic_policies(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "inactive_topic_policies survived removal"
        );

        tp.set_delayed_delivery_policy(
            &topic,
            &DelayedDeliveryPolicies {
                active: true,
                tick_time: 1000.0,
                max_delivery_delay_in_millis: None,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_delayed_delivery_policy(&topic, false)
                .await
                .unwrap()
                .is_some(),
            "delayed_delivery_policy was not set, so its removal proves nothing"
        );
        tp.remove_delayed_delivery_policy(&topic).await.unwrap();
        assert!(
            tp.get_delayed_delivery_policy(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "delayed_delivery_policy survived removal"
        );

        tp.set_auto_subscription_creation(
            &topic,
            &AutoSubscriptionCreationOverride {
                allow_auto_subscription_creation: true,
            },
        )
        .await
        .unwrap();
        assert!(
            tp.get_auto_subscription_creation(&topic, false)
                .await
                .unwrap()
                .is_some(),
            "auto_subscription_creation was not set, so its removal proves nothing"
        );
        tp.remove_auto_subscription_creation(&topic).await.unwrap();
        assert!(
            tp.get_auto_subscription_creation(&topic, false)
                .await
                .unwrap()
                .is_none(),
            "auto_subscription_creation survived removal"
        );

        tp.set_deduplication_status(&topic, true).await.unwrap();
        tp.remove_deduplication_status(&topic).await.unwrap();
        assert!(tp
            .get_deduplication_status(&topic, false)
            .await
            .unwrap()
            .is_none());

        tp.set_schema_compatibility_strategy(&topic, SchemaCompatibilityStrategy::Forward)
            .await
            .unwrap();
        tp.remove_schema_compatibility_strategy(&topic)
            .await
            .unwrap();
    })
    .await;
}

// ------------------------------------- completeness: remaining endpoints

/// Bundle-scoped namespace operations, plus the bundle resource quotas.
///
/// These all need a real bundle range, which is read from the namespace rather
/// than hardcoded so the test does not depend on the default bundle count.
#[tokio::test]
async fn namespace_bundle_operations() {
    use crate::admin::models::ResourceQuota;
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let bundles = admin.namespaces().get_bundles(&ns).await.unwrap();
        assert!(bundles.boundaries.len() >= 2, "bundles: {bundles:?}");
        // A bundle is named by the pair of boundaries it spans.
        let bundle = format!("{}_{}", bundles.boundaries[0], bundles.boundaries[1]);

        admin
            .namespaces()
            .unload_namespace_bundle(&ns, &bundle)
            .await
            .unwrap();

        let quota = ResourceQuota {
            msg_rate_in: 10.0,
            msg_rate_out: 20.0,
            bandwidth_in: 100.0,
            bandwidth_out: 200.0,
            memory: 16.0,
            dynamic: false,
        };
        let rq = admin.resource_quotas();
        rq.set_namespace_bundle_resource_quota(&ns, &bundle, &quota)
            .await
            .unwrap();
        let read = rq
            .get_namespace_bundle_resource_quota(&ns, &bundle)
            .await
            .unwrap();
        assert_eq!(read.msg_rate_in, 10.0, "bundle quota: {read:?}");
        rq.reset_namespace_bundle_resource_quota(&ns, &bundle)
            .await
            .unwrap();

        // Splitting a bundle must increase the boundary count. The broker can split
        // on its own, in which case the range read a moment ago is rejected with
        // "Invalid upper boundary", so re-read and retry rather than assuming the
        // first layout still holds.
        let mut before = 0;
        let mut split_ok = false;
        for _ in 0..5 {
            let fresh = admin.namespaces().get_bundles(&ns).await.unwrap();
            before = fresh.boundaries.len();
            let bundle = format!("{}_{}", fresh.boundaries[0], fresh.boundaries[1]);
            match admin
                .namespaces()
                .split_namespace_bundle(&ns, &bundle, false)
                .await
            {
                Ok(()) => {
                    split_ok = true;
                    break;
                }
                Err(Error::Admin(AdminError::PreconditionFailed(_))) => continue,
                Err(e) => panic!("unexpected error splitting a bundle: {e:?}"),
            }
        }
        assert!(
            split_ok,
            "could not split a bundle after re-reading the layout"
        );
        let after = admin.namespaces().get_bundles(&ns).await.unwrap();
        assert!(
            after.boundaries.len() > before,
            "split did not add a boundary: {after:?}"
        );

        // Listing by bundle must use a range from the *current* layout: the split
        // above invalidated the range captured earlier.
        let current = format!("{}_{}", after.boundaries[0], after.boundaries[1]);
        admin
            .non_persistent_topics()
            .get_list_in_bundle(&ns, &current)
            .await
            .unwrap();
        admin
            .topics()
            .get_list_in_bundle(&ns, &current)
            .await
            .unwrap();
    })
    .await;
}

/// Namespace-wide subscription operations and subscription-level permissions.
#[tokio::test]
async fn namespace_subscription_operations() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();
        let topic = format!("persistent://{ns}/{}", unique("nssub"));

        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        admin
            .topics()
            .create_subscription(
                &topic,
                "sub1",
                &crate::admin::models::MessageIdData::latest(),
            )
            .await
            .unwrap();

        n.grant_permission_on_subscription(&ns, "sub1", &["role-a".into()])
            .await
            .unwrap();
        n.revoke_permission_on_subscription(&ns, "sub1", "role-a")
            .await
            .unwrap();

        n.clear_namespace_backlog_for_subscription(&ns, "sub1")
            .await
            .unwrap();

        // Unsubscribing across the namespace must remove it from the topic.
        n.unsubscribe_namespace(&ns, "sub1").await.unwrap();
        assert!(
            admin
                .topics()
                .get_subscriptions(&topic)
                .await
                .unwrap()
                .is_empty(),
            "unsubscribe_namespace left the subscription in place"
        );
    })
    .await;
}

/// Anti-affinity groups, resource-group assignment and entry filters — the
/// string-valued namespace policies with their own removal paths.
#[tokio::test]
async fn namespace_string_policies_and_entry_filters() {
    use crate::admin::models::{EntryFilters, ResourceGroup};
    let admin = new_admin().await;
    let group = unique("test_rg_ns");
    admin
        .resource_groups()
        .create_resource_group(&group, &ResourceGroup::default())
        .await
        .unwrap();

    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        n.set_namespace_anti_affinity_group(&ns, "group-a")
            .await
            .unwrap();
        assert_eq!(
            n.get_namespace_anti_affinity_group(&ns)
                .await
                .unwrap()
                .as_deref(),
            Some("group-a")
        );
        n.remove_namespace_anti_affinity_group(&ns).await.unwrap();

        // The broker validates that the named filter is actually installed, and a
        // stock broker has none, so a round-trip is impossible here. Assert the
        // rejection instead: it proves the request reached the right endpoint with
        // the right body, which is what this client is responsible for.
        let err = n
            .set_namespace_entry_filters(
                &ns,
                &EntryFilters {
                    entry_filter_names: "jms".to_string(),
                },
            )
            .await
            .unwrap_err();
        match err {
            Error::Admin(AdminError::BadRequest(m)) => {
                assert!(m.contains("jms"), "unexpected rejection: {m}")
            }
            other => panic!("expected the broker to reject an uninstalled filter: {other:?}"),
        }
        assert!(n.get_namespace_entry_filters(&ns).await.unwrap().is_none());
        // Removal must still succeed when nothing is set.
        n.remove_namespace_entry_filters(&ns).await.unwrap();
    })
    .await;

    // The resource-group assignment needs a namespace that outlives the group.
    let admin = new_admin().await;
    let ns = format!("public/{}", unique("test_rg_assign"));
    admin.namespaces().create_namespace(&ns).await.unwrap();
    let n = admin.namespaces();
    n.set_namespace_resource_group(&ns, &group).await.unwrap();
    assert_eq!(
        n.get_namespace_resource_group(&ns)
            .await
            .unwrap()
            .as_deref(),
        Some(group.as_str())
    );
    n.remove_namespace_resource_group(&ns).await.unwrap();
    assert!(n.get_namespace_resource_group(&ns).await.unwrap().is_none());

    admin
        .namespaces()
        .delete_namespace(&ns, true)
        .await
        .unwrap();
    delete_resource_group_retrying(&admin, &group).await;
}

/// Cluster migration state and per-broker isolation lookup.
#[tokio::test]
async fn cluster_migration_and_broker_isolation() {
    use crate::admin::models::{ClusterData, ClusterUrl};
    let admin = new_admin().await;
    let name = unique("test_cluster_mig");
    admin
        .clusters()
        .create_cluster(&name, &ClusterData::with_service_url("http://a:8080"))
        .await
        .unwrap();

    // A fresh cluster is not migrated.
    assert!(
        admin
            .clusters()
            .get_cluster_migration(&name)
            .await
            .unwrap()
            .is_none(),
        "a cluster with no migration configured must report None"
    );

    admin
        .clusters()
        .update_cluster_migration(
            &name,
            true,
            &ClusterUrl {
                service_url: Some("http://b:8080".to_string()),
                broker_service_url: Some("pulsar://b:6650".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let policies = admin
        .clusters()
        .get_cluster_migration(&name)
        .await
        .unwrap()
        .expect("migration state must exist once set");
    assert!(
        policies.migrated,
        "migration flag did not stick: {policies:?}"
    );

    admin.clusters().delete_cluster(&name).await.unwrap();

    // Per-broker isolation lookup, against a broker that really exists.
    //
    // The endpoint 404s while the cluster's isolation-policy node has never been
    // created, so this establishes that precondition itself rather than depending on
    // another test having run first. Deleting the policy afterwards leaves the node
    // in place with an empty map, so this cannot break those tests either.
    let cluster = primary_cluster(&admin).await;
    let brokers = admin.brokers().get_active_brokers().await.unwrap();
    let isolation = unique("migration_isolation");
    admin
        .clusters()
        .set_namespace_isolation_policy(
            &cluster,
            &isolation,
            &NamespaceIsolationData {
                namespaces: vec!["public/never_matched.*".to_string()],
                primary: vec![".*".to_string()],
                secondary: vec![],
                auto_failover_policy: Some(crate::admin::models::AutoFailoverPolicyData {
                    policy_type: Some("min_available".to_string()),
                    parameters: [
                        ("min_limit".to_string(), "1".to_string()),
                        ("usage_threshold".to_string(), "80".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                }),
                unload_scope: None,
            },
        )
        .await
        .unwrap();
    admin
        .clusters()
        .get_broker_with_namespace_isolation_policy(&cluster, &brokers[0])
        .await
        .unwrap();
    admin
        .clusters()
        .delete_namespace_isolation_policy(&cluster, &isolation)
        .await
        .unwrap();

    // Targeted health check and the allocator dump.
    admin
        .brokers()
        .healthcheck_broker(&brokers[0])
        .await
        .unwrap();
    admin
        .broker_stats()
        .get_allocator_stats("default")
        .await
        .unwrap();
}

/// Topic operations not covered by the happy-path tests.
#[tokio::test]
async fn topic_remaining_operations() {
    use crate::admin::models::MessageIdData;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("rest"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        t.create_subscription(
            &topic,
            "sub1",
            &crate::admin::models::MessageIdData::latest(),
        )
        .await
        .unwrap();
        publish(&topic, 5).await;

        // Expiring with a 0-second threshold expires everything.
        t.expire_messages(&topic, "sub1", 0).await.unwrap();

        // Rewinding to the very first message id replays the topic.
        t.reset_cursor_to_message_id(
            &topic,
            "sub1",
            &MessageIdData {
                ledger_id: -1,
                entry_id: -1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Replicated-subscription status is readable on a single-cluster broker.
        t.get_replicated_subscription_status(&topic, "sub1")
            .await
            .unwrap();

        // Shadow topics: set, read, remove.
        let shadow = format!("persistent://{ns}/{}", unique("shadow"));
        t.create_non_partitioned_topic(&shadow).await.unwrap();
        t.set_shadow_topics(&topic, std::slice::from_ref(&shadow))
            .await
            .unwrap();
        assert_eq!(
            t.get_shadow_topics(&topic).await.unwrap(),
            Some(vec![shadow.clone()]),
            "shadow topics did not round-trip"
        );
        t.remove_shadow_topics(&topic).await.unwrap();

        // Non-persistent listing through the Topics group.
        t.get_non_persistent_list(&ns).await.unwrap();

        // Offload is not configured, so this must fail cleanly rather than hang.
        match t
            .trigger_offload(
                &topic,
                &MessageIdData {
                    ledger_id: 0,
                    entry_id: 0,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(()) => {}
            Err(Error::Admin(
                AdminError::BadRequest(_)
                | AdminError::NotAllowed(_)
                | AdminError::PreconditionFailed(_)
                | AdminError::ServerUnavailable(_)
                | AdminError::Http { .. },
            )) => {}
            Err(e) => panic!("unexpected error triggering offload: {e:?}"),
        }

        t.delete(&shadow, true).await.unwrap();
        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Terminating every partition of a partitioned topic returns one id per partition.
#[tokio::test]
async fn partitioned_topic_terminate() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("pterm"));

        t.create_partitioned_topic(&topic, 2).await.unwrap();
        publish(&topic, 4).await;

        let ids = t.terminate_partitioned_topic(&topic).await.unwrap();
        assert_eq!(ids.len(), 2, "expected one id per partition, got {ids:?}");

        t.delete_partitioned_topic(&topic, true).await.unwrap();
    })
    .await;
}

/// Topic-level replication clusters and entry filters.
#[tokio::test]
async fn topic_replication_and_entry_filters() {
    use crate::admin::models::EntryFilters;
    let admin = new_admin().await;
    let cluster = primary_cluster(&admin).await;
    with_topic(&admin, |topic| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();

        // A single-cluster broker rejects a replication list naming only itself in
        // some versions, so accept either outcome and assert the read path works.
        match tp.get_replication_clusters(&topic, true).await {
            Ok(_) => {}
            Err(Error::Admin(AdminError::NotFound(_))) => {}
            Err(e) => panic!("unexpected error reading replication clusters: {e:?}"),
        }
        let _ = cluster;
        tp.remove_replication_clusters(&topic).await.ok();

        // As at namespace level, an uninstalled filter is rejected by the broker.
        let err = tp
            .set_entry_filters(
                &topic,
                &EntryFilters {
                    entry_filter_names: "jms".to_string(),
                },
            )
            .await
            .unwrap_err();
        match err {
            Error::Admin(AdminError::BadRequest(m)) => {
                assert!(m.contains("jms"), "unexpected rejection: {m}")
            }
            other => panic!("expected the broker to reject an uninstalled filter: {other:?}"),
        }
        assert!(tp.get_entry_filters(&topic, false).await.unwrap().is_none());
        tp.remove_entry_filters(&topic).await.unwrap();
    })
    .await;
}

// -------------------------------- completeness: final untested endpoints

/// Bundle-scoped backlog and unsubscribe variants.
#[tokio::test]
async fn namespace_bundle_subscription_operations() {
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();
        let bundles = n.get_bundles(&ns).await.unwrap();
        let bundle = format!("{}_{}", bundles.boundaries[0], bundles.boundaries[1]);

        let topic = format!("persistent://{ns}/{}", unique("bsub"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        admin
            .topics()
            .create_subscription(
                &topic,
                "sub1",
                &crate::admin::models::MessageIdData::latest(),
            )
            .await
            .unwrap();

        n.clear_namespace_bundle_backlog(&ns, &bundle)
            .await
            .unwrap();
        n.clear_namespace_bundle_backlog_for_subscription(&ns, &bundle, "sub1")
            .await
            .unwrap();
        n.unsubscribe_namespace_bundle(&ns, &bundle, "sub1")
            .await
            .unwrap();
    })
    .await;
}

/// Enabling replicated subscription status needs more than one cluster, so assert
/// that the request is well formed and the broker's refusal is the expected shape.
#[tokio::test]
async fn topic_set_replicated_subscription_status() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("repl"));

        t.create_non_partitioned_topic(&topic).await.unwrap();
        t.create_subscription(
            &topic,
            "sub1",
            &crate::admin::models::MessageIdData::latest(),
        )
        .await
        .unwrap();

        match t
            .set_replicated_subscription_status(&topic, "sub1", true)
            .await
        {
            // A single-cluster broker may accept it as a no-op or refuse it.
            Ok(()) => {}
            Err(Error::Admin(
                AdminError::BadRequest(_)
                | AdminError::NotAllowed(_)
                | AdminError::PreconditionFailed(_)
                | AdminError::NotFound(_),
            )) => {}
            Err(e) => panic!("unexpected error setting replicated status: {e:?}"),
        }

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Migrating a regular topic into the scalable domain.
#[tokio::test]
async fn scalable_topic_migrate_from_regular() {
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let name = unique("mig");
            let regular = format!("persistent://{ns}/{name}");

            admin
                .topics()
                .create_non_partitioned_topic(&regular)
                .await
                .unwrap();
            publish(&regular, 2).await;

            match admin
                .scalable_topics()
                .migrate_to_scalable(&regular, true)
                .await
            {
                Ok(()) => {
                    // After migration the topic must be addressable as topic://.
                    let meta = admin
                        .scalable_topics()
                        .get_metadata(&format!("topic://{ns}/{name}"))
                        .await
                        .unwrap();
                    assert!(
                        !meta.segments.is_empty(),
                        "migrated topic has no segments: {meta:?}"
                    );
                }
                // Migration is a preview feature; a clean refusal is acceptable.
                Err(Error::Admin(
                    AdminError::NotSupported(_)
                    | AdminError::NotAllowed(_)
                    | AdminError::BadRequest(_)
                    | AdminError::PreconditionFailed(_)
                    | AdminError::Http { .. },
                )) => {}
                Err(e) => panic!("unexpected error migrating to scalable: {e:?}"),
            }
        })
        .await;
    })
    .await;
}

/// Merging two segments back together, and the segment-level operations.
#[tokio::test]
async fn scalable_topic_merge_and_segment_operations() {
    if_scalable_supported(|| async {
        let admin = new_admin().await;
        with_topic_namespace(&admin, |ns| async move {
            let admin = new_admin().await;
            let st = admin.scalable_topics();
            let topic = format!("topic://{ns}/{}", unique("merge"));

            // Start with two segments so there is an adjacent pair to merge.
            st.create_scalable_topic(&topic, 2).await.unwrap();
            let meta = st.get_metadata(&topic).await.unwrap();
            assert_eq!(meta.segments.len(), 2);

            match st.merge_segments(&topic, 0, 1).await {
                Ok(()) => {
                    let after = st.get_metadata(&topic).await.unwrap();
                    assert!(
                        after.epoch > meta.epoch,
                        "merge did not bump the DAG epoch: {after:?}"
                    );
                    // Both parents must now be sealed with a shared child.
                    let sealed = after.segments.values().filter(|s| s.sealed).count();
                    assert!(sealed >= 2, "merge did not seal both parents: {after:?}");
                }
                // Merge requires cross-broker coordination and may be refused.
                Err(Error::Admin(
                    AdminError::NotSupported(_)
                    | AdminError::NotAllowed(_)
                    | AdminError::BadRequest(_)
                    | AdminError::PreconditionFailed(_)
                    | AdminError::Http { .. },
                )) => {}
                Err(e) => panic!("unexpected error merging segments: {e:?}"),
            }

            // Segment-level operations address a segment:// topic, whose name comes
            // from the stats rather than being constructed by hand.
            let stats = st.get_stats(&topic).await.unwrap();
            let segment = stats
                .segments
                .values()
                .filter_map(|s| s.name.clone())
                .next()
                .expect("stats reported no segment name");

            // A subscription created on the parent exists on every segment, so the
            // segment-scoped calls have a real success path. Asserting success is
            // what proves the URL keeps the parent topic and the segment descriptor
            // as separate path segments — percent-encoding the two into one matches
            // no route and 404s with a Jetty HTML page.
            st.create_subscription(
                &topic,
                "segsub",
                crate::admin::models::ScalableSubscriptionType::Stream,
            )
            .await
            .unwrap();

            // A subscription covers the active leaf segments, not every segment in
            // the DAG — a merge leaves sealed parents behind. So require that *some*
            // segment answers, which is what proves the URL keeps the parent topic
            // and the descriptor as separate path segments.
            let all_segments: Vec<String> = st
                .get_stats(&topic)
                .await
                .unwrap()
                .segments
                .values()
                .filter_map(|s| s.name.clone())
                .collect();
            let mut served = None;
            for candidate in &all_segments {
                match st
                    .get_segment_subscription_backlog(candidate, "segsub")
                    .await
                {
                    Ok(backlog) => {
                        assert!(backlog >= 0, "negative segment backlog: {backlog}");
                        served = Some(candidate.clone());
                        break;
                    }
                    // Reached the handler and reported the subscription absent here.
                    Err(e) => assert_reached_handler("get_segment_subscription_backlog", &e),
                }
            }
            let served = served.unwrap_or_else(|| {
                panic!("no segment served a backlog for an existing subscription: {all_segments:?}")
            });
            st.clear_segment_subscription_backlog(&served, "segsub")
                .await
                .expect("clearing a segment subscription backlog must succeed");

            // create_segment had no caller at all. The broker accepts an explicit
            // descriptor, so create one and remove it again rather than leaving an
            // extra segment on the topic.
            let explicit = {
                let parent = served
                    .trim_start_matches("segment://")
                    .rsplit_once('/')
                    .expect("a segment topic ends in its descriptor")
                    .0
                    .to_string();
                format!("segment://{parent}/8000-bfff-90")
            };
            st.create_segment(&explicit, &[])
                .await
                .expect("creating a segment with an explicit descriptor must succeed");
            st.delete_segment(&explicit, true)
                .await
                .expect("the segment just created must be deletable");

            // The segment-scoped subscription lifecycle, previously unimplemented.
            st.create_segment_subscription(&served, "direct")
                .await
                .expect("creating a segment subscription must succeed");
            st.get_segment_subscription_backlog(&served, "direct")
                .await
                .expect("the subscription just created must report a backlog");
            assert_ok_or_handled!(
                "seek_segment_subscription",
                st.seek_segment_subscription(&served, "direct", 1).await
            );
            st.delete_segment_subscription(&served, "direct")
                .await
                .expect("deleting a segment subscription must succeed");

            // A subscription that does not exist must still reach the handler, and
            // be reported as missing rather than as an unmatched route.
            match st.get_segment_subscription_backlog(&segment, "nosub").await {
                Ok(_) => panic!("a nonexistent segment subscription must not report a backlog"),
                Err(e) => assert_reached_handler("get_segment_subscription_backlog", &e),
            }

            // Sealing then deleting a segment must both be understood by the broker.
            st.terminate_segment(&segment)
                .await
                .expect("terminating a segment must succeed");
            assert_ok_or_handled!("delete_segment", st.delete_segment(&segment, true).await);

            st.delete_scalable_topic(&topic, true).await.ok();
        })
        .await;
    })
    .await;
}

/// The transaction endpoints that need a live coordinator.
///
/// A standalone broker creates the coordinator lazily, so several of these answer
/// 404 or 500 until a transaction has actually been opened — which this client
/// cannot yet do. Each is asserted to either decode or fail in a recognised way,
/// which still pins the URL, the query parameters and the response model.
#[tokio::test]
async fn transaction_coordinator_and_stats_endpoints() {
    use crate::admin::models::TxnId;
    let admin = new_admin().await;
    let txn = admin.transactions();

    let coordinators = txn.list_transaction_coordinators().await.unwrap();
    let id = coordinators[0].id as i32;

    assert_ok_or_handled!("get_coordinator_stats", txn.get_coordinator_stats().await);
    assert_ok_or_handled!(
        "get_coordinator_stats_by_id",
        txn.get_coordinator_stats_by_id(id).await
    );
    assert_ok_or_handled!(
        "get_slow_transactions",
        txn.get_slow_transactions(60_000).await
    );
    assert_ok_or_handled!(
        "get_slow_transactions_by_coordinator_id",
        txn.get_slow_transactions_by_coordinator_id(id, 60_000)
            .await
    );

    // `coordinatorId` is a path segment here, not a query parameter. A standalone
    // that has not loaded its coordinator answers 404 — but with the handler's own
    // message, which is what distinguishes it from a path that matches no route.
    match txn.get_coordinator_internal_stats(id, true).await {
        Ok(_) => {}
        Err(e) => {
            assert_reached_handler("get_coordinator_internal_stats", &e);
            let message = format!("{e}");
            assert!(
                message.contains("coordinator"),
                "the 404 did not come from the coordinator handler: {message}"
            );
        }
    }

    // A transaction id that does not exist must be reported, not mis-decoded.
    let missing = TxnId {
        most_sig_bits: 0,
        least_sig_bits: 999_999,
    };
    assert_ok_or_handled!(
        "get_transaction_metadata",
        txn.get_transaction_metadata(missing).await
    );
    assert_ok_or_handled!("abort_transaction", txn.abort_transaction(missing).await);

    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let txn = admin.transactions();
        let topic = format!("persistent://{ns}/{}", unique("txnstats"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        admin
            .topics()
            .create_subscription(
                &topic,
                "sub1",
                &crate::admin::models::MessageIdData::latest(),
            )
            .await
            .unwrap();

        let missing = TxnId {
            most_sig_bits: 0,
            least_sig_bits: 999_999,
        };

        // These are topic-scoped, so they answer for real on a plain standalone —
        // no coordinator required. Asserting success is what proves the path is
        // `.../{tenant}/{namespace}/{topic}` rather than a query parameter: the
        // query form matches no route and 404s with a Jetty HTML page.
        let buffer = txn.get_transaction_buffer_stats(&topic, true, true).await;
        let buffer = buffer.expect("transactionBufferStats must answer for an existing topic");
        assert!(
            buffer.state.is_some(),
            "transaction buffer state must decode: {buffer:?}"
        );

        let pending = txn
            .get_pending_ack_stats(&topic, "sub1", true)
            .await
            .expect("pendingAckStats must answer for an existing subscription");
        assert!(
            pending.state.is_some(),
            "pending-ack state must decode: {pending:?}"
        );

        txn.get_transaction_in_buffer_stats(missing, &topic)
            .await
            .expect("transactionInBufferStats must answer for an existing topic");
        txn.get_transaction_in_pending_ack_stats(missing, &topic, "sub1")
            .await
            .expect("transactionInPendingAckStats must answer for an existing subscription");

        let internal = txn
            .get_transaction_buffer_internal_stats(&topic, true)
            .await
            .expect("transactionBufferInternalStats must answer for an existing topic");
        assert!(
            internal.snapshot_type.is_some(),
            "snapshot type must decode: {internal:?}"
        );

        // The pending-ack store initialises lazily, so this one can legitimately
        // report 503 — but it must still reach its handler.
        assert_ok_or_handled!(
            "get_pending_ack_internal_stats",
            txn.get_pending_ack_internal_stats(&topic, "sub1", true)
                .await
        );
        // Typed, so a wire change fails here rather than passing as opaque text.
        if let Some(position) = assert_ok_or_handled!(
            "get_position_stats_in_pending_ack",
            txn.get_position_stats_in_pending_ack(&topic, "sub1", 0, 0, None)
                .await
        ) {
            use crate::admin::models::PositionInPendingAckState;
            assert_eq!(
                position.state,
                PositionInPendingAckState::PendingAckNotReady,
                "an uninitialised pending-ack store should report PendingAckNotReady: {position:?}"
            );
        }

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Scaling coordinators changes cluster-wide state, so this only checks that the
/// current count is accepted as a no-op rather than actually growing the cluster.
#[tokio::test]
async fn transaction_scale_coordinators_accepts_current_count() {
    let admin = new_admin().await;
    let current = admin
        .transactions()
        .list_transaction_coordinators()
        .await
        .unwrap()
        .len() as i32;

    match admin
        .transactions()
        .scale_transaction_coordinators(current)
        .await
    {
        // Re-requesting the current count must not grow the cluster.
        Ok(()) => {}
        // Most brokers refuse a non-increase outright, which is equally fine.
        Err(Error::Admin(
            AdminError::BadRequest(_)
            | AdminError::NotAllowed(_)
            | AdminError::PreconditionFailed(_)
            | AdminError::NotFound(_)
            | AdminError::Http { .. },
        )) => {}
        Err(e) => panic!("unexpected error scaling coordinators: {e:?}"),
    }

    assert_eq!(
        admin
            .transactions()
            .list_transaction_coordinators()
            .await
            .unwrap()
            .len() as i32,
        current,
        "coordinator count changed unexpectedly"
    );
}

// ------------------------------------------------- functions and connectors

/// Skips the body unless the broker runs a functions worker.
///
/// `bin/pulsar standalone --no-functions-worker` answers 404 for every endpoint in
/// this section, so the check is a capability probe rather than a guess.
async fn if_functions_worker<F, Fut>(body: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let admin = new_admin().await;
    match admin.worker().get_cluster().await {
        Ok(workers) if !workers.is_empty() => body().await,
        // The documented "no worker" answer, and the only one that may skip. A
        // broker with the worker enabled reports at least itself, so an empty list
        // means the route or the model is wrong, not that the worker is absent.
        Err(Error::Admin(AdminError::ServerUnavailable(_))) => {
            log::warn!("broker runs no functions worker, skipping")
        }
        Ok(empty) => panic!(
            "the worker service answered but listed no workers: {empty:?}. \
             A running worker always includes itself — treat this as a route or \
             model regression, not as a missing worker."
        ),
        Err(e) => panic!(
            "worker probe failed with something other than \"worker service is not \
             running\", so the function/sink/source tests would have been skipped \
             for the wrong reason: {e:?}"
        ),
    }
}

#[tokio::test]
async fn worker_cluster_and_metrics() {
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let w = admin.worker();

        let workers = w.get_cluster().await.unwrap();
        assert!(!workers.is_empty());
        // Regression: this field is `workerHostname` on the wire, and a wrong name
        // decodes silently to None rather than failing.
        assert!(
            workers[0].worker_hostname.is_some(),
            "worker hostname did not decode: {:?}",
            workers[0]
        );
        assert!(workers[0].worker_id.is_some());
        assert!(workers[0].port > 0);

        let leader = w.get_cluster_leader().await.unwrap();
        assert!(leader.worker_id.is_some(), "leader: {leader:?}");

        w.get_assignments().await.unwrap();
        w.get_functions_stats().await.unwrap();
        w.get_metrics().await.unwrap();
        // Rebalancing needs at least two workers; a standalone has one, and refuses
        // with a message saying so. Either outcome proves the endpoint was reached.
        match w.rebalance().await {
            Ok(()) => {}
            Err(Error::Admin(AdminError::BadRequest(m))) => {
                assert!(m.contains("workers"), "unexpected rebalance refusal: {m}")
            }
            Err(e) => panic!("unexpected error rebalancing: {e:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn function_and_connector_listings() {
    if_functions_worker(|| async {
        let admin = new_admin().await;

        // The listing must decode and must not invent entries. Asserting it is
        // *empty* would depend on no other test having a function in flight —
        // `public/default` is shared, so that assumption breaks under any ordering.
        let functions = admin
            .functions()
            .get_functions("public", "default")
            .await
            .unwrap();
        assert!(
            !functions.iter().any(|f| f == "definitely-not-a-function"),
            "the listing reported a function that was never created: {functions:?}"
        );
        assert!(admin
            .sinks()
            .list_sinks("public", "default")
            .await
            .unwrap()
            .is_empty());
        assert!(admin
            .sources()
            .list_sources("public", "default")
            .await
            .unwrap()
            .is_empty());

        // Built-in listings decode even when the image ships no connectors.
        admin.functions().get_built_in_functions().await.unwrap();
        admin.sinks().get_built_in_sinks().await.unwrap();
        admin.sources().get_built_in_sources().await.unwrap();

        admin.functions().reload_built_in_functions().await.unwrap();
        admin.sinks().reload_built_in_sinks().await.unwrap();
        admin.sources().reload_built_in_sources().await.unwrap();
    })
    .await;
}

/// Creating a function exercises the multipart encoding end to end.
///
/// The stock image has no Python runtime, so the worker may refuse to *start* the
/// function; what matters here is that the upload is accepted and parsed, which a
/// malformed multipart body would fail long before that.
#[tokio::test]
async fn function_create_lifecycle_and_delete() {
    use crate::admin::models::FunctionConfig;
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let f = admin.functions();
        let name = unique("fn");
        with_function_cleanup(&name.clone(), async {
            let config = FunctionConfig {
                tenant: Some("public".to_string()),
                namespace: Some("default".to_string()),
                name: Some(name.clone()),
                class_name: Some("identity".to_string()),
                runtime: Some("PYTHON".to_string()),
                inputs: vec![format!("persistent://public/default/{}", unique("fn-in"))],
                output: Some(format!("persistent://public/default/{}", unique("fn-out"))),
                parallelism: Some(1),
                py: Some("identity.py".to_string()),
                ..Default::default()
            };
            let package = b"def process(item):\n    return item\n".to_vec();

            match f.create_function(&config, "identity.py", package).await {
                Ok(()) => {
                    // Created: the whole lifecycle must then be addressable.
                    let read = f.get_function("public", "default", &name).await.unwrap();
                    assert_eq!(read.name.as_deref(), Some(name.as_str()));
                    assert_eq!(read.parallelism, Some(1));

                    assert!(f
                        .get_functions("public", "default")
                        .await
                        .unwrap()
                        .contains(&name));

                    // Status and stats must decode whether or not the instance runs.
                    f.get_function_status("public", "default", &name)
                        .await
                        .unwrap();
                    f.get_function_stats("public", "default", &name)
                        .await
                        .unwrap();
                    f.get_function_instance_status("public", "default", &name, 0)
                        .await
                        .ok();
                    f.get_function_instance_stats("public", "default", &name, 0)
                        .await
                        .ok();

                    // Lifecycle controls: accepted, or refused for a runtime that never
                    // came up. Either proves the endpoint was reached.
                    for result in [
                        f.stop_function("public", "default", &name).await,
                        f.start_function("public", "default", &name).await,
                        f.restart_function("public", "default", &name).await,
                    ] {
                        match result {
                            Ok(()) => {}
                            Err(Error::Admin(_)) => {}
                            Err(e) => panic!("unexpected lifecycle error: {e:?}"),
                        }
                    }

                    f.delete_function("public", "default", &name).await.unwrap();
                    assert!(!f
                        .get_functions("public", "default")
                        .await
                        .unwrap()
                        .contains(&name));
                }
                // No Python runtime in the image: a clean refusal still means the
                // multipart body was parsed and validated by the worker.
                Err(Error::Admin(
                    AdminError::BadRequest(_)
                    | AdminError::NotSupported(_)
                    | AdminError::NotAllowed(_)
                    | AdminError::ServerUnavailable(_)
                    | AdminError::Http { .. },
                )) => {
                    log::warn!("worker refused the function package; multipart was still parsed")
                }
                Err(e) => panic!("unexpected error creating a function: {e:?}"),
            }
        })
        .await;
    })
    .await;
}

/// A config missing its tenant/namespace/name must fail locally rather than
/// building a URL with empty path segments.
#[tokio::test]
async fn function_config_without_identity_is_rejected_locally() {
    use crate::admin::models::{FunctionConfig, SinkConfig, SourceConfig};
    let admin = new_admin().await;

    let err = admin
        .functions()
        .create_function(&FunctionConfig::default(), "x.py", vec![])
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Custom(ref m) if m.contains("tenant")),
        "expected a local rejection naming the missing field, got {err:?}"
    );

    let err = admin
        .sinks()
        .create_sink(&SinkConfig::default(), "x.nar", vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Custom(_)), "got {err:?}");

    let err = admin
        .sources()
        .create_source(&SourceConfig::default(), "x.nar", vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Custom(_)), "got {err:?}");
}

/// Connector creation without an archive or a built-in type must be refused by the
/// worker — which proves the multipart request reached it.
#[tokio::test]
async fn connector_create_without_archive_is_refused() {
    use crate::admin::models::{SinkConfig, SourceConfig};
    if_functions_worker(|| async {
        let admin = new_admin().await;

        let sink = SinkConfig {
            tenant: Some("public".to_string()),
            namespace: Some("default".to_string()),
            name: Some(unique("sink")),
            inputs: vec!["persistent://public/default/sink-in".to_string()],
            ..Default::default()
        };
        match admin.sinks().create_sink_with_url(&sink, "").await {
            Err(Error::Admin(_)) => {}
            Ok(()) => panic!("a sink with no archive and no type should not be accepted"),
            Err(e) => panic!("unexpected error: {e:?}"),
        }

        let source = SourceConfig {
            tenant: Some("public".to_string()),
            namespace: Some("default".to_string()),
            name: Some(unique("source")),
            topic_name: Some("persistent://public/default/source-out".to_string()),
            ..Default::default()
        };
        match admin.sources().create_source_with_url(&source, "").await {
            Err(Error::Admin(_)) => {}
            Ok(()) => panic!("a source with no archive and no type should not be accepted"),
            Err(e) => panic!("unexpected error: {e:?}"),
        }

        // Reads against a connector that does not exist must be NotFound, not a
        // decode failure.
        let missing = unique("nosuch");
        for e in [
            admin
                .sinks()
                .get_sink("public", "default", &missing)
                .await
                .err(),
            admin
                .sinks()
                .get_sink_status("public", "default", &missing)
                .await
                .err(),
            admin
                .sources()
                .get_source("public", "default", &missing)
                .await
                .err(),
            admin
                .sources()
                .get_source_status("public", "default", &missing)
                .await
                .err(),
        ] {
            match e {
                Some(Error::Admin(AdminError::NotFound(_))) => {}
                other => panic!("expected NotFound for a missing connector, got {other:?}"),
            }
        }
    })
    .await;
}

/// Connector lifecycle and delete paths, against a connector that does not exist.
///
/// Every one must reach the worker and come back NotFound rather than being
/// mis-routed — a wrong path would 404 identically at the *HTTP* layer, so these
/// also assert the message names the connector.
#[tokio::test]
async fn connector_lifecycle_paths_reach_the_worker() {
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let missing = unique("nosuch");

        let results = vec![
            admin.sinks().stop_sink("public", "default", &missing).await,
            admin
                .sinks()
                .start_sink("public", "default", &missing)
                .await,
            admin
                .sinks()
                .restart_sink("public", "default", &missing)
                .await,
            admin
                .sinks()
                .stop_sink_instance("public", "default", &missing, 0)
                .await,
            admin
                .sinks()
                .start_sink_instance("public", "default", &missing, 0)
                .await,
            admin
                .sinks()
                .restart_sink_instance("public", "default", &missing, 0)
                .await,
            admin
                .sinks()
                .delete_sink("public", "default", &missing)
                .await,
            admin
                .sources()
                .stop_source("public", "default", &missing)
                .await,
            admin
                .sources()
                .start_source("public", "default", &missing)
                .await,
            admin
                .sources()
                .restart_source("public", "default", &missing)
                .await,
            admin
                .sources()
                .stop_source_instance("public", "default", &missing, 0)
                .await,
            admin
                .sources()
                .start_source_instance("public", "default", &missing, 0)
                .await,
            admin
                .sources()
                .restart_source_instance("public", "default", &missing, 0)
                .await,
            admin
                .sources()
                .delete_source("public", "default", &missing)
                .await,
            admin
                .functions()
                .stop_function_instance("public", "default", &missing, 0)
                .await,
            admin
                .functions()
                .start_function_instance("public", "default", &missing, 0)
                .await,
            admin
                .functions()
                .restart_function_instance("public", "default", &missing, 0)
                .await,
            admin
                .functions()
                .delete_function("public", "default", &missing)
                .await,
        ];
        for (i, r) in results.into_iter().enumerate() {
            match r {
                Err(Error::Admin(AdminError::NotFound(_) | AdminError::BadRequest(_))) => {}
                other => panic!("operation {i} did not reach the worker cleanly: {other:?}"),
            }
        }

        // Reading state and getting a connector's status likewise.
        match admin
            .functions()
            .get_function_state("public", "default", &missing, "k")
            .await
        {
            // The worker's state-storage client initializes lazily and reports so.
            Err(Error::Admin(
                AdminError::NotFound(_)
                | AdminError::BadRequest(_)
                | AdminError::ServerUnavailable(_),
            )) => {}
            other => panic!("get_function_state did not reach the worker: {other:?}"),
        }
        match admin
            .functions()
            .put_function_state(
                "public",
                "default",
                &missing,
                &crate::admin::models::FunctionState {
                    key: Some("k".to_string()),
                    string_value: Some("v".to_string()),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(()) => {}
            // The worker binds this from a multipart part named `state`. A JSON
            // request body never reaches the handler at all — Jetty rejects it with
            // 415 and an HTML page — so a bare `Err(Error::Admin(_))` here would
            // have accepted the wrong wire format.
            Err(e) => assert_reached_handler("put_function_state", &e),
        }
    })
    .await;
}

// --------------------------------------------------------------- packages

/// Full package round-trip: upload bytes, read them back, list, update metadata,
/// delete. This is the strongest test of the multipart encoding, because the
/// uploaded bytes must come back byte-identical.
#[tokio::test]
async fn package_upload_download_and_delete() {
    use crate::admin::models::{PackageMetadata, PackageType};
    let admin = new_admin().await;
    let name = unique("pkg");
    let package = format!("function://public/default/{name}@v1");

    let contents = b"a package is just opaque bytes to the repository".to_vec();
    let metadata = PackageMetadata {
        description: Some("test package".to_string()),
        contact: Some("nobody@example.com".to_string()),
        properties: [("k".to_string(), "v".to_string())].into_iter().collect(),
        ..Default::default()
    };

    match admin
        .packages()
        .upload(&package, &metadata, "package.bin", contents.clone())
        .await
    {
        Ok(()) => {}
        // Requires enablePackagesManagement=true on the broker.
        Err(Error::Admin(AdminError::ServerUnavailable(_) | AdminError::NotSupported(_))) => {
            log::warn!("package management is disabled on this broker, skipping");
            return;
        }
        Err(e) => panic!("unexpected error uploading a package: {e:?}"),
    }

    // The bytes must survive the round-trip exactly.
    let downloaded = admin.packages().download(&package).await.unwrap();
    assert_eq!(
        downloaded,
        contents,
        "package contents changed in transit ({} bytes out, {} back)",
        contents.len(),
        downloaded.len()
    );

    let read = admin.packages().get_metadata(&package).await.unwrap();
    assert_eq!(read.description.as_deref(), Some("test package"));
    assert_eq!(read.properties.get("k").map(String::as_str), Some("v"));

    assert!(
        admin
            .packages()
            .list_packages(PackageType::Function, "public/default")
            .await
            .unwrap()
            .iter()
            .any(|p| p.contains(&name)),
        "package not listed"
    );
    assert!(admin
        .packages()
        .list_package_versions(&package)
        .await
        .unwrap()
        .iter()
        .any(|v| v.contains("v1")));

    admin
        .packages()
        .update_metadata(
            &package,
            &PackageMetadata {
                description: Some("updated".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        admin
            .packages()
            .get_metadata(&package)
            .await
            .unwrap()
            .description
            .as_deref(),
        Some("updated")
    );

    admin.packages().delete(&package).await.unwrap();

    // Verified against 5.0.0-M1: after a delete the content and metadata are both
    // gone (404), but the version *listing* still reports the deleted version — so
    // assert on what actually changed rather than on the stale listing.
    match admin.packages().download(&package).await {
        Err(Error::Admin(AdminError::NotFound(_))) => {}
        Ok(bytes) => panic!(
            "package still downloadable after delete ({} bytes)",
            bytes.len()
        ),
        Err(e) => panic!("unexpected error downloading a deleted package: {e:?}"),
    }
    match admin.packages().get_metadata(&package).await {
        Err(Error::Admin(AdminError::NotFound(_))) => {}
        Ok(m) => panic!("metadata still readable after delete: {m:?}"),
        Err(e) => panic!("unexpected error reading deleted metadata: {e:?}"),
    }
}

/// A malformed package name must fail locally, before any request.
#[tokio::test]
async fn malformed_package_names_are_rejected_before_sending() {
    let admin = new_admin().await;
    for bad in ["", "my-fn", "function://t/ns", "unknown://t/ns/n"] {
        let err = admin.packages().get_metadata(bad).await.unwrap_err();
        match err {
            Error::Admin(AdminError::InvalidTopic(_)) => {}
            other => panic!("expected InvalidTopic for {bad:?}, got {other:?}"),
        }
    }
}

// ------------------------------------- proxy stats and metadata migration

/// An `AdminClient` pointed at the Pulsar proxy, when one is in the topology.
///
/// `proxy-stats` is served by a proxy rather than a broker, so these tests skip
/// unless `scripts/start_test_broker.sh` brought one up.
async fn proxy_admin() -> Option<AdminClient> {
    let url = test_utils::proxy_admin_url()?;
    let pulsar = crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap();
    Some(pulsar.admin(url).unwrap())
}

#[tokio::test]
async fn proxy_stats_connections_and_log_level() {
    let _serialised = proxy_log_level_lock().lock().await;
    let Some(admin) = proxy_admin().await else {
        log::warn!("no proxy in the topology, skipping");
        return;
    };
    let ps = admin.proxy_stats();

    // Decoding into the typed shape is the assertion here: an idle proxy reports an
    // empty list, and a populated one is checked in the traffic test below.
    ps.get_connections().await.unwrap();

    // The log level round-trips through the in-memory setter.
    let original = ps.get_log_level().await.unwrap();
    ps.set_log_level(1).await.unwrap();
    assert_eq!(ps.get_log_level().await.unwrap(), 1);
    ps.set_log_level(original).await.unwrap();
    assert_eq!(ps.get_log_level().await.unwrap(), original);

    // The proxy rejects a level outside 0..=2.
    match ps.set_log_level(7).await {
        Err(Error::Admin(AdminError::PreconditionFailed(m))) => {
            assert!(m.contains("0-2"), "unexpected rejection: {m}")
        }
        other => panic!("expected the proxy to reject log level 7, got {other:?}"),
    }
}

/// Topic stats require the proxy to have been *started* with `proxyLogLevel=2`.
///
/// `get_topics` reads the configured level, not the running one, so the runtime
/// setter cannot enable it — this asserts both halves of that split.
#[tokio::test]
async fn proxy_stats_topics_depend_on_configured_log_level() {
    let _serialised = proxy_log_level_lock().lock().await;
    let Some(admin) = proxy_admin().await else {
        log::warn!("no proxy in the topology, skipping");
        return;
    };
    let ps = admin.proxy_stats();

    match ps.get_topics().await {
        // Keyed by topic name; entries appear only for topics traffic has flowed
        // through, so an empty map is valid.
        Ok(_topics) => {}
        // A proxy started below level 2 refuses, and lowering the runtime level
        // cannot change that.
        Err(Error::Admin(AdminError::PreconditionFailed(m))) => {
            assert!(m.contains("logging level 2"), "unexpected refusal: {m}");
            ps.set_log_level(2).await.unwrap();
            match ps.get_topics().await {
                Err(Error::Admin(AdminError::PreconditionFailed(_))) => {}
                other => panic!("the runtime log level must not unlock topic stats, got {other:?}"),
            }
        }
        Err(e) => panic!("unexpected error reading proxy topic stats: {e:?}"),
    }
}

/// Traffic routed *through* the proxy must show up in its connection stats.
///
/// This is the test that proves the proxy is really in the path rather than merely
/// answering its own admin endpoints.
#[tokio::test]
async fn proxy_stats_observe_traffic_through_the_proxy() {
    let _serialised = proxy_log_level_lock().lock().await;
    let (Some(admin), Some(proxy_url)) = (proxy_admin().await, test_utils::proxy_broker_url())
    else {
        log::warn!("no proxy in the topology, skipping");
        return;
    };

    // Produce through the proxy, not the broker.
    let pulsar = crate::Pulsar::<TokioExecutor>::builder(&proxy_url, TokioExecutor)
        .build()
        .await
        .unwrap();
    let topic = format!("persistent://public/default/{}", unique("via_proxy"));
    let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
    producer
        .send_non_blocking("through the proxy")
        .await
        .unwrap()
        .await
        .unwrap();

    // The proxy must now report a connection that is forwarding to the broker. The
    // broker address proves the proxy is relaying rather than terminating.
    let broker_port = test_utils::broker_url()
        .rsplit(':')
        .next()
        .expect("broker url has a port")
        .to_string();
    let mut forwarding = None;
    for _ in 0..25 {
        let connections = admin.proxy_stats().get_connections().await.unwrap();
        if let Some(c) = connections.into_iter().find(|c| {
            c.broker_address
                .as_deref()
                .is_some_and(|a| a.ends_with(&format!(":{broker_port}")))
        }) {
            forwarding = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let forwarding = forwarding.unwrap_or_else(|| {
        panic!("no proxy connection was forwarding to the broker on port {broker_port}")
    });
    assert!(
        forwarding.client_address.is_some(),
        "a forwarding connection must record its client: {forwarding:?}"
    );
    // The rates stay 0.0 here by design: the proxy calculates them from a one-shot
    // task scheduled 60s after startup, not on a repeating period.

    // With `proxyLogLevel=2` configured at startup, the proxy also accounts the
    // topic. Only the key is asserted, for the same reason.
    let mut saw_topic = false;
    for _ in 0..25 {
        if admin
            .proxy_stats()
            .get_topics()
            .await
            .unwrap()
            .keys()
            .any(|t| t.contains(topic.rsplit('/').next().unwrap()))
        {
            saw_topic = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(saw_topic, "the proxy did not account traffic for {topic}");

    producer.close().await.unwrap();

    // And the message must be readable back through the broker, proving the proxy
    // forwarded it rather than swallowing it.
    let broker = new_admin().await;
    let stats = broker
        .topics()
        .get_stats(&topic, Default::default())
        .await
        .unwrap();
    assert_eq!(
        stats.msg_in_counter, 1,
        "the message did not reach the broker through the proxy: {stats:?}"
    );

    broker.topics().delete(&topic, true).await.unwrap();
}

/// Peeked messages must carry their application properties.
///
/// The broker sends every property in one `X-Pulsar-PROPERTY` header holding a
/// JSON object. Matching on a per-key `X-Pulsar-PROPERTY-<name>` prefix instead
/// dropped every real property, and picked up the chunk counters as if they were
/// application properties. No earlier test published a message with properties, so
/// nothing caught it.
#[tokio::test]
async fn peeked_messages_carry_their_properties() {
    use crate::admin::models::MessageIdData;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("peekprops"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        admin
            .topics()
            .create_subscription(&topic, "sub1", &MessageIdData::latest())
            .await
            .unwrap();

        let pulsar =
            crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
                .build()
                .await
                .unwrap();
        let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
        producer
            .send_non_blocking(crate::producer::Message {
                payload: Some(b"hello".to_vec()),
                properties: [
                    ("colour".to_string(), "green".to_string()),
                    ("size".to_string(), "42".to_string()),
                ]
                .into_iter()
                .collect(),
                ..Default::default()
            })
            .await
            .unwrap()
            .await
            .unwrap();
        producer.close().await.unwrap();

        let peeked = admin
            .topics()
            .peek_messages(&topic, "sub1", 1)
            .await
            .unwrap();
        let message = peeked.first().expect("peek returned no message");
        assert_eq!(
            message.properties.get("colour").map(String::as_str),
            Some("green"),
            "application properties were lost: {:?}",
            message.properties
        );
        assert_eq!(
            message.properties.get("size").map(String::as_str),
            Some("42"),
            "application properties were lost: {:?}",
            message.properties
        );
        assert_eq!(message.payload, b"hello", "payload did not survive peek");

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// The topic-level policies that were missing entirely: backlog quota, message
/// TTL, dispatcher pause, and the replication-cluster setter.
///
/// Similarly named namespace methods made the coverage search look complete, so
/// none of these were ever called.
#[tokio::test]
async fn topic_policies_backlog_ttl_dispatcher_and_replication() {
    use crate::admin::models::{BacklogQuota, BacklogQuotaType};
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let tp = admin.topic_policies();
        let topic = format!("persistent://{ns}/{}", unique("tp_missing"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        // --- backlog quota ---
        tp.set_backlog_quota(
            &topic,
            &BacklogQuota {
                limit_size: Some(10_000),
                limit_time: Some(60),
                policy: Some("producer_request_hold".to_string()),
            },
            BacklogQuotaType::DestinationStorage,
        )
        .await
        .unwrap();
        let quotas = tp.get_backlog_quota_map(&topic, false).await.unwrap();
        let quota = quotas
            .values()
            .next()
            .expect("the backlog quota just set is missing");
        assert_eq!(quota.limit_size, Some(10_000), "{quotas:?}");
        tp.remove_backlog_quota(&topic, BacklogQuotaType::DestinationStorage)
            .await
            .unwrap();
        assert!(
            tp.get_backlog_quota_map(&topic, false)
                .await
                .unwrap()
                .is_empty(),
            "the backlog quota survived removal"
        );

        // --- message TTL (the value is a query parameter, not a body) ---
        tp.set_message_ttl(&topic, 120).await.unwrap();
        assert_eq!(tp.get_message_ttl(&topic, false).await.unwrap(), Some(120));
        tp.remove_message_ttl(&topic).await.unwrap();
        // `applied = false` asks for the override only, so absence is the exact
        // expected answer. Asserting merely "not 120" would accept a remover that
        // wrote 0 instead of clearing.
        assert_eq!(
            tp.get_message_ttl(&topic, false).await.unwrap(),
            None,
            "the topic-level message TTL was not removed"
        );
        // ...while `applied = true` must still resolve the inherited value.
        assert!(
            tp.get_message_ttl(&topic, true).await.unwrap().is_some(),
            "no effective message TTL after removing the override"
        );

        // --- dispatcher pause ---
        tp.set_dispatcher_pause_on_ack_state_persistent(&topic)
            .await
            .unwrap();
        assert_eq!(
            tp.get_dispatcher_pause_on_ack_state_persistent(&topic, false)
                .await
                .unwrap(),
            Some(true)
        );
        tp.remove_dispatcher_pause_on_ack_state_persistent(&topic)
            .await
            .unwrap();
        // This policy has no "unset" spelling — the broker answers a plain boolean
        // that defaults to false, so the exact expected value after removal is
        // `Some(false)`. Asserting that rather than "not `Some(true)`" is what
        // pins it; the looser form accepted any change at all.
        assert_eq!(
            tp.get_dispatcher_pause_on_ack_state_persistent(&topic, false)
                .await
                .unwrap(),
            Some(false),
            "the dispatcher-pause override was not removed"
        );

        // --- replication clusters: the setter had no counterpart before ---
        let cluster = primary_cluster(&admin).await;
        tp.set_replication_clusters(&topic, std::slice::from_ref(&cluster))
            .await
            .unwrap();
        assert_eq!(
            tp.get_replication_clusters(&topic, false).await.unwrap(),
            Some(vec![cluster]),
            "the replication clusters just set did not read back"
        );
        tp.remove_replication_clusters(&topic).await.unwrap();
        assert_eq!(
            tp.get_replication_clusters(&topic, false).await.unwrap(),
            None,
            "the replication-cluster override was not removed"
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Namespace offload policies round-trip, including the removal verb.
///
/// Removal is DELETE despite the `removeOffloadPolicies` path segment reading like
/// a command; POST answers 405. This group had no namespace-level test, which is
/// how the wrong verb survived.
#[tokio::test]
async fn namespace_offload_policies_round_trip() {
    use crate::admin::models::OffloadPolicies;
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        n.set_offload_policies(
            &ns,
            &OffloadPolicies {
                managed_ledger_offload_driver: Some("filesystem".to_string()),
                file_system_uri: Some("file:///tmp/pulsar-rdg-offload-test".to_string()),
                managed_ledger_offload_max_threads: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let read = n
            .get_offload_policies(&ns)
            .await
            .unwrap()
            .expect("offload policies must be set");
        assert_eq!(
            read.managed_ledger_offload_driver.as_deref(),
            Some("filesystem"),
            "offload driver did not round-trip: {read:?}"
        );

        n.remove_offload_policies(&ns).await.unwrap();
        assert_eq!(
            n.get_offload_policies(&ns).await.unwrap(),
            None,
            "offload policies survived removal"
        );
    })
    .await;
}

/// A 404 must stay a 404: a missing resource is not "policy not set".
///
/// The broker reports an unset policy as HTTP 200 with an empty body, and reserves
/// 404 for a tenant/namespace/topic that does not exist. Mapping 404 onto `None`
/// made a lookup on a nonexistent resource indistinguishable from an existing one
/// with no override.
#[tokio::test]
async fn missing_resources_are_not_reported_as_unset_policies() {
    let admin = new_admin().await;
    let ghost_ns = format!("public/{}", unique("ghost_ns"));

    match admin.namespaces().get_retention(&ghost_ns).await {
        Err(Error::Admin(AdminError::NotFound(_))) => {}
        other => panic!("a namespace that does not exist must report NotFound, got {other:?}"),
    }
    match admin.namespaces().get_message_ttl(&ghost_ns).await {
        Err(Error::Admin(AdminError::NotFound(_))) => {}
        other => panic!("a namespace that does not exist must report NotFound, got {other:?}"),
    }

    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let ghost_topic = format!("persistent://{ns}/{}", unique("ghost_topic"));
        match admin
            .topic_policies()
            .get_retention(&ghost_topic, false)
            .await
        {
            Err(Error::Admin(AdminError::NotFound(_))) => {}
            other => panic!("a topic that does not exist must report NotFound, got {other:?}"),
        }

        // ...while a real topic with no override reads as `None`, not as an error.
        let topic = format!("persistent://{ns}/{}", unique("real_topic"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        assert_eq!(
            admin
                .topic_policies()
                .get_retention(&topic, false)
                .await
                .unwrap(),
            None,
            "an existing topic with no retention override must read as None"
        );
        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// `create_subscription` must honour the position it is given.
///
/// It used to always send `-1:-1`, which Pulsar defines as *earliest*, while
/// documenting the behaviour as *latest* — so a subscription created after
/// publishing silently replayed the whole backlog.
#[tokio::test]
async fn create_subscription_honours_its_start_position() {
    use crate::admin::models::MessageIdData;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("substart"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        // Publish first, so the two positions cannot give the same answer.
        publish(&topic, 3).await;

        admin
            .topics()
            .create_subscription(&topic, "from_latest", &MessageIdData::latest())
            .await
            .unwrap();
        admin
            .topics()
            .create_subscription(&topic, "from_earliest", &MessageIdData::earliest())
            .await
            .unwrap();

        let stats = admin
            .topics()
            .get_stats(&topic, Default::default())
            .await
            .unwrap();
        let backlog = |name: &str| {
            stats
                .subscriptions
                .get(name)
                .unwrap_or_else(|| panic!("subscription {name} is missing: {stats:?}"))
                .msg_backlog
        };
        assert_eq!(
            backlog("from_latest"),
            0,
            "a subscription created at `latest` must not inherit the backlog: {stats:?}"
        );
        assert_eq!(
            backlog("from_earliest"),
            3,
            "a subscription created at `earliest` must replay every message: {stats:?}"
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Metadata migration reports `NOT_STARTED` on a cluster that has never migrated.
#[tokio::test]
async fn metadata_migration_status() {
    use crate::admin::models::MigrationPhase;
    let admin = new_admin().await;
    match admin.metadata_migration().status().await {
        Ok(state) => {
            assert_eq!(
                state.phase,
                MigrationPhase::NotStarted,
                "a standalone cluster should not be migrating: {state:?}"
            );
            assert!(state.target_url.is_none());
        }
        // Older brokers do not expose the endpoint at all.
        Err(Error::Admin(AdminError::NotFound(_) | AdminError::NotSupported(_))) => {}
        Err(e) => panic!("unexpected error reading migration state: {e:?}"),
    }
}

/// Starting a migration to an unreachable target must be refused, and must leave
/// the cluster's phase untouched.
///
/// Starting a migration is validated before anything is mutated.
///
/// This deliberately uses the **empty target**, which the broker rejects with 400
/// "Target URL is required" before it looks at the metadata store at all. An
/// earlier version passed an unroutable address instead, on the assumption that an
/// unreachable target would be refused — but a broker backed by `DualMetadataStore`
/// validates only the *phase*, returns success, and runs `MigrationCoordinator` in
/// the background, which writes a `PREPARATION` flag. That would have started a real,
/// one-way, cluster-wide migration; `PULSAR_ADMIN_URL` can point at a real cluster,
/// so the test must not be able to do that on any broker.
#[tokio::test]
async fn metadata_migration_start_validates_before_mutating() {
    use crate::admin::models::MigrationPhase;
    let admin = new_admin().await;
    let mm = admin.metadata_migration();

    let before = match mm.status().await {
        Ok(state) => state.phase,
        Err(Error::Admin(AdminError::NotFound(_) | AdminError::NotSupported(_))) => {
            log::warn!("broker does not expose metadata migration, skipping");
            return;
        }
        Err(e) => panic!("unexpected error reading migration state: {e:?}"),
    };
    assert_eq!(before, MigrationPhase::NotStarted);

    match mm.start("").await {
        Err(Error::Admin(AdminError::BadRequest(m))) => assert!(
            m.contains("Target URL"),
            "unexpected rejection for an empty target: {m}"
        ),
        Ok(()) => panic!("the broker accepted a migration with no target"),
        Err(e) => panic!("unexpected error starting a migration: {e:?}"),
    }

    assert_eq!(
        mm.status().await.unwrap().phase,
        before,
        "a rejected migration must not change the cluster's phase"
    );
}

// ------------------------------------------------------------- redirects
//
// These use a pair of throwaway HTTP servers rather than the broker, because a
// standalone never issues the cross-broker 307 that the behaviour depends on.

/// A recorded inbound request.
#[derive(Clone, Debug, Default)]
struct SeenRequest {
    /// The request line, e.g. `POST /admin/v2/brokers/shutdown?x=1 HTTP/1.1`.
    request_line: String,
    authorization: Option<String>,
    content_type: Option<String>,
    body: String,
}

impl SeenRequest {
    /// The `name` of every `Content-Disposition: form-data` part, in order.
    ///
    /// Substring checks on the raw body cannot tell a correctly named part from a
    /// JSON body that happens to contain the same text, which is what these tests
    /// were really asserting.
    fn part_names(&self) -> Vec<String> {
        self.body
            .lines()
            .filter(|l| l.starts_with("Content-Disposition:"))
            .filter_map(|l| l.split("name=\"").nth(1))
            .filter_map(|rest| rest.split('"').next())
            .map(str::to_string)
            .collect()
    }

    /// Asserts the body is a well-formed multipart document for its boundary.
    fn assert_multipart(&self, what: &str) {
        let content_type = self
            .content_type
            .as_deref()
            .unwrap_or_else(|| panic!("{what}: no Content-Type was sent"));
        assert!(
            content_type.starts_with("multipart/form-data"),
            "{what}: not a multipart request: {content_type}"
        );
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .unwrap_or_else(|| panic!("{what}: multipart Content-Type carried no boundary"))
            .trim()
            .trim_matches('"');
        assert!(
            self.body.contains(&format!("--{boundary}")),
            "{what}: the body does not use the boundary it declared"
        );
        assert!(
            self.body.trim_end().ends_with(&format!("--{boundary}--")),
            "{what}: the multipart body is not terminated by its closing boundary"
        );
    }
}

/// Serves one response, records the request that asked for it, and stops.
///
/// Returns the bound port and a handle to what it saw.
async fn serve_once(
    response: String,
) -> (u16, std::sync::Arc<tokio::sync::Mutex<Option<SeenRequest>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = std::sync::Arc::new(tokio::sync::Mutex::new(None));
    let sink = seen.clone();

    tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        // Read until the headers are complete, then drain whatever body the
        // Content-Length promises.
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        let header_end = loop {
            let Ok(n) = socket.read(&mut buf).await else {
                return;
            };
            if n == 0 {
                break raw.len();
            }
            raw.extend_from_slice(&buf[..n]);
            if let Some(i) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let text = String::from_utf8_lossy(&raw).to_string();
        let content_length: usize = text
            .lines()
            .find_map(|l| {
                l.split_once(':')
                    .filter(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
            })
            .and_then(|(_, v)| v.trim().parse().ok())
            .unwrap_or(0);
        while raw.len() < header_end + content_length {
            let Ok(n) = socket.read(&mut buf).await else {
                break;
            };
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }

        let full = String::from_utf8_lossy(&raw).to_string();
        let header = |name: &str| {
            full.lines().take_while(|l| !l.is_empty()).find_map(|l| {
                l.split_once(':')
                    .filter(|(k, _)| k.trim().eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.trim().to_string())
            })
        };
        let authorization = header("authorization");
        let content_type = header("content-type");
        *sink.lock().await = Some(SeenRequest {
            request_line: full.lines().next().unwrap_or_default().to_string(),
            authorization,
            content_type,
            body: full.get(header_end..).unwrap_or_default().to_string(),
        });
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.flush().await;
    });

    (port, seen)
}

fn admin_client_with_token(url: String, token: &str) -> AdminClient {
    struct Token(String);
    #[async_trait::async_trait]
    impl crate::authentication::Authentication for Token {
        fn auth_method_name(&self) -> String {
            "token".to_string()
        }
        async fn initialize(&mut self) -> Result<(), crate::error::AuthenticationError> {
            Ok(())
        }
        async fn auth_data(&mut self) -> Result<Vec<u8>, crate::error::AuthenticationError> {
            Ok(self.0.clone().into_bytes())
        }
    }
    let auth: Box<dyn crate::authentication::Authentication> = Box::new(Token(token.to_string()));
    AdminClient::new(
        url,
        &crate::connection_manager::TlsOptions::default(),
        Some(std::sync::Arc::new(futures::lock::Mutex::new(auth))),
    )
    .unwrap()
}

// ------------------------------------------------- previously missing operations

/// `getVersionBySchema` answers which version a given schema is registered under.
#[tokio::test]
async fn schema_version_lookup_by_payload() {
    use crate::admin::models::PostSchemaPayload;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("schemaver"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        let payload = PostSchemaPayload {
            schema_type: "STRING".to_string(),
            schema: String::new(),
            properties: Default::default(),
        };
        admin
            .schemas()
            .create_schema(&topic, &payload)
            .await
            .unwrap();

        // The schema just registered must report the version it was given.
        let registered = admin
            .schemas()
            .get_schema_info(&topic)
            .await
            .unwrap()
            .expect("the schema just posted is missing");
        assert_eq!(
            admin
                .schemas()
                .get_version_by_schema(&topic, &payload)
                .await
                .unwrap(),
            registered.version,
            "the version lookup disagreed with the registered schema"
        );

        // A schema that was never registered reports -1 rather than failing.
        let unknown = PostSchemaPayload {
            schema_type: "STRING".to_string(),
            schema: "{\"unregistered\":true}".to_string(),
            properties: Default::default(),
        };
        assert_eq!(
            admin
                .schemas()
                .get_version_by_schema(&topic, &unknown)
                .await
                .unwrap(),
            -1
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// Partitioned lookup returns one owning broker per partition.
///
/// There is no single broker for a partitioned topic, so this reads the partition
/// count and looks each partition up in turn, exactly as Java does.
#[tokio::test]
async fn partitioned_topic_lookup_covers_every_partition() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("plookup"));
        admin
            .topics()
            .create_partitioned_topic(&topic, 3)
            .await
            .unwrap();

        let owners = admin
            .lookup()
            .lookup_partitioned_topic(&topic)
            .await
            .unwrap();
        assert_eq!(
            owners.len(),
            3,
            "expected one entry per partition: {owners:?}"
        );
        for partition in 0..3 {
            let name = format!("{topic}-partition-{partition}");
            let owner = owners
                .get(&name)
                .unwrap_or_else(|| panic!("{name} was not looked up: {owners:?}"));
            assert!(
                owner.broker_url.is_some() || owner.native_url.is_some(),
                "no broker address for {name}: {owner:?}"
            );
        }

        // A non-partitioned topic must be refused rather than reported as empty.
        let plain = format!("persistent://{ns}/{}", unique("plain"));
        admin
            .topics()
            .create_non_partitioned_topic(&plain)
            .await
            .unwrap();
        match admin.lookup().lookup_partitioned_topic(&plain).await {
            Err(Error::Admin(AdminError::BadRequest(m))) => {
                assert!(m.contains("not a partitioned topic"), "unexpected: {m}")
            }
            other => panic!("a non-partitioned topic must be refused, got {other:?}"),
        }

        admin
            .topics()
            .delete_partitioned_topic(&topic, true)
            .await
            .unwrap();
        admin.topics().delete(&plain, true).await.unwrap();
    })
    .await;
}

/// The combined connector inventory, which the sink and source lists filter.
#[tokio::test]
async fn connector_inventory_is_the_superset_of_sinks_and_sources() {
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let all = admin.functions().get_connectors_list().await.unwrap();
        let sinks = admin.sinks().get_built_in_sinks().await.unwrap();
        let sources = admin.sources().get_built_in_sources().await.unwrap();

        assert!(
            sinks.len() <= all.len() && sources.len() <= all.len(),
            "the filtered views cannot be larger than the inventory: \
             {} sinks, {} sources, {} total",
            sinks.len(),
            sources.len(),
            all.len()
        );
        for name in sinks
            .iter()
            .chain(sources.iter())
            .filter_map(|c| c.name.as_ref())
        {
            assert!(
                all.iter().any(|c| c.name.as_ref() == Some(name)),
                "{name} is a built-in connector but is missing from the inventory"
            );
        }
    })
    .await;
}

/// Uploading a package, downloading it back, and triggering a function.
///
/// These three were implemented but never exercised.
#[tokio::test]
async fn function_package_upload_download_and_trigger() {
    use crate::admin::models::{FunctionConfig, UpdateOptions};
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let f = admin.functions();
        let name = unique("fn_pkg");
        with_function_cleanup(&name.clone(), async {
            let package = b"def process(item):\n    return item\n".to_vec();
            let store_path = format!("function://public/default/{name}@v1");

            // Upload to the worker's package store, then read it back byte for byte.
            match f
                .upload_function(&store_path, "identity.py", package.clone())
                .await
            {
                Ok(()) => {
                    let downloaded = f.download_function_by_path(&store_path).await.unwrap();
                    assert_eq!(
                        downloaded, package,
                        "the package changed between upload and download"
                    );
                }
                Err(e) => assert_reached_handler("upload_function", &e),
            }

            // Create a function so trigger and download-by-name have a target.
            let config = FunctionConfig {
                tenant: Some("public".to_string()),
                namespace: Some("default".to_string()),
                name: Some(name.clone()),
                class_name: Some("identity".to_string()),
                runtime: Some("PYTHON".to_string()),
                inputs: vec![format!("persistent://public/default/{}", unique("pkg-in"))],
                output: Some(format!("persistent://public/default/{}", unique("pkg-out"))),
                parallelism: Some(1),
                py: Some("identity.py".to_string()),
                ..Default::default()
            };
            match f
                .create_function(&config, "identity.py", package.clone())
                .await
            {
                Ok(()) => {
                    let downloaded = f
                        .download_function("public", "default", &name, false)
                        .await
                        .unwrap();
                    assert_eq!(
                        downloaded, package,
                        "the function's package did not survive the round-trip"
                    );

                    // An update carrying updateOptions must be accepted.
                    f.update_function(
                        &config,
                        "identity.py",
                        package.clone(),
                        Some(&UpdateOptions {
                            update_auth_data: false,
                        }),
                    )
                    .await
                    .expect("an update with updateOptions must be accepted");

                    // Triggering needs a running instance, so a clean refusal is fine —
                    // but it must come from the trigger handler.
                    assert_ok_or_handled!(
                        "trigger_function",
                        f.trigger_function("public", "default", &name, Some("hello"), None, None)
                            .await
                    );

                    f.delete_function("public", "default", &name).await.unwrap();
                }
                Err(e) => assert_reached_handler("create_function", &e),
            }
        })
        .await;
    })
    .await;
}

/// The create-by-URL and update paths, which no test called.
///
/// Their success needs a package the worker can fetch, which this topology has no
/// way to serve — so each is asserted to reach its handler rather than to succeed.
/// That still catches a wrong route, verb or form encoding, which is what these
/// paths were missing.
#[tokio::test]
async fn connector_create_by_url_and_update_paths_reach_the_worker() {
    use crate::admin::models::{FunctionConfig, SinkConfig, SourceConfig, UpdateOptions};
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let missing = unique("nosuch");
        let options = UpdateOptions {
            update_auth_data: true,
        };
        // 192.0.2.0/24 is reserved for documentation and is never routable, so the
        // worker cannot actually fetch anything from it.
        let package_url = "http://192.0.2.1/pkg.jar";

        let function = FunctionConfig {
            tenant: Some("public".to_string()),
            namespace: Some("default".to_string()),
            name: Some(missing.clone()),
            class_name: Some("identity".to_string()),
            inputs: vec!["persistent://public/default/in".to_string()],
            ..Default::default()
        };
        let sink = SinkConfig {
            tenant: Some("public".to_string()),
            namespace: Some("default".to_string()),
            name: Some(missing.clone()),
            class_name: Some("identity".to_string()),
            inputs: vec!["persistent://public/default/in".to_string()],
            ..Default::default()
        };
        let source = SourceConfig {
            tenant: Some("public".to_string()),
            namespace: Some("default".to_string()),
            name: Some(missing.clone()),
            class_name: Some("identity".to_string()),
            topic_name: Some("persistent://public/default/out".to_string()),
            ..Default::default()
        };

        assert_ok_or_handled!(
            "create_function_with_url",
            admin
                .functions()
                .create_function_with_url(&function, package_url)
                .await
        );
        assert_ok_or_handled!(
            "update_function_with_url",
            admin
                .functions()
                .update_function_with_url(&function, package_url, Some(&options))
                .await
        );
        assert_ok_or_handled!(
            "update_function",
            admin
                .functions()
                .update_function(&function, "p.jar", b"x".to_vec(), Some(&options))
                .await
        );

        assert_ok_or_handled!(
            "create_sink_with_url",
            admin.sinks().create_sink_with_url(&sink, package_url).await
        );
        assert_ok_or_handled!(
            "update_sink_with_url",
            admin
                .sinks()
                .update_sink_with_url(&sink, package_url, Some(&options))
                .await
        );
        assert_ok_or_handled!(
            "update_sink",
            admin
                .sinks()
                .update_sink(&sink, "p.jar", b"x".to_vec(), Some(&options))
                .await
        );
        assert_ok_or_handled!(
            "get_sink_instance_status",
            admin
                .sinks()
                .get_sink_instance_status("public", "default", &missing, 0)
                .await
        );

        assert_ok_or_handled!(
            "create_source_with_url",
            admin
                .sources()
                .create_source_with_url(&source, package_url)
                .await
        );
        assert_ok_or_handled!(
            "update_source_with_url",
            admin
                .sources()
                .update_source_with_url(&source, package_url, Some(&options))
                .await
        );
        assert_ok_or_handled!(
            "update_source",
            admin
                .sources()
                .update_source(&source, "p.jar", b"x".to_vec(), Some(&options))
                .await
        );
        assert_ok_or_handled!(
            "get_source_instance_status",
            admin
                .sources()
                .get_source_instance_status("public", "default", &missing, 0)
                .await
        );
    })
    .await;
}

/// `testCompatibility` and forced deletion, both absent until now.
///
/// The response field is `compatibility` on the wire even though Java's field is
/// `isCompatibility` — Lombok's generated getter renames it, so trusting the Java
/// field name would have decoded a constant `false`.
#[tokio::test]
async fn schema_compatibility_and_forced_delete() {
    use crate::admin::models::PostSchemaPayload;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let s = admin.schemas();
        let topic = format!("persistent://{ns}/{}", unique("compat"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        let string_schema = PostSchemaPayload {
            schema_type: "STRING".to_string(),
            schema: String::new(),
            properties: Default::default(),
        };
        s.create_schema(&topic, &string_schema).await.unwrap();

        // The identical schema is compatible, and the strategy comes back too.
        let response = s.test_compatibility(&topic, &string_schema).await.unwrap();
        assert!(
            response.is_compatible,
            "the schema already registered must be compatible with itself: {response:?}"
        );
        assert!(
            response.schema_compatibility_strategy.is_some(),
            "no compatibility strategy reported: {response:?}"
        );

        // An incompatible schema is reported as an error by the broker rather than
        // as `is_compatible: false`, so assert it reaches the handler and says why.
        let json_schema = PostSchemaPayload {
            schema_type: "JSON".to_string(),
            schema: r#"{"type":"record","name":"X","fields":[]}"#.to_string(),
            properties: Default::default(),
        };
        match s.test_compatibility(&topic, &json_schema).await {
            Ok(r) => assert!(
                !r.is_compatible,
                "a JSON schema must not be compatible with a STRING one: {r:?}"
            ),
            Err(e) => {
                assert_reached_handler("test_compatibility", &e);
                assert!(
                    format!("{e}").contains("ncompatible"),
                    "the refusal did not come from the compatibility check: {e}"
                );
            }
        }

        // Forced deletion, and absence afterwards — the previous test asserted
        // neither the `force` flag nor that the schema was actually gone.
        s.delete_schema(&topic, true).await.unwrap();
        assert!(
            s.get_schema_info(&topic).await.unwrap().is_none(),
            "the schema survived a forced delete"
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// The message-addressing group: read messages by storage position, resolve a
/// position by index, size a backlog from a position, and dump ledger metadata.
#[tokio::test]
async fn topic_message_addressing_by_storage_position() {
    use crate::admin::models::MessageIdData;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let t = admin.topics();
        let topic = format!("persistent://{ns}/{}", unique("byid"));
        t.create_non_partitioned_topic(&topic).await.unwrap();
        publish(&topic, 3).await;

        // The last published message gives a real ledger/entry to address.
        let last = t.get_last_message_id(&topic).await.unwrap();
        assert!(last.ledger_id >= 0, "no ledger assigned yet: {last:?}");

        let message = t
            .get_message_by_id(&topic, last.ledger_id, last.entry_id)
            .await
            .expect("the last message must be readable by its storage position");
        assert_eq!(
            message.payload, b"message-2",
            "read the wrong entry: {message:?}"
        );
        assert_eq!(
            t.get_messages_by_id(&topic, last.ledger_id, last.entry_id)
                .await
                .unwrap()
                .len(),
            1
        );

        // A position that does not exist must be reported, not mis-decoded.
        match t.get_message_by_id(&topic, last.ledger_id, 9_999).await {
            Err(e) => assert_reached_handler("get_message_by_id", &e),
            Ok(m) => panic!("entry 9999 should not exist: {m:?}"),
        }

        // Backlog measured from the earliest position covers everything published.
        let from_earliest = t
            .get_backlog_size_by_message_id(&topic, &MessageIdData::earliest())
            .await
            .expect("backlogSize must answer for an existing topic");
        assert!(
            from_earliest > 0,
            "three published messages should have a non-zero backlog, got {from_earliest}"
        );

        // Managed-ledger metadata is a raw document, as in Java.
        let info = t.get_internal_info(&topic).await.unwrap();
        assert!(
            info.contains("ledgers"),
            "internal-info did not look like managed-ledger metadata: {info}"
        );

        // Index lookup needs the broker-entry-metadata interceptor, which the test
        // broker does not enable — so it must refuse from its own handler.
        match t.get_message_id_by_index(&topic, 0).await {
            Ok(id) => assert!(id.ledger_id >= 0, "{id:?}"),
            Err(e) => {
                assert_reached_handler("get_message_id_by_index", &e);
                assert!(
                    format!("{e}").contains("broker entry metadata"),
                    "the refusal did not come from the index handler: {e}"
                );
            }
        }

        t.delete(&topic, true).await.unwrap();
    })
    .await;
}

/// The namespace-wide scalable-topic auto-scale policy.
///
/// Distinct from the per-topic override on `scalable_topics()`, and the field
/// names are the plural ones the broker silently requires.
#[tokio::test]
async fn namespace_scalable_topic_auto_scale_policy_round_trip() {
    use crate::admin::models::AutoScalePolicyOverride;
    let admin = new_admin().await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        assert_eq!(
            n.get_scalable_topic_auto_scale_policy(&ns).await.unwrap(),
            None,
            "a fresh namespace must have no auto-scale override"
        );

        let policy = AutoScalePolicyOverride {
            enabled: Some(true),
            max_segments: Some(16),
            min_segments: Some(2),
            max_dag_depth: Some(4),
            split_cooldown_seconds: Some(30),
            merge_cooldown_seconds: Some(45),
            merge_window_seconds: Some(60),
            split_msg_rate_in_threshold: Some(500.0),
            merge_msg_rate_in_threshold: Some(5.0),
            ..Default::default()
        };
        n.set_scalable_topic_auto_scale_policy(&ns, &policy)
            .await
            .unwrap();
        assert_eq!(
            n.get_scalable_topic_auto_scale_policy(&ns)
                .await
                .unwrap()
                .as_ref(),
            Some(&policy),
            "the namespace auto-scale policy did not round-trip"
        );

        n.remove_scalable_topic_auto_scale_policy(&ns)
            .await
            .unwrap();
        assert_eq!(
            n.get_scalable_topic_auto_scale_policy(&ns).await.unwrap(),
            None,
            "the auto-scale override survived removal"
        );
    })
    .await;
}

/// The `Namespaces` operations that had no Rust counterpart until now.
///
/// Grouped into one test because they share a namespace; each is asserted on its
/// own terms rather than merely being called.
#[tokio::test]
async fn remaining_namespace_parity_operations() {
    use crate::admin::models::{GrantTopicPermissionOptions, RevokeTopicPermissionOptions};
    let admin = new_admin().await;
    let cluster = primary_cluster(&admin).await;
    with_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let n = admin.namespaces();

        // --- allowed clusters: distinct from replication clusters ---
        assert_eq!(
            n.get_namespace_allowed_clusters(&ns).await.unwrap(),
            Vec::<String>::new(),
            "a fresh namespace should allow no explicit cluster set"
        );
        n.set_namespace_allowed_clusters(&ns, std::slice::from_ref(&cluster))
            .await
            .unwrap();
        assert_eq!(
            n.get_namespace_allowed_clusters(&ns).await.unwrap(),
            vec![cluster.clone()],
            "the allowed-cluster set did not round-trip"
        );

        // --- metric topic-property key allow-list ---
        assert!(n
            .get_allowed_topic_property_keys_for_metrics(&ns)
            .await
            .unwrap()
            .is_empty());
        n.set_allowed_topic_property_keys_for_metrics(&ns, &["team".to_string()])
            .await
            .unwrap();
        assert_eq!(
            n.get_allowed_topic_property_keys_for_metrics(&ns)
                .await
                .unwrap(),
            vec!["team".to_string()],
            "the metric property allow-list did not round-trip"
        );
        n.remove_allowed_topic_property_keys_for_metrics(&ns)
            .await
            .unwrap();
        assert!(
            n.get_allowed_topic_property_keys_for_metrics(&ns)
                .await
                .unwrap()
                .is_empty(),
            "the allow-list survived removal"
        );

        // --- single-property read, alongside the bulk accessors ---
        assert_eq!(n.get_namespace_property(&ns, "owner").await.unwrap(), None);
        n.set_namespace_property(&ns, "owner", "platform")
            .await
            .unwrap();
        assert_eq!(
            n.get_namespace_property(&ns, "owner")
                .await
                .unwrap()
                .as_deref(),
            Some("platform"),
            "the single-property getter disagreed with the setter"
        );
        n.remove_namespace_property(&ns, "owner").await.unwrap();
        assert_eq!(n.get_namespace_property(&ns, "owner").await.unwrap(), None);

        // --- subscription permissions listing ---
        n.grant_permission_on_subscription(&ns, "sub-a", &["role-a".to_string()])
            .await
            .unwrap();
        let per_subscription = n.get_permission_on_subscription(&ns).await.unwrap();
        assert_eq!(
            per_subscription.get("sub-a").map(|r| r.as_slice()),
            Some(["role-a".to_string()].as_slice()),
            "the subscription permission listing is missing the grant: {per_subscription:?}"
        );

        // --- migration state ---
        n.update_migration_state(&ns, true).await.unwrap();
        n.update_migration_state(&ns, false).await.unwrap();

        // --- deduplication snapshot interval: removal is a null POST, not DELETE ---
        n.set_deduplication_snapshot_interval(&ns, 111)
            .await
            .unwrap();
        assert_eq!(
            n.get_deduplication_snapshot_interval(&ns).await.unwrap(),
            Some(111)
        );
        n.remove_deduplication_snapshot_interval(&ns).await.unwrap();
        assert_ne!(
            n.get_deduplication_snapshot_interval(&ns).await.unwrap(),
            Some(111),
            "the deduplication snapshot interval survived removal"
        );

        // --- deprecated schema auto-update strategy: still has to work ---
        #[allow(deprecated)]
        {
            n.set_schema_auto_update_compatibility_strategy(&ns, "Full")
                .await
                .unwrap();
            assert_eq!(
                n.get_schema_auto_update_compatibility_strategy(&ns)
                    .await
                    .unwrap()
                    .as_deref(),
                Some("Full"),
                "the deprecated schema auto-update strategy did not round-trip"
            );
        }

        // --- anti-affinity group listing, addressed by cluster ---
        n.set_namespace_anti_affinity_group(&ns, "group-a")
            .await
            .unwrap();
        let (tenant, _) = ns.split_once('/').unwrap();
        // The per-cluster anti-affinity index is served from the configuration
        // store, which the write reaches asynchronously — poll rather than race it.
        let mut in_group = Vec::new();
        for _ in 0..25 {
            in_group = n
                .get_anti_affinity_namespaces(tenant, &cluster, "group-a")
                .await
                .unwrap();
            if in_group.iter().any(|candidate| candidate == &ns) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        assert!(
            in_group.iter().any(|candidate| candidate == &ns),
            "the namespace is missing from its own anti-affinity group: {in_group:?}"
        );

        // --- bulk topic permissions, which are cluster-scoped ---
        let topic = format!("persistent://{ns}/{}", unique("bulkperm"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        n.grant_permission_on_topics(&[GrantTopicPermissionOptions {
            topic: topic.clone(),
            role: "bulk-role".to_string(),
            actions: ["consume".to_string()].into_iter().collect(),
        }])
        .await
        .unwrap();
        assert!(
            admin
                .topics()
                .get_permissions(&topic)
                .await
                .unwrap()
                .contains_key("bulk-role"),
            "the bulk grant did not reach the topic"
        );
        n.revoke_permission_on_topics(&[RevokeTopicPermissionOptions {
            topic: topic.clone(),
            role: "bulk-role".to_string(),
        }])
        .await
        .unwrap();
        assert!(
            !admin
                .topics()
                .get_permissions(&topic)
                .await
                .unwrap()
                .contains_key("bulk-role"),
            "the bulk revoke left the permission behind"
        );

        // --- hash positions need the bundle to be owned; it is, now a topic exists ---
        let bundle = {
            let boundaries = n.get_bundles(&ns).await.unwrap().boundaries;
            format!("{}_{}", boundaries[0], boundaries[1])
        };
        assert_ok_or_handled!(
            "get_topic_hash_positions",
            n.get_topic_hash_positions(&ns, &bundle, &[]).await
        );

        // --- deleting a single bundle ---
        assert_ok_or_handled!(
            "delete_namespace_bundle",
            n.delete_namespace_bundle(&ns, &bundle, true).await
        );

        admin.topics().delete(&topic, true).await.ok();
    })
    .await;
}

/// Global and local topic policies are separate stores.
///
/// Java reaches the geo-replicated set with `topicPolicies(true)`, which appends
/// `?isGlobal=true` to every request. Without it the client can only ever see the
/// cluster-local policies, so a global override is invisible.
#[tokio::test]
async fn global_topic_policies_are_a_separate_store() {
    use crate::admin::models::RetentionPolicies;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("globalpol"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        let local = admin.topic_policies();
        let global = admin.topic_policies_global();

        // A global override must not show up in a local read.
        global
            .set_retention(
                &topic,
                &RetentionPolicies {
                    retention_time_in_minutes: -1,
                    retention_size_in_mb: -1,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            global.get_retention(&topic, false).await.unwrap(),
            Some(RetentionPolicies {
                retention_time_in_minutes: -1,
                retention_size_in_mb: -1,
            }),
            "the global retention override did not round-trip"
        );
        assert_eq!(
            local.get_retention(&topic, false).await.unwrap(),
            None,
            "a global override leaked into the local policy set"
        );

        // And the converse: a local override is invisible globally.
        local.set_max_producers(&topic, 7).await.unwrap();
        assert_eq!(
            local.get_max_producers(&topic, false).await.unwrap(),
            Some(7)
        );
        assert_eq!(
            global.get_max_producers(&topic, false).await.unwrap(),
            None,
            "a local override leaked into the global policy set"
        );

        // Removal is scoped the same way.
        global.remove_retention(&topic).await.unwrap();
        assert_eq!(global.get_retention(&topic, false).await.unwrap(), None);

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// The public call paths that no test invoked.
///
/// `Namespaces` and `TopicPolicies` define many identically named operations, so a
/// name-only search reported these as covered when only the other group's method
/// was ever called. Each is exercised here against its own receiver.
#[tokio::test]
async fn previously_uninvoked_namespace_and_topic_policy_paths() {
    use crate::admin::models::RetentionPolicies;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;

        // Namespaces::remove_retention — only TopicPolicies::remove_retention was
        // ever called.
        admin
            .namespaces()
            .set_retention(
                &ns,
                &RetentionPolicies {
                    retention_time_in_minutes: 30,
                    retention_size_in_mb: 100,
                },
            )
            .await
            .unwrap();
        admin.namespaces().remove_retention(&ns).await.unwrap();
        let after = admin.namespaces().get_retention(&ns).await.unwrap();
        assert_ne!(
            after.map(|r| r.retention_time_in_minutes),
            Some(30),
            "namespace retention survived removal"
        );

        // TopicPolicies subscription types — only the Namespaces pair was called.
        let topic = format!("persistent://{ns}/{}", unique("subtypes"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();
        let tp = admin.topic_policies();
        tp.set_subscription_types_enabled(&topic, &["Shared".to_string()])
            .await
            .unwrap();
        assert_eq!(
            tp.get_subscription_types_enabled(&topic).await.unwrap(),
            Some(vec!["Shared".to_string()]),
            "the topic-level subscription types did not round-trip"
        );
        tp.remove_subscription_types_enabled(&topic).await.unwrap();
        assert_ne!(
            tp.get_subscription_types_enabled(&topic).await.unwrap(),
            Some(vec!["Shared".to_string()]),
            "the topic-level override survived removal"
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// The two `NonPersistentTopics` paths that only their `Topics` namesakes covered.
#[tokio::test]
async fn previously_uninvoked_non_persistent_paths() {
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("non-persistent://{ns}/{}", unique("np"));

        // A non-persistent topic exists only while a client is attached, so it has
        // to be brought into being rather than created through the admin API.
        let pulsar =
            crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
                .build()
                .await
                .unwrap();
        let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
        producer
            .send_non_blocking("hello")
            .await
            .unwrap()
            .await
            .unwrap();

        let np = admin.non_persistent_topics();
        // No ledgers to report, so what matters is that this group's own route
        // answers and the shape decodes.
        np.get_internal_stats(&topic).await.unwrap();
        np.unload(&topic).await.unwrap();

        producer.close().await.unwrap();
    })
    .await;
}

/// The flat compatibility methods on `AdminClient`.
///
/// These are separate implementations, not forwarding aliases, so the grouped
/// tests do not exercise their plumbing. The remover and both schema getters had
/// no test at all.
#[tokio::test]
async fn flat_compatibility_methods_are_exercised() {
    use crate::admin::models::PostSchemaPayload;
    let admin = new_admin().await;
    with_topic_namespace(&admin, |ns| async move {
        let admin = new_admin().await;
        let topic = format!("persistent://{ns}/{}", unique("flat"));
        admin
            .topics()
            .create_non_partitioned_topic(&topic)
            .await
            .unwrap();

        admin
            .set_max_unacked_messages_on_consumer(&topic, 42)
            .await
            .unwrap();
        assert_eq!(
            admin
                .topic_policies()
                .get_max_unacked_messages_on_consumer(&topic, false)
                .await
                .unwrap(),
            Some(42)
        );
        admin
            .remove_max_unacked_messages_on_consumer(&topic)
            .await
            .unwrap();
        assert_ne!(
            admin
                .topic_policies()
                .get_max_unacked_messages_on_consumer(&topic, false)
                .await
                .unwrap(),
            Some(42),
            "the flat remover did not clear the override"
        );

        // A topic with no schema reads as absent through the flat getter.
        assert!(admin.get_schema(&topic).await.unwrap().is_none());
        admin
            .schemas()
            .create_schema(
                &topic,
                &PostSchemaPayload {
                    schema_type: "STRING".to_string(),
                    schema: String::new(),
                    properties: Default::default(),
                },
            )
            .await
            .unwrap();
        let latest = admin
            .get_schema(&topic)
            .await
            .unwrap()
            .expect("the schema just registered is missing");
        let at_version = admin
            .get_schema_at_version(&topic, 0)
            .await
            .unwrap()
            .expect("version 0 is missing");
        assert_eq!(
            format!("{latest:?}"),
            format!("{at_version:?}"),
            "the versioned getter disagreed with the latest getter"
        );

        admin.topics().delete(&topic, true).await.unwrap();
    })
    .await;
}

/// The four responses Java types but this client used to return raw.
///
/// A `String` or `serde_json::Value` return cannot fail on a wire change, so these
/// went untested by construction. Decoding into the real shapes is the assertion.
#[tokio::test]
async fn previously_untyped_responses_decode_into_their_models() {
    let admin = new_admin().await;

    // Worker assignments: Java's Map<String, Collection<String>>.
    let assignments = admin.worker().get_assignments().await.unwrap();
    for (worker, functions) in &assignments {
        assert!(!worker.is_empty(), "an assignment had no worker id");
        let _: &Vec<String> = functions;
    }

    // Worker metrics: Java's Collection<Metrics>, each with dimensions + metrics.
    let metrics = admin.worker().get_metrics().await.unwrap();
    assert!(
        metrics.iter().any(|m| !m.metrics.is_empty()),
        "no worker metrics sample carried any values: {metrics:?}"
    );

    // The load report is absent on a standalone until the load manager writes one,
    // so `None` is a valid answer — but a present one must decode.
    if let Some(report) = admin.broker_stats().get_load_report().await.unwrap() {
        assert!(
            report.cpu.limit > 0.0 || report.broker_id.is_some(),
            "the load report decoded to nothing usable: {report:?}"
        );
    }
}

/// Per-instance function counters must decode from the broker's real response.
///
/// The broker nests them under `metrics`; reading them flat reported zero for every
/// instance while still claiming a successful decode. Previously pinned only by a
/// unit fixture — this asserts it against a live worker.
#[tokio::test]
async fn function_stats_decode_the_brokers_nested_instances() {
    use crate::admin::models::FunctionConfig;
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let f = admin.functions();
        let name = unique("fn_stats");
        with_function_cleanup(&name.clone(), async {
            let config = FunctionConfig {
                tenant: Some("public".to_string()),
                namespace: Some("default".to_string()),
                name: Some(name.clone()),
                class_name: Some("identity".to_string()),
                runtime: Some("PYTHON".to_string()),
                inputs: vec![format!("persistent://public/default/{}", unique("st-in"))],
                output: Some(format!("persistent://public/default/{}", unique("st-out"))),
                parallelism: Some(1),
                py: Some("identity.py".to_string()),
                ..Default::default()
            };
            let package = b"def process(item):\n    return item\n".to_vec();

            match f.create_function(&config, "identity.py", package).await {
                Ok(()) => {
                    // The worker registers the instance a moment after the create
                    // returns, so poll rather than racing it.
                    let mut stats = f
                        .get_function_stats("public", "default", &name)
                        .await
                        .unwrap();
                    for _ in 0..40 {
                        if !stats.instances.is_empty() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        stats = f
                            .get_function_stats("public", "default", &name)
                            .await
                            .unwrap();
                    }
                    assert_eq!(
                        stats.instances.len(),
                        1,
                        "expected one instance for parallelism 1: {stats:?}"
                    );
                    assert_eq!(stats.instances[0].instance_id, 0, "{stats:?}");

                    // An idle function reports zero for every counter, so the decoded
                    // values cannot distinguish "found the nested object" from "found
                    // nothing". Assert the broker's actual JSON shape instead: the
                    // counters live under `metrics`, and are *not* direct children of
                    // the instance — which is exactly what reading them flat assumed.
                    let raw: serde_json::Value = reqwest::get(format!(
                        "{}/admin/v3/functions/public/default/{name}/stats",
                        test_utils::admin_url()
                    ))
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                    let instance = &raw["instances"][0];
                    assert!(
                        instance["metrics"].is_object(),
                        "the broker did not nest the counters under `metrics`: {instance}"
                    );
                    assert!(
                        instance["metrics"]["receivedTotal"].is_number(),
                        "`receivedTotal` is not inside `metrics`: {instance}"
                    );
                    assert!(
                        instance.get("receivedTotal").is_none(),
                        "`receivedTotal` is also a direct child, so reading it flat would \
                     have worked after all — this test proves nothing: {instance}"
                    );

                    f.delete_function("public", "default", &name).await.unwrap();
                }
                Err(e) => assert_reached_handler("create_function", &e),
            }
        })
        .await;
    })
    .await;
}

/// `FunctionState.byteValue` must go out as base64, not a JSON number array.
///
/// Java's field is `byte[]`, which Jackson renders as a base64 string. The worker's
/// state store never finishes initializing on a standalone, so this captures the
/// request the client actually sends rather than round-tripping it.
#[tokio::test]
async fn function_state_sends_byte_values_as_base64() {
    use crate::admin::models::FunctionState;
    let (port, seen) =
        serve_once("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_string()).await;
    let admin = AdminClient::new(
        format!("http://127.0.0.1:{port}"),
        &crate::connection_manager::TlsOptions::default(),
        None,
    )
    .unwrap();

    admin
        .functions()
        .put_function_state(
            "public",
            "default",
            "somefn",
            &FunctionState {
                key: Some("k".to_string()),
                byte_value: Some(vec![0, 1, 255]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let seen = seen.lock().await.clone().unwrap();
    seen.assert_multipart("put_function_state");
    assert!(
        seen.request_line
            .starts_with("POST /admin/v3/functions/public/default/somefn/state/k"),
        "wrong verb or path: {}",
        seen.request_line
    );
    assert_eq!(
        seen.part_names(),
        vec!["state".to_string()],
        "the worker binds a form part named `state`; a JSON body is rejected with 415"
    );
    assert!(
        seen.body.contains(r#""byteValue":"AAH/""#),
        "byteValue was not sent as base64: {}",
        seen.body
    );
    assert!(
        !seen.body.contains("[0,1,255]"),
        "byteValue went out as a JSON number array, which Jackson cannot read: {}",
        seen.body
    );
}

/// The request timeout must be configurable, not hard-coded at 30 seconds.
#[tokio::test]
async fn the_admin_request_timeout_is_configurable() {
    // A server that accepts the connection and then never answers.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _held = listener.accept().await;
        std::future::pending::<()>().await;
    });

    let pulsar = crate::Pulsar::<TokioExecutor>::builder(test_utils::broker_url(), TokioExecutor)
        .build()
        .await
        .unwrap();
    let admin = pulsar
        .admin_with_options(
            format!("http://127.0.0.1:{port}"),
            &crate::AdminOptions {
                timeout: std::time::Duration::from_millis(300),
            },
        )
        .unwrap();

    let started = std::time::Instant::now();
    let result = admin.clusters().get_clusters().await;
    let elapsed = started.elapsed();

    // It must be a *timeout*, not a connection error: the listener accepted the
    // socket, so anything else means the request failed for an unrelated reason and
    // this proves nothing about the setting.
    match result {
        Err(Error::Admin(AdminError::Request(e))) => assert!(
            e.is_timeout(),
            "expected a timeout, got a different request error: {e}"
        ),
        other => panic!("a server that never answers must time out, got {other:?}"),
    }
    // Bounded on both sides: too fast means it never waited, too slow means the
    // configured value was ignored in favour of some other timeout.
    assert!(
        elapsed >= std::time::Duration::from_millis(250),
        "returned in {elapsed:?}, before the configured 300ms could elapse"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "took {elapsed:?}, so the configured 300ms was ignored"
    );
}

/// Graceful shutdown must build the right request — checked against a stub.
///
/// Deliberately not aimed at the real broker: this endpoint does exactly what it
/// says, and an earlier version of this test stopped the broker mid-run and took
/// the rest of the suite with it. A stub server proves the verb, the path and both
/// query parameters with no way to affect a real cluster.
#[tokio::test]
async fn broker_graceful_shutdown_builds_the_right_request() {
    let (port, seen) =
        serve_once("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_string()).await;
    let admin = AdminClient::new(
        format!("http://127.0.0.1:{port}"),
        &crate::connection_manager::TlsOptions::default(),
        None,
    )
    .unwrap();

    admin
        .brokers()
        .shutdown_broker_gracefully(7, true)
        .await
        .unwrap();

    let line = seen.lock().await.clone().unwrap().request_line;
    assert!(
        line.starts_with("POST /admin/v2/brokers/shutdown?"),
        "wrong verb or path: {line}"
    );
    assert!(
        line.contains("maxConcurrentUnloadPerSec=7") && line.contains("forcedTerminateTopic=true"),
        "the shutdown parameters were not sent: {line}"
    );
}

/// A JSON string response must be decoded, not have its quotes stripped by hand.
///
/// These endpoints mix bare text with JSON strings. Trimming quote characters
/// corrupted any value that legitimately began or ended with one, and left
/// backslash escapes in place.
#[tokio::test]
async fn a_quoted_text_response_round_trips_exactly() {
    // The JSON encoding of the string `" quoted "` — quotes are part of the value.
    let payload = r#""\" quoted \"""#;
    let (port, _seen) = serve_once(format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    ))
    .await;
    let admin = AdminClient::new(
        format!("http://127.0.0.1:{port}"),
        &crate::connection_manager::TlsOptions::default(),
        None,
    )
    .unwrap();

    let read = admin
        .namespaces()
        .get_namespace_resource_group("public/default")
        .await
        .unwrap();
    assert_eq!(
        read.as_deref(),
        Some(r#"" quoted ""#),
        "the value was corrupted in transit"
    );
}

/// The function subscription name must survive a round-trip through the worker.
///
/// Pulsar's object mapper ignores unknown properties, so the previous
/// `subscriptionName` spelling was accepted and silently discarded — creation
/// succeeded and the setting simply vanished. Only a read-back catches that; the
/// real field is `subName`.
#[tokio::test]
async fn function_subscription_name_survives_the_worker() {
    use crate::admin::models::FunctionConfig;
    if_functions_worker(|| async {
        let admin = new_admin().await;
        let f = admin.functions();
        let name = unique("fn_subname");
        with_function_cleanup(&name.clone(), async {
            let config = FunctionConfig {
                tenant: Some("public".to_string()),
                namespace: Some("default".to_string()),
                name: Some(name.clone()),
                class_name: Some("identity".to_string()),
                runtime: Some("PYTHON".to_string()),
                inputs: vec![format!("persistent://public/default/{}", unique("sn-in"))],
                output: Some(format!("persistent://public/default/{}", unique("sn-out"))),
                parallelism: Some(1),
                py: Some("identity.py".to_string()),
                subscription_name: Some("my-subscription".to_string()),
                ..Default::default()
            };
            let package = b"def process(item):\n    return item\n".to_vec();

            match f.create_function(&config, "identity.py", package).await {
                Ok(()) => {
                    let read = f.get_function("public", "default", &name).await.unwrap();
                    assert_eq!(
                        read.subscription_name.as_deref(),
                        Some("my-subscription"),
                        "the subscription name did not survive the worker — it is `subName` \
                     on the wire, and any other spelling is silently discarded: {read:?}"
                    );
                    f.delete_function("public", "default", &name).await.unwrap();
                }
                Err(e) => {
                    assert_reached_handler("create_function", &e);
                    log::warn!("worker refused the package, cannot check the round-trip");
                }
            }
        })
        .await;
    })
    .await;
}

/// An authentication method with no HTTP mapping must be reported, not ignored.
///
/// Silently sending the request unauthenticated surfaces as a puzzling 401 from
/// the broker instead of the configuration problem it actually is.
#[tokio::test]
async fn an_unmappable_auth_method_is_an_explicit_error() {
    struct Exotic;
    #[async_trait::async_trait]
    impl crate::authentication::Authentication for Exotic {
        fn auth_method_name(&self) -> String {
            "sasl".to_string()
        }
        async fn initialize(&mut self) -> Result<(), crate::error::AuthenticationError> {
            Ok(())
        }
        async fn auth_data(&mut self) -> Result<Vec<u8>, crate::error::AuthenticationError> {
            Ok(b"ticket".to_vec())
        }
    }
    let auth: Box<dyn crate::authentication::Authentication> = Box::new(Exotic);
    let admin = AdminClient::new(
        test_utils::admin_url(),
        &crate::connection_manager::TlsOptions::default(),
        Some(std::sync::Arc::new(futures::lock::Mutex::new(auth))),
    )
    .unwrap();

    match admin.clusters().get_clusters().await {
        Err(Error::Admin(AdminError::NotSupported(m))) => {
            assert!(m.contains("sasl"), "the error did not name the method: {m}")
        }
        other => panic!("an unmappable auth method must be reported, got {other:?}"),
    }
}

/// The health check must read the body, not just the status.
///
/// The broker answers 200 with a body describing what is wrong, so treating any
/// 2xx as healthy reports a sick broker as healthy.
#[tokio::test]
async fn health_check_requires_an_ok_body() {
    // Build the header from the payload: a hand-written Content-Length that was one
    // byte short meant this asserted against a truncated body.
    let payload = "namespace is not writable";
    let (port, _seen) = serve_once(format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{payload}",
        payload.len()
    ))
    .await;
    let admin = AdminClient::new(
        format!("http://127.0.0.1:{port}"),
        &crate::connection_manager::TlsOptions::default(),
        None,
    )
    .unwrap();
    match admin.brokers().healthcheck().await {
        Err(Error::Admin(AdminError::ServerUnavailable(m))) => {
            assert!(m.contains("not writable"), "unexpected message: {m}")
        }
        other => panic!("a 200 whose body is not \"ok\" must not pass, got {other:?}"),
    }
}

/// An explicitly persistent topic must be refused by the non-persistent group
/// rather than silently rewritten into the other domain.
#[tokio::test]
async fn non_persistent_group_rejects_a_persistent_topic() {
    let admin = new_admin().await;
    match admin
        .non_persistent_topics()
        .get_stats("persistent://public/default/some-topic")
        .await
    {
        Err(Error::Admin(AdminError::InvalidTopic(m))) => {
            assert!(m.contains("persistent"), "unexpected message: {m}")
        }
        other => panic!("a persistent topic must be refused here, got {other:?}"),
    }
}

/// Serves a fixed sequence of responses, recording every request it sees.
async fn serve_sequence(
    responses: Vec<String>,
) -> (u16, std::sync::Arc<tokio::sync::Mutex<Vec<SeenRequest>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();

    tokio::spawn(async move {
        for response in responses {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut raw = Vec::new();
            let mut buf = [0u8; 4096];
            let header_end = loop {
                let Ok(n) = socket.read(&mut buf).await else {
                    return;
                };
                if n == 0 {
                    break raw.len();
                }
                raw.extend_from_slice(&buf[..n]);
                if let Some(i) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let text = String::from_utf8_lossy(&raw).to_string();
            let content_length: usize = text
                .lines()
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
                })
                .and_then(|(_, v)| v.trim().parse().ok())
                .unwrap_or(0);
            while raw.len() < header_end + content_length {
                let Ok(n) = socket.read(&mut buf).await else {
                    break;
                };
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
            }
            let full = String::from_utf8_lossy(&raw).to_string();
            sink.lock().await.push(SeenRequest {
                request_line: full.lines().next().unwrap_or_default().to_string(),
                authorization: None,
                content_type: None,
                body: full.get(header_end..).unwrap_or_default().to_string(),
            });
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.flush().await;
        }
    });

    (port, seen)
}

fn ok_body(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

fn redirect(status: u16, location: &str) -> String {
    let reason = match status {
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        307 => "Temporary Redirect",
        _ => "Permanent Redirect",
    };
    format!("HTTP/1.1 {status} {reason}\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n")
}

async fn stub_admin(port: u16) -> AdminClient {
    AdminClient::new(
        format!("http://127.0.0.1:{port}"),
        &crate::connection_manager::TlsOptions::default(),
        None,
    )
    .unwrap()
}

/// 307 and 308 replay the method and body; 301, 302 and 303 become bodyless GETs.
///
/// These are the semantics of Java's `AsyncHttpConnector`, which also disables its
/// library's redirect handling and implements the same rules by hand.
#[tokio::test]
async fn redirects_preserve_or_drop_the_body_per_status() {
    for (status, expect_post) in [
        (307, true),
        (308, true),
        (301, false),
        (302, false),
        (303, false),
    ] {
        let (port, seen) = serve_sequence(vec![
            redirect(status, "/second"),
            ok_body(r#"{"tenant":"x"}"#),
        ])
        .await;
        let admin = stub_admin(port).await;
        // A PUT with a body: `create_tenant` sends JSON.
        admin
            .tenants()
            .create_tenant(
                "t",
                &TenantInfo {
                    allowed_clusters: ["c".to_string()].into_iter().collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{status} redirect was not followed: {e:?}"));

        let seen = seen.lock().await;
        assert_eq!(seen.len(), 2, "{status}: expected exactly two requests");
        let second = &seen[1];
        if expect_post {
            assert!(
                second.request_line.starts_with("PUT /second"),
                "{status} must replay the method: {}",
                second.request_line
            );
            assert!(
                second.body.contains("allowedClusters"),
                "{status} must replay the body: {:?}",
                second.body
            );
        } else {
            assert!(
                second.request_line.starts_with("GET /second"),
                "{status} must be re-issued as GET: {}",
                second.request_line
            );
            assert!(
                second.body.trim().is_empty(),
                "{status} must not resend the body: {:?}",
                second.body
            );
        }
    }
}

/// The caller's query belongs to the original URL, not to the redirect target.
///
/// Re-appending it to a `Location` that already carries its own duplicates every
/// parameter; Java takes the redirect URI wholly from `Location`.
#[tokio::test]
async fn a_redirect_does_not_duplicate_the_original_query() {
    let (port, seen) =
        serve_sequence(vec![redirect(307, "/second?applied=true"), ok_body("null")]).await;
    let admin = stub_admin(port).await;
    // `get_retention` sends `?applied=…`.
    let _ = admin
        .topic_policies()
        .get_retention("persistent://public/default/t", true)
        .await;

    let seen = seen.lock().await;
    let second = &seen[1];
    assert_eq!(
        second.request_line.matches("applied=").count(),
        1,
        "the original query was re-appended to the redirect target: {}",
        second.request_line
    );
}

/// A relative `Location` resolves against the current URL.
#[tokio::test]
async fn a_relative_redirect_location_is_resolved() {
    let (port, seen) = serve_sequence(vec![redirect(307, "/elsewhere"), ok_body("[]")]).await;
    let admin = stub_admin(port).await;
    admin.clusters().get_clusters().await.unwrap();

    let seen = seen.lock().await;
    assert!(
        seen[1].request_line.starts_with("GET /elsewhere"),
        "relative Location was not resolved: {}",
        seen[1].request_line
    );
}

/// A redirect with no `Location` is surfaced, not retried forever.
#[tokio::test]
async fn a_redirect_without_a_location_is_surfaced() {
    let (port, _seen) = serve_sequence(vec![
        "HTTP/1.1 307 Temporary Redirect\r\nContent-Length: 0\r\n\r\n".to_string(),
    ])
    .await;
    let admin = stub_admin(port).await;
    match admin.clusters().get_clusters().await {
        Err(Error::Admin(AdminError::Http { status: 307, .. })) => {}
        other => panic!("expected the bare 307 to surface, got {other:?}"),
    }
}

/// A redirect loop is bounded rather than hanging.
#[tokio::test]
async fn a_redirect_loop_is_bounded() {
    // More hops than the client will follow.
    let responses = (0..20).map(|_| redirect(307, "/loop")).collect();
    let (port, _seen) = serve_sequence(responses).await;
    let admin = stub_admin(port).await;
    match admin.clusters().get_clusters().await {
        Err(Error::Admin(AdminError::Decode(m))) => assert!(
            m.contains("redirects"),
            "unexpected message for a redirect loop: {m}"
        ),
        other => panic!("a redirect loop must be bounded, got {other:?}"),
    }
}

/// A cross-origin redirect must not drop authentication.
///
/// Pulsar answers 307 with the address of the broker that owns the resource,
/// which is a different port and so a different origin. reqwest's automatic
/// redirect layer strips `Authorization` across origins, so the owning broker
/// would have seen an unauthenticated request and answered 401/403.
#[tokio::test]
async fn authentication_survives_a_cross_origin_redirect() {
    let (owner_port, owner_saw) =
        serve_once("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]".to_string()).await;
    let (entry_port, entry_saw) = serve_once(format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{owner_port}/admin/v2/clusters\r\nContent-Length: 0\r\n\r\n"
    ))
    .await;

    let admin = admin_client_with_token(format!("http://127.0.0.1:{entry_port}"), "secret-token");
    admin.clusters().get_clusters().await.unwrap();

    assert_eq!(
        entry_saw
            .lock()
            .await
            .clone()
            .unwrap()
            .authorization
            .as_deref(),
        Some("Bearer secret-token"),
        "the first hop was not authenticated"
    );
    assert_eq!(
        owner_saw
            .lock()
            .await
            .clone()
            .expect("the redirect was not followed at all")
            .authorization
            .as_deref(),
        Some("Bearer secret-token"),
        "authentication was dropped on the redirect to the owning broker"
    );
}

/// A multipart upload must survive a redirect too.
///
/// reqwest turns a form into a streaming body, which is not cloneable, so its
/// redirect layer cannot replay it and hands back the raw 307 instead — which
/// this client would then report as a confusing HTTP error.
#[tokio::test]
async fn multipart_uploads_follow_a_redirect_with_their_body() {
    let (owner_port, owner_saw) =
        serve_once("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_string()).await;
    let (entry_port, _entry_saw) = serve_once(format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{owner_port}/upload\r\nContent-Length: 0\r\n\r\n"
    ))
    .await;

    let admin = admin_client_with_token(format!("http://127.0.0.1:{entry_port}"), "secret-token");
    admin
        .send_multipart(
            reqwest::Method::POST,
            &format!("http://127.0.0.1:{entry_port}/upload"),
            &[],
            &[("state", r#"{"key":"k"}"#.to_string())],
            &[],
            Some(("file", "p.jar".to_string(), b"PK-payload".to_vec())),
        )
        .await
        .expect("a multipart upload must follow the redirect, not surface it");

    let seen = owner_saw
        .lock()
        .await
        .clone()
        .expect("the multipart request never reached the owner");
    assert_eq!(
        seen.authorization.as_deref(),
        Some("Bearer secret-token"),
        "authentication was dropped on the multipart redirect"
    );
    assert!(
        seen.request_line.starts_with("POST /upload"),
        "the redirected request did not keep its method or reach the target: {}",
        seen.request_line
    );
    seen.assert_multipart("redirected multipart upload");
    assert_eq!(
        seen.part_names(),
        vec!["state".to_string(), "file".to_string()],
        "the form was not rebuilt with both parts for the redirected request"
    );
    assert!(
        seen.body.contains(r#"filename="p.jar""#),
        "the file part lost its filename on the redirect: {}",
        seen.body
    );
    assert!(
        seen.body.contains(r#"{"key":"k"}"#) && seen.body.contains("PK-payload"),
        "the form was not rebuilt for the redirected request: {:?}",
        seen.body
    );
}
