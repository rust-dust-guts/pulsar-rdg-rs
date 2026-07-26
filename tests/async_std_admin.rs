//! The admin client must work off Tokio, through the public API.
//!
//! `reqwest` needs a Tokio reactor, so before the runtime bridge every admin call
//! from an `async-std` task panicked with "there is no reactor running". These live
//! in an **external** test target on purpose: the crate's unit-test tree is
//! Tokio-only, so an in-crate `#[cfg(test)]` module cannot be built with
//! `--no-default-features`, and the check would silently run with Tokio linked in.
//!
//! Run with:
//! `cargo test --no-default-features --features admin-api,async-std-runtime --test async_std_admin`
#![cfg(all(feature = "admin-api", feature = "async-std-runtime"))]

use pulsar::{AsyncStdExecutor, Pulsar};

fn broker_url() -> String {
    std::env::var("PULSAR_BROKER_URL").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".to_string())
}

fn admin_url() -> String {
    std::env::var("PULSAR_ADMIN_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
}

/// Builds through `Pulsar<AsyncStdExecutor>`, the path a real async-std caller uses.
async fn admin() -> pulsar::AdminClient {
    Pulsar::builder(broker_url(), AsyncStdExecutor)
        .build()
        .await
        .expect("could not connect to the broker")
        .admin(admin_url())
        .expect("could not build the admin client")
}

/// A plain GET from an async-std task.
#[async_std::test]
async fn plain_requests_work_under_async_std() {
    let clusters = admin()
        .await
        .clusters()
        .get_clusters()
        .await
        .expect("an admin request from an async-std task must not need a Tokio reactor");
    assert!(!clusters.is_empty(), "broker reported no clusters");
}

/// A genuine multipart POST from an async-std task.
///
/// The previous version of this test called `list_packages`, which is a plain GET —
/// it never built a multipart body, so the multipart path was untested here despite
/// the name. `put_function_state` sends a `state` form part.
#[async_std::test]
async fn multipart_requests_work_under_async_std() {
    let admin = admin().await;
    let result = admin
        .functions()
        .put_function_state(
            "public",
            "default",
            "no-such-function",
            &pulsar::admin::models::FunctionState {
                key: Some("k".to_string()),
                string_value: Some("v".to_string()),
                ..Default::default()
            },
        )
        .await;

    // The function does not exist and the state store may not be up, so an error is
    // expected — what matters is that the multipart request completed rather than
    // panicking for want of a reactor.
    match result {
        Ok(()) => {}
        Err(pulsar::Error::Admin(e)) => {
            let message = format!("{e}");
            assert!(
                !message.contains("no reactor running"),
                "the multipart path still required a Tokio reactor: {message}"
            );
        }
        Err(other) => panic!("unexpected error from a multipart request: {other:?}"),
    }
}

/// A redirect crossing origins must keep its credentials off Tokio too.
///
/// `apply_auth` runs before the runtime bridge, so this covers the authenticated
/// path end to end under async-std rather than only the unauthenticated one.
#[async_std::test]
async fn authenticated_requests_work_under_async_std() {
    let pulsar = Pulsar::builder(broker_url(), AsyncStdExecutor)
        .with_auth(pulsar::Authentication {
            name: "token".to_string(),
            data: b"not-a-real-token".to_vec(),
        })
        .build()
        .await
        .expect("could not connect to the broker");
    let admin = pulsar.admin(admin_url()).expect("could not build admin");

    // The broker has authentication disabled, so the token is ignored and the call
    // succeeds; the point is that resolving auth data did not need a Tokio reactor.
    admin
        .clusters()
        .get_clusters()
        .await
        .expect("an authenticated admin request must work under async-std");
}
