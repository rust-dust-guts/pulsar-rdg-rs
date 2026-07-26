<p align="center">
  <img src="https://avatars.githubusercontent.com/u/309050667?s=200&v=4" alt="Rust, Dust &amp; Guts" width="140" />
</p>

<h1 align="center">pulsar-rdg-client</h1>

<p align="center">
  Pure-Rust, runtime-agnostic async client for <a href="https://pulsar.apache.org/">Apache Pulsar</a>
</p>

<p align="center">
  <a href="https://crates.io/crates/pulsar"><img src="https://img.shields.io/crates/v/pulsar.svg" alt="crates.io" /></a>
  <a href="https://docs.rs/pulsar"><img src="https://img.shields.io/docsrs/pulsar" alt="docs.rs" /></a>
</p>

---

Part of [**Rust, Dust & Guts**](https://github.com/rust-dust-guts) — *agent-assisted rewrite of
open-source libraries and software to Rust. The goal is to fullfill the missing gaps in Rust
ecosystem. Use it on your own risk.*

This is a fork of [`streamnative/pulsar-rs`](https://github.com/streamnative/pulsar-rs) carrying
agent-assisted implementations of features the upstream client is missing, while waiting for official
support. **Use it at your own risk.**

A pure Rust client for Apache Pulsar that does not depend on the C++ Pulsar library. It provides an
async/await based API, compatible with [Tokio](https://tokio.rs/) and [async-std](https://async.rs/).

## Features

Inherited from upstream:

- URL based (`pulsar://` and `pulsar+ssl://`) connections with DNS lookup;
- Multi topic consumers (based on a regex or list);
- TLS connection;
- Configurable executor (Tokio or async-std);
- Automatic reconnection with exponential back off;
- Message batching;
- Compression with LZ4, zlib, zstd or Snappy (can be deactivated with Cargo features);
- Telemetry using [tracing](https://github.com/tokio-rs/tracing) crate (can be activated with Cargo features).

Added in this fork:

- **Java-compatible partition routing** — `HashingScheme::{JavaStringHash, Murmur3_32Hash}`,
  defaulting to `JavaStringHash` as the Java client does, so the same key reaches the same partition
  across clients. Upstream hashes keys in a way that matches no other Pulsar client;
- **Keys honoured under every routing policy**, including the unconfigured default;
- **Protocol feature negotiation** — the client advertises what it implements and reads the broker's
  capabilities back via `Pulsar::broker_features()`;
- **PIP-344 partition metadata lookup without topic auto-creation**;
- **Chunked messages detected and rejected** rather than delivered as truncated payloads;
- Wire protocol brought to field-level parity with Pulsar 5.0 for all non-scalable-topic messages;
- **A near-complete Admin API** — 498 operations across all 21 of the Java client's admin interfaces,
  up from 3 upstream, grouped behind accessors (`admin.namespaces()`, `admin.topics()`, …) with
  typed request and response models rather than raw JSON. Enable the `admin-api` feature. Works
  under both executors: requests run on the ambient Tokio runtime when there is one, and on a
  small runtime the client owns otherwise, so async-std callers are supported too;
- **Scalable topics (PIP-460)** — `topic://` names, the segment DAG, split and merge, and the
  segment-scoped subscription operations, against Pulsar 5.0.0-M1.

See [docs/feature-gap-plan.md](docs/feature-gap-plan.md) for the full Java-vs-Rust feature matrix,
what this fork changed, and the roadmap.

## Getting Started

Add the following dependencies in your `Cargo.toml`:

```toml
futures = "0.3"
pulsar = "7.0.0"
tokio = "1.0"
```

Try out [examples](examples):

- [producer](examples/producer.rs)
- [consumer](examples/consumer.rs)
- [reader](examples/reader.rs)

## Running the tests

The integration tests need a live broker. `scripts/start_test_broker.sh` starts a throwaway Pulsar
standalone — plus a proxy in front of it, for the `proxy-stats` tests — on random free ports, and
prints the environment the suite reads:

```bash
broker_env=$(./scripts/start_test_broker.sh) && eval "$broker_env" && cargo test --features admin-api
```

Clean up with `docker rm -f pulsar-rs-test pulsar-rs-test-proxy`. Set `SKIP_PROXY=1` to start the
broker alone; the `proxy-stats` tests then skip. All four of `PULSAR_BROKER_URL`, `PULSAR_ADMIN_URL`,
`PULSAR_PROXY_URL` and `PULSAR_PROXY_ADMIN_URL` can also be set by hand to point at existing
services.

## Upgrading from upstream `pulsar-rs` 6.x

Version 7.0.0 changes partition routing to match the Java client. This is a **behaviour break**: a
6.x producer and a 7.x producer will route the same key to *different* partitions, so per-key
ordering is not preserved across a mixed-version fleet.

Upgrade all producers for a given topic together, or drain the topic before switching. See
[the migration note](docs/feature-gap-plan.md#migrating-partition-routing-from-6x) for details.

### Admin API source breaks

`AdminClient`'s flat methods moved onto groups (`admin.namespaces()`, `admin.topics()`, …) because
`Namespaces` and `TopicPolicies` define ~30 identically named operations that cannot coexist on one
type. Beyond that regrouping, these signatures changed in ways the compiler will point at:

| Was | Now | Why |
|---|---|---|
| `set_max_unacked_messages_per_consumer` | `topic_policies().set_max_unacked_messages_on_consumer` | matches the `on_consumer` / `on_subscription` pair |
| `remove_max_unacked_messages_per_consumer` | `topic_policies().remove_max_unacked_messages_on_consumer` | as above |
| `topics().create_subscription(topic, sub)` | `create_subscription(topic, sub, &MessageIdData::latest())` | it always sent `-1:-1`, which Pulsar defines as *earliest*, while documenting *latest* |
| `set_dispatcher_pause_on_ack_state_persistent(ns, bool)` | `set_dispatcher_pause_on_ack_state_persistent(ns)` | the broker ignores the body; POST enables and DELETE clears, so `false` read back as `true` |
| `set_namespace_replication_clusters(ns, clusters)` | `set_namespace_replication_clusters(ns, clusters, compare_topic_partitions)` | exposes Java's partition-compatibility guard |
| `update_function` / `update_sink` / `update_source` and their `_with_url` forms | same, plus a trailing `options: Option<&UpdateOptions>` | carries Java's `updateOptions` part, whose `updateAuthData` flag refreshes stored credentials; pass `None` for the old behaviour |

The request timeout is now configurable: `pulsar.admin_with_options(url, &AdminOptions { timeout })`.
`admin(url)` keeps the previous fixed 30-second timeout.

Policy getters no longer turn HTTP 404 into `Ok(None)`. An unset policy is reported by the broker as
200 with an empty body and still reads as `None`; a 404 now surfaces as `AdminError::NotFound`, so a
lookup against a namespace or topic that does not exist is no longer indistinguishable from one with
no override.

## Contribution

This project welcomes your PR and issues. For example, refactoring, adding features, correcting
English, etc.

### Credits

This fork stands on [`streamnative/pulsar-rs`](https://github.com/streamnative/pulsar-rs). The
overwhelming majority of this codebase is the work of its upstream maintainers and contributors:

- [@CleverAkanoa](https://github.com/CleverAkanoa)
- [@DonghunLouisLee](https://github.com/DonghunLouisLee)
- [@FlorentinDUBOIS](https://github.com/FlorentinDUBOIS)
- [@Geal](https://github.com/Geal)
- [@fantapsody](https://github.com/fantapsody)
- [@freeznet](https://github.com/freeznet)
- [@stearnsc](https://github.com/stearnsc)

<a href="https://github.com/streamnative/pulsar-rs/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=streamnative/pulsar-rs" alt="upstream contributors" />
</a>

[StreamNative](https://streamnative.io/), founded in 2019 by the original creators of Apache Pulsar,
maintains the upstream project and is one of the leading contributors to Apache Pulsar itself.

## License

This library is licensed under the terms of both the MIT license and the Apache License (Version
2.0), and may include packages written by third parties which carry their own copyright notices and
license terms.

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT), and [COPYRIGHT](COPYRIGHT) for
details.

## History

The upstream project was originally created by [@stearnsc](https://github.com/stearnsc) and others at
[Wyyerd](https://github.com/wyyerd) in 2018. In 2022 the original creators
[transferred the repository to StreamNative](https://github.com/streamnative-oss/sn-pulsar-rs/issues/20),
where it is actively maintained.

This fork was branched from upstream `master` at `95a0d73` under the
[Rust, Dust & Guts](https://github.com/rust-dust-guts) organization.
