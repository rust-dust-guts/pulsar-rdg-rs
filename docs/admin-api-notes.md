# Admin API — implementation notes

**231 lib tests + 20 doctests green**, plus 3 async-std tests in an
external target against Pulsar 5.0.0-M1;
`cargo fmt --all --check` and both CI clippy feature sets clean.

**498 operations across all 21 Java admin interfaces** (502 public call paths, four of
which are flat compatibility shims duplicating grouped semantics), every group covered
by broker-backed tests. All 21 interfaces are implemented; five individual operations remain, one
deliberately. See below.

See [Known gaps](#known-gaps) for what "tested" means per
operation — not every one is proven on its success path.

**Every request body and every policy type is a real struct.** `Policies` (52
fields) and `OffloadPolicies` (31 fields) are now real structs, and the two
enum-valued policies take `SubscriptionAuthMode` / `SchemaCompatibilityStrategy`
rather than `&str`.

Nine *responses* remain untyped, all of them genuinely free-form diagnostic
documents. See [Still untyped](#still-untyped).

## What landed

The admin client went from 3 operations to **498**, restructured from a single
`src/admin.rs` into a module per resource group (120 typed models).

### Architecture

Operations are grouped behind accessors, mirroring the Java client's separate
interfaces:

```rust
admin.clusters().create_cluster(..)
admin.namespaces().set_retention(..)
admin.tenants().create_tenant(..)
admin.brokers().get_leader_broker()
admin.bookies().get_bookies()
admin.resource_groups().get_resource_group(..)
admin.resource_quotas().get_default_resource_quota()
```

This was forced by a collision: `Namespaces` and `TopicPolicies` both define
`setRetention`, `setMaxProducers`, `setMaxUnackedMessages…` and ~30 more. Flat
methods on `AdminClient` cannot hold both — I hit the collision mid-build. Grouping
also keeps the remaining `Topics` work from colliding.

| Group | Ops | Tested |
|---|--:|---|
| `bookies` | 5 | ✅ rack info round-trip |
| `broker_stats` | 6 | ✅ all dumps decode; load report typed and pinned by fixture |
| `brokers` | 15 | ✅ all read-only endpoints + dynamic config round-trip; graceful shutdown checked against a stub |
| `clusters` | 19 | ✅ CRUD, peers, failure domains, isolation policies |
| `functions` | 26 | ✅ multipart create/update, package upload/download, trigger, status/stats, state, builtins |
| `lookup` | 3 | ✅ broker, bundle range, per-partition lookup |
| `metadata_migration` | 2 | ✅ phase read; a start with no target is refused before the store is touched |
| `namespaces` | 156 | ✅ CRUD, every policy, permissions (incl. bulk topic grants), properties, allowed clusters, anti-affinity groups, metric label allow-list, scalable-topic auto-scale, bundles |
| `non_persistent_topics` | 7 | ✅ own stats shape, internal stats, unload, partitioned variant |
| `packages` | 7 | ✅ byte-exact upload/download round-trip, typed name parser |
| `proxy_stats` | 4 | ✅ exercised through a real proxy: traffic shows up in its connection and topic stats |
| `resource_groups` | 5 | ✅ CRUD |
| `resource_quotas` | 5 | ✅ default quota round-trip |
| `scalable_topics` | 25 | ✅ create/split/merge/segments/subscriptions against 5.0.0-M1 |
| `schemas` | 8 | ✅ STRING and AVRO round-trips, versions, metadata, compatibility, forced delete |
| `sinks` | 17 | ✅ create/update by file and URL, lifecycle, builtins |
| `sources` | 17 | ✅ create/update by file and URL, lifecycle, builtins |
| `tenants` | 5 | ✅ CRUD + rejection of unknown clusters |
| `topic_policies` | 88 | ✅ every policy round-trips, plus namespace-override precedence and exact post-removal values |
| `topics` | 56 | ✅ lifecycle, partitions, subscriptions, cursors, stats, peek/examine, message addressing by storage position, maintenance |
| `transactions` | 16 | ✅ coordinators; topic-scoped stats asserted on success, coordinator-scoped on handler reach |
| `worker` | 6 | ✅ cluster, leader, typed assignments and metrics, rebalance |
| flat on `AdminClient` | 4 | ✅ compatibility shims kept from upstream; separate implementations, so tested separately |

**498 grouped operations plus 4 flat compatibility methods = 502 public call paths.**
That is Rust call paths, not distinct Java/REST operations: the four flat methods duplicate
grouped semantics, so the unique operation count is the grouped figure.

### Foundation

- **Typed error taxonomy.** `AdminError` now maps HTTP status onto
  `NotFound` / `Conflict` / `BadRequest` / `NotAuthorized` / `PreconditionFailed` /
  `NotAllowed` / `NotSupported` / `ServerUnavailable`, extracting the broker's
  `{"reason": …}` so the message is the broker's own explanation. Plus
  `is_retriable()`, which is true only for transient server-side conditions.
- **Request plumbing.** `send_empty` / `send_json` / `send_json_opt` / `send_text`,
  with per-segment percent-encoding that escapes `/` so a name cannot inject path
  segments.
- **Typed models** in `admin::models`, every shape verified against a live broker.
- **Redirects followed by hand.** Automatic redirects are off. reqwest strips
  `Authorization` when a redirect crosses origin — which is exactly what Pulsar's
  307 to the broker that owns a resource does — and it cannot replay a streaming
  multipart body at all, so uploads landing on a non-owner worker came back as the
  raw 307. Each hop re-applies auth and rebuilds the form.
- **Runtime-agnostic.** reqwest needs a Tokio reactor, so admin calls from an
  `async-std` task used to panic with "there is no reactor running". Requests run
  inline when a Tokio runtime is already current, and on a small shared runtime the
  client owns otherwise.
- **Configurable timeout.** `Pulsar::admin_with_options(url, &AdminOptions {
  timeout })`; `admin()` keeps the 30-second default. No retry layer, deliberately —
  Java's `BaseResource.sync()` has none either, so errors propagate.
- **Local and global topic policies.** `admin.topic_policies()` reaches the
  cluster-local set, `admin.topic_policies_global()` the geo-replicated one — Java's
  `topicPolicies()` and `topicPolicies(true)`. The flag appends `?isGlobal=true` to
  every request in the group, applied once in the group's URL builder rather than at
  ~90 call sites. The two really are separate stores: a global override is invisible
  to a local read and vice versa, which
  `global_topic_policies_are_a_separate_store` asserts in both directions. This was
  missed by the earlier per-interface diff because it is an *accessor* on
  `PulsarAdmin`, not a method on an interface.

## Findings worth knowing

**Java field names are not the wire format.** Three cases where trusting the Java
class would have shipped silently-broken code:

1. `ResourceGroup` is **plural** on the wire — `publishRateInMsgs`, not
   `publishRateInMsg`. The broker answers **204 and ignores the whole body** when
   names don't match, so only a read-back catches it.
2. `retentionSizeInMB` has an uppercase acronym. serde's `camelCase` produces
   `retentionSizeInMb`, the field arrives unset as 0, and the broker then rejects
   the body for "mixing a zero with a non-zero limit" — an error that points
   nowhere near the real cause.
3. `NamespaceIsolationData` is **snake_case**, unlike every other policy type. A
   camelCase body is rejected with 400.

All three are now pinned by unit tests that assert the exact JSON.

A fourth, found later and worth reading as a cautionary tale.
`AutoScalePolicyOverride` had five singular field names where the wire is plural —
`maxSegment` for `maxSegments`, `splitCooldownSecond` for `splitCooldownSeconds`,
and so on. The broker answered **204 and kept only `enabled`**. The test asserted
only `enabled` and carried a comment blaming the preview broker: *"5.0.0-M1 accepts
the whole body but persists only `enabled`… asserting them would test the preview's
incompleteness rather than this client."* That was wrong — the symptom was real, the
diagnosis was not, and the comment then protected the bug from being found. The rule
that follows: when a write appears to be ignored, suspect your own field names before
the broker, and assert the **whole** object on read-back rather than the one field
that happens to work.

**A few endpoints take their body as raw text, not JSON.** The namespace
anti-affinity group is one: the broker binds the entity straight onto a Java
`String`, so JSON-encoding it stores the name *with its quotes*. The getter then
echoes the stored text back — which is valid JSON for the same string — so a
get/set round-trip looked correct while
`getAntiAffinityNamespaces` could never find the namespace. Both directions now use
raw text, and `send_raw_text` exists for this case. The neighbouring
`schemaAutoUpdateCompatibilityStrategy` endpoint *does* take a typed enum, so JSON
is right there; the two cannot be told apart without reading the broker signature.

**Not every policy is removable.** Verified by probing: eight namespace policies
have a setter and getter but **no DELETE** — `deduplicationSnapshotInterval`,
`offloadThreshold`, `offloadThresholdInSeconds`, `encryptionRequired`,
`schemaValidationEnforced`, `isAllowAutoUpdateSchema`, `subscriptionAuthMode`,
`schemaCompatibilityStrategy`. The broker routes the unmatched DELETE to its
delete-bundle handler and answers `412 Invalid bundle range`. No `remove_*` is
generated for those.

**`maxTopicsPerNamespace` reports `Some(0)` after removal**, not `None`, unlike
every other scalar policy.

**`removeProperty` answers with the bare previous value**, not JSON.

**A broker-side race in `deleteResourceGroup`.** It validates that no namespace
references the group by walking every namespace; if one is deleted concurrently the
walk 404s with "Namespace does not exist". Reproducible, and it affects real callers,
not just tests. See **Resolved** below for how this is handled.

**Two proxy settings that silently disable what they gate.** `brokerProxyAllowedTargetPorts`
defaults to `6650,6651`, so a proxy in front of a broker on any other port refuses every
connection with `Given port … isn't allowed` — which surfaces to the client only as a generic
connect failure. And `/proxy-stats/topics` checks the **configured** `proxyLogLevel`, not the
running one: `ProxyStats.topics()` reads
`proxyService().getConfiguration().getProxyLogLevel()` while the `POST /proxy-stats/logging/{n}`
setter only calls `proxyService().setProxyLogLevel(…)`. Raising the level at runtime therefore
cannot unlock topic stats — it must be 2 at startup. Both are asserted.

**Proxy stat *rates* are calculated once, not periodically.** `ProxyService` schedules its
rate calculation with `statsExecutor.schedule(…, 60, SECONDS)` — a one-shot, not
`scheduleAtFixedRate`. So `requestRate` / `byteRate` / `msgRateIn` are `0.0` except in a narrow
window, and the tests assert on the presence of connection and topic *entries* instead.

## Known gaps

A per-method diff against all 21 Java admin interfaces leaves **five operations
unimplemented** — one deliberately, four not yet:

**Deliberate.** `Namespaces::getReplicationConfigVersion`. Java's client still offers
it, but `configversion` appears nowhere in the broker's admin resources — the path
falls through to the DELETE-only delete-bundle route, so a live broker answers 405
with a Jetty error page. Implementing it would add a method that can only ever fail.

**Not yet implemented**, all on `Topics`:

| Missing | Notes |
|---|---|
| `getSchemaValidationEnforced` / `set` | the *topic-level* pair; the namespace-level pair is present on `namespaces` |
| `createShadowTopic` | the shadow-topic *policy* (`get`/`set`/`remove_shadow_topics`) is present; creating one is not |
| `getShadowSource` | reads back which topic a shadow follows |

These four were missed because the per-method diff is name-based: `getMessageTTL`
does not match `get_message_ttl` under naive case conversion, so the residual list
had false positives that masked the real entries. Counting alone will not catch this
— each apparent difference has to be resolved by hand.

Some apparent gaps were not gaps, and are worth recording so the next diff does not
re-raise them. Java's `Clusters::create*`/`update*` failure-domain and
isolation-policy pairs both delegate to one `set*` call.
`Topics::enableDeduplication` / `disableDeduplication` / `getDeduplicationEnabled`
and `Functions::getSinks` / `getSources` are `@Deprecated` aliases.
`Schemas::getSchemaInfoWithVersion` hits the same route as `getSchemaInfo`, whose
version this client already carries on `SchemaInfo`. And a dozen more differ only by
name — `getTopics` is `get_namespace_topics`, `unload` is `unload_namespace`,
`setOffloadDeleteLag` is `set_offload_deletion_lag`, and so on.

Coverage is still not uniform, and it is worth being precise about what "tested"
means per operation:

* **Asserted to succeed** — the majority. The call returns and its response is
  checked.
* **Asserted to reach its handler** — operations whose success needs state this
  topology cannot create: a loaded transaction coordinator, a multi-broker cluster,
  a package URL the worker can fetch, a running function instance. These use
  `assert_reached_handler` / `assert_ok_or_handled!`, which fails on a wrong route,
  verb or form encoding but does not prove the happy path.
* **Asserted against a stub server** — `shutdown_broker_gracefully`, because
  calling it for real stops the broker (an earlier version of that test did exactly
  that and took the rest of the suite with it). A local server records the request
  so the verb, path and query parameters are still checked.

`LoadManagerReport` is typed from Java's `LocalBrokerData` but pinned only by a
fixture: a standalone answers `load-report` with 204, so there is no populated
response to decode here. It needs a real multi-broker cluster to verify live.

Both shapes that were previously pinned only by unit fixtures are now covered
against real traffic: the nested per-instance `metrics` object is asserted on the
broker's own JSON, and `FunctionState.byteValue` is asserted on the request the
client actually sends (the worker's state store never finishes initialising on a
standalone, so a round-trip is not available).

## Still untyped

Nine responses are not modelled, and all of them are genuinely free-form — the
document's shape depends on the deployment: `broker_stats().get_metrics()`,
`get_mbeans()`, `get_topics()`, `get_allocator_stats()` and
`get_pending_bookie_ops_stats()`, plus
per-instance status for `functions()`, `sinks()` and `sources()`.

The four that Java types and this client used to return raw — `worker()`'s
assignments and metrics, `broker_stats().get_load_report()` and
`transactions().get_position_stats_in_pending_ack()` — are now
`BTreeMap<String, Vec<String>>`, `Vec<Metrics>`, `LoadManagerReport` and
`PositionInPendingAckStats`, each covered by a test.

Function, sink and source *request* configs still carry
`BTreeMap<String, serde_json::Value>` for `userConfig`, `secrets` and `configs`.
That is arbitrary user data by definition, not a modelling gap.

## Resolved

**Retry on the `deleteResourceGroup` race — matched to Java.** Java's admin client
has **no retry layer at all**: `BaseResource.sync()` just waits on the future and
wraps whatever comes back. So `delete_resource_group` propagates the error, exactly
as Java does. The retry lives only in the test that creates the contention, with a
comment saying so.

## Broker quirks found while typing the policies

* **`subscriptionExpirationTime` (topic level) is set by query parameter**, not a
  JSON body. A body is accepted with 204 and silently ignored.
* **Retention must exceed the configured backlog quota.** This broker defaults to
  10GB, so a small `retentionSizeInMB` is rejected with a message that does not
  mention the default. `-1` (unlimited) always passes.
* **`managedLedgerOffloadedReadPriority` and `s3ManagedLedgerOffloadRegion` are
  accepted (204) but never echoed back** by 5.0.0-M1, so they cannot be
  round-trip asserted. Sending them is still correct.
* The offload read-back adds computed fields (`s3Driver`, `gcsDriver`,
  `fileSystemDriver`) that are not settable; they are ignored on decode.

## Multipart upload

Function, connector and package upload needed a `multipart/form-data` layer, added as
`send_multipart` with JSON, text and file parts. Field names match the Java client exactly
(`functionConfig` / `sinkConfig` / `sourceConfig` / `metadata` for the document, `data` / `file` for
the archive, `url` for a worker-fetched package).

The package round-trip is the strongest test of it: bytes uploaded must come back **byte-identical**
from `download`, which no amount of malformed multipart framing would survive.

Three more broker behaviours found here:

* **`rebalance` needs at least two workers** and refuses on a standalone with a message saying so.
* **Package `delete` removes the content but leaves the version listed.** Verified directly: after a
  delete both `download` and `get_metadata` answer 404, yet `list_package_versions` still reports the
  version. The test asserts the content is gone rather than trusting the listing.
* **The function state store initializes lazily**, so `get_function_state` can answer
  `ServerUnavailable` ("State storage client is not done initializing") on a freshly started worker.

`WorkerInfo.workerHostname` is also worth noting: the field is `workerHostname` on the wire, and a
wrong Rust field name decodes silently to `None` rather than failing — caught by asserting the value
is present, not merely that the response parsed.

## Test infrastructure change

`scripts/start_test_broker.sh` and the CI workflow now set
`forceDeleteNamespaceAllowed=true`, `forceDeleteTenantAllowed=true`,
`enablePackagesManagement=true` and `transactionCoordinatorEnabled=true`, and the
broker runs **with** its functions worker (previously `--no-functions-worker`) so the
function, connector, package and worker groups are exercised rather than skipped.

Both also start a **Pulsar proxy** in front of the broker, so `proxy-stats` is
exercised against a real proxy instead of only asserting a clean 404 from a broker.
The proxy joins the broker's network namespace, which is what makes
`advertisedAddress=127.0.0.1` correct for both: a client connecting through the proxy
still looks the topic up first and hands the proxy the broker's *advertised* address
to dial. On a separate network that address would be the proxy itself. `SKIP_PROXY=1`
starts the broker alone and those tests skip.

**An already-running container will not pick any of this up — restart it:**

```bash
broker_env=$(./scripts/start_test_broker.sh) && eval "$broker_env"
```
