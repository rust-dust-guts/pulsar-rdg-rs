# pulsar-rs ↔ Java client feature gap analysis & roadmap

**Rust client:** `pulsar-rust-dust-guts`. Baseline for this analysis was `95a0d73` (v6.4.1) —
13,745 LOC across 30 files in `src/`, with a 1,066-line `PulsarApi.proto`. Phase 0 has since
landed (see the progress log below); the tree is now v7.0.0, 15,493 LOC across 31 files, proto
1,104 lines.
**Java reference:** `apache/pulsar` master (5.0.0 preview) — `pulsar-client-api` (v4, 82 API types),
`pulsar-client-api-v5` + `pulsar-client-v5` (new in 5.0, 14,317 LOC), `pulsar-client-admin-api` (35
interfaces), `pulsar-common/src/main/proto/PulsarApi.proto` (1,456 lines).

Method: interface-by-interface method extraction on the Java side, symbol/grep sweep on the Rust
side, plus a normalized field-level diff of the two `PulsarApi.proto` files. Everything marked ❌
below was confirmed absent, not assumed.

**Legend:** ✅ full · 🟡 partial / raw-protocol only · ❌ missing · ❌ᵍ still missing, but now
detected and rejected rather than silently mishandled.

The two Rust columns are two different clients:

| Column | Meaning |
|---|---|
| **pulsar-rs** | upstream [`streamnative/pulsar-rs`](https://github.com/streamnative/pulsar-rs) `master` @ `95a0d73` |
| **Rust RDG** | this fork, `pulsar-rust-dust-guts`, at its current working tree |

`95a0d73` is also this fork's merge-base and upstream's tip, so the fork carries no commits ahead of
it — every row where the two columns differ is work done here. Everything in the shared git history
(the admin REST client, `get_schema`, the connection data/control-plane split) is *upstream* work.
See [§12](#12-divergence-from-upstream-streamnativepulsar-rs) for the consolidated list.
**Priority:** **P0** silent incorrectness or interop break · **P1** blocks common production use ·
**P2** important, has a workaround · **P3** long tail

---

## Progress log

| Phase | Item | Status |
|---|---|---|
| Test infra | Configurable broker endpoints (`PULSAR_BROKER_URL` / `PULSAR_ADMIN_URL`), `scripts/start_test_broker.sh` | **done** |
| Test infra | CI matrix extended to Pulsar `5.0.0-M1` | **done** |
| Test infra | CI: `PULSAR_PREFIX_*` overrides now actually applied (+ guard step, namespace readiness wait) | **done** |
| Review | `NonZeroUsize` partition count, `make_hash(self)`, hot-path clone removed, version → 7.0.0 | **done** |
| Phase 0 | **D1** `HashingScheme` (Java parity, `JavaStringHash` default) | **done** |
| Phase 0 | **D2** `RoutingPolicy::Single` and the no-policy default now hash-route keyed messages | **done** |
| Phase 0 | Golden-vector harness generated from Pulsar's own Java source (`scripts/gen_java_hash_vectors.sh`, 100 vectors) | **done** |
| Phase 0 | **D3** chunked-message guard (reject rather than deliver truncated) | **done** |
| Phase 0 | `FeatureFlags` 4–9 + `CommandConnected.feature_flags`; client now advertises `supports_auth_refresh` / `supports_partial_producer` / PIP-344 | **done** |
| Phase 0 | PIP-344 `metadata_auto_creation_enabled` with capability check | **done** |
| Phase 0 | Remaining field-level proto gaps (all non-scalable-topic messages now match 5.0) | **done** |
| Phase 0 | Per-message null/b64 flags preserved when unpacking batches | **done** |
| Phase 0 | **Publishing** a protocol null value or a binary key | **not done** — see below |
| **Phase 0** | **substantially complete**, one item outstanding | ⚠️ |
| Phase 1 | null-value + binary-key publish/consume API, `autoUpdatePartitions`, TLS client auth, `listenerName`, reader primitives, ack grouping, … | next |

**Correction to an earlier completion claim.** Phase 0 originally listed
"`null_value` / `null_partition_key` / `partition_key_b64_encoded` proto fields **+ handling**", and
was marked complete when only the field-level work was finished — which turned out to be a no-op,
since those fields already existed upstream. The consume side is now correct (per-message flags are
no longer inherited from the batch envelope), but the **publish** side is not:

* `producer::Message` models the payload as `Vec<u8>` and the key as `Option<String>`, so it cannot
  express "value absent" as distinct from "empty value", nor a binary key.
* A Java consumer therefore cannot distinguish a Rust empty payload from a null one.
* Binary keys additionally need base64 encoding *before* hashing, to match
  `TypedMessageBuilderImpl.keyBytes` — see the note on `HashingScheme::make_hash`.

This needs an API change to `producer::Message` plus bidirectional Rust↔Java round-trip tests, so it
is carried into Phase 1 as its own item rather than left implied.

Test count: **50 → 73** (plus 20 doctests). All green against Pulsar 5.0.0-M1; `cargo fmt --all
--check` and both CI clippy feature sets clean.

### Correction to §1 (wire protocol)

The original proto comparison over-reported the gap. The normalizer collapsed runs of
whitespace but Java writes `[ default = false ]` with spaces inside the brackets, so
identical fields diffed as missing. Re-run with whitespace stripped entirely,
`MessageMetadata.partition_key_b64_encoded`, `SingleMessageMetadata.{null_value,
null_partition_key, partition_key_b64_encoded}` and `CommandSend.is_chunk` were **already
present**. The genuine field-level gaps were the ones now closed; the real remaining gap is
the scalable-topic command set (29 messages, 18 command types), deferred to Phase 5.

### Test parity target

The Java client's ~2,160 `@Test` methods break down as: `pulsar-client` unit 704, `pulsar-client-v5`
96, `pulsar-client-admin` 52, `pulsar-client-api-v5` 14, plus the client-facing integration suites
that live in `pulsar-broker` — `client/api` 801 and `client/impl` 493. Rust is at 73.

Closing that as a standalone project would cost more than all the feature phases combined, and a raw
count is a weak metric anyway (much of the Java total is the same behaviour re-run across broker
configurations). The workable rule, which converges to the same place:

> **Every feature ported also ports the Java tests for that feature.** A phase is not done until its
> Rust test count is within striking distance of the corresponding Java test count, counted per
> feature rather than in aggregate.

Applied to D1/D2: Java covers hashing in `Murmur3_32HashTest`, `JavaStringHashTest`,
`MessageRouterTest`, `RoundRobinPartitionMessageRouterImplTest`,
`SinglePartitionMessageRouterImplTest` and `PartitionedProducerConsumerTest` — 11 relevant `@Test`
methods. We shipped 13 (9 unit + 4 broker-backed), and the golden-vector table exercises 100 keys ×
9 partition counts × 2 schemes = 1,800 assertions the Java suite has no equivalent of.

---

## 0. Correctness defects found during the sweep

These are not "gaps" — they are cases where the Rust client does something silently different from
every other Pulsar client. They should land before any feature work.

### D1 — Partition routing is incompatible with every other Pulsar client · **P0** — ✅ FIXED

The original code in [`src/routing_policy.rs`](../src/routing_policy.rs) was:

```rust
let hash = murmur3_32(&mut key.as_bytes(), 0).unwrap_or(0);
(hash % partition_count as u32) as usize
```

Java has two hashing schemes and **defaults to `JavaStringHash`** (`ProducerConfigurationData:135`):

| Scheme | Java computation |
|---|---|
| `JavaStringHash` (default) | `signSafeMod(String.hashCode(key), n)` |
| `Murmur3_32Hash` | `signSafeMod(murmur3_32(key, seed=0) & 0x7FFFFFFF, n)` |

The Rust implementation matches **neither**. Two independent consequences:

1. A Rust producer and a Java producer publishing the same key to the same partitioned topic pick
   **different partitions**. Per-key ordering is silently broken in any mixed-language fleet, and the
   breakage is invisible — no error, no warning, just interleaved keys.
2. Even if a `Murmur3_32Hash` option existed, the missing `& 0x7FFFFFFF` mask (`Murmur3_32Hash.java:53`)
   makes Rust diverge for every key whose hash has bit 31 set — roughly half of them.

**Fixed.** Added a `HashingScheme` enum defaulting to `JavaStringHash`, masked murmur to 31 bits, and
pinned both schemes to 100 golden vectors generated from Pulsar's own Java source.

Measured blast radius, from the generator run: the `murmur3` crate's *algorithm* is byte-identical to
Pulsar's `Murmur3_32Hash` — the sole defect was the missing `& 0x7FFFFFFF`, which misrouted **exactly
50 of 100** sample keys. The `JavaStringHash` default was simply absent, so *every* key diverged from
a default-configured Java producer.

### D2 — `RoutingPolicy::Single` ignores the partition key · **P0** — ✅ FIXED

`choose_partition` returned a fixed producer for `Single`, discarding `message.partition_key`. Java's
`SinglePartitionMessageRouterImpl` hash-routes whenever a key is present and only falls back to its
fixed partition for unkeyed messages.

The same bug was also present — and worse — in the `None` arm: a producer that never configures a
routing policy at all did pure round-robin and ignored keys entirely. That is the default path, so it
affected more users than `Single` did. Both now route through the shared `route_by_key` helper.

### D3 — Chunked messages are delivered as raw fragments · **P0**

Zero occurrences of `chunk` anywhere in `src/`. `MessageMetadata` in the fork's proto does carry
`uuid`/`chunk_id`/`num_chunks_from_msg`/`total_chunk_msg_size`, and the broker will happily dispatch
chunks to a Rust consumer. The consumer has no reassembly path, so each fragment surfaces as an
independent message with a truncated payload. Any topic written by a chunking Java producer is
**silently corrupted on read**. Until reassembly exists, the consumer should at minimum detect
`num_chunks_from_msg > 1` and error rather than hand back a fragment.

### D4 — `with_receiver_queue_size(0)` silently becomes 1000 · **P3**

[`src/consumer/options.rs`](../src/consumer/options.rs) — `// todo: support zero_queue_size consumer`.
Requesting a zero-queue (fully synchronous) consumer gets you a 1000-deep prefetch instead. Should
reject or implement, not silently substitute.

---

## 1. Wire protocol coverage

Strict field-level diff of the two `.proto` files (whitespace stripped entirely — see the
correction above). **All field-level gaps are now closed**: every message the fork defines matches
its Pulsar 5.0 counterpart field for field. What remains is the scalable-topic and watcher command
set — **28 messages/enums and 17 `BaseCommand.Type` values**, all of them Phase 5 subsystems rather
than individual fields.

| Protocol area | Commands | Java | pulsar-rs | Rust RDG | Prio | Consequence of absence |
|---|---|:--:|:--:|:--:|:--:|---|
| Topic list watcher | `WATCH_TOPIC_LIST` 64-67 | ✅ | ❌ | ❌ | **P1** | Regex/pattern subscriptions fall back to client-side polling |
| Topic migration | `TOPIC_MIGRATED` 68 | ✅ | ❌ | ❌ | **P1** | Blue/green cluster migration breaks the client instead of redirecting |
| Scalable topic DAG watch | `SCALABLE_TOPIC_LOOKUP` 70, `_UPDATE` 71, `_CLOSE` 72 | ✅ | ❌ | ❌ | **P2** | No `topic://` support at all |
| Scalable consumer controller | `SCALABLE_TOPIC_SUBSCRIBE` 73, `_RESPONSE` 74, `_ASSIGNMENT_UPDATE` 75 | ✅ | ❌ | ❌ | **P2** | No StreamConsumer / CheckpointConsumer |
| Scalable namespace watch | `WATCH_SCALABLE_TOPICS` 76-78 | ✅ | ❌ | ❌ | **P3** | No multi-topic scalable consumer |
| TC discovery | `WATCH_TC_ASSIGNMENTS` 79-81 | ✅ | ❌ | ❌ | **P3** | Prereq for 5.0 transactions |
| `CommandConnected.feature_flags` (4) | — | ✅ | ❌ | ✅ | **P1** | Client could not learn broker capabilities |
| `FeatureFlags` 4-9 | `supports_topic_watchers`, `..._get_partitioned_metadata_without_auto_creation`, `..._repl_dedup_by_lid_and_eid`, `..._topic_watcher_reconcile`, `..._scalable_topics`, `..._tc_metadata_discovery` | ✅ | ❌ | ✅ | **P1** | Fork advertises 1, 3, 5 and reads all six from the broker |
| `CommandSend.message_id` (9) | — | ✅ | ❌ | ✅ | **P0** | Addresses a chunk within a chunked send (`is_chunk` was already present) |
| `MessageMetadata.schema_id` (32) | — | ✅ | ❌ | ✅ | P2 | 5.0 schema-by-id |
| `MessageMetadata.entry_hash_min/max` (33/34) | — | ✅ | ❌ | ✅ | P2 | PIP-486 entry bucketing |
| `MessageMetadata.compacted_batch_indexes` (31) | — | ✅ | ❌ | ✅ | P3 | Batch-aware compaction |
| `MessageMetadata`/`SingleMessageMetadata` `partition_key_b64_encoded`, `null_value`, `null_partition_key` | — | ✅ | ✅ | ✅ | — | Already present upstream; originally mis-reported as missing (see correction above) |
| `CommandClose{Consumer,Producer}.assignedBrokerServiceUrl(Tls)` | — | ✅ | ❌ | ✅ | P2 | Field present; not yet consumed, so a close still costs an extra lookup |
| `CommandPartitionedTopicMetadata.metadata_auto_creation_enabled` | — | ✅ | ❌ | ✅ | **P1** | Metadata lookup no longer has to auto-create the topic |
| `CommandGetTopicsOfNamespace.properties` | — | ✅ | ❌ | ✅ | P3 | Property-filtered topic listing |
| `KeySharedMeta.entryBucketDispatch` | — | ✅ | ❌ | ✅ | P2 | PIP-486 |
| `CommandConnect.proxy_version` | — | ✅ | ❌ | ✅ | P3 | Proxy observability |
| `Schema.Type` `AutoConsume`(21), `External`(22) | — | ✅ | ❌ | ✅ | P2 | Schema layer |

---

## 2. Producer

| Feature | Java | pulsar-rs | Rust RDG | Prio |
|---|:--:|:--:|:--:|:--:|
| Hashing scheme (`JavaStringHash` / `Murmur3_32Hash`) | ✅ | ❌ | ✅ | **P0** (D1) |
| Routing: RoundRobin / SinglePartition / Custom | ✅ | 🟡 | ✅ | **P0** (D2) |
| Message chunking (`enableChunking`, `chunkMaxMessageSize`) | ✅ | ❌ | ❌ᵍ | **P0** (D3) |
| `autoUpdatePartitions` (+ interval) | ✅ | ❌ | ❌ | **P1** — partition count increases are never picked up; new partitions get no traffic for the process lifetime |
| `sendTimeout` | ✅ | ❌ | ❌ | **P1** — only a per-connection `outbound_channel_size` (default 100); no per-message deadline |
| `maxPendingMessages` / `maxPendingMessagesAcrossPartitions` | ✅ | 🟡 | 🟡 | **P1** — `block_queue_if_full` + channel depth is not an equivalent bound |
| `initialSequenceId` | ✅ | ❌ | ❌ | **P1** — broker-side dedup cannot survive a producer restart |
| `getStats()`, `getLastSequenceId()` | ✅ | ❌ | ❌ | **P1** |
| `BatcherBuilder` (`KEY_BASED`) | ✅ | ❌ | ❌ | **P1** — key-based batching is required for correct Key_Shared delivery with batching on |
| E2E encryption (`cryptoKeyReader`, `addEncryptionKey`, `cryptoFailureAction`) | ✅ | ❌ | ❌ | P2 — proto fields are pass-through only; `ProducerOptions.encrypted` is documented "not implemented yet" |
| Producer access mode | ✅ | 🟡 | 🟡 | P2 — raw `Option<i32>`, no typed enum |
| `compressionMinMsgBodySize` | ✅ | ❌ | ❌ | P3 |
| `roundRobinRouterBatchingPartitionSwitchFrequency` | ✅ | ❌ | ❌ | P3 |
| `enableMultiSchema` | ✅ | ❌ | ❌ | P3 |
| `enableLazyStartPartitionedProducers` | ✅ | ❌ | ❌ | P3 |
| Producer interceptors | ✅ | ❌ | ❌ | P3 |
| `disableReplication` / `replicationClusters` per message | ✅ | ❌ | ❌ | P3 |
| Batching (size / bytes / timeout) | ✅ | ✅ | ✅ | — |
| Compression LZ4 / ZLIB / ZSTD / SNAPPY | ✅ | ✅ | ✅ | — |
| `deliverAt` / `deliverAfter` | ✅ | ✅ | ✅ | — |
| `flush` | ✅ | ✅ (`send_batch`) | ✅ (`send_batch`) | — |
| Ordering key, properties, event time | ✅ | ✅ | ✅ | — |

## 3. Consumer

| Feature | Java | pulsar-rs | Rust RDG | Prio |
|---|:--:|:--:|:--:|:--:|
| Chunked message reassembly | ✅ | ❌ | ❌ᵍ | **P0** (D3) |
| `autoUpdatePartitions` | ✅ | ❌ | ❌ | **P1** — same as producer: added partitions are never consumed |
| `acknowledgmentGroupTime` / `maxAcknowledgmentGroupSize` | ✅ | ❌ | ❌ | **P1** — every ack is its own command; measurable throughput cost at high rates |
| `enableBatchIndexAcknowledgment` | ✅ | ❌ | ❌ | **P1** — acking one message in a batch redelivers the whole batch |
| `enableRetry` / retry-letter topic / `reconsumeLater` | ✅ | ❌ | ❌ | **P1** — the standard delayed-retry pattern is unavailable |
| DLQ policy completeness | ✅ | 🟡 | 🟡 | **P1** — has `max_redeliver_count` + `dead_letter_topic`; missing `retryLetterTopic`, `initialSubscriptionName`, producer customizer |
| `KeySharedPolicy` (AUTO_SPLIT / STICKY ranges, `allowOutOfOrderDelivery`) | ✅ | ❌ | ❌ | **P1** — `Key_Shared` subtype works but no `KeySharedMeta` is sent, so sticky ranges are impossible |
| `negativeAckRedeliveryDelay` / backoff / precision | ✅ | ❌ | ❌ | **P1** — `nack` exists but redelivery timing is not configurable |
| `hasReachedEndOfTopic` | ✅ | 🟡 | 🟡 | **P1** — engine consumes the command internally ([`consumer/engine.rs:364`](../src/consumer/engine.rs:364)) but exposes no API |
| `replicateSubscriptionState` | ✅ | ❌ | ❌ | **P1** — geo-replicated subscriptions |
| `pause()` / `resume()` | ✅ | ❌ | ❌ | P2 |
| `batchReceive` + `BatchReceivePolicy` | ✅ | ❌ | ❌ | P2 |
| `ackTimeout` + `ackTimeoutTickTime` | ✅ | 🟡 | 🟡 | P2 — `unacked_message_redelivery_delay` covers the basic case; no tick time, no `ackTimeoutRedeliveryBackoff` |
| Pattern subscription | ✅ | 🟡 | 🟡 | P2 — Rust polls `get_topics_of_namespace` on a 30 s timer; Java uses broker-push `WATCH_TOPIC_LIST`. `RegexSubscriptionMode` (persistent / non-persistent / all) missing entirely |
| E2E decryption + `cryptoFailureAction` + `DecryptFailListener` | ✅ | ❌ | ❌ | P2 |
| `TransactionIsolationLevel` | ✅ | ❌ | ❌ | P2 |
| `subscriptionProperties` | ✅ | ❌ | ❌ | P2 |
| `isAckReceiptEnabled` | ✅ | ❌ | ❌ | P2 |
| `startPaused` | ✅ | ❌ | ❌ | P3 |
| `autoScaledReceiverQueueSizeEnabled` | ✅ | ❌ | ❌ | P3 |
| `maxTotalReceiverQueueSizeAcrossPartitions` | ✅ | ❌ | ❌ | P3 |
| `messageListener` + `messageListenerExecutor` | ✅ | ❌ | ❌ | P3 — `Stream` impl is the idiomatic Rust equivalent |
| `consumerEventListener` | ✅ | 🟡 | 🟡 | P3 — handled internally, not surfaced |
| Consumer interceptors | ✅ | ❌ | ❌ | P3 |
| `poolMessages` | ✅ | ❌ | ❌ | P3 |
| `MessagePayloadProcessor` | ✅ | ❌ | ❌ | P3 |
| `topicConfiguration` (per-topic overrides) | ✅ | ❌ | ❌ | P3 |
| Exclusive / Failover / Shared / Key_Shared | ✅ | ✅ | ✅ | — |
| ack / cumulative ack / nack | ✅ | ✅ | ✅ | — |
| `seek` by message id and by timestamp | ✅ | ✅ | ✅ | — |
| `getLastMessageId(s)` | ✅ | ✅ | ✅ | — |
| `unsubscribe`, `redeliverUnacknowledged`, `getStats`, durable/non-durable, `priorityLevel`, `readCompacted`, `receiverQueueSize` | ✅ | ✅ | ✅ | — |

## 4. Reader

| Feature | Java | pulsar-rs | Rust RDG | Prio |
|---|:--:|:--:|:--:|:--:|
| `hasMessageAvailable()` | ✅ | ❌ | ❌ | **P1** — the canonical "read to end of topic then stop" loop cannot be written |
| `startMessageIdInclusive` | ✅ | ❌ | ❌ | **P1** — off-by-one on resume from a stored position |
| Partitioned-topic reader | ✅ | ❌ | ❌ | P2 — explicitly rejected in `cf67345` |
| Multi-topic reader | ✅ | ❌ | ❌ | P2 |
| `startMessageFromRollbackDuration` | ✅ | ❌ | ❌ | P3 |
| `keyHashRange` | ✅ | ❌ | ❌ | P3 |
| `readerListener` | ✅ | ❌ | ❌ | P3 |
| `startMessageId`, `seek`, `getLastMessageId`, `readCompacted` | ✅ | ✅ | ✅ | — |

## 5. Schema

The Rust client has no schema *layer* — only a raw `proto::Schema` you fill in by hand, plus
`SerializeMessage`/`DeserializeMessage` traits the application implements itself.

| Feature | Java | pulsar-rs | Rust RDG | Prio |
|---|:--:|:--:|:--:|:--:|
| Multi-version decode driven by per-message `schema_version` | ✅ | 🟡 | 🟡 | **P1** — `Consumer::get_schema()` and producer-side `schema_version` attach exist (`fef65c1`), but there is no version→reader cache, so a topic that has evolved its schema decodes with the wrong reader |
| Typed primitives (STRING, INT8-64, BOOL, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, INSTANT, LOCAL_*) | ✅ | ❌ | ❌ | P2 |
| AVRO | ✅ | ❌ | ❌ | P2 — the most common wire schema in Pulsar deployments |
| `KeyValue` (INLINE / SEPARATED) | ✅ | ❌ | ❌ | P2 — admin client can *parse* KeyValue schema JSON but producers/consumers cannot use it |
| `AUTO_CONSUME` / `AUTO_PRODUCE_BYTES` / `GenericRecord` | ✅ | ❌ | ❌ | P2 |
| PROTOBUF / PROTOBUF_NATIVE | ✅ | ❌ | ❌ | P3 |
| `SchemaBuilder` / `SchemaDefinition` / `RecordSchemaBuilder` | ✅ | ❌ | ❌ | P3 |
| JSON | ✅ | 🟡 (hand-rolled per type) | 🟡 (hand-rolled per type) | — |

## 6. Security & authentication

| Feature | Java | pulsar-rs | Rust RDG | Prio |
|---|:--:|:--:|:--:|:--:|
| TLS client-certificate auth (`AuthenticationTls`) | ✅ | ❌ | ❌ | **P1** — no `tlsCertificateFilePath` / `tlsKeyFilePath`; mTLS-authenticated clusters are unreachable |
| `listenerName` (advertised listener) | ✅ | ❌ | ❌ | **P1** — required for Kubernetes / multi-network clusters; without it lookups return unreachable internal addresses |
| `proxyServiceUrl` + `ProxyProtocol` (SNI routing) | ✅ | ❌ | ❌ | P2 — `proxy_through_service_url` is *read* in `service_discovery.rs:312` but not configurable |
| Athenz | ✅ | ❌ | ❌ | P3 |
| SASL / Kerberos | ✅ | ❌ | ❌ | P3 |
| `tlsCiphers` / `tlsProtocols` / `sslFactoryPlugin` | ✅ | ❌ | ❌ | P3 |
| Socks5 proxy | ✅ | ❌ | ❌ | P3 |
| TLS transport, hostname verification, custom CA chain | ✅ | ✅ | ✅ | — |
| Token, Basic, OAuth2 (client credentials), custom `Authentication` trait, auth challenge / token refresh | ✅ | ✅ | ✅ | — |

## 7. Client & connection

| Feature | Java | pulsar-rs | Rust RDG | Prio |
|---|:--:|:--:|:--:|:--:|
| `getPartitionedMetadata` without auto-creation (PIP-344) | ✅ | ❌ | ✅ | **P1** — every lookup used to create a topic as a side effect |
| Topic-migration handling (`CommandTopicMigrated`) | ✅ | ❌ | ❌ | **P1** |
| `connectionsPerBroker` | ✅ | ❌ | ❌ | P2 — one connection per broker address (`connection_manager.rs:177`); a single TCP stream caps throughput |
| `memoryLimit` (client-wide) | ✅ | ❌ | ❌ | P2 — no backpressure ceiling across producers |
| `statsInterval` / built-in client metrics | ✅ | ❌ | ❌ | P2 |
| OpenTelemetry | ✅ | 🟡 | 🟡 | P2 — `telemetry` feature emits `tracing` spans; no OTel metrics |
| `maxConcurrentLookupRequests` / `maxLookupRequests` / `maxLookupRedirects` | ✅ | ❌ | ❌ | P2 |
| `TableView` | ✅ | ❌ | ❌ | P2 |
| Transactions (client-side) | ✅ | ❌ | ❌ | P2 — only a `ServerError::InvalidTxnStatus` variant exists |
| `serviceUrlProvider` / `updateServiceUrl` | ✅ | ❌ | ❌ | P3 |
| `AutoClusterFailover` / `ControlledClusterFailover` / URL quarantine | ✅ | ❌ | ❌ | P3 |
| `dnsServerAddresses` / `dnsLookupBind` | ✅ | ❌ | ❌ | P3 |
| Shared resources / thread-pool config | ✅ | n/a | n/a | — (executor abstraction) |
| `keepAliveInterval`, `operationTimeout`, connection retry/backoff, `connectionMaxIdleSeconds`, `getPartitionsForTopic` | ✅ | ✅ | ✅ | — |

---

## 8. Admin API

This is the largest gap by raw surface area. Java exposes ~550 unique operations across 21
interfaces (~1,400 methods counting `*Async` variants). The Rust `AdminClient`
([`src/admin.rs`](../src/admin.rs), 519 lines including tests) exposes 3.

| Java interface | Unique ops | Rust RDG | Coverage |
|---|:--:|:--:|:--:|
| `Namespaces` | 156 | 0 | 0% |
| `Topics` | 133 | 1 (`maxUnackedMessagesOnConsumer` set/remove) | <1% |
| `TopicPolicies` (v2 policy path) | 88 | 0 | 0% |
| **`ScalableTopics`** (new in 5.0) | 24 | 0 | 0% |
| `Clusters` | 21 | 0 | 0% |
| `Transactions` | 16 | 0 | 0% |
| `Brokers` | 13 | 0 | 0% |
| `Schemas` | 8 | 2 (`get`, `get at version`) | 25% |
| `NonPersistentTopics` | 7 | 0 | 0% |
| `Packages` | 7 | 0 | 0% |
| `BrokerStats` | 6 | 0 | 0% |
| `Tenants` | 5 | 0 | 0% |
| `Bookies` | 5 | 0 | 0% |
| `ResourceGroups` | 5 | 0 | 0% |
| `ResourceQuotas` | 5 | 0 | 0% |
| `Lookup` | 3 | 0 | 0% (binary-protocol lookup exists separately) |
| `Functions` / `Sources` / `Sinks` / `Worker` | ~74 | 0 | 0% |
| `ProxyStats` / `MetadataMigration` | 3 | 0 | 0% |

Also missing, and needed before bulk endpoint work is worth doing:

- **Typed policy models.** Java's `pulsar-common` carries ~200 `policies/data` classes
  (`RetentionPolicies`, `BacklogQuota`, `DispatchRate`, `PersistencePolicies`, `TopicStats`,
  `PartitionedTopicStats`, `OffloadPolicies`, `InactiveTopicPolicies`, …). Every admin op takes or
  returns one. Rust has none.
- **Error taxonomy.** `AdminError` ([`src/error.rs:509`](../src/error.rs:509)) collapses everything to
  `Http { status, body }`. Java distinguishes `NotFoundException`, `ConflictException`,
  `NotAuthorizedException`, `PreconditionFailedException`, `NotAllowedException`, … Callers
  currently have to string-match HTTP bodies.
- **No retry / timeout policy.** Fixed 30 s `reqwest` timeout, no retry, no `PulsarAdminBuilder`
  equivalent. TLS + auth reuse from the `Pulsar` client is already done correctly.
- **No async-std support.** `admin-api` hard-requires `reqwest` + tokio, so the `async-std-runtime`
  feature cannot use the admin client at all.

---

## 9. Pulsar 5.0 scalable topics (`topic://`)

The new topic type is PIP-460 "Scalable Topics" — a DAG of hash-range **segments** that the broker
splits and merges at runtime. Key ordering is preserved across scaling because routing is
range-based rather than `hash(key) % n`. Segment count is invisible to the application.

The client work is substantial and mostly new subsystems rather than gap-filling. PIP-460 states
plainly: *"Existing Pulsar clients are not compatible with scalable topics."* Java implements it in
a separate module (`pulsar-client-v5`, 14,317 LOC) rather than extending the v4 client.

| Component | What it does | Rust RDG | Effort |
|---|---|:--:|:--:|
| Proto regeneration | 29 messages/enums, 18 command types, `FeatureFlags` 4-9 | ❌ | S |
| `DagWatchClient` | Persistent `SCALABLE_TOPIC_LOOKUP` session; broker pushes `ScalableTopicDAG` on every split/merge; `resolved_topic_name` normalization; `create_if_missing` | ❌ | M |
| `ClientSegmentLayout` | DAG → active segment set; parent→child happens-before traversal order | ❌ | M |
| `SegmentRouter` | Raw (unmasked) murmur3_32; **high 16 bits** = segment hash ring, **low 16 bits** = PIP-486 entry bucket; legacy-topic `mod n` fallback | ❌ | S |
| `ScalableTopicProducer` | Per-segment child producers over `segment://…`; re-route in flight when the layout changes; entry-bucket batch grouping stamping `entry_hash_min`/`max`; batching disabled when E2E encryption is on | ❌ | L |
| `QueueConsumer` | Subscribe all active segments, Shared dispatch, no cross-segment ordering | ❌ | M |
| `StreamConsumer` | `SCALABLE_TOPIC_SUBSCRIBE` → controller-assigned exclusive segments; `_ASSIGNMENT_UPDATE` rebalance; consumer identity + grace-period lease | ❌ | L |
| `CheckpointConsumer` | Serializable `Checkpoint` across all assigned segments — the Flink/Spark integration point | ❌ | L |
| PIP-486 Key_Shared | Key_Shared STICKY declaring owned `bucket_ranges`; `KeySharedMeta.entryBucketDispatch` | ❌ | M |
| `ScalableTopicsWatcher` | `WATCH_SCALABLE_TOPICS` namespace-level topic-list watch | ❌ | M |
| Legacy-segment support | `SegmentInfoProto.legacy_topic_name` — lets `topic://` address a not-yet-migrated regular topic | ❌ | S |
| `ScalableTopics` admin (24 ops) | create / migrate / split / merge / auto-scale policy / segment subs / stats | ❌ | M |
| Metadata-driven transactions (PIP-473) | `WATCH_TC_ASSIGNMENTS` + TC discovery | ❌ | L |

**Important sequencing note:** PIP-466 says the V5 API "also works with existing partitioned and
non-partitioned topics" and is a *full replacement*, with the long-term intent that scalable topics
become the default. So the choice is not "add scalable topics to the existing Rust client" — it's
whether to build a second, parallel API surface (`pulsar::v5`) as Java did, or extend in place.
Given the fork already diverges from upstream `pulsar-rs`, a `v5` module mirroring Java's split is
the lower-risk path: it keeps the v4 code untouched while the 5.0 protocol is still in preview and
subject to change.

Also worth noting: **Pulsar 5.0 explicitly defers** geo-replication, transactions, and Flink/Beam
connectors for scalable topics. Rust shouldn't chase what the server hasn't shipped.

---

## 10. Ranked roadmap

Ordering is by *risk removed per unit of work*, not by size. Each phase has a stated exit test —
the phase isn't done until that test passes.

### Phase 0 — Stop the silent wrongness (~1 week)

Nothing here is large; all of it produces incorrect data today.

1. **D1** `HashingScheme` enum, `JavaStringHash` default, masked murmur, `signSafeMod`.
2. **D2** `Single` routing must hash-route keyed messages.
3. **D3** Chunk detection + hard error (reassembly lands in Phase 2).
4. `null_value` / `null_partition_key` / `partition_key_b64_encoded` proto fields + handling.
5. `metadata_auto_creation_enabled=false` on metadata lookups.
6. Advertise `FeatureFlags` 4-9 correctly and read `CommandConnected.feature_flags`.

> **Exit test:** a key-vector table (≥100 keys × {1,2,4,8,16,64} partitions) asserted against
> partition indices computed by the Java client, for both hashing schemes. Plus a round-trip test
> for null values and binary keys, Rust producer → Java consumer and vice versa.

### Phase 1 — Production must-haves (~3–4 weeks)

Everything a real deployment hits within the first week.

| Item | Why it's here |
|---|---|
| `autoUpdatePartitions` (producer + consumer) | Scaling a topic silently strands traffic |
| TLS client-certificate auth | Locks out mTLS clusters entirely |
| `listenerName` | Locks out every Kubernetes/multi-network cluster |
| `Reader::has_message_available` + `startMessageIdInclusive` | The two most-requested reader primitives |
| Ack grouping (`acknowledgmentGroupTime`, `maxAcknowledgmentGroupSize`) | Throughput |
| Batch index acknowledgment | Prevents whole-batch redelivery |
| `KeySharedPolicy` (AUTO_SPLIT / STICKY, `allowOutOfOrderDelivery`) | Key_Shared is unusable at scale without it |
| Retry-letter topic + `reconsume_later`; DLQ completeness | The standard retry pattern |
| `negativeAckRedeliveryDelay` + backoff | Nack currently redelivers on the broker's terms only |
| `sendTimeout`, `initialSequenceId`, `maxPendingMessages` | Delivery guarantees + dedup across restarts |
| `hasReachedEndOfTopic` public API | Command is already parsed; just expose it |
| `CommandTopicMigrated` handling | Cluster migration |
| `replicateSubscriptionState` | Geo-replication |
| Producer/consumer stats + `getLastSequenceId` | Operability |
| Schema-version → reader cache | Correct decode on evolved topics |

> **Exit test:** parity harness (below) green for producer/consumer/reader across partitioned and
> non-partitioned topics, Key_Shared with sticky ranges, DLQ + retry-letter flow, and a
> partition-count increase mid-run.

### Phase 2 — Chunking, encryption, batch receive (~2 weeks)

Full chunked-message produce and reassemble (`is_chunk`, uuid tracking, `maxPendingChunkedMessage`,
`expireTimeOfIncompleteChunkedMessage`, `autoAckOldestChunkedMessageOnQueueFull`); E2E
encrypt/decrypt with a `CryptoKeyReader` equivalent and `cryptoFailureAction`; `batchReceive` +
`BatchReceivePolicy`; `pause`/`resume`; `connectionsPerBroker`; client `memoryLimit`.

> **Exit test:** Java produces a 20 MB chunked encrypted message, Rust consumes it byte-identically,
> and the reverse.

### Phase 3 — Admin client, done properly (~4–6 weeks)

Do the foundation once, then the endpoints are mechanical:

1. **Foundation** — typed `AdminError` taxonomy mapped from HTTP status; retry + timeout policy;
   a request builder that handles path encoding, `authoritative`, and query params; `serde` policy
   models generated or hand-written per group; async-std support (or a runtime-agnostic HTTP layer).
2. **Tier 1 endpoints** — `Topics` (create/delete/list/stats/internal-stats/partitioned metadata/
   subscriptions/reset-cursor/peek/terminate/unload/permissions), `Namespaces` (create/delete/list/
   retention/backlog quota/TTL/dedup/persistence/auto-topic-creation/permissions), `Tenants`,
   `Clusters`, `Schemas` (write/delete/compatibility). ≈120 ops covers the overwhelming majority of
   real usage.
3. **Tier 2** — `TopicPolicies`, `Brokers`, `BrokerStats`, `Bookies`, `ResourceGroups`,
   `NonPersistentTopics`.
4. **Tier 3** — `Functions` / `Sources` / `Sinks` / `Packages` / `Transactions` / `Worker`. Large,
   rarely needed from a Rust app; defer until asked for.

> **Exit test:** each endpoint group has an integration test against a real broker asserting the
> typed model round-trips (set → get → remove → get returns default).

### Phase 4 — Schema layer (~3 weeks)

Typed primitive schemas; AVRO (via `apache-avro`); `KeyValue` INLINE/SEPARATED;
`AUTO_CONSUME`/`GenericRecord`; `Schema.Type` 21/22. Slot after Phase 3 because a working
`Schemas` admin client makes schema testing far easier.

### Phase 5 — Scalable topics, `pulsar::v5` (~8–12 weeks)

Order within the phase, each step independently shippable:

1. Proto regeneration + `FeatureFlags::supports_scalable_topics`.
2. `SegmentRouter` + `ClientSegmentLayout` (pure functions — unit-testable against Java's
   `SegmentRouterTest` vectors, no broker needed).
3. `DagWatchClient`.
4. `ScalableTopicProducer` (with legacy-segment fallback, so `topic://` works against unmigrated
   topics from day one).
5. `QueueConsumer` — simplest consumer, no controller session.
6. `ScalableTopics` admin ops (needed to drive split/merge in tests).
7. `StreamConsumer` + controller session + assignment rebalance.
8. PIP-486 entry-bucket Key_Shared.
9. `CheckpointConsumer`.
10. `ScalableTopicsWatcher`.

> **Exit test:** a Rust producer and Rust StreamConsumer maintain per-key ordering across an
> admin-triggered split *and* merge, with a Java client on the same topic as cross-check.

### Phase 6 — Long tail (opportunistic)

Client transactions + `WATCH_TC_ASSIGNMENTS`, `TableView`, `WATCH_TOPIC_LIST` pattern subscriptions
(replacing the 30 s poll), interceptors, `AutoClusterFailover`, Athenz, SASL, Socks5, `proxyServiceUrl`
+ SNI, OTel metrics, `poolMessages`, `MessagePayloadProcessor`.

---

## 11. On the code-migration-kit

Checked `anthropics/code-migration-kit-with-claude-code`. As a whole framework it's the wrong shape
for this: it's built for *total* structure-preserving language ports (translate every file, delete
the source language — the Bun Zig→Rust case). Here the Rust client already exists, is idiomatic
async Rust rather than a transliteration of Netty-based Java, and the work is selective gap-filling.
Its dependency-map / manifest / build-daemon machinery assumes a file-per-file work list we don't have.

Three parts are worth taking:

1. **The parity harness (`00b-judge-setup.md`) — take this first, before Phase 0.** A cross-language
   conformance harness that runs the Java client and the Rust client against the same broker and
   asserts identical observable behaviour is precisely what would have caught D1, D2, and D3. It is
   also the exit test for nearly every phase above. Concretely: a small Java driver + Rust driver
   behind a shared JSON scenario format (produce these keyed messages → assert partition placement,
   ordering, ack behaviour, payload bytes), runnable against a docker-compose broker.
2. **`RULEBOOK.md`.** We will translate dozens of Java features one at a time over months. A decision
   table pinning the recurring Java→Rust idiom choices — `CompletableFuture` → `async fn`, builder
   `clone()` → owned builders, checked-exception hierarchies → `Error` enum variants,
   `ScheduledExecutorService` → `Executor::interval`, `Listener` interfaces → `Stream`/channel,
   `loadConf(Map)` → dropped — keeps Phase 3's ~120 admin endpoints and Phase 5's subsystems
   internally consistent instead of drifting per session.
3. **`queue_runner.mjs` + the implementer/reviewer/fixer fan-out.** Genuinely applicable to Phase 3
   tier-1 admin endpoints and the ~200 policy models: highly repetitive, independently verifiable,
   mechanical work. Feed it a manifest of endpoint specs rather than a file dependency graph.

Skip the feasibility, bakeoff, survey-build, and burndown steps — they presuppose a big-bang
translation with a compile-everything-at-the-end phase.

---

---

## 12. Divergence from upstream `streamnative/pulsar-rs`

Upstream `master` is at `95a0d73`, which is also this fork's merge-base — the fork carries **no
commits** ahead of upstream, so everything below originates here and nothing in the shared history
does. In particular the admin REST client, `get_schema`, the reader partitioned-topic guard, and the
connection data/control-plane split are all **upstream** work.

### Behaviour changes (fix real divergence from the Java client and the broker)

| Added here | What upstream does | Why it matters |
|---|---|---|
| `HashingScheme` (`JavaStringHash` default + `Murmur3_32Hash`) | Upstream hashes `murmur3_32(key) % n` with no sign-bit mask and no scheme choice | Upstream misroutes **every** key vs a default Java producer, and 50% vs a Murmur-configured one. Silent per-key ordering loss in mixed fleets |
| Keyed messages route by hash under `Single` **and** the unconfigured default | Upstream ignores the key in both | The unconfigured default is the common case, so upstream loses key ordering for most users who set a key |
| Chunked-message guard (`ConsumerError::UnsupportedChunkedMessage`) | Upstream has no `chunk` handling at all | Upstream hands the application a chunk as if it were a whole message — silent payload truncation on any topic a chunking producer writes |
| Advertise `FeatureFlags` in `CommandConnect` | Upstream sends none | Without `supports_auth_refresh` the broker **closes the connection** on credential expiry (`ServerCnx.java:1620`) instead of issuing an auth challenge — which upstream already implements |
| Read `CommandConnected.feature_flags` into `BrokerFeatures`, exposed as `Pulsar::broker_features()` | Upstream parses and discards the whole message | No way to gate on broker capability |
| PIP-344 `lookup_partitioned_topic_number_with_options` | Upstream always allows auto-creation | "Does this topic exist?" silently creates it |
| `NonZeroUsize` partition count | Upstream takes `usize` and would `% 0` | Turns an unrepresentable state into a compile error |

### API surface

| Added here | Detail |
|---|---|
| `HashingScheme`, re-exported with `RoutingPolicy` / `CustomRoutingPolicy` | new public enum; `ProducerOptions::hashing_scheme` |
| `BrokerFeatures` + `Pulsar::broker_features()` | new public type and method |
| `Pulsar::lookup_partitioned_topic_number_with_options` | new method; the existing one is unchanged |
| `ConnectionError::NotSupported`, `ConsumerError::UnsupportedChunkedMessage` | new error variants |
| `RoutingPolicy::compute_partition_index_for_key` | **breaking**: now takes `NonZeroUsize` + `HashingScheme` |
| `ProducerOptions` | **breaking**: gained a field (breaks exhaustive struct literals) |

Version bumped 6.4.1 → **7.0.0** for the two breaking changes.

### Wire protocol

15 field-level additions to `PulsarApi.proto`, closing every non-scalable-topic gap against Pulsar
5.0: `FeatureFlags` 4–9, `CommandConnected.feature_flags`,
`CommandPartitionedTopicMetadata.metadata_auto_creation_enabled`, `CommandSend.message_id`,
`CommandConnect.proxy_version`, `MessageMetadata.{compacted_batch_indexes, schema_id, entry_hash_min,
entry_hash_max}`, `KeySharedMeta.entryBucketDispatch`,
`CommandClose{Producer,Consumer}.assignedBrokerServiceUrl{,Tls}`,
`CommandGetTopicsOfNamespace.properties`, and `Schema.Type::{AutoConsume, External}`.

### Test and CI infrastructure

| Added here | Detail |
|---|---|
| Broker endpoints via `PULSAR_BROKER_URL` / `PULSAR_ADMIN_URL` | upstream hardcodes `127.0.0.1:6650` / `:8080` in every test, so the suite cannot run beside an existing broker |
| `scripts/start_test_broker.sh` | throwaway broker on random free ports |
| `scripts/gen_java_hash_vectors.sh` + `src/routing_policy_java_vectors.rs` | 100 golden vectors generated from Pulsar's own Java source; reproducible byte-for-byte |
| CI matrix entry `5.0.0-M1` | upstream tops out at 4.1.2 |
| CI: run `apply-config-from-env.py` | upstream's `PULSAR_PREFIX_*` env vars are **silently ignored** — verified inside the container. It works only because the defaults coincide with the port mapping and Linux runners can route to container IPs |
| CI: config-applied guard step + `public/default` readiness wait | fails loudly if overrides stop applying; removes a real "Namespace not found" startup race |
| Test count | 50 → 73 (+20 doctests) |

## Migrating partition routing from 6.x

Version 7.0.0 replaces the routing hash. This is a deliberate behaviour break, and it is not safe to
roll out gradually.

Upstream 6.x computes `murmur3_32(key) % n` on the **raw, unmasked** 32-bit hash. 7.0.0 computes
Java's `JavaStringHash` by default, or `Murmur3_32Hash` (masked to 31 bits) on request. For any key
whose raw Murmur hash has bit 31 set, masking subtracts exactly `2^31`, which changes the chosen
partition for every partition count not a divisor of `2^31` — i.e. every count that is not a power of
two.

Worked example with 3 partitions and key `"abc"`:

| Client | Hash | Partition |
|---|---|---|
| 6.x | `3017643002` (raw) | `3017643002 % 3` = **2** |
| 7.0.0 `Murmur3_32Hash` | `870159354` (masked) | `870159354 % 3` = **0** |
| 7.0.0 default `JavaStringHash` | `96354` | `96354 % 3` = **0** |

During a rolling upgrade, 6.x and 7.0.0 producers publishing the same key therefore write to
different partitions, and per-key ordering is silently lost for the duration of the deployment.

**Safe upgrade paths**, in order of preference:

1. **Upgrade all producers for a topic together** — stop the 6.x producers, deploy 7.0.0, restart.
   Consumers are unaffected: routing is a producer-side decision.
2. **Drain and cut over** — stop producing, let consumers reach the end of every partition, then
   deploy.
3. **Only if neither is possible** — keep keys off the affected topics until the fleet is uniform.

There is deliberately **no bug-compatible "legacy raw Murmur" scheme**. Adding one would let the
divergence persist indefinitely and would give the wrong answer against every Java, Go, Python and
C++ client, which is the defect being fixed. If a coordinated cutover is genuinely impossible for
your deployment, that decision belongs to you rather than to a default — open an issue and it can be
added as an explicitly deprecated option.

---

## 13. Summary

| Area | Java surface | Rust RDG coverage | Worst consequence today |
|---|---|---|---|
| Wire protocol | 81 command types | ~63 | No 5.0 topics, no topic watchers, no migration handling |
| Producer | 38 builder options | ~12 | **Wrong partition vs Java for the same key** |
| Consumer | 48 builder options | ~14 | **Chunked topics silently corrupt**; no retry-letter; Key_Shared unusable at scale |
| Reader | 25 builder options | ~8 | Cannot detect end of topic |
| Schema | Full typed layer + AVRO/KeyValue/AUTO | Raw proto only | Wrong decode on evolved topics |
| Auth | 8 providers | 4 | mTLS clusters unreachable |
| Client | 60 builder options | ~10 | No listener name → unusable on k8s multi-network |
| Admin | ~550 ops, 21 interfaces | 3 ops | Effectively no admin capability |
| Scalable topics (5.0) | 13 subsystems, 24 admin ops | 0 | No `topic://` support |

**Recommended immediate action:** build the parity harness, then run Phase 0. Phase 0 is roughly a
week of work and removes three classes of silent data incorrectness — it is worth more than any
amount of new feature surface added on top of a client that routes keys differently from everything
else in the ecosystem.
