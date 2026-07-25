use std::{num::NonZeroUsize, sync::Arc};

use murmur3::murmur3_32;

use crate::producer::Message;

#[cfg(test)]
#[path = "routing_policy_java_vectors.rs"]
pub(crate) mod java_vectors;

/// Hash function used to map a message's partition key to a partition index.
///
/// Mirrors `org.apache.pulsar.client.api.HashingScheme`. The default is
/// [`HashingScheme::JavaStringHash`], matching the Java client, so that a Rust
/// producer and a Java producer publishing the same key to the same partitioned
/// topic select the same partition. Choosing a different scheme than the other
/// producers on a topic breaks per-key ordering.
// Variant names mirror the Java enum exactly; the underscore is deliberate so the
// mapping to `HashingScheme.Murmur3_32Hash` is unambiguous.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HashingScheme {
    /// Java's `String.hashCode()`, masked to 31 bits. The Java client default.
    #[default]
    JavaStringHash,
    /// MurmurHash3 x86 32-bit with seed 0, masked to 31 bits. This is also the
    /// hash the broker uses for `Key_Shared` dispatch.
    Murmur3_32Hash,
}

impl HashingScheme {
    /// Hashes a partition key to a non-negative 31-bit value.
    ///
    /// Equivalent to `Hash.makeHash(String)` in the Java client.
    ///
    /// `key` must be the partition key exactly as it appears in
    /// `MessageMetadata.partition_key`. For binary keys Java stores the
    /// *base64-encoded* text there and sets `partition_key_b64_encoded`, then
    /// hashes that text — so when binary keys are added here they must be
    /// encoded before hashing, not hashed as raw bytes.
    pub fn make_hash(self, key: &str) -> u32 {
        match self {
            HashingScheme::JavaStringHash => java_string_hash(key),
            HashingScheme::Murmur3_32Hash => murmur3_32_hash(key.as_bytes()),
        }
    }
}

/// Java's `String.hashCode()`, masked to 31 bits.
///
/// Iterates UTF-16 code units rather than Unicode scalar values: Java stores
/// non-BMP characters as surrogate pairs and folds each half in separately, so
/// hashing `char`s would diverge for keys containing emoji or other
/// astral-plane characters.
fn java_string_hash(key: &str) -> u32 {
    let mut h: i32 = 0;
    for unit in key.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(i32::from(unit));
    }
    (h & i32::MAX) as u32
}

/// MurmurHash3 x86 32-bit with seed 0, masked to 31 bits.
///
/// Equivalent to `org.apache.pulsar.common.util.Murmur3_32Hash.makeHash`, which
/// clears the sign bit — without that mask half of all keys route to a different
/// partition than the Java client picks. The broker uses the same function for
/// `Key_Shared` hash ranges, so this must stay bit-exact; the golden vectors in
/// `routing_policy_java_vectors.rs` pin it.
fn murmur3_32_hash(mut bytes: &[u8]) -> u32 {
    // Reading from a byte slice is infallible, so this cannot error.
    let hash = murmur3_32(&mut bytes, 0).expect("hashing a byte slice cannot fail");
    hash & (i32::MAX as u32)
}

/// Java's `MathUtils.signSafeMod`.
///
/// The hash is already non-negative, so this is a plain modulo; it exists to
/// mirror the Java call chain and to keep the intent explicit.
///
/// `partition_count` is a `NonZeroUsize` because a zero-partition topic has no
/// partition to route to, and `% 0` would panic well away from the mistake.
fn sign_safe_mod(hash: u32, partition_count: NonZeroUsize) -> usize {
    // usize -> u64 is lossless on every supported target, so a partition count
    // above u32::MAX cannot silently wrap into a bogus index.
    (u64::from(hash) % partition_count.get() as u64) as usize
}

#[derive(Clone, Default)]
pub enum RoutingPolicy {
    #[default]
    RoundRobin,
    Single,
    Custom(Arc<dyn CustomRoutingPolicy>),
}

impl RoutingPolicy {
    /// Maps a partition key to a partition index using `hashing_scheme`.
    ///
    /// Equivalent to `signSafeMod(hash.makeHash(key), numPartitions)` in the
    /// Java message routers. The returned index is always less than
    /// `partition_count`.
    pub fn compute_partition_index_for_key(
        key: &str,
        partition_count: NonZeroUsize,
        hashing_scheme: HashingScheme,
    ) -> usize {
        sign_safe_mod(hashing_scheme.make_hash(key), partition_count)
    }
}

pub trait CustomRoutingPolicy: Send + Sync {
    fn route(&self, message: &Message, num_producers: usize) -> usize;
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use uuid::Uuid;

    use super::{
        java_vectors::{HashVector, PARTITION_COUNTS, VECTORS},
        HashingScheme, RoutingPolicy,
    };

    /// Shorthand for the tests, which only ever use literal non-zero counts.
    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test partition counts are non-zero")
    }

    /// `HashingScheme::JavaStringHash` must reproduce `String.hashCode() & MAX_VALUE`
    /// for every golden key, including the non-BMP ones.
    #[test]
    fn java_string_hash_matches_java() {
        for HashVector {
            key,
            java_string_hash,
            ..
        } in VECTORS
        {
            assert_eq!(
                HashingScheme::JavaStringHash.make_hash(key),
                *java_string_hash,
                "JavaStringHash mismatch for key {key:?}"
            );
        }
    }

    /// `HashingScheme::Murmur3_32Hash` must reproduce `Murmur3Hash32.makeHash`.
    #[test]
    fn murmur3_32_hash_matches_java() {
        for HashVector {
            key,
            murmur3_32_hash,
            ..
        } in VECTORS
        {
            assert_eq!(
                HashingScheme::Murmur3_32Hash.make_hash(key),
                *murmur3_32_hash,
                "Murmur3_32Hash mismatch for key {key:?}"
            );
        }
    }

    /// End-to-end: the partition a key routes to must match the Java client for
    /// every scheme and every partition count.
    #[test]
    fn partition_index_matches_java() {
        for vector in VECTORS {
            for (i, &partition_count) in PARTITION_COUNTS.iter().enumerate() {
                let count = nz(partition_count as usize);

                assert_eq!(
                    RoutingPolicy::compute_partition_index_for_key(
                        vector.key,
                        count,
                        HashingScheme::JavaStringHash,
                    ),
                    vector.jsh_partitions[i] as usize,
                    "JavaStringHash partition mismatch for key {:?} with {count} partitions",
                    vector.key,
                );

                assert_eq!(
                    RoutingPolicy::compute_partition_index_for_key(
                        vector.key,
                        count,
                        HashingScheme::Murmur3_32Hash,
                    ),
                    vector.m3_partitions[i] as usize,
                    "Murmur3_32Hash partition mismatch for key {:?} with {count} partitions",
                    vector.key,
                );
            }
        }
    }

    /// Regression test for the sign-bit mask.
    ///
    /// The `murmur3` crate's algorithm matches Pulsar's exactly, but its raw
    /// output is a full 32-bit value while Java's `makeHash` clears bit 31. The
    /// original implementation here took `raw % partition_count`, which diverges
    /// from Java for every key whose hash has the high bit set — about half of
    /// them. This asserts the masking still happens, and that the bug it fixes
    /// was real and widespread rather than a rounding curiosity.
    #[test]
    fn murmur3_sign_bit_is_masked() {
        let mut unmasked_would_differ = 0;

        for v in VECTORS {
            let raw = murmur3::murmur3_32(&mut v.key.as_bytes(), 0).unwrap();
            assert_eq!(
                raw & (i32::MAX as u32),
                v.murmur3_32_hash,
                "masking the crate's raw hash must reproduce Java for key {:?}",
                v.key
            );
            // Anything with bit 31 set would have routed elsewhere before the
            // mask was applied.
            if raw != v.murmur3_32_hash {
                unmasked_would_differ += 1;
            }
        }

        assert!(
            unmasked_would_differ > VECTORS.len() / 4,
            "expected the missing mask to have affected a large fraction of keys, \
             got {unmasked_would_differ}/{}",
            VECTORS.len()
        );
    }

    /// The two schemes must actually differ, otherwise the vectors above would
    /// pass even if `make_hash` ignored its scheme.
    #[test]
    fn schemes_are_distinct() {
        let differing = VECTORS
            .iter()
            .filter(|v| v.java_string_hash != v.murmur3_32_hash)
            .count();
        assert!(
            differing > VECTORS.len() / 2,
            "expected the two schemes to disagree on most keys, got {differing}/{}",
            VECTORS.len()
        );
    }

    /// The Java client's default is `JavaStringHash`; ours must match or every
    /// unconfigured Rust producer silently disagrees with every Java producer.
    #[test]
    fn default_scheme_matches_java_client_default() {
        assert_eq!(HashingScheme::default(), HashingScheme::JavaStringHash);
    }

    /// Hashes are masked to 31 bits, so an index is always in range.
    #[test]
    fn partition_index_always_in_range() {
        for partition_count in [1usize, 2, 3, 5, 17, 128] {
            for scheme in [HashingScheme::JavaStringHash, HashingScheme::Murmur3_32Hash] {
                for _ in 0..200 {
                    let key = Uuid::new_v4().to_string();
                    let index = RoutingPolicy::compute_partition_index_for_key(
                        &key,
                        nz(partition_count),
                        scheme,
                    );
                    assert!(
                        index < partition_count,
                        "{scheme:?} produced index {index} for {partition_count} partitions"
                    );
                }
            }
        }
    }

    #[test]
    fn test_compute_partition_index_consistency() {
        let partition_count = 4;
        let key = Uuid::new_v4().to_string();

        let partition_index = RoutingPolicy::compute_partition_index_for_key(
            &key,
            nz(partition_count),
            HashingScheme::default(),
        );
        assert!(partition_index < partition_count);

        for _ in 0..10 {
            let other_partition_index = RoutingPolicy::compute_partition_index_for_key(
                &key,
                nz(partition_count),
                HashingScheme::default(),
            );
            assert!(other_partition_index < partition_count);
            assert_eq!(
                partition_index, other_partition_index,
                "partition index should be deterministic for the same key"
            );
        }
    }

    #[test]
    fn test_compute_partition_index_distribution() {
        let partition_count = 4;

        for scheme in [HashingScheme::JavaStringHash, HashingScheme::Murmur3_32Hash] {
            let mut partition_counts = vec![0; partition_count];

            let total = 1000;
            for _ in 0..total {
                let partition_index = RoutingPolicy::compute_partition_index_for_key(
                    &Uuid::new_v4().to_string(),
                    nz(partition_count),
                    scheme,
                );
                partition_counts[partition_index] += 1;
            }

            for count in partition_counts {
                let ratio = count as f64 / total as f64;
                let expected_ratio = 1.0 / partition_count as f64;

                assert!(
                    ratio > (expected_ratio - 0.1) && ratio < (expected_ratio + 0.1),
                    "{scheme:?}: distribution ratio {ratio} is not near expected ratio {expected_ratio}",
                );
            }
        }
    }
}
