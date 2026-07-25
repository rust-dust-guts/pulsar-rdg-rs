#!/usr/bin/env bash
#
# Regenerates src/routing_policy_java_vectors.rs — the golden partition-routing
# vectors that pin `HashingScheme` to byte-for-byte parity with the Java client.
#
# The generator (GenJavaHashVectors.java) carries verbatim copies of Pulsar's own
# hash implementations, so it needs only a JDK, not a Pulsar checkout. If Pulsar
# ever changes those algorithms, re-copy the bodies from:
#   pulsar-common/src/main/java/org/apache/pulsar/common/util/Murmur3_32Hash.java
#   pulsar-client/src/main/java/org/apache/pulsar/client/impl/JavaStringHash.java
#   pulsar-client/src/main/java/org/apache/pulsar/client/impl/Murmur3Hash32.java
#   pulsar-client/src/main/java/org/apache/pulsar/client/util/MathUtils.java
#
# Usage: scripts/gen_java_hash_vectors.sh
#
# Requires: java 17+ (single-file source execution), python3.

set -euo pipefail

cd "$(dirname "$0")/.."

OUT_RS="src/routing_policy_java_vectors.rs"
OUT_JSON="scripts/java_hash_vectors.json"

command -v java >/dev/null || { echo "java not found on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found on PATH" >&2; exit 1; }

echo "generating vectors with $(java -version 2>&1 | head -1)"
java scripts/GenJavaHashVectors.java > "${OUT_JSON}"

python3 - "${OUT_JSON}" > "${OUT_RS}" <<'PY'
import json
import sys

with open(sys.argv[1]) as fh:
    vectors = json.load(fh)


def rust_str(value):
    out = []
    for ch in value:
        code = ord(ch)
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif 0x20 <= code <= 0x7E:
            out.append(ch)
        else:
            # Emit non-ASCII as an escape so the file stays pure ASCII and
            # astral-plane keys survive review diffs unambiguously.
            out.append("\\u{%x}" % code)
    return '"' + "".join(out) + '"'


print("""// @generated — DO NOT EDIT BY HAND.
//
// Golden partition-routing vectors produced by Apache Pulsar's own Java
// implementation, used to pin `HashingScheme` to byte-for-byte Java parity.
//
// Sources (copied verbatim into the generator):
//   org.apache.pulsar.common.util.Murmur3_32Hash   — makeHash0/fmix/mixK1/mixH1
//   org.apache.pulsar.client.impl.Murmur3Hash32    — makeHash(String)
//   org.apache.pulsar.client.impl.JavaStringHash   — makeHash(String)
//   org.apache.pulsar.client.util.MathUtils        — signSafeMod
//
// Regenerate with scripts/gen_java_hash_vectors.sh (requires a JDK).

/// Partition counts each vector's expected indices are computed for.
pub(crate) const PARTITION_COUNTS: [u32; 9] = [1, 2, 3, 4, 7, 8, 16, 64, 100];

pub(crate) struct HashVector {
    /// The partition key.
    pub key: &'static str,
    /// `JavaStringHash.makeHash(key)`
    pub java_string_hash: u32,
    /// `Murmur3Hash32.makeHash(key)`
    pub murmur3_32_hash: u32,
    /// `signSafeMod(java_string_hash, n)` for each n in [`PARTITION_COUNTS`].
    pub jsh_partitions: [u32; 9],
    /// `signSafeMod(murmur3_32_hash, n)` for each n in [`PARTITION_COUNTS`].
    pub m3_partitions: [u32; 9],
}
""")
print("pub(crate) const VECTORS: &[HashVector] = &[")
for vector in vectors:
    print("    HashVector {")
    print("        key: %s," % rust_str(vector["key"]))
    print("        java_string_hash: %d," % vector["java_string_hash"])
    print("        murmur3_32_hash: %d," % vector["murmur3_32_hash"])
    print("        jsh_partitions: [%s]," % ", ".join(str(x) for x in vector["jsh_partitions"]))
    print("        m3_partitions: [%s]," % ", ".join(str(x) for x in vector["m3_partitions"]))
    print("    },")
print("];")
PY

cargo fmt -- "${OUT_RS}" 2>/dev/null || true
count=$(grep -c '^    HashVector {' "${OUT_RS}")
echo "wrote ${OUT_RS} (${count} vectors)"
