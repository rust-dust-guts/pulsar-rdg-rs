// Golden-vector generator for pulsar-rs partition-routing parity tests.
//
// The two hash bodies below are copied VERBATIM from Apache Pulsar:
//   org.apache.pulsar.common.util.Murmur3_32Hash        (makeHash0/fmix/mixK1/mixH1)
//   org.apache.pulsar.client.impl.JavaStringHash        (makeHash)
//   org.apache.pulsar.client.util.MathUtils             (signSafeMod)
// The only substitution is Guava's UnsignedBytes.toInt(b) -> (b & 0xFF), which is
// exactly Guava's implementation, so the arithmetic is unchanged.
//
// Scheme wiring mirrors org.apache.pulsar.client.impl.MessageRouterBase:
//   JavaStringHash   -> JavaStringHash.getInstance()
//   Murmur3_32Hash   -> Murmur3Hash32.getInstance()  (== Murmur3_32Hash.makeHash & MAX_VALUE)

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class GenVectors {

    // ---- org.apache.pulsar.common.util.Murmur3_32Hash ----
    private static final int CHUNK_SIZE = 4;
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int SEED = 0;

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        return h1 * 5 + 0xe6546b64;
    }

    private static int fmix(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    private static int makeHash0(byte[] bytes) {
        int len = bytes.length;
        int reminder = len % CHUNK_SIZE;
        int h1 = SEED;

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        while (byteBuffer.remaining() >= CHUNK_SIZE) {
            int k1 = byteBuffer.getInt();
            k1 = mixK1(k1);
            h1 = mixH1(h1, k1);
        }

        int k1 = 0;
        for (int i = 0; i < reminder; i++) {
            k1 ^= (byteBuffer.get() & 0xFF) << (i * 8);
        }

        h1 ^= mixK1(k1);
        h1 ^= len;
        h1 = fmix(h1);

        return h1;
    }

    /** Murmur3_32Hash.makeHash(byte[]) */
    static int murmurMakeHash(byte[] b) {
        return makeHash0(b) & Integer.MAX_VALUE;
    }

    /** Murmur3Hash32.makeHash(String) — what MessageRouterBase actually calls. */
    static int murmurScheme(String s) {
        return murmurMakeHash(s.getBytes(StandardCharsets.UTF_8)) & Integer.MAX_VALUE;
    }

    /** JavaStringHash.makeHash(String) — the Java client DEFAULT scheme. */
    static int javaStringScheme(String s) {
        return s.hashCode() & Integer.MAX_VALUE;
    }

    /** MathUtils.signSafeMod(long, int) */
    static int signSafeMod(long dividend, int divisor) {
        int mod = (int) (dividend % divisor);
        if (mod < 0) {
            mod += divisor;
        }
        return mod;
    }

    static final int[] PARTITION_COUNTS = {1, 2, 3, 4, 7, 8, 16, 64, 100};

    public static void main(String[] args) {
        String[] keys = buildKeys();

        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        for (int i = 0; i < keys.length; i++) {
            String k = keys[i];
            sb.append("  {\"key\": ").append(jsonString(k));
            sb.append(", \"java_string_hash\": ").append(javaStringScheme(k));
            sb.append(", \"murmur3_32_hash\": ").append(murmurScheme(k));
            sb.append(", \"jsh_partitions\": [");
            for (int p = 0; p < PARTITION_COUNTS.length; p++) {
                if (p > 0) {
                    sb.append(", ");
                }
                sb.append(signSafeMod(javaStringScheme(k), PARTITION_COUNTS[p]));
            }
            sb.append("], \"m3_partitions\": [");
            for (int p = 0; p < PARTITION_COUNTS.length; p++) {
                if (p > 0) {
                    sb.append(", ");
                }
                sb.append(signSafeMod(murmurScheme(k), PARTITION_COUNTS[p]));
            }
            sb.append("]}");
            if (i < keys.length - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append("]\n");
        System.out.print(sb);
    }

    static String[] buildKeys() {
        java.util.List<String> keys = new java.util.ArrayList<>();

        // Edge cases: empty, single char, all tail-length residues mod 4.
        keys.add("");
        keys.add("a");
        keys.add("ab");
        keys.add("abc");
        keys.add("abcd");
        keys.add("abcde");
        keys.add("abcdef");
        keys.add("abcdefg");
        keys.add("abcdefgh");

        // Keys whose Murmur3 raw hash has bit 31 set exercise the & MAX_VALUE mask;
        // keys whose String.hashCode() is negative exercise it for JavaStringHash.
        // The sweep below covers both classes densely.
        for (int i = 0; i < 64; i++) {
            keys.add("key-" + i);
        }
        for (int i = 0; i < 16; i++) {
            keys.add("user_" + (i * 7919) + "_session");
        }

        // Realistic shapes.
        keys.add("order-2026-07-25T12:00:00Z");
        keys.add("tenant/namespace/entity#42");
        keys.add("00000000-0000-0000-0000-000000000000");
        keys.add("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        // Non-ASCII: BMP multi-byte UTF-8.
        keys.add("ключ");
        keys.add("日本語のキー");
        keys.add("clé-café");

        // Non-BMP (surrogate pairs). String.hashCode() iterates UTF-16 code units,
        // so each of these counts as 2 chars in JavaStringHash but 4 bytes in UTF-8.
        // This is the case a naive Rust `chars()` implementation gets wrong.
        keys.add("😀");                 // U+1F600 grinning face
        keys.add("key-🚀-suffix");      // U+1F680 rocket
        keys.add("𐀀𐀀");     // U+10000 twice

        // Long key crossing many 4-byte chunks.
        StringBuilder longKey = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longKey.append("segment").append(i).append('-');
        }
        keys.add(longKey.toString());

        return keys.toArray(new String[0]);
    }

    static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    // Escape everything non-ASCII as \\uXXXX so the emitted JSON is
                    // pure ASCII and surrogate pairs survive round-tripping exactly.
                    if (c < 0x20 || c > 0x7E) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
