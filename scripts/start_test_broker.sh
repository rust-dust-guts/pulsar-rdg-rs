#!/usr/bin/env bash
#
# Starts a throwaway Pulsar standalone broker for the integration tests and
# prints the environment variables the test suite reads.
#
# Ports are picked at random by default so this never collides with a broker you
# already have running. The broker is configured to *listen* on those ports
# inside the container and to advertise 127.0.0.1, because the client follows the
# address returned by topic lookup — a plain `-p host:6650` mapping makes the
# broker advertise its container hostname, which the host cannot resolve.
#
# Usage — capture first, then eval. `eval "$(...)"` reports *eval's* status, so a
# failed start that printed nothing would look like success and the tests would
# silently run against the default endpoints or a stale broker:
#
#   broker_env=$(scripts/start_test_broker.sh) && eval "$broker_env"
#   cargo test --features admin-api
#   docker rm -f pulsar-rs-test
#
# Environment:
#   PULSAR_IMAGE_TAG    image tag to run          (default: 5.0.0-M1)
#   PULSAR_BROKER_PORT  fixed broker port         (default: random free port)
#   PULSAR_ADMIN_PORT   fixed admin port          (default: random free port)
#   CONTAINER_NAME      docker container name     (default: pulsar-rs-test)

set -euo pipefail

IMAGE_TAG="${PULSAR_IMAGE_TAG:-5.0.0-M1}"
CONTAINER_NAME="${CONTAINER_NAME:-pulsar-rs-test}"

free_port() {
  python3 - <<'PY'
import random, socket
while True:
    p = random.randint(20000, 60000)
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", p))
        except OSError:
            continue
    print(p)
    break
PY
}

BROKER_PORT="${PULSAR_BROKER_PORT:-$(free_port)}"
ADMIN_PORT="${PULSAR_ADMIN_PORT:-$(free_port)}"
while [ "$ADMIN_PORT" = "$BROKER_PORT" ]; do
  ADMIN_PORT="$(free_port)"
done

echo "# starting apachepulsar/pulsar:${IMAGE_TAG} as ${CONTAINER_NAME}" >&2
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

docker run -d --name "${CONTAINER_NAME}" \
  -p "${BROKER_PORT}:${BROKER_PORT}" -p "${ADMIN_PORT}:${ADMIN_PORT}" \
  -e PULSAR_PREFIX_brokerServicePort="${BROKER_PORT}" \
  -e PULSAR_PREFIX_webServicePort="${ADMIN_PORT}" \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  -e PULSAR_PREFIX_topicLevelPoliciesEnabled=true \
  -e PULSAR_PREFIX_systemTopicEnabled=true \
  -e PULSAR_PREFIX_allowAutoTopicCreation=true \
  -e PULSAR_PREFIX_brokerDeduplicationEnabled=true \
  -e PULSAR_PREFIX_transactionCoordinatorEnabled=true \
  "apachepulsar/pulsar:${IMAGE_TAG}" \
  sh -c 'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker' \
  >/dev/null

# The health endpoint can answer before public/default exists, which every test
# needs, so wait on the namespace rather than on health.
echo "# waiting for public/default namespace on port ${ADMIN_PORT}" >&2
for _ in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:${ADMIN_PORT}/admin/v2/namespaces/public" 2>/dev/null \
      | grep -q 'public/default'; then
    version=$(curl -s "http://127.0.0.1:${ADMIN_PORT}/admin/v2/brokers/version" || echo unknown)
    echo "# broker ${version} ready" >&2
    echo "export PULSAR_BROKER_URL=pulsar://127.0.0.1:${BROKER_PORT}"
    echo "export PULSAR_ADMIN_URL=http://127.0.0.1:${ADMIN_PORT}"
    exit 0
  fi
  sleep 1
done

echo "# broker did not become ready; recent logs:" >&2
docker logs --tail=100 "${CONTAINER_NAME}" >&2 || true
exit 1
