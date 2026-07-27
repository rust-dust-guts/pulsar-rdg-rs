#!/usr/bin/env bash
#
# Starts a throwaway Pulsar standalone broker — and a Pulsar proxy in front of it —
# for the integration tests, and prints the environment variables the suite reads.
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
#   docker rm -f pulsar-rs-test pulsar-rs-test-proxy
#
# Environment:
#   PULSAR_IMAGE_TAG    image tag to run          (default: 5.0.0-M1)
#   PULSAR_BROKER_PORT  fixed broker port         (default: random free port)
#   PULSAR_ADMIN_PORT   fixed admin port          (default: random free port)
#   PULSAR_PROXY_PORT   fixed proxy service port  (default: random free port)
#   PULSAR_PROXY_WEB_PORT fixed proxy web port     (default: random free port)
#   CONTAINER_NAME      docker container name     (default: pulsar-rs-test)
#   SKIP_PROXY          set to 1 to skip the proxy (proxy-stats tests then skip)

set -euo pipefail

IMAGE_TAG="${PULSAR_IMAGE_TAG:-5.0.0-M1}"
CONTAINER_NAME="${CONTAINER_NAME:-pulsar-rs-test}"
PROXY_NAME="${CONTAINER_NAME}-proxy"

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

distinct_port() {
  local port
  while :; do
    port="$(free_port)"
    local taken=0 used
    for used in "$@"; do
      [ "$port" = "$used" ] && taken=1
    done
    [ "$taken" = 0 ] && { echo "$port"; return; }
  done
}

BROKER_PORT="${PULSAR_BROKER_PORT:-$(free_port)}"
ADMIN_PORT="${PULSAR_ADMIN_PORT:-$(distinct_port "$BROKER_PORT")}"
PROXY_PORT="${PULSAR_PROXY_PORT:-$(distinct_port "$BROKER_PORT" "$ADMIN_PORT")}"
PROXY_WEB_PORT="${PULSAR_PROXY_WEB_PORT:-$(distinct_port "$BROKER_PORT" "$ADMIN_PORT" "$PROXY_PORT")}"

echo "# starting apachepulsar/pulsar:${IMAGE_TAG} as ${CONTAINER_NAME}" >&2
docker rm -f "${CONTAINER_NAME}" "${PROXY_NAME}" >/dev/null 2>&1 || true

# The proxy's ports are published here, not on the proxy container: the proxy
# joins this container's network namespace (see below) and only the container that
# owns a namespace can publish ports into it.
#
# `advertisedListeners` names one extra listener, "external", pointing at the same
# address the broker already advertises. It exists for the listener-name tests and
# is inert for everything else: a lookup that names no listener keeps getting the
# default URLs, and `internalListenerName` still resolves to the "internal" entry
# synthesized from brokerServicePort/webServicePort.
docker run -d --name "${CONTAINER_NAME}" \
  -p "${BROKER_PORT}:${BROKER_PORT}" -p "${ADMIN_PORT}:${ADMIN_PORT}" \
  -p "${PROXY_PORT}:${PROXY_PORT}" -p "${PROXY_WEB_PORT}:${PROXY_WEB_PORT}" \
  -e PULSAR_PREFIX_brokerServicePort="${BROKER_PORT}" \
  -e PULSAR_PREFIX_webServicePort="${ADMIN_PORT}" \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  -e PULSAR_PREFIX_advertisedListeners="external:pulsar://127.0.0.1:${BROKER_PORT}" \
  -e PULSAR_PREFIX_topicLevelPoliciesEnabled=true \
  -e PULSAR_PREFIX_systemTopicEnabled=true \
  -e PULSAR_PREFIX_allowAutoTopicCreation=true \
  -e PULSAR_PREFIX_brokerDeduplicationEnabled=true \
  -e PULSAR_PREFIX_transactionCoordinatorEnabled=true \
  -e PULSAR_PREFIX_forceDeleteNamespaceAllowed=true \
  -e PULSAR_PREFIX_forceDeleteTenantAllowed=true \
  -e PULSAR_PREFIX_enablePackagesManagement=true \
  "apachepulsar/pulsar:${IMAGE_TAG}" \
  sh -c 'bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone' \
  >/dev/null

# The health endpoint can answer before public/default exists, which every test
# needs, so wait on the namespace rather than on health.
echo "# waiting for public/default namespace on port ${ADMIN_PORT}" >&2
for _ in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:${ADMIN_PORT}/admin/v2/namespaces/public" 2>/dev/null \
      | grep -q 'public/default'; then
    version=$(curl -s "http://127.0.0.1:${ADMIN_PORT}/admin/v2/brokers/version" || echo unknown)
    echo "# broker ${version} ready" >&2
    BROKER_READY=1
    break
  fi
  sleep 1
done

if [ "${BROKER_READY:-0}" != "1" ]; then
  echo "# broker did not become ready; recent logs:" >&2
  docker logs --tail=100 "${CONTAINER_NAME}" >&2 || true
  exit 1
fi

echo "export PULSAR_BROKER_URL=pulsar://127.0.0.1:${BROKER_PORT}"
echo "export PULSAR_ADMIN_URL=http://127.0.0.1:${ADMIN_PORT}"

# ---------------------------------------------------------------- proxy
#
# `proxy-stats` is served by a Pulsar proxy, not a broker, so exercising it needs
# one in the topology. The proxy runs in "direct" mode — pointed straight at the
# broker rather than discovering it through the metadata store — which keeps it
# independent of how the broker stores metadata.
#
# It shares the broker's network namespace, so both see the same loopback. That is
# what makes advertisedAddress=127.0.0.1 work for the proxy as well as for the host:
# a client connecting through the proxy still looks the topic up first and hands the
# proxy the *advertised* address to dial. On a separate network 127.0.0.1 would be
# the proxy itself, and every connection would fail with "Connection refused".
#
# Two settings are easy to miss:
#   * brokerProxyAllowedTargetPorts defaults to "6650,6651", so a randomised broker
#     port is refused with "Given port ... isn't allowed" and no client can connect.
#   * proxyLogLevel must be 2 at *startup* for /proxy-stats/topics; the runtime
#     setter only changes it in memory and does not unlock that endpoint.
#
# SKIP_PROXY=1 is the only way to opt out. A proxy that was asked for but failed to
# start is a hard error: exiting 0 with only the broker exports would let the
# documented command carry on and quietly skip the proxy tests — hiding exactly the
# topology and configuration regressions they exist to catch. The unset lines matter
# too, or a stale PULSAR_PROXY_URL left in the caller's shell would point the tests
# at a proxy that is gone.
if [ "${SKIP_PROXY:-0}" = "1" ]; then
  echo "# SKIP_PROXY set; proxy-stats tests will skip" >&2
  echo "unset PULSAR_PROXY_URL"
  echo "unset PULSAR_PROXY_ADMIN_URL"
  exit 0
fi

echo "# starting proxy as ${PROXY_NAME} (web ${PROXY_WEB_PORT})" >&2
docker run -d --name "${PROXY_NAME}" --network "container:${CONTAINER_NAME}" \
  -e PULSAR_PREFIX_servicePort="${PROXY_PORT}" \
  -e PULSAR_PREFIX_webServicePort="${PROXY_WEB_PORT}" \
  -e PULSAR_PREFIX_brokerServiceURL="pulsar://127.0.0.1:${BROKER_PORT}" \
  -e PULSAR_PREFIX_brokerWebServiceURL="http://127.0.0.1:${ADMIN_PORT}" \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  -e PULSAR_PREFIX_clusterName=standalone \
  -e PULSAR_PREFIX_proxyLogLevel=2 \
  -e PULSAR_PREFIX_brokerProxyAllowedTargetPorts="${BROKER_PORT}" \
  "apachepulsar/pulsar:${IMAGE_TAG}" \
  sh -c 'bin/apply-config-from-env.py conf/proxy.conf && exec bin/pulsar proxy' \
  >/dev/null

for _ in $(seq 1 90); do
  if curl -sf "http://127.0.0.1:${PROXY_WEB_PORT}/proxy-stats/connections" >/dev/null 2>&1; then
    echo "# proxy ready" >&2
    echo "export PULSAR_PROXY_URL=pulsar://127.0.0.1:${PROXY_PORT}"
    echo "export PULSAR_PROXY_ADMIN_URL=http://127.0.0.1:${PROXY_WEB_PORT}"
    exit 0
  fi
  if [ "$(docker inspect -f '{{.State.Running}}' "${PROXY_NAME}" 2>/dev/null)" != "true" ]; then
    break
  fi
  sleep 1
done

echo "# proxy did not become ready. Recent logs:" >&2
docker logs --tail=40 "${PROXY_NAME}" >&2 || true
echo "# (set SKIP_PROXY=1 to run the broker alone and skip the proxy-stats tests)" >&2
exit 1
