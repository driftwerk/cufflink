#!/usr/bin/env bash
set -Eeuo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
network="${OBSERVABILITY_NETWORK:-cufflink-observability}"
otlp_port="${OTLP_PORT:-4318}"
loki_port="${LOKI_PORT:-3100}"
tempo_port="${TEMPO_PORT:-3200}"
prometheus_port="${PROMETHEUS_PORT:-9090}"
pinned_otlp_port="${PINNED_OTLP_PORT:-4319}"
pinned_tenant="--env=CUFFLINK_TENANT=${PINNED_TENANT:-pinned}"
services=(loki tempo prometheus otel-collector otel-collector-tenant)

declared() {
  jq -r --arg service "$1" ".services[\$service] | $2" <<<"$compose"
}

read_compose() {
  compose=$(docker compose --profile '*' -f "$root/docker-compose.yml" config --format json)
}

image() {
  declared "$1" '.image // error("no image")'
}

mounts() {
  declared "$1" '.volumes[] | select(.type == "bind") | "--volume=\(.source):\(.target):ro"'
}

with_lines() {
  local lines=() line
  while IFS= read -r line; do
    lines+=("$line")
  done
  "$@" ${lines[@]+"${lines[@]}"}
}

check() {
  local service="$1"
  shift
  {
    mounts "$service"
    printf '%s\n' "$@"
  } | with_lines docker run --rm
}

validate() {
  read_compose
  check otel-collector "$(image otel-collector)" validate --config=/etc/otelcol-contrib/config.yaml
  check otel-collector-tenant "$pinned_tenant" "$(image otel-collector-tenant)" validate --config=/etc/otelcol-contrib/config.yaml
  check loki "$(image loki)" -config.file=/etc/loki/local-config.yaml -verify-config
  check tempo "$(image tempo)" -config.file=/etc/tempo.yaml -config.verify=true
  check prometheus --entrypoint promtool "$(image prometheus)" check config /etc/prometheus/prometheus.yml
}

start() {
  local service="$1" published="$2"
  shift 2
  {
    mounts "$service"
    printf '%s\n' "--publish=127.0.0.1:$published" "$@" "$(image "$service")"
    declared "$service" '.command // [] | .[]'
  } | with_lines docker run --detach --name "$network-$service" \
    --network "$network" --network-alias "$service" >/dev/null
}

answers() {
  local what="$1"
  shift
  for _ in $(seq 1 120); do
    if curl --silent --fail --output /dev/null "$@"; then
      return 0
    fi
    sleep 1
  done
  echo "$what did not answer" >&2
  docker logs --tail 50 "$network-$what" >&2 || true
  return 1
}

takes_otlp() {
  answers "$1" --header 'Content-Type: application/json' --data '{}' \
    "http://127.0.0.1:$2/v1/traces"
}

up() {
  read_compose
  docker network create "$network" >/dev/null
  trap down ERR
  start loki "$loki_port:3100"
  start tempo "$tempo_port:3200"
  start prometheus "$prometheus_port:9090"
  start otel-collector "$otlp_port:4318"
  start otel-collector-tenant "$pinned_otlp_port:4318" "$pinned_tenant"
  answers loki "http://127.0.0.1:$loki_port/ready"
  answers tempo "http://127.0.0.1:$tempo_port/ready"
  answers prometheus "http://127.0.0.1:$prometheus_port/-/ready"
  takes_otlp otel-collector "$otlp_port"
  takes_otlp otel-collector-tenant "$pinned_otlp_port"
}

down() {
  for service in "${services[@]}"; do
    docker rm --force --volumes "$network-$service" >/dev/null 2>&1 || true
  done
  docker network rm "$network" >/dev/null 2>&1 || true
}

case "${1:-}" in
  validate) validate ;;
  up) up ;;
  down) down ;;
  *)
    echo "usage: $0 validate|up|down" >&2
    exit 2
    ;;
esac
