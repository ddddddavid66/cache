#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-http://127.0.0.1:2379}"
GOCACHE="${GOCACHE:-/tmp/newcache-gocache}"
RUN_ALL="${RUN_ALL:-1}"

mkdir -p "${GOCACHE}"

first_endpoint="${ETCD_ENDPOINTS%%,*}"

check_etcd() {
	if ! command -v curl >/dev/null 2>&1; then
		echo "curl is required to check etcd health" >&2
		return 1
	fi
	if ! curl -fsS "${first_endpoint}/health" >/dev/null; then
		echo "etcd is not healthy at ${first_endpoint}" >&2
		echo "start etcd first, or set ETCD_ENDPOINTS=http://host:2379" >&2
		return 1
	fi
}

cd "${ROOT_DIR}"

if [[ "${RUN_ALL}" == "1" ]]; then
	echo "running full package tests..."
	GOCACHE="${GOCACHE}" go test ./...
fi

echo "checking etcd at ${first_endpoint}..."
check_etcd

echo "running real etcd three-node integration test..."
ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
GOCACHE="${GOCACHE}" \
go test -count=1 -v ./internal/integration
