#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/bin"
RUN_DIR="${ROOT_DIR}/run"
LOG_DIR="${ROOT_DIR}/logs"
DATA_DIR="${ROOT_DIR}/data"

HOST="${HOST:-127.0.0.1}"
PORTS="${PORTS:-8001 8002 8003}"
NODES="${NODES:-A B C}"
SERVICE="${SERVICE:-new-cache}"
GROUP="${GROUP:-scores}"
ETCD_ENDPOINTS="${ETCD_ENDPOINTS:-http://127.0.0.1:2379}"
GOCACHE="${GOCACHE:-/tmp/newcache-gocache}"

mkdir -p "${BIN_DIR}" "${RUN_DIR}" "${LOG_DIR}" "${DATA_DIR}" "${GOCACHE}"

check_etcd() {
	if command -v curl >/dev/null 2>&1; then
		local first_endpoint="${ETCD_ENDPOINTS%%,*}"
		if ! curl -fsS "${first_endpoint}/health" >/dev/null; then
			echo "etcd is not healthy at ${first_endpoint}" >&2
			echo "start etcd first, or set ETCD_ENDPOINTS=http://host:2379" >&2
			exit 1
		fi
	fi
}

is_running() {
	local pid_file="$1"
	if [[ ! -f "${pid_file}" ]]; then
		return 1
	fi
	local pid
	pid="$(cat "${pid_file}")"
	[[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

start_node() {
	local node="$1"
	local port="$2"
	local pid_file="${RUN_DIR}/cache-node-${node}.pid"
	local log_file="${LOG_DIR}/cache-node-${node}.log"

	if is_running "${pid_file}"; then
		echo "node ${node} already running, pid=$(cat "${pid_file}")"
		return
	fi

	nohup "${BIN_DIR}/cache-node" \
		-host "${HOST}" \
		-port "${port}" \
		-node "${node}" \
		-service "${SERVICE}" \
		-group "${GROUP}" \
		-etcd "${ETCD_ENDPOINTS}" \
		-data-dir "${DATA_DIR}" \
		>>"${log_file}" 2>&1 &

	echo "$!" >"${pid_file}"
	echo "started node ${node}: ${HOST}:${port}, pid=$(cat "${pid_file}"), log=${log_file}"
}

check_etcd

echo "building cache-node..."
GOCACHE="${GOCACHE}" go build -o "${BIN_DIR}/cache-node" ./cmd/cache-node

read -r -a node_list <<<"${NODES}"
read -r -a port_list <<<"${PORTS}"

if [[ "${#node_list[@]}" -ne "${#port_list[@]}" ]]; then
	echo "NODES count must match PORTS count" >&2
	exit 1
fi

for i in "${!node_list[@]}"; do
	start_node "${node_list[$i]}" "${port_list[$i]}"
done

echo "all nodes started"
echo "service=${SERVICE}, group=${GROUP}, etcd=${ETCD_ENDPOINTS}"
echo "stop with: for f in ${RUN_DIR}/cache-node-*.pid; do kill \"\$(cat \"\$f\")\"; done"
