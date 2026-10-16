#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p results/demo
pids=""
cleanup() {
  trap - EXIT INT TERM
  for pid in $pids; do kill "$pid" 2>/dev/null || true; done
  for pid in $pids; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM
export SIM_ADMIN_TOKEN="${SIM_ADMIN_TOKEN:-local-demo}"
./bin/simulator -listen 127.0.0.1:9001 -capacity 512 -token-delay 2ms >results/demo/backend-a.log 2>&1 &
pids="$pids $!"
./bin/simulator -listen 127.0.0.1:9002 -capacity 1024 -token-delay 3ms >results/demo/backend-b.log 2>&1 &
pids="$pids $!"
./bin/simulator -listen 127.0.0.1:9003 -capacity 2048 -token-delay 4ms >results/demo/backend-c.log 2>&1 &
pids="$pids $!"
./bin/gateway -config configs/local.json >results/demo/gateway.log 2>&1 &
pids="$pids $!"
echo 'Gateway: http://127.0.0.1:8080 | readiness: /readyz | metrics: /metrics'
echo 'Logs: results/demo/ | Stop with Ctrl-C'
wait
