#!/usr/bin/env bash
# 三个限 CPU/内存的容器，中间用二层延迟转发注入同机房 / 跨可用区的延迟和抖动。
# 这台内核没有 sch_netem，tc netem 的失败输出记在 netem-probe.txt。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
OUT="${OUT:-$ROOT/bench-data/realistic}"
mkdir -p "$OUT"
DOCKER="${DOCKER:-sudo docker}"
IMAGE=keelstone-bench:local

CPUS="${CPUS:-0.5}"
MEM="${MEM:-256m}"
TICK=20ms
ELECTION=20
CLIENTS="${CLIENTS:-16}"
KEYS="${KEYS:-4096}"
DURATION="${DURATION:-12s}"
WARMUP="${WARMUP:-3s}"

PEERS="n1=http://10.10.0.11:43121,n2=http://10.10.0.12:43121,n3=http://10.10.0.13:43121"
ADDRS="http://10.10.0.11:43121,http://10.10.0.12:43121,http://10.10.0.13:43121"

build_bins() {
  mkdir -p "$ROOT/bin"
  if [[ ! -x /tmp/keelstone-before ]]; then
    echo "缺少 /tmp/keelstone-before（流水线改动之前的二进制）" >&2
    exit 1
  fi
  cp /tmp/keelstone-before "$ROOT/bin/keelstone-before"
  CGO_ENABLED=0 go build -o "$ROOT/bin/keelstone" "$ROOT/cmd/keelstone"
  CGO_ENABLED=0 go build -o "$ROOT/bin/bench" "$ROOT/cmd/bench"
  CGO_ENABLED=0 go build -o "$ROOT/bin/l2delay" "$ROOT/cmd/l2delay"
}

build_image() {
  mkdir -p "$OUT/image"
  cp "$ROOT/bin/keelstone" "$ROOT/bin/keelstone-before" "$ROOT/bin/bench" "$OUT/image/"
  cp "$ROOT/deploy/bench/Dockerfile" "$OUT/image/Dockerfile"
  $DOCKER build -t "$IMAGE" "$OUT/image"
}

stop_switch() {
  if [[ -f "$OUT/l2delay.pid" ]]; then
    kill "$(cat "$OUT/l2delay.pid")" >/dev/null 2>&1 || true
    wait "$(cat "$OUT/l2delay.pid")" >/dev/null 2>&1 || true
    rm -f "$OUT/l2delay.pid"
  fi
  sudo pkill -f 'bin/l2delay' >/dev/null 2>&1 || true
}

wipe() {
  stop_switch
  $DOCKER rm -f keelstone-n1 keelstone-n2 keelstone-n3 keelstone-client >/dev/null 2>&1 || true
  $DOCKER volume rm -f keelstone-data-n1 keelstone-data-n2 keelstone-data-n3 >/dev/null 2>&1 || true
  for iface in ks-n1 ks-n2 ks-n3 ks-cli; do
    sudo ip link del "$iface" >/dev/null 2>&1 || true
  done
}

start_nodes() {
  local bin=$1
  local inflight=$2
  local i ip
  for i in 1 2 3; do
    ip="10.10.0.1${i}"
    local -a args=(
      --id "n$i" --listen :43121 --advertise "http://${ip}:43121"
      --data /data --peers "$PEERS"
      --tick "$TICK" --election-tick "$ELECTION" --heartbeat-tick 1
      --snapshot-entries 8000
    )
    if [[ "$bin" == "keelstone" ]]; then
      args+=(--max-inflight "$inflight" --admin-crash)
    fi
    $DOCKER run -d --name "keelstone-n$i" \
      --network none \
      --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
      --cap-add NET_ADMIN \
      -v "keelstone-data-n$i:/data" \
      --entrypoint "/usr/local/bin/$bin" \
      "$IMAGE" \
      "${args[@]}"
  done
}

attach_veth() {
  local cname=$1 ip=$2 hostif=$3
  local pid
  pid="$($DOCKER inspect -f '{{.State.Pid}}' "$cname")"
  sudo ip link del "$hostif" >/dev/null 2>&1 || true
  sudo ip link add "$hostif" type veth peer name "${hostif}p"
  sudo ip link set "${hostif}p" netns "$pid"
  sudo nsenter -t "$pid" -n ip link set lo up
  sudo nsenter -t "$pid" -n ip link set "${hostif}p" name eth0
  sudo nsenter -t "$pid" -n ip link set eth0 up
  sudo nsenter -t "$pid" -n ip addr add "${ip}/24" dev eth0
  sudo ip link set "$hostif" up
  # 校验和卸载会让 AF_PACKET 转发看到未填校验和的 TCP，对端直接丢掉。
  sudo ethtool -K "$hostif" tx off rx off gro off gso off tso off >/dev/null 2>&1 || true
  sudo nsenter -t "$pid" -n ethtool -K eth0 tx off rx off gro off gso off tso off >/dev/null 2>&1 || true
}

start_switch() {
  local delay=$1 jitter=$2
  stop_switch
  sudo "$ROOT/bin/l2delay" -ifs ks-n1,ks-n2,ks-n3,ks-cli -delay "$delay" -jitter "$jitter" \
    >"$OUT/l2delay.log" 2>&1 &
  echo $! >"$OUT/l2delay.pid"
  sleep 0.2
}

wait_leader() {
  local i
  for _ in $(seq 1 80); do
    local leaders=0 up=0
    for i in 1 2 3; do
      if $DOCKER exec "keelstone-n$i" wget -q -T 1 -O - "http://127.0.0.1:43121/admin/status" 2>/dev/null | grep -q '"role":"leader"'; then
        leaders=$((leaders + 1))
        up=$((up + 1))
      elif $DOCKER exec "keelstone-n$i" wget -q -T 1 -O - "http://127.0.0.1:43121/healthz" >/dev/null 2>&1; then
        up=$((up + 1))
      fi
    done
    if [[ "$up" == "3" && "$leaders" == "1" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "cluster not ready" >&2
  $DOCKER logs keelstone-n1 >&2 || true
  $DOCKER logs keelstone-n2 >&2 || true
  return 1
}

cgroup_and_ping() {
  local profile=$1
  {
    echo "PROFILE $profile"
    echo "cpu.max=$($DOCKER exec keelstone-n1 cat /sys/fs/cgroup/cpu.max)"
    echo "memory.max=$($DOCKER exec keelstone-n1 cat /sys/fs/cgroup/memory.max)"
    echo "--- ping client -> n2 ---"
    $DOCKER exec keelstone-client ping -c 20 -i 0.05 -q 10.10.0.12
    echo "--- ping n1 -> n2 ---"
    $DOCKER exec keelstone-n1 ping -c 20 -i 0.05 -q 10.10.0.12
  } >"$OUT/netem-${profile}.txt" 2>&1 || true
}

merge_result() {
  local tag=$1 src=$2
  python3 - "$tag" "$src" <<'PY'
import json, sys
tag, path = sys.argv[1], sys.argv[2]
meta = dict(part.split("=", 1) for part in tag.split(","))
with open(path) as f:
    rep = json.load(f)
rep.update(meta)
print(json.dumps(rep, ensure_ascii=False))
PY
}

prepare_case() {
  local bin=$1 inflight=$2 delay=$3 jitter=$4
  wipe
  start_nodes "$bin" "$inflight"
  attach_veth keelstone-n1 10.10.0.11 ks-n1
  attach_veth keelstone-n2 10.10.0.12 ks-n2
  attach_veth keelstone-n3 10.10.0.13 ks-n3
  $DOCKER run -d --name keelstone-client \
    --network none \
    --cpus 1 --memory 512m --memory-swap 512m \
    --cap-add NET_ADMIN \
    --entrypoint sleep \
    "$IMAGE" infinity >/dev/null
  attach_veth keelstone-client 10.10.0.20 ks-cli
  start_switch "$delay" "$jitter"
  wait_leader
}

one_case() {
  local profile=$1 delay=$2 jitter=$3 variant=$4 bin=$5 inflight=$6 workload=$7
  echo "=== $profile $variant workload=$workload inflight=$inflight ===" >&2
  prepare_case "$bin" "$inflight" "$delay" "$jitter"
  if [[ "$workload" == "A" ]]; then
    cgroup_and_ping "$profile"
  fi
  local raw="$OUT/raw-${profile}-${variant}-${workload}.json"
  if ! $DOCKER exec keelstone-client bench \
      -addrs "$ADDRS" -clients "$CLIENTS" -keys "$KEYS" \
      -duration "$DURATION" -warmup "$WARMUP" \
      -mode=load -workload="$workload" >"$raw"; then
    echo "bench failed for $profile $variant $workload" >&2
    return 1
  fi
  merge_result "profile=$profile,variant=$variant,binary=$bin,max_inflight=$inflight,delay=$delay,jitter=$jitter" "$raw" | tee -a "$OUT/results.jsonl"
}

failover_case() {
  local profile=$1 delay=$2 jitter=$3
  echo "=== failover-load $profile ===" >&2
  prepare_case keelstone 8 "$delay" "$jitter"
  # 后台保持 YCSB A。测量本身在客户端里打写，主机用 docker kill -s KILL 杀掉当前 leader。
  $DOCKER exec -d keelstone-client bench \
    -addrs "$ADDRS" -clients "$CLIENTS" -keys "$KEYS" \
    -duration 25s -warmup 1s -mode=load -workload=A
  sleep 2
  local leader=""
  local i st
  for i in 1 2 3; do
    st="$($DOCKER exec "keelstone-n$i" wget -q -T 1 -O - http://127.0.0.1:43121/admin/status)"
    if printf '%s' "$st" | grep -q '"role":"leader"'; then
      leader="n$i"
    fi
  done
  if [[ -z "$leader" ]]; then
    echo "failover: no leader" >&2
    return 1
  fi
  local watch="$OUT/raw-failover-${profile}.watch"
  local readyf="$OUT/raw-failover-${profile}.ready"
  : >"$readyf"
  # 探针尝试 150ms。负载客户端仍用默认 2s。2s 会把死连接上的整段预算算进故障转移。
  $DOCKER exec keelstone-client bench -addrs "$ADDRS" -mode=failover-watch -old-leader "$leader" -attempt 150ms >"$watch" 2>"$readyf" &
  local wpid=$!
  local ready=0
  for _ in $(seq 1 80); do
    if grep -q READY "$readyf" 2>/dev/null; then
      ready=1
      break
    fi
    sleep 0.05
  done
  if [[ "$ready" != "1" ]]; then
    echo "failover watch did not print READY" >&2
    kill "$wpid" >/dev/null 2>&1 || true
    wait "$wpid" || true
    return 1
  fi
  local t0
  t0=$(date +%s%N)
  $DOCKER kill -s KILL "keelstone-$leader" >/dev/null
  wait "$wpid"
  local code
  code=$($DOCKER inspect -f '{{.State.ExitCode}}' "keelstone-$leader")
  python3 - "$t0" "$watch" "$profile" "$leader" "$code" >>"$OUT/results.jsonl" <<'PY'
import json, sys
t0 = int(sys.argv[1])
watch, profile, old, code = sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
line = open(watch).read().strip().splitlines()[-1]
got = json.loads(line)
ms = (int(got["success_unix_nano"]) - t0) / 1e6
print(json.dumps({
    "mode": "failover-load",
    "profile": profile,
    "variant": "after",
    "binary": "keelstone",
    "max_inflight": "8",
    "workload": "A",
    "old_leader": old,
    "killed_exit_code": code,
    "new_leader": got["leader"],
    "probe_attempt": "150ms",
    "failover_ms": ms,
    "note": "YCSB A 负载中，主机 docker kill -s KILL 当前 leader 的时刻，到新 leader 上第一次 Put 返回的墙上时钟",
}, ensure_ascii=False))
PY
}

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  lscpu | sed -n '1,20p'
  grep MemTotal /proc/meminfo
  go version
  $DOCKER version --format 'docker={{.Server.Version}} storage={{.Server.Version}}'
  $DOCKER info --format 'driver={{.Driver}}'
  echo '--- tc netem probe ---'
  sudo tc qdisc replace dev lo root netem delay 1ms || true
  echo "modules_disabled=$(cat /proc/sys/kernel/modules_disabled)"
} >"$OUT/host.txt" 2>&1

if [[ -z "${APPEND_RESULTS:-}" ]]; then
  : >"$OUT/results.jsonl"
fi
build_bins
build_image
wipe

# 每帧单向延迟。RTT 大约是两倍，因为请求和响应各被 hold 一次。
# same-dc:    0.25ms ± 0.05ms  → RTT ~0.5ms
# cross-zone: 2ms ± 0.4ms      → RTT ~4ms
for spec in "same-dc 0.25ms 0.05ms" "cross-zone 2ms 0.4ms"; do
  # shellcheck disable=SC2086
  set -- $spec
  name=$1
  delay=$2
  jitter=$3
  if [[ -n "${ONLY_PROFILE:-}" && "$name" != "$ONLY_PROFILE" ]]; then
    continue
  fi
  if [[ -z "${SKIP_LOAD:-}" ]]; then
  for workload in A B; do
    if [[ "${ONLY_VARIANT:-}" != "after" ]]; then
      one_case "$name" "$delay" "$jitter" before keelstone-before 1 "$workload"
    fi
    if [[ "${ONLY_VARIANT:-}" != "before" ]]; then
      one_case "$name" "$delay" "$jitter" after keelstone 8 "$workload"
    fi
  done
  fi
  if [[ -z "${SKIP_FAILOVER:-}" ]]; then
    samples="${FAILOVER_SAMPLES:-1}"
    s=1
    while [[ "$s" -le "$samples" ]]; do
      failover_case "$name" "$delay" "$jitter"
      s=$((s + 1))
    done
  fi
done

wipe
echo "DONE $OUT/results.jsonl" >&2
