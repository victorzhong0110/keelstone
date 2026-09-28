# Keelstone

Keelstone 是内部微服务的配置中心和元数据存储。功能开关、灰度比例、路由表写成键值，由单个 Raft 组复制。读路径要求强一致：挂掉一个节点，也不能读到过期配置。

仓库：https://github.com/victorzhong0110/keelstone

实现是一份原创的单 Raft 组键值存储。选举、日志复制、WAL、快照、ReadIndex 线性读、幂等客户端和成员变更都在本仓库里，没有引用 etcd 或 Hashicorp 的 Raft 库。

Keelstone stores configuration and metadata for internal microservices. Feature flags, rollout ratios, and routing tables are key-value records replicated by one Raft group. Reads are linearizable: losing one node must not serve a stale config.

Repository: https://github.com/victorzhong0110/keelstone

The implementation is an original single-group Raft key-value store in Go. Leader election, log replication, a WAL, snapshots, ReadIndex linearizable reads, an idempotent client, and membership change are in this repository. The core does not import etcd/raft or hashicorp/raft.

- 设计说明：[docs/design.md](docs/design.md)
- 环回实测：[docs/benchmark.md](docs/benchmark.md)
- 容器、延迟和 YCSB：[docs/benchmark-realistic.md](docs/benchmark-realistic.md)

## 中文

### 能做什么

- 三副本或五副本选出一个 leader，复制 `Put` / `Delete` / `CAS`
- 崩溃之后用 WAL 和快照把已提交的数据恢复出来
- 线性读走 ReadIndex（不是依赖时钟的 lease）
- 客户端带 `X-Client-Id` 和 `X-Request-Id`，超时后用同一个请求号重试
- 成员变更：`add_learner` → `promote` → `remove`，一次一台
- 进程内故障注入（分区、丢包、延迟、乱序、崩溃）并用 Porcupine 检查线性一致性

### 布局

```
cmd/keelstone          节点进程
cmd/bench           吞吐和故障转移测量
cmd/l2delay         用户态二层延迟。这台内核没有 sch_netem
scripts/bench-realistic.sh   三个限资源容器上的 YCSB 对比
internal/raft       选举、复制、WAL、快照、ReadIndex
internal/kv         状态机和幂等请求号
internal/server     HTTP API
internal/client     重定向和重试
internal/cluster    故障注入
internal/lincheck   按 key 划分的线性模型
```

### 本地三个节点

端口放在 32768 以下，避开 Linux 临时端口，否则高压之后的 TIME_WAIT 会让下一次绑定失败。

```bash
go build -o bin/keelstone ./cmd/keelstone
PEERS=n1=http://127.0.0.1:23121,n2=http://127.0.0.1:23122,n3=http://127.0.0.1:23123
mkdir -p data/n1 data/n2 data/n3
bin/keelstone --id n1 --listen 127.0.0.1:23121 --advertise http://127.0.0.1:23121 --data data/n1 --peers $PEERS &
bin/keelstone --id n2 --listen 127.0.0.1:23122 --advertise http://127.0.0.1:23122 --data data/n2 --peers $PEERS &
bin/keelstone --id n3 --listen 127.0.0.1:23123 --advertise http://127.0.0.1:23123 --data data/n3 --peers $PEERS &
```

```bash
curl -s http://127.0.0.1:23121/admin/status
curl -s -X PUT http://127.0.0.1:23121/v1/kv/hello \
  -H 'Content-Type: application/json' \
  -H 'X-Client-Id: demo' -H 'X-Request-Id: 1' \
  -d '{"value":"world"}'
curl -s http://127.0.0.1:23121/v1/kv/hello
curl -s -X POST http://127.0.0.1:23121/v1/kv/hello/cas \
  -H 'Content-Type: application/json' \
  -H 'X-Client-Id: demo' -H 'X-Request-Id: 2' \
  -d '{"expected":"world","exp_present":true,"value":"raft"}'
curl -s -X DELETE http://127.0.0.1:23121/v1/kv/hello \
  -H 'X-Client-Id: demo' -H 'X-Request-Id: 3'
```

不是 leader 时返回 HTTP 409，JSON 里有 `leader_addr`。客户端库会跟着这个地址重试，并保持同一个请求号。

加入第四台时，先让它以 learner 启动，再在 leader 上提升：

```bash
bin/keelstone --id n4 --listen 127.0.0.1:23124 --advertise http://127.0.0.1:23124 \
  --data data/n4 --join --peers $PEERS,n4=http://127.0.0.1:23124
curl -s -X POST http://127.0.0.1:23121/admin/members \
  -H 'Content-Type: application/json' \
  -d '{"op":"add_learner","id":"n4","addr":"http://127.0.0.1:23124"}'
curl -s -X POST http://127.0.0.1:23121/admin/members \
  -H 'Content-Type: application/json' \
  -d '{"op":"promote","id":"n4","addr":"http://127.0.0.1:23124"}'
```

`promote` 之前要等 n4 的 `applied` 追上 leader，否则新的法定人数会卡在这台空节点上。

### Docker

```bash
docker compose up --build
docker compose -f docker-compose.five.yml up --build
```

三节点映射到宿主机 `23121–23123`。容器内部仍监听 `43121`，用服务名互相访问。

### 测试和压测

```bash
go test -count=1 ./...
go test -race -count=1 ./...
make bench
```

`KEELSTONE_FAULT_ITERS=20 go test -run TestFaultLinearizable ./internal/cluster/` 会多跑几轮故障注入。改复制窗口之前的 20/20 在 `docs/benchmark.md`，改完之后的 20/20 在 `docs/benchmark-realistic.md`。

`scripts/bench-realistic.sh` 把三个节点放进单独的容器（各 0.5 CPU、256 MiB），用 `cmd/l2delay` 在 veth 上加同机房或跨区延迟。这台内核 `tc netem` 不可用，脚本会把失败输出留下来。

打开 `KEELSTONE_DEBUG=1` 可以看到每个节点的角色变化。

## English

### What it does

- Elects a leader and replicates `Put`, `Delete`, and `CAS` across 3 or 5 voters
- Recovers committed data from a CRC-checked WAL and snapshots
- Serves linearizable reads with ReadIndex (no clock-based lease)
- Retries with a stable client id and request id so a timed-out write is not applied twice
- Changes membership one server at a time: `add_learner`, then `promote`, then `remove`
- Injects partitions, drops, delay, reorder, and crashes, and checks histories with Porcupine

### Run a local cluster

Use ports below the Linux ephemeral range (`32768` on this machine). After a high-QPS run, `TIME_WAIT` sockets occupy that range and the next bind fails.

```bash
go build -o bin/keelstone ./cmd/keelstone
PEERS=n1=http://127.0.0.1:23121,n2=http://127.0.0.1:23122,n3=http://127.0.0.1:23123
mkdir -p data/n1 data/n2 data/n3
bin/keelstone --id n1 --listen 127.0.0.1:23121 --advertise http://127.0.0.1:23121 --data data/n1 --peers $PEERS &
bin/keelstone --id n2 --listen 127.0.0.1:23122 --advertise http://127.0.0.1:23122 --data data/n2 --peers $PEERS &
bin/keelstone --id n3 --listen 127.0.0.1:23123 --advertise http://127.0.0.1:23123 --data data/n3 --peers $PEERS &
```

```bash
curl -s http://127.0.0.1:23121/admin/status
curl -s -X PUT http://127.0.0.1:23121/v1/kv/hello \
  -H 'Content-Type: application/json' \
  -H 'X-Client-Id: demo' -H 'X-Request-Id: 1' \
  -d '{"value":"world"}'
curl -s http://127.0.0.1:23121/v1/kv/hello
```

A follower responds with HTTP 409 and `leader_addr`. The client keeps the same request id across redirects.

To add a fourth node, start it with `--join`, then:

```bash
curl -s -X POST http://127.0.0.1:23121/admin/members \
  -H 'Content-Type: application/json' \
  -d '{"op":"add_learner","id":"n4","addr":"http://127.0.0.1:23124"}'
# wait until n4's applied index catches the leader, then:
curl -s -X POST http://127.0.0.1:23121/admin/members \
  -H 'Content-Type: application/json' \
  -d '{"op":"promote","id":"n4","addr":"http://127.0.0.1:23124"}'
```

### Tests

```bash
go test -count=1 ./...
go test -race -count=1 ./...
make bench
```

`docs/benchmark.md` records one loopback measurement: write/read throughput and P99 for 3 and 5 nodes, five leader-failover samples, and 20/20 randomized linearizability runs. `docs/benchmark-realistic.md` records a measurement with three cgroup-limited containers, injected latency, YCSB A/B, a replication-window before/after, failover under load, and another 20/20 linearizability run after that window landed. Those numbers are not targets.

Set `KEELSTONE_DEBUG=1` to log role changes.

## References / 致谢

实现是原创的。下面这些仓库和课程只用来对照设计和取舍，没有复制它们的源码。MIT 6.824 的实验答案没有、也不应放进公开仓库。

| 来源 | 许可证 | 借鉴了什么 |
|---|---|---|
| [Raft 论文](https://raft.github.io/raft.pdf) 与 Ongaro 学位论文 | 论文 | 选举、Figure 8、快照、PreVote（9.6）、ReadIndex、一次一台的成员变更 |
| [talent-plan/tinykv](https://github.com/talent-plan/tinykv) | Apache-2.0 | 课程把「单机 KV → Raft → 线性读」拆开的路径；自带测试的想法 |
| [etcd-io/raft](https://github.com/etcd-io/raft) | Apache-2.0 | 单线程状态机、把持久化和网络留在核心外面。这里没有做成 etcd 的 `Ready`/`Advance`，而是把 fsync 写在发送旁边，持久化顺序和发送顺序在同一段代码里 |
| [hashicorp/raft](https://github.com/hashicorp/raft) | MPL-2.0 | 快照和用户状态机分开的边界。本仓库的 `StateMachine` 接口是同一类划分，格式不同 |
| MIT 6.824 | 课程材料，禁止公开实验答案 | 用故障注入加线性一致性检查来验收 Raft，而不是只测快乐路径 |
| [anishathalye/porcupine](https://github.com/anishathalye/porcupine) | MIT | 作为库调用，用来检查按 key 分区的历史 |

Porcupine 是唯一的第三方运行时依赖。
