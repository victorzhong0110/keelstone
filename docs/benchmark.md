# 压测记录

数字来自 2026-09-28 在本机上的实测，没有估测或套用别人的结果。原始 JSON 由 `cmd/bench` 打到标准输出。

下面是同一台虚拟机上的环回结果。容器、CPU 限额、注入延迟和 YCSB A/B 在 [benchmark-realistic.md](benchmark-realistic.md)。那一页的线性一致性是复制窗口改完之后重跑的；本页 33.58 s 的 20 轮是改窗口之前的。

## 机器

采集时间：2026-09-28T07:10:05Z（负载测试开始）。故障转移在 07:15Z 左右。

| 项 | 值 |
|---|---|
| 系统 | Linux 6.12.94+ #1 SMP PREEMPT_DYNAMIC Thu Sep 24 16:04:37 UTC 2026 x86_64 |
| CPU | Intel(R) Xeon(R) Processor，family 6 model 207，4 vCPU，KVM |
| 缓存 | L1d 192 KiB，L2 8 MiB，L3 320 MiB（`lscpu`） |
| 内存 | MemTotal 16398384 kB（约 15.6 GiB） |
| 磁盘 | 根分区 `overlay`，`df -T /` |
| Go | go1.22.2 linux/amd64 |

进程都在同一台虚拟机上，节点之间走 127.0.0.1 的 HTTP，没有跨机 RTT。

## 方法

```bash
go build -o bin/keelstone ./cmd/keelstone
go build -o bin/bench ./cmd/bench
```

`bin/bench` 自己拉起 `keelstone` 子进程。每个节点：

- `--tick 20ms --election-tick 8 --heartbeat-tick 1 --snapshot-entries 2000`
- WAL 每次 `Append` 和每次 HardState 重写都会 `fsync`（见 `internal/raft/storage.go`）
- 8 个客户端，键空间 64，值是很短的字符串
- 预热 2 秒不计入结果，正式采样 10 秒
- 纯读之前会先把 64 个键写满
- 延迟是客户端从发请求到拿到响应，含重定向

P99 取排序后下标 `int((n-1)*0.99)` 的样本。

## 负载

| 场景 | 成功请求 | 错误 | 采样秒数 | 吞吐 (op/s) | P50 (ms) | P99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| 3 节点纯写 | 39620 | 0 | 10.000677 | 3961.73 | 1.501 | 5.526 |
| 3 节点纯读 | 189856 | 0 | 10.000010 | 18985.58 | 0.354 | 1.465 |
| 5 节点纯写 | 13045 | 0 | 10.000069 | 1304.49 | 3.630 | 21.354 |
| 5 节点纯读 | 127653 | 0 | 10.001717 | 12763.11 | 0.526 | 2.141 |

命令：

```bash
./bin/bench -mode=load -spawn=3 -read-ratio=0 -duration=10s -warmup=2s -clients=8 -base-port=43121
./bin/bench -mode=load -spawn=3 -read-ratio=1 -duration=10s -warmup=2s -clients=8 -base-port=43221
./bin/bench -mode=load -spawn=5 -read-ratio=0 -duration=10s -warmup=2s -clients=8 -base-port=46121
./bin/bench -mode=load -spawn=5 -read-ratio=1 -duration=10s -warmup=2s -clients=8 -base-port=46221
```

这四次的标准错误里，每个 `keelstone` 都打印了 `listening`，没有绑端口失败。

读比写快一个数量级，是因为写路径每次提交都要 fsync 日志和 HardState，还要等多数派复制；读走 ReadIndex，只多一次法定人数心跳，不写盘。五节点比三节点慢，是因为法定人数从 2 变成 3，复制和心跳都要多等一个回复。这是环回上的单次采样，不是多次实验的均值。

## Leader 故障转移

三节点，`SIGKILL` 当前 leader，从发出 Kill 到新 leader 上第一次 `Put` 成功：

| 次数 | 端口 | 毫秒 |
|---|---|---:|
| 1 | 23161 | 424.672 |
| 2 | 23171 | 529.507 |
| 3 | 23181 | 438.227 |
| 4 | 23191 | 421.792 |
| 5 | 23201 | 440.007 |

五次都成功。最小 421.792 ms，最大 529.507 ms。选举超时基数是 8 个 20 ms tick，随机落在 160–300 ms，再加上客户端重试，所以结果落在这个区间是符合实现的，不是单独测「选举定时器」。

```bash
./bin/bench -mode=failover -spawn=3 -base-port=23161
```

端口选在 32768 以下，避开本机临时端口范围 `32768–60999`。高 QPS 之后大量 TIME_WAIT 会占掉临时端口，绑在那个区间里会偶发 `address already in use`。

## 线性一致性

```bash
KEELSTONE_FAULT_ITERS=20 go test -count=1 -timeout 10m -run TestFaultLinearizable ./internal/cluster/ -v
```

2026-09-28 这次结果：**20/20 通过**（`fault_passed=20 fault_total=20`），耗时 33.58 s。

每一轮是 3 个节点、3 个客户端、约 1.5 秒的随机故障（分区、15%–35% 丢包、延迟、乱序、最多同时崩溃 1 个节点），然后恢复网络、重启宕机节点、等状态机一致，再用 Porcupine 按 key 检查历史。默认的 `go test ./...` 每个包只跑 3 轮，上面这 20 轮是额外加的。

同一次测量中 `go test -race -count=1 ./...` 通过（含默认 3 轮故障注入）。
