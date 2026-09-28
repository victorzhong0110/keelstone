// Command bench 测量集群的写/读吞吐、P99，以及杀掉 leader 之后的故障转移时间。
// 数字全部来自这次进程里的采样，不读任何预先写好的结果。
//
// 本地拉起进程：
//
//	bench -mode=load -spawn=3
//
// 对着已经在跑的集群（容器或本机）测 YCSB 风格负载：
//
//	bench -mode=load -addrs http://n1:43121,http://n2:43121,http://n3:43121 -workload=A
//
// workload A 是 50% 读 / 50% 写，B 是 95% 读 / 5% 写，键都按 Zipf(s=0.99) 抽样。
// failover-load 在负载跑起来之后 POST /admin/crash，计从发出到新 leader 上第一次写成功。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/victorzhong0110/keelstone/internal/client"
	"github.com/victorzhong0110/keelstone/internal/raft"
)

type report struct {
	Mode       string  `json:"mode"`
	Workload   string  `json:"workload,omitempty"`
	Dist       string  `json:"dist,omitempty"`
	Nodes      int     `json:"nodes,omitempty"`
	Clients    int     `json:"clients,omitempty"`
	Keys       int     `json:"keys,omitempty"`
	ReadRatio  float64 `json:"read_ratio,omitempty"`
	Ops        int     `json:"ops,omitempty"`
	ReadOps    int     `json:"read_ops,omitempty"`
	WriteOps   int     `json:"write_ops,omitempty"`
	Errors     int     `json:"errors,omitempty"`
	Seconds    float64 `json:"seconds,omitempty"`
	Throughput float64 `json:"throughput_ops,omitempty"`
	P50Ms      float64 `json:"p50_ms,omitempty"`
	P99Ms      float64 `json:"p99_ms,omitempty"`
	P50ReadMs  float64 `json:"p50_read_ms,omitempty"`
	P99ReadMs  float64 `json:"p99_read_ms,omitempty"`
	P50WriteMs float64 `json:"p50_write_ms,omitempty"`
	P99WriteMs float64 `json:"p99_write_ms,omitempty"`
	FailoverMs float64 `json:"failover_ms,omitempty"`
	Note       string  `json:"note,omitempty"`
}

type sample struct {
	lat  time.Duration
	read bool
}

// httpAttempt 是单次 HTTP 尝试的上限。负载默认 2s；故障转移探针可以单独调短，
// 避免死掉的 leader 连接把整段尝试预算耗完，测出来的时间只剩客户端超时。
var httpAttempt = 2 * time.Second

func main() {
	mode := flag.String("mode", "load", "load、failover、failover-load 或 failover-watch")
	bin := flag.String("bin", "bin/keelstone", "keelstone 可执行文件，仅在自己拉起进程时使用")
	spawn := flag.Int("spawn", 3, "拉起的节点数")
	basePort := flag.Int("base-port", 23121, "第一个节点的端口，需低于本机临时端口范围")
	addrsFlag := flag.String("addrs", "", "已有集群地址，逗号分隔。设置后不再拉起进程")
	clientsN := flag.Int("clients", 8, "并发客户端")
	duration := flag.Duration("duration", 10*time.Second, "正式采样时长")
	warmup := flag.Duration("warmup", 2*time.Second, "预热，不计入结果")
	readRatio := flag.Float64("read-ratio", 0, "0 纯写，1 纯读；workload 不为空时被覆盖")
	workload := flag.String("workload", "", "A（50/50）或 B（95/5），键按 Zipf 抽样")
	dist := flag.String("dist", "uniform", "uniform 或 zipf；workload 不为空时强制 zipf")
	keys := flag.Int("keys", 64, "键空间")
	inflight := flag.Int("max-inflight", 8, "拉起进程时传给 keelstone")
	oldLeader := flag.String("old-leader", "", "failover-watch：旧 leader 的 id")
	attempt := flag.Duration("attempt", 2*time.Second, "单次 HTTP 尝试上限")
	dataRoot := flag.String("data", "", "数据目录，默认临时目录")
	flag.Parse()
	httpAttempt = *attempt

	wl, ratio, distName, err := resolveWorkload(*workload, *readRatio, *dist)
	if err != nil {
		log.Fatal(err)
	}
	if *keys < 2 {
		log.Fatal("keys >= 2")
	}

	var cmds []*exec.Cmd
	addrs := splitAddrs(*addrsFlag)
	spawned := len(addrs) == 0
	if spawned {
		if *spawn < 1 {
			log.Fatal("spawn >= 1")
		}
		cmds, addrs, err = spawnNodes(*bin, *spawn, *basePort, *dataRoot, *inflight)
		if err != nil {
			log.Fatal(err)
		}
		defer killAll(cmds)
		if err := waitReady(cmds, addrs, 15*time.Second); err != nil {
			killAll(cmds)
			log.Fatal(err)
		}
	} else if err := waitReady(nil, addrs, 20*time.Second); err != nil {
		log.Fatal(err)
	}

	var runErr error
	switch *mode {
	case "load":
		var rep report
		rep, runErr = runLoad(addrs, *clientsN, *duration, *warmup, ratio, distName, *keys)
		if runErr == nil {
			rep.Mode = "load"
			rep.Workload = wl
			rep.Dist = distName
			rep.Nodes = len(addrs)
			dump(rep)
		}
	case "failover":
		if !spawned {
			runErr = fmt.Errorf("failover 需要本进程拉起的节点；容器请用 failover-load")
			break
		}
		var ms float64
		ms, runErr = runFailover(cmds, addrs)
		if runErr == nil {
			dump(report{Mode: "failover", Nodes: len(addrs), FailoverMs: ms, Note: "从 Kill(leader) 到新 leader 上第一次写成功"})
		}
	case "failover-watch":
		if *oldLeader == "" {
			runErr = fmt.Errorf("failover-watch 需要 -old-leader")
			break
		}
		runErr = runFailoverWatch(addrs, *oldLeader)
	case "failover-load":
		var rep report
		rep, runErr = runFailoverLoad(addrs, *clientsN, ratio, distName, *keys, *warmup)
		if runErr == nil {
			rep.Workload = wl
			rep.Dist = distName
			rep.Nodes = len(addrs)
			dump(rep)
		}
	default:
		runErr = fmt.Errorf("unknown mode %q", *mode)
	}
	if runErr != nil {
		killAll(cmds)
		fmt.Fprintln(os.Stderr, runErr)
		os.Exit(1)
	}
}

func resolveWorkload(name string, ratio float64, dist string) (string, float64, string, error) {
	switch name {
	case "":
		if dist != "uniform" && dist != "zipf" {
			return "", 0, "", fmt.Errorf("dist 只能是 uniform 或 zipf")
		}
		return "", ratio, dist, nil
	case "A", "a":
		return "A", 0.50, "zipf", nil
	case "B", "b":
		return "B", 0.95, "zipf", nil
	default:
		return "", 0, "", fmt.Errorf("workload 只能是 A 或 B")
	}
}

func splitAddrs(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			part := s[start:i]
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	return out
}

func spawnNodes(bin string, spawn, basePort int, dataRoot string, inflight int) ([]*exec.Cmd, []string, error) {
	root := dataRoot
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "keelstone-bench-")
		if err != nil {
			return nil, nil, err
		}
	}
	absBin, err := filepath.Abs(bin)
	if err != nil {
		return nil, nil, err
	}
	var peers []string
	var addrs []string
	for i := 0; i < spawn; i++ {
		id := fmt.Sprintf("n%d", i+1)
		addr := fmt.Sprintf("http://127.0.0.1:%d", basePort+i)
		peers = append(peers, id+"="+addr)
		addrs = append(addrs, addr)
	}
	peerArg := ""
	for i, p := range peers {
		if i > 0 {
			peerArg += ","
		}
		peerArg += p
	}
	var cmds []*exec.Cmd
	for i := 0; i < spawn; i++ {
		id := fmt.Sprintf("n%d", i+1)
		dir := filepath.Join(root, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			killAll(cmds)
			return nil, nil, err
		}
		cmd := exec.Command(absBin,
			"--id", id,
			"--listen", fmt.Sprintf("127.0.0.1:%d", basePort+i),
			"--advertise", addrs[i],
			"--data", dir,
			"--peers", peerArg,
			"--tick", "20ms",
			"--election-tick", "8",
			"--heartbeat-tick", "1",
			"--snapshot-entries", "2000",
			"--max-inflight", fmt.Sprintf("%d", inflight),
		)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			killAll(cmds)
			return nil, nil, err
		}
		cmds = append(cmds, cmd)
	}
	return cmds, addrs, nil
}

func killAll(cmds []*exec.Cmd) {
	for _, c := range cmds {
		if c == nil || c.Process == nil {
			continue
		}
		_ = c.Process.Kill()
		_, _ = c.Process.Wait()
	}
}

func alive(c *exec.Cmd) bool {
	if c == nil || c.Process == nil {
		return false
	}
	err := c.Process.Signal(syscall.Signal(0))
	return err == nil
}

func waitReady(cmds []*exec.Cmd, addrs []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, c := range cmds {
			if !alive(c) {
				return fmt.Errorf("节点 n%d 启动后退出，地址 %s 可能被占用", i+1, addrs[i])
			}
		}
		up := 0
		leaders := 0
		for _, a := range addrs {
			st, err := getStatus(a)
			if err != nil {
				continue
			}
			up++
			if st.Role == "leader" {
				leaders++
			}
		}
		if up == len(addrs) && leaders == 1 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("集群没有在时限内就绪（%d 个地址）", len(addrs))
}

func getStatus(addr string) (raft.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/admin/status", nil)
	if err != nil {
		return raft.Status{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return raft.Status{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return raft.Status{}, err
	}
	var st raft.Status
	if err := json.Unmarshal(body, &st); err != nil {
		return raft.Status{}, err
	}
	return st, nil
}

func newClient(id string, addrs []string) *client.Client {
	c := client.New(id, addrs)
	c.SetAttempt(httpAttempt)
	return c
}

func prefill(addrs []string, keys, workers int) error {
	if workers < 1 {
		workers = 1
	}
	if workers > keys {
		workers = keys
	}
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	chunk := (keys + workers - 1) / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cli := newClient(fmt.Sprintf("prefill-%d", w), addrs)
			lo := w * chunk
			hi := lo + chunk
			if hi > keys {
				hi = keys
			}
			for i := lo; i < hi; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				_, err := cli.Put(ctx, fmt.Sprintf("k%d", i), "seed")
				cancel()
				if err != nil {
					errCh <- fmt.Errorf("prefill k%d: %w", i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

type keyPick struct {
	zipf *rand.Zipf
	keys int
	seq  int
}

func newPick(dist string, keys int, seed int64) keyPick {
	p := keyPick{keys: keys}
	if dist == "zipf" {
		p.zipf = rand.NewZipf(rand.New(rand.NewSource(seed)), 0.99, 1, uint64(keys-1))
	}
	return p
}

func (p *keyPick) next() string {
	p.seq++
	if p.zipf != nil {
		return fmt.Sprintf("k%d", p.zipf.Uint64())
	}
	return fmt.Sprintf("k%d", p.seq%p.keys)
}

func runLoad(addrs []string, clientsN int, duration, warmup time.Duration, readRatio float64, dist string, keys int) (report, error) {
	if readRatio > 0 {
		workers := clientsN
		if workers > 32 {
			workers = 32
		}
		if err := prefill(addrs, keys, workers); err != nil {
			return report{}, err
		}
	}
	return sampleLoad(addrs, clientsN, duration, warmup, readRatio, dist, keys, false)
}

func sampleLoad(addrs []string, clientsN int, duration, warmup time.Duration, readRatio float64, dist string, keys int, countOnly bool) (report, error) {
	var sampling atomic.Bool
	var wg sync.WaitGroup
	stop := make(chan struct{})
	locals := make([][]sample, clientsN)
	errs := make([]int, clientsN)
	for i := 0; i < clientsN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cli := newClient(fmt.Sprintf("bench-%d", i), addrs)
			pick := newPick(dist, keys, int64(1000+i))
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := pick.next()
				read := readRatio > 0 && rand.Float64() < readRatio
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				t0 := time.Now()
				var err error
				if read {
					_, err = cli.Get(ctx, key)
				} else {
					_, err = cli.Put(ctx, key, fmt.Sprintf("v-%d-%d", i, pick.seq))
				}
				cancel()
				dt := time.Since(t0)
				if !sampling.Load() {
					continue
				}
				if err != nil {
					errs[i]++
					continue
				}
				if !countOnly {
					locals[i] = append(locals[i], sample{lat: dt, read: read})
				}
			}
		}(i)
	}
	time.Sleep(warmup)
	sampling.Store(true)
	t0 := time.Now()
	time.Sleep(duration)
	seconds := time.Since(t0).Seconds()
	close(stop)
	wg.Wait()

	var all, reads, writes []time.Duration
	ops, readOps, writeOps, errN := 0, 0, 0, 0
	for i := range locals {
		errN += errs[i]
		for _, s := range locals[i] {
			ops++
			all = append(all, s.lat)
			if s.read {
				readOps++
				reads = append(reads, s.lat)
			} else {
				writeOps++
				writes = append(writes, s.lat)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	sort.Slice(reads, func(i, j int) bool { return reads[i] < reads[j] })
	sort.Slice(writes, func(i, j int) bool { return writes[i] < writes[j] })
	return report{
		Clients:    clientsN,
		Keys:       keys,
		ReadRatio:  readRatio,
		Ops:        ops,
		ReadOps:    readOps,
		WriteOps:   writeOps,
		Errors:     errN,
		Seconds:    seconds,
		Throughput: float64(ops) / seconds,
		P50Ms:      pct(all, 0.50),
		P99Ms:      pct(all, 0.99),
		P50ReadMs:  pct(reads, 0.50),
		P99ReadMs:  pct(reads, 0.99),
		P50WriteMs: pct(writes, 0.50),
		P99WriteMs: pct(writes, 0.99),
	}, nil
}

func pct(lats []time.Duration, p float64) float64 {
	if len(lats) == 0 {
		return 0
	}
	idx := int(float64(len(lats)-1) * p)
	return float64(lats[idx].Microseconds()) / 1000
}

func runFailover(cmds []*exec.Cmd, addrs []string) (float64, error) {
	leaderIdx := -1
	for i, a := range addrs {
		st, err := getStatus(a)
		if err == nil && st.Role == "leader" {
			leaderIdx = i
			break
		}
	}
	if leaderIdx < 0 {
		return 0, fmt.Errorf("failover: 没有 leader")
	}
	cli := newClient("failover", addrs)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := cli.Put(ctx, "warm", "1"); err != nil {
		cancel()
		return 0, fmt.Errorf("failover warmup: %w", err)
	}
	cancel()

	t0 := time.Now()
	if err := cmds[leaderIdx].Process.Kill(); err != nil {
		return 0, err
	}
	var last error
	for time.Since(t0) < 15*time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := cli.Put(ctx, "fail", fmt.Sprintf("%d", time.Now().UnixNano()))
		cancel()
		if err == nil {
			return float64(time.Since(t0).Microseconds()) / 1000, nil
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	return 0, fmt.Errorf("failover 超时: %v", last)
}

func findLeader(addrs []string) (id, addr string, err error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, a := range addrs {
			st, err := getStatus(a)
			if err == nil && st.Role == "leader" {
				if st.LeaderAddr != "" {
					return st.ID, st.LeaderAddr, nil
				}
				return st.ID, a, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", "", fmt.Errorf("没有 leader")
}

func runFailoverLoad(addrs []string, clientsN int, readRatio float64, dist string, keys int, warmup time.Duration) (report, error) {
	if err := prefill(addrs, keys, min(8, clientsN)); err != nil {
		return report{}, err
	}
	var sampling atomic.Bool
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var bgErr atomic.Int64
	var bgOK atomic.Int64
	for i := 0; i < clientsN; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cli := newClient(fmt.Sprintf("load-%d", i), addrs)
			pick := newPick(dist, keys, int64(2000+i))
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := pick.next()
				read := readRatio > 0 && rand.Float64() < readRatio
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				var err error
				if read {
					_, err = cli.Get(ctx, key)
				} else {
					_, err = cli.Put(ctx, key, "x")
				}
				cancel()
				if !sampling.Load() {
					continue
				}
				if err != nil {
					bgErr.Add(1)
				} else {
					bgOK.Add(1)
				}
			}
		}(i)
	}
	time.Sleep(warmup)
	sampling.Store(true)
	// 确认负载已经打在 leader 上，再崩溃。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && bgOK.Load() < 10 {
		time.Sleep(20 * time.Millisecond)
	}
	leaderID, leaderAddr, err := findLeader(addrs)
	if err != nil {
		close(stop)
		wg.Wait()
		return report{}, err
	}
	t0 := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, leaderAddr+"/admin/crash", nil)
		if err != nil {
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		_ = err
	}()
	cli := newClient("failover", addrs)
	var last error
	var ms float64
	ok := false
	for time.Since(t0) < 20*time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := cli.Put(ctx, "fail", fmt.Sprintf("%d", time.Now().UnixNano()))
		cancel()
		// 崩溃请求和第一次 Put 会赛跑。写在旧 leader 上成功不算故障转移完成。
		if err == nil && res.Leader != "" && res.Leader != leaderID {
			ms = float64(time.Since(t0).Microseconds()) / 1000
			ok = true
			break
		}
		if err != nil {
			last = err
		} else {
			last = fmt.Errorf("写仍落在旧 leader %s", res.Leader)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 再留一小段，让后台负载在新 leader 上留下成功次数。
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	if !ok {
		return report{}, fmt.Errorf("failover-load 超时: %v", last)
	}
	return report{
		Mode:       "failover-load",
		Clients:    clientsN,
		Keys:       keys,
		ReadRatio:  readRatio,
		Ops:        int(bgOK.Load()),
		Errors:     int(bgErr.Load()),
		FailoverMs: ms,
		Note:       "负载进行中，从 POST /admin/crash 到新 leader 上第一次 Put 成功。Ops/Errors 是崩溃标记打开之后后台客户端的计数，含故障窗口",
	}, nil
}

// runFailoverWatch 在负载之外再发写，直到响应里的 leader 不再是 oldID。
// 先往 stderr 打 READY，调用方这时再 SIGKILL 旧 leader。成功时 stdout 一行 JSON，带墙上时钟。
func runFailoverWatch(addrs []string, oldID string) error {
	fmt.Fprintln(os.Stderr, "READY")
	cli := newClient("failover-watch", addrs)
	deadline := time.Now().Add(20 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := cli.Put(ctx, "fail", fmt.Sprintf("%d", time.Now().UnixNano()))
		cancel()
		if err == nil && res.Leader != "" && res.Leader != oldID {
			fmt.Printf("{\"success_unix_nano\":%d,\"leader\":%q}\n", time.Now().UnixNano(), res.Leader)
			return nil
		}
		if err != nil {
			last = err
		}
		time.Sleep(15 * time.Millisecond)
	}
	if last == nil {
		last = fmt.Errorf("没有等到新 leader")
	}
	return last
}

func dump(r report) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		log.Fatal(err)
	}
}
