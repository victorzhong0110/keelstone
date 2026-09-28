package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/victorzhong0110/keelstone/internal/kv"
	"github.com/victorzhong0110/keelstone/internal/lincheck"
	"github.com/victorzhong0110/keelstone/internal/raft"
)

// FaultNet 是进程内的不可靠网络：分区、丢包、延迟、乱序、以及「这个节点已经崩溃」。
type FaultNet struct {
	mu       sync.Mutex
	cur      map[string]*raft.Node
	crashed  map[string]bool
	group    map[string]int
	drop     float64
	delayMin time.Duration
	delayMax time.Duration
	reorder  bool
	rng      *rand.Rand
}

func NewFaultNet(seed int64) *FaultNet {
	return &FaultNet{
		cur:     map[string]*raft.Node{},
		crashed: map[string]bool{},
		group:   map[string]int{},
		rng:     rand.New(rand.NewSource(seed)),
	}
}

func (f *FaultNet) setNode(id string, n *raft.Node) {
	f.mu.Lock()
	f.cur[id] = n
	f.mu.Unlock()
}

func (f *FaultNet) setCrashed(id string, on bool) {
	f.mu.Lock()
	f.crashed[id] = on
	f.mu.Unlock()
}

// Isolate 把一个节点放进另一侧分区。Heal 恢复全连通并清掉丢包和延迟。
func (f *FaultNet) Isolate(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.group {
		f.group[k] = 0
	}
	f.group[id] = 1
}

func (f *FaultNet) Heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.group {
		f.group[k] = 0
	}
	f.drop = 0
	f.delayMin, f.delayMax = 0, 0
	f.reorder = false
}

func (f *FaultNet) SetDrop(p float64) {
	f.mu.Lock()
	f.drop = p
	f.mu.Unlock()
}

func (f *FaultNet) SetDelay(min, max time.Duration) {
	f.mu.Lock()
	f.delayMin, f.delayMax = min, max
	f.mu.Unlock()
}

func (f *FaultNet) SetReorder(on bool) {
	f.mu.Lock()
	f.reorder = on
	f.mu.Unlock()
}

// Send 是 Raft 传输回调。延迟投递放在别的 goroutine 里，避免堵住事件循环。
func (f *FaultNet) Send(m raft.Message) {
	f.mu.Lock()
	if f.crashed[m.From] || f.crashed[m.To] || f.group[m.From] != f.group[m.To] {
		f.mu.Unlock()
		return
	}
	if f.drop > 0 && f.rng.Float64() < f.drop {
		f.mu.Unlock()
		return
	}
	delay := f.delayMin
	if f.delayMax > f.delayMin {
		delay += time.Duration(f.rng.Int63n(int64(f.delayMax - f.delayMin + 1)))
	}
	if f.reorder && f.rng.Intn(2) == 0 {
		delay += time.Duration(1+f.rng.Intn(20)) * time.Millisecond
	}
	dst := f.cur[m.To]
	f.mu.Unlock()
	if dst == nil {
		return
	}
	deliver := func() {
		f.mu.Lock()
		crashed := f.crashed[m.To]
		node := f.cur[m.To]
		f.mu.Unlock()
		if crashed || node == nil {
			return
		}
		node.Deliver(m)
	}
	if delay <= 0 {
		deliver()
		return
	}
	time.AfterFunc(delay, deliver)
}

// Cluster 是一组共享 FaultNet 的节点，存储对象在崩溃后保留。
type Cluster struct {
	t        *testing.T
	ids      []string
	peers    []raft.Peer
	net      *FaultNet
	mu       sync.Mutex
	nodes    map[string]*raft.Node
	machines map[string]*kv.Machine
	stores   map[string]raft.Storage
	cfgs     map[string]raft.Config
}

func newCluster(t *testing.T, n int, seed int64) *Cluster {
	t.Helper()
	c := &Cluster{
		t:        t,
		net:      NewFaultNet(seed),
		nodes:    map[string]*raft.Node{},
		machines: map[string]*kv.Machine{},
		stores:   map[string]raft.Storage{},
		cfgs:     map[string]raft.Config{},
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		c.ids = append(c.ids, id)
		c.peers = append(c.peers, raft.Peer{ID: id, Addr: "mem://" + id})
	}
	for i, id := range c.ids {
		cfg := raft.Config{
			ID: id, Addr: c.peers[i].Addr, Peers: c.peers,
			TickInterval: 12 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1,
			SnapshotEntries: 24, Seed: seed + int64(i)*17,
		}
		c.cfgs[id] = cfg
		c.stores[id] = raft.NewMemoryStorage()
		c.start(id)
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *Cluster) start(id string) {
	m := kv.NewMachine()
	n, err := raft.NewNode(c.cfgs[id], c.stores[id], m)
	if err != nil {
		c.t.Fatal(err)
	}
	n.SetTransport(c.net.Send)
	c.mu.Lock()
	c.nodes[id] = n
	c.machines[id] = m
	c.mu.Unlock()
	c.net.setNode(id, n)
	c.net.setCrashed(id, false)
	n.Start()
}

func (c *Cluster) stopAll() {
	c.mu.Lock()
	nodes := make([]*raft.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
}

func (c *Cluster) Crash(id string) {
	c.mu.Lock()
	n := c.nodes[id]
	c.mu.Unlock()
	c.net.setCrashed(id, true)
	if n != nil {
		n.Stop()
	}
}

func (c *Cluster) Restart(id string) {
	c.mu.Lock()
	old := c.nodes[id]
	c.mu.Unlock()
	c.net.setCrashed(id, true)
	if old != nil {
		old.Stop()
	}
	c.start(id)
}

type replica struct {
	n *raft.Node
	m *kv.Machine
}

func (c *Cluster) pick(rng *rand.Rand) replica {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.ids[rng.Intn(len(c.ids))]
	return replica{n: c.nodes[id], m: c.machines[id]}
}

func (c *Cluster) WaitConverged() error {
	deadline := time.Now().Add(8 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		c.mu.Lock()
		nodes := make([]*raft.Node, 0, len(c.ids))
		machs := make([]*kv.Machine, 0, len(c.ids))
		for _, id := range c.ids {
			nodes = append(nodes, c.nodes[id])
			machs = append(machs, c.machines[id])
		}
		c.mu.Unlock()
		ok, detail := converged(nodes, machs)
		last = detail
		if ok {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("not converged: %s", last)
}

func converged(nodes []*raft.Node, machs []*kv.Machine) (bool, string) {
	if len(nodes) == 0 {
		return false, "empty"
	}
	leaders := 0
	var applied uint64
	same := true
	parts := make([]string, 0, len(nodes))
	for i, n := range nodes {
		st := n.Status()
		parts = append(parts, fmt.Sprintf("%s role=%s term=%d commit=%d applied=%d lead=%s", st.ID, st.Role, st.Term, st.Commit, st.Applied, st.Leader))
		if st.Role == raft.Leader.String() {
			leaders++
		}
		if i == 0 {
			applied = st.Applied
		} else if st.Applied != applied {
			same = false
		}
	}
	if leaders != 1 || !same {
		return false, fmt.Sprint(parts)
	}
	base := machs[0].Clone()
	for _, m := range machs[1:] {
		if fmt.Sprint(m.Clone()) != fmt.Sprint(base) {
			return false, "state mismatch " + fmt.Sprint(parts)
		}
	}
	return true, ""
}

type recorded struct {
	op      porcupine.Operation
	client  string
	req     uint64
	mutate  bool
	unknown bool
}

func TestFaultLinearizable(t *testing.T) {
	iters := 3
	if testing.Short() {
		iters = 1
	}
	if v := os.Getenv("KEELSTONE_FAULT_ITERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatal(v)
		}
		iters = n
	}
	passed := 0
	for i := 0; i < iters; i++ {
		seed := time.Now().UnixNano() + int64(i)*10007
		if err := runFaultOnce(t, seed); err != nil {
			t.Fatalf("iter %d seed %d: %v", i, seed, err)
		}
		passed++
		t.Logf("fault iter %d/%d seed %d ok", i+1, iters, seed)
	}
	t.Logf("fault_passed=%d fault_total=%d", passed, iters)
}

func runFaultOnce(t *testing.T, seed int64) error {
	c := newCluster(t, 3, seed)
	defer c.stopAll()
	rng := rand.New(rand.NewSource(seed))
	var mu sync.Mutex
	var hist []recorded
	var successes atomicInt
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for id := 0; id < 3; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientLoop(c, id, rand.New(rand.NewSource(seed+int64(id+1))), stop, &mu, &hist, &successes)
		}(id)
	}
	crashed := map[string]bool{}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		id := c.ids[rng.Intn(len(c.ids))]
		switch rng.Intn(7) {
		case 0:
			c.net.Isolate(id)
		case 1:
			c.net.Heal()
		case 2:
			c.net.SetDrop(0.15 + rng.Float64()*0.2)
		case 3:
			c.net.SetDelay(time.Millisecond, time.Duration(5+rng.Intn(25))*time.Millisecond)
		case 4:
			c.net.SetReorder(true)
		case 5:
			if len(crashed) >= 1 {
				for down := range crashed {
					c.Restart(down)
					delete(crashed, down)
					break
				}
			} else {
				c.Crash(id)
				crashed[id] = true
			}
		default:
			c.net.Heal()
			if len(crashed) > 0 {
				for down := range crashed {
					c.Restart(down)
					delete(crashed, down)
				}
			}
		}
		time.Sleep(time.Duration(70+rng.Intn(90)) * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	c.net.Heal()
	for down := range crashed {
		c.Restart(down)
	}
	if err := c.WaitConverged(); err != nil {
		return err
	}
	// 静止之后再做一轮确定的读写，确认集群还能服务，并放进历史。
	if err := finalOps(c, &mu, &hist, &successes); err != nil {
		return err
	}
	if successes.v < 5 {
		return fmt.Errorf("only %d successful ops", successes.v)
	}
	resolved := resolve(c, hist)
	res := porcupine.CheckOperationsTimeout(lincheck.Model(), resolved, 8*time.Second)
	if res != porcupine.Ok {
		return fmt.Errorf("porcupine %s ops=%d", res, len(resolved))
	}
	return nil
}

type atomicInt struct {
	mu sync.Mutex
	v  int
}

func (a *atomicInt) inc() {
	a.mu.Lock()
	a.v++
	a.mu.Unlock()
}

func clientLoop(c *Cluster, id int, rng *rand.Rand, stop <-chan struct{}, mu *sync.Mutex, hist *[]recorded, okN *atomicInt) {
	keys := []string{"a", "b", "c", "d"}
	clientID := fmt.Sprintf("c%d", id)
	var seq uint64
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := keys[rng.Intn(len(keys))]
		kind := lincheck.Kind(rng.Intn(4))
		seq++
		switch kind {
		case lincheck.OpGet:
			doGet(c, id, rng, clientID, key, mu, hist, okN)
		case lincheck.OpPut:
			doMut(c, id, rng, clientID, seq, lincheck.OpPut, key, fmt.Sprintf("v%d-%d", id, seq), "", false, mu, hist, okN)
		case lincheck.OpDelete:
			doMut(c, id, rng, clientID, seq, lincheck.OpDelete, key, "", "", false, mu, hist, okN)
		default:
			doMut(c, id, rng, clientID, seq, lincheck.OpCAS, key, "cas", "nope", true, mu, hist, okN)
		}
	}
}

func doGet(c *Cluster, id int, rng *rand.Rand, clientID, key string, mu *sync.Mutex, hist *[]recorded, okN *atomicInt) {
	start := time.Now().UnixNano()
	deadline := time.Now().Add(350 * time.Millisecond)
	var val string
	var found bool
	success := false
	for time.Now().Before(deadline) {
		rep := c.pick(rng)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err := rep.n.WaitLinearizable(ctx)
		cancel()
		if err != nil {
			continue
		}
		val, found = rep.m.Get(key)
		success = true
		break
	}
	if !success {
		return
	}
	end := time.Now().UnixNano()
	if end <= start {
		end = start + 1
	}
	okN.inc()
	mu.Lock()
	*hist = append(*hist, recorded{op: porcupine.Operation{
		ClientId: id,
		Input:    lincheck.Input{Op: lincheck.OpGet, Key: key},
		Output:   lincheck.Output{OK: true, Found: found, Value: val},
		Call:     start,
		Return:   end,
	}})
	mu.Unlock()
	_ = clientID
}

func doMut(c *Cluster, id int, rng *rand.Rand, clientID string, seq uint64, kind lincheck.Kind, key, val, exp string, expPresent bool, mu *sync.Mutex, hist *[]recorded, okN *atomicInt) {
	opName := "put"
	switch kind {
	case lincheck.OpDelete:
		opName = "delete"
	case lincheck.OpCAS:
		opName = "cas"
	}
	raw, _ := json.Marshal(kv.Command{
		Op: opName, Key: key, Value: val, Expected: exp, ExpPresent: expPresent,
		ClientID: clientID, RequestID: seq,
	})
	start := time.Now().UnixNano()
	deadline := time.Now().Add(400 * time.Millisecond)
	var out kv.Result
	success := false
	for time.Now().Before(deadline) {
		rep := c.pick(rng)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		res, err := rep.n.Propose(ctx, raw)
		cancel()
		if err != nil {
			continue
		}
		if err := json.Unmarshal(res.Data, &out); err != nil {
			continue
		}
		success = true
		break
	}
	end := time.Now().UnixNano()
	if end <= start {
		end = start + 1
	}
	rec := recorded{
		client: clientID,
		req:    seq,
		mutate: true,
		op: porcupine.Operation{
			ClientId: id,
			Input: lincheck.Input{
				Op: kind, Key: key, Value: val, Expected: exp, ExpPresent: expPresent,
			},
			Call:   start,
			Return: end,
		},
	}
	if success {
		okN.inc()
		rec.op.Output = lincheck.Output{
			OK: out.OK, Found: out.Found, Value: out.Value,
			Swapped: out.Swapped, Prev: out.Prev, PrevFound: out.PrevFound,
		}
	} else {
		rec.unknown = true
		rec.op.Output = lincheck.Output{Unknown: true}
	}
	mu.Lock()
	*hist = append(*hist, rec)
	mu.Unlock()
}

func finalOps(c *Cluster, mu *sync.Mutex, hist *[]recorded, okN *atomicInt) error {
	deadline := time.Now().Add(4 * time.Second)
	var lead *raft.Node
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, n := range c.nodes {
			if n.Status().Role == raft.Leader.String() {
				lead = n
			}
		}
		c.mu.Unlock()
		if lead != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lead == nil {
		return fmt.Errorf("no leader after heal")
	}
	start := time.Now().UnixNano()
	raw, _ := json.Marshal(kv.Command{Op: "put", Key: "z", Value: "final", ClientID: "final", RequestID: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := lead.Propose(ctx, raw)
	if err != nil {
		return fmt.Errorf("final put: %w", err)
	}
	var out kv.Result
	if err := json.Unmarshal(res.Data, &out); err != nil {
		return err
	}
	end := time.Now().UnixNano()
	okN.inc()
	mu.Lock()
	*hist = append(*hist, recorded{op: porcupine.Operation{
		ClientId: 0,
		Input:    lincheck.Input{Op: lincheck.OpPut, Key: "z", Value: "final"},
		Output:   lincheck.Output{OK: true, Found: true, Value: out.Value},
		Call:     start,
		Return:   end,
	}})
	mu.Unlock()
	return nil
}

func resolve(c *Cluster, hist []recorded) []porcupine.Operation {
	c.mu.Lock()
	var mach *kv.Machine
	for _, m := range c.machines {
		mach = m
		break
	}
	c.mu.Unlock()
	now := time.Now().UnixNano()
	var out []porcupine.Operation
	for _, rec := range hist {
		if !rec.unknown {
			out = append(out, rec.op)
			continue
		}
		res, ok := mach.Recall(rec.client, rec.req)
		if !ok {
			// 静止之后所有副本都没有这条请求，说明它没有提交。
			continue
		}
		op := rec.op
		op.Output = lincheck.Output{
			OK: res.OK, Found: res.Found, Value: res.Value,
			Swapped: res.Swapped, Prev: res.Prev, PrevFound: res.PrevFound,
		}
		if now <= op.Call {
			now = op.Call + 1
		}
		op.Return = now
		out = append(out, op)
	}
	return out
}
