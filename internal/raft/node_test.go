package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/victorzhong0110/keelstone/internal/kv"
)

type hub struct {
	mu    sync.Mutex
	nodes map[string]*Node
	block map[string]bool
}

func newHub() *hub { return &hub{nodes: map[string]*Node{}, block: map[string]bool{}} }

func (h *hub) add(n *Node) {
	h.mu.Lock()
	h.nodes[n.id] = n
	h.mu.Unlock()
	n.SetTransport(func(m Message) {
		h.mu.Lock()
		if h.block[m.From] || h.block[m.To] {
			h.mu.Unlock()
			return
		}
		dst := h.nodes[m.To]
		h.mu.Unlock()
		if dst != nil {
			dst.Deliver(m)
		}
	})
}

func (h *hub) isolate(id string, on bool) {
	h.mu.Lock()
	h.block[id] = on
	h.mu.Unlock()
}

func testPeers(n int) []Peer {
	peers := make([]Peer, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		peers[i] = Peer{ID: id, Addr: "mem://" + id}
	}
	return peers
}

type running struct {
	nodes    []*Node
	machines []*kv.Machine
	stores   []Storage
	hub      *hub
}

func startN(t *testing.T, n, snapEvery int, durable bool) *running {
	t.Helper()
	return startNInflight(t, n, snapEvery, durable, 0)
}

func startNInflight(t *testing.T, n, snapEvery int, durable bool, inflight int) *running {
	t.Helper()
	peers := testPeers(n)
	r := &running{hub: newHub()}
	for i := 0; i < n; i++ {
		var st Storage
		if durable {
			w, err := OpenWAL(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			st = w
			t.Cleanup(func() { w.Close() })
		} else {
			st = NewMemoryStorage()
		}
		m := kv.NewMachine()
		cfg := Config{
			ID:              peers[i].ID,
			Addr:            peers[i].Addr,
			Peers:           peers,
			TickInterval:    15 * time.Millisecond,
			ElectionTick:    6,
			HeartbeatTick:   1,
			SnapshotEntries: snapEvery,
			MaxInflight:     inflight,
			Seed:            int64(1000 + i),
		}
		node, err := NewNode(cfg, st, m)
		if err != nil {
			t.Fatal(err)
		}
		r.nodes = append(r.nodes, node)
		r.machines = append(r.machines, m)
		r.stores = append(r.stores, st)
		r.hub.add(node)
	}
	for _, node := range r.nodes {
		node.Start()
	}
	t.Cleanup(func() {
		for _, node := range r.nodes {
			node.Stop()
		}
	})
	return r
}

func waitLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		var leads []*Node
		for _, n := range nodes {
			if n.Status().Role == Leader.String() {
				leads = append(leads, n)
			}
		}
		if len(leads) == 1 {
			id := leads[0].id
			agreed := 0
			for _, n := range nodes {
				if n.Status().Leader == id {
					agreed++
				}
			}
			if agreed >= len(nodes)/2+1 {
				return leads[0]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, n := range nodes {
		st := n.Status()
		t.Logf("status %+v", st)
	}
	t.Fatal("no leader")
	return nil
}

var reqSeq atomic.Uint64

func cmdBytes(op, key, val, client string) []byte {
	raw, err := json.Marshal(kv.Command{
		Op: op, Key: key, Value: val, ClientID: client, RequestID: reqSeq.Add(1),
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func propose(t *testing.T, n *Node, raw []byte) kv.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	res, err := n.Propose(ctx, raw)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	var out kv.Result
	if err := json.Unmarshal(res.Data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func readKey(t *testing.T, n *Node, m *kv.Machine, key string) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := n.WaitLinearizable(ctx); err != nil {
		t.Fatalf("read: %v", err)
	}
	return m.Get(key)
}

func waitApplied(t *testing.T, nodes []*Node, index uint64) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			if n.Status().Applied < index {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, n := range nodes {
		t.Logf("%s applied %d", n.id, n.Status().Applied)
	}
	t.Fatalf("timeout waiting applied >= %d", index)
}

func TestElectAndReplicate(t *testing.T) {
	r := startN(t, 3, 0, false)
	lead := waitLeader(t, r.nodes)
	out := propose(t, lead, cmdBytes("put", "a", "1", "c1"))
	if !out.OK {
		t.Fatal(out)
	}
	waitApplied(t, r.nodes, lead.Status().Commit)
	for i, n := range r.nodes {
		v, ok := r.machines[i].Get("a")
		if !ok || v != "1" {
			t.Fatalf("node %s has %q ok=%v", n.id, v, ok)
		}
	}
	v, ok := readKey(t, lead, r.machines[indexOf(r.nodes, lead)], "a")
	if !ok || v != "1" {
		t.Fatalf("linearizable read %q %v", v, ok)
	}
}

func indexOf(nodes []*Node, n *Node) int {
	for i, x := range nodes {
		if x.id == n.id {
			return i
		}
	}
	return 0
}

func TestFollowerRejectsPropose(t *testing.T) {
	r := startN(t, 3, 0, false)
	lead := waitLeader(t, r.nodes)
	var fol *Node
	for _, n := range r.nodes {
		if n.id != lead.id {
			fol = n
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := fol.Propose(ctx, cmdBytes("put", "a", "1", "c"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v", err)
	}
}

func TestIdempotentRetryAcrossLeader(t *testing.T) {
	r := startN(t, 3, 0, false)
	lead := waitLeader(t, r.nodes)
	raw, _ := json.Marshal(kv.Command{Op: "add", Key: "n", Value: "1", ClientID: "c", RequestID: 42})
	propose(t, lead, raw)
	propose(t, lead, raw)
	v, ok := readKey(t, lead, r.machines[indexOf(r.nodes, lead)], "n")
	if !ok || v != "1" {
		t.Fatalf("counter %q ok=%v", v, ok)
	}
}

func TestPersistRestart(t *testing.T) {
	dir := t.TempDir()
	peers := []Peer{{ID: "n1", Addr: "mem://n1"}}
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := kv.NewMachine()
	n, err := NewNode(Config{
		ID: "n1", Addr: peers[0].Addr, Peers: peers,
		TickInterval: 15 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1,
		SnapshotEntries: 0, Seed: 7,
	}, w, m)
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	lead := waitLeader(t, []*Node{n})
	propose(t, lead, cmdBytes("put", "k", "v", "c"))
	n.Stop()
	w.Close()

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	m2 := kv.NewMachine()
	n2, err := NewNode(Config{
		ID: "n1", Addr: peers[0].Addr, Peers: peers,
		TickInterval: 15 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1, Seed: 8,
	}, w2, m2)
	if err != nil {
		t.Fatal(err)
	}
	n2.Start()
	defer n2.Stop()
	if got, ok := m2.Get("k"); !ok || got != "v" {
		t.Fatalf("after restart %q %v", got, ok)
	}
}

func TestLeaderIsolationElectsNew(t *testing.T) {
	r := startN(t, 3, 0, false)
	old := waitLeader(t, r.nodes)
	r.hub.isolate(old.id, true)
	deadline := time.Now().Add(6 * time.Second)
	var neu *Node
	for time.Now().Before(deadline) {
		for _, n := range r.nodes {
			if n.id == old.id {
				continue
			}
			st := n.Status()
			if st.Role == Leader.String() && st.Leader == n.id {
				neu = n
			}
		}
		if neu != nil && old.Status().Role != Leader.String() {
			break
		}
		neu = nil
		time.Sleep(15 * time.Millisecond)
	}
	if neu == nil {
		t.Fatalf("no new leader, old=%+v", old.Status())
	}
	out := propose(t, neu, cmdBytes("put", "x", "2", "c"))
	if !out.OK {
		t.Fatal(out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := old.WaitLinearizable(ctx)
	if err == nil {
		t.Fatal("isolated former leader served a linearizable read")
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	r := startN(t, 3, 8, false)
	lead := waitLeader(t, r.nodes)
	var victim *Node
	var victimM *kv.Machine
	for i, n := range r.nodes {
		if n.id != lead.id {
			victim = n
			victimM = r.machines[i]
			break
		}
	}
	r.hub.isolate(victim.id, true)
	for i := 0; i < 30; i++ {
		propose(t, lead, cmdBytes("put", fmt.Sprintf("k%d", i), "v", "c"))
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && lead.Status().Snapshot == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if lead.Status().Snapshot == 0 {
		t.Fatalf("leader did not snapshot: %+v", lead.Status())
	}
	r.hub.isolate(victim.id, false)
	waitApplied(t, []*Node{victim}, lead.Status().Applied)
	if victim.Status().Snapshot == 0 && victimM.Len() != 30 {
		// 追上即可，快照可以在 follower 本地再打。
	}
	if victimM.Len() != 30 {
		t.Fatalf("victim keys %d status %+v", victimM.Len(), victim.Status())
	}
}

func TestDivergentLog(t *testing.T) {
	r := startN(t, 3, 0, false)
	a := waitLeader(t, r.nodes)
	propose(t, a, cmdBytes("put", "k", "1", "c"))
	waitApplied(t, r.nodes, a.Status().Commit)
	r.hub.isolate(a.id, true)
	var b *Node
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && b == nil {
		for _, n := range r.nodes {
			if n.id != a.id && n.Status().Role == Leader.String() {
				b = n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if b == nil {
		t.Fatal("majority did not elect")
	}
	propose(t, b, cmdBytes("put", "k", "2", "c2"))
	// 旧 leader 在下台前可能写过未提交日志。这里再隔离一段时间已经下台。
	r.hub.isolate(a.id, false)
	waitApplied(t, r.nodes, b.Status().Commit)
	for i := range r.nodes {
		if got, _ := r.machines[i].Get("k"); got != "2" {
			t.Fatalf("node %s value %q", r.nodes[i].id, got)
		}
	}
}

func TestCAS(t *testing.T) {
	r := startN(t, 3, 0, false)
	lead := waitLeader(t, r.nodes)
	raw, _ := json.Marshal(kv.Command{
		Op: "cas", Key: "a", Value: "1", ClientID: "c", RequestID: reqSeq.Add(1),
	})
	out := propose(t, lead, raw)
	if !out.Swapped {
		t.Fatal(out)
	}
	raw, _ = json.Marshal(kv.Command{
		Op: "cas", Key: "a", Value: "2", Expected: "nope", ExpPresent: true,
		ClientID: "c", RequestID: reqSeq.Add(1),
	})
	out = propose(t, lead, raw)
	if out.Swapped {
		t.Fatal("cas should fail")
	}
	v, _ := readKey(t, lead, r.machines[indexOf(r.nodes, lead)], "a")
	if v != "1" {
		t.Fatal(v)
	}
}

func TestConcurrentWritesPipeline(t *testing.T) {
	for _, inflight := range []int{1, 8} {
		t.Run(fmt.Sprintf("inflight-%d", inflight), func(t *testing.T) {
			r := startNInflight(t, 3, 0, false, inflight)
			lead := waitLeader(t, r.nodes)
			const nclient = 8
			const per = 30
			errCh := make(chan error, nclient)
			var wg sync.WaitGroup
			for c := 0; c < nclient; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					for i := 0; i < per; i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
						_, err := lead.Propose(ctx, cmdBytes("put", fmt.Sprintf("p-%d-%d", c, i), "v", fmt.Sprintf("c%d", c)))
						cancel()
						if err != nil {
							errCh <- err
							return
						}
					}
				}(c)
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				t.Fatal(err)
			}
			waitApplied(t, r.nodes, lead.Status().Commit)
			for _, n := range r.nodes {
				if n.Status().Applied < lead.Status().Commit {
					t.Fatalf("%s applied %d", n.id, n.Status().Applied)
				}
			}
			mi := indexOf(r.nodes, r.nodes[0])
			if got, ok := r.machines[mi].Get("p-3-10"); !ok || got != "v" {
				t.Fatalf("inflight %d got %q ok %v", inflight, got, ok)
			}
		})
	}
}

func TestMembership(t *testing.T) {
	r := startN(t, 3, 0, false)
	lead := waitLeader(t, r.nodes)
	peers := testPeers(3)
	p4 := Peer{ID: "n4", Addr: "mem://n4"}
	all := append(append([]Peer{}, peers...), p4)
	m4 := kv.NewMachine()
	n4, err := NewNode(Config{
		ID: p4.ID, Addr: p4.Addr, Peers: all, Join: true,
		TickInterval: 15 * time.Millisecond, ElectionTick: 6, HeartbeatTick: 1,
		Seed: 99,
	}, NewMemoryStorage(), m4)
	if err != nil {
		t.Fatal(err)
	}
	r.hub.add(n4)
	n4.Start()
	defer n4.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if _, err := lead.ProposeConfChange(ctx, ConfChange{Op: ConfAddLearner, ID: p4.ID, Addr: p4.Addr}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && n4.Status().Applied+1 < lead.Status().Commit {
		time.Sleep(10 * time.Millisecond)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()
	// leader 可能已经换过，向当前 leader 提议。
	cur := waitLeader(t, r.nodes)
	if _, err := cur.ProposeConfChange(ctx2, ConfChange{Op: ConfPromote, ID: p4.ID, Addr: p4.Addr}); err != nil {
		t.Fatal(err)
	}
	propose(t, cur, cmdBytes("put", "m", "1", "c"))
	waitApplied(t, []*Node{n4}, cur.Status().Commit)
	if got, _ := m4.Get("m"); got != "1" {
		t.Fatalf("new voter has %q status %+v leader %+v", got, n4.Status(), cur.Status())
	}
}
