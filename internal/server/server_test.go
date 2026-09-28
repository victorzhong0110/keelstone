package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/victorzhong0110/keelstone/internal/client"
	"github.com/victorzhong0110/keelstone/internal/raft"
	"github.com/victorzhong0110/keelstone/internal/transport"
)

func TestHTTPPutGetCAS(t *testing.T) {
	const n = 3
	lns := make([]net.Listener, n)
	peers := make([]raft.Peer, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		peers[i] = raft.Peer{ID: fmt.Sprintf("n%d", i+1), Addr: "http://" + ln.Addr().String()}
	}
	servers := make([]*Server, n)
	for i := 0; i < n; i++ {
		s, err := New(raft.Config{
			ID: peers[i].ID, Addr: peers[i].Addr, Peers: peers,
			TickInterval: 15 * time.Millisecond, ElectionTick: 6, HeartbeatTick: 1,
			SnapshotEntries: 64, Seed: int64(50 + i),
		}, raft.NewMemoryStorage())
		if err != nil {
			t.Fatal(err)
		}
		tr := transport.New(s.Node().PeerAddr)
		s.Node().SetTransport(tr.Send)
		servers[i] = s
		go func(s *Server, ln net.Listener) {
			_ = s.Serve(ln)
		}(s, lns[i])
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Close()
		}
	})

	urls := make([]string, n)
	for i, p := range peers {
		urls[i] = p.Addr
	}
	cli := client.New("http-client", urls)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, "hello", "world"); err != nil {
		t.Fatal(err)
	}
	// 换一个起点，迫使客户端跟着 leader 提示走。
	cli2 := client.New("http-client-2", []string{urls[0], urls[1], urls[2]})
	got, err := cli2.Get(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Value != "world" {
		t.Fatalf("%+v", got)
	}
	sw, err := cli.CAS(ctx, "hello", "world", "raft", true)
	if err != nil || !sw.Swapped {
		t.Fatalf("cas %+v %v", sw, err)
	}
	if _, err := cli.Delete(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	got, err = cli.Get(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if got.Found {
		t.Fatalf("still present %+v", got)
	}
}
