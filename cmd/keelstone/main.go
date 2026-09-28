// Command keelstone 启动一个 Raft KV 副本。
//
//	keelstone --id n1 --listen :43121 --advertise http://127.0.0.1:43121 \
//	  --data ./data/n1 \
//	  --peers n1=http://127.0.0.1:43121,n2=http://127.0.0.1:43122,n3=http://127.0.0.1:43123
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/victorzhong0110/keelstone/internal/raft"
	"github.com/victorzhong0110/keelstone/internal/server"
	"github.com/victorzhong0110/keelstone/internal/transport"
)

func main() {
	id := flag.String("id", "", "本节点 id")
	listen := flag.String("listen", ":43121", "监听地址")
	advertise := flag.String("advertise", "", "其他节点访问本节点的 URL，默认取 peers 里自己的地址")
	data := flag.String("data", "", "WAL 目录")
	peersFlag := flag.String("peers", "", "id=http://host:port，逗号分隔，必须包含本节点")
	join := flag.Bool("join", false, "以 learner 身份启动，等待被加入集群")
	tick := flag.Duration("tick", 40_000_000, "Raft tick 间隔（默认 40ms）")
	election := flag.Int("election-tick", 10, "选举超时的 tick 基数，实际超时在 [n, 2n) 之间随机")
	heartbeat := flag.Int("heartbeat-tick", 1, "心跳间隔，单位是 tick")
	snapEvery := flag.Int("snapshot-entries", 1024, "日志超过这么多条就打快照；0 表示关闭")
	inflight := flag.Int("max-inflight", 8, "每个 follower 的在途 AppendEntries 上限；1 关闭流水线")
	adminCrash := flag.Bool("admin-crash", false, "允许 POST /admin/crash 立刻 SIGKILL 本进程")
	flag.Parse()

	if *id == "" || *data == "" || *peersFlag == "" {
		log.Fatal("需要 --id --data --peers")
	}
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatal(err)
	}
	addr := *advertise
	if addr == "" {
		for _, p := range peers {
			if p.ID == *id {
				addr = p.Addr
			}
		}
	}
	if addr == "" {
		log.Fatal("peers 里没有本节点，或者没有 --advertise")
	}
	wal, err := raft.OpenWAL(*data)
	if err != nil {
		log.Fatal(err)
	}
	defer wal.Close()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	cfg := raft.Config{
		ID: *id, Addr: addr, Peers: peers, Join: *join,
		TickInterval: *tick, ElectionTick: *election, HeartbeatTick: *heartbeat,
		SnapshotEntries: *snapEvery, MaxInflight: *inflight,
	}
	srv, err := server.New(cfg, wal)
	if err != nil {
		log.Fatal(err)
	}
	if *adminCrash {
		srv.EnableCrash()
	}
	tr := transport.New(srv.Node().PeerAddr)
	srv.Node().SetTransport(tr.Send)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	log.Printf("keelstone %s listening %s advertise %s", *id, ln.Addr(), addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil {
			log.Fatal(err)
		}
	case <-sig:
		log.Printf("shutting down %s", *id)
		if err := srv.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func parsePeers(s string) ([]raft.Peer, error) {
	var out []raft.Peer
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, errString("peer 格式应为 id=http://host:port: " + part)
		}
		out = append(out, raft.Peer{ID: id, Addr: addr})
	}
	if len(out) == 0 {
		return nil, errString("空的 peers")
	}
	return out, nil
}

type errString string

func (e errString) Error() string { return string(e) }
