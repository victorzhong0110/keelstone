// Package server 把 Raft 节点暴露成 HTTP。
// 对等复制走 POST /raft；客户端走 /v1/kv。没有引入 gRPC，是为了让报文和状态机
// 都可以用 curl 看清楚。消息结构本身和 RPC 一一对应，换成 gRPC 只是换传输。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/victorzhong0110/keelstone/internal/kv"
	"github.com/victorzhong0110/keelstone/internal/raft"
)

// Server 是一个 KV 副本进程。
type Server struct {
	node       *raft.Node
	mach       *kv.Machine
	http       *http.Server
	ln         net.Listener
	allowCrash bool
}

// New 恢复存储并创建节点，但还不会监听端口。
func New(cfg raft.Config, st raft.Storage) (*Server, error) {
	m := kv.NewMachine()
	n, err := raft.NewNode(cfg, st, m)
	if err != nil {
		return nil, err
	}
	s := &Server{node: n, mach: m}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/raft", s.handleRaft)
	mux.HandleFunc("/admin/status", s.handleStatus)
	mux.HandleFunc("/admin/snapshot", s.handleSnapshot)
	mux.HandleFunc("/admin/members", s.handleMembers)
	mux.HandleFunc("/admin/crash", s.handleCrash)
	mux.HandleFunc("/v1/kv/", s.handleKV)
	s.http = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s, nil
}

// Node 返回底层 Raft 节点，测试和传输层会用到。
func (s *Server) Node() *raft.Node { return s.node }

// EnableCrash 打开 POST /admin/crash。进程会立刻 SIGKILL 自己，用来在压测里制造 leader 崩溃。
// 默认关闭。
func (s *Server) EnableCrash() { s.allowCrash = true }

// Machine 返回状态机。
func (s *Server) Machine() *kv.Machine { return s.mach }

// Serve 在 ln 上提供 HTTP，并启动 Raft 循环。调用方需要事先 SetTransport。
func (s *Server) Serve(ln net.Listener) error {
	s.ln = ln
	s.node.Start()
	return s.http.Serve(ln)
}

// Close 停止 HTTP 和 Raft。
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := s.http.Shutdown(ctx)
	s.node.Stop()
	return err
}

func (s *Server) handleRaft(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var m raft.Message
	if err := json.Unmarshal(body, &m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.node.Deliver(m)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Status())
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	s.node.ForceSnapshot()
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}

func (s *Server) handleCrash(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !s.allowCrash {
		http.NotFound(w, r)
		return
	}
	// 不等待响应。和外部 SIGKILL 一样，日志停在上一次 fsync。
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
}

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var cc raft.ConfChange
	if err := json.NewDecoder(r.Body).Decode(&cc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	res, err := s.node.ProposeConfChange(ctx, cc)
	if err != nil {
		s.writeRaftErr(w, err, res)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "index": res.Index})
}

type kvBody struct {
	Value      string `json:"value"`
	Expected   string `json:"expected"`
	ExpPresent bool   `json:"exp_present"`
}

type kvResp struct {
	OK         bool   `json:"ok"`
	Found      bool   `json:"found"`
	Value      string `json:"value,omitempty"`
	Swapped    bool   `json:"swapped"`
	Prev       string `json:"prev,omitempty"`
	PrevFound  bool   `json:"prev_found"`
	Index      uint64 `json:"index,omitempty"`
	Leader     string `json:"leader,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
	key := path
	op := ""
	if strings.HasSuffix(path, "/cas") {
		key = strings.TrimSuffix(path, "/cas")
		op = "cas"
	}
	key = strings.Trim(key, "/")
	if key == "" {
		http.Error(w, "empty key", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handleWrite(w, r, "put", key)
	case http.MethodDelete:
		s.handleWrite(w, r, "delete", key)
	case http.MethodPost:
		if op != "cas" {
			http.Error(w, "use POST /v1/kv/{key}/cas", http.StatusMethodNotAllowed)
			return
		}
		s.handleWrite(w, r, "cas", key)
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.node.WaitLinearizable(ctx); err != nil {
		s.writeRaftErr(w, err, raft.ApplyResult{})
		return
	}
	v, ok := s.mach.Get(key)
	st := s.node.Status()
	writeJSON(w, http.StatusOK, kvResp{OK: true, Found: ok, Value: v, Leader: st.Leader, LeaderAddr: st.LeaderAddr})
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, op, key string) {
	var body kvBody
	if r.ContentLength != 0 {
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	clientID := r.Header.Get("X-Client-Id")
	if clientID == "" {
		http.Error(w, "X-Client-Id required", http.StatusBadRequest)
		return
	}
	reqStr := r.Header.Get("X-Request-Id")
	reqID, err := strconv.ParseUint(reqStr, 10, 64)
	if err != nil || reqID == 0 {
		http.Error(w, "X-Request-Id required", http.StatusBadRequest)
		return
	}
	raw, err := json.Marshal(kv.Command{
		Op: op, Key: key, Value: body.Value, Expected: body.Expected,
		ExpPresent: body.ExpPresent, ClientID: clientID, RequestID: reqID,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	res, err := s.node.Propose(ctx, raw)
	if err != nil {
		s.writeRaftErr(w, err, res)
		return
	}
	var out kv.Result
	if err := json.Unmarshal(res.Data, &out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	code := http.StatusOK
	if out.Error != "" {
		code = http.StatusBadRequest
	}
	st := s.node.Status()
	writeJSON(w, code, kvResp{
		OK: out.OK, Found: out.Found, Value: out.Value, Swapped: out.Swapped,
		Prev: out.Prev, PrevFound: out.PrevFound, Index: res.Index,
		Leader: st.Leader, LeaderAddr: st.LeaderAddr, Error: out.Error,
	})
}

func (s *Server) writeRaftErr(w http.ResponseWriter, err error, res raft.ApplyResult) {
	st := s.node.Status()
	leader, addr := res.Leader, res.LeaderAddr
	if leader == "" {
		leader, addr = st.Leader, st.LeaderAddr
	}
	code := http.StatusServiceUnavailable
	msg := err.Error()
	if errors.Is(err, raft.ErrNotLeader) {
		code = http.StatusConflict
		msg = raft.ErrNotLeader.Error()
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = http.StatusGatewayTimeout
	}
	writeJSON(w, code, kvResp{Error: msg, Leader: leader, LeaderAddr: addr})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
