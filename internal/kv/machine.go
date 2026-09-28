// Package kv 是 Raft 之上的确定性状态机。
// 所有写操作都带 client_id + request_id。同一个请求号只会生效一次，
// 因此客户端在超时后用同一个号重试是安全的：要么拿到第一次的结果，要么补做一次。
package kv

import (
	"encoding/json"
	"strconv"
	"sync"
)

// Command 是写入 Raft 日志的一条 KV 操作。Get 不走日志，走 ReadIndex。
type Command struct {
	Op         string `json:"op"` // put | delete | cas | add
	Key        string `json:"key"`
	Value      string `json:"value,omitempty"`
	Expected   string `json:"expected,omitempty"`
	ExpPresent bool   `json:"exp_present"`
	ClientID   string `json:"client_id"`
	RequestID  uint64 `json:"request_id"`
}

// Result 是状态机应用一条命令后的确定性输出，会返回给当时的 leader 客户端，
// 也会按 request_id 缓存，供重试时原样返回。
type Result struct {
	OK        bool   `json:"ok"`
	Found     bool   `json:"found"`
	Value     string `json:"value,omitempty"`
	Swapped   bool   `json:"swapped"`
	Prev      string `json:"prev,omitempty"`
	PrevFound bool   `json:"prev_found"`
	Error     string `json:"error,omitempty"`
}

type session struct {
	LastID uint64            `json:"last_id"`
	Result Result            `json:"result"`
	All    map[uint64]Result `json:"all,omitempty"`
}

// snapshotFile 是快照里的状态机部分。成员关系由 raft.Snapshot 另存。
type snapshotFile struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

// Machine 只由 Raft 事件循环调用 Apply/Snapshot/Restore，
// Get 可以在 ReadIndex 返回之后由其他 goroutine 调用，因此这里用互斥锁。
type Machine struct {
	mu       sync.Mutex
	data     map[string]string
	sessions map[string]session
}

func NewMachine() *Machine {
	return &Machine{
		data:     map[string]string{},
		sessions: map[string]session{},
	}
}

// Apply 执行一条已经提交的命令。重复的 request_id 直接返回缓存，不修改数据。
func (m *Machine) Apply(_ uint64, raw []byte) []byte {
	var cmd Command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return mustJSON(Result{Error: "bad command"})
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if cmd.ClientID != "" {
		s := m.sessions[cmd.ClientID]
		if s.All == nil {
			s.All = map[uint64]Result{}
		}
		// 同一个请求号永远返回第一次的结果，包括比 LastID 更早、但仍然留在快照里的请求。
		if prev, ok := s.All[cmd.RequestID]; ok {
			return mustJSON(prev)
		}
		if s.LastID != 0 && cmd.RequestID < s.LastID {
			return mustJSON(Result{Error: "stale request"})
		}
		res := m.execLocked(cmd)
		s.All[cmd.RequestID] = res
		s.LastID = cmd.RequestID
		s.Result = res
		m.sessions[cmd.ClientID] = s
		return mustJSON(res)
	}
	res := m.execLocked(cmd)
	return mustJSON(res)
}

func (m *Machine) execLocked(cmd Command) Result {
	switch cmd.Op {
	case "put":
		if cmd.Key == "" {
			return Result{Error: "empty key"}
		}
		m.data[cmd.Key] = cmd.Value
		return Result{OK: true, Found: true, Value: cmd.Value}
	case "delete":
		if cmd.Key == "" {
			return Result{Error: "empty key"}
		}
		_, found := m.data[cmd.Key]
		delete(m.data, cmd.Key)
		return Result{OK: true, Found: found}
	case "cas":
		if cmd.Key == "" {
			return Result{Error: "empty key"}
		}
		prev, found := m.data[cmd.Key]
		match := (!cmd.ExpPresent && !found) || (cmd.ExpPresent && found && prev == cmd.Expected)
		res := Result{OK: true, Prev: prev, PrevFound: found, Found: found}
		if !match {
			return res
		}
		m.data[cmd.Key] = cmd.Value
		res.Swapped = true
		res.Value = cmd.Value
		res.Found = true
		return res
	case "add":
		// add 用来演示幂等：同一个 request_id 不会把计数器加两次。
		if cmd.Key == "" {
			return Result{Error: "empty key"}
		}
		delta, err := strconv.ParseInt(cmd.Value, 10, 64)
		if err != nil {
			return Result{Error: "bad delta"}
		}
		cur, _ := strconv.ParseInt(m.data[cmd.Key], 10, 64)
		cur += delta
		m.data[cmd.Key] = strconv.FormatInt(cur, 10)
		return Result{OK: true, Found: true, Value: m.data[cmd.Key]}
	default:
		return Result{Error: "unknown op"}
	}
}

// Recall 报告这个客户端请求号是否已经应用到状态机。
// 线性一致性测试用它区分「超时但其实提交了」和「确实没提交」。
func (m *Machine) Recall(clientID string, requestID uint64) (Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[clientID]
	if !ok || s.All == nil {
		return Result{}, false
	}
	r, ok := s.All[requestID]
	return r, ok
}

// Get 是本地读。线性一致性由调用方先完成 ReadIndex 来保证。
func (m *Machine) Get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok
}

// Len 返回当前键的数量，测试用来等副本追上。
func (m *Machine) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

// Clone 返回数据的副本，便于测试比较多副本是否一致。
func (m *Machine) Clone() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out
}

func (m *Machine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	body := snapshotFile{
		Data:     make(map[string]string, len(m.data)),
		Sessions: make(map[string]session, len(m.sessions)),
	}
	for k, v := range m.data {
		body.Data[k] = v
	}
	for k, v := range m.sessions {
		body.Sessions[k] = v
	}
	return json.Marshal(body)
}

func (m *Machine) Restore(raw []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(raw) == 0 {
		m.data = map[string]string{}
		m.sessions = map[string]session{}
		return nil
	}
	var body snapshotFile
	if err := json.Unmarshal(raw, &body); err != nil {
		return err
	}
	if body.Data == nil {
		body.Data = map[string]string{}
	}
	if body.Sessions == nil {
		body.Sessions = map[string]session{}
	}
	m.data = body.Data
	m.sessions = body.Sessions
	return nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"error":"marshal"}`)
	}
	return b
}
