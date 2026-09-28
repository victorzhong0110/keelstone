// Package raft 是一份原创的 Raft 实现，不封装 etcd 或 Hashicorp 的库。
//
// 算法依据是 Ongaro & Ousterhout 的论文
// "In Search of an Understandable Consensus Algorithm" 以及学位论文中的
// 成员变更、PreVote、ReadIndex。接口形态参考了 etcd raft 的「单线程状态机 +
// 外部负责持久化与网络」这一分层思想，但类型、控制流和存储格式都是独立编写的，
// 没有复制 etcd、Hashicorp 或 TinyKV 的源码。
package raft

import "fmt"

// Role 是节点在某一任期内的角色。PreCandidate 对应学位论文 9.6 节的 PreVote：
// 在真正自增 term 之前先探询，避免分区节点把集群的 term 抬高。
type Role int

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", int(r))
	}
}

// EntryType 区分复制到日志里的记录。
// 只有 EntryCommand 会进入 KV 状态机；EntryConfig 改变投票成员；
// EntryNoop 是 leader 上任后追加的空记录，用来满足「只提交当前任期的日志」这一条。
type EntryType uint8

const (
	EntryCommand EntryType = iota + 1
	EntryNoop
	EntryConfig
)

// Entry 是 Raft 日志中的一条记录。Index 从 1 开始，0 表示「日志还是空的」。
type Entry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data,omitempty"`
}

// Peer 是集群中的一个进程。Addr 是它的 HTTP 根地址，内存网络测试里可以是占位符。
type Peer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// ConfChange 是单步成员变更。同一时刻只允许一条未提交的配置日志，
// 这是论文里「一次只增删一台」能够省去 joint consensus 的前提。
type ConfChange struct {
	Op   string `json:"op"` // add_learner | promote | remove
	ID   string `json:"id"`
	Addr string `json:"addr,omitempty"`
}

const (
	ConfAddLearner = "add_learner"
	ConfPromote    = "promote"
	ConfRemove     = "remove"
)

// HardState 是必须在响应 RPC 之前落盘的易失任期状态。
// Commit 不是论文 Figure 2 的持久化字段，但把它写进 HardState 后，
// 重启可以直接重放 (snapshot, commit] 而不是干等新 leader 的心跳。
type HardState struct {
	Term   uint64 `json:"term"`
	Vote   string `json:"vote"`
	Commit uint64 `json:"commit"`
}

// Snapshot 是某一日志下标处的状态机镜像，外加当时已经生效的成员关系。
// 快照下标及之前的日志都可以丢掉。
type Snapshot struct {
	Index    uint64 `json:"index"`
	Term     uint64 `json:"term"`
	Voters   []Peer `json:"voters,omitempty"`
	Learners []Peer `json:"learners,omitempty"`
	Data     []byte `json:"data,omitempty"`
}

// MsgType 是节点之间交换的消息。传输层只负责投递，不解释语义。
type MsgType int

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgPreVote
	MsgPreVoteResp
	MsgApp
	MsgAppResp
	MsgSnap
	MsgSnapResp
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgPreVote:
		return "PreVote"
	case MsgPreVoteResp:
		return "PreVoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	case MsgSnap:
		return "Snap"
	case MsgSnapResp:
		return "SnapResp"
	default:
		return fmt.Sprintf("msg(%d)", int(t))
	}
}

// Message 覆盖选举、复制和快照。字段复用是为了让 JSON 传输保持扁平：
//
//	Vote/PreVote: Index/LogTerm 是候选人最后一条日志
//	App:          Index/LogTerm 是 prevLogIndex/prevLogTerm，Entries 是新日志
//	AppResp:      成功时 Index 是与 leader 对齐的最后下标；失败时 RejectHint 是建议的 nextIndex
//	Probe:        leader 发出的这一轮复制序号，用来丢弃过期响应
//	Context:      选举轮次，或 ReadIndex 的心跳轮次
type Message struct {
	Type         MsgType  `json:"type"`
	From         string   `json:"from"`
	To           string   `json:"to"`
	Term         uint64   `json:"term"`
	Index        uint64   `json:"index"`
	LogTerm      uint64   `json:"log_term"`
	Entries      []Entry  `json:"entries,omitempty"`
	Commit       uint64   `json:"commit"`
	Reject       bool     `json:"reject"`
	RejectHint   uint64   `json:"reject_hint"`
	ConflictTerm uint64   `json:"conflict_term"`
	Probe        uint64   `json:"probe"`
	Context      uint64   `json:"context"`
	Snapshot     Snapshot `json:"snapshot,omitempty"`
}

// Status 是对外发布的只读快照，HTTP 和测试可以并发读取。
type Status struct {
	ID         string            `json:"id"`
	Role       string            `json:"role"`
	Term       uint64            `json:"term"`
	Leader     string            `json:"leader"`
	LeaderAddr string            `json:"leader_addr,omitempty"`
	Commit     uint64            `json:"commit"`
	Applied    uint64            `json:"applied"`
	LastIndex  uint64            `json:"last_index"`
	Snapshot   uint64            `json:"snapshot"`
	Voters     []string          `json:"voters"`
	Learners   []string          `json:"learners"`
	PeerAddrs  map[string]string `json:"peer_addrs,omitempty"`
}

// ApplyResult 是一条日志被状态机应用之后返回给提议者的结果。
// Err 非空表示这次提议没有在本节点上完成（通常是丢掉了领导权）；
// 调用方必须用同一个幂等请求号重试，因为日志仍有可能已经被多数派提交。
type ApplyResult struct {
	Index      uint64
	Data       []byte
	Err        error
	Leader     string
	LeaderAddr string
}
