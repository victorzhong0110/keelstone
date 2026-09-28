package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	defaultTickInterval  = 50 * time.Millisecond
	defaultElectionTick  = 10
	defaultHeartbeatTick = 1
	appendBatch          = 32
	proposeBatch         = 64
)

var (
	// ErrNotLeader 表示提议或线性读没有打在当前 leader 上。
	ErrNotLeader = errors.New("not leader")
	// ErrStopped 表示节点已经停止。
	ErrStopped = errors.New("raft: stopped")
	// ErrConf 表示成员变更被拒绝（同时只允许一条未提交的配置）。
	ErrConf = errors.New("raft: configuration rejected")
)

// StateMachine 是确定性状态机。Apply 的 raw 是日志里的命令字节，返回值会交给提议者。
// Restore 用快照整体替换内存状态。这三个方法都可能在 Raft 循环里被调用。
type StateMachine interface {
	Apply(index uint64, data []byte) []byte
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Config 是一个节点的静态配置。Peers 在首次启动时构成投票成员；
// 之后以日志里的配置为准。Join 为 true 时本节点以 learner 身份加入，不发起选举。
type Config struct {
	ID              string
	Addr            string
	Peers           []Peer
	Join            bool
	TickInterval    time.Duration
	ElectionTick    int
	HeartbeatTick   int
	SnapshotEntries int
	// MaxInflight 是每个 follower 允许同时在途的 AppendEntries 数。
	// 1 是探测式复制：响应回来才发下一条。大于 1 时，第一条成功之后进入流水线，
	// next 按已发出的下标乐观前进。0 在 NewNode 里当成 8。
	MaxInflight int
	Seed        int64
	Debug       bool
}

// Node 把 Raft 状态机、持久化和状态机应用放在同一个事件循环里。
// 外部 goroutine 只能通过 Propose / WaitLinearizable / Deliver / Status 接触它。
// 这样写是为了让「哪些状态只能单线程访问」在代码结构上就看出来。
type Node struct {
	id   string
	addr string
	cfg  Config

	storage Storage
	sm      StateMachine
	debug   bool

	// 下面这一组只允许事件循环访问。
	term         uint64
	vote         string
	storedHS     HardState
	log          *raftLog
	role         Role
	leader       string
	commitIndex  uint64
	lastApplied  uint64
	voters       map[string]Peer
	learners     map[string]Peer
	snapVoters   map[string]Peer
	snapLearners map[string]Peer
	prs          map[string]*progress
	rng          *rand.Rand

	electionElapsed  int
	electionTimeout  int
	electionTick     int
	heartbeatElapsed int
	heartbeatTick    int
	quorumElapsed    int
	tickInterval     time.Duration
	snapEvery        int

	voteRound   uint64
	votes       map[string]bool
	preVotes    map[string]bool
	termStart   uint64 // 本任期第一条日志（noop）的下标，ReadIndex 必须等它提交
	waiters     map[uint64]chan ApplyResult
	barrier     *readBarrier
	queuedReads []readReq
	readSeq     uint64
	latestSnap  Snapshot

	transport func(Message)

	inbox     chan Message
	propC     chan proposal
	readC     chan readReq
	snapC     chan struct{}
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once

	statusMu sync.Mutex
	status   Status
}

// sentApp 是一条已经发出、还没被对上的 AppendEntries。
// 流水线里用它识别乱序：后发出的消息先到，不能把 next 拉回，也不能当成日志冲突。
type sentApp struct {
	probe uint64
	prev  uint64
	last  uint64
}

type progress struct {
	next       uint64
	match      uint64
	paused     bool
	probe      uint64
	floor      uint64 // 小于等于 floor 的响应来自已经作废的流水线
	replicate  bool   // true：乐观推进 next，允许多条在途；false：一次一条的探测
	acked      bool   // 本轮 checkQuorum 窗口内是否回复过
	sent       []sentApp
	quietTicks int
	readPing   bool // ReadIndex 需要一条带着当前 barrier 的消息，即使窗口已满
}

type proposal struct {
	typ  EntryType
	data []byte
	ch   chan ApplyResult
}

type readReq struct {
	ch chan error
}

type readBarrier struct {
	id      uint64
	index   uint64
	acks    map[string]struct{}
	waiters []readReq
}

// NewNode 从 Storage 恢复任期、日志和快照，并把已提交的日志重放到状态机。
// 返回后还没开始选举，调用 Start 才会跑事件循环。
func NewNode(cfg Config, st Storage, sm StateMachine) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: empty id")
	}
	if st == nil {
		return nil, errors.New("raft: nil storage")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = defaultElectionTick
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = defaultHeartbeatTick
	}
	// 0 表示使用默认窗口。1 保持「上一条响应回来之前不发下一条」。
	if cfg.MaxInflight == 0 {
		cfg.MaxInflight = 8
	}
	hs, ents, snap, err := st.InitialState()
	if err != nil {
		return nil, err
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano() ^ int64(hashString(cfg.ID))
	}
	n := &Node{
		id:            cfg.ID,
		addr:          cfg.Addr,
		cfg:           cfg,
		storage:       st,
		sm:            sm,
		debug:         cfg.Debug || os.Getenv("KEELSTONE_DEBUG") == "1",
		term:          hs.Term,
		vote:          hs.Vote,
		storedHS:      hs,
		log:           newLog(snap.Index, snap.Term, ents),
		role:          Follower,
		commitIndex:   hs.Commit,
		rng:           rand.New(rand.NewSource(seed)),
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		tickInterval:  cfg.TickInterval,
		snapEvery:     cfg.SnapshotEntries,
		waiters:       map[uint64]chan ApplyResult{},
		prs:           map[string]*progress{},
		latestSnap:    snap,
		inbox:         make(chan Message, 1024),
		propC:         make(chan proposal, 256),
		readC:         make(chan readReq, 256),
		snapC:         make(chan struct{}, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	if snap.Index > 0 {
		n.snapVoters = peerMap(snap.Voters)
		n.snapLearners = peerMap(snap.Learners)
	} else {
		n.snapVoters, n.snapLearners = bootstrapPeers(cfg)
	}
	if n.commitIndex < snap.Index {
		n.commitIndex = snap.Index
	}
	n.lastApplied = snap.Index
	if sm != nil && len(snap.Data) > 0 {
		if err := sm.Restore(snap.Data); err != nil {
			return nil, err
		}
	}
	n.replayToCommit()
	n.refreshConfig()
	n.resetElection()
	n.publish()
	return n, nil
}

func (n *Node) replayToCommit() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, ok := n.log.entry(n.lastApplied)
		if !ok {
			log.Panicf("raft %s: cannot replay committed index %d", n.id, n.lastApplied)
		}
		if e.Type == EntryCommand && n.sm != nil {
			n.sm.Apply(e.Index, e.Data)
		}
	}
}

// SetTransport 必须在 Start 之前调用。回调在事件循环上执行，应当尽快返回。
func (n *Node) SetTransport(fn func(Message)) { n.transport = fn }

// PeerAddr 返回当前配置里某个节点的地址。只允许在传输回调里使用，
// 因为成员表没有锁，调用方必须已经在事件循环上。
func (n *Node) PeerAddr(id string) string { return n.addrOf(id) }

// Start 启动事件循环。
func (n *Node) Start() {
	n.startOnce.Do(func() { go n.loop() })
}

// Stop 停止事件循环并等待它退出。可以多次调用。没 Start 过也不会卡住。
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stop) })
	n.startOnce.Do(func() { close(n.done) })
	<-n.done
}

// Deliver 把一条网络消息交给事件循环。缓冲区满时丢弃，复制循环会重试。
func (n *Node) Deliver(m Message) {
	select {
	case <-n.stop:
		return
	case n.inbox <- m:
	default:
	}
}

// Propose 追加一条普通命令。返回时命令已经应用到本节点的状态机，
// 因此对调用者来说这条写是已提交的。超时不代表没有提交，必须用同一请求号重试。
func (n *Node) Propose(ctx context.Context, data []byte) (ApplyResult, error) {
	return n.propose(ctx, EntryCommand, data)
}

// ProposeConfChange 追加一条成员变更。语义与 Propose 相同。
func (n *Node) ProposeConfChange(ctx context.Context, cc ConfChange) (ApplyResult, error) {
	raw, err := json.Marshal(cc)
	if err != nil {
		return ApplyResult{}, err
	}
	return n.propose(ctx, EntryConfig, raw)
}

func (n *Node) propose(ctx context.Context, typ EntryType, data []byte) (ApplyResult, error) {
	ch := make(chan ApplyResult, 1)
	p := proposal{typ: typ, data: data, ch: ch}
	select {
	case <-n.stop:
		return ApplyResult{}, ErrStopped
	case <-ctx.Done():
		return ApplyResult{}, ctx.Err()
	case n.propC <- p:
	}
	select {
	case res := <-ch:
		if res.Err != nil {
			return res, res.Err
		}
		return res, nil
	case <-ctx.Done():
		return ApplyResult{}, ctx.Err()
	case <-n.stop:
		return ApplyResult{}, ErrStopped
	}
}

// WaitLinearizable 实现 ReadIndex：确认自己仍是 leader，并且状态机至少应用到
// 发起读时的 commitIndex。返回 nil 之后，调用方可以读本地状态机。
func (n *Node) WaitLinearizable(ctx context.Context) error {
	ch := make(chan error, 1)
	select {
	case <-n.stop:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	case n.readC <- readReq{ch: ch}:
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stop:
		return ErrStopped
	}
}

// ForceSnapshot 请求事件循环尽快打一个快照。
func (n *Node) ForceSnapshot() {
	select {
	case <-n.stop:
	case n.snapC <- struct{}{}:
	}
}

// Status 返回最近一次发布的状态副本。
func (n *Node) Status() Status {
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	st := n.status
	st.Voters = append([]string(nil), st.Voters...)
	st.Learners = append([]string(nil), st.Learners...)
	if st.PeerAddrs != nil {
		cp := make(map[string]string, len(st.PeerAddrs))
		for k, v := range st.PeerAddrs {
			cp[k] = v
		}
		st.PeerAddrs = cp
	}
	return st
}

func (n *Node) loop() {
	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()
	defer close(n.done)
	defer n.shutdown()
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.tick()
		case m := <-n.inbox:
			n.Step(m)
			n.drainInbox(128)
		case p := <-n.propC:
			n.handleProposals(p)
		case r := <-n.readC:
			n.handleRead(r)
			n.drainReads(64)
		case <-n.snapC:
			n.takeSnapshot()
		}
		n.publish()
	}
}

func (n *Node) drainInbox(limit int) {
	for i := 0; i < limit; i++ {
		select {
		case m := <-n.inbox:
			n.Step(m)
		default:
			return
		}
	}
}

func (n *Node) drainReads(limit int) {
	for i := 0; i < limit; i++ {
		select {
		case r := <-n.readC:
			n.handleRead(r)
		default:
			return
		}
	}
}

func (n *Node) shutdown() {
	n.failWaiters(ErrStopped)
	n.failReads(ErrStopped)
}

func (n *Node) tick() {
	if n.role == Leader {
		n.tickLeader()
		return
	}
	n.electionElapsed++
	if n.electionElapsed < n.electionTimeout {
		return
	}
	n.resetElection()
	if !n.isVoter(n.id) {
		return
	}
	// 选举超时后先 PreVote。真正的 term 自增要等到预投票拿到多数。
	n.startPreVote()
}

func (n *Node) tickLeader() {
	n.heartbeatElapsed++
	if n.heartbeatElapsed >= n.heartbeatTick {
		n.heartbeatElapsed = 0
		for id, pr := range n.prs {
			if id == n.id {
				continue
			}
			pr.quietTicks++
			if pr.replicate {
				// 流水线的响应丢了的话，窗口会一直占着。大约 200ms 没有回包就退回探测，从 match 重发。
				if pr.quietTicks >= 10 && len(pr.sent) > 0 {
					pr.next = pr.match + 1
					n.becomeProbe(pr)
				}
				continue
			}
			// 探测模式在途的那一条没回来之前不要重发。每拍都重发会把 probe 加一，
			// 大快照的响应回来时对不上号，leader 就永远停在「再发一次快照」。
			// 大约 10 个心跳还没回包，才当作丢失重发。
			if pr.paused && pr.quietTicks < 10 {
				continue
			}
			pr.paused = false
		}
		n.bcastAppend()
	}
	n.quorumElapsed++
	if n.quorumElapsed >= n.electionTimeout {
		n.quorumElapsed = 0
		n.checkQuorum()
	}
}

// checkQuorum 在大约一个选举超时内没有拿到多数心跳回复时主动下台。
// 否则一个被分区的旧 leader 会一直对客户端自称 leader。
func (n *Node) checkQuorum() {
	acks := 0
	if n.isVoter(n.id) {
		acks++
	}
	for id, pr := range n.prs {
		if id == n.id || !n.isVoter(id) {
			continue
		}
		if pr.acked {
			acks++
		}
		pr.acked = false
	}
	if acks < n.quorum() {
		n.dlogf("checkQuorum failed acks=%d need=%d term=%d", acks, n.quorum(), n.term)
		n.becomeFollower(n.term, "")
	}
}

func (n *Node) startPreVote() {
	n.role = PreCandidate
	n.leader = ""
	n.voteRound++
	n.preVotes = map[string]bool{n.id: true}
	n.dlogf("pre-vote term %d round %d", n.term+1, n.voteRound)
	if n.granted(n.preVotes) >= n.quorum() {
		n.becomeCandidate()
		return
	}
	idx := n.log.lastIndex()
	for id := range n.voters {
		if id == n.id {
			continue
		}
		n.send(Message{
			Type:    MsgPreVote,
			To:      id,
			Term:    n.term + 1,
			Index:   idx,
			LogTerm: n.log.lastTerm(),
			Context: n.voteRound,
		})
	}
}

func (n *Node) becomeCandidate() {
	n.role = Candidate
	n.term++
	n.vote = n.id
	n.leader = ""
	n.voteRound++
	n.votes = map[string]bool{n.id: true}
	n.persistHard()
	n.resetElection()
	n.dlogf("candidate term %d", n.term)
	if n.granted(n.votes) >= n.quorum() {
		n.becomeLeader()
		return
	}
	idx := n.log.lastIndex()
	for id := range n.voters {
		if id == n.id {
			continue
		}
		n.send(Message{
			Type:    MsgVote,
			To:      id,
			Term:    n.term,
			Index:   idx,
			LogTerm: n.log.lastTerm(),
			Context: n.voteRound,
		})
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.votes = nil
	n.preVotes = nil
	n.resetElection()
	n.heartbeatElapsed = 0
	n.quorumElapsed = 0
	last := n.log.lastIndex()
	n.prs = map[string]*progress{}
	for id := range n.allPeers() {
		n.prs[id] = &progress{next: last + 1}
	}
	// 上任立刻追加一条当前任期的 noop。Figure 8：只靠当前任期的日志才能推进 commitIndex，
	// 这条 noop 提交之后，它前面的旧任期日志也就一起提交了。
	e := n.appendEntry(EntryNoop, nil)
	n.termStart = e.Index
	n.prs[n.id].match = e.Index
	n.prs[n.id].next = e.Index + 1
	n.dlogf("leader term %d noop %d", n.term, e.Index)
	n.maybeCommit()
	n.bcastAppend()
}

func (n *Node) becomeFollower(term uint64, leader string) {
	prev := n.role
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.preVotes = nil
	if term > n.term {
		n.term = term
		n.vote = ""
		n.persistHard()
	}
	n.resetElection()
	n.dlogf("follower term %d leader %q", n.term, leader)
	// 先发布状态再叫醒等待者，客户端读到的 leader 提示才不是旧的。
	n.publish()
	if prev == Leader || prev == Candidate || prev == PreCandidate {
		n.failWaiters(ErrNotLeader)
		n.failReads(ErrNotLeader)
	}
}

// Step 是事件循环里处理一条消息的入口。PreVote 单独走，因为它即使携带更大的
// “预期任期”也不能改写本机的 term。
func (n *Node) Step(m Message) {
	if m.From == "" || m.From == n.id {
		return
	}
	switch m.Type {
	case MsgPreVote, MsgPreVoteResp:
		n.stepPreVote(m)
		return
	}
	if m.Term > n.term {
		lead := ""
		if m.Type == MsgApp || m.Type == MsgSnap {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	} else if m.Term < n.term && m.Term > 0 {
		n.rejectStale(m)
		return
	}
	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppResp(m)
	case MsgSnap:
		n.handleSnap(m)
	case MsgSnapResp:
		n.handleSnapResp(m)
	}
}

func (n *Node) rejectStale(m Message) {
	switch m.Type {
	case MsgVote:
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: true, Context: m.Context})
	case MsgApp:
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Probe: m.Probe, Context: m.Context})
	case MsgSnap:
		n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, Reject: true, Probe: m.Probe})
	}
}

func (n *Node) stepPreVote(m Message) {
	if m.Type == MsgPreVote {
		n.handlePreVote(m)
		return
	}
	// 拒绝并且对方任期比我们高：对方已经在一个更新的任期里，跟着走。
	// 同意票上的 Term 是「预期任期」= 本地 term+1，不能当成已经发生的任期。
	if m.Reject && m.Term > n.term {
		n.becomeFollower(m.Term, "")
		return
	}
	if !m.Reject && m.Term > n.term+1 {
		n.becomeFollower(m.Term, "")
		return
	}
	if n.role != PreCandidate || m.Context != n.voteRound {
		return
	}
	n.preVotes[m.From] = !m.Reject
	g, r := n.countVotes(n.preVotes)
	if g >= n.quorum() {
		n.becomeCandidate()
	} else if r >= n.quorum() {
		n.becomeFollower(n.term, "")
	}
}

func (n *Node) handlePreVote(m Message) {
	reject := func(term uint64) {
		n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: term, Reject: true, Context: m.Context})
	}
	if m.Term <= n.term {
		reject(n.term)
		return
	}
	// 最近听到过 leader，或自己就是健康的 leader：不把票探给一个可能被分区的节点。
	if n.role == Leader || (n.leader != "" && n.electionElapsed < n.electionTimeout) {
		reject(n.term)
		return
	}
	if !n.log.upToDate(m.Index, m.LogTerm) {
		reject(n.term)
		return
	}
	n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term, Reject: false, Context: m.Context})
}

func (n *Node) handleVote(m Message) {
	grant := (n.vote == "" || n.vote == m.From) && n.log.upToDate(m.Index, m.LogTerm)
	if grant {
		n.vote = m.From
		n.persistHard()
		n.resetElection()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: !grant, Context: m.Context})
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term || m.Context != n.voteRound {
		return
	}
	n.votes[m.From] = !m.Reject
	g, r := n.countVotes(n.votes)
	if g >= n.quorum() {
		n.becomeLeader()
	} else if r >= n.quorum() {
		n.becomeFollower(n.term, "")
	}
}

func (n *Node) handleAppend(m Message) {
	if n.role != Follower {
		n.becomeFollower(n.term, m.From)
	} else {
		n.leader = m.From
		n.resetElection()
	}
	if !n.log.matchTerm(m.Index, m.LogTerm) {
		hint, ct := n.log.rejectHint(m.Index)
		n.send(Message{
			Type: MsgAppResp, To: m.From, Term: n.term, Reject: true,
			Index: m.Index, RejectHint: hint, ConflictTerm: ct,
			Probe: m.Probe, Context: m.Context,
		})
		return
	}
	truncFrom, didTrunc, novel, ok := n.log.maybeAppend(m.Index, m.Entries)
	if !ok {
		n.send(Message{
			Type: MsgAppResp, To: m.From, Term: n.term, Reject: true,
			Index: m.Index, RejectHint: n.log.lastIndex() + 1,
			Probe: m.Probe, Context: m.Context,
		})
		return
	}
	if didTrunc {
		n.must(n.storage.TruncateSuffix(truncFrom))
	}
	if len(novel) > 0 {
		n.must(n.storage.Append(novel))
	}
	if didTrunc || len(novel) > 0 {
		n.refreshConfig()
	}
	// 回复的 Index 是 leader 这条消息覆盖到的位置，而不是 follower 自己的 lastIndex。
	// follower 可能还留着旧 leader 的冲突后缀；用 lastIndex 会让 leader 误以为那些记录已经对齐。
	matched := m.Index + uint64(len(m.Entries))
	n.send(Message{
		Type: MsgAppResp, To: m.From, Term: n.term,
		Index: matched, Probe: m.Probe, Context: m.Context,
	})
	if m.Commit > n.commitIndex {
		n.advanceCommit(min(m.Commit, matched))
	}
}

func (n *Node) handleAppResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	// 任何响应都说明这条链路还在。丢包重发看的是「完全没有响应」。
	pr.quietTicks = 0
	if !m.Reject {
		pr.acked = true
		if n.barrier != nil && m.Context == n.barrier.id && n.isVoter(m.From) {
			n.barrier.acks[m.From] = struct{}{}
			n.maybeFinishRead()
		}
	}
	if pr.replicate {
		n.handleReplicateResp(m, pr)
		return
	}
	// 探测模式只认最新一条。心跳重发会把 probe 加一，迟到的旧响应不能再改 next。
	if m.Probe != pr.probe {
		return
	}
	pr.paused = false
	if m.Reject {
		if m.Index < pr.match {
			n.sendAppend(m.From)
			return
		}
		pr.next = n.rejectNext(pr, m)
		n.sendAppend(m.From)
		return
	}
	if m.Index > pr.match {
		pr.match = m.Index
		pr.next = pr.match + 1
	}
	n.maybeCommit()
	if n.maxInflight() > 1 {
		pr.replicate = true
		pr.sent = nil
	}
	n.sendAppend(m.From)
}

// handleReplicateResp 处理流水线上的响应。成功就推进 match；
// 真正的日志冲突退回探测；仅仅是乱序（后面的消息先到）则把 next 拉回缺口，继续发。
func (n *Node) handleReplicateResp(m Message, pr *progress) {
	if m.Probe <= pr.floor {
		if !m.Reject && m.Index > pr.match {
			pr.match = m.Index
			if pr.next < pr.match+1 {
				pr.next = pr.match + 1
			}
			n.maybeCommit()
			n.sendAppend(m.From)
		}
		return
	}
	var meta sentApp
	found := false
	kept := pr.sent[:0]
	for _, s := range pr.sent {
		if s.probe == m.Probe {
			meta = s
			found = true
			continue
		}
		kept = append(kept, s)
	}
	pr.sent = kept
	if !found {
		if !m.Reject && m.Index > pr.match {
			pr.match = m.Index
			if pr.next < pr.match+1 {
				pr.next = pr.match + 1
			}
			n.maybeCommit()
			n.sendAppend(m.From)
		}
		return
	}
	if m.Reject {
		if m.Index <= pr.match {
			n.sendAppend(m.From)
			return
		}
		earlier := false
		for _, s := range pr.sent {
			if s.probe < m.Probe {
				earlier = true
				break
			}
		}
		if earlier {
			// 这条消息的前驱还在路上。follower 拒绝是因为 prev 还没到，不是日志分叉。
			restart := meta.prev + 1
			if restart < pr.match+1 {
				restart = pr.match + 1
			}
			if pr.next > restart {
				pr.next = restart
			}
			trimmed := pr.sent[:0]
			for _, s := range pr.sent {
				if s.prev >= meta.prev {
					continue
				}
				trimmed = append(trimmed, s)
			}
			pr.sent = trimmed
			// 先别把缺口再发出去。前驱还在路上，现在发只会再被拒绝，把窗口打满。
			// 前驱的成功响应会调用 sendAppend，从拉回后的 next 继续。
			return
		}
		pr.next = n.rejectNext(pr, m)
		n.becomeProbe(pr)
		n.sendAppend(m.From)
		return
	}
	if m.Index > pr.match {
		pr.match = m.Index
		if pr.next < pr.match+1 {
			pr.next = pr.match + 1
		}
	}
	n.maybeCommit()
	n.sendAppend(m.From)
}

func (n *Node) rejectNext(pr *progress, m Message) uint64 {
	next := m.RejectHint
	if m.ConflictTerm > 0 {
		if idx, ok := n.log.lastIndexOfTerm(m.ConflictTerm); ok {
			next = idx + 1
		}
	}
	if next < 1 {
		next = 1
	}
	if next < pr.match+1 {
		next = pr.match + 1
	}
	// 探测模式要求 next 必须下降，否则一条坏的 hint 会让复制停在同一处。
	// 流水线的 next 是乐观值，可能远大于真实冲突点，不能用「只减一」这条规则。
	if !pr.replicate && next >= pr.next {
		if pr.next > pr.match+1 {
			next = pr.next - 1
		} else {
			next = pr.match + 1
		}
	}
	if pr.replicate && next > pr.next {
		next = pr.match + 1
	}
	return next
}

// becomeProbe 丢掉在途窗口。之后的响应如果 probe 不超过 floor，只允许抬高 match，不能改 next。
func (n *Node) becomeProbe(pr *progress) {
	pr.replicate = false
	pr.paused = false
	pr.sent = nil
	pr.readPing = false
	pr.floor = pr.probe
	if pr.next < pr.match+1 {
		pr.next = pr.match + 1
	}
}

func (n *Node) maxInflight() int {
	if n.cfg.MaxInflight <= 1 {
		return 1
	}
	return n.cfg.MaxInflight
}

func (n *Node) handleSnap(m Message) {
	if n.role != Follower {
		n.becomeFollower(n.term, m.From)
	} else {
		n.leader = m.From
		n.resetElection()
	}
	snap := m.Snapshot
	// 快照不能把已提交状态倒回去。已经覆盖到的快照直接应答，让 leader 继续发后续日志。
	if snap.Index <= n.commitIndex {
		n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, Index: snap.Index, Probe: m.Probe})
		return
	}
	keep := n.log.installSnapshot(snap.Index, snap.Term)
	n.must(n.storage.SaveSnapshot(snap, keep))
	if n.sm != nil {
		n.must(n.sm.Restore(snap.Data))
	}
	n.snapVoters = peerMap(snap.Voters)
	n.snapLearners = peerMap(snap.Learners)
	n.commitIndex = snap.Index
	n.lastApplied = snap.Index
	n.latestSnap = snap
	n.persistHard()
	n.refreshConfig()
	n.send(Message{Type: MsgSnapResp, To: m.From, Term: n.term, Index: snap.Index, Probe: m.Probe})
}

func (n *Node) handleSnapResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.quietTicks = 0
	if !m.Reject {
		pr.acked = true
	}
	if m.Probe != pr.probe {
		return
	}
	pr.paused = false
	if m.Reject {
		n.sendAppend(m.From)
		return
	}
	if m.Index > pr.match {
		pr.match = m.Index
		pr.next = m.Index + 1
	}
	n.maybeCommit()
	n.sendAppend(m.From)
}

func (n *Node) handleProposals(first proposal) {
	batch := []proposal{first}
	for len(batch) < proposeBatch {
		select {
		case p := <-n.propC:
			batch = append(batch, p)
		default:
			n.proposeBatch(batch)
			return
		}
	}
	n.proposeBatch(batch)
}

func (n *Node) proposeBatch(batch []proposal) {
	if n.role != Leader {
		for _, p := range batch {
			n.replyProp(p, ApplyResult{Err: ErrNotLeader, Leader: n.leader, LeaderAddr: n.addrOf(n.leader)})
		}
		return
	}
	var novel []Entry
	for _, p := range batch {
		if p.typ == EntryConfig {
			if n.pendingConf() {
				n.replyProp(p, ApplyResult{Err: fmt.Errorf("%w: one change at a time", ErrConf), Leader: n.id, LeaderAddr: n.addrOf(n.id)})
				continue
			}
			if err := n.validateConf(p.data); err != nil {
				n.replyProp(p, ApplyResult{Err: err, Leader: n.id, LeaderAddr: n.addrOf(n.id)})
				continue
			}
		}
		e := n.log.append(n.term, p.typ, p.data)
		novel = append(novel, e)
		if p.ch != nil {
			n.waiters[e.Index] = p.ch
		}
	}
	if len(novel) == 0 {
		return
	}
	n.must(n.storage.Append(novel))
	n.refreshConfig()
	if pr := n.prs[n.id]; pr != nil {
		pr.match = n.log.lastIndex()
		pr.next = n.log.lastIndex() + 1
	}
	// 探测消息还在途时不要放开 paused。负载一高，每条新日志都会把 probe 加一，
	// 快照或这一轮 AppendEntries 的响应就永远对不上。响应回来后 sendAppend 会带上新后缀。
	n.maybeCommit()
	n.bcastAppend()
}

func (n *Node) pendingConf() bool {
	for _, e := range n.log.entries {
		if e.Index > n.commitIndex && e.Type == EntryConfig {
			return true
		}
	}
	return false
}

func (n *Node) validateConf(raw []byte) error {
	var cc ConfChange
	if err := json.Unmarshal(raw, &cc); err != nil {
		return err
	}
	if cc.ID == "" {
		return fmt.Errorf("%w: empty id", ErrConf)
	}
	switch cc.Op {
	case ConfAddLearner:
		if _, ok := n.voters[cc.ID]; ok {
			return fmt.Errorf("%w: already voter", ErrConf)
		}
		if _, ok := n.learners[cc.ID]; ok {
			return fmt.Errorf("%w: already learner", ErrConf)
		}
		if cc.Addr == "" {
			return fmt.Errorf("%w: addr required", ErrConf)
		}
	case ConfPromote:
		if _, ok := n.learners[cc.ID]; !ok {
			return fmt.Errorf("%w: not a learner", ErrConf)
		}
	case ConfRemove:
		_, isV := n.voters[cc.ID]
		_, isL := n.learners[cc.ID]
		if !isV && !isL {
			return fmt.Errorf("%w: unknown peer", ErrConf)
		}
		if isV && len(n.voters) <= 1 {
			return fmt.Errorf("%w: cannot remove the last voter", ErrConf)
		}
	default:
		return fmt.Errorf("%w: unknown op", ErrConf)
	}
	return nil
}

func (n *Node) appendEntry(typ EntryType, data []byte) Entry {
	e := n.log.append(n.term, typ, data)
	n.must(n.storage.Append([]Entry{e}))
	return e
}

func (n *Node) bcastAppend() {
	for id := range n.prs {
		if id != n.id {
			n.sendAppend(id)
		}
	}
}

func (n *Node) sendAppend(to string) {
	pr := n.prs[to]
	if pr == nil {
		return
	}
	if !pr.replicate {
		n.sendAppendProbe(to, pr)
		return
	}
	limit := n.maxInflight()
	for len(pr.sent) < limit || pr.readPing {
		if !n.sendAppendPipeline(to, pr) {
			return
		}
	}
}

// sendAppendProbe 一次只发一条，发出后停住，直到响应或下一次心跳。
func (n *Node) sendAppendProbe(to string, pr *progress) {
	if pr.paused {
		return
	}
	if n.log.snapIndex > 0 && pr.next <= n.log.snapIndex {
		n.sendSnapshot(to)
		return
	}
	prev := uint64(0)
	if pr.next > 0 {
		prev = pr.next - 1
	}
	term, ok := n.log.term(prev)
	if !ok {
		n.sendSnapshot(to)
		return
	}
	var ents []Entry
	last := n.log.lastIndex()
	if pr.next <= last {
		end := last + 1
		if end > pr.next+appendBatch {
			end = pr.next + appendBatch
		}
		ents = n.log.slice(pr.next, end)
	}
	pr.probe++
	pr.paused = true
	n.send(n.appMessage(to, prev, term, ents, pr.probe))
}

// sendAppendPipeline 在窗口里再塞一条。next 按已发出的最后下标前进，不必等响应。
// 没有新日志且已经有在途消息时返回 false。返回 false 也表示这次改回了探测模式（要发快照）。
func (n *Node) sendAppendPipeline(to string, pr *progress) bool {
	if n.log.snapIndex > 0 && pr.next <= n.log.snapIndex {
		pr.next = pr.match + 1
		n.becomeProbe(pr)
		n.sendSnapshot(to)
		return false
	}
	prev := uint64(0)
	if pr.next > 0 {
		prev = pr.next - 1
	}
	term, ok := n.log.term(prev)
	if !ok {
		pr.next = pr.match + 1
		n.becomeProbe(pr)
		n.sendSnapshot(to)
		return false
	}
	last := n.log.lastIndex()
	force := pr.readPing
	if pr.next > last && !force && len(pr.sent) > 0 {
		return false
	}
	var ents []Entry
	if pr.next <= last {
		end := last + 1
		if end > pr.next+uint64(appendBatch) {
			end = pr.next + uint64(appendBatch)
		}
		ents = n.log.slice(pr.next, end)
	}
	pr.probe++
	pr.readPing = false
	n.send(n.appMessage(to, prev, term, ents, pr.probe))
	covered := prev
	if len(ents) > 0 {
		covered = ents[len(ents)-1].Index
		pr.next = covered + 1
	}
	pr.sent = append(pr.sent, sentApp{probe: pr.probe, prev: prev, last: covered})
	// 空心跳只占一个在途槽，用来续租和 ReadIndex。不要把窗口填满心跳。
	return len(ents) > 0
}

func (n *Node) appMessage(to string, prev, term uint64, ents []Entry, probe uint64) Message {
	var ctx uint64
	if n.barrier != nil {
		ctx = n.barrier.id
	}
	return Message{
		Type: MsgApp, To: to, Term: n.term,
		Index: prev, LogTerm: term, Entries: ents, Commit: n.commitIndex,
		Probe: probe, Context: ctx,
	}
}

func (n *Node) sendSnapshot(to string) {
	pr := n.prs[to]
	if pr == nil || pr.paused {
		return
	}
	if n.latestSnap.Index < n.log.snapIndex {
		n.takeSnapshot()
	}
	if n.latestSnap.Index == 0 {
		pr.next = n.log.snapIndex + 1
		if pr.next == 0 {
			pr.next = 1
		}
		// 没有快照可发（空状态机）。改走普通复制；此时 next 已经大于 snapIndex。
		if n.log.snapIndex == 0 {
			prevTerm, ok := n.log.term(0)
			if ok {
				pr.probe++
				pr.paused = true
				n.send(Message{
					Type: MsgApp, To: to, Term: n.term,
					Index: 0, LogTerm: prevTerm, Commit: n.commitIndex, Probe: pr.probe,
				})
			}
		}
		return
	}
	pr.probe++
	pr.paused = true
	n.send(Message{Type: MsgSnap, To: to, Term: n.term, Snapshot: n.latestSnap, Probe: pr.probe})
}

// maybeCommit 只提升「当前任期」且被多数投票成员复制的最大下标。
// 旧任期的日志要等本任期的记录（通常是 noop）提交后，作为前缀被一起应用。
func (n *Node) maybeCommit() {
	if n.role != Leader || len(n.voters) == 0 {
		return
	}
	matches := make([]uint64, 0, len(n.voters))
	for id := range n.voters {
		if id == n.id {
			matches = append(matches, n.log.lastIndex())
			continue
		}
		if pr := n.prs[id]; pr != nil {
			matches = append(matches, pr.match)
		} else {
			matches = append(matches, 0)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	q := matches[n.quorum()-1]
	if q <= n.commitIndex {
		return
	}
	term, ok := n.log.term(q)
	if ok && term == n.term {
		n.advanceCommit(q)
	}
}

func (n *Node) advanceCommit(to uint64) {
	last := n.log.lastIndex()
	if to > last {
		to = last
	}
	if to <= n.commitIndex {
		return
	}
	n.commitIndex = to
	n.persistHard()
	n.apply()
}

func (n *Node) apply() {
	for n.lastApplied < n.commitIndex {
		idx := n.lastApplied + 1
		e, ok := n.log.entry(idx)
		if !ok {
			log.Panicf("raft %s: missing entry %d snap=%d last=%d", n.id, idx, n.log.snapIndex, n.log.lastIndex())
		}
		var data []byte
		if e.Type == EntryCommand && n.sm != nil {
			data = n.sm.Apply(e.Index, e.Data)
		}
		n.lastApplied = idx
		// 等待通道是无缓冲的。先发布状态，Propose 返回后读到的 commit/applied 才包含这条日志。
		n.publish()
		if ch, ok := n.waiters[idx]; ok {
			ch <- ApplyResult{Index: idx, Data: data, Leader: n.id, LeaderAddr: n.addrOf(n.id)}
			delete(n.waiters, idx)
		}
	}
	if n.snapEvery > 0 && n.log.length() >= n.snapEvery {
		n.takeSnapshot()
	}
	n.flushQueuedReads()
	n.maybeFinishRead()
	if n.role == Leader && !n.isVoter(n.id) {
		// 自己被移除且移除记录已经提交：立刻下台，并且不再参选。
		n.becomeFollower(n.term, "")
	}
}

func (n *Node) takeSnapshot() {
	if n.lastApplied == 0 || n.lastApplied <= n.log.snapIndex {
		return
	}
	var data []byte
	if n.sm != nil {
		b, err := n.sm.Snapshot()
		n.must(err)
		data = b
	}
	term, ok := n.log.term(n.lastApplied)
	if !ok {
		return
	}
	voters, learners := n.configAt(n.lastApplied)
	snap := Snapshot{
		Index:    n.lastApplied,
		Term:     term,
		Voters:   peerList(voters),
		Learners: peerList(learners),
		Data:     data,
	}
	keep := n.log.entriesAfter(n.lastApplied)
	n.must(n.storage.SaveSnapshot(snap, keep))
	n.snapVoters = voters
	n.snapLearners = learners
	n.log.compact(n.lastApplied, term)
	n.latestSnap = snap
	n.refreshConfig()
	n.dlogf("snapshot index %d", snap.Index)
}

func (n *Node) handleRead(r readReq) {
	if n.role != Leader {
		r.ch <- ErrNotLeader
		return
	}
	// 本任期还没有提交过日志时不能读，否则可能读到一段后来被新 leader 覆盖的状态。
	if n.termStart == 0 || n.commitIndex < n.termStart {
		n.queuedReads = append(n.queuedReads, r)
		return
	}
	n.enqueueRead(r)
}

func (n *Node) flushQueuedReads() {
	if n.role != Leader || n.termStart == 0 || n.commitIndex < n.termStart || len(n.queuedReads) == 0 {
		return
	}
	qs := n.queuedReads
	n.queuedReads = nil
	for _, r := range qs {
		n.enqueueRead(r)
	}
}

func (n *Node) enqueueRead(r readReq) {
	if n.barrier == nil {
		n.readSeq++
		n.barrier = &readBarrier{
			id:    n.readSeq,
			index: n.commitIndex,
			acks:  map[string]struct{}{},
		}
		if n.isVoter(n.id) {
			n.barrier.acks[n.id] = struct{}{}
		}
		for id, pr := range n.prs {
			if id == n.id {
				continue
			}
			if pr.replicate {
				pr.readPing = true
			}
		}
		n.bcastAppend()
	}
	n.barrier.waiters = append(n.barrier.waiters, r)
	n.maybeFinishRead()
}

func (n *Node) maybeFinishRead() {
	b := n.barrier
	if b == nil || n.role != Leader {
		return
	}
	if n.readAckCount() < n.quorum() || n.lastApplied < b.index {
		return
	}
	for _, w := range b.waiters {
		w.ch <- nil
	}
	n.barrier = nil
}

func (n *Node) readAckCount() int {
	if n.barrier == nil {
		return 0
	}
	c := 0
	for id := range n.barrier.acks {
		if n.isVoter(id) {
			c++
		}
	}
	return c
}

func (n *Node) refreshConfig() {
	n.voters, n.learners = n.configAt(n.log.lastIndex())
	if n.role != Leader {
		return
	}
	known := n.allPeers()
	for id := range known {
		if _, ok := n.prs[id]; ok {
			continue
		}
		next := uint64(1)
		if n.log.snapIndex > 0 {
			// 小于等于快照下标会触发 InstallSnapshot，新节点不必把已压缩的日志再传一遍。
			next = n.log.snapIndex
		}
		n.prs[id] = &progress{next: next}
	}
	for id := range n.prs {
		if id == n.id {
			continue
		}
		if _, ok := known[id]; !ok {
			delete(n.prs, id)
		}
	}
}

func (n *Node) configAt(index uint64) (map[string]Peer, map[string]Peer) {
	voters := clonePeers(n.snapVoters)
	learners := clonePeers(n.snapLearners)
	for _, e := range n.log.entries {
		if e.Index > index {
			break
		}
		if e.Type == EntryConfig {
			applyConf(voters, learners, e.Data)
		}
	}
	return voters, learners
}

func (n *Node) allPeers() map[string]Peer {
	out := clonePeers(n.voters)
	for id, p := range n.learners {
		out[id] = p
	}
	return out
}

func (n *Node) isVoter(id string) bool {
	_, ok := n.voters[id]
	return ok
}

func (n *Node) quorum() int { return len(n.voters)/2 + 1 }

func (n *Node) granted(votes map[string]bool) int {
	g, _ := n.countVotes(votes)
	return g
}

func (n *Node) countVotes(votes map[string]bool) (grant, reject int) {
	for id := range n.voters {
		v, ok := votes[id]
		if !ok {
			continue
		}
		if v {
			grant++
		} else {
			reject++
		}
	}
	return grant, reject
}

func (n *Node) persistHard() {
	hs := HardState{Term: n.term, Vote: n.vote, Commit: n.commitIndex}
	if hs == n.storedHS {
		return
	}
	n.must(n.storage.SetHardState(hs))
	n.storedHS = hs
}

func (n *Node) resetElection() {
	n.electionElapsed = 0
	span := n.electionTick
	if span < 1 {
		span = 1
	}
	n.electionTimeout = span + n.rng.Intn(span)
}

func (n *Node) send(m Message) {
	if n.transport == nil {
		return
	}
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.transport(cloneMessage(m))
}

func (n *Node) replyProp(p proposal, res ApplyResult) {
	if p.ch != nil {
		p.ch <- res
	}
}

func (n *Node) failWaiters(err error) {
	for idx, ch := range n.waiters {
		ch <- ApplyResult{Err: err, Index: idx, Leader: n.leader, LeaderAddr: n.addrOf(n.leader)}
		delete(n.waiters, idx)
	}
}

func (n *Node) failReads(err error) {
	if n.barrier != nil {
		for _, w := range n.barrier.waiters {
			w.ch <- err
		}
		n.barrier = nil
	}
	for _, w := range n.queuedReads {
		w.ch <- err
	}
	n.queuedReads = nil
}

func (n *Node) addrOf(id string) string {
	if id == "" {
		return ""
	}
	if p, ok := n.voters[id]; ok && p.Addr != "" {
		return p.Addr
	}
	if p, ok := n.learners[id]; ok && p.Addr != "" {
		return p.Addr
	}
	if id == n.id {
		return n.addr
	}
	return ""
}

func (n *Node) publish() {
	st := Status{
		ID:         n.id,
		Role:       n.role.String(),
		Term:       n.term,
		Leader:     n.leader,
		LeaderAddr: n.addrOf(n.leader),
		Commit:     n.commitIndex,
		Applied:    n.lastApplied,
		LastIndex:  n.log.lastIndex(),
		Snapshot:   n.log.snapIndex,
		Voters:     mapKeys(n.voters),
		Learners:   mapKeys(n.learners),
		PeerAddrs:  map[string]string{},
	}
	for id, p := range n.voters {
		st.PeerAddrs[id] = p.Addr
	}
	for id, p := range n.learners {
		st.PeerAddrs[id] = p.Addr
	}
	n.statusMu.Lock()
	n.status = st
	n.statusMu.Unlock()
}

func (n *Node) must(err error) {
	if err != nil {
		log.Panicf("raft %s: storage: %v", n.id, err)
	}
}

func (n *Node) dlogf(format string, args ...any) {
	if n.debug {
		log.Printf("raft %s: "+format, append([]any{n.id}, args...)...)
	}
}

func bootstrapPeers(cfg Config) (voters, learners map[string]Peer) {
	voters = map[string]Peer{}
	learners = map[string]Peer{}
	found := false
	for _, p := range cfg.Peers {
		if p.ID == cfg.ID {
			found = true
			if p.Addr == "" {
				p.Addr = cfg.Addr
			}
		}
		if cfg.Join && p.ID == cfg.ID {
			learners[p.ID] = p
			continue
		}
		voters[p.ID] = p
	}
	if !found {
		p := Peer{ID: cfg.ID, Addr: cfg.Addr}
		if cfg.Join {
			learners[p.ID] = p
		} else {
			voters[p.ID] = p
		}
	}
	return voters, learners
}

func applyConf(voters, learners map[string]Peer, raw []byte) {
	var cc ConfChange
	if err := json.Unmarshal(raw, &cc); err != nil || cc.ID == "" {
		return
	}
	switch cc.Op {
	case ConfAddLearner:
		if _, ok := voters[cc.ID]; ok {
			return
		}
		p := learners[cc.ID]
		p.ID = cc.ID
		if cc.Addr != "" {
			p.Addr = cc.Addr
		}
		learners[cc.ID] = p
	case ConfPromote:
		p, ok := learners[cc.ID]
		if !ok {
			return
		}
		if cc.Addr != "" {
			p.Addr = cc.Addr
		}
		delete(learners, cc.ID)
		voters[cc.ID] = p
	case ConfRemove:
		delete(voters, cc.ID)
		delete(learners, cc.ID)
	}
}

func clonePeers(in map[string]Peer) map[string]Peer {
	out := make(map[string]Peer, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func peerMap(list []Peer) map[string]Peer {
	out := make(map[string]Peer, len(list))
	for _, p := range list {
		out[p.ID] = p
	}
	return out
}

func peerList(m map[string]Peer) []Peer {
	out := make([]Peer, 0, len(m))
	for _, p := range m {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func mapKeys(m map[string]Peer) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func cloneMessage(m Message) Message {
	m.Entries = cloneEntries(m.Entries)
	if m.Type == MsgSnap {
		m.Snapshot = cloneSnap(m.Snapshot)
	} else {
		m.Snapshot = Snapshot{}
	}
	return m
}

func hashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
