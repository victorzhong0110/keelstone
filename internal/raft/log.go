package raft

// raftLog 是内存中的日志。snapIndex/snapTerm 代表已经被压缩掉的前缀，
// 它在逻辑上仍是一条「哑元」，这样 prevLogIndex == snapIndex 时任期比对仍然成立。
// entries 只保存下标严格大于 snapIndex 的记录，并且下标连续。
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry
}

func newLog(snapIndex, snapTerm uint64, entries []Entry) *raftLog {
	l := &raftLog{snapIndex: snapIndex, snapTerm: snapTerm}
	for _, e := range entries {
		if e.Index > snapIndex {
			l.entries = append(l.entries, e)
		}
	}
	return l
}

func (l *raftLog) lastIndex() uint64 {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Index
	}
	return l.snapIndex
}

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

func (l *raftLog) term(index uint64) (uint64, bool) {
	if index == l.snapIndex {
		return l.snapTerm, true
	}
	e, ok := l.entry(index)
	if !ok {
		return 0, false
	}
	return e.Term, true
}

func (l *raftLog) entry(index uint64) (Entry, bool) {
	if index <= l.snapIndex || index > l.lastIndex() {
		return Entry{}, false
	}
	e := l.entries[index-l.snapIndex-1]
	if e.Index != index {
		return Entry{}, false
	}
	return e, true
}

func (l *raftLog) matchTerm(index, term uint64) bool {
	t, ok := l.term(index)
	return ok && t == term
}

// upToDate 实现论文 5.4.1 的选举限制：先比最后一条的任期，任期相同再比长度。
// 这保证当选的 leader 一定持有所有已经提交的日志。
func (l *raftLog) upToDate(index, term uint64) bool {
	lastTerm := l.lastTerm()
	if term != lastTerm {
		return term > lastTerm
	}
	return index >= l.lastIndex()
}

// append 在本机日志末尾追加一条，调用方负责持久化。
func (l *raftLog) append(term uint64, typ EntryType, data []byte) Entry {
	e := Entry{
		Index: l.lastIndex() + 1,
		Term:  term,
		Type:  typ,
		Data:  append([]byte(nil), data...),
	}
	l.entries = append(l.entries, e)
	return e
}

// maybeAppend 处理 leader 发来的日志后缀。
// 已有且任期相同的前缀跳过；第一次任期冲突时截断冲突位置及其后的所有记录，再接上新后缀。
// novel 是真正新写入、需要落盘的部分。ok 为 false 表示下标不连续，调用方应拒绝这条消息。
func (l *raftLog) maybeAppend(prev uint64, ents []Entry) (truncateFrom uint64, didTruncate bool, novel []Entry, ok bool) {
	for i, e := range ents {
		if e.Index != prev+uint64(i)+1 {
			return 0, false, nil, false
		}
	}
	for i, e := range ents {
		if e.Index <= l.lastIndex() {
			if t, match := l.term(e.Index); match && t == e.Term {
				continue
			}
			l.truncateSuffix(e.Index)
			novel = cloneEntries(ents[i:])
			l.entries = append(l.entries, novel...)
			return e.Index, true, novel, true
		}
		novel = cloneEntries(ents[i:])
		l.entries = append(l.entries, novel...)
		return 0, false, novel, true
	}
	return 0, false, nil, true
}

func (l *raftLog) truncateSuffix(from uint64) {
	if from <= l.snapIndex {
		l.entries = nil
		return
	}
	if from > l.lastIndex() {
		return
	}
	keep := int(from - l.snapIndex - 1)
	if keep < 0 {
		keep = 0
	}
	l.entries = l.entries[:keep]
}

// slice 返回 [from, to) 的副本，避免调用方和日志共享底层数组。
func (l *raftLog) slice(from, to uint64) []Entry {
	if from >= to || from <= l.snapIndex && to <= l.snapIndex {
		return nil
	}
	if from <= l.snapIndex {
		from = l.snapIndex + 1
	}
	if to > l.lastIndex()+1 {
		to = l.lastIndex() + 1
	}
	if from >= to {
		return nil
	}
	lo := int(from - l.snapIndex - 1)
	hi := int(to - l.snapIndex - 1)
	return cloneEntries(l.entries[lo:hi])
}

func (l *raftLog) entriesAfter(index uint64) []Entry {
	var out []Entry
	for _, e := range l.entries {
		if e.Index > index {
			out = append(out, e)
		}
	}
	return cloneEntries(out)
}

func (l *raftLog) compact(index, term uint64) {
	var rest []Entry
	for _, e := range l.entries {
		if e.Index > index {
			rest = append(rest, e)
		}
	}
	l.entries = rest
	l.snapIndex = index
	l.snapTerm = term
}

// installSnapshot 安装快照。如果本地恰好有同一下标、同一任期的日志，
// 保留它之后的后缀（那些记录可能是快照之后又复制来的）；否则整段日志作废。
func (l *raftLog) installSnapshot(index, term uint64) (keep []Entry) {
	if t, ok := l.term(index); ok && t == term {
		keep = l.entriesAfter(index)
		l.entries = cloneEntries(keep)
		l.snapIndex = index
		l.snapTerm = term
		return keep
	}
	l.entries = nil
	l.snapIndex = index
	l.snapTerm = term
	return nil
}

// rejectHint 在 AppendEntries 一致性检查失败时，告诉 leader 下一轮从哪里重试。
// 若冲突下标处的任期已知，同时返回该任期，leader 可以整段跳过这个任期（快速回退）。
func (l *raftLog) rejectHint(prev uint64) (hint uint64, conflictTerm uint64) {
	last := l.lastIndex()
	if prev > last {
		return last + 1, 0
	}
	if prev < l.snapIndex {
		// prev 落在已压缩的前缀里，调用方应改发快照。
		return l.snapIndex, 0
	}
	term, ok := l.term(prev)
	if !ok {
		return last + 1, 0
	}
	first := prev
	for first > l.snapIndex+1 {
		t, ok := l.term(first - 1)
		if !ok || t != term {
			break
		}
		first--
	}
	return first, term
}

func (l *raftLog) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		e := l.entries[i]
		if e.Term == term {
			return e.Index, true
		}
		if e.Term < term {
			break
		}
	}
	if l.snapTerm == term {
		return l.snapIndex, true
	}
	return 0, false
}

func (l *raftLog) length() int { return len(l.entries) }

func cloneEntries(in []Entry) []Entry {
	if len(in) == 0 {
		return nil
	}
	out := make([]Entry, len(in))
	for i, e := range in {
		out[i] = e
		if e.Data != nil {
			out[i].Data = append([]byte(nil), e.Data...)
		}
	}
	return out
}
