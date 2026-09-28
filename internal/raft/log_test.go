package raft

import "testing"

func TestMaybeAppendConflict(t *testing.T) {
	l := newLog(0, 0, nil)
	l.append(1, EntryCommand, []byte("a"))
	l.append(1, EntryCommand, []byte("b"))
	l.append(2, EntryCommand, []byte("c"))
	// leader 从下标 1 重发，下标 2 任期不同，应截掉 2 和 3。
	ents := []Entry{
		{Index: 2, Term: 3, Type: EntryCommand, Data: []byte("B")},
		{Index: 3, Term: 3, Type: EntryCommand, Data: []byte("C")},
	}
	from, trunc, novel, ok := l.maybeAppend(1, ents)
	if !ok || !trunc || from != 2 || len(novel) != 2 {
		t.Fatalf("trunc=%v from=%d novel=%d ok=%v", trunc, from, len(novel), ok)
	}
	if l.lastIndex() != 3 || l.lastTerm() != 3 {
		t.Fatalf("last %d term %d", l.lastIndex(), l.lastTerm())
	}
	e, _ := l.entry(2)
	if string(e.Data) != "B" {
		t.Fatalf("entry2 %s", e.Data)
	}
}

func TestUpToDate(t *testing.T) {
	l := newLog(0, 0, nil)
	l.append(1, EntryNoop, nil)
	l.append(2, EntryNoop, nil)
	if l.upToDate(1, 1) {
		t.Fatal("shorter older term should not be up to date")
	}
	if !l.upToDate(5, 3) {
		t.Fatal("higher term is up to date")
	}
	if !l.upToDate(2, 2) {
		t.Fatal("same log is up to date")
	}
	if l.upToDate(1, 2) {
		t.Fatal("same term but shorter is behind")
	}
}

func TestCompactAndTerm(t *testing.T) {
	l := newLog(0, 0, nil)
	l.append(1, EntryNoop, nil)
	l.append(1, EntryCommand, []byte("x"))
	l.compact(1, 1)
	if l.snapIndex != 1 || l.lastIndex() != 2 {
		t.Fatalf("snap %d last %d", l.snapIndex, l.lastIndex())
	}
	term, ok := l.term(1)
	if !ok || term != 1 {
		t.Fatalf("dummy term %d %v", term, ok)
	}
	if _, ok := l.entry(1); ok {
		t.Fatal("compacted entry should not be addressable as a full entry")
	}
}
