package kv

import "testing"

func TestMachineIdempotentAdd(t *testing.T) {
	m := NewMachine()
	cmd := Command{Op: "add", Key: "c", Value: "1", ClientID: "cli", RequestID: 7}
	raw := mustJSON(cmd)
	m.Apply(1, raw)
	m.Apply(2, raw)
	got, ok := m.Get("c")
	if !ok || got != "1" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	cmd.RequestID = 8
	m.Apply(3, mustJSON(cmd))
	got, _ = m.Get("c")
	if got != "2" {
		t.Fatalf("second request = %s", got)
	}
	if _, ok := m.Recall("cli", 7); !ok {
		t.Fatal("recall")
	}
}

func TestCASAndSnapshot(t *testing.T) {
	m := NewMachine()
	m.Apply(1, mustJSON(Command{Op: "cas", Key: "a", Value: "1", ClientID: "c", RequestID: 1}))
	snap, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	m.Apply(2, mustJSON(Command{Op: "put", Key: "a", Value: "9", ClientID: "c", RequestID: 2}))
	other := NewMachine()
	if err := other.Restore(snap); err != nil {
		t.Fatal(err)
	}
	got, ok := other.Get("a")
	if !ok || got != "1" {
		t.Fatalf("restored %q %v", got, ok)
	}
	// 重放同一个请求号不能再次插入。
	other.Apply(1, mustJSON(Command{Op: "cas", Key: "a", Value: "1", ClientID: "c", RequestID: 1}))
	if other.Len() != 1 {
		t.Fatal(other.Clone())
	}
}
