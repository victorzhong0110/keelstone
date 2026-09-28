package lincheck

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func op(client int, call, ret int64, in Input, out Output) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    in,
		Call:     call,
		Output:   out,
		Return:   ret,
	}
}

func TestHistoryOK(t *testing.T) {
	hist := []porcupine.Operation{
		op(0, 1, 5, Input{Op: OpPut, Key: "a", Value: "1"}, Output{OK: true, Found: true, Value: "1"}),
		op(1, 6, 8, Input{Op: OpGet, Key: "a"}, Output{OK: true, Found: true, Value: "1"}),
	}
	if res := porcupine.CheckOperationsTimeout(Model(), hist, time.Second); res != porcupine.Ok {
		t.Fatal(res)
	}
}

func TestStaleReadIllegal(t *testing.T) {
	// put 已经返回之后才开始的 get，不能读到旧值。
	hist := []porcupine.Operation{
		op(0, 1, 5, Input{Op: OpPut, Key: "a", Value: "1"}, Output{OK: true, Found: true, Value: "1"}),
		op(1, 6, 8, Input{Op: OpGet, Key: "a"}, Output{OK: true, Found: false}),
	}
	if res := porcupine.CheckOperationsTimeout(Model(), hist, time.Second); res != porcupine.Illegal {
		t.Fatal(res)
	}
}

func TestConcurrentStaleIsOK(t *testing.T) {
	// 区间重叠时，读可以排在写之前。
	hist := []porcupine.Operation{
		op(0, 1, 10, Input{Op: OpPut, Key: "a", Value: "1"}, Output{OK: true}),
		op(1, 2, 4, Input{Op: OpGet, Key: "a"}, Output{OK: true, Found: false}),
	}
	if res := porcupine.CheckOperationsTimeout(Model(), hist, time.Second); res != porcupine.Ok {
		t.Fatal(res)
	}
}
