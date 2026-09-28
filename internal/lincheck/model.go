// Package lincheck 用 Porcupine 检查 KV 历史是否线性一致。
// 操作按 key 划分，每个 key 的状态就是「有没有值、值是什么」。
package lincheck

import (
	"sort"
	"strconv"

	"github.com/anishathalye/porcupine"
)

// Kind 是历史里的操作类型。
type Kind uint8

const (
	OpGet Kind = iota
	OpPut
	OpDelete
	OpCAS
	OpAdd
)

// Input 是客户端调用。
type Input struct {
	Op         Kind
	Key        string
	Value      string
	Expected   string
	ExpPresent bool
}

// Output 是客户端观察到的结果。Unknown 为 true 时表示调用超时，
// 测试会在集群静止后用状态机里的请求缓存把 Unknown 消掉，
// 这里仍然接受 Unknown，便于单独对模型做单元测试。
type Output struct {
	Unknown   bool
	OK        bool
	Found     bool
	Value     string
	Swapped   bool
	Prev      string
	PrevFound bool
}

type kvState struct {
	Present bool
	Value   string
}

// Model 返回按 key 分区的线性一致性模型。
func Model() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Partition: partition,
		Init:      func() []interface{} { return []interface{}{kvState{}} },
		Step:      step,
		Equal:     func(a, b interface{}) bool { return a.(kvState) == b.(kvState) },
		DescribeOperation: func(input, output interface{}) string {
			in := input.(Input)
			out := output.(Output)
			return strconv.Itoa(int(in.Op)) + " " + in.Key + " " + in.Value + " => " + out.Value
		},
		DescribeState: func(state interface{}) string {
			st := state.(kvState)
			if !st.Present {
				return "nil"
			}
			return st.Value
		},
	}
	return nm.ToModel()
}

func partition(history []porcupine.Operation) [][]porcupine.Operation {
	buckets := map[string][]porcupine.Operation{}
	var keys []string
	for _, op := range history {
		k := op.Input.(Input).Key
		if _, ok := buckets[k]; !ok {
			keys = append(keys, k)
		}
		buckets[k] = append(buckets[k], op)
	}
	sort.Strings(keys)
	out := make([][]porcupine.Operation, 0, len(keys))
	for _, k := range keys {
		out = append(out, buckets[k])
	}
	return out
}

func step(state, input, output interface{}) []interface{} {
	st := state.(kvState)
	in := input.(Input)
	out := output.(Output)
	switch in.Op {
	case OpGet:
		if out.Unknown {
			return []interface{}{st}
		}
		if st.Present != out.Found || (out.Found && st.Value != out.Value) {
			return nil
		}
		return []interface{}{st}
	case OpPut:
		next := kvState{Present: true, Value: in.Value}
		if out.Unknown {
			return unique(st, next)
		}
		if !out.OK {
			return nil
		}
		return []interface{}{next}
	case OpDelete:
		next := kvState{}
		if out.Unknown {
			return unique(st, next)
		}
		if !out.OK {
			return nil
		}
		return []interface{}{next}
	case OpAdd:
		delta, err := strconv.ParseInt(in.Value, 10, 64)
		if err != nil {
			return nil
		}
		cur := int64(0)
		if st.Present {
			cur, err = strconv.ParseInt(st.Value, 10, 64)
			if err != nil {
				return nil
			}
		}
		next := kvState{Present: true, Value: strconv.FormatInt(cur+delta, 10)}
		if out.Unknown {
			return unique(st, next)
		}
		if !out.OK || out.Value != next.Value {
			return nil
		}
		return []interface{}{next}
	case OpCAS:
		matched := (!in.ExpPresent && !st.Present) || (in.ExpPresent && st.Present && st.Value == in.Expected)
		swapped := kvState{Present: true, Value: in.Value}
		if out.Unknown {
			if matched {
				return unique(st, swapped)
			}
			return []interface{}{st}
		}
		if !out.OK || out.PrevFound != st.Present || (st.Present && out.Prev != st.Value) {
			return nil
		}
		if out.Swapped {
			if !matched {
				return nil
			}
			return []interface{}{swapped}
		}
		if matched {
			return nil
		}
		return []interface{}{st}
	default:
		return nil
	}
}

func unique(a, b kvState) []interface{} {
	if a == b {
		return []interface{}{a}
	}
	return []interface{}{a, b}
}
