package raft

import (
	"fmt"
	"testing"
)

// 基线 benchmark：量化 raftLog 里的线性扫描成本。
//
// 闸门 1 差异清单：
//   日志长度     —— 实测 1k/10k/100k，生产的日志长度取决于 compaction 策略
//   Entry.Data   —— 实测 32B 负载，生产的 value 大小取决于业务
//   并发度       —— raftLog 本身无锁，调用方持锁；此处测纯函数成本
//
// 关注点：entriesAfter 在 node.go:1340 的应用循环里被调用，
// lastIndexOfTerm 在 node.go:892 的快速回退里被调用。

func benchLog(n int, dataLen int) *raftLog {
	l := &raftLog{}
	data := make([]byte, dataLen)
	for i := 0; i < n; i++ {
		l.append(1, EntryCommand, data)
	}
	return l
}

func BenchmarkLogAppend(b *testing.B) {
	data := make([]byte, 32)
	l := &raftLog{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.append(1, EntryCommand, data)
	}
}

// entriesAfter：扫描全量日志找 index 之后的部分。
// node.go:1340 每应用一条就要调一次。
func BenchmarkLogEntriesAfter(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			l := benchLog(n, 32)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = l.entriesAfter(0)
			}
		})
	}
}

// entriesAfter 在 index 接近末尾时的表现（只需返回少量记录）
func BenchmarkLogEntriesAfterTail(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			l := benchLog(n, 32)
			idx := l.lastIndex() - 10
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = l.entriesAfter(idx)
			}
		})
	}
}

// lastIndexOfTerm：反向扫描找某任期的最后一条。
// node.go:892 的快速回退路径。
func BenchmarkLogLastIndexOfTerm(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			l := &raftLog{}
			data := make([]byte, 32)
			// 前 90% 是 term 1，最后 10% 是 term 2（模拟正常的任期推进）
			for i := 0; i < n*9/10; i++ {
				l.append(1, EntryCommand, data)
			}
			for i := 0; i < n/10; i++ {
				l.append(2, EntryCommand, data)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = l.lastIndexOfTerm(2)
			}
		})
	}
}

// term(index)：单点查询，O(1)。作为对照基准。
func BenchmarkLogTerm(b *testing.B) {
	l := benchLog(100000, 32)
	idx := l.lastIndex() - 1
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = l.term(idx)
	}
}

// entry(index)：单点查询，O(1)
func BenchmarkLogEntry(b *testing.B) {
	l := benchLog(100000, 32)
	idx := l.lastIndex() - 1
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = l.entry(idx)
	}
}

// slice：区间复制，node.go 复制日志给 follower 的路径
func BenchmarkLogSlice(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			l := benchLog(n, 32)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = l.slice(l.snapIndex+1, l.snapIndex+11)
			}
		})
	}
}
