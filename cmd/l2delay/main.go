// Command l2delay 在若干 veth 之间做二层转发，并按帧增加延迟和抖动。
//
// 这台机器的内核没有 sch_netem（tc qdisc replace ... netem 返回
// "Specified qdisc kind is unknown"，且 modules_disabled=1，模块装不进去）。
// 容器仍是独立的网络命名空间；帧从一侧 veth 读出，等到 delay±jitter 再写进另一侧。
// 单向延迟加在每一帧上，请求和响应各算一次，所以 ping 的 RTT 大约是 2×delay。
// 抖动用正态分布，标准差是 -jitter，和 netem 的 "distribution normal" 同一含义。
package main

import (
	"container/heap"
	"encoding/binary"
	"flag"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	packetAddMembership  = 1
	packetMrPromisc      = 1
	packetIgnoreOutgoing = 23
)

func main() {
	ifs := flag.String("ifs", "", "逗号分隔的主机侧 veth 名")
	delay := flag.Duration("delay", 0, "每帧的平均单向延迟")
	jitter := flag.Duration("jitter", 0, "正态分布的标准差")
	flag.Parse()
	names := split(*ifs)
	if len(names) < 2 {
		log.Fatal("至少两个接口")
	}
	ports := make([]port, 0, len(names))
	for _, name := range names {
		p, err := openPort(name)
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		ports = append(ports, p)
		log.Printf("port %s ifindex %d", name, p.idx)
	}
	sw := &bridge{
		ports:  ports,
		delay:  *delay,
		jitter: *jitter,
		learn:  map[[6]byte]int{},
		jobs:   make(chan job, 8192),
	}
	go sw.schedule()
	for i := range ports {
		go sw.read(i)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

type port struct {
	name string
	idx  int
	fd   int
}

func openPort(name string) (port, error) {
	ifi, err := net.InterfaceByName(name)
	if err != nil {
		return port{}, err
	}
	fd, err := syscall.Socket(syscall.AF_PACKET, syscall.SOCK_RAW, int(htons(syscall.ETH_P_ALL)))
	if err != nil {
		return port{}, err
	}
	if err := syscall.Bind(fd, &syscall.SockaddrLinklayer{
		Protocol: htons(syscall.ETH_P_ALL),
		Ifindex:  ifi.Index,
	}); err != nil {
		syscall.Close(fd)
		return port{}, err
	}
	// 自己发出去的帧不要再读回来，否则会在端口之间来回复制。
	if err := syscall.SetsockoptInt(fd, syscall.SOL_PACKET, packetIgnoreOutgoing, 1); err != nil {
		syscall.Close(fd)
		return port{}, err
	}
	mreq := make([]byte, 16)
	binary.LittleEndian.PutUint32(mreq[0:4], uint32(ifi.Index))
	binary.LittleEndian.PutUint16(mreq[4:6], packetMrPromisc)
	if err := setsockoptBytes(fd, syscall.SOL_PACKET, packetAddMembership, mreq); err != nil {
		syscall.Close(fd)
		return port{}, err
	}
	return port{name: name, idx: ifi.Index, fd: fd}, nil
}

func htons(v uint16) uint16 {
	return v<<8 | v>>8
}

func setsockoptBytes(fd, level, opt int, b []byte) error {
	_, _, errno := syscall.Syscall6(syscall.SYS_SETSOCKOPT, uintptr(fd), uintptr(level), uintptr(opt), uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

type bridge struct {
	ports  []port
	delay  time.Duration
	jitter time.Duration
	mu     sync.Mutex
	learn  map[[6]byte]int
	jobs   chan job
}

type job struct {
	when time.Time
	fd   int
	idx  int
	data []byte
}

func (b *bridge) read(i int) {
	buf := make([]byte, 65536)
	p := b.ports[i]
	for {
		n, _, err := syscall.Recvfrom(p.fd, buf, 0)
		if err != nil || n < 14 {
			continue
		}
		frame := make([]byte, n)
		copy(frame, buf[:n])
		var src [6]byte
		copy(src[:], frame[6:12])
		b.mu.Lock()
		b.learn[src] = i
		b.mu.Unlock()
		when := time.Now().Add(b.sample())
		dsts := b.destinations(frame[:6], i)
		for _, d := range dsts {
			data := frame
			if len(dsts) > 1 {
				data = append([]byte(nil), frame...)
			}
			b.jobs <- job{when: when, fd: b.ports[d].fd, idx: b.ports[d].idx, data: data}
		}
	}
}

func (b *bridge) destinations(dst []byte, from int) []int {
	bcast := true
	for _, x := range dst {
		if x != 0xff {
			bcast = false
			break
		}
	}
	if !bcast {
		var key [6]byte
		copy(key[:], dst)
		b.mu.Lock()
		to, ok := b.learn[key]
		b.mu.Unlock()
		if ok && to != from {
			return []int{to}
		}
	}
	out := make([]int, 0, len(b.ports)-1)
	for i := range b.ports {
		if i != from {
			out = append(out, i)
		}
	}
	return out
}

func (b *bridge) sample() time.Duration {
	if b.jitter <= 0 {
		if b.delay < 0 {
			return 0
		}
		return b.delay
	}
	u1 := rand.Float64()
	u2 := rand.Float64()
	if u1 < 1e-12 {
		u1 = 1e-12
	}
	z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	ns := float64(b.delay) + z*float64(b.jitter)
	if ns < 0 {
		return 0
	}
	return time.Duration(ns)
}

func (b *bridge) schedule() {
	h := &jobHeap{}
	heap.Init(h)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		wait := time.Hour
		if h.Len() > 0 {
			wait = time.Until((*h)[0].when)
			if wait < 0 {
				wait = 0
			}
		}
		if wait > 0 && wait < 300*time.Microsecond {
			deadline := time.Now().Add(wait)
			for time.Now().Before(deadline) {
				select {
				case j := <-b.jobs:
					heap.Push(h, j)
				default:
				}
			}
			b.flushDue(h)
			continue
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
		select {
		case j := <-b.jobs:
			heap.Push(h, j)
		case <-timer.C:
			b.flushDue(h)
		}
	}
}

func (b *bridge) flushDue(h *jobHeap) {
	now := time.Now()
	for h.Len() > 0 && !now.Before((*h)[0].when) {
		j := heap.Pop(h).(job)
		_ = syscall.Sendto(j.fd, j.data, 0, &syscall.SockaddrLinklayer{Ifindex: j.idx})
	}
}

type jobHeap []job

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return h[i].when.Before(h[j].when) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *jobHeap) Push(x any)        { *h = append(*h, x.(job)) }
func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
