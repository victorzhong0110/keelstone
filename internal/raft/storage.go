package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Storage 是 Raft 的持久化接口。
// 实现必须在方法返回前把数据写到崩溃后仍能读到的介质；内存实现只用于测试，
// 用来模拟「进程没了、磁盘还在」。
type Storage interface {
	InitialState() (HardState, []Entry, Snapshot, error)
	SetHardState(hs HardState) error
	Append(entries []Entry) error
	// TruncateSuffix 删除下标 >= from 的日志（冲突截断）。
	TruncateSuffix(from uint64) error
	// SaveSnapshot 原子地保存快照，并把 WAL 重写成只剩 keep（下标大于快照的后缀）。
	SaveSnapshot(snap Snapshot, keep []Entry) error
}

// MemoryStorage 把「磁盘」放在内存里，供故障注入模拟进程崩溃：
// Node 停掉后 Storage 对象还在，新 Node 从它恢复。
type MemoryStorage struct {
	mu   sync.Mutex
	hs   HardState
	snap Snapshot
	ents []Entry
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) InitialState() (HardState, []Entry, Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, cloneEntries(s.ents), cloneSnap(s.snap), nil
}

func (s *MemoryStorage) SetHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	return nil
}

func (s *MemoryStorage) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ents = append(s.ents, cloneEntries(entries)...)
	return nil
}

func (s *MemoryStorage) TruncateSuffix(from uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keep []Entry
	for _, e := range s.ents {
		if e.Index < from {
			keep = append(keep, e)
		}
	}
	s.ents = keep
	return nil
}

func (s *MemoryStorage) SaveSnapshot(snap Snapshot, keep []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = cloneSnap(snap)
	s.ents = cloneEntries(keep)
	if s.hs.Commit < snap.Index {
		s.hs.Commit = snap.Index
	}
	return nil
}

func cloneSnap(s Snapshot) Snapshot {
	out := s
	out.Voters = append([]Peer(nil), s.Voters...)
	out.Learners = append([]Peer(nil), s.Learners...)
	if s.Data != nil {
		out.Data = append([]byte(nil), s.Data...)
	}
	return out
}

// WAL 布局（全部本项目自定义，不是任何开源 Raft 的段格式）：
//
//	hardstate  原子替换的小文件：term、vote、commit
//	snapshot   原子替换：元数据 JSON + 状态机字节
//	wal        追加写的记录流，记录类型只有 entry 和 truncate
//
// 崩溃恢复：先读快照，再重放 WAL，丢掉下标 <= 快照下标的日志。
// 文件尾上 CRC 对不上的半条记录会被截掉，相当于丢掉最后一次没写完的追加。
type WAL struct {
	dir string
	f   *os.File
}

const (
	walMagic = "RKW1"
	recEntry = byte(1)
	recTrunc = byte(2)
)

func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, f: f}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		if _, err := f.Write([]byte(walMagic)); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	return w, nil
}

func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

func (w *WAL) InitialState() (HardState, []Entry, Snapshot, error) {
	hs, err := readHardState(filepath.Join(w.dir, "hardstate"))
	if err != nil {
		return HardState{}, nil, Snapshot{}, err
	}
	snap, err := readSnapshot(filepath.Join(w.dir, "snapshot"))
	if err != nil {
		return HardState{}, nil, Snapshot{}, err
	}
	ents, err := w.replay(snap.Index)
	if err != nil {
		return HardState{}, nil, Snapshot{}, err
	}
	if hs.Commit < snap.Index {
		hs.Commit = snap.Index
	}
	return hs, ents, snap, nil
}

func (w *WAL) SetHardState(hs HardState) error {
	return writeHardState(filepath.Join(w.dir, "hardstate"), hs)
}

func (w *WAL) Append(entries []Entry) error {
	for _, e := range entries {
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := w.writeRecord(recEntry, payload); err != nil {
			return err
		}
	}
	return w.f.Sync()
}

func (w *WAL) TruncateSuffix(from uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], from)
	if err := w.writeRecord(recTrunc, buf[:]); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) SaveSnapshot(snap Snapshot, keep []Entry) error {
	if err := writeSnapshot(filepath.Join(w.dir, "snapshot"), snap); err != nil {
		return err
	}
	// 先把新 WAL 写到临时文件再替换，崩溃时旧 WAL 还在，恢复规则会忽略快照已覆盖的前缀。
	tmp := filepath.Join(w.dir, "wal.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(walMagic)); err != nil {
		f.Close()
		return err
	}
	for _, e := range keep {
		payload, err := json.Marshal(e)
		if err != nil {
			f.Close()
			return err
		}
		if err := writeRecordTo(f, recEntry, payload); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	dest := filepath.Join(w.dir, "wal")
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}
	nf, err := os.OpenFile(dest, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := nf.Seek(0, io.SeekEnd); err != nil {
		nf.Close()
		return err
	}
	w.f = nf
	return nil
}

func (w *WAL) replay(snapIndex uint64) ([]Entry, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	magic := make([]byte, len(walMagic))
	if _, err := io.ReadFull(w.f, magic); err != nil {
		return nil, fmt.Errorf("wal header: %w", err)
	}
	if string(magic) != walMagic {
		return nil, errors.New("wal: bad magic")
	}
	var ents []Entry
	good := int64(len(walMagic))
	for {
		var hdr [5]byte
		_, err := io.ReadFull(w.f, hdr[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		typ := hdr[0]
		n := binary.BigEndian.Uint32(hdr[1:])
		if n > 32<<20 {
			return nil, errors.New("wal: record too large")
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			break
		}
		var cbuf [4]byte
		if _, err := io.ReadFull(w.f, cbuf[:]); err != nil {
			break
		}
		sum := crc32.ChecksumIEEE(append([]byte{typ}, payload...))
		if binary.BigEndian.Uint32(cbuf[:]) != sum {
			break
		}
		switch typ {
		case recEntry:
			var e Entry
			if err := json.Unmarshal(payload, &e); err != nil {
				return nil, err
			}
			if len(ents) > 0 && e.Index <= ents[len(ents)-1].Index {
				var keep []Entry
				for _, old := range ents {
					if old.Index < e.Index {
						keep = append(keep, old)
					}
				}
				ents = keep
			}
			ents = append(ents, e)
		case recTrunc:
			if len(payload) != 8 {
				return nil, errors.New("wal: bad truncate")
			}
			from := binary.BigEndian.Uint64(payload)
			var keep []Entry
			for _, e := range ents {
				if e.Index < from {
					keep = append(keep, e)
				}
			}
			ents = keep
		default:
			return nil, fmt.Errorf("wal: unknown record %d", typ)
		}
		off, err := w.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		good = off
	}
	st, err := w.f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != good {
		if err := w.f.Truncate(good); err != nil {
			return nil, err
		}
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	var out []Entry
	for _, e := range ents {
		if e.Index > snapIndex {
			out = append(out, e)
		}
	}
	return out, nil
}

func (w *WAL) writeRecord(typ byte, payload []byte) error {
	return writeRecordTo(w.f, typ, payload)
}

func writeRecordTo(f *os.File, typ byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		return err
	}
	sum := crc32.ChecksumIEEE(append([]byte{typ}, payload...))
	var cbuf [4]byte
	binary.BigEndian.PutUint32(cbuf[:], sum)
	_, err := f.Write(cbuf[:])
	return err
}

func writeHardState(path string, hs HardState) error {
	body, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	return atomicWrite(path, body)
}

func readHardState(path string) (HardState, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, err
	}
	var hs HardState
	if err := json.Unmarshal(b, &hs); err != nil {
		return HardState{}, err
	}
	return hs, nil
}

type snapDisk struct {
	Index    uint64 `json:"index"`
	Term     uint64 `json:"term"`
	Voters   []Peer `json:"voters"`
	Learners []Peer `json:"learners"`
	Data     []byte `json:"data"`
}

func writeSnapshot(path string, snap Snapshot) error {
	body, err := json.Marshal(snapDisk{
		Index: snap.Index, Term: snap.Term,
		Voters: snap.Voters, Learners: snap.Learners, Data: snap.Data,
	})
	if err != nil {
		return err
	}
	return atomicWrite(path, body)
}

func readSnapshot(path string) (Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	var d snapDisk
	if err := json.Unmarshal(b, &d); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Index: d.Index, Term: d.Term, Voters: d.Voters, Learners: d.Learners, Data: d.Data}, nil
}

func atomicWrite(path string, body []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
