// Package transport 在节点之间用 HTTP 投递 Raft 消息。
// 发送是异步的：Raft 循环不能等网络。响应也是一条反向的 Raft 消息，而不是 HTTP body。
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/victorzhong0110/keelstone/internal/raft"
)

// HTTP 按节点 id 查找地址并 POST /raft。
// Lookup 在 Raft 事件循环上被调用，需要直接读节点里的成员表，不能等异步发布。
type HTTP struct {
	Lookup func(id string) string
	Client *http.Client
}

func New(lookup func(id string) string) *HTTP {
	return &HTTP{
		Lookup: lookup,
		Client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				// 流水线会同时有多条 AppendEntries。默认每个 host 只留 2 条空闲连接，
				// 多出来的请求会反复握手。
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Send 实现 raft 的传输回调。地址缺失或节点是自己时直接丢掉。
func (h *HTTP) Send(m raft.Message) {
	if h.Lookup == nil || m.To == m.From {
		return
	}
	addr := h.Lookup(m.To)
	if addr == "" {
		return
	}
	body, err := json.Marshal(m)
	if err != nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/raft", bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := h.Client.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
}
