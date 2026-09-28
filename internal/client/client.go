// Package client 是带 leader 重定向和幂等请求号的 KV 客户端。
// 一次调用内部的所有重试共用同一个 request id，所以超时后重试不会把写做两次。
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Client 轮询一组副本地址。成功的响应或 not_leader 提示会更新当前 leader。
type Client struct {
	id      string
	seq     atomic.Uint64
	mu      sync.Mutex
	urls    []string
	cursor  int
	http    *http.Client
	attempt time.Duration
}

// SetAttempt 是单次 HTTP 尝试的上限。调用方的 ctx 仍然管整次重试。
func (c *Client) SetAttempt(d time.Duration) {
	if d > 0 {
		c.attempt = d
	}
}

func New(id string, urls []string) *Client {
	cp := append([]string(nil), urls...)
	return &Client{
		id:   id,
		urls: cp,
		http: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		attempt: 400 * time.Millisecond,
	}
}

// Result 是一次 KV 调用的结果。
type Result struct {
	OK        bool
	Found     bool
	Value     string
	Swapped   bool
	Prev      string
	PrevFound bool
	Index     uint64
	Leader    string
}

type wire struct {
	Value      string `json:"value,omitempty"`
	Expected   string `json:"expected,omitempty"`
	ExpPresent bool   `json:"exp_present,omitempty"`
}

type resp struct {
	OK         bool   `json:"ok"`
	Found      bool   `json:"found"`
	Value      string `json:"value"`
	Swapped    bool   `json:"swapped"`
	Prev       string `json:"prev"`
	PrevFound  bool   `json:"prev_found"`
	Index      uint64 `json:"index"`
	Leader     string `json:"leader"`
	LeaderAddr string `json:"leader_addr"`
	Error      string `json:"error"`
}

func (c *Client) Get(ctx context.Context, key string) (Result, error) {
	return c.do(ctx, http.MethodGet, key, "", nil, 0)
}

func (c *Client) Put(ctx context.Context, key, value string) (Result, error) {
	return c.do(ctx, http.MethodPut, key, "put", &wire{Value: value}, c.seq.Add(1))
}

func (c *Client) Delete(ctx context.Context, key string) (Result, error) {
	return c.do(ctx, http.MethodDelete, key, "delete", &wire{}, c.seq.Add(1))
}

// CAS 在键不存在时把 ExpPresent 设为 false。
func (c *Client) CAS(ctx context.Context, key, expected, value string, expPresent bool) (Result, error) {
	return c.do(ctx, http.MethodPost, key+"/cas", "cas", &wire{Value: value, Expected: expected, ExpPresent: expPresent}, c.seq.Add(1))
}

func (c *Client) do(ctx context.Context, method, suffix, _ string, body *wire, reqID uint64) (Result, error) {
	var raw []byte
	var err error
	if body != nil && method != http.MethodDelete {
		raw, err = json.Marshal(body)
		if err != nil {
			return Result{}, err
		}
	}
	backoff := 15 * time.Millisecond
	var last error
	for {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return Result{}, last
			}
			return Result{}, err
		}
		url := c.pick() + "/v1/kv/" + suffix
		res, rerr := c.once(ctx, method, url, raw, reqID)
		if rerr == nil && res.Error == "" {
			return toResult(res), nil
		}
		if rerr == nil && res.Error == "not leader" {
			c.follow(res.LeaderAddr)
			last = errors.New("not leader")
		} else if rerr != nil {
			last = rerr
			c.rotate()
		} else {
			return Result{}, errors.New(res.Error)
		}
		select {
		case <-ctx.Done():
			if last != nil {
				return Result{}, last
			}
			return Result{}, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

func (c *Client) once(ctx context.Context, method, url string, raw []byte, reqID uint64) (resp, error) {
	actx, cancel := context.WithTimeout(ctx, c.attempt)
	defer cancel()
	var rdr io.Reader
	if raw != nil && method != http.MethodGet {
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(actx, method, url, rdr)
	if err != nil {
		return resp{}, err
	}
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Client-Id", c.id)
		req.Header.Set("X-Request-Id", fmt.Sprintf("%d", reqID))
	}
	httpResp, err := c.http.Do(req)
	if err != nil {
		return resp{}, err
	}
	defer httpResp.Body.Close()
	var out resp
	if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil && !errors.Is(err, io.EOF) {
		return resp{}, err
	}
	if httpResp.StatusCode == http.StatusConflict || out.Error == "not leader" {
		out.Error = "not leader"
		return out, nil
	}
	if httpResp.StatusCode >= 300 && out.Error == "" {
		out.Error = httpResp.Status
	}
	return out, nil
}

func (c *Client) pick() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.urls) == 0 {
		return ""
	}
	return strings.TrimRight(c.urls[c.cursor%len(c.urls)], "/")
}

func (c *Client) rotate() {
	c.mu.Lock()
	c.cursor++
	c.mu.Unlock()
}

func (c *Client) follow(addr string) {
	if addr == "" {
		c.rotate()
		return
	}
	addr = strings.TrimRight(addr, "/")
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, u := range c.urls {
		if strings.TrimRight(u, "/") == addr {
			c.cursor = i
			return
		}
	}
	c.urls = append(c.urls, addr)
	c.cursor = len(c.urls) - 1
}

func toResult(r resp) Result {
	return Result{
		OK: r.OK, Found: r.Found, Value: r.Value, Swapped: r.Swapped,
		Prev: r.Prev, PrevFound: r.PrevFound, Index: r.Index, Leader: r.Leader,
	}
}
