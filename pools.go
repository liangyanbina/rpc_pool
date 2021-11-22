package rpc_pool

import (
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

func NewCreateRpcPool(addrs []string, retry, timeout, resetTime int64) *ConnPool {
	p := &ConnPool{Retry: int(retry), ResetTime: resetTime}
	rand.Seed(time.Now().UnixNano())
	for _, idx := range rand.Perm(len(addrs)) {
		// log.Println(addrs[idx])
		p.AddCli(NewRcpCli(addrs[idx], time.Duration(timeout)))
	}
	return p
}

type RcpClier interface {
	Call(method string, args interface{}, reply interface{}) error
	Close()
	ServerConn() error
	GetAddr() string
}

type ConnPool struct {
	sync.Mutex
	RpcClis   []RcpClier
	Idx       int
	Retry     int   //异常重试次数
	ResetTime int64 // 复位连接池下标周期,单位：秒
}

func (c *ConnPool) AddCli(cli *RpcCli) {
	c.Lock()
	defer c.Unlock()
	c.RpcClis = append(c.RpcClis, cli)
}

func (c *ConnPool) IncrIdx() {
	c.Lock()
	defer c.Unlock()
	c.RpcClis[c.Idx].Close()
	c.Idx += 1
	if c.Idx >= len(c.RpcClis) {
		c.Idx = 0
		return
	}
}

func (c *ConnPool) SetIdx(idx int) {
	c.Lock()
	defer c.Unlock()
	c.RpcClis[c.Idx].Close()
	c.Idx = idx
}

func (c *ConnPool) Destroy() {
	c.Lock()
	defer c.Unlock()
	for idx := range c.RpcClis {
		c.RpcClis[idx].Close()
	}
}

func (c *ConnPool) GetIdx() int {
	c.Lock()
	defer c.Unlock()
	return c.Idx
}

func (c *ConnPool) Call(method string, args interface{}, reply interface{}) error {
	if time.Now().Unix()%c.ResetTime == 0 && c.GetIdx() != 0 {
		c.SetIdx(0)
	}
	firstIdx := c.GetIdx()
	retry := 0
	for {
		curIdx := c.GetIdx()
		err := c.RpcClis[curIdx].Call(method, args, reply)
		// log.Printf("call %s cur_idx:%d idx:%d ", c.RpcClis[curIdx].GetAddr(), curIdx, firstIdx)
		if err != nil {
			log.Printf("dial %s fail: %v\r\n", c.RpcClis[curIdx].GetAddr(), err)
			retry++
			if retry >= c.Retry {
				retry = 0
				c.IncrIdx()
				if c.GetIdx() == firstIdx {
					return err
				}

			}
			time.Sleep(time.Duration(math.Pow(2.0, float64(retry))) * time.Second)
			continue
		}
		return nil
	}
}
