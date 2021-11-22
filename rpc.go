package rpc_pool

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"
)

func NewRcpCli(addr string, timeout time.Duration) *RpcCli {
	return &RpcCli{Addr: addr, Timeout: timeout}
}

type RpcCli struct {
	sync.Mutex
	Addr    string
	Timeout time.Duration
	Cli     *rpc.Client
}

func (c *RpcCli) String() string {
	return fmt.Sprintf("<add:%s timeout:%d> cli:%v\n", c.Addr, c.Timeout, c.Cli)
}

func (c *RpcCli) GetAddr() string {
	return c.Addr
}

func (c *RpcCli) ServerConn() error {
	c.Lock()
	defer c.Unlock()
	conn, err := net.DialTimeout("tcp", c.Addr, time.Duration(c.Timeout))
	if err != nil {
		return err
	}
	c.Cli = jsonrpc.NewClient(conn)
	return nil
}

func (c *RpcCli) Close() {
	c.Lock()
	defer c.Unlock()
	if c.Cli != nil {
		c.Cli.Close()
		c.Cli = nil
	}
}

func (c *RpcCli) Call(method string, args interface{}, reply interface{}) error {
	if c.Cli == nil {
		if err := c.ServerConn(); err != nil {
			return err
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- c.Cli.Call(method, args, reply)
	}()

	select {
	case <-time.After(time.Second * c.Timeout):
		c.Close()
		return errors.New("call rpc timeout")
	case err := <-done:
		if err != nil {
			c.Close()
			return err
		}
	}
	return nil
}
