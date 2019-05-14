
package main

import (
	"fmt"
	"net"
	"strings"
	"bytes"
)

type RedisClient struct {
	Network string
	Addr    string
	conn    net.Conn
	sendCmd *bytes.Buffer
	reply   []byte
}

func (c *RedisClient) connect() {
	conn, err := net.Dial(c.Network, c.Addr)
	if err != nil {
		fmt.Println("connect err ", err)
	}
	c.conn = conn
}

func (c *RedisClient) Do(key, cmd string)(string,error) {
	c.do(key,cmd)
	c.write()
	r,err := c.getReply()
	return r,err
}

func (c *RedisClient) do(key,cmd string) {
	cmd_argv := strings.Fields(cmd)
	c.sendCmd.WriteString(fmt.Sprintf("*%d\r\n", len(cmd_argv)+1 ))
	c.sendCmd.WriteString(fmt.Sprintf("$%d\r\n", len(key)))
	c.sendCmd.WriteString(key)
	c.sendCmd.WriteString("\r\n")
	for _, arg := range cmd_argv {
		c.sendCmd.WriteString(fmt.Sprintf("$%d\r\n", len(arg)))
		c.sendCmd.WriteString(arg)
		c.sendCmd.WriteString("\r\n")
	}
}

func (c *RedisClient) write() {
	c.conn.Write(c.sendCmd.Bytes())
	c.sendCmd.Reset()
}

func (c *RedisClient) getReply() (string,error) {
	n,err := c.conn.Read(c.reply)
	if err != nil {
		return "",err
	}
	switch {
	case c.reply[0] == '+' || c.reply[0] == '-' || c.reply[0] == ':':
		return string(c.reply[1:n]),nil
	}
	return string(c.reply[2:]),nil
}

func NewClient(addr,network string) *RedisClient {
	c := RedisClient{
		Network:network,
		Addr:addr,
		sendCmd:&bytes.Buffer{},
		reply:make([]byte,2048),
	}
	c.connect()
	return &c
}

func main() {
	c := NewClient("127.0.0.1:6379","tcp")
	set,err := c.Do("set","a 10")
	get,err := c.Do("get","a")
	incr,err := c.Do("incr","a")
	fmt.Println("set is ",set,"get is",get,"incr is",incr,"err is",err)
}
