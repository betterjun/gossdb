package ssdb

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Client struct {
	sock    *net.TCPConn
	recvBuf bytes.Buffer
}

// Connect returns a Client.
func Connect(ip string, port int) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	var c Client
	c.sock = sock
	return &c, nil
}

// Close closes the Client connection.
func (c *Client) Close() error {
	return c.sock.Close()
}

// Auth verifies the password for the server.
func (c *Client) Auth(pwd interface{}) error {
	resp, err := c.do("auth", pwd)
	if err != nil {
		return err
	}
	// resp[0] is the status, could be ok, not_found, error, fail, client_error
	if resp[0] != "ok" {
		return fmt.Errorf(resp[0])
	}
	return nil
}

// DBsize returns the approxy size of server in bytes.
func (c *Client) DBsize() (int64, error) {
	resp, err := c.do("dbsize")
	if err != nil {
		return 0, err
	}
	if resp[0] != "ok" {
		return 0, fmt.Errorf(resp[0])
	}
	return strconv.ParseInt(resp[1], 10, 64)
}

// FlushDB deletes all data in ssdb server. If type is provided, delete all data of specific type.
// The optional dataType, could be kv, hash, zset, list, and empty to delete all.
// Notice: The command "flushdb" is not a real command until 1.9.2, before that,
// it is provided by ssdb-cli, not on the server side.
func (c *Client) FlushDB(dataType string) (string, error) {
	resp, err := c.do("flushdb", dataType)
	if err != nil {
		return "", err
	}
	if resp[0] != "ok" {
		return "", fmt.Errorf(resp[0])
	}
	fmt.Printf("FlushDB returns line:%v\n", len(resp))
	return strings.Join(resp[1:], ""), nil
}

// Info returns information about the server.
// The optional dataType, could be cmd, leveldb, and empty for cmd.
func (c *Client) Info(dataType string) (string, error) {
	resp, err := c.do("info", dataType)
	if err != nil {
		return "", err
	}
	if resp[0] != "ok" {
		return "", fmt.Errorf(resp[0])
	}
	fmt.Printf("Info returns line:%v\n", len(resp))
	return strings.Join(resp[1:], ""), nil
}

// Set sets the value of the key.
func (c *Client) Set(key string, value interface{}) error {
	resp, err := c.do("set", key, value)
	fmt.Printf("Set returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return nil
	}
	return fmt.Errorf(resp[0])
}

// Setx sets the value of the key, with a number of seconds to live.
func (c *Client) Setx(key string, value interface{}, ttl int64) error {
	resp, err := c.do("setx", key, value, ttl)
	fmt.Printf("Setx returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return nil
	}
	return fmt.Errorf(resp[0])
}

// Setnx sets the value only when the key doesn't exist.
// Return values: 1: value is set, 0: key already exists.
func (c *Client) Setnx(key string, value interface{}) (int64, error) {
	resp, err := c.do("setnx", key, value)
	fmt.Printf("Setnx returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return 0, err
	}

	switch len(resp) {
	case 0:
		return 0, fmt.Errorf("no response received")
	case 1:
		return 0, fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			return strconv.ParseInt(resp[1], 10, 64)
		} else {
			return 0, fmt.Errorf(resp[0])
		}
	}
}

// Get returns the value of the key. If the key is not existed, error "not_found" is returned.
func (c *Client) Get(key string) (string, error) {
	resp, err := c.do("get", key)
	fmt.Printf("Get returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return "", err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1], nil
	}
	return "", fmt.Errorf(resp[0])
}

// Getset Sets a value and returns the previous entry at that key.
// If the key already exists, the value related to that key is returned.
// Otherwise return not_found Status Code. The value is either added or updated.
func (c *Client) Getset(key string, value interface{}) (string, error) {
	resp, err := c.do("getset", key, value)
	fmt.Printf("Getset returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return "", err
	}

	switch len(resp) {
	case 0:
		return "", fmt.Errorf("no response received")
	case 1:
		return "", fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			return strings.Join(resp[1:], ""), nil
		} else {
			return "", fmt.Errorf(resp[0])
		}
	}
}

// Del deletes the specified key.
func (c *Client) Del(key string) error {
	resp, err := c.do("del", key)
	fmt.Printf("Del returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return err
	}

	//response looks like this: [ok 1]
	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return fmt.Errorf(resp[0])
}

// Exists checks whether the key is existed.
// If the key exists, return 1, otherwise return 0.
func (c *Client) Exists(key string) (int64, error) {
	resp, err := c.do("exists", key)
	fmt.Printf("Exists returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return 0, err
	}

	switch len(resp) {
	case 0:
		return 0, fmt.Errorf("no response received")
	case 1:
		return 0, fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			return strconv.ParseInt(resp[1], 10, 64)
		} else {
			return 0, fmt.Errorf(resp[0])
		}
	}
}

// Expire sets the time left to live in seconds, only for keys of KV type.
// If the key exists and ttl is set, return 1, otherwise return 0.
func (c *Client) Expire(key string, ttl int64) (int64, error) {
	resp, err := c.do("expire", key, ttl)
	fmt.Printf("Expire returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return 0, err
	}

	switch len(resp) {
	case 0:
		return 0, fmt.Errorf("no response received")
	case 1:
		return 0, fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			return strconv.ParseInt(resp[1], 10, 64)
		} else {
			return 0, fmt.Errorf(resp[0])
		}
	}
}

// Ttl returns the time left to live in seconds, only for keys of KV type.
// Time to live of the key, in seconds, -1 if there is no associated expire to the key.
func (c *Client) Ttl(key string) (int64, error) {
	resp, err := c.do("ttl", key)
	fmt.Printf("Ttl returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return 0, err
	}

	switch len(resp) {
	case 0:
		return 0, fmt.Errorf("no response received")
	case 1:
		return 0, fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			return strconv.ParseInt(resp[1], 10, 64)
		} else {
			return 0, fmt.Errorf(resp[0])
		}
	}
}

// Incr increase the key by number.
// The new value. If the old value cannot be converted to an integer, returns error Status Code.
func (c *Client) Incr(key string, number int64) (int64, error) {
	resp, err := c.do("incr", key, number)
	fmt.Printf("Incr returns line:%v, %v\n", len(resp), strings.Join(resp, " "))
	if err != nil {
		return 0, err
	}

	switch len(resp) {
	case 0:
		return 0, fmt.Errorf("no response received")
	case 1:
		return 0, fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			return strconv.ParseInt(resp[1], 10, 64)
		} else {
			return 0, fmt.Errorf(resp[0])
		}
	}
}

func (c *Client) do(args ...interface{}) ([]string, error) {
	err := c.send(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.recv()
	return resp, err
}

func (c *Client) send(args []interface{}) error {
	var buf bytes.Buffer
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case []string:
			for _, s := range arg {
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case int:
			s = fmt.Sprintf("%d", arg)
		case int64:
			s = fmt.Sprintf("%d", arg)
		case float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	_, err := c.sock.Write(buf.Bytes())
	return err
}

func (c *Client) recv() ([]string, error) {
	var tmp [8192]byte
	for {
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recvBuf.Write(tmp[0:n])
	}
}

func (c *Client) parse() []string {
	resp := []string{}
	buf := c.recvBuf.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recvBuf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.recvBuf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	//fmt.Printf("buf.size: %d packet not ready...\n", len(buf))
	return []string{}
}
