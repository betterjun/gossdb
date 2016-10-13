package ssdb

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
)

type Client struct {
	sock    *net.TCPConn
	recvBuf bytes.Buffer
	err     error
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
func (c *Client) Auth(pwd string) error {
	return c.doReturn("auth", pwd)
}

// DBsize returns the approxy size of server in bytes.
func (c *Client) DBsize() (int64, error) {
	return c.doReturnInt("dbsize")
}

// FlushDB deletes all data in ssdb server. If type is provided, delete all data of specific type.
// The optional dataType, could be kv, hash, zset, list, and empty to delete all.
// Notice: The command "flushdb" is not a real command until 1.9.2, before that,
// it is provided by ssdb-cli, not on the server side.
func (c *Client) FlushDB(dataType string) error {
	return c.doReturn("flushdb", dataType)
}

// Info returns information about the server.
// The optional dataType, could be cmd, leveldb, and empty for cmd.
func (c *Client) Info(dataType string) (string, error) {
	return c.doReturnString("info", dataType)
}

// Set sets the value of the key.
func (c *Client) Set(key string, value interface{}) error {
	return c.doReturn("set", key, value)
}

// Setx sets the value of the key, with a number of seconds to live.
func (c *Client) Setx(key string, value interface{}, ttl int64) error {
	return c.doReturn("setx", key, value, ttl)
}

// Setnx sets the value only when the key doesn't exist.
// Return values: 1: value is set, 0: key already exists.
func (c *Client) Setnx(key string, value interface{}) (int64, error) {
	return c.doReturnInt("setnx", key, value)
}

// Get returns the value of the key. If the key is not existed, error "not_found" is returned.
func (c *Client) Get(key string) (string, error) {
	return c.doReturnString("get", key)
}

// Getset Sets a value and returns the previous entry at that key.
// If the key already exists, the value related to that key is returned.
// Otherwise return not_found Status Code. The value is either added or updated.
func (c *Client) Getset(key string, value interface{}) (string, error) {
	return c.doReturnString("getset", key, value)
}

// Del deletes the specified key.
func (c *Client) Del(key string) error {
	return c.doReturn("del", key)
}

// Exists checks whether the key is existed.
// If the key exists, return 1, otherwise return 0.
func (c *Client) Exists(key string) (int64, error) {
	return c.doReturnInt("exists", key)
}

// Expire sets the time left to live in seconds, only for keys of KV type.
// If the key exists and ttl is set, return 1, otherwise return 0.
func (c *Client) Expire(key string, ttl int64) (int64, error) {
	return c.doReturnInt("expire", key, ttl)
}

// Ttl returns the time left to live in seconds, only for keys of KV type.
// Time to live of the key, in seconds, -1 if there is no associated expire to the key.
func (c *Client) Ttl(key string) (int64, error) {
	return c.doReturnInt("ttl", key)
}

// Incr increase the key by number.
// The new value. If the old value cannot be converted to an integer, returns error Status Code.
func (c *Client) Incr(key string, number int64) (int64, error) {
	return c.doReturnInt("incr", key, number)
}

/* Setbit changes a single bit of a string. The string is auto expanded.
Parameters
    key -
    offset - bit offset, must in range of [0, 1073741824].
    val - 0 or 1.
Return Value
	The value of the bit before it was set: 0 or 1. If val is not 0 or 1, returns false.
*/
func (c *Client) Setbit(key string, offset int32, value int8) (int64, error) {
	return c.doReturnInt("setbit", key, offset, value)
}

/* Getbit return a single bit out of a string.
Parameters
    key -
    offset - bit offset.
Return Value
	0 or 1.
*/
func (c *Client) Getbit(key string, offset int32) (int64, error) {
	return c.doReturnInt("getbit", key, offset)
}

/*
Countbit counts the number of set bits (population counting) in a string.
Unlike bitcount, it takes part of the string by start and size, not start and end.
Parameters
    key -
    start - Optional, inclusive, if start is negative, count from start'th character from the end of string.
    size - Optional, if size is negative, then that many characters will be omitted from the end of string.
Return Value
	The number of bits set to 1.
*/
func (c *Client) Countbit(key string, args ...int) (int64, error) {
	return c.doReturnInt("countbit", key, args)
}

/*
Bitcount counts the number of set bits (population counting) in a string. Like Redis's bitcount.
Parameters
    key -
    start - Optional, inclusive, if start is negative, count from start'th character from the end of string.
    end - Optional, inclusive.
Return Value
	The number of bits set to 1.
*/
func (c *Client) Bitcount(key string, args ...int) (int64, error) {
	return c.doReturnInt("bitcount", key, args)
}

/*
Substr returns part of a string, like PHP's substr() function.
Parameters
    key -
    start - Optional, the offset of first byte returned. If start is negative,
		the returned string will start at the start'th character from the end of string.
    size - Optional, number of bytes returned. If size is negative,
		then that many characters will be omitted from the end of string.
Return Value
	The extracted part of the string.
*/
func (c *Client) Substr(key string, args ...int) (string, error) {
	return c.doReturnString("substr", key, args)
}

/*
Strlen returns the number of bytes of a string.
Parameters
    key -
Return Value
	The number of bytes of the string, if key not exists, returns 0.
*/
func (c *Client) Strlen(key string) (int64, error) {
	return c.doReturnInt("strlen", key)
}

func (c *Client) Keys(keyStart, keyEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("keys", keyStart, keyEnd, limit)
}

func (c *Client) Rkeys(keyStart, keyEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("rkeys", keyStart, keyEnd, limit)
}

func (c *Client) Scan(keyStart, keyEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("scan", keyStart, keyEnd, limit)
}

func (c *Client) Rscan(keyStart, keyEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("rscan", keyStart, keyEnd, limit)
}

func (c *Client) doReturn(args ...interface{}) error {
	err := c.send(args)
	if err != nil {
		return err
	}

	resp, err := c.recv()
	if err != nil {
		return err
	}
	fmt.Printf("doReturn: %v returns %v lines, %q\n", args[0], len(resp), strings.Join(resp, "|"))

	switch len(resp) {
	case 0:
		return fmt.Errorf("no response received")
	default:
		if resp[0] == "ok" {
			return nil
		} else {
			return fmt.Errorf(resp[0])
		}
	}
}

func (c *Client) doReturnInt(args ...interface{}) (int64, error) {
	err := c.send(args)
	if err != nil {
		return 0, err
	}

	resp, err := c.recv()
	if err != nil {
		return 0, err
	}
	fmt.Printf("doReturnInt: %v returns %v lines, %q\n", args[0], len(resp), strings.Join(resp, "|"))

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

func (c *Client) doReturnString(args ...interface{}) (string, error) {
	err := c.send(args)
	if err != nil {
		return "", err
	}

	resp, err := c.recv()
	if err != nil {
		return "", err
	}
	fmt.Printf("doReturnString: %v returns %v lines, %q\n", args[0], len(resp), strings.Join(resp, "|"))

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

func (c *Client) doReturnStringSlice(args ...interface{}) ([]string, error) {
	err := c.send(args)
	if err != nil {
		return nil, err
	}

	resp, err := c.recv()
	if err != nil {
		return nil, err
	}
	fmt.Printf("doReturnString: %v returns %v lines, %q\n", args[0], len(resp), strings.Join(resp, "|"))

	switch len(resp) {
	case 0:
		return nil, fmt.Errorf("no response received")
	case 1:
		return nil, fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			return resp[1:], nil
		} else {
			return nil, fmt.Errorf(resp[0])
		}
	}
}

func (c *Client) doReturnStringMap(args ...interface{}) (map[string]string, error) {
	err := c.send(args)
	if err != nil {
		return nil, err
	}

	resp, err := c.recv()
	if err != nil {
		return nil, err
	}
	fmt.Printf("doReturnString: %v returns %v lines, %q\n", args[0], len(resp), strings.Join(resp, "|"))

	switch len(resp) {
	case 0:
		return nil, fmt.Errorf("no response received")
	case 1:
		return nil, fmt.Errorf(resp[0])
	default:
		if resp[0] == "ok" {
			kv := resp[1:]
			m := make(map[string]string)
			for i := 0; i < len(kv); i = i + 2 {
				m[kv[i]] = kv[i+1]
			}
			return m, nil
		} else {
			return nil, fmt.Errorf(resp[0])
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
		case []int:
			for _, d := range arg {
				s = fmt.Sprintf("%d", d)
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case int8, int16, int32, int64, int,
			uint8, uint16, uint32, uint64, uint:
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
			a := reflect.TypeOf(arg)
			return fmt.Errorf("bad arguments of type %v", a.Kind())
			//return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	_, c.err = c.sock.Write(buf.Bytes())
	return c.err
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
			c.err = err
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
