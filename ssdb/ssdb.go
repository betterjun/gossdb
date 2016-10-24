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

// Keys works likely Scan, but only return the keys.
// Just refer the Scan description below.
func (c *Client) Keys(keyStart, keyEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("keys", keyStart, keyEnd, limit)
}

// Rkeys works likely Keys, but in reverse order.
func (c *Client) Rkeys(keyStart, keyEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("rkeys", keyStart, keyEnd, limit)
}

/*
Scan lists key-value pairs with keys in range (keyStart, keyEnd].
("", ""] means no range limit.
This command can do wildchar * like search, but only prefix search,
and the * char must never occur in key_start and key_end!

Parameters
    keyStart - The lower bound(not included) of keys to be returned, empty string means -inf(no limit).
    keyEnd - The upper bound(inclusive) of keys to be returned, empty string means +inf(no limit).
    limit - Up to that many pairs will be returned.
Return Value
	An associative array containing the key-value pairs. Like [k1 v1 k2 v2 ...]
*/
func (c *Client) Scan(keyStart, keyEnd string, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("scan", keyStart, keyEnd, limit)
}

// Rscan works likely Scan, but in reverse order.
func (c *Client) Rscan(keyStart, keyEnd string, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("rscan", keyStart, keyEnd, limit)
}

/*
MultiSet sets multiple key-value pairs(kvs) in one method call.
Parameters
    key1 value1 key2 value2 ...
Return Value
	Number of keys are set.
*/
func (c *Client) MultiSet(args ...interface{}) (int64, error) {
	return c.doReturnInt("multi_set", args)
}

/*
MultiGet get the values related to the specified multiple keys.
Parameters
    key1 key2 ...
Return Value
	Key-value list.
*/
func (c *Client) MultiGet(keys ...interface{}) ([]string, error) {
	return c.doReturnStringSlice("multi_get", keys)
}

/*
MultiDel deletes specified keys.
Parameters
    key1 key2 ...
Return Value
	Number of keys are deleted.
*/
func (c *Client) MultiDel(keys ...interface{}) (int64, error) {
	return c.doReturnInt("multi_del", keys)
}

// For hash map operations.
/*
Hset sets the string value in argument as value of the key of a hashmap.
Parameters
    name - The name of the hashmap
    key - The key of the key-value pair in the hashmap
    value - The value of the key-value pair in the hashmap
Return Value
	Returns 1 if key is a new key in the hashmap and value is set, else returns 0.
*/
func (c *Client) Hset(name, key string, value interface{}) (int64, error) {
	return c.doReturnInt("hset", name, key, value)
}

/*
Hget gets the value related to the specified key of a hashmap.
Parameters
    name - The name of the hashmap
    key - The key of the key-value pair in the hashmap
Return Value
	Return the value to the key, if the key does not exists, return not_found Status Code.
*/
func (c *Client) Hget(name, key string) (string, error) {
	return c.doReturnString("hget", name, key)
}

// Hdel deletes specified key of a hashmap.
// If the key exists, return 1, otherwise return 0.
func (c *Client) Hdel(name, key string) (int64, error) {
	return c.doReturnInt("hdel", name, key)
}

/*
Hincr increases the number stored at key in a hashmap by num. The num argument could be a negative integer.
The old number is first converted to an integer before increment, assuming it was stored as literal integer.
Parameters
    name - the name of the hashmap
    key - The key of the key-value pair in the hashmap
    num - Optional, must be a signed integer, default is 1
Return Value
	The new value. If the old value cannot be converted to an integer, returns error Status Code.
*/
func (c *Client) Hincr(name, key string, num int) (int64, error) {
	return c.doReturnInt("hincr", name, key, num)
}

// Hexists verifies if the specified key exists in a hashmap.
// If the key exists, return 1, otherwise return 0.
func (c *Client) Hexists(name, key string) (int64, error) {
	return c.doReturnInt("hexists", name, key)
}

// Hsize returns the number of key-value pairs in the hashmap.
func (c *Client) Hsize(name string) (int64, error) {
	return c.doReturnInt("hsize", name)
}

// Hlist lists hashmap names in range (name_start, name_end].
func (c *Client) Hlist(nameStart, nameEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("hlist", nameStart, nameEnd, limit)
}

// Hrlist works like Hlist, but in reverse order.
func (c *Client) Hrlist(nameStart, nameEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("hrlist", nameStart, nameEnd, limit)
}

// Hrlist works like Hlist, but in reverse order.
func (c *Client) Hkeys(name, keyStart, keyEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("hkeys", name, keyStart, keyEnd, limit)
}

// Hgetall returns the whole hash, as an array of strings indexed by strings.
func (c *Client) Hgetall(name string) (OrderedMap, error) {
	return c.doReturnStringMap("hgetall", name)
}

/*
Hscan lists key-value pairs of a hashmap with keys in range (key_start, key_end].
For more details, refer Scan.
*/
func (c *Client) Hscan(name, keyStart, keyEnd string, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("hscan", name, keyStart, keyEnd, limit)
}

// Hrscan works likely Hscan, but in reverse order.
func (c *Client) Hrscan(name, keyStart, keyEnd string, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("hrscan", name, keyStart, keyEnd, limit)
}

/*
Hclear deletes all keys in a hashmap.
The number of keys deleted in that hashmap is returned.
*/
func (c *Client) Hclear(name string) (int64, error) {
	return c.doReturnInt("hclear", name)
}

/*
MultiHset sets multiple key-value pairs(kvs) of a hashmap in one method call.
Parameters
    name key1 value1 key2 value2 ...
Return Value
	Number of keys are set.
*/
func (c *Client) MultiHset(args ...interface{}) (int64, error) {
	return c.doReturnInt("multi_hset", args)
}

/*
MultiHget get the values related to the specified multiple keys of a hashmap.
Parameters
    name key1 key2 ...
Return Value
	Key-value list.
*/
func (c *Client) MultiHget(keys ...interface{}) ([]string, error) {
	return c.doReturnStringSlice("multi_hget", keys)
}

/*
MultiHdel deletes specified multiple keys in a hashmap.
Parameters
    name key1 key2 ...
Return Value
	Number of keys are deleted.
*/
func (c *Client) MultiHdel(keys ...interface{}) (int64, error) {
	return c.doReturnInt("multi_hdel", keys)
}

// For hash map operations.
/*
Zset sets the score of the key of a zset.
Parameters
    name - The name of the zset
    key - The key of the key-score pair in the hashmap
    score - The score of the key-score pair in the hashmap
Return Value
	Returns 1 if key is not existed before, else returns 0.
*/
func (c *Client) Zset(name, key string, score int64) (int64, error) {
	return c.doReturnInt("zset", name, key, score)
}

/*
Hget gets the score related to the specified key of a zset
Parameters
    name - The name of the zset
    key - The key of the key-score pair in the zset
Return Value
	Return the value to the key, if the key does not exists, return not_found Status Code.
	Return null if key not found, false on error, otherwise, the score related to this key is returned.
*/
func (c *Client) Zget(name, key string) (int64, error) {
	return c.doReturnInt("zget", name, key)
}

// Hdel deletes specified key of a hashmap.
// If the key exists, return 1, otherwise return 0.
func (c *Client) Zdel(name, key string) (int64, error) {
	return c.doReturnInt("zdel", name, key)
}

/*
Hincr increases the number stored at key in a hashmap by num. The num argument could be a negative integer.
The old number is first converted to an integer before increment, assuming it was stored as literal integer.
Parameters
    name - the name of the hashmap
    key - The key of the key-value pair in the hashmap
    num - Optional, must be a signed integer, default is 1
Return Value
	The new value. If the old value cannot be converted to an integer, returns error Status Code.
*/
func (c *Client) Zincr(name, key string, num int) (int64, error) {
	return c.doReturnInt("zincr", name, key, num)
}

// Hexists verifies if the specified key exists in a hashmap.
// If the key exists, return 1, otherwise return 0.
func (c *Client) Zexists(name, key string) (int64, error) {
	return c.doReturnInt("zexists", name, key)
}

// Hsize returns the number of key-value pairs in the hashmap.
func (c *Client) Zsize(name string) (int64, error) {
	return c.doReturnInt("zsize", name)
}

// Hlist lists hashmap names in range (name_start, name_end].
func (c *Client) Zlist(nameStart, nameEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("zlist", nameStart, nameEnd, limit)
}

// Hrlist works like Hlist, but in reverse order.
func (c *Client) Zrlist(nameStart, nameEnd string, limit int) ([]string, error) {
	return c.doReturnStringSlice("zrlist", nameStart, nameEnd, limit)
}

// Hrlist works like Hlist, but in reverse order.
func (c *Client) Zkeys(name, keyStart string, scoreStart, scoreEnd int64, limit int) ([]string, error) {
	return c.doReturnStringSlice("zkeys", name, keyStart, scoreStart, scoreEnd, limit)
}

/*
Hscan lists key-value pairs of a hashmap with keys in range (key_start, key_end].
For more details, refer Scan.
*/
func (c *Client) Zscan(name, keyStart string, scoreStart, scoreEnd int64, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("zscan", name, keyStart, scoreStart, scoreEnd, limit)
}

// Hrscan works likely Hscan, but in reverse order.
func (c *Client) Zrscan(name, keyStart string, scoreStart, scoreEnd int64, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("zrscan", name, keyStart, scoreStart, scoreEnd, limit)
}

/*
Description

Important! This method may be extremly SLOW! May not be used in an online service.

Returns the rank(index) of a given key in the specified sorted set, starting at 0 for the item with the smallest score. zrrank starts at 0 for the item with the largest score.
Parameters

    name - The name of the zset.
    key -

Return Value

false on error, otherwise the rank(index) of a specified key, start at 0. null if not found.
*/
func (c *Client) Zrank(name, key string) (int64, error) {
	return c.doReturnInt("zrank", name, key)
}

// Hrscan works likely Hscan, but in reverse order.
func (c *Client) Zrrank(name, key string) (int64, error) {
	return c.doReturnInt("zrrank", name, key)
}

/*
Description

Important! This method is SLOW for large offset!

Returns a range of key-score pairs by index range [offset, offset + limit). Zrrange iterates in reverse order.
Parameters

    name - The name of the zset.
    offset - Positive integer, the returned pairs will start at this offset.
    limit - Positive integer, up to this number of pairs will be returned.

Return Value

false on error, otherwise an array containing key-score pairs.
*/
func (c *Client) Zrange(name string, offset, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("zrange", name, offset, limit)
}

// Hrscan works likely Hscan, but in reverse order.
func (c *Client) Zrrange(name string, offset, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("zrrange", name, offset, limit)
}

/*
Hclear deletes all keys in a hashmap.
The number of keys deleted in that hashmap is returned.
*/
func (c *Client) Zclear(name string) (int64, error) {
	return c.doReturnInt("zclear", name)
}

/*
Description

Returns the number of elements of the sorted set stored at the specified key which have scores in the range [start,end].
Parameters

    name - The name of the zset.
    start - The minimum score related to keys(inclusive), empty string means -inf(no limit).
    end - The maximum score related to keys(inclusive), empty string means +inf(no limit).

Return Value

false on error, or the number of keys in specified range.
*/
func (c *Client) Zcount(name string, start, end int64) (int64, error) {
	return c.doReturnInt("zcount", name, start, end)
}

func (c *Client) Zsum(name string, start, end int64) (int64, error) {
	return c.doReturnInt("zsum", name, start, end)
}
func (c *Client) Zavg(name string, start, end int64) (float64, error) {
	str, err := c.doReturnString("zavg", name, start, end)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(str, 64)
}
func (c *Client) Zremrangebyrank(name string, start, end int64) (int64, error) {
	return c.doReturnInt("zremrangebyrank", name, start, end)
}
func (c *Client) Zremrangebyscore(name string, start, end int64) (int64, error) {
	return c.doReturnInt("zremrangebyscore", name, start, end)
}

/*
Delete and return `limit` element(s) from front of the zset.
Parameters

    name - The name of the zset.
    limit - The number of elements to be deleted and returned.

Return Value

false on error, otherwise an array containing key-score pairs.
*/
func (c *Client) Zpopfront(name string, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("zpop_front", name, limit)
}

func (c *Client) Zpopback(name string, limit int) (OrderedMap, error) {
	return c.doReturnStringMap("zpop_back", name, limit)
}

/*
MultiHset sets multiple key-value pairs(kvs) of a hashmap in one method call.
Parameters
    name key1 value1 key2 value2 ...
Return Value
	Number of keys are set.
*/
func (c *Client) MultiZset(args ...interface{}) (int64, error) {
	return c.doReturnInt("multi_zset", args)
}

/*
MultiHget get the values related to the specified multiple keys of a hashmap.
Parameters
    name key1 key2 ...
Return Value
	Key-value list.
*/
func (c *Client) MultiZget(keys ...interface{}) ([]string, error) {
	return c.doReturnStringSlice("multi_zget", keys)
}

/*
MultiHdel deletes specified multiple keys in a hashmap.
Parameters
    name key1 key2 ...
Return Value
	Number of keys are deleted.
*/
func (c *Client) MultiZdel(keys ...interface{}) (int64, error) {
	return c.doReturnInt("multi_zdel", keys)
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
		if resp[0] == "ok" {
			return 0, fmt.Errorf("no data found")
		} else {
			return 0, fmt.Errorf(resp[0])
		}
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
		if resp[0] == "ok" {
			return "", fmt.Errorf("no data found")
		} else {
			return "", fmt.Errorf(resp[0])
		}
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
		if resp[0] == "ok" {
			return nil, fmt.Errorf("no data found")
		} else {
			return nil, fmt.Errorf(resp[0])
		}

	default:
		if resp[0] == "ok" {
			return resp[1:], nil
		} else {
			return nil, fmt.Errorf(resp[0])
		}
	}
}

func (c *Client) doReturnStringMap(args ...interface{}) (OrderedMap, error) {
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
		if resp[0] == "ok" {
			return nil, fmt.Errorf("no data found")
		} else {
			return nil, fmt.Errorf(resp[0])
		}
	default:
		if resp[0] == "ok" {
			return newMap(resp[1:]), nil
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
	bytes, err := formatData(args)
	if err != nil {
		return err
	}
	_, c.err = c.sock.Write(bytes)
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
