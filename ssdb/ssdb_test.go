package ssdb

import (
	"fmt"
	"testing"
)

var (
	ServerAddr = "192.168.254.22"
	ServerPort = 8888
	Password   = "CreateSessionResponseAtDecode-longlong"
)

func newPool() (*Pool, error) {
	if ServerAddr == "" || ServerPort == 0 {
		return nil, fmt.Errorf("server not configured")
	}
	return NewPool(ServerAddr, ServerPort, Password, 100)
}

/*
func TestKV(t *testing.T) {
	p, err := newPool()
	if err != nil {
		t.Fatal(err)
	}

	c := p.Get()

	size, err := c.DBsize()
	if err != nil {
		t.Fatalf("DBsize failed, err:%v\n", err)
	}
	t.Logf("DBsize result:%v\n", size)

	result, err := c.Info("cmd")
	if err != nil {
		t.Fatalf("Info failed, err:%v\n", err)
		t.Log(result)
	}
	//t.Logf("Info result:%v\n", result)

	result, err = c.Info("leveldb")
	if err != nil {
		t.Fatalf("Info failed, err:%v\n", err)
	}
	//t.Logf("Info result:%v\n", result)

	err = c.FlushDB("kv")
	if err != nil {
		t.Fatalf("FlushDB failed, err:%v\n", err)
	}

	err = c.FlushDB("")
	if err != nil {
		t.Fatalf("FlushDB failed, err:%v\n", err)
	}

	var key = "gossdb"
	var value = "value \t\t\tfor test\r\nsecond line"
	err = c.Set(key, value)
	if err != nil {
		t.Fatalf("Set failed, err:%v\n", err)
	}

	v, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get failed, err:%v\n", err)
	}
	if v != value {
		t.Fatalf("Get failed, expected:%v, got:%v\n", value, v)
	}

	err = c.Del(key)
	if err != nil {
		t.Fatalf("Del failed, err:%v\n", err)
	}

	v, err = c.Get(key)
	if err != nil {
		t.Logf("Get failed, err:%v\n", err)
	} else {
		t.Fatalf("Get key after deleted, value:%v\n", v)
	}

	err = c.Setx(key, value, 1)
	if err != nil {
		t.Fatalf("Setx failed, err:%v\n", err)
	}

	time.Sleep(time.Second * 2)

	v, err = c.Get(key)
	if err != nil {
		t.Logf("Get failed, err:%v\n", err)
	} else {
		t.Fatalf("Get key after ttl expired, value:%v\n", v)
	}

	ret, err := c.Exists(key)
	if err != nil {
		t.Fatalf("Exists failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Exists result:%v\n", ret)
	}

	ret, err = c.Setnx(key, value)
	if err != nil {
		t.Fatalf("Setnx failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Setnx result:%v\n", ret)
	}

	ret, err = c.Setnx(key, value)
	if err != nil {
		t.Fatalf("Setnx failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Setnx result:%v\n", ret)
	}

	ret, err = c.Exists(key)
	if err != nil {
		t.Fatalf("Exists failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Exists result:%v\n", ret)
	}

	var value2 = "value2"
	v, err = c.Getset(key, value2)
	if err != nil {
		t.Fatalf("Getset failed, err:%v\n", err)
	}
	if v != value {
		t.Fatalf("Getset result:%v\n", v)
	}

	ret, err = c.Ttl(key)
	if err != nil {
		t.Fatalf("Ttl failed, err:%v\n", err)
	}
	if ret != -1 {
		t.Fatalf("Ttl result:%v\n", ret)
	}

	ret, err = c.Expire(key, 1)
	if err != nil {
		t.Fatalf("Expire failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Expire result:%v\n", ret)
	}

	ret, err = c.Ttl(key)
	if err != nil {
		t.Fatalf("Ttl failed, err:%v\n", err)
	}
	if ret == -1 {
		t.Fatalf("Ttl result:%v\n", ret)
	}

	// the key is not existed
	intKey := "intKey"
	ret, err = c.Incr(intKey, 1)
	if err != nil {
		t.Logf("Incr failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Incr result, expected:%v, got:%v\n", 1, ret)
	}

	// the key is an integer
	ret, err = c.Incr(intKey, 1)
	if err != nil {
		t.Fatalf("Incr failed, err:%v\n", err)
	}
	if ret != 2 {
		t.Fatalf("Incr result, expected:%v, got:%v\n", 2, ret)
	}

	// the key is not an integer
	ret, err = c.Incr(key, 1)
	if err != nil {
		t.Logf("Incr failed, err:%v\n", err)
	}

	// even the key is not existed, setbit will set it.
	keyBitMap := "bitmap"
	ret, err = c.Setbit(keyBitMap, 100, 1)
	if err != nil {
		t.Fatalf("Setbit failed, err:%v\n", err)
	}

	ret, err = c.Getbit(keyBitMap, 100)
	if err != nil {
		t.Fatalf("Getbit failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Getbit result, expected:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Countbit(keyBitMap, 1, 10)
	if err != nil {
		t.Fatalf("Countbit failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Countbit result, expected:%v, got:%v\n", 0, ret)
	}
	ret, err = c.Countbit(keyBitMap)
	if err != nil {
		t.Fatalf("Countbit failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Countbit result, expected:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Bitcount(keyBitMap, 1, 10)
	if err != nil {
		t.Fatalf("Bitcount failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Bitcount result, expected:%v, got:%v\n", 0, ret)
	}
	ret, err = c.Bitcount(keyBitMap)
	if err != nil {
		t.Fatalf("Bitcount failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Bitcount result, expected:%v, got:%v\n", 1, ret)
	}

	err = c.Set(key, value)
	if err != nil {
		t.Fatalf("Set failed, err:%v\n", err)
	}

	v, err = c.Substr(key, 0, 2)
	if err != nil {
		t.Fatalf("Substr failed, err:%v\n", err)
	}
	if v != "va" {
		t.Fatalf("Substr result, expected:%v, got:%v\n", "va", v)
	}

	ret, err = c.Strlen(key)
	if err != nil {
		t.Fatalf("Strlen failed, err:%v\n", err)
	}
	if int(ret) != len(value) {
		t.Fatalf("Strlen result, expected:%v, got:%v\n", len(value), ret)
	}

	keys, err := c.Keys("", "", 1000)
	if err != nil {
		t.Fatalf("Keys failed, err:%v\n", err)
	}
	t.Logf("Keys result, keys:%v\n", keys)

	keys, err = c.Rkeys("", "", 1000)
	if err != nil {
		t.Fatalf("Rkeys failed, err:%v\n", err)
	}
	t.Logf("Rkeys result, keys:%v\n", keys)

	om, err := c.Scan("", "", 1000)
	if err != nil {
		t.Fatalf("Scan failed, err:%v\n", err)
	}
	t.Logf("Scan result, keys:%v\n", om.Keys())
	t.Logf("Scan result, vals:%v\n", om.Values())
	t.Logf("Scan result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Scan result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Scan failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Scan failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Scan result, Next():%q %q\n", k, v)
		}
	}

	om, err = c.Rscan("", "", 1000)
	if err != nil {
		t.Fatalf("Rscan failed, err:%v\n", err)
	}
	t.Logf("Rscan result, keys:%v\n", om.Keys())
	t.Logf("Rscan result, vals:%v\n", om.Values())
	t.Logf("Rscan result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Rscan result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Rscan failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Rscan failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Rscan result, Next():%q %q\n", k, v)
		}
	}

	ret, err = c.MultiSet("a", 1, "b", 2, "c", 4)
	if err != nil {
		t.Fatalf("MultiSet failed, err:%v\n", err)
	}
	if ret != 3 {
		t.Fatalf("MultiSet result, expected:%v, got:%v\n", 3, ret)
	}

	vals, err := c.MultiGet("a", "b", "c")
	if err != nil {
		t.Fatalf("MultiGet failed, err:%v\n", err)
	}
	if len(vals) != 6 {
		t.Fatalf("MultiGet result, expected:%v, got:%v\n", 6, len(vals))
	}

	ret, err = c.MultiDel("a")
	if err != nil {
		t.Fatalf("MultiDel failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("MultiDel result, expected:%v, got:%v\n", 1, ret)
	}

	vals, err = c.MultiGet("a", "b", "c")
	if err != nil {
		t.Fatalf("MultiGet failed, err:%v\n", err)
	}
	if len(vals) != 4 {
		t.Fatalf("MultiGet result, expected:%v, got:%v\n", 4, len(vals))
	}

	p.Release(c)
}

func TestHashmap(t *testing.T) {
	p, err := newPool()
	if err != nil {
		t.Fatal(err)
	}

	c := p.Get()

	var name = "hm"
	var key = "gossdb"
	var value = "value \t\t\tfor test\r\nsecond line"
	ret, err := c.Hset(name, key, value)
	if err != nil {
		t.Fatalf("Hset failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Hset failed, expect:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Hset(name, key, value)
	if err != nil {
		t.Fatalf("Hset failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Hset failed, expect:%v, got:%v\n", 0, ret)
	}

	v, err := c.Hget(name, key)
	if err != nil {
		t.Fatalf("Hget failed, err:%v\n", err)
	}
	if v != value {
		t.Fatalf("Hget failed, expected:%v, got:%v\n", value, v)
	}

	ret, err = c.Hexists(name, key)
	if err != nil {
		t.Fatalf("Hexists failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Hexists failed, expect:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Hdel(name, key)
	if err != nil {
		t.Fatalf("Hdel failed, err:%v\n", err)
	}

	ret, err = c.Hexists(name, key)
	if err != nil {
		t.Fatalf("Hexists failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Hexists failed, expect:%v, got:%v\n", 0, ret)
	}

	ret, err = c.Hexists("not-existed-hm", key)
	if err != nil {
		t.Fatalf("Hexists failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Hexists failed, expect:%v, got:%v\n", 0, ret)
	}

	v, err = c.Hget(name, key)
	if err != nil {
		t.Logf("Hget failed, err:%v\n", err)
	} else {
		t.Fatalf("Hget key after deleted, value:%v\n", v)
	}

	ret, err = c.Hincr(name, key, 2)
	if err != nil {
		t.Fatalf("Hincr failed, err:%v\n", err)
	} else {
		t.Logf("Hincr key after deleted, value:%v\n", ret)
	}

	v, err = c.Hget(name, key)
	if err != nil {
		t.Logf("Hget failed, err:%v\n", err)
	} else {
		t.Logf("Hget key after Hincr, value:%v\n", v)
	}

	ret, err = c.Hsize(name)
	if err != nil {
		t.Fatalf("Hsize failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Hsize failed, expect:%v, got:%v\n", 1, ret)
	}

	slice, err := c.Hlist("", "", 100)
	if err != nil {
		t.Fatalf("Hlist failed, err:%v\n", err)
	}
	t.Logf("Hlist result, %v\n", slice)

	slice, err = c.Hrlist("", "", 100)
	if err != nil {
		t.Fatalf("Hrlist failed, err:%v\n", err)
	}
	t.Logf("Hrlist result, %v\n", slice)

	slice, err = c.Hkeys(name, "", "", 100)
	if err != nil {
		t.Fatalf("Hkeys failed, err:%v\n", err)
	}
	t.Logf("Hkeys result, %v\n", slice)

	om, err := c.Hgetall(name)
	if err != nil {
		t.Fatalf("Hgetall failed, err:%v\n", err)
	}
	t.Logf("Hgetall result, keys:%v\n", om.Keys())
	t.Logf("Hgetall result, vals:%v\n", om.Values())
	t.Logf("Hgetall result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Hgetall result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Hgetall failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Hgetall failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Hgetall result, Next():%q %q\n", k, v)
		}
	}

	ret, err = c.MultiHset(name, "a", 1, "b", 2, "c", 4)
	if err != nil {
		t.Fatalf("MultiHset failed, err:%v\n", err)
	}
	if ret != 3 {
		t.Fatalf("MultiHset result, expected:%v, got:%v\n", 3, ret)
	}

	om, err = c.Hscan(name, "", "", 1000)
	if err != nil {
		t.Fatalf("Hscan failed, err:%v\n", err)
	}
	t.Logf("Hscan result, keys:%v\n", om.Keys())
	t.Logf("Hscan result, vals:%v\n", om.Values())
	t.Logf("Hscan result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Hscan result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Hscan failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Hscan failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Hscan result, Next():%q %q\n", k, v)
		}
	}

	om, err = c.Hrscan(name, "", "", 1000)
	if err != nil {
		t.Fatalf("Hrscan failed, err:%v\n", err)
	}
	t.Logf("Hrscan result, keys:%v\n", om.Keys())
	t.Logf("Hrscan result, vals:%v\n", om.Values())
	t.Logf("Hrscan result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Hrscan result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Hrscan failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Hrscan failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Hrscan result, Next():%q %q\n", k, v)
		}
	}

	vals, err := c.MultiHget(name, "a", "b", "c")
	if err != nil {
		t.Fatalf("MultiHget failed, err:%v\n", err)
	}
	if len(vals) != 6 {
		t.Fatalf("MultiHget result, expected:%v, got:%v\n", 6, len(vals))
	}

	ret, err = c.MultiHdel(name, "a")
	if err != nil {
		t.Fatalf("MultiHdel failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("MultiHdel result, expected:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Hclear(name)
	if err != nil {
		t.Fatalf("Hclear failed, err:%v\n", err)
	}
	if ret != 3 {
		t.Fatalf("Hclear result, expected:%v, got:%v\n", 3, ret)
	}

	vals, err = c.MultiHget(name, "a", "b", "c")
	if err != nil && err.Error() != "no data found" {
		t.Fatalf("MultiHget failed, err:%v\n", err)
	}
	if len(vals) != 0 {
		t.Fatalf("MultiHget result, expected:%v, got:%v\n", 0, len(vals))
	}

	p.Release(c)
}
*/

func TestSorted(t *testing.T) {
	p, err := newPool()
	if err != nil {
		t.Fatal(err)
	}

	c := p.Get()

	var name = "z-set-map"
	var key = "gossdb"
	var value int64 = 1000

	_, err = c.Zclear(name)
	if err != nil {
		t.Fatalf("Zclear failed, err:%v\n", err)
	}
	ret, err := c.Zset(name, key, value)
	if err != nil {
		t.Fatalf("Zset failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Zset failed, expected:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Zset(name, key, value)
	if err != nil {
		t.Fatalf("Zset failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Zset failed, expected:%v, got:%v\n", 0, ret)
	}

	v, err := c.Zget(name, key)
	if err != nil {
		t.Fatalf("Zget failed, err:%v\n", err)
	}
	if v != value {
		t.Fatalf("Zget failed, expected:%v, got:%v\n", value, v)
	}

	ret, err = c.Zexists(name, key)
	if err != nil {
		t.Fatalf("Zexists failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Zexists failed, expect:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Zdel(name, key)
	if err != nil {
		t.Fatalf("Zdel failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Zdel failed, expect:%v, got:%v\n", 1, ret)
	}
	ret, err = c.Zdel(name, key)
	if err != nil {
		t.Fatalf("Zdel failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Zdel failed, expect:%v, got:%v\n", 0, ret)
	}

	ret, err = c.Zexists(name, key)
	if err != nil {
		t.Fatalf("Zexists failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Zexists failed, expect:%v, got:%v\n", 0, ret)
	}

	ret, err = c.Zexists("not-existed-hm", key)
	if err != nil {
		t.Fatalf("Zexists failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Zexists failed, expect:%v, got:%v\n", 0, ret)
	}

	ret, err = c.Zincr(name, key, 2)
	if err != nil {
		t.Fatalf("Zincr failed, err:%v\n", err)
	} else {
		t.Logf("Zincr key after deleted, value:%v\n", ret)
	}

	ret, err = c.Zsize(name)
	if err != nil {
		t.Fatalf("Zsize failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Zsize failed, expect:%v, got:%v\n", 1, ret)
	}

	slice, err := c.Zlist("", "", 100)
	if err != nil {
		t.Fatalf("Zlist failed, err:%v\n", err)
	}
	t.Logf("Zlist result, %v\n", slice)

	slice, err = c.Zrlist("", "", 100)
	if err != nil {
		t.Fatalf("Zrlist failed, err:%v\n", err)
	}
	t.Logf("Zrlist result, %v\n", slice)

	ret, err = c.MultiZset(name, "a", 1, "b", 2, "c", 4)
	if err != nil {
		t.Fatalf("MultiZset failed, err:%v\n", err)
	}
	if ret != 3 {
		t.Fatalf("MultiZset result, expected:%v, got:%v\n", 3, ret)
	}

	slice, err = c.Zkeys(name, "", 0, 10000, 100)
	if err != nil {
		t.Fatalf("Zkeys failed, err:%v\n", err)
	}
	t.Logf("Zkeys result, %v\n", slice)

	om, err := c.Zscan(name, "", 0, 1000, 1000)
	if err != nil {
		t.Fatalf("Zscan failed, err:%v\n", err)
	}
	t.Logf("Zscan result, keys:%v\n", om.Keys())
	t.Logf("Zscan result, vals:%v\n", om.Values())
	t.Logf("Zscan result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Zscan result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Zscan failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Zscan failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Zscan result, Next():%q %q\n", k, v)
		}
	}

	om, err = c.Zrscan(name, "", 1000, 0, 1000)
	if err != nil {
		t.Fatalf("Zrscan failed, err:%v\n", err)
	}
	t.Logf("Zrscan result, keys:%v\n", om.Keys())
	t.Logf("Zrscan result, vals:%v\n", om.Values())
	t.Logf("Zrscan result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Zrscan result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Zrscan failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Zrscan failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Zrscan result, Next():%q %q\n", k, v)
		}
	}

	ret, err = c.Zrank(name, "a")
	if err != nil {
		t.Fatalf("Zrank failed, err:%v\n", err)
	}
	if ret != 0 {
		t.Fatalf("Zrank result, expected:%v, got:%v\n", 0, ret)
	}

	ret, err = c.Zrrank(name, "a")
	if err != nil {
		t.Fatalf("Zrrank failed, err:%v\n", err)
	}
	if ret != 3 {
		t.Fatalf("Zrrank result, expected:%v, got:%v\n", 3, ret)
	}

	om, err = c.Zrange(name, 0, 1000)
	if err != nil {
		t.Fatalf("Zrange failed, err:%v\n", err)
	}
	t.Logf("Zrange result, keys:%v\n", om.Keys())
	t.Logf("Zrange result, vals:%v\n", om.Values())
	t.Logf("Zrange result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Zrange result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Zrange failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Zrange failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Zrange result, Next():%q %q\n", k, v)
		}
	}

	om, err = c.Zrrange(name, 0, 1000)
	if err != nil {
		t.Fatalf("Zrrange failed, err:%v\n", err)
	}
	t.Logf("Zrrange result, keys:%v\n", om.Keys())
	t.Logf("Zrrange result, vals:%v\n", om.Values())
	t.Logf("Zrrange result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Zrrange result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Zrrange failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Zrrange failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Zrrange result, Next():%q %q\n", k, v)
		}
	}

	ret, err = c.Zcount(name, 0, 1000)
	if err != nil {
		t.Fatalf("Zcount failed, err:%v\n", err)
	}
	if ret != 4 {
		t.Fatalf("Zcount result, expected:%v, got:%v\n", 4, ret)
	}
	ret, err = c.Zsum(name, 0, 1000)
	if err != nil {
		t.Fatalf("Zsum failed, err:%v\n", err)
	}
	t.Logf("Zcount result:%v\n", ret)
	retFloat, err := c.Zavg(name, 0, 1000)
	if err != nil {
		t.Fatalf("Zavg failed, err:%v\n", err)
	}
	t.Logf("Zavg result:%v\n", retFloat)

	vals, err := c.MultiZget(name, "a", "b", "c")
	if err != nil {
		t.Fatalf("MultiZget failed, err:%v\n", err)
	}
	if len(vals) != 6 {
		t.Fatalf("MultiZget result, expected:%v, got:%v\n", 6, len(vals))
	}

	ret, err = c.MultiZdel(name, "a")
	if err != nil {
		t.Fatalf("MultiZdel failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("MultiZdel result, expected:%v, got:%v\n", 1, ret)
	}

	ret, err = c.Zremrangebyrank(name, 0, 1000)
	if err != nil {
		t.Fatalf("Zremrangebyrank failed, err:%v\n", err)
	}
	if ret != 3 {
		t.Fatalf("Zremrangebyrank result, expected:%v, got:%v\n", 2, ret)
	}

	ret, err = c.MultiZset(name, "a", 1, "b", 2, "c", 4)
	if err != nil {
		t.Fatalf("MultiZset failed, err:%v\n", err)
	}
	if ret != 3 {
		t.Fatalf("MultiZset result, expected:%v, got:%v\n", 3, ret)
	}

	om, err = c.Zpopfront(name, 1)
	if err != nil {
		t.Fatalf("Zpopfront failed, err:%v\n", err)
	}
	t.Logf("Zpopfront result, keys:%v\n", om.Keys())
	t.Logf("Zpopfront result, vals:%v\n", om.Values())
	t.Logf("Zpopfront result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Zpopfront result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Zpopfront failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Zpopfront failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Zpopfront result, Next():%q %q\n", k, v)
		}
	}

	om, err = c.Zpopback(name, 1)
	if err != nil {
		t.Fatalf("Zpopback failed, err:%v\n", err)
	}
	t.Logf("Zpopback result, keys:%v\n", om.Keys())
	t.Logf("Zpopback result, vals:%v\n", om.Values())
	t.Logf("Zpopback result, length:%v\n", om.Length())
	for i := 0; i < om.Length(); i++ {
		k, v := om.Index(i)
		t.Logf("Zpopback result, index(%v):%q %q\n", i, k, v)
		val, ok := om.Lookup(k)
		if ok != true {
			t.Fatalf("Zpopback failed, Lookup(%v) not found\n", k)
		}
		if val != v {
			t.Fatalf("Zpopback failed, Lookup(%v)=%q != %q", k, val, v)
		}
	}
	for {
		if k, v, e := om.Next(); e == true {
			break
		} else {
			t.Logf("Zpopback result, Next():%q %q\n", k, v)
		}
	}

	ret, err = c.Zremrangebyscore(name, 0, 1000)
	if err != nil {
		t.Fatalf("Zremrangebyscore failed, err:%v\n", err)
	}
	if ret != 1 {
		t.Fatalf("Zremrangebyscore result, expected:%v, got:%v\n", 1, ret)
	}

	p.Release(c)
}
