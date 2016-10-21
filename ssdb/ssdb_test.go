package ssdb

import (
	"testing"
	"time"
)

//"github.com/betterjun/gossdb/ssdb"

func TestToString(t *testing.T) {
	p, err := NewPool("192.168.254.22", 8888, "CreateSessionResponseAtDecode-longlong", 100)
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
	t.Logf("Keys failed, keys:%v\n", keys)

	keys, err = c.Rkeys("", "", 1000)
	if err != nil {
		t.Fatalf("Rkeys failed, err:%v\n", err)
	}
	t.Logf("Rkeys failed, keys:%v\n", keys)

	kvs, err := c.Scan("", "", 1000)
	if err != nil {
		t.Fatalf("Scan failed, err:%v\n", err)
	}
	t.Logf("Scan failed, kvs:%v\n", kvs)

	kvs, err = c.Rscan("", "", 1000)
	if err != nil {
		t.Fatalf("Rscan failed, err:%v\n", kvs)
	}
	t.Logf("Rscan failed, kvs:%v\n", kvs)

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
