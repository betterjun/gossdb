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

	result, err = c.FlushDB("kv")
	if err != nil {
		t.Fatalf("FlushDB failed, err:%v\n", err)
	}
	//t.Logf("FlushDB result:%v\n", result)

	result, err = c.FlushDB("")
	if err != nil {
		t.Fatalf("FlushDB failed, err:%v\n", err)
	}
	//t.Logf("FlushDB result:%v\n", result)

	var key = "gossdb"
	var value = "value for test"
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

	p.Release(c)
}
