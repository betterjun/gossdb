package ssdb

import (
	"fmt"
	"os"
	"testing"
)

// variables for bench test
var (
	poolForBench   *Pool
	connForBench   *Client
	keyForBench    = "kfb"
	hashForBench   = "hmBench"
	sortedForBench = "sortedBench"
	queueForBench  = "queueForBench"
	data           = make([]byte, 1024)
	strData        = string(data)
)

func init() {
	poolForBench, err := newPool()
	if err != nil {
		fmt.Println("connect to server error:%v", err)
		os.Exit(0)
	}

	connForBench = poolForBench.Get()
	err = connForBench.Set(keyForBench, 1)
	if err != nil {
		fmt.Println("set default value error:%v", err)
		os.Exit(0)
	}

	_, err = connForBench.Hset(hashForBench, keyForBench, 1)
	if err != nil {
		fmt.Println("set default value error:%v", err)
		os.Exit(0)
	}

	_, err = connForBench.Zset(sortedForBench, keyForBench, 1)
	if err != nil {
		fmt.Println("set default value error:%v", err)
		os.Exit(0)
	}

	_, err = connForBench.Qpush(queueForBench, strData)
	if err != nil {
		fmt.Println("set default value error:%v", err)
		os.Exit(0)
	}
}

func BenchmarkKVGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Get(keyForBench)
	}
}

func BenchmarkKVSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Set(keyForBench, data)
	}
}

func BenchmarkHashGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Hget(hashForBench, keyForBench)
	}
}

func BenchmarkHashSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Hset(hashForBench, keyForBench, data)
	}
}

func BenchmarkSortedGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Hget(sortedForBench, keyForBench)
	}
}

func BenchmarkSortedSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Hset(sortedForBench, keyForBench, data)
	}
}

func BenchmarkQueuePushFront(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.QpushFront(queueForBench, strData)
	}
}

func BenchmarkQueuePopFront(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.QpopFront(queueForBench, 1)
	}
}

func BenchmarkQueuePushBack(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.QpushBack(queueForBench, strData)
	}
}

func BenchmarkQueuePopBack(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.QpopBack(queueForBench, 1)
	}
}

func BenchmarkQueueFront(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Qfront(queueForBench)
	}
}

func BenchmarkQueueBack(b *testing.B) {
	for i := 0; i < b.N; i++ {
		connForBench.Qback(queueForBench)
	}
}
