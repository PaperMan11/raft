package raft

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

// 可排序 slice
type uint64Slice []uint64

func (s uint64Slice) Len() int           { return len(s) }
func (s uint64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// WriteFile 将数据写入以文件名命名的文件。
// 如果文件不存在，WriteFile会使用perm权限创建它;
// 否则WriteFile会在写入前截断它。 这是从ioutil复制的。
// WriteFile，并添加了一个Sync调用，以确保数据到达磁盘。s
func writeFileSynced(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer f.Close() // Idempotent

	n, err := f.Write(data)
	if err == nil && n < len(data) {
		return io.ErrShortWrite
	} else if err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	return f.Close()
}

// Waits for a random time between two durations and sends the current time on
// the returned channel.
//
// 等待两个持续时间之间的随机时间，并在返回的通道上发送当前时间。
func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return time.After(d)
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
