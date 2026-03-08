package util

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var src = rand.NewSource(time.Now().UnixNano())

const (
	// 6 bits to represent a letter index
	letterIDBits = 6
	// All 1-bits as many as letterIdBits
	letterIDMask = 1<<letterIDBits - 1
	letterIDMax  = 63 / letterIDBits
)

// Rand gets random number
type Rand struct {
	seed int64
	pool *sync.Pool
}

// New creates rand
func New() *Rand {
	r := new(Rand)
	r.pool = &sync.Pool{
		New: func() any {
			return rand.New(rand.NewSource(r.newSeed()))
		},
	}
	return r
}

func (r *Rand) newSeed() int64 {
	var seed int64
	for {
		seed = time.Now().UnixNano()
		cur := atomic.LoadInt64(&r.seed)
		if cur != seed {
			if atomic.CompareAndSwapInt64(&r.seed, cur, seed) {
				break
			}
		}
		time.Sleep(time.Nanosecond)
	}
	return seed
}

// Intn returns, as an int, a non-negative pseudo-random number in [0,n).
// It panics if n <= 0
// Intn 获取随机数
func (r *Rand) Intn(n int) int {
	rd := r.pool.Get().(*rand.Rand)
	i := rd.Intn(n)
	r.pool.Put(rd)
	return i
}

// RandStr 随机字符串
func RandStr(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIDMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIDMax
		}
		if idx := int(cache & letterIDMask); idx < len(letters) {
			b[i] = letters[idx]
			i--
		}
		cache >>= letterIDBits
		remain--
	}
	return *(*string)(unsafe.Pointer(&b))
}
