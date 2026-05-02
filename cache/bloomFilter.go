package cache

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits []uint64 // 位数组，每个 uint64 存 64 位
	k    uint32   // hash 函数个数
	m    uint32   // 位数组总位数
}

// NewBloomFilter n=预期元素数 p=期望误判率(如0.01)
func NewBloomFilter(n int, p float64) *BloomFilter {
	// m = -n*ln(p) / (ln2)^2  位数组大小
	m := uint32(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
	// k = m/n * ln2  hash 函数个数
	k := uint32(float64(m) / float64(n) * math.Ln2)
	// uint64 个数 = m/64 向上取整
	return &BloomFilter{
		bits: make([]uint64, (m+63)/64),
		k:    k,
		m:    m,
	}
}

// Add 插入 key
func (bf *BloomFilter) Add(key string) {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m // 用双重 hash 模拟 k 个 hash 函数
		word, bit := pos/64, pos%64
		bf.bits[word] |= 1 << bit // 置 1
	}
}

// MayExist 可能存在 (true) 或一定不存在 (false)
func (bf *BloomFilter) MayExist(key string) bool {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		word, bit := pos/64, pos%64
		if bf.bits[word]&(1<<bit) == 0 { // 任一位是 0 → 一定不存在
			return false
		}
	}
	return true // 所有位都是 1 → 可能存在（有误判）
}

// hash 二次 hash，返回 h1 和 h2
func (bf *BloomFilter) hash(key string) (uint32, uint32) {
	h := fnv.New32()
	h.Write([]byte(key))
	return h.Sum32(), h.Sum32() >> 16
}
