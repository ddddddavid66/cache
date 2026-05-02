package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

type Hash func([]byte) uint32

type Map struct {
	hash     Hash
	keys     []uint32          // 切片 保存哈希值
	replicas int               // 虚拟节点的数量
	hashMap  map[uint32]string // hash -> 对应的真实节点
}

func DeafaulyHash(b []byte) uint32 {
	return uint32(crc32.ChecksumIEEE(b))
}

func NewMap(replicas int, fn Hash) *Map {
	if fn == nil {
		fn = DeafaulyHash
	}
	return &Map{
		hash:     fn,
		replicas: replicas,
		hashMap:  make(map[uint32]string),
	}
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := m.getHash(key, i)
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	// 排序  nlogn 算法
	sort.Slice(m.keys, func(i int, j int) bool {
		return m.keys[i] < m.keys[j]
	})
}

func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := m.hash([]byte(key))
	index := m.getIndex(hash)
	return m.hashMap[m.keys[index]]
}

// 节点变化很频繁时，可以重建整个环，
// 逻辑更简单，也避免切片频繁删除的成本。
func (m *Map) Remove(key string) { // 删除他所有的虚拟节点
	for i := 0; i < m.replicas; i++ {
		hash := m.getHash(key, i)
		index := sort.Search(len(m.keys), func(i int) bool {
			return m.keys[i] >= hash
		})
		if index < len(m.keys) && m.keys[index] == hash {
			delete(m.hashMap, hash)                              // 删除hash
			m.keys = append(m.keys[:index], m.keys[index+1:]...) // keys删除
		}
	}
}

func (m *Map) getIndex(hash uint32) int {
	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if index == len(m.keys) { // 说明 这个hash是最大的 交给 keys[0] 这个节点处理
		index = 0
	}
	return index
}

func (m *Map) getHash(key string, i int) uint32 {
	var b strings.Builder
	b.WriteString(strconv.Itoa(i)) //NOTE 高性能写法
	b.WriteByte('-')
	b.WriteString(key)
	return m.hash([]byte(b.String()))
}

func (m *Map) Len() int {
	return len(m.keys)
}
