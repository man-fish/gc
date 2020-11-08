package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash maps bytes to uint32 key
type Hash func(data []byte) uint32

// Map is hash key container
type Map struct {
	// hash func
	hash Hash
	// virtual nodes limit
	replicas int
	// hash ring
	keys []int
	// virtual nodes and real nodes map
	hashmap map[int]string
}

// New is the constructor of Map
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashmap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add adds some keys to the hash. this key is for node - nodes key.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashmap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// Remove use to remove a key and its virtual keys on the ring and map
func (m *Map) Remove(key string) {
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		idx := sort.SearchInts(m.keys, hash)
		m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
		delete(m.hashmap, hash)
	}
}

// Get gets the closest item in the hash to the provided key. this key is for kv - value key.
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	return m.hashmap[m.keys[idx%len(m.keys)]]
}
