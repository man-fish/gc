package consistenthash

import (
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {
	hash := New(3, func(data []byte) uint32 {
		i, _ := strconv.Atoi(string(data))
		return uint32(i)
	})
	// 2, 4, 8, 12, 14, 18, 22, 24, 28
	hash.Add("2", "4", "8")

	testCase := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"17": "8",
	}

	for key, value := range testCase {
		if hash.Get(key) != value {
			t.Errorf("Asking for %s, should have yielded %s", key, value)
		}
	}

	hash.Remove("2")
	testCase = map[string]string{
		"2":  "4",
		"10": "4",
	}

	for key, value := range testCase {
		if hash.Get(key) != value {
			t.Errorf("Asking for %s, should have yielded %s", key, value)
		}
	}
}
