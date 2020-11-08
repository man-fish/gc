package gc

// ByteView holds an immmutable slice of byte
type ByteView struct {
	b []byte
}

// Len returns length of byte slice to impelement the Value interface
func (bv ByteView) Len() int {
	return len(bv.b)
}

// ByteSlice return a copy of byteView
func (bv ByteView) ByteSlice() []byte {
	return bv.b
}

// String
func (bv ByteView) String() string {
	return string(bv.b)
}

func cloneBytes(b []byte) (c []byte) {
	c = make([]byte, len(b))
	copy(c, b)
	return
}
