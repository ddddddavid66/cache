package cache

type ByteView struct {
	b []byte
}

func NewByteView(b []byte) ByteView {
	dst := make([]byte, len(b))
	copy(dst, b)
	return ByteView{b: dst}
}

func (v ByteView) Len() int {
	return len(v.b)
}

func (v ByteView) ByteSlice() []byte {
	return append([]byte(nil), v.b...)
}

func (v ByteView) String() string {
	return string(v.b)
}
