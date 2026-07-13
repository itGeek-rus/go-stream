package core

func BufferSize(size int) int {
	if size < 0 {
		return 0
	}
	return size
}
