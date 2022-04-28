package triparclient

import (
	"container/list"
	"sync"
)

type BufferPoolIface interface {
	Get() []byte
	Put(buffer []byte)
}

type BufferPool struct {
	cap        int
	size       int
	mx         sync.Mutex
	cond       *sync.Cond
	bufferSize int64
	buffers    *list.List
}

func NewBufferPool(capacity int, bufferSize int64) *BufferPool {
	bp := &BufferPool{
		cap:        capacity,
		size:       0,
		bufferSize: bufferSize,
		buffers:    list.New(),
	}

	bp.cond = sync.NewCond(&bp.mx)

	return bp
}

func (bp *BufferPool) Get() []byte {
	bp.mx.Lock()
	defer bp.mx.Unlock()

	for bp.buffers.Len() == 0 {
		if bp.size < bp.cap {
			bp.size++
			nubuf := make([]byte, bp.bufferSize)
			return nubuf
		} else {
			bp.cond.Wait()
		}
	}
	front := bp.buffers.Front()
	bp.buffers.Remove(front)
	return front.Value.([]byte)
}

func (bp *BufferPool) Put(buffer []byte) {
	bp.mx.Lock()
	defer bp.mx.Unlock()

	bp.buffers.PushFront(buffer)
	bp.cond.Signal()
}
