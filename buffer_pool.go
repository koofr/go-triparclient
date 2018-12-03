package triparclient

import (
	"container/list"
	"sync"
)

type BufferPool struct {
	cap        int
	size       int
	mx         *sync.Mutex
	cond       *sync.Cond
	bufferSize int64
	buffers    *list.List
}

func NewBufferPool(capacity int, bufferSize int64) *BufferPool {
	mx := &sync.Mutex{}
	return &BufferPool{
		cap:        capacity,
		size:       0,
		bufferSize: bufferSize,
		mx:         mx,
		cond:       sync.NewCond(mx),
		buffers:    list.New(),
	}
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
