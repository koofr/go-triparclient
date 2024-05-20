package triparclient

import (
	"time"

	. "github.com/onsi/ginkgo/v2/dsl/core"
	. "github.com/onsi/gomega"
)

var _ = Describe("BufferPool", func() {
	Describe("Get", func() {
		It("should wait if size >= cap", func() {
			bp := NewBufferPool(2, 10)

			b1 := bp.Get()
			b2 := bp.Get()

			go func() {
				time.Sleep(100 * time.Millisecond)
				bp.Put(b1)
			}()

			start := time.Now()
			b3 := bp.Get()
			Expect(time.Since(start)).To(BeNumerically(">", 90*time.Millisecond))

			bp.Put(b2)
			bp.Put(b3)
		})
	})
})
