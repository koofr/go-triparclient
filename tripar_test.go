package triparclient_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	triparclient "."
	ioutils "github.com/koofr/go-ioutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	TRIPAR_MAX_BUFFERS = 1024
	TRIPAR_BUFFER_SIZE = 1024 * 1024
)

type LongDataReader struct {
	size   int
	at     int
	failAt int
}

func NewLongDataReader(size int) *LongDataReader {
	return &LongDataReader{
		size:   size,
		at:     0,
		failAt: -1,
	}
}

func NewFailingLongDataReader(size int, failAt int) *LongDataReader {
	return &LongDataReader{
		size:   size,
		at:     0,
		failAt: failAt,
	}
}

func (r *LongDataReader) Read(p []byte) (n int, err error) {
	if r.at >= r.size {
		return 0, io.EOF
	}
	n = 0
	for n < len(p) && r.at < r.size {
		p[n] = byte(r.at % 10)
		r.at++
		if r.at == r.failAt {
			err = errors.New("You wanted this to fail.")
			return
		}
		n++
	}
	if r.at >= r.size {
		err = io.EOF
	} else {
		err = nil
	}
	return
}

func purge(client *triparclient.TriparClient, path string) (err error) {
	entries, err := client.List(path)
	if err != nil {
		return err
	}
	for _, entry := range entries.Entries {
		info, err := client.Stat(path + "/" + entry.Name)
		if err != nil {
			return err
		}
		if info.IsDir() {
			err = purge(client, path+"/"+entry.Name)
			if err != nil {
				return err
			}
			err = client.DeleteDirectory(path + "/" + entry.Name)
			if err != nil {
				return err
			}
		} else {
			err = client.DeleteObject(path + "/" + entry.Name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

var _ = Describe("TriparClient", func() {
	var client *triparclient.TriparClient

	endpoint := os.Getenv("TRIPAR_ENDPOINT")
	user := os.Getenv("TRIPAR_USERNAME")
	pass := os.Getenv("TRIPAR_PASSWORD")
	share := os.Getenv("TRIPAR_SHARE")
	root := os.Getenv("TRIPAR_ROOT")

	bp := triparclient.NewBufferPool(TRIPAR_MAX_BUFFERS, TRIPAR_BUFFER_SIZE)

	if endpoint == "" || user == "" || pass == "" || share == "" || root == "" {
		fmt.Println("TRIPAR_ENDPOINT, TRIPAR_USERNAME, TRIPAR_PASSWORD, TRIPAR_SHARE, TRIPAR_ROOT env variables missing")
		return
	}

	BeforeEach(func() {
		var err error

		client, err = triparclient.NewTriparClient(endpoint, user, pass, share, bp)
		Expect(err).NotTo(HaveOccurred())

		_, err = client.Stat(root)
		if err == nil {
			err = purge(client, root)
			Expect(err).NotTo(HaveOccurred())
		} else if err == triparclient.ERR_NOT_FOUND {
			err = client.CreateDirectory(root)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	Describe("GetObject", func() {
		It("should get object", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, err := client.GetObject(root+"/object", nil)
			Expect(err).NotTo(HaveOccurred())

			defer reader.Close()

			data, _ := ioutil.ReadAll(reader)

			Expect(string(data)).To(Equal("12345"))
		})

		It("should get object range", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, err := client.GetObject(root+"/object", &ioutils.FileSpan{2, 3})
			Expect(err).NotTo(HaveOccurred())

			defer reader.Close()

			data, _ := ioutil.ReadAll(reader)

			Expect(string(data)).To(Equal("34"))
		})

		It("should not get inexisting object", func() {
			_, err := client.GetObject(root+"/object-inexisting", nil)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Stat", func() {
		It("should get object info", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			object, err := client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())

			Expect(object.Status.Size).To(Equal(int64(5)))
			Expect(object.Path).To(Equal(root + "/object"))
		})

		It("should not get inexisting object info", func() {
			_, err := client.Stat(root + "/object-inexisting")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Fsync", func() {
		It("should put commit data to disk", func() {
			data := bytes.NewBufferString("12345")
			err := client.PutObject(root+"/new-object", data)
			Expect(err).NotTo(HaveOccurred())

			object, err := client.Stat(root + "/new-object")
			Expect(err).NotTo(HaveOccurred())

			Expect(object.Status.Size).To(Equal(int64(5)))

			err = client.Fsync(root + "/new-object")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("PutObject", func() {
		It("should put object", func() {
			data := bytes.NewBufferString("12345")
			err := client.PutObject(root+"/new-object", data)
			Expect(err).NotTo(HaveOccurred())

			object, err := client.Stat(root + "/new-object")
			Expect(err).NotTo(HaveOccurred())

			Expect(object.Status.Size).To(Equal(int64(5)))
		})

		It("should put a large object", func() {
			data := NewLongDataReader(4*1024*1024 + 17)
			err := client.PutObject(root+"/large-object", data)
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(root + "/large-object")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(4*1024*1024 + 17)))

			reader, err := client.GetObject(root+"/large-object", &ioutils.FileSpan{2*1024*1024 + 71, 2*1024*1024 + 71 + 23 - 1})
			Expect(err).NotTo(HaveOccurred())

			defer reader.Close()

			fetched, _ := ioutil.ReadAll(reader)
			expected := make([]byte, 23)
			for i := 0; i < 23; i++ {
				expected[i] = byte((2*1024*1024 + 71 + i) % 10)
			}
			Expect(fetched).To(Equal(expected))
		})

		It("should remove partially written objects after a failure", func() {
			data := NewFailingLongDataReader(4*1024*1024+17, 2*1024*1024+101)
			err := client.PutObject(root+"/large-object", data)
			Expect(err).To(HaveOccurred())

			_, err = client.Stat(root + "/large-object")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(triparclient.ERR_NOT_FOUND))
		})

	})

	Describe("DeleteObject", func() {
		It("should delete object", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())

			err = client.DeleteObject(root + "/object")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/object")
			Expect(err).To(HaveOccurred())
		})

		It("should not delete inexistent object", func() {
			err := client.DeleteObject(root + "/object-inexisting")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("CreateDirectory", func() {
		It("should create a directory", func() {
			err := client.CreateDirectory(root + "/subdir")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should not create an existing directory", func() {
			err := client.CreateDirectory(root + "/subdir")
			Expect(err).NotTo(HaveOccurred())

			err = client.CreateDirectory(root + "/subdir")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("CreateDirectories", func() {
		It("should create a directory tree", func() {
			err := client.CreateDirectories(root + "/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should create a directory tree when it partially exists", func() {
			err := client.CreateDirectory(root + "/subdir")
			Expect(err).NotTo(HaveOccurred())
			err = client.CreateDirectories(root + "/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())
		})

	})

	Describe("DeleteDirectory", func() {
		It("should delete a directory", func() {
			err := client.CreateDirectory(root + "/subdir")
			Expect(err).NotTo(HaveOccurred())

			err = client.DeleteDirectory(root + "/subdir")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/subdir")
			Expect(err).To(HaveOccurred())
		})

		It("should not delete a file", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			err = client.DeleteDirectory(root + "/object")
			Expect(err).To(HaveOccurred())
		})

		It("should not delete an inexisting file", func() {
			err := client.DeleteDirectory(root + "/inexistent-subdir")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("MoveObject", func() {
		It("should move an object", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())

			err = client.MoveObject(root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/object")
			Expect(err).To(HaveOccurred())

			info2, err := client.Stat(root + "/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(info2.Status.Size))
		})

		It("should not move an inexistent object", func() {
			err := client.MoveObject(root+"/object-inexisting", root+"/object-inexisting-2")
			Expect(err).To(HaveOccurred())
		})

		It("should move over an existing object", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())
			err = client.PutObject(root+"/object2", bytes.NewBufferString("123456"))
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())
			_, err = client.Stat(root + "/object2")
			Expect(err).NotTo(HaveOccurred())

			err = client.MoveObject(root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/object")
			Expect(err).To(HaveOccurred())
			info, err := client.Stat(root + "/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(5)))
		})
	})

	Describe("CopyObject", func() {
		It("should copy an object", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())

			err = client.CopyObject(root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			info, err = client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())

			info2, err := client.Stat(root + "/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(info2.Status.Size))
		})

		It("should not copy an inexistent object", func() {
			err := client.CopyObject(root+"/object-inexisting", root+"/object-inexisting-2")
			Expect(err).To(HaveOccurred())
		})

		It("should copy over an existing object", func() {
			err := client.PutObject(root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())
			err = client.PutObject(root+"/object2", bytes.NewBufferString("123456"))
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())
			_, err = client.Stat(root + "/object2")
			Expect(err).NotTo(HaveOccurred())

			err = client.CopyObject(root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(root + "/object")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(5)))
			info, err = client.Stat(root + "/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(5)))
		})
	})

})
