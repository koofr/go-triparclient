package triparclient_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	ioutils "github.com/koofr/go-ioutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/koofr/go-triparclient"
)

const (
	TriparMaxBuffers = 1024
	TriparBufferSize = 1024 * 1024
	TriparGetSize    = 1024 * 1024
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

func purge(ctx context.Context, client *TriparClient, path string) (err error) {
	entries, err := client.List(ctx, path)
	if err != nil {
		return err
	}
	for _, entry := range entries.Entries {
		info, err := client.Stat(ctx, path+"/"+entry.Name)
		if err != nil {
			return err
		}
		if info.IsDir() {
			err = purge(ctx, client, path+"/"+entry.Name)
			if err != nil {
				return err
			}
			err = client.DeleteDirectory(ctx, path+"/"+entry.Name)
			if err != nil {
				return err
			}
		} else {
			err = client.DeleteObject(ctx, path+"/"+entry.Name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

var _ = Describe("TriparClient", func() {
	var ctx context.Context
	var cbp *countingBufferPool
	var client *TriparClient

	endpoint := os.Getenv("TRIPAR_ENDPOINT")
	user := os.Getenv("TRIPAR_USERNAME")
	pass := os.Getenv("TRIPAR_PASSWORD")
	share := os.Getenv("TRIPAR_SHARE")
	root := os.Getenv("TRIPAR_ROOT")

	bp := NewBufferPool(TriparMaxBuffers, TriparBufferSize)

	if endpoint == "" || user == "" || pass == "" || share == "" || root == "" {
		fmt.Println("TRIPAR_ENDPOINT, TRIPAR_USERNAME, TRIPAR_PASSWORD, TRIPAR_SHARE, TRIPAR_ROOT env variables missing")
		return
	}

	initClient := func(getChunkSize int64) {
		var err error

		client, err = NewTriparClient(endpoint, user, pass, share, cbp, getChunkSize)
		Expect(err).NotTo(HaveOccurred())

		client.HTTPClient.Client.Transport = &safeTransport{
			transport: http.DefaultTransport,
			urlPrefix: endpoint + "/" + share + "/" + root,
		}
	}

	BeforeEach(func() {
		ctx = context.Background()

		cbp = &countingBufferPool{
			upstream: bp,
		}

		initClient(TriparGetSize)

		_, err := client.Stat(ctx, root)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				err = client.CreateDirectory(ctx, root)
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
		} else {
			err = purge(ctx, client, root)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		Expect(cbp.GetCount()).To(BeZero())
	})

	Describe("GetObject", func() {
		It("should get object", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, stat, err := client.GetObject(ctx, root+"/object", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(stat.Status.Size).To(Equal(int64(5)))

			defer reader.Close()

			data, err := ioutil.ReadAll(reader)
			Expect(err).NotTo(HaveOccurred())

			Expect(string(data)).To(Equal("12345"))
		})

		It("should get object range", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, _, err := client.GetObject(ctx, root+"/object", &ioutils.FileSpan{Start: 2, End: 3})
			Expect(err).NotTo(HaveOccurred())

			defer reader.Close()

			data, err := ioutil.ReadAll(reader)
			Expect(err).NotTo(HaveOccurred())

			Expect(string(data)).To(Equal("34"))
		})

		It("should get object range by chunks", func() {
			initClient(2)

			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			reader, _, err := client.GetObject(ctx, root+"/object", &ioutils.FileSpan{Start: 1, End: 4})
			Expect(err).NotTo(HaveOccurred())

			defer reader.Close()

			data, err := ioutil.ReadAll(reader)
			Expect(err).NotTo(HaveOccurred())

			Expect(string(data)).To(Equal("2345"))
		})

		It("should fail if get object range by chunks request fails", func() {
			initClient(2)

			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			originalTransport := client.HTTPClient.Client.Transport

			requestErr := errors.New("request error")

			client.HTTPClient.Client = &http.Client{
				Transport: funcTransport(func(r *http.Request) (*http.Response, error) {
					if strings.Contains(r.URL.String(), "cmd=") {
						return originalTransport.RoundTrip(r)
					}
					return nil, requestErr
				}),
			}

			reader, _, err := client.GetObject(ctx, root+"/object", &ioutils.FileSpan{Start: 1, End: 4})
			Expect(err).NotTo(HaveOccurred())

			defer reader.Close()

			_, err = ioutil.ReadAll(reader)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(requestErr))
		})

		It("should fail to get object range by chunks for an invalid span", func() {
			initClient(2)

			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			_, _, err = client.GetObject(ctx, root+"/object", &ioutils.FileSpan{Start: 1, End: 10})
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ErrBadRange))
		})

		It("should not get a directory", func() {
			_, _, err := client.GetObject(ctx, root, nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ErrNotAFile))
		})

		It("should not get a non-existent object", func() {
			_, _, err := client.GetObject(ctx, root+"/object-nonexistent", nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ErrNotFound))
		})
	})

	Describe("Stat", func() {
		It("should get object info", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			object, err := client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())

			Expect(object.Status.Size).To(Equal(int64(5)))
			Expect(object.Path).To(Equal(root + "/object"))
		})

		It("should not get a non-existent object info", func() {
			_, err := client.Stat(ctx, root+"/object-nonexistent")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Fsync", func() {
		It("should put commit data to disk", func() {
			data := bytes.NewBufferString("12345")
			err := client.PutObject(ctx, root+"/new-object", data)
			Expect(err).NotTo(HaveOccurred())

			object, err := client.Stat(ctx, root+"/new-object")
			Expect(err).NotTo(HaveOccurred())

			Expect(object.Status.Size).To(Equal(int64(5)))

			err = client.Fsync(ctx, root+"/new-object")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("PutObject", func() {
		It("should put object", func() {
			data := bytes.NewBufferString("12345")
			err := client.PutObject(ctx, root+"/new-object", data)
			Expect(err).NotTo(HaveOccurred())

			object, err := client.Stat(ctx, root+"/new-object")
			Expect(err).NotTo(HaveOccurred())

			Expect(object.Status.Size).To(Equal(int64(5)))
		})

		It("should put a large object", func() {
			data := NewLongDataReader(4*1024*1024 + 17)
			err := client.PutObject(ctx, root+"/large-object", data)
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(ctx, root+"/large-object")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(4*1024*1024 + 17)))

			reader, stat, err := client.GetObject(ctx, root+"/large-object", &ioutils.FileSpan{Start: 2*1024*1024 - 71, End: 2*1024*1024 - 71 + 142 - 1})
			Expect(err).NotTo(HaveOccurred())
			Expect(stat.Status.Size).To(Equal(int64(4*1024*1024 + 17)))

			defer reader.Close()

			fetched, _ := ioutil.ReadAll(reader)
			expected := make([]byte, 142)
			for i := 0; i < 142; i++ {
				expected[i] = byte((2*1024*1024 - 71 + i) % 10)
			}
			Expect(fetched).To(Equal(expected))
		})

		It("should remove partially written objects after a failure", func() {
			data := NewFailingLongDataReader(4*1024*1024+17, 2*1024*1024+101)
			err := client.PutObject(ctx, root+"/large-object", data)
			Expect(err).To(HaveOccurred())

			_, err = client.Stat(ctx, root+"/large-object")
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ErrNotFound))
		})

		It("should not ignore reader ErrUnexpectedEOF", func() {
			data := io.MultiReader(
				bytes.NewBufferString("12345"),
				ioutils.NewErrorReader(io.ErrUnexpectedEOF),
			)
			err := client.PutObject(ctx, root+"/new-object", data)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(io.ErrUnexpectedEOF))
		})

		It("should fail if request fails", func() {
			requestErr := errors.New("request error")

			client.HTTPClient.Client = &http.Client{
				Transport: funcTransport(func(r *http.Request) (*http.Response, error) {
					return nil, requestErr
				}),
			}

			err := client.PutObject(ctx, root+"/new-object", bytes.NewBufferString("12345"))
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(requestErr))
		})

		It("should fail if request fails with a large reader", func() {
			requestErr := errors.New("request error")

			client.HTTPClient.Client = &http.Client{
				Transport: funcTransport(func(r *http.Request) (*http.Response, error) {
					return nil, requestErr
				}),
			}

			err := client.PutObject(ctx, root+"/new-object", NewLongDataReader(4*1024*1024+17))
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(requestErr))
		})

		It("should fail if directory already exists", func() {
			Expect(client.CreateDirectory(ctx, root+"/new-object")).To(Succeed())

			err := client.PutObject(ctx, root+"/new-object", bytes.NewBufferString("12345"))
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ErrNotAFile))
		})
	})

	Describe("DeleteObject", func() {
		It("should delete object", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())

			err = client.DeleteObject(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/object")
			Expect(err).To(HaveOccurred())
		})

		It("should not delete inexistent object", func() {
			err := client.DeleteObject(ctx, root+"/object-nonexistent")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("CreateDirectory", func() {
		It("should create a directory", func() {
			err := client.CreateDirectory(ctx, root+"/subdir")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should not create an existing directory", func() {
			err := client.CreateDirectory(ctx, root+"/subdir")
			Expect(err).NotTo(HaveOccurred())

			err = client.CreateDirectory(ctx, root+"/subdir")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("CreateDirectories", func() {
		It("should create a directory tree", func() {
			err := client.CreateDirectories(ctx, root+"/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should create a directory tree when it partially exists", func() {
			err := client.CreateDirectory(ctx, root+"/subdir")
			Expect(err).NotTo(HaveOccurred())
			err = client.CreateDirectories(ctx, root+"/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/subdir/subsubdir/subsubsubdir")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("DeleteDirectory", func() {
		It("should delete a directory", func() {
			err := client.CreateDirectory(ctx, root+"/subdir")
			Expect(err).NotTo(HaveOccurred())

			err = client.DeleteDirectory(ctx, root+"/subdir")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/subdir")
			Expect(err).To(HaveOccurred())
		})

		It("should not delete a file", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			err = client.DeleteDirectory(ctx, root+"/object")
			Expect(err).To(HaveOccurred())
		})

		It("should not delete a non-existent file", func() {
			err := client.DeleteDirectory(ctx, root+"/inexistent-subdir")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("MoveObject", func() {
		It("should move an object", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())

			err = client.MoveObject(ctx, root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/object")
			Expect(err).To(HaveOccurred())

			info2, err := client.Stat(ctx, root+"/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(info2.Status.Size))
		})

		It("should not move a non-existent object", func() {
			err := client.MoveObject(ctx, root+"/object-nonexistent", root+"/object-nonexistent-2")
			Expect(err).To(HaveOccurred())
		})

		It("should move over an existing object", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())
			err = client.PutObject(ctx, root+"/object2", bytes.NewBufferString("123456"))
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())
			_, err = client.Stat(ctx, root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			err = client.MoveObject(ctx, root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/object")
			Expect(err).To(HaveOccurred())
			info, err := client.Stat(ctx, root+"/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(5)))
		})
	})

	Describe("CopyObject", func() {
		It("should copy an object", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())

			err = client.CopyObject(ctx, root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			info, err = client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())

			info2, err := client.Stat(ctx, root+"/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(info2.Status.Size))
		})

		It("should not copy a non-existent object", func() {
			err := client.CopyObject(ctx, root+"/object-nonexistent", root+"/object-nonexistent-2")
			Expect(err).To(HaveOccurred())
		})

		It("should copy over an existing object", func() {
			err := client.PutObject(ctx, root+"/object", bytes.NewBufferString("12345"))
			Expect(err).NotTo(HaveOccurred())
			err = client.PutObject(ctx, root+"/object2", bytes.NewBufferString("123456"))
			Expect(err).NotTo(HaveOccurred())

			_, err = client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())
			_, err = client.Stat(ctx, root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			err = client.CopyObject(ctx, root+"/object", root+"/object2")
			Expect(err).NotTo(HaveOccurred())

			info, err := client.Stat(ctx, root+"/object")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(5)))
			info, err = client.Stat(ctx, root+"/object2")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Status.Size).To(Equal(int64(5)))
		})
	})
})

var _ = Describe("UnmarshalTriparError", func() {
	It("should translate error", func() {
		err := UnmarshalTriparError(&http.Response{
			Body: io.NopCloser(strings.NewReader(`{
				"error_code": 2,
				"long_message": "The requested path was not found (error code 2)",
				"short_message": "No such file or directory"
			}`)),
		})
		Expect(err).To(MatchError(ErrNotFound))
		Expect(err.Error()).To(Equal("tripar error: The requested path was not found (error code 2): not found"))
	})

	It("should return nil if response body is empty", func() {
		err := UnmarshalTriparError(&http.Response{
			Body: io.NopCloser(strings.NewReader("")),
		})
		Expect(err).To(BeNil())
	})

	It("should return nil if response body json does not contain error fields", func() {
		err := UnmarshalTriparError(&http.Response{
			Body: io.NopCloser(strings.NewReader(`{"ok": true}`)),
		})
		Expect(err).To(BeNil())
	})

	It("should return err if response body read fails", func() {
		bodyReadErr := errors.New("body read error")
		err := UnmarshalTriparError(&http.Response{
			Body: io.NopCloser(ioutils.NewErrorReader(bodyReadErr)),
		})
		Expect(err).To(MatchError(bodyReadErr))
	})

	It("should return err if response body is not a valid json", func() {
		err := UnmarshalTriparError(&http.Response{
			Body: io.NopCloser(strings.NewReader(`{`)),
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal("failed to json unmarshal error response: unexpected end of JSON input"))
	})
})

var _ = Describe("UnmarshalTriparResponse", func() {
	It("should unmarshal the response", func() {
		var entries *Entries
		err := UnmarshalTriparResponse(&http.Response{
			Body: io.NopCloser(strings.NewReader(`{
				"entries": [
					{
						"name": "Entry name"
					}
				]
			}`)),
		}, &entries)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(Equal(&Entries{
			Entries: []Entry{
				{
					Name: "Entry name",
				},
			},
		}))
	})

	It("should translate error", func() {
		err := UnmarshalTriparResponse(&http.Response{
			Body: io.NopCloser(strings.NewReader(`{
				"error_code": 2,
				"long_message": "The requested path was not found (error code 2)",
				"short_message": "No such file or directory"
			}`)),
		}, nil)
		Expect(err).To(MatchError(ErrNotFound))
		Expect(err.Error()).To(Equal("tripar error: The requested path was not found (error code 2): not found"))
	})

	It("should return err if response body is empty", func() {
		err := UnmarshalTriparResponse(&http.Response{
			Body: io.NopCloser(strings.NewReader("")),
		}, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal("failed to json unmarshal error response: unexpected end of JSON input"))
	})

	It("should return nil if response body json does not contain error fields", func() {
		err := UnmarshalTriparResponse(&http.Response{
			Body: io.NopCloser(strings.NewReader(`{"ok": true}`)),
		}, nil)
		Expect(err).To(BeNil())
	})

	It("should return err if response body read fails", func() {
		bodyReadErr := errors.New("body read error")
		err := UnmarshalTriparResponse(&http.Response{
			Body: io.NopCloser(ioutils.NewErrorReader(bodyReadErr)),
		}, nil)
		Expect(err).To(MatchError(bodyReadErr))
	})

	It("should return err if response body is not a valid json", func() {
		err := UnmarshalTriparResponse(&http.Response{
			Body: io.NopCloser(strings.NewReader(`{`)),
		}, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal("failed to json unmarshal error response: unexpected end of JSON input"))
	})
})

type safeTransport struct {
	transport http.RoundTripper
	urlPrefix string
}

func (t *safeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	u := *r.URL
	u.Path = u.Opaque
	u.Opaque = ""
	if !strings.HasPrefix(u.String(), t.urlPrefix) {
		panic("request url does not start with " + t.urlPrefix + ": " + u.String())
	}
	res, err := t.transport.RoundTrip(r)
	return res, err
}

type funcTransport func(r *http.Request) (*http.Response, error)

func (t funcTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return t(r)
}

type countingBufferPool struct {
	upstream BufferPoolIface
	count    int64
}

func (p *countingBufferPool) GetCount() int64 {
	return atomic.LoadInt64(&p.count)
}

func (p *countingBufferPool) Get() []byte {
	atomic.AddInt64(&p.count, 1)
	return p.upstream.Get()
}

func (p *countingBufferPool) Put(buffer []byte) {
	atomic.AddInt64(&p.count, -1)
	p.upstream.Put(buffer)
}
