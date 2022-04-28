package triparclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	httpclient "github.com/koofr/go-httpclient"
	ioutils "github.com/koofr/go-ioutils"
	"golang.org/x/xerrors"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrNotAFile      = errors.New("not a file")
	ErrAlreadyExists = errors.New("already exists")
	ErrBadRange      = errors.New("bad range")
	ErrOther         = errors.New("unknown error")
)

type TriparClient struct {
	HTTPClient   *httpclient.HTTPClient
	bufferPool   BufferPoolIface
	getChunkSize int64
}

func basicAuth(user string, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func translateError(err *Error) error {
	switch err.Code {
	case 2:
		return ErrNotFound
	case 17:
		return ErrAlreadyExists
	case 21:
		return ErrNotAFile
	case 10004:
		return ErrBadRange
	default:
		return err
	}
}

func NewTriparClient(
	endpoint string,
	user string,
	pass string,
	share string,
	bp BufferPoolIface,
	getChunkSize int64,
) (tp *TriparClient, err error) {
	if share != "" {
		if !strings.HasSuffix(endpoint, "/") {
			endpoint += "/"
		}
		endpoint += share
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	client := httpclient.Insecure()
	client.BaseURL = u
	client.Headers.Set("Authorization", basicAuth(user, pass))

	tp = &TriparClient{
		HTTPClient:   client,
		bufferPool:   bp,
		getChunkSize: getChunkSize,
	}

	return tp, nil
}

func (tp *TriparClient) request(req *httpclient.RequestData) (response *http.Response, err error) {
	return tp.HTTPClient.Request(req)
}

func (tp *TriparClient) path(path string) string {
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func (tp *TriparClient) cmd(cmd string) (params url.Values) {
	params = make(url.Values)
	params.Set("cmd", cmd)
	return params
}

func (tp *TriparClient) Stat(ctx context.Context, path string) (info Stat, err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           tp.path(path),
		Params:         tp.cmd("stat"),
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return Stat{}, xerrors.Errorf("stat request error: %w", err)
	}

	if err := UnmarshalTriparResponse(rsp, &info); err != nil {
		return Stat{}, xerrors.Errorf("stat response error: %w", err)
	}

	return info, nil
}

func (tp *TriparClient) DeleteDirectory(ctx context.Context, path string) (err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "DELETE",
		Path:           tp.path(path),
		Params:         tp.cmd("rmdir"),
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return xerrors.Errorf("delete directory request error: %w", err)
	}

	if err := UnmarshalTriparError(rsp); err != nil {
		return xerrors.Errorf("delete directory response error: %w", err)
	}

	return nil
}

func (tp *TriparClient) CreateDirectory(ctx context.Context, path string) (err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           tp.path(path),
		Params:         tp.cmd("mkdir"),
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return xerrors.Errorf("create directory directory request error: %w", err)
	}

	if err := UnmarshalTriparError(rsp); err != nil {
		return xerrors.Errorf("create directory response error: %w", err)
	}

	return nil
}

func (tp *TriparClient) CreateDirectories(ctx context.Context, path string) (err error) {
	params := tp.cmd("mkdir")
	params.Set("parents", "true")
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           tp.path(path),
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return xerrors.Errorf("delete directories request error: %w", err)
	}

	if err := UnmarshalTriparError(rsp); err != nil {
		return xerrors.Errorf("delete directories response error: %w", err)
	}

	return nil
}

func (tp *TriparClient) List(ctx context.Context, path string) (entries Entries, err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           tp.path(path),
		Params:         tp.cmd("ls"),
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return Entries{}, xerrors.Errorf("list request error: %w", err)
	}

	if err := UnmarshalTriparResponse(rsp, &entries); err != nil {
		return Entries{}, xerrors.Errorf("list response error: %w", err)
	}

	return entries, nil
}

func (tp *TriparClient) GetObject(
	ctx context.Context,
	path string,
	span *ioutils.FileSpan,
) (rd io.ReadCloser, info *Stat, err error) {
	stat, err := tp.Stat(ctx, path)
	if err != nil {
		return nil, nil, xerrors.Errorf("get object stat error: %w", err)
	}

	if span == nil || span.End-span.Start <= tp.getChunkSize {
		rd, err = tp.getObjectComplete(ctx, path, span, stat)
		if err != nil {
			return nil, nil, xerrors.Errorf("getObjectComplete error: %w", err)
		}
		return rd, &stat, nil
	}

	rd, err = tp.getObjectByChunks(ctx, path, span, stat)
	if err != nil {
		return nil, nil, xerrors.Errorf("getObjectByChunks error: %w", err)
	}
	return rd, &stat, nil
}

func (tp *TriparClient) getObjectResponse(
	ctx context.Context,
	path string,
	span *ioutils.FileSpan,
) (resp *http.Response, err error) {
	req := httpclient.RequestData{
		Context:        ctx,
		Method:         "GET",
		Path:           tp.path(path),
		ExpectedStatus: []int{http.StatusOK, http.StatusPartialContent},
	}
	if span != nil {
		req.Headers = make(http.Header)
		req.Headers.Set("Range", fmt.Sprintf("bytes=%d-%d", span.Start, span.End))
	}
	rsp, err := tp.request(&req)
	if err != nil {
		return nil, xerrors.Errorf("getObject request error: %w", err)
	}

	ctype := rsp.Header.Get("Content-Type")
	if !strings.HasPrefix(ctype, "application/octet-stream") {
		return nil, xerrors.Errorf("unexpected content-type error: %w", UnmarshalTriparError(rsp))
	}

	return rsp, nil
}

func (tp *TriparClient) getObjectComplete(
	ctx context.Context,
	path string,
	span *ioutils.FileSpan,
	stat Stat,
) (rd io.ReadCloser, err error) {
	rsp, err := tp.getObjectResponse(ctx, path, span)
	if err != nil {
		return nil, err
	}
	return rsp.Body, nil
}

func (tp *TriparClient) getObjectByChunks(
	ctx context.Context,
	path string,
	span *ioutils.FileSpan,
	stat Stat,
) (rd io.ReadCloser, err error) {
	/* NOTE: we will fetch files in chunks, as Object Access API implementation
	   seems to have a problem with (a) large files and (b) large ranges. fuck
	   HPE. */

	left := stat.Status.Size
	start := int64(0)
	if span != nil {
		left = span.End - span.Start + 1
		start = span.Start
	}

	if left-start > stat.Status.Size || start < 0 || left <= 0 {
		return nil, ErrBadRange
	}

	r, w := io.Pipe()

	nextChunk := func() error {
		len := left
		if len > tp.getChunkSize {
			len = tp.getChunkSize
		}

		rsp, err := tp.getObjectResponse(ctx, path, &ioutils.FileSpan{Start: start, End: start + len - 1})
		if err != nil {
			return xerrors.Errorf("getObjectByChunks getObjectResponse error: %w", err)
		}
		defer rsp.Body.Close()

		rlen, err := strconv.ParseInt(rsp.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			return err
		}

		left -= rlen
		start += rlen

		n, err := io.Copy(w, rsp.Body)
		if err != nil {
			return err
		}
		if n != rlen {
			return xerrors.Errorf("failed to copy whole response: %d != %d", n, rlen)
		}

		return nil
	}

	go func() {
		for left > 0 {
			if err := nextChunk(); err != nil {
				w.CloseWithError(err)
				return
			}
		}

		w.Close()
	}()

	return r, nil
}

func (tp *TriparClient) Fsync(ctx context.Context, path string) (err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "POST",
		Path:           tp.path(path),
		Params:         tp.cmd("fsync"),
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return xerrors.Errorf("fsync request error: %w", err)
	}

	if err := UnmarshalTriparError(rsp); err != nil {
		return xerrors.Errorf("fsync response error: %w", err)
	}

	return nil
}

type PutPiece struct {
	Buffer []byte
	Read   int
	Err    error
}

func (tp *TriparClient) PutObject(ctx context.Context, path string, reader io.Reader) (err error) {
	pipe := make(chan *PutPiece, 1)

	pipeWriterDone := make(chan struct{})
	pipeReaderDone := make(chan struct{})

	defer func() {
		close(pipeReaderDone)

		// we need to drain the pipe and put the buffers back to the pool
		for piece := range pipe {
			tp.bufferPool.Put(piece.Buffer)
		}

		<-pipeWriterDone
	}()

	go func() {
		defer close(pipe)
		defer close(pipeWriterDone)

		for {
			piece := &PutPiece{
				Buffer: tp.bufferPool.Get(),
				Read:   0,
				Err:    nil,
			}

			// Fill the whole buffer so that we minimise the number of writes, as the
			// latency of PUT/POST requests negatively impacts performance. We cannot
			// use io.ReadFull because it returns ErrUnexpectedEOF which we must not
			// ignore otherwise we might ignore ErrUnexpectedEOF from the upstream
			// reader.
			piece.Read, piece.Err = ioutils.ReadFillBuffer(reader, piece.Buffer)

			select {
			case pipe <- piece:
			case <-pipeReaderDone:
				tp.bufferPool.Put(piece.Buffer)
				return
			}

			if piece.Err != nil {
				break
			}
		}
	}()

	written := 0

	defer func() {
		if err != nil {
			_ = tp.DeleteObject(ctx, path)
		}
	}()

	handlePiece := func(piece *PutPiece) error {
		defer tp.bufferPool.Put(piece.Buffer)

		if piece.Err != nil && piece.Err != io.EOF {
			return piece.Err
		}

		req := &httpclient.RequestData{
			Context:          ctx,
			Path:             tp.path(path),
			ExpectedStatus:   []int{http.StatusOK, http.StatusCreated},
			ReqReader:        bytes.NewReader(piece.Buffer[:piece.Read]),
			ReqContentLength: int64(piece.Read),
		}
		if written == 0 {
			req.Method = "PUT"
		} else {
			req.Method = "POST"
			req.Headers = make(http.Header)
			req.Headers.Set("Range", fmt.Sprintf("bytes=%d-%d", written, written+piece.Read-1))
		}
		rsp, err := tp.request(req)
		if err != nil {
			return xerrors.Errorf("put object request error: %w", err)
		}
		if err := UnmarshalTriparError(rsp); err != nil {
			return xerrors.Errorf("put object response error: %w", err)
		}

		written += piece.Read

		return nil
	}

	for {
		piece, ok := <-pipe
		if !ok {
			return nil
		}

		if err := handlePiece(piece); err != nil {
			return err
		}
	}
}

func (tp *TriparClient) DeleteObject(ctx context.Context, path string) (err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "DELETE",
		Path:           tp.path(path),
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return xerrors.Errorf("delete object request error: %w", err)
	}

	if err := UnmarshalTriparError(rsp); err != nil {
		return xerrors.Errorf("delete object response error: %w", err)
	}

	return nil
}

func (tp *TriparClient) MoveObject(ctx context.Context, path string, nupath string) (err error) {
	params := tp.cmd("mv")
	params.Set("destination", nupath)
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "POST",
		Path:           tp.path(path),
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return xerrors.Errorf("move object request error: %w", err)
	}

	if err := UnmarshalTriparError(rsp); err != nil {
		return xerrors.Errorf("move object response error: %w", err)
	}

	return nil
}

func (tp *TriparClient) CopyObject(ctx context.Context, path string, nupath string) (err error) {
	params := tp.cmd("cp")
	params.Set("destination", nupath)
	params.Set("overwrite", "true")
	rsp, err := tp.request(&httpclient.RequestData{
		Context:        ctx,
		Method:         "PUT",
		Path:           tp.path(path),
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
	})
	if err != nil {
		return xerrors.Errorf("copy object request error: %w", err)
	}

	if err := UnmarshalTriparError(rsp); err != nil {
		return xerrors.Errorf("copy object response error: %w", err)
	}

	return nil
}

func UnmarshalTriparError(r *http.Response) (err error) {
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	if len(body) == 0 {
		return nil
	}

	perr, err := UnmarshalError(body)
	if err != nil {
		return xerrors.Errorf("failed to json unmarshal error response: %w", err)
	}
	if perr != nil {
		return xerrors.Errorf("tripar error: %s: %w", perr.LMsg, translateError(perr))
	}

	return nil
}

func UnmarshalTriparResponse(r *http.Response, i interface{}) error {
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return xerrors.Errorf("failed to read response body: %w", err)
	}

	perr, err := UnmarshalError(body)
	if err != nil {
		return xerrors.Errorf("failed to json unmarshal error response: %w", err)
	}
	if perr != nil {
		return xerrors.Errorf("tripar error: %s: %w", perr.LMsg, translateError(perr))
	}

	if err := json.Unmarshal(body, &i); err != nil {
		return xerrors.Errorf("failed to json unmarshal response: %w", err)
	}

	return nil
}
