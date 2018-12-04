package triparclient

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	httpclient "github.com/koofr/go-httpclient"
	ioutils "github.com/koofr/go-ioutils"
)

var (
	ERR_NOT_FOUND      = errors.New("NotFound")
	ERR_NOT_A_FILE     = errors.New("NotAFile")
	ERR_ALREADY_EXISTS = errors.New("AlreadyExists")
	ERR_OTHER          = errors.New("Wtf")
)

type TriparClient struct {
	HTTPClient *httpclient.HTTPClient
	bufferPool *BufferPool
}

func basicAuth(user string, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func translateError(err Error) error {
	switch err.Code {
	case 2:
		return ERR_NOT_FOUND
	case 17:
		return ERR_ALREADY_EXISTS
	case 21:
		return ERR_NOT_A_FILE
	default:
		return err
	}
}

func NewTriparClient(endpoint string, user string, pass string, share string, bp *BufferPool) (tp *TriparClient, err error) {
	if share != "" {
		if !strings.HasSuffix(endpoint, "/") {
			endpoint += "/"
		}
		endpoint += share
	}

	u, err := url.Parse(endpoint)

	if err != nil {
		return
	}

	client := httpclient.Insecure()
	client.BaseURL = u
	client.Headers.Set("Authorization", basicAuth(user, pass))

	tp = &TriparClient{
		HTTPClient: client,
		bufferPool: bp,
	}

	return
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
	return
}

func (tp *TriparClient) unmarshalTriparError(r *http.Response) (err error) {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return
	}

	if len(body) == 0 {
		err = nil
	} else {
		perr := Error{}
		if json.Unmarshal(body, &perr) != nil {
			err = nil
		} else {
			err = translateError(perr)
		}
	}
	return
}

func (tp *TriparClient) unmarshalTriparResponse(r *http.Response,
	i interface{}) (err error) {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return
	}

	perr := Error{}
	err = json.Unmarshal(body, &perr)
	if err == ERR_NOT_AN_ERROR {
		err = json.Unmarshal(body, &i)
	} else {
		err = translateError(perr)
	}
	return
}

func (tp *TriparClient) Stat(path string) (info Stat, err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Method:         "GET",
		Path:           tp.path(path),
		Params:         tp.cmd("stat"),
		ExpectedStatus: []int{http.StatusOK},
	})

	if err != nil {
		return
	}

	err = tp.unmarshalTriparResponse(rsp, &info)
	return
}

func (tp *TriparClient) DeleteDirectory(path string) (err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Method:         "DELETE",
		Path:           tp.path(path),
		Params:         tp.cmd("rmdir"),
		ExpectedStatus: []int{http.StatusOK},
	})

	if err != nil {
		return
	}

	err = tp.unmarshalTriparError(rsp)
	return
}

func (tp *TriparClient) CreateDirectory(path string) (err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Method:         "PUT",
		Path:           tp.path(path),
		Params:         tp.cmd("mkdir"),
		ExpectedStatus: []int{http.StatusOK},
	})

	if err != nil {
		return
	}

	err = tp.unmarshalTriparError(rsp)
	return
}

func (tp *TriparClient) List(path string) (entries Entries, err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Method:         "GET",
		Path:           tp.path(path),
		Params:         tp.cmd("ls"),
		ExpectedStatus: []int{http.StatusOK},
	})

	if err != nil {
		return
	}

	err = tp.unmarshalTriparResponse(rsp, &entries)
	return
}

func (tp *TriparClient) GetObject(path string, span *ioutils.FileSpan) (rd io.ReadCloser, err error) {
	req := httpclient.RequestData{
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
		return nil, err
	}
	ctype := rsp.Header.Get("Content-Type")
	if strings.HasPrefix(ctype, "application/octet-stream") {
		return rsp.Body, nil
	} else {
		err = tp.unmarshalTriparError(rsp)
		if err != nil {
			err = ERR_OTHER
		}
		return nil, err
	}
}

type PutPiece struct {
	Buffer []byte
	Read   int
	Err    error
}

func (tp *TriparClient) PutObject(path string, reader io.Reader) (err error) {
	pipe := make(chan *PutPiece, 1)
	go func() {
		for {
			piece := &PutPiece{
				Buffer: tp.bufferPool.Get(),
				Read:   0,
				Err:    nil,
			}
			piece.Read, piece.Err = reader.Read(piece.Buffer)
			pipe <- piece
			if piece.Err != nil {
				break
			}
		}
	}()
	written := 0
	defer func() {
		if err != nil {
			tp.DeleteObject(path)
		}
	}()
	for {
		piece := <-pipe
		if piece.Read > 0 {
			req := &httpclient.RequestData{
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
			rsp, werr := tp.request(req)
			tp.bufferPool.Put(piece.Buffer)
			if werr != nil {
				err = werr
				break
			}
			werr = tp.unmarshalTriparError(rsp)
			if werr != nil {
				err = werr
				break
			}
			written += piece.Read
		} else {
			tp.bufferPool.Put(piece.Buffer)
		}
		if piece.Err != nil {
			if piece.Err != io.EOF {
				err = piece.Err
			}
			break
		}
	}
	return
}

func (tp *TriparClient) DeleteObject(path string) (err error) {
	rsp, err := tp.request(&httpclient.RequestData{
		Method:         "DELETE",
		Path:           tp.path(path),
		ExpectedStatus: []int{http.StatusOK},
	})

	if err != nil {
		return
	}

	err = tp.unmarshalTriparError(rsp)
	return
}
