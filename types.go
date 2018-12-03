package triparclient

import (
	"encoding/json"
	"errors"
)

type Status struct {
	Atime   float64 `json:"atime"`
	Blksize int64   `json:"blksize"`
	Blocks  int64   `json:"blocks"`
	Ctime   float64 `json:"ctime"`
	Dev     int32   `json:"dev"`
	Gid     int32   `json:"gid"`
	//	Ino     int64   `json:"ino"`
	Mode  int32   `json:"mode"`
	Mtime float64 `json:"mtime"`
	Nlink int32   `json:"nlink"`
	Rdev  int32   `json:"rdev"`
	Size  int64   `json:"size"`
	Uid   int32   `json:"uid"`
}

type Stat struct {
	Path   string `json:"path"`
	Status Status `json:"status"`
}

func (s Stat) IsDir() bool {
	return (((s.Status.Mode) & (0170000)) == (0040000))
}

type Entries struct {
	Entries []Entry `json:"entries"`
}

type Entry struct {
	Name string `json:"name"`
}

type Error struct {
	Code int    `json:"error_code"`
	LMsg string `json:"long_message"`
	SMsg string `json:"short_message"`
}

func (e Error) Error() string {
	return e.SMsg
}

var ERR_NOT_AN_ERROR = errors.New("NotAnErrorCode")

func (e *Error) UnmarshalJSON(data []byte) (err error) {
	required := struct {
		Code *int    `json:"error_code"`
		LMsg *string `json:"long_message"`
		SMsg *string `json:"short_message"`
	}{}
	err = json.Unmarshal(data, &required)
	if err != nil {
		return
	}
	if required.Code == nil || required.LMsg == nil || required.SMsg == nil {
		err = ERR_NOT_AN_ERROR
	} else {
		e.Code = *required.Code
		e.LMsg = *required.LMsg
		e.SMsg = *required.SMsg
	}
	return
}
