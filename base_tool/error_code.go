// base tool of es
package basetool

import (
	"fmt"
)

const (
	Ok = 0

	ErrInvalidParam         = -10000
	ErrInternalServerFailed = -10001
	ErrInvalidIndex         = -10002
	ErrInvalidNumber        = -10003
	ErrInvalidContent       = -10004

	ErrNotFound        = -10100
	ErrNotEqual        = -10101
	ErrMakeDirFailed   = -10102
	ErrOpenFileFailed  = -10103
	ErrWriteFileFailed = -10104
	ErrReadLineFailed  = -10105
	ErrStatFileFailed  = -10106

	ErrHttpDoFailed         = -10200
	ErrNewRequestFailed     = -10201
	ErrIoUtilReadAllFailed  = -10202
	ErrIoUtilReadFileFailed = -10203

	ErrTlsLoadX509Failed = -10300

	ErrJsonUnmarshalFailed = -10400
	ErrJsonMarshalFailed   = -10401

	ErrAtoiFailed = -10500

	ErrRespErr = -10600
)

// error including code and message
type Error struct {
	Code    int
	Message string
}

// implemation Error interface
func (e Error) Error() string {
	return fmt.Sprintf("%v: %v", e.Code, e.Message)
}

// Decode internal Error
func DecodeErr(err error) (int, string) {
	if err == nil {
		return Ok, "Sucess"
	}
	switch typed := err.(type) {
	case Error:
		return typed.Code, typed.Message
	default:
	}

	return ErrInternalServerFailed, err.Error()
}

// Implemation of Assert function
func Assert(flag bool, info interface{}) {
	if flag == false {
		panic(info)
	}
}
