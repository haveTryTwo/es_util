// Package basetool implements a tool of es
package basetool

import (
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// operation using http
type EsOpNoTls struct {
	IpPort string
}

func (esOpNoTls *EsOpNoTls) opInternal(op string, uri string, body io.Reader) ([]byte, error) { // {{{
	exist := CheckExist(op, []string{GET, PUT, POST, DELETE})
	if !exist {
		return nil, Error{Code: ErrInvalidParam, Message: "Invalid op: " + op}
	}

	url := "http://" + esOpNoTls.IpPort + "/" + uri
	req, err := http.NewRequest(op, url, body)
	if err != nil {
		return nil, Error{Code: ErrNewRequestFailed, Message: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		log.Printf("url:%v, err:%v", url, err.Error())
		return nil, Error{Code: ErrHttpDoFailed, Message: err.Error()}
	}

	defer resp.Body.Close()

	respByte, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("url:%v, err:%v", url, err.Error())
		return nil, Error{Code: ErrIoUtilReadAllFailed, Message: err.Error()}
	}

	return respByte, nil
} // }}}

// Get interface of http
func (esOpNoTls *EsOpNoTls) Get(uri string) ([]byte, error) { // {{{
	return esOpNoTls.opInternal(GET, uri, nil)
} // }}}

// Put interface of http
func (esOpNoTls *EsOpNoTls) Put(uri string, params string) ([]byte, error) { // {{{
	return esOpNoTls.opInternal(PUT, uri, strings.NewReader(params))
} // }}}

// Post interface of http
func (esOpNoTls *EsOpNoTls) Post(uri string, params string) ([]byte, error) { // {{{
	return esOpNoTls.opInternal(POST, uri, strings.NewReader(params))
} // }}}
