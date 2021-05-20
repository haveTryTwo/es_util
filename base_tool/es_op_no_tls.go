package base_tool

import (
	//    "encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type EsOpNoTls struct {
	IpPort string
}

func (esOpNoTls *EsOpNoTls) Get(uri string) ([]byte, error) { // {{{
	url := "http://" + esOpNoTls.IpPort + "/" + uri
	req, err := http.NewRequest("GET", url, nil)
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

func (esOpNoTls *EsOpNoTls) Put(uri string, params string) ([]byte, error) { // {{{
	url := "http://" + esOpNoTls.IpPort + "/" + uri
	req, err := http.NewRequest("PUT", url, strings.NewReader(params))
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

func (esOpNoTls *EsOpNoTls) Post(uri string, params string) ([]byte, error) { // {{{
	url := "http://" + esOpNoTls.IpPort + "/" + uri
	req, err := http.NewRequest("POST", url, strings.NewReader(params))
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
