// Package basetool implements a tool of es
package basetool

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

// operation using https
type EsOpWithTls struct {
	IpPort         string
	ClientCertFile string
	ClientKeyFile  string
	CaCertFile     string
}

func (esOpWithTls *EsOpWithTls) opInternal(op string, uri string, body io.Reader) ([]byte, error) { // {{{
	exist := CheckExist(op, []string{GET, PUT, POST, DELETE})
	if !exist {
		return nil, Error{Code: ErrInvalidParam, Message: "Invalid op: " + op}
	}

	var cert tls.Certificate
	var err error
	if esOpWithTls.ClientCertFile != "" && esOpWithTls.ClientKeyFile != "" {
		cert, err = tls.LoadX509KeyPair(esOpWithTls.ClientCertFile, esOpWithTls.ClientKeyFile)
		if err != nil {
			log.Printf("Error creating x509 keypair from client cert file %v and client key file %v",
				esOpWithTls.ClientCertFile, esOpWithTls.ClientKeyFile)
			return nil, Error{Code: ErrTlsLoadX509Failed, Message: err.Error()}
		}
	}

	caCert, err := ioutil.ReadFile(esOpWithTls.CaCertFile)
	if err != nil {
		log.Printf("Error opening cert file %v, Error: %v", esOpWithTls.CaCertFile, err)
		return nil, Error{Code: ErrIoUtilReadFileFailed, Message: err.Error()}
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	t := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		},
	}

	client := http.Client{Transport: t, Timeout: 120 * time.Second}

	url := "https://" + esOpWithTls.IpPort + "/" + uri
	req, err := http.NewRequest(op, url, body)
	if err != nil {
		return nil, Error{Code: ErrNewRequestFailed, Message: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
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

// Get interface of https
func (esOpWithTls *EsOpWithTls) Get(uri string) ([]byte, error) { // {{{
	return esOpWithTls.opInternal(GET, uri, nil)
} // }}}

// Put interface of https
func (esOpWithTls *EsOpWithTls) Put(uri string, params string) ([]byte, error) { // {{{
	return esOpWithTls.opInternal(PUT, uri, strings.NewReader(params))
} // }}}

// Post interface of https
func (esOpWithTls *EsOpWithTls) Post(uri string, params string) ([]byte, error) { // {{{
	return esOpWithTls.opInternal(POST, uri, strings.NewReader(params))
} // }}}
