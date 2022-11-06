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

// EsOpWithTls operation using https
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

	client := &http.Client{Transport: t, Timeout: 120 * time.Second}

	url := "https://" + esOpWithTls.IpPort + "/" + uri

	return httpReqInternal(op, url, body, client)
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

// Delete interface of https
func (esOpWithTls *EsOpWithTls) Delete(uri string) ([]byte, error) { // {{{
	return esOpWithTls.opInternal(DELETE, uri, nil)
} // }}}
