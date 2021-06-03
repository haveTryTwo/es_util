// base tool of es
package basetool

import (
	"crypto/tls"
	"crypto/x509"
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

// Get interface of https
func (esOpWithTls *EsOpWithTls) Get(uri string) ([]byte, error) { // {{{
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

	client := http.Client{Transport: t, Timeout: 60 * time.Second}

	url := "https://" + esOpWithTls.IpPort + "/" + uri
	req, err := http.NewRequest("GET", url, nil)
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

// Put interface of https
func (esOpWithTls *EsOpWithTls) Put(uri string, params string) ([]byte, error) { // {{{
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

	client := http.Client{Transport: t, Timeout: 60 * time.Second}

	url := "https://" + esOpWithTls.IpPort + "/" + uri
	req, err := http.NewRequest("PUT", url, strings.NewReader(params))
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

// Post interface of https
func (esOpWithTls *EsOpWithTls) Post(uri string, params string) ([]byte, error) { // {{{
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

	client := http.Client{Transport: t, Timeout: 60 * time.Second}

	url := "https://" + esOpWithTls.IpPort + "/" + uri
	req, err := http.NewRequest("POST", url, strings.NewReader(params))
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
