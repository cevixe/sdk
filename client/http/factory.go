package http

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

func NewClient(httpSettings ClientSettings) (*http.Client, error) {

	var client http.Client
	tr := &http.Transport{
		ResponseHeaderTimeout: httpSettings.ResponseHeader,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: httpSettings.ConnKeepAlive,
			Timeout:   httpSettings.Connect,
		}).DialContext,
		MaxIdleConns:          httpSettings.MaxAllIdleConns,
		IdleConnTimeout:       httpSettings.IdleConn,
		TLSHandshakeTimeout:   httpSettings.TLSHandshake,
		MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
		ExpectContinueTimeout: httpSettings.ExpectContinue,
	}

	err := http2.ConfigureTransport(tr)
	if err != nil {
		return &client, err
	}

	client = http.Client{
		Transport: tr,
	}

	return &client, nil
}

func NewDefaultClient() *http.Client {

	httpClient, err := NewClient(ClientSettings{
		Connect:          5 * time.Second,
		ExpectContinue:   1 * time.Second,
		IdleConn:         90 * time.Second,
		ConnKeepAlive:    60 * time.Second,
		MaxAllIdleConns:  100,
		MaxHostIdleConns: 10,
		ResponseHeader:   5 * time.Second,
		TLSHandshake:     5 * time.Second,
	})

	if err != nil {
		panic(fmt.Errorf("cannot create default http client\n%v", err))
	}

	return httpClient
}
