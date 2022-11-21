package http

import (
	"fmt"
	"net/http"
	"sync"
)

type sslContextWarmerImpl struct {
	httpClient *http.Client
}

func NewSslContextWarmer(httpClient *http.Client) SslContextWarmer {
	return &sslContextWarmerImpl{
		httpClient: httpClient,
	}
}

func (w sslContextWarmerImpl) WarmUp(region string, services []string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(services))
	for _, item := range services {
		go w.warmUpAwsService(region, item, wg)
	}
	wg.Wait()
}

const AWSServiceURLTemplate = "https://%s.%s.amazonaws.com"

func (w sslContextWarmerImpl) warmUpAwsService(region string, service string, waitGroup *sync.WaitGroup) {
	url := fmt.Sprintf(AWSServiceURLTemplate, service, region)
	_, _ = w.httpClient.Head(url)
	waitGroup.Done()
}
