package http

import (
	"fmt"
	"net/http"
	"sync"
)

func WarmUp(client *http.Client, region string, services []string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(services))
	for _, item := range services {
		go warmUpAwsService(client, region, item, wg)
	}
	wg.Wait()
}

func warmUpAwsService(client *http.Client, region string, service string, waitGroup *sync.WaitGroup) {
	format := "https://%s.%s.amazonaws.com"
	url := fmt.Sprintf(format, service, region)
	_, _ = client.Head(url)
	waitGroup.Done()
}
