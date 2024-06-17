package utils

import (
	"log"
	"net/http"
	"time"
)

func HTTPWithRetry(f func(string) (*http.Response, error), url string) (*http.Response, error) {
	count := 10
	var resp *http.Response
	var err error
	for i := 0; i < count; i++ {
		resp, err = f(url)
		if err != nil {
			log.Printf("[utils] Error calling url %v\n", url)
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	return resp, err
}
