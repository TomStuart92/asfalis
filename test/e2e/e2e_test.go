package e2e

import (
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSetRecord(t *testing.T) {
	client := &http.Client{}
	req, _ := http.NewRequest("PUT", "http://127.0.0.1:12380/key", strings.NewReader("Key"))
	res, err := client.Do(req)
	if err != nil {
		t.Error(err)
	}
	if res.StatusCode != http.StatusNoContent {
		t.Errorf("Response code was %v; want 204", res.StatusCode)
	}
}

func TestGetRecord(t *testing.T) {
	client := &http.Client{}
	req, _ := http.NewRequest("PUT", "http://127.0.0.1:12380/key", strings.NewReader("Value"))
	res, err := client.Do(req)
	if err != nil {
		t.Error(err)
	}
	if res.StatusCode != http.StatusNoContent {
		t.Errorf("Response code was %v; want 204", res.StatusCode)
	}

	time.Sleep(2 * time.Second)
	req, _ = http.NewRequest("GET", "http://127.0.0.1:12380/key", nil)
	res, err = client.Do(req)
	if err != nil {
		t.Error(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("Response code was %v; want 200", res.StatusCode)
	}
	if body, _ := ioutil.ReadAll(res.Body); string(body) != "Value" {
		t.Errorf("Incorrect Value got %s, want value", body)
	}
}
