package client

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	h   *http.Client
	url string
}

func NewClient(url string, h *http.Client) *Client {
	if h == nil {
		tr := &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: true, // assume input is already compressed
		}
		h = &http.Client{Transport: tr}
	}
	if !strings.HasSuffix(url, "/") {
		url = url + "/"
	}
	return &Client{url: url, h: h}
}

func (c *Client) endpoint(op string, key string) string {
	return fmt.Sprintf("%s%s/%s", c.url, op, key)
}

func (c *Client) Set(key string, blob io.Reader) error {
	resp, err := c.h.Post(c.endpoint("set", key), "octet/stream", blob)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	sb := string(body)
	if sb != "OK" {
		return fmt.Errorf("expected OK got '%s'", sb)
	}
	return nil
}

func (c *Client) Delete(key string) error {
	resp, err := c.h.Get(c.endpoint("delete", key))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	sb := string(body)
	if sb != "OK" {
		return fmt.Errorf("expected OK got '%s'", sb)
	}
	return nil
}

func (c *Client) Get(key string) (io.ReadCloser, error) {
	resp, err := c.h.Get(c.endpoint("get", key))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		return nil, fmt.Errorf("%s", string(body))
	}
	return resp.Body, nil
}