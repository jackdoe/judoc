package client

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestExample(t *testing.T) {
	c := NewClient("http://localhost:9122/", nil)

	err := c.Set("abc", bytes.NewReader([]byte("abcdef")))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := c.Get("abc")
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	if string(body) != "abcdef" {
		t.Fatalf("unexpected %s", string(body))
	}

	err = c.Delete("abc")
	if err != nil {
		t.Fatal(err)
	}
	reader, err = c.Get("abc")
	if err == nil {
		reader.Close()
		t.Fatal("expected error")
	}
}
