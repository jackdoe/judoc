package client

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImprSrcUnsafe(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func TestExample(t *testing.T) {
	c := NewClient("http://localhost:9122/", nil)
	for i := 0; i < 1000; i++ {

		ns := RandStringBytesMaskImprSrcUnsafe(10)
		key := RandStringBytesMaskImprSrcUnsafe(100)
		// make sure we overwrite properly
		for j := 0; j < 4; j++ {
			data := []byte(RandStringBytesMaskImprSrcUnsafe((i * 100) + rand.Intn(100)))
			err := c.Set(ns, key, bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}

			reader, err := c.Get(ns, key)
			if err != nil {
				t.Fatal(err)
			}

			body, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			reader.Close()
			if !bytes.Equal(body, data) {
				t.Fatalf("unexpected %s", string(body))
			}

			err = c.Delete(ns, key)
			if err != nil {
				t.Fatal(err)
			}
			reader, err = c.Get(ns, key)
			if err == nil {
				reader.Close()
				t.Fatal("expected error")
			}
		}
	}
}
