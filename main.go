package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	log "github.com/sirupsen/logrus"
)

/*

$ sudo docker run -p 9042:9042 scylladb/scylla
$ sudo docker exec -t -i $( sudo docker ps | grep scylla | awk '{ print $1 }') cqlsh

CREATE KEYSPACE "baxx"  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};
CREATE TABLE baxx.blocks (key ascii, id bigint, part blob, PRIMARY KEY ((key), id));

*/

func main() {
	var pbind = flag.String("bind", ":9122", "bind")

	var pcluster = flag.String("cluster", "127.0.0.1", "comma separated values of the cassandra cluster")
	var pblockSize = flag.Int("block-size", 4*1024*1024, "block size in bytes")
	var pkeyspace = flag.String("keyspace", "baxx", "cassandra keyspace")
	flag.Parse()

	cluster := gocql.NewCluster(strings.Split(*pcluster, ",")...)
	cluster.Keyspace = *pkeyspace
	cluster.Consistency = gocql.Any
	cluster.Timeout = 1 * time.Minute
	session, err := cluster.CreateSession()
	if err != nil {
		log.Panic(err)
	}
	defer session.Close()

	http.HandleFunc("/set/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.RequestURI, "/set/")

		body := r.Body
		defer body.Close()

		if err := DeleteObject(key, session); err != nil {
			http.Error(w, err.Error(), 500)
		} else {
			err := WriteObject(*pblockSize, key, body, session)
			if err != nil {
				http.Error(w, err.Error(), 500)
			} else {
				fmt.Fprintf(w, "OK")
			}
		}
	})

	http.HandleFunc("/get/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.RequestURI, "/get/")

		w.Header().Set("Transfer-Encoding", "chunked")
		reader, err := ReadObject(key, session)
		if err != nil {
			http.Error(w, err.Error(), 500)
		} else {
			_, err := io.Copy(w, reader)
			if err != nil {
				http.Error(w, err.Error(), 500)
			}
		}
	})

	http.HandleFunc("/delete/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.RequestURI, "/delete/")

		if err := DeleteObject(key, session); err != nil {
			http.Error(w, err.Error(), 500)
		} else {
			fmt.Fprintf(w, "OK")
		}
	})

	log.Fatal(http.ListenAndServe(*pbind, nil))
}

func DeleteObject(key string, session *gocql.Session) error {
	log.Infof("removing %s", key)
	if err := session.Query(`DELETE FROM blocks WHERE key = ?`, key).Exec(); err != nil {
		log.Warnf("error removing, key: %s, error: %s", key, err.Error())
		return err
	}
	return nil
}

func WriteObject(blockSize int, key string, body io.Reader, session *gocql.Session) error {
	log.Infof("setting %s", key)
	buf := make([]byte, blockSize)
	id := int64(0)
	for {
		end := false
		n, err := io.ReadFull(body, buf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				end = true
			} else {
				log.Warnf("error reading, key: %s, error: %s", key, err.Error())
				if err := DeleteObject(key, session); err != nil {
					return err
				}
				return err
			}
		}
		if n > 0 {
			id++
			part := buf[:n]
			t0 := time.Now()
			if err := session.Query(`INSERT INTO blocks (key, id, part) VALUES (?, ?, ?)`, key, id, part).Exec(); err != nil {
				log.Warnf("error inserting, key: %s, block id: %d, error: %s", key, id, err.Error())
				if err := DeleteObject(key, session); err != nil {
					return err
				}
				return err
			}
			log.Infof("  key: %s creating block id: %d, size %d, took %d", key, id, len(part), time.Now().Sub(t0).Nanoseconds()/1e6)
		}
		if end {
			break
		}
	}
	return nil
}

type ChunkReader struct {
	blocks     []int64
	blockIndex int
	part       []byte
	cursor     int
	key        string
	session    *gocql.Session
}

func (c *ChunkReader) ReadBlock() error {
	if len(c.blocks) == c.blockIndex {
		return io.EOF
	}
	t0 := time.Now()

	if err := c.session.Query(`SELECT part FROM blocks WHERE key = ? AND id = ?`, c.key, c.blocks[c.blockIndex]).Consistency(gocql.One).Scan(&c.part); err != nil {
		return err
	}
	log.Printf("  key: %s reading block %d, size: %d, took: %d", c.key, c.blocks[c.blockIndex], len(c.part), time.Now().Sub(t0).Nanoseconds()/1e6)
	c.blockIndex++
	return nil
}

func (c *ChunkReader) Read(p []byte) (int, error) {
	if c.part == nil || c.cursor == len(c.part) {
		if err := c.ReadBlock(); err != nil {
			return 0, err
		}

		c.cursor = 0
	}
	n := copy(p, c.part[c.cursor:])
	c.cursor += n
	return n, nil
}

func ReadObject(key string, session *gocql.Session) (*ChunkReader, error) {
	log.Infof("getting %s", key)

	blocks := []int64{}
	block := int64(-1)
	for {
		ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
		iter := session.Query(`SELECT id FROM blocks WHERE key = ? AND id > ? LIMIT 20`, key, block).WithContext(ctx).Consistency(gocql.One).Iter()
		atLeastOne := false
		for iter.Scan(&block) {
			blocks = append(blocks, block)
			atLeastOne = true
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		if !atLeastOne {
			break
		}
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("NOTFOUND")
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i] < blocks[j]
	})
	return &ChunkReader{blocks: blocks, key: key, session: session}, nil
}
