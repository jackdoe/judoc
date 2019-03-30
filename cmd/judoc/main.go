package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

/*

$ sudo docker run -p 9042:9042 scylladb/scylla
$ sudo docker exec -t -i $( sudo docker ps | grep scylla | awk '{ print $1 }') cqlsh

CREATE KEYSPACE "baxx"  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};
CREATE TABLE baxx.blocks (key ascii, id int, part blob, created_at timestamp, PRIMARY KEY ((key), id));
CREATE TABLE baxx.blocks_id_cache (key ascii, ids list<int>, created_at timestamp, PRIMARY KEY (key));

# with 4mb block size, 10gb file has 2k rows, this is super slow to query from cassandra,
# SELECT part FROM blocks WHERE key=abc
# so we can just have id cache
# this also means we never do walkable query, and dont care about cassandra's tombstones
# the reason we dont just keep the max block id, is because i think in the future we will
# parallelize the writing, and then blocks will not be continuous

*/

func main() {
	var pbind = flag.String("bind", ":9122", "bind")

	var pcluster = flag.String("cluster", "127.0.0.1", "comma separated values of the cassandra cluster")
	var pblockSize = flag.Int("block-size", 4*1024*1024, "block size in bytes")
	var pconsistency = flag.String("consistency", "ANY", "write consistency: ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, LOCAL_ONE")
	var pkeyspace = flag.String("keyspace", "baxx", "cassandra keyspace")
	var pcapath = flag.String("capath", "", "ssl ca path")
	var pkeypath = flag.String("keypath", "", "ssl key path")
	var pcertpath = flag.String("certpath", "", "ssl cert path")
	flag.Parse()
	consistency := gocql.Any
	err := consistency.UnmarshalText([]byte(*pconsistency))
	if err != nil {
		log.Panic(err)
	}

	cluster := gocql.NewCluster(strings.Split(*pcluster, ",")...)
	cluster.Keyspace = *pkeyspace
	cluster.Consistency = consistency
	cluster.Timeout = 1 * time.Minute
	if *pcapath != "" || *pcertpath != "" || *pkeypath != "" {
		cluster.SslOpts = &gocql.SslOptions{
			EnableHostVerification: true,
			CaPath:                 *pcapath,
			KeyPath:                *pkeypath,
			CertPath:               *pcertpath,
		}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		log.Panic(err)
	}
	defer session.Close()
	log.Printf("bind to: %s, cassandra: %s/%s consistency: %s, block size: %d", *pbind, *pcluster, *pkeyspace, consistency, *pblockSize)
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
			if err.Error() == "NOTFOUND" {
				http.Error(w, err.Error(), 404)
			} else {
				http.Error(w, err.Error(), 500)
			}
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
	if err := session.Query(`DELETE FROM blocks_id_cache WHERE key = ?`, key).Exec(); err != nil {
		log.Warnf("error removing id cache, key: %s, error: %s", key, err.Error())
	}

	if err := session.Query(`DELETE FROM blocks WHERE key = ?`, key).Exec(); err != nil {
		log.Warnf("error removing, key: %s, error: %s", key, err.Error())
		return err
	}

	return nil
}

func WriteObject(blockSize int, key string, body io.Reader, session *gocql.Session) error {
	log.Infof("setting %s", key)
	buf := make([]byte, blockSize)
	id := int(0)
	ids := []int{}

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
			if err := session.Query(`INSERT INTO blocks (key, id, part, created_at) VALUES (?, ?, ?, ?)`, key, id, part, time.Now()).Exec(); err != nil {
				log.Warnf("error inserting, key: %s, block id: %d, error: %s", key, id, err.Error())
				if err := DeleteObject(key, session); err != nil {
					return err
				}
				return err
			}
			ids = append(ids, id)
			log.Infof("  key: %s creating block id: %d, size %d, took %d", key, id, len(part), time.Now().Sub(t0).Nanoseconds()/1e6)
		}
		if end {
			break
		}
	}

	if err := session.Query(`INSERT INTO blocks_id_cache (key, ids, created_at) VALUES (?, ?,?)`, key, ids, time.Now()).Exec(); err != nil {
		log.Warnf("error inserting id cache, key: %s, error: %s", key, err.Error())
		if err := DeleteObject(key, session); err != nil {
			return err
		}
		return err
	}
	return nil
}

type ChunkReader struct {
	blocks     []int
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
	id := c.blocks[c.blockIndex]
	if err := c.session.Query(`SELECT part FROM blocks WHERE key = ? AND id = ?`, c.key, id).Consistency(gocql.One).Scan(&c.part); err != nil {
		return err
	}
	log.Printf("  key: %s reading block %d, size: %d, took: %d", c.key, id, len(c.part), time.Now().Sub(t0).Nanoseconds()/1e6)
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

	var blocks []int
	if err := session.Query(`SELECT ids FROM blocks_id_cache WHERE key = ?`, key).Consistency(gocql.One).Scan(&blocks); err != nil {
		return nil, err
	}

	if len(blocks) == 0 {
		return nil, fmt.Errorf("NOTFOUND")
	}

	return &ChunkReader{blocks: blocks, key: key, session: session}, nil
}
