# judoc - simple put/get/delete blob store
http wrapper on top of cassandra
chunks the blobs in 4mb chunks
(4mb is too much for cassandra but ok for scylla)

# run

$ sudo docker run -p 9042:9042 scylladb/scylla
$ sudo docker exec -t -i $( sudo docker ps | grep scylla | awk '{ print $1 }') cqlsh

CREATE TABLE baxx.blocks (id timeuuid, part blob, PRIMARY KEY(id));
CREATE TABLE baxx.files (key ascii, namespace ascii, blocks list<uuid>, modified_at timestamp, PRIMARY KEY (key, namespace));
CREATE INDEX ON baxx.files (namespace)

$ go run main.go

# query

$ curl -T file http://localhost:9122/io/namespace/example
OK
$ curl http://localhost:9122/io/namespace/example > example.dl
$ curl -XDELETE http://localhost:9122/io/namespace/example
OK
$ curl http://localhost:9122/io/namespace/example
NOTFOUND


