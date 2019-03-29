# judoc - simple put/get/delete blob store
http wrapper on top of cassandra
chunks the blobs in 4mb chunks
(4mb is too much for cassandra but ok for scylla)

# run

$ sudo docker run -p 9042:9042 scylladb/scylla
$ sudo docker exec -t -i $( sudo docker ps | grep scylla | awk '{ print $1 }') cqlsh

CREATE KEYSPACE "baxx"  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};
CREATE TABLE baxx.blocks (key ascii, id int, part blob, created_at timestamp, PRIMARY KEY ((key), id));
CREATE TABLE baxx.blocks_id_cache (key ascii, ids list<int>, created_at timestamp, PRIMARY KEY (key));

$ go run main.go

# query

$ curl -T file http://localhost:9122/set/example
OK
$ curl http://localhost:9122/get/example > example.dl
$ curl http://localhost:9122/delete/example
OK
$ curl http://localhost:9122/get/example
NOTFOUND

