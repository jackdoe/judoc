FROM golang:alpine

RUN apk update && apk add bash git

RUN go get -v github.com/jackdoe/judoc/cmd/judoc && go install -v github.com/jackdoe/judoc/cmd/judoc && rm -rf /go/src

CMD /go/bin/judoc -keyspace ${JUDOC_KEYSPACE:-baxx} -consistency ${JUDOC_CONSISTENCY:-TWO} -cluster ${JUDOC_CLUSTER:-127.0.0.1} -block-size ${JUDOC_BLOCK_SIZE:-4194304} -bind ${JUDOC_BIND:-:9122} $JUDOC_ARGS
