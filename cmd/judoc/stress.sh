#!/bin/bash
for i in `seq 1 6000`; do
    dd if=/dev/urandom of=/tmp/stress.$i bs=$(echo $RANDOM)K count=$i
    curl -T /tmp/stress.$i http://localhost:9122/io/xyz/abc
    curl http://localhost:9122/io/xyz/abc > /tmp/stress.$i.dl
    diff /tmp/stress.$i /tmp/stress.$i.dl
    rc=$?
    if [ $rc = 0 ]; then
        echo /tmp/stress.$i /tmp/stress.$i.dl are the same
    else
        echo /tmp/stress.$i /tmp/stress.$i.dl panic
        exit 1
    fi
done