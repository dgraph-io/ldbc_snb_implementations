#!/bin/sh

DIR=`dirname $0`

HOST="localhost"
if [ "x$1" != "x" ]; then
    HOST="$1"
fi

PORT="8080"
if [ "x$2" != "x" ]; then
    PORT="$2"
fi

grep -v "//" "$DIR/schema.graphql" | grep -v "^\s*$"  > /tmp/schema
curl $HOST:$PORT/query -XPOST --data-binary @/tmp/schema | python -m json.tool

