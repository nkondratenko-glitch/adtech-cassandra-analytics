#!/usr/bin/env bash
set -e
docker exec -i adtech-cassandra cqlsh < cassandra/schema.cql
