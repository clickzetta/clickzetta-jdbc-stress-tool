#!/usr/bin/env bash

set -e

# compile jar
mvn package

# build & push image
docker buildx build . --platform linux/amd64,linux/arm64 --push -t clickzetta/jdbc-stress-tool:dev
