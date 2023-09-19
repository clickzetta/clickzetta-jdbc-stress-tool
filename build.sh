#!/usr/bin/env bash

set -e

# compile jar
mvn package

# build image
docker build . -t clickzetta/jdbc-stress-tool:dev
# to build x64_64 image on osx
# docker buildx build . --platform linux/amd64 -t clickzetta/jdbc-stress-tool:dev

