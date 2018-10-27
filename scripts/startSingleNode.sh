#!/usr/bin/env bash

export GO111MODULE=on
go run . --id 1 --cluster http://127.0.0.1:12379 --port 12380