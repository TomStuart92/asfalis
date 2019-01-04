#!/usr/bin/env bash

# Build Binary
echo "Building"
go build .


# Start Server
echo "Starting Three Node Cluster"
./asfalis --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380 &
SERVER_PID_1=$!;
./asfalis --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380 &
SERVER_PID_2=$!;
./asfalis --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380 &
SERVER_PID_3=$!;

sleep 5

# Run Tests
echo "Running Tests"
go clean -testcache
go test ./test/e2e
sleep 1

# Teardown
echo "Tearing Down"
kill -9 $SERVER_PID_1
kill -9 $SERVER_PID_2
kill -9 $SERVER_PID_3

rm -rf raftexample-*
rm -rf raftexample-1-snap
rm -rf raftexample-2
rm -rf raftexample-2-snap
rm -rf raftexample-3
rm -rf raftexample-3-snap
echo "Done"
