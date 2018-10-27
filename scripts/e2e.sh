#!/usr/bin/env bash

# Build Binary
echo "Building"
go build .


# Start Server
echo "Starting Single Node Server"
./asfalis --id 1 --cluster http://127.0.0.1:12379 --port 12380 &
SERVER_PID=$!;
echo $SERVER_PID
sleep 5

# Run Tests
echo "Running Tests"
go test ./test/e2e
sleep 1

# Teardown
echo "Tearing Down"
kill -9 $SERVER_PID
rm -rf raftexample-1
rm -rf raftexample-1-snap
echo "Done"
