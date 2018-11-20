# asfalis

asfalís is a distributed in memory key-value store written in go. It uses gRPC to communicate between nodes, has a simple RESTFUL set/get interface and implements the RAFT algorithm to achieve consensus.

Inspiration is broadly based upon [etcd](https://github.com/etcd-io/etcd). 

# Requirements 
- Go 1.11.1


## Running

### Single Node
Start The Node:
```
export GO111MODULE=on
go run . --id 1 --cluster http://127.0.0.1:12379 --port 12380
```
Set A Key:
```
curl -L http://127.0.0.1:12380/my-key -XPUT -d hello
```
Retrieve the stored key:

```
curl -L http://127.0.0.1:12380/my-key
```


### Multi Node
```
export GO111MODULE=on
go run . --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
go run . --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
go run . --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380
```
Set A Key:
```
curl -L http://127.0.0.1:12380/my-key -XPUT -d hello
```
Retrieve the stored key from a different node. (Consistency model is eventual so you might need to retry).

```
curl -L http://127.0.0.1:22380/my-key
```
