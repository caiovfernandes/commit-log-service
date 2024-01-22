# commit-log-service
A simple JSON/HTTP commit log service that accepts and responds with JSON and stores the records in those requests to an in- memory log commit log service developed in Golang.


# Usage
1. `go run cmd/server/main.go`


# Request snnipets
## Producing Logs:
- `curl -X POST localhost:8080 -d \
    '{"record": {"value": "TGV0J3MgR28gIzEK"}}'`
- `curl -X POST localhost:8080 -d \
    '{"record": {"value": "TGV0J3MgR28gIzIK"}}'`
- `curl -X POST localhost:8080 -d \
    '{"record": {"value": "TGV0J3MgR28gIzmK"}}'`

## Consuming Logs:
- `curl -X GET localhost:8080 -d '{"offset": 0}'`
- `curl -X GET localhost:8080 -d '{"offset": 1}'`
- `curl -X GET localhost:8080 -d '{"offset": 2}'`


## Compiling protobuf schema into go struct
- `protoc api/v1/*.proto --go_out=. --go_opt=path=source_relative --proto_path=.`


## Requirements
- protobuf (binary)
- protobuf (go lib)
- protoc-gen-go-grpc
- protoc-gen-go
