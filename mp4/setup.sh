#!/bin/bash

go mod init mp4

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

echo 'export PATH="$PATH:$(go env GOPATH)/bin"' >> ~/.bashrc
source ~/.bashrc

which protoc-gen-go
which protoc-gen-go-grpc
protoc --version
protoc-gen-go --version
protoc-gen-go-grpc --version

echo "Setup complete."
