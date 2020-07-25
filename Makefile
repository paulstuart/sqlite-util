SHELL := /bin/bash

export PATH := $(PATH):$(GOROOT)/bin:$(GOPATH)/bin

GOFLAGS ?= $(GOFLAGS:)

path:
	@echo $$PATH

bench:
	@go test -run=^$$ -bench=. -benchmem 

build:
	@go build $(goflags) ./...

profile:
	@go test -coverprofile cover.out

html:
	@go tool cover -html cover.out

show:	profile html

cover:
	@go test -cover $(arg1)  $(goflags) ./...

escape:
	@go build -gcflags '-m' db.go lite.go table.go

test: 
	@go test $(GOFLAGS)

clean:
	@go clean $(GOFLAGS) -i ./...

.PHONY: all test clean build alpine install fresh cover profile html show escape

## EOF
