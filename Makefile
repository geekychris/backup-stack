.PHONY: check build test vet fmt

check: fmt vet build test

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

fmt:
	@test -z "$$(gofmt -l . | grep -v '^vendor/')" || (gofmt -l . && exit 1)
