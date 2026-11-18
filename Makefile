.PHONY: check build test vet fmt

# Pure Go everywhere (modernc.org/sqlite needs no cgo); the image is static.
export CGO_ENABLED = 0

check: fmt vet build test

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

fmt:
	@test -z "$$(gofmt -l . | grep -v '^vendor/')" || (gofmt -l . && exit 1)
