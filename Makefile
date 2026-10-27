BINARY      := bin/edge-gateway
PKG         := ./...
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2

.PHONY: all build test race bench lint vet staticcheck run clean

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/edge-gateway

test:
	go test -count=1 $(PKG)

race:
	go test -race -count=1 $(PKG)

bench:
	go test -run='^$$' -bench=. -benchmem $(PKG)

vet:
	go vet $(PKG)

staticcheck:
	go run $(STATICCHECK) $(PKG)

lint: vet staticcheck

run: build
	$(BINARY) -config configs/example.yaml

clean:
	rm -rf bin
