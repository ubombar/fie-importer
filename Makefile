.PHONY: build lint fmt tidy test cover clean help

help:
	@echo "Valid targets:"
	@echo "  build       - Format, lint, generate docs, and build fie-importer binary"
	@echo "  lint        - Format code and run linters"
	@echo "  fmt         - Format code"
	@echo "  tidy        - Tidy go modules"
	@echo "  test        - Run tests with race detection and generate coverage profile"
	@echo "  cover       - View test coverage in browser"
	@echo "  clean       - Remove built binaries and coverage files"

build: lint
	go build -o fie-importer ./cmd/main.go

lint: fmt
	golangci-lint run

fmt:
	go fmt ./...

tidy:
	go mod tidy

test:
	go test -v -race -coverprofile=coverage.out ./...

cover:
	go tool cover -html=coverage.out

clean:
	rm -f fie-importer coverage.out

