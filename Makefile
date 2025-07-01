
TESTA   := ${shell go list ./... | grep -v /cmd/ }

all: check

check: gen lint test

cover:
	go test -coverprofile=cover.out ${TESTA} && \
	go tool cover -func=cover.out

gen:
	go generate ./...

lint:
	golangci-lint run ./...

test:
	go test -count 1 ${TESTA}

.PHONY: all check cover gen lint test

