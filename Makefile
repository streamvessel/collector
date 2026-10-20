# sv-log-agent build. The Go toolchain and every collector component are
# pinned, so `make generate` and `make build` are reproducible.

GO_VERSION   ?= 1.26.10
OCB_VERSION  ?= 0.163.0
export GOTOOLCHAIN = go$(GO_VERSION)
export CGO_ENABLED = 0

GOBIN        := $(shell go env GOPATH)/bin
OCB          := $(GOBIN)/builder
DIST         := cmd/sv-log-agent
LDFLAGS      := -s -w -buildid=

.PHONY: all ocb generate check-generated build test lint e2e clean

all: lint test build

ocb:
	@$(OCB) version 2>/dev/null | grep -q "v$(OCB_VERSION)$$" || \
		go install go.opentelemetry.io/collector/cmd/builder@v$(OCB_VERSION)

# Regenerate cmd/sv-log-agent from builder-config.yaml.
generate: ocb
	$(OCB) --config builder-config.yaml --skip-compilation

# Fails when the committed distribution differs from what ocb generates.
check-generated: generate
	git diff --exit-code -- $(DIST)

build:
	cd $(DIST) && go build -trimpath -ldflags "$(LDFLAGS)" -o ../../bin/sv-log-agent .

test:
	cd exporter/svexporter && go test -race -count=1 ./...
	cd test/e2e/fakesvbe && go vet ./...

lint:
	cd exporter/svexporter && go vet ./... && test -z "$$(gofmt -l .)"
	cd test/e2e/fakesvbe && test -z "$$(gofmt -l .)"

# Docker Compose stack: kill-mid-batch and sv-be outage, nothing lost.
e2e:
	test/e2e/run.sh

clean:
	rm -rf bin dist
