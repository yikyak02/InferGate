GO ?= $(if $(wildcard .tools/go/bin/go),$(CURDIR)/.tools/go/bin/go,go)
KUBECTL ?= kubectl
export GOCACHE := $(CURDIR)/.cache/go-build
export GOMODCACHE := $(CURDIR)/.cache/go-mod
export GOTOOLCHAIN := local

.PHONY: build test check smoke demo benchmark clean k8s-render k8s-apply k8s-smoke
build:
	@mkdir -p bin
	$(GO) build -o bin/gateway ./cmd/gateway
	$(GO) build -o bin/simulator ./cmd/simulator
	$(GO) build -o bin/loadgen ./cmd/loadgen
	$(GO) build -o bin/experiment ./cmd/experiment
test:
	$(GO) test -race -timeout=45s -count=1 ./...
check:
	$(GO) vet ./...
	@test -z "$$($(GO) fmt ./...)" || (echo 'Go files needed formatting'; exit 1)
demo: build
	sh scripts/demo.sh
smoke: build
	python3 scripts/smoke.py
benchmark: build
	./bin/experiment -output results/comparison
k8s-render:
	@$(KUBECTL) kustomize .
k8s-apply:
	$(KUBECTL) apply -k .
k8s-smoke:
	python3 scripts/kubernetes_smoke.py --kubectl "$(KUBECTL)"
clean:
	rm -rf bin .cache
