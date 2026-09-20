.PHONY: check fmt fmt-check vet test build integration integration-auth integration-pgboss integration-interop interop-up interop-build pgboss-deps lab-up lab-auth-up lab-down lab-certs bench-build fetch-pgque
.PHONY: container-build integration-container
.PHONY: lab-quorum-up lab-quorum-down integration-quorum

VERSION ?= $(shell cat VERSION)

check: fmt-check vet test build

fmt:
	gofmt -w cmd internal

fmt-check:
	@files="$$(gofmt -l cmd internal)" || exit $$?; \
	if [ -n "$$files" ]; then printf 'Run make fmt:\n%s\n' "$$files"; exit 1; fi

vet:
	go vet ./...

test:
	go test -race ./...

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-X main.version=$(VERSION)" -o bin/row-relay ./cmd/row-relay

container-build:
	docker build --build-arg VERSION=$(VERSION) --tag rowrelay:local .

integration-container: container-build
	go test -race -tags=integration,container ./internal/relay -run TestPackagedRelay -count=1 -timeout=2m

bench-build: build
	go build -trimpath -o bin/row-bench ./cmd/row-bench

integration: build
	go test -race -tags=integration ./internal/relay -count=1 -timeout=5m

lab-certs:
	python3 scripts/lab-certs.py

lab-up:
	docker compose -f local/compose.yml up -d --wait

lab-auth-up: lab-certs
	docker compose -f local/compose.yml -f local/auth.compose.yml up -d --wait

integration-auth:
	go test -race -tags=integration,kafkaauth ./internal/relay -run TestSASLTLSAndTransactionAuthorization -count=1 -timeout=1m

lab-quorum-up:
	docker compose -f local/compose.yml -f local/quorum.compose.yml up -d --wait

integration-quorum:
	go test -race -tags=integration,quorum ./internal/relay -run TestKafkaQuorumLossPreservesSourceAndRecovers -count=1 -timeout=4m

lab-quorum-down:
	docker compose -f local/compose.yml -f local/quorum.compose.yml stop kafka2 kafka3
	docker compose -f local/compose.yml up -d --wait

pgboss-deps:
	npm ci --prefix benchmarks/pgboss --ignore-scripts --no-audit --no-fund
	docker pull "$$(cat benchmarks/pgboss/node-image.txt)"

integration-pgboss:
	go test -race -tags=integration ./cmd/row-bench -count=1 -timeout=2m

interop-up:
	docker compose -f local/compose.yml -f local/interop.compose.yml up -d registry

interop-build:
	MAVEN_OPTS=-Xmx512m mvn -B -ntp -f interop/jvm/pom.xml compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt

integration-interop:
	go test -race -tags=integration,protobuf ./internal/relay -run TestPreparedProtobufWithRealRegistryAndJVMConsumer -count=1 -timeout=4m

lab-down:
	docker compose -f local/compose.yml -f local/interop.compose.yml -f local/quorum.compose.yml stop

fetch-pgque:
	sh scripts/fetch-pgque.sh
