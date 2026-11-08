# Builds and tests share the host with other projects: keep cargo to 4 jobs.
CARGO := CARGO_BUILD_JOBS=4 cargo
COMPOSE := docker compose -f docker-compose.test.yml
IMAGE ?= lazywf:dev
# Keep in step with the FROM lines of the Dockerfile.
BASE_IMAGES := docker.io/library/rust:1.99.1-slim-bookworm gcr.io/distroless/static-debian12:nonroot

.PHONY: build release test test-roaring lint servers servers-down test-e2e test-e2e-all test-e2e-search \
	test-e2e-queues test-matrix test-prerelease coverage coverage-unit coverage-e2e coverage-report \
	docker-smoke docker-e2e image-digests

build:
	$(CARGO) build

release:
	$(CARGO) build --release

# The integer key sets have two backends: the default hash set and roaring bitmaps behind the
# `roaring` feature. The unit tests and the lint run for both; the integration tests run on the
# default.
# The reserved-tables test is a unit test in its own process: set_reserved takes effect once per
# process, and the planner's own unit tests take that call in the lib test binary.
test:
	$(CARGO) test --lib
	$(CARGO) test --test reserved
	$(MAKE) --no-print-directory test-roaring

test-roaring:
	$(CARGO) test --lib --features roaring
	$(CARGO) test --test reserved --features roaring

# The lazyview copies (vendor/lazyview.lock) are checked against the lock, and on CI
# (CI=true) also against lazyview itself at the locked commit.
lint:
	$(CARGO) fmt --check
	$(CARGO) clippy --all-targets -- -D warnings
	$(CARGO) clippy --all-targets --features roaring -- -D warnings
	scripts/sync-lazyview.sh --check $(if $(CI),--upstream)

servers:
	$(COMPOSE) up -d --wait

servers-down:
	$(COMPOSE) --profile search --profile varnish --profile matrix --profile prerelease \
	  --profile sqs --profile kinesis --profile amqp --profile pubsub --profile smtp \
	  --profile kafka --profile eventbridge down -v

# The integration servers and test threads; override them to run against other servers.
E2E_DSN ?= mysql://root:root@127.0.0.1:34184
E2E_DSN_MARIADB ?= mysql://root:root@127.0.0.1:34111
E2E_THREADS ?= 4

# Integration tests against MySQL 8.4 (GTIDs on).
test-e2e:
	LAZYWF_TEST_DSN=$(E2E_DSN) $(CARGO) test --test e2e -- --test-threads=$(E2E_THREADS)

# The same tests on both servers of the default set (MySQL 8.4, MariaDB 10.11).
test-e2e-all: test-e2e
	LAZYWF_TEST_DSN=$(E2E_DSN_MARIADB) $(CARGO) test --test e2e -- --test-threads=$(E2E_THREADS)

# The search re-sync against OpenSearch 2.19, Elasticsearch 7.17 and 8.17 (compose profile
# `search`), one engine at a time: three JVMs at once do not fit on a shared host.
E2E_DB := LAZYWF_TEST_DSN=$(E2E_DSN)
test-e2e-search:
	$(COMPOSE) --profile search up -d --wait opensearch
	$(E2E_DB) LAZYWF_TEST_OPENSEARCH=http://127.0.0.1:34200 $(CARGO) test --test e2e search_opensearch
	$(COMPOSE) --profile search stop opensearch
	$(COMPOSE) --profile search up -d --wait es7
	$(E2E_DB) LAZYWF_TEST_ES7=http://127.0.0.1:34207 $(CARGO) test --test e2e search_elasticsearch7
	$(COMPOSE) --profile search stop es7
	$(COMPOSE) --profile search up -d --wait es8
	mkdir -p tmp/out && docker cp lazywf-test-es8-1:/usr/share/elasticsearch/config/certs/http_ca.crt tmp/out/es8-ca.crt
	$(E2E_DB) LAZYWF_TEST_ES8=https://127.0.0.1:34202 LAZYWF_TEST_ES8_CA=$(CURDIR)/tmp/out/es8-ca.crt $(CARGO) test --test e2e search_elasticsearch8
	$(COMPOSE) --profile search stop es8

# The queue kinds against their emulators (sqs: ElasticMQ, kinesis: kinesis-mock, amqp:
# RabbitMQ, pubsub: Google's emulator, smtp: Mailpit, kafka: a Redpanda single node, eventbridge:
# moto; compose profiles of the same names). The seven are small enough to run together on the
# host; the tests are run per kind with their env vars, and the services are stopped even when a
# test fails. The kafka tests run one at a time (the restart one stops the shared broker). The
# log_secrets binary runs against the emulators too (the same seven vars; its lambda flow needs
# none of them); its other flows need the default `make servers` set (MySQL, S3 and Redis among
# it).
QUEUE_PROFILES := --profile sqs --profile kinesis --profile amqp --profile pubsub --profile smtp \
  --profile kafka --profile eventbridge
QUEUE_SERVICES := elasticmq kinesis-mock rabbitmq pubsub-emulator mailpit kafka moto
test-e2e-queues:
	$(COMPOSE) $(QUEUE_PROFILES) up -d --wait $(QUEUE_SERVICES); rc=$$?; \
	if [ $$rc -eq 0 ]; then \
	  $(E2E_DB) LAZYWF_TEST_SQS=http://127.0.0.1:34561 $(CARGO) test --test e2e sqs_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_KINESIS=http://127.0.0.1:34562 $(CARGO) test --test e2e kinesis_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_AMQP=127.0.0.1:34560 $(CARGO) test --test e2e amqp_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_PUBSUB=http://127.0.0.1:34563 $(CARGO) test --test e2e pubsub_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_SMTP=127.0.0.1:34565 $(CARGO) test --test e2e smtp_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_KAFKA=127.0.0.1:34564 $(CARGO) test --test e2e kafka_ -- --test-threads=1 || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_EVENTBRIDGE=http://127.0.0.1:34569 $(CARGO) test --test e2e eventbridge_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_SQS=http://127.0.0.1:34561 LAZYWF_TEST_KINESIS=http://127.0.0.1:34562 \
	    LAZYWF_TEST_AMQP=127.0.0.1:34560 LAZYWF_TEST_PUBSUB=http://127.0.0.1:34563 \
	    LAZYWF_TEST_SMTP=127.0.0.1:34565 LAZYWF_TEST_EVENTBRIDGE=http://127.0.0.1:34569 \
	    LAZYWF_TEST_KAFKA=127.0.0.1:34564 LAZYWF_TEST_KAFKA_ADMIN=http://127.0.0.1:34567 \
	    $(CARGO) test --test log_secrets -- --test-threads=$(E2E_THREADS) || rc=1; \
	fi; \
	$(COMPOSE) $(QUEUE_PROFILES) stop $(QUEUE_SERVICES); exit $$rc

# The integration tests on every other required line (README "Supported servers"), one server
# at a time (service:port; the targets of `make servers` must be up). MariaDB 10.6 goes after
# 2026-12-31, when RDS for MariaDB 10.6 standard support ends.
MATRIX := mysql80:34180 mysql97:34197 mysql2610:34210 percona84:34284 percona97:34297 \
	mariadb106:34106 mariadb114:34114 mariadb118:34118 mariadb123:34123 mariadb130:34130
test-matrix:
	@set -e; for sp in $(MATRIX); do s=$${sp%%:*}; p=$${sp##*:}; \
	  echo "=== $$s"; $(COMPOSE) --profile matrix --profile prerelease up -d --wait $$s; \
	  LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:$$p $(CARGO) test --test e2e -- --test-threads=4 || { $(COMPOSE) --profile matrix --profile prerelease stop $$s; exit 1; }; \
	  $(COMPOSE) --profile matrix --profile prerelease stop $$s; done

# The advisory pre-releases (MariaDB RC and preview): the same, never required.
PRERELEASE := mariadb131-rc:34131 mariadb132-pre:34132
test-prerelease:
	@$(MAKE) --no-print-directory test-matrix MATRIX="$(PRERELEASE)"

# Test coverage, informational (not a gate): the unit tests and the e2e suite (against $(E2E_DSN),
# so `make servers` up) under one instrumented build with cargo-llvm-cov, pinned to 0.9.1. The two
# --no-report runs accumulate their profile data; coverage-report turns it into one lcov file,
# one HTML report (tmp/coverage/) and the summary table (scripts/coverage-summary.py).
# CI installs the published 0.9.1 prebuilt binary (checksum verified); locally:
# cargo install cargo-llvm-cov --version 0.9.1. CARGO_PROFILE_*_DEBUG=0: an instrumented build
# with debug info does not fit a shared host; the coverage data is unaffected.
COV_ENV := CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
# Two jobs for the instrumented build: it peaks in memory where the default 4 does not fit a
# shared host (or a 16 GiB runner) with debug info; CARGO_PROFILE_*_DEBUG=0 is what lets it fit.
coverage-unit coverage-e2e coverage-report: CARGO := CARGO_BUILD_JOBS=2 cargo

coverage-unit:
	$(COV_ENV) $(CARGO) llvm-cov --no-report --lib --bins

coverage-e2e:
	$(E2E_DB) $(COV_ENV) $(CARGO) llvm-cov --no-report --test e2e -- --test-threads=$(E2E_THREADS)

coverage-report:
	@mkdir -p tmp/coverage
	$(COV_ENV) $(CARGO) llvm-cov report --lcov --output-path tmp/coverage/cov.lcov
	# (report --html writes into <dir>/html)
	$(COV_ENV) $(CARGO) llvm-cov report --html --output-dir tmp/coverage
	$(COV_ENV) $(CARGO) llvm-cov report --json --output-path tmp/coverage/cov.json
	@python3 scripts/coverage-summary.py tmp/coverage/cov.json vendor/lazyview.lock \
	  | tee tmp/coverage/summary.md

coverage:
	@v=$$(cargo llvm-cov --version 2>/dev/null | awk '{print $$2}'); \
	if [[ -z $$v ]]; then \
	  echo "cargo-llvm-cov not found: cargo install cargo-llvm-cov --version 0.9.1 (CI uses the 0.9.1 prebuilt)" >&2; exit 1; \
	fi; \
	if [[ $$v != 0.9.1 ]]; then echo "note: cargo-llvm-cov $$v found, CI pins the 0.9.1 prebuilt" >&2; fi
	$(MAKE) --no-print-directory coverage-unit coverage-e2e coverage-report

# A loaded image, checked without a shell inside it: version, exit 2 without configuration,
# user, entrypoint, health check, no shell. CI runs it on both platforms of every build.
docker-smoke: SHELL := bash
docker-smoke:
	@v=$$(docker run --rm "$(IMAGE)" --version); echo "version: $$v"; \
	  [[ $$v =~ ^lazywf\ [0-9]+\.[0-9]+\.[0-9]+ ]]
	@rc=0; docker run --rm "$(IMAGE)" >/dev/null 2>&1 || rc=$$?; \
	  if [[ $$rc -ne 2 ]]; then echo "expected exit 2 (configuration error) without env, got $$rc" >&2; exit 1; fi; \
	  echo "no-config exit code: $$rc (ok)"
	@cfg=$$(docker image inspect "$(IMAGE)" --format '{{.Config.User}} {{json .Config.Entrypoint}}'); echo "user/entrypoint: $$cfg"; \
	  [[ $$cfg == '65532:65532 ["/lazywf"]' ]]
	@hc=$$(docker image inspect "$(IMAGE)" --format '{{json .Config.Healthcheck.Test}}'); echo "healthcheck: $$hc"; \
	  [[ $$hc == '["CMD","/lazywf","healthcheck"]' ]]
	@for sh in /bin/sh /bin/bash /busybox/sh sh; do \
	  if docker run --rm --entrypoint "$$sh" "$(IMAGE)" -c true >/dev/null 2>&1; then echo "image has a shell: $$sh" >&2; exit 1; fi; \
	done; echo "no shell in the image (ok)"

# The image against the compose MySQL 8.4 and Redis: ready, healthy, uid 65532, a row fires a
# Redis SET, exit 0 on SIGTERM.
docker-e2e:
	scripts/docker-e2e.sh "$(IMAGE)"

# The current multi-arch index digests, to refresh the pins in the Dockerfile.
image-digests:
	@for ref in $(BASE_IMAGES); do \
	  printf '%-50s ' "$$ref"; docker buildx imagetools inspect "$$ref" --format '{{json .Manifest.Digest}}'; echo; \
	done
