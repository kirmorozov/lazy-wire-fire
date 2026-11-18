# Builds and tests share the host with other projects: keep cargo to 4 jobs, unless
# CARGO_BUILD_JOBS in the environment asks for fewer (or more).
CARGO_BUILD_JOBS ?= 4
CARGO := CARGO_BUILD_JOBS=$(CARGO_BUILD_JOBS) cargo
COMPOSE := docker compose -f docker-compose.test.yml
IMAGE ?= lazywf:dev
# Keep in step with the FROM lines of the Dockerfile.
BASE_IMAGES := docker.io/library/rust:1.100.0-slim-bookworm gcr.io/distroless/static-debian12:nonroot

.PHONY: build release test test-roaring test-grpc lint servers servers-down test-e2e test-e2e-all test-e2e-search \
	test-e2e-queues test-e2e-pgsrc test-chaos test-chaos-all test-matrix test-prerelease coverage coverage-unit coverage-e2e coverage-report \
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
# `--bins` runs the binary's own unit tests (`src/main.rs`): `--lib` does not build them, so
# without that line nothing but the informational coverage job would ever run them. The `roaring`
# feature gates only the lib's key set (`src/intset.rs`) and nothing those tests reach, so
# `test-roaring` needs no twin of the line.
test:
	$(CARGO) test --lib
	$(CARGO) test --bins
	$(CARGO) test --test reserved
	$(MAKE) --no-print-directory test-grpc
	$(MAKE) --no-print-directory test-roaring

test-roaring:
	$(CARGO) test --lib --features roaring
	$(CARGO) test --test reserved --features roaring

# The `grpc` kind end to end (tests/grpc_stream.rs): the client the tool ships against a
# hand-rolled h2 server of lazywf's own, in the same process on 127.0.0.1. No servers, no
# containers, no environment variables: it runs with the unit tests, locally and on CI.
test-grpc:
	$(CARGO) test --test grpc_stream

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
	  --profile kafka --profile eventbridge --profile sns --profile pgsrc down -v

# The integration servers and test threads; override them to run against other servers.
E2E_DSN ?= mysql://root:root@127.0.0.1:34184
E2E_DSN_MARIADB ?= mysql://root:root@127.0.0.1:34111
E2E_THREADS ?= 4
# The target services of the default profile a run talks to besides the server under test (the same
# set tmp/e2e-shared.sh calls TARGETS, which runs this suite per run from a worker's own binary).
E2E_TARGETS := redis valkey postgres s3

# Owner rule (2026-11-16, and tmp/e2e-shared.sh): a test server is a composition for the run, not a
# container that stays up for days. Every target below brings up exactly the services its run talks
# to and removes them again with `rm -sfv` per service, so a run leaves no container and no volume
# behind. Never `down`: `down` deletes the project network, and with `-v` the named volumes, so the
# next run has to create both again instead of finding them as this file declares them — dropping
# them is `make servers-down`'s job, where an operator asks for it. Nothing hangs on the network
# either: lazywf reaches every service through its published 127.0.0.1 port, and lazyview's toxiproxy
# (lvs-toxiproxy) is on docker's own bridge, not on lazywf-test_default (checked on the host: that
# network has no containers, and no test resolves a service name).
# These targets own the compose project `lazywf-test` for the length of a run, so on this host they
# run ONE AT A TIME: two `make test-e2e` runs side by side tear down each other's servers. Take the
# same heavy-suite lock tmp/e2e-shared.sh takes (lazyview's tmp/out/heavy-suite.lock) around a run.
# The removal sits in a trap on EXIT, with INT and TERM made to exit through it, so a run that fails
# or is killed cleans up too. KEEP_SERVERS=1 leaves the services to the caller: CI pre-starts its
# services, reads `docker compose logs` after a failed run, and tears the set down itself.
#
# Integration tests against MySQL 8.4 (GTIDs on) and the target services.
test-e2e:
	@set -e; \
	trap '[ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) rm -sfv mysql84 $(E2E_TARGETS) >/dev/null 2>&1 || true' EXIT; \
	trap 'exit 1' INT TERM; \
	$(COMPOSE) up -d --wait mysql84 $(E2E_TARGETS); \
	LAZYWF_TEST_DSN=$(E2E_DSN) $(CARGO) test --test e2e -- --test-threads=$(E2E_THREADS)

# The same tests on both servers of the default set (MySQL 8.4, MariaDB 10.11). Self-contained rather
# than `test-e2e-all: test-e2e`: both runs need the same target services, and the first one's
# teardown would take them away from the second. The MySQL run goes first and a failure of it stops
# the MariaDB run, as before.
test-e2e-all:
	@set -e; \
	trap '[ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) rm -sfv mysql84 mariadb1011 $(E2E_TARGETS) >/dev/null 2>&1 || true' EXIT; \
	trap 'exit 1' INT TERM; \
	$(COMPOSE) up -d --wait mysql84 mariadb1011 $(E2E_TARGETS); \
	LAZYWF_TEST_DSN=$(E2E_DSN) $(CARGO) test --test e2e -- --test-threads=$(E2E_THREADS); \
	LAZYWF_TEST_DSN=$(E2E_DSN_MARIADB) $(CARGO) test --test e2e -- --test-threads=$(E2E_THREADS)

# The chaos seeds (tests/chaos.rs): the engine as a child, in-process fault proxies between it
# and the servers, faults gated on observed events. Children plus proxies: two test threads.
# On the shared servers of this host run it through the shared runner (BIN=<chaos binary>
# tmp/e2e-shared.sh), which holds the heavy-suite lock and cleans the lwf_* schemas.
test-chaos:
	LAZYWF_TEST_DSN=$(E2E_DSN) $(CARGO) test --test chaos -- --test-threads=2

# The same on both servers of the default set.
test-chaos-all: test-chaos
	LAZYWF_TEST_DSN=$(E2E_DSN_MARIADB) $(CARGO) test --test chaos -- --test-threads=2

# The search re-sync against OpenSearch 2.19, Elasticsearch 7.17 and 8.17 (compose profile
# `search`), one engine at a time: three JVMs at once do not fit on a shared host. MySQL 8.4 comes up
# with the first engine (the re-sync reads its tables) and every engine goes away as soon as its own
# run is over, so only one JVM is up at a time still; the trap covers a run that dies halfway.
E2E_DB := LAZYWF_TEST_DSN=$(E2E_DSN)
test-e2e-search:
	@set -e; \
	trap '[ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) --profile search rm -sfv mysql84 opensearch es7 es8 >/dev/null 2>&1 || true' EXIT; \
	trap 'exit 1' INT TERM; \
	$(COMPOSE) --profile search up -d --wait mysql84 opensearch; \
	$(E2E_DB) LAZYWF_TEST_OPENSEARCH=http://127.0.0.1:34200 $(CARGO) test --test e2e search_opensearch; \
	$(COMPOSE) --profile search rm -sfv opensearch; \
	$(COMPOSE) --profile search up -d --wait es7; \
	$(E2E_DB) LAZYWF_TEST_ES7=http://127.0.0.1:34207 $(CARGO) test --test e2e search_elasticsearch7; \
	$(COMPOSE) --profile search rm -sfv es7; \
	$(COMPOSE) --profile search up -d --wait es8; \
	mkdir -p tmp/out && docker cp lazywf-test-es8-1:/usr/share/elasticsearch/config/certs/http_ca.crt tmp/out/es8-ca.crt; \
	$(E2E_DB) LAZYWF_TEST_ES8=https://127.0.0.1:34202 LAZYWF_TEST_ES8_CA=$(CURDIR)/tmp/out/es8-ca.crt $(CARGO) test --test e2e search_elasticsearch8

# The queue kinds against their emulators (sqs: ElasticMQ, kinesis: kinesis-mock, amqp:
# RabbitMQ, pubsub: Google's emulator, smtp: Mailpit, kafka: a Redpanda single node, eventbridge
# and sns: moto; compose profiles of the same names). The seven services are small enough to run
# together on the host, with MySQL 8.4 and the S3 and Redis targets the e2e suite and the
# log_secrets binary need next to them (they used to be left to a standing `make servers`); the
# tests are run per kind with their env vars, and the services are removed even when a test fails.
# The kafka tests run one at a time (the restart one stops the shared broker). The log_secrets
# binary runs against the emulators too (the same vars; its lambda flow needs none of them).
QUEUE_PROFILES := --profile sqs --profile kinesis --profile amqp --profile pubsub --profile smtp \
  --profile kafka --profile eventbridge --profile sns
QUEUE_SERVICES := elasticmq kinesis-mock rabbitmq pubsub-emulator mailpit kafka moto
QUEUE_SERVERS := mysql84 s3 redis $(QUEUE_SERVICES)
test-e2e-queues:
	@set -e; rc=0; \
	trap '[ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) $(QUEUE_PROFILES) rm -sfv $(QUEUE_SERVERS) >/dev/null 2>&1 || true' EXIT; \
	trap 'exit 1' INT TERM; \
	$(COMPOSE) $(QUEUE_PROFILES) up -d --wait $(QUEUE_SERVERS); \
	  $(E2E_DB) LAZYWF_TEST_SQS=http://127.0.0.1:34561 $(CARGO) test --test e2e sqs_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_KINESIS=http://127.0.0.1:34562 $(CARGO) test --test e2e kinesis_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_AMQP=127.0.0.1:34560 $(CARGO) test --test e2e amqp_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_SERVICEBUS=127.0.0.1:34610 $(CARGO) test --test e2e servicebus_ -- --test-threads=1 || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_RABBITMQ_STREAM=127.0.0.1:34572 LAZYWF_TEST_AMQP=127.0.0.1:34560 $(CARGO) test --test e2e rabbitmq_stream_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_PUBSUB=http://127.0.0.1:34563 $(CARGO) test --test e2e pubsub_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_SMTP=127.0.0.1:34565 $(CARGO) test --test e2e smtp_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_KAFKA=127.0.0.1:34564 $(CARGO) test --test e2e kafka_ -- --test-threads=1 || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_EVENTBRIDGE=http://127.0.0.1:34569 $(CARGO) test --test e2e eventbridge_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_SNS=http://127.0.0.1:34569 $(CARGO) test --test e2e sns_ || rc=1; \
	  $(E2E_DB) LAZYWF_TEST_SQS=http://127.0.0.1:34561 LAZYWF_TEST_KINESIS=http://127.0.0.1:34562 \
	    LAZYWF_TEST_AMQP=127.0.0.1:34560 LAZYWF_TEST_PUBSUB=http://127.0.0.1:34563 \
	    LAZYWF_TEST_RABBITMQ_STREAM=127.0.0.1:34572 \
	    LAZYWF_TEST_SMTP=127.0.0.1:34565 LAZYWF_TEST_EVENTBRIDGE=http://127.0.0.1:34569 \
	    LAZYWF_TEST_SNS=http://127.0.0.1:34569 \
	    LAZYWF_TEST_KAFKA=127.0.0.1:34564 LAZYWF_TEST_KAFKA_ADMIN=http://127.0.0.1:34567 \
	    LAZYWF_TEST_SERVICEBUS=127.0.0.1:34610 \
	    $(CARGO) test --test log_secrets -- --test-threads=$(E2E_THREADS) || rc=1; \
	exit $$rc

# The pgoutput source tests against the `pgsrc` compose service (a PostgreSQL with
# wal_level=logical, the same image as the sink). Opt-in like the queue kinds: the engine cannot
# read a PG source yet (issue #57 — lazyview leads the integration and lazywf copies the reader),
# so this joins neither test-e2e, test-e2e-all nor the default matrix until the engine PR lands;
# CI runs it as the advisory `postgres-src` job. The tests gate on LAZYWF_TEST_PG (absent: they
# skip) and drive the publisher through SQL only — the replication-protocol client is not here.
E2E_PG_DSN ?= host=127.0.0.1 port=34532 user=lwfsrc password=lwfsrcpw dbname=lwfsrc
test-e2e-pgsrc:
	@set -e; \
	trap '[ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) --profile pgsrc rm -sfv pgsrc >/dev/null 2>&1 || true' EXIT; \
	trap 'exit 1' INT TERM; \
	$(COMPOSE) --profile pgsrc up -d --wait pgsrc; \
	LAZYWF_TEST_PG=1 LAZYWF_TEST_PG_DSN="$(E2E_PG_DSN)" $(CARGO) test --test e2e pg_ -- --test-threads=$(E2E_THREADS)

# The integration tests on every other required line (README "Supported servers"), one server
# at a time (service:port; the target services of `make servers` come up with it). MariaDB 10.6 goes
# after 2026-12-31, when RDS for MariaDB 10.6 standard support ends.
# A server that fails its run is removed too: the test line's own `||` does it before exiting, and
# the trap covers a failure of the `up` itself (it removes every server of $(MATRIX), which compose
# answers with an empty removal for the ones already gone).
MATRIX := mysql80:34180 mysql97:34197 mysql2610:34210 percona84:34284 percona97:34297 \
	mariadb106:34106 mariadb114:34114 mariadb118:34118 mariadb123:34123 mariadb130:34130
# The same list as service names, for the trap (derived, so overriding MATRIX cannot drift them).
MATRIX_SERVICES := $(foreach sp,$(MATRIX),$(firstword $(subst :, ,$(sp))))
test-matrix:
	@set -e; \
	trap '[ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) --profile matrix --profile prerelease rm -sfv $(MATRIX_SERVICES) $(E2E_TARGETS) >/dev/null 2>&1 || true' EXIT; \
	trap 'exit 1' INT TERM; \
	$(COMPOSE) up -d --wait $(E2E_TARGETS); \
	for sp in $(MATRIX); do s=$${sp%%:*}; p=$${sp##*:}; \
	  echo "=== $$s"; $(COMPOSE) --profile matrix --profile prerelease up -d --wait $$s; \
	  LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:$$p $(CARGO) test --test e2e -- --test-threads=4 || { [ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) --profile matrix --profile prerelease rm -sfv $$s; exit 1; }; \
	  [ -n "$(KEEP_SERVERS)" ] || $(COMPOSE) --profile matrix --profile prerelease rm -sfv $$s; done

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
# An explicit CARGO_BUILD_JOBS still wins over the 2, from the environment or the command line.
COVERAGE_JOBS := $(if $(filter environment command line,$(origin CARGO_BUILD_JOBS)),$(CARGO_BUILD_JOBS),2)
coverage-unit coverage-e2e coverage-report: CARGO := CARGO_BUILD_JOBS=$(COVERAGE_JOBS) cargo

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
