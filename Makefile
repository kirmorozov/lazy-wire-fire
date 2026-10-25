# Builds and tests share the host with other projects: keep cargo to 4 jobs.
CARGO := CARGO_BUILD_JOBS=4 cargo
COMPOSE := docker compose -f docker-compose.test.yml

.PHONY: build release test lint servers servers-down test-e2e test-e2e-all

build:
	$(CARGO) build

release:
	$(CARGO) build --release

test:
	$(CARGO) test --lib

lint:
	$(CARGO) fmt --check
	$(CARGO) clippy --all-targets -- -D warnings

servers:
	$(COMPOSE) up -d --wait

servers-down:
	$(COMPOSE) down -v

# Integration tests against MySQL 8.4 (GTIDs on).
test-e2e:
	LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:34184 $(CARGO) test --test e2e -- --test-threads=4

# The same tests on every server of the compose file.
test-e2e-all: test-e2e
	LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:34180 $(CARGO) test --test e2e -- --test-threads=4
	LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:34111 $(CARGO) test --test e2e -- --test-threads=4
