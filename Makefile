# Builds and tests share the host with other projects: keep cargo to 4 jobs.
CARGO := CARGO_BUILD_JOBS=4 cargo
COMPOSE := docker compose -f docker-compose.test.yml

.PHONY: build release test lint servers servers-down test-e2e test-e2e-all test-e2e-search

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
	$(COMPOSE) --profile search down -v

# Integration tests against MySQL 8.4 (GTIDs on).
test-e2e:
	LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:34184 $(CARGO) test --test e2e -- --test-threads=4

# The same tests on every server of the compose file.
test-e2e-all: test-e2e
	LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:34180 $(CARGO) test --test e2e -- --test-threads=4
	LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:34111 $(CARGO) test --test e2e -- --test-threads=4

# The search re-sync against OpenSearch 2.19, Elasticsearch 7.17 and 8.17 (compose profile
# `search`), one engine at a time: three JVMs at once do not fit on a shared host.
E2E_DB := LAZYWF_TEST_DSN=mysql://root:root@127.0.0.1:34184
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
