#!/usr/bin/env bash
# The image end to end, run the way it runs in production: against the compose MySQL 8.4
# (GTIDs) and Redis of docker-compose.test.yml, on the host network. It must become ready and
# healthy as uid 65532, turn a committed row into a Redis SET and exit 0 on SIGTERM.
#
#   make docker-e2e IMAGE=lazywf:dev       (starts mysql84 and redis if they are not up)
set -euo pipefail

image=${1:?usage: docker-e2e.sh IMAGE}
compose=(docker compose -f docker-compose.test.yml)
name=lazywf-docker-e2e
schema=lwf_image
health=127.0.0.1:34881

sql() { "${compose[@]}" exec -T -e MYSQL_PWD=root mysql84 mysql -uroot -NB "$@"; }
redis() { "${compose[@]}" exec -T redis redis-cli -a redispw --no-auth-warning -n 7 "$@"; }
status() { curl -sf "http://${health}/status"; }

ready() { curl -sf "http://${health}/readyz" >/dev/null; }
healthy() { [[ $(docker inspect -f '{{.State.Health.Status}}' "$name") == healthy ]]; }
rule_loaded() { status | jq -e '.rules.docs' >/dev/null; }
fired() { [[ $(redis GET img_1) == *'"ann"'* ]]; }

wait_for() { # description, check
  for _ in $(seq 60); do
    if "$2"; then echo "ok: $1"; return 0; fi
    sleep 1
  done
  echo "timeout: $1" >&2
  return 1
}

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
on_exit() {
  local rc=$?
  if ((rc != 0)) && docker inspect "$name" >/dev/null 2>&1; then
    echo "--- ${name} log (last 100 lines)" >&2
    docker logs "$name" 2>&1 | tail -n 100 >&2 || true
  fi
  cleanup
}
trap on_exit EXIT

"${compose[@]}" up -d --wait mysql84 redis
sql -e "DROP DATABASE IF EXISTS ${schema}; CREATE DATABASE ${schema}"
redis DEL img_1 >/dev/null
cleanup

docker run -d --name "$name" --network host \
  -e LAZYWF_DSN="mysql://root:root@127.0.0.1:34184/${schema}" \
  -e LAZYWF_HEALTH_ADDR="$health" -e LAZYWF_STATUS_MS=500 -e LWF_REDIS_PW=redispw \
  --health-interval 2s --health-start-period 30s \
  "$image" >/dev/null

wait_for "/readyz" ready
wait_for "container healthy" healthy
# The process really runs as 65532 (not just the image config).
pid=$(docker inspect -f '{{.State.Pid}}' "$name")
if [[ -r /proc/${pid}/status ]]; then
  ids=$(awk '/^(Uid|Gid):/ { printf "%s %s ", $1, $2 }' "/proc/${pid}/status")
else # the daemon's PIDs are not visible here (a sandbox): ask it
  ids=$(docker top "$name" -o pid,user,group | awk 'NR == 2 { printf "Uid: %s Gid: %s ", $2, $3 }')
fi
echo "process: ${ids}"
[[ $ids == "Uid: 65532 Gid: 65532 " ]]

# A Redis target, a rule and its trigger, then a row: one SET with the row's document.
sql "$schema" -e "
  CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50));
  INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars)
    VALUES ('cache', 'redis', '127.0.0.1:34379/7', '{\"password\": \"env:LWF_REDIS_PW\", \"prefix\": \"img_\"}');
  INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms)
    VALUES ('docs', 'cache', 'key', 'SELECT id AS _key, name FROM customers WHERE id IN (:keys)',
            '{{#each docs}}SET {{redis_quote target.vars.prefix key}} {{redis_quote (json doc)}}{{/each}}', 100);
  INSERT INTO lazy_wire_fire_trigger (rule, source_table, \`key\`) VALUES ('docs', 'customers', '{{new.id}}');"
wait_for "rule loaded" rule_loaded
status | jq -e '.rules.docs.error == null' >/dev/null || { status | jq .rules.docs >&2; exit 1; }
sql "$schema" -e "INSERT INTO customers VALUES (1, 'ann')"
wait_for "Redis SET from the binlog" fired
echo "img_1 = $(redis GET img_1)"

docker stop -t 20 "$name" >/dev/null
code=$(docker inspect -f '{{.State.ExitCode}}' "$name")
if [[ $code != 0 ]]; then
  echo "exit code ${code} after SIGTERM, want 0" >&2
  exit 1
fi
echo "ok: exit 0 on SIGTERM"
sql -e "DROP DATABASE ${schema}"
