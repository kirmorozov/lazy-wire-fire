#!/usr/bin/env bash
# Markdown for the CI job summaries of the integration jobs.
#
#   scripts/ci-summary.sh versions SERVICE...           server versions (table rows) on stdout
#   scripts/ci-summary.sh report TITLE LOG [VERSIONS]   the summary: versions, one row per test run
#
# LOG is the output of a make test-e2e* / test-matrix target: each cargo run is preceded by its
# command line (LAZYWF_TEST_DSN=…:PORT … cargo test --test e2e [FILTER]) or `=== SERVICE`.
set -euo pipefail

compose=(docker compose -f docker-compose.test.yml)

# PORT SERVICE per line, from the compose file's "127.0.0.1:PORT:..." mappings.
ports() {
  awk '/^  [a-z0-9]+:$/ { s = $1; sub(":", "", s) }
       match($0, /127\.0\.0\.1:[0-9]+:/) { p = substr($0, RSTART + 10, RLENGTH - 11); print p, s }' \
    docker-compose.test.yml
}

case ${1:-} in
  versions)
    shift
    for s in "$@"; do
      v=$("${compose[@]}" exec -T -e MYSQL_PWD=root "$s" \
            sh -c 'exec "$(command -v mariadb || command -v mysql)" -uroot -NB -e "SELECT VERSION()"' 2>/dev/null) \
        || v="(not reachable)"
      echo "| \`$s\` | $v |"
    done
    ;;
  report)
    title=$2 log=$3 versions=${4:-}
    echo "### $title"
    echo
    if [[ -n $versions && -s $versions ]]; then
      echo "| server | version |"
      echo "|---|---|"
      cat "$versions"
      echo
    fi
    echo "| run | result |"
    echo "|---|---|"
    if [[ -f $log ]]; then
      awk 'NR == FNR { svc[$1] = $2; next }
           /^=== / { run = $2; next }
           /cargo test/ && /LAZYWF_TEST_DSN=/ {
             run = ""
             if (match($0, /127\.0\.0\.1:[0-9]+/)) { p = substr($0, RSTART + 10, RLENGTH - 10); run = (p in svc) ? svc[p] : p }
             if (match($0, /--test e2e [a-z0-9_]+/)) run = run " " substr($0, RSTART + 11, RLENGTH - 11)
             next
           }
           /^test result:/ { sub(/^test result: /, ""); printf "| %s | %s |\n", (run == "" ? "e2e" : run), $0; n++ }
           END { if (!n) print "| — | no test result: see the log |" }' <(ports) "$log"
    else
      echo "| — | no log: the job stopped before the tests |"
    fi
    ;;
  *)
    echo "usage: $0 versions SERVICE... | report TITLE LOG [VERSIONS]" >&2
    exit 2
    ;;
esac
