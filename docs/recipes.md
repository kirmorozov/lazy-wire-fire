# HTTP recipes

Copy-ready recipes for the services people most often point the `http` kinds at: two
search engines, one OLAP store, two CDN purge APIs, an automation hub and a chat channel.
Each gives the endpoint form, the auth, the request as it goes on the wire, what to put in
`expect`, the batch limit that binds, and how a delete is expressed.

> **These are examples; CI does not run them.** The test suite exercises the built-in sinks
> and the `http` kinds against OpenSearch, Elasticsearch and Varnish containers, never against
> Meilisearch, Typesense, ClickHouse, Cloudflare, Fastly, Slack or a hosted hub. The endpoint
> forms and `expect` clauses below come from each vendor's own documentation and from the
> request-building code. Try one against your own account or container before production data
> drives it.

Three tables and one kind family do all of it: `lazy_wire_fire_target` for the address, the
credentials and the check, `lazy_wire_fire_rule` for the select and the template,
`lazy_wire_fire_trigger` for the keys ([reference → targets](reference.md#targets),
[rules](reference.md#rules), [triggers](reference.md#triggers)).

* [How the http kinds work](#how-the-http-kinds-work)
* [Meilisearch](#meilisearch-index-and-delete-documents)
* [Typesense](#typesense-import-documents)
* [ClickHouse](#clickhouse-insert-rows)
* [Cloudflare](#cloudflare-purge-by-url)
* [Fastly](#fastly-purge-surrogate-keys)
* [A signed webhook](#a-signed-webhook-to-an-automation-hub)
* [Slack](#slack-post-to-a-channel)

## How the http kinds work

**The template is the whole request**: request line (any method, `HTTP/1.1` optional), header
lines, a blank line, the body. The tool adds only `Content-Length` (from the body; one from the
template is dropped), `Host` (unless the template writes one), `User-Agent` (`lazywf/<version>`)
and the kind's authentication, in that order: the template's own header lines first, then
`User-Agent`, then `Host`, then the credential header, then `Content-Length` last. No
`Accept-Encoding`, no `Connection`, no content type of its own — the template writes
`Content-Type` when the body has one. A `GET`, `HEAD`, `DELETE`, `OPTIONS`, `PURGE` or `BAN` with
an empty body goes out without `Content-Length`. Blank output sends nothing. A control character in the request line or a header is refused, and `{{x}}` turns control
characters into spaces, so a value cannot start a second header or request (`{{{x}}}` opts out);
values go through the helper of the place they land in — `json` in JSON, `url_encode` in a path or
query ([reference → security](reference.md#security)).

**Auth is the kind**, and the credential is a `vars` reference:

| kind | credential | sends |
|---|---|---|
| `http` | — | nothing |
| `http.basic` | `vars.username`, `vars.password` | `Authorization: Basic <base64>` |
| `http.bearer` | `vars.token` | `Authorization: Bearer <token>` |
| `http.header` | `vars.header`, `vars.value` | `<vars.header>: <resolved value>` — one header |

Each credential is a literal, `env:NAME` or `file:/path`; a file's trailing newline is dropped and
a reference is resolved on every request, so a rotated secret applies without a restart. `file:`
must resolve inside `LAZYWF_SECRET_DIRS` (default `/run/secrets:/var/run/secrets`) and `env:`
cannot name lazywf's own `LAZYWF_*` settings. **References are resolved only there**: nothing is
resolved in `endpoints`, `template` or `expect`, so a token written into a template is a literal
that everyone who can read the table can see (`target.vars` in a template also holds no
credentials). One consequence worth remembering: `http.header` carries exactly one header, so a
service that wants two custom headers gets one from `vars` and the other written by the template —
and a template header is *rendered*, while `vars.value` is *resolved as a credential* and never
rendered, so data belongs in a template header and secrets in `vars`.

**`expect` is a column of the target, not of the rule.** It is one MySQL / MariaDB condition over
the answer as the row `r` of a derived table, which the source database evaluates:
`SELECT 1 FROM (SELECT …) AS r WHERE (<expect>)`. The columns for http are `status`, `headers`
(JSON object, names lower case, repeats joined with `, `), `body` (the body when it parses as
JSON, else NULL) and `text` (the body as text). Empty: a 2xx is met. A condition that names
`status` replaces that rule, so write the band into it. An unmet condition on a 5xx, 408 or 429 is
transient (waited out and retried); on any other answer it is a refusal, and a refusal is split
until the offending keys stand alone — a key refused 5 times while others went through is dropped
and named in `last_error` ([reference → targets](reference.md#targets),
[security](reference.md#security)). The column holds 1024 characters. `JSON_VALUE` of a JSON
boolean differs between MySQL (`true`) and MariaDB (`1`); `JSON_CONTAINS` and
`JSON_UNQUOTE(JSON_EXTRACT(…))` read alike on both.

Two rules share a target when they want the same check; rules that need different answers need
two targets on the same address.

**One action renders exactly one request** — so a batch that would have to "add these, delete
those" is one body and therefore one of the two. `batch_max_keys` caps the keys of that request
(`batch` mode holds 10000 keys when the column is empty) and `batch_max_bytes` caps its rendered
bytes; there is no bytes guard for http beyond that, so the vendor's own limit is the one to
respect ([reference → rules](reference.md#rules)). `fire_per = 'key'` sends one request per key,
which is what a per-document or per-message API wants. A rule sends its actions in one lane, so a
burst drains as a queue of requests rather than in parallel; `timeout_ms` (per action and
endpoint, default 10000) bounds each.

**Pin the hosts.** A target's endpoint hosts must match `LAZYWF_ALLOWED_HOSTS` when it is set
(exact names, `*.suffix`, IP literals or CIDR ranges, each optionally `:port`); an unmatched
endpoint is a load error naming the host ([reference → security](reference.md#security)). Where
the configuration tables cannot be admin-only, set it.

**Keep the path in the template.** An endpoint's prefix (`http(s)://host:port[/prefix]`) is put
before every path, with its trailing `/` stripped, so an endpoint of
`https://hooks.example/services/T1/B1/K1` and a request line of `POST /` arrive as
`/services/T1/B1/K1/` — with a trailing slash. Put the origin in `endpoints` and the whole path in
the request line, as below.

**Nothing to run first.** A rule is checked when it loads, not when it fires: an `expect` with
bad syntax, an unknown column or (on MySQL) an invalid JSON path is reported as the rule's error
in the log, in `/status`, and in `last_error` as soon as it fires. Send one change and read
`last_error` and `/status` (`actions_ok`, `actions_failed`) before sending a thousand.

## Meilisearch: index and delete documents

| | |
|---|---|
| **Endpoint** | `POST /indexes/{index_uid}/documents` (add or replace, an array of objects), `POST /indexes/{index_uid}/documents/delete-batch` (an array of primary key values) |
| **Auth** | `kind = 'http.bearer'`, `vars.token` → `env:MEILI_MASTER_KEY`. Without a master key set on the instance, use `kind = 'http'` and no credential |
| **Renders** | `[{…},{…}]` with `Content-Type: application/json` (the add endpoint also takes `application/x-ndjson` and `text/csv`) |
| **`expect`** | `status BETWEEN 200 AND 299 AND JSON_UNQUOTE(JSON_EXTRACT(body, '$.status')) IN ('enqueued','processing','succeeded')` — both endpoints answer **HTTP 202**, not 200 |
| **Batch limit** | No document-count limit is documented. The instance refuses a payload over `MEILI_HTTP_PAYLOAD_SIZE_LIMIT` (default `100000000`, ~100 MB), so set `batch_max_bytes` from one worst-case document; it is a refusal, not a retry |
| **Deletes** | `delete-batch` is a different path, and one action is one request: either `fire_per = 'key'` with a template that branches on `op`, or two rules whose triggers split `insert,update` from `delete` (below) |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, vars) VALUES
 ('meili', 'http.bearer', 'http://search:7700',
  'status BETWEEN 200 AND 299 AND JSON_UNQUOTE(JSON_EXTRACT(body, ''$.status'')) IN (''enqueued'', ''processing'', ''succeeded'')',
  '{"token": "env:MEILI_MASTER_KEY"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_bytes, selects, template) VALUES
 ('meili_add', 'meili', 'batch', 8000000,
  'SELECT id AS _key, id, name, price FROM products WHERE id IN (:keys)',
  'POST /indexes/products/documents HTTP/1.1
Content-Type: application/json

[{{#each docs}}{{#unless @first}}, {{/unless}}{{json doc}}{{/each}}]');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, template) VALUES
 ('meili_del', 'meili', 'batch',
  'POST /indexes/products/documents/delete-batch HTTP/1.1
Content-Type: application/json

[{{#each keys}}{{#unless @first}}, {{/unless}}{{json this}}{{/each}}]');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('meili_add', 'products', 'insert,update', 'name,price', '{{new.id}}'),
 ('meili_del', 'products', 'delete',        NULL,         '{{new.id}}');
```

The add rule's batch goes out like this (`Authorization` is the kind's, not the template's):

```text
POST /indexes/products/documents HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: search:7700
Authorization: Bearer <what env:MEILI_MASTER_KEY holds>
Content-Length: 71

[{"id":1,"name":"Bowl","price":12.50},{"id":2,"name":"Mat","price":30}]
```

* **202 is the answer, and it means "enqueued".** The body is a task
  (`{"taskUid":147,"indexUid":"products","status":"enqueued",…}`) whose status is one of
  `enqueued`, `processing`, `succeeded`, `failed`, `canceled`. Asserting the accepted status is
  the whole check lazywf can make: a task that fails later is Meilisearch's, not a failed action,
  and reading it back is the tasks API, out of scope here. The band is there for the 2xx
  answers: it is the `$.status` check that proves acceptance (a task in `enqueued`,
  `processing` or `succeeded`), not the status alone. A rejection (a bad document, a missing
  index) answers 4xx, which on http is transient whatever the condition says, so an unmet
  condition here means an unexpected 2xx.
* **The delete rule has no select**: with no selects, `docs` is empty and the keys are the data,
  so `{{#each keys}}` is the id list (on a delete, `{{new.id}}` names the deleted row's image).
* **A batch whose select returns nothing sends `[]`** — an empty add, which Meilisearch accepts,
  and an add to a missing index creates it. To send nothing instead, guard the template so it
  renders blank when there is nothing to send: `{{#if docs}}POST /indexes/…` … `{{/if}}` around
  the whole request, since blank output is not sent.
* **Mixed adds and deletes in one rule** means `fire_per = 'key'` and one template that branches,
  as the S3 example in [reference → examples](reference.md#examples) does:

  ```text
  {{#each docs}}{{#if (eq op "delete")}}POST /indexes/products/documents/delete-batch HTTP/1.1
  Content-Type: application/json

  [{{json key}}]
  {{else}}POST /indexes/products/documents HTTP/1.1
  Content-Type: application/json

  {{json doc}}{{/if}}{{/each}}
  ```

  with `fire_per = 'key'` and a select carrying `NULL AS _op` (a key no select returns is then a
  delete). Two rules are simpler and keep batching; per-key branching costs one request per
  document.

## Typesense: import documents

| | |
|---|---|
| **Endpoint** | `POST /collections/{collection}/documents/import?action=create\|update\|upsert` (`batch_size=N` is the server's own chunking, default 40; `return_id=true` puts ids in the answer) |
| **Auth** | `kind = 'http.header'`, `vars.header` = `x-typesense-api-key`, `vars.value` → `env:TYPESENSE_API_KEY` |
| **Renders** | JSON Lines: one document object per line, no array, no trailing comma |
| **`expect`** | Required. Import answers **HTTP 200 always** and reports each document on its own line (`{"success": true, "id":"0"}` / `{"success": false, "error": …}`), so the status band alone passes a rejected batch: `status BETWEEN 200 AND 299 AND text NOT LIKE '%"success": false%' AND text NOT LIKE '%"success":false%'` |
| **Batch limit** | 10 GB per import request, far above any `batch_max_keys` worth setting. The knob that matters is `timeout_ms`: an import that outlives it is a transient failure and the whole batch goes again, so `upsert` (not `create`) is the safe action |
| **Deletes** | Not an import action: `DELETE /collections/{collection}/documents/{id}?ignore_not_found=true` per key, or `DELETE /collections/{collection}/documents?filter_by=id: […]`; the answer is `{"num_deleted":…}` |

`body` is NULL here — JSONL is not one JSON document — so only `text` can see the per-line
results.

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('ts_import', 'http.header', 'http://search:8108',
  'status BETWEEN 200 AND 299 AND text NOT LIKE ''%"success": false%'' AND text NOT LIKE ''%"success":false%''',
  300000, '{"header": "x-typesense-api-key", "value": "env:TYPESENSE_API_KEY"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template) VALUES
 ('ts_add', 'ts_import', 'batch', 500,
  'SELECT id AS _key, id, name, price FROM products WHERE id IN (:keys)',
  'POST /collections/products/documents/import?action=upsert HTTP/1.1
Content-Type: application/x-ndjson

{{#each docs}}{{#unless @first}}
{{/unless}}{{json doc}}{{/each}}');

INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars) VALUES
 ('ts_delete', 'http.header', 'http://search:8108',
  '{"header": "x-typesense-api-key", "value": "env:TYPESENSE_API_KEY"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, template) VALUES
 ('ts_del', 'ts_delete', 'key',
  '{{#each keys}}DELETE /collections/products/documents/{{url_encode this}}?ignore_not_found=true HTTP/1.1
{{/each}}');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('ts_add', 'products', 'insert,update', 'name,price', '{{new.id}}'),
 ('ts_del', 'products', 'delete',        NULL,         '{{new.id}}');
```

```text
POST /collections/products/documents/import?action=upsert HTTP/1.1
Content-Type: application/x-ndjson
User-Agent: lazywf/<version>
Host: search:8108
x-typesense-api-key: <what env:TYPESENSE_API_KEY holds>
Content-Length: 69

{"id":1,"name":"Bowl","price":12.50}
{"id":2,"name":"Mat","price":30}
```

* **Two targets, one address**: the import rule needs the per-line check and the delete rule does
  not, and `expect` belongs to the target. `status BETWEEN 200 AND 299` in the condition keeps the
  band the condition would otherwise replace.
* **Why two `LIKE` patterns**: the vendor shows the failure line as `{"success": false, …}` but
  does not fix the spacing after the colon, and `LIKE` compares case-sensitively in the
  connection's collation. A rejected line refuses the whole action; the keys are then split until
  the bad ones stand alone, and after 5 refusals a key is dropped and named in `last_error`. That
  is the right outcome for a document the schema refuses.
* **`timeout_ms` 300000** because Typesense does not bound import time and a client that gives up
  mid-import loses a *transient* answer, which re-sends the whole batch. With `action=upsert` the
  re-send is harmless; with `action=create` the retry reports the already-imported documents as
  `success: false` lines and the `expect` then refuses keys that are in fact stored.
* **`ignore_not_found=true`** makes a missing document a success instead of an error, which is
  what a change feed wants. Asserting `num_deleted` is only worth it when a missing document is
  genuinely a failure.

## ClickHouse: insert rows

| | |
|---|---|
| **Endpoint** | `POST /?query=INSERT%20INTO%20…%20FORMAT%20JSONEachRow&wait_end_of_query=1`, the rows in the body (writes are **POST-only**: a GET is readonly) |
| **Auth** | `kind = 'http.basic'`, `vars.username` and `vars.password` → `env:CLICKHOUSE_PASSWORD`. ClickHouse also takes `X-ClickHouse-User` / `X-ClickHouse-Key`, which needs two headers — `http.header` carries one, so use Basic, or one header from `vars` and the other written by the template |
| **Renders** | The rows, after the statement in the `query` URL parameter (a line feed goes between the two parts) |
| **`expect`** | `status = 200 AND text = ''` — a request that returns no data table answers with an **empty body**, and errors come back as HTTP 500 with the text in the body |
| **Batch limit** | None per request is documented for writes; the statement travels in the URL and URLs are capped at 1 MiB by default (`http_max_uri_size`), so keep data in the body and set `batch_max_bytes` from the rendered size |
| **Deletes** | `DELETE FROM db.tbl WHERE id IN (…)` — a lightweight delete, sent the same way. It is a mutation: rows are marked, and physically removed at the next merge |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('clickhouse', 'http.basic', 'http://analytics:8123',
  'status = 200 AND text = ''''',
  60000, '{"username": "ingest", "password": "env:CLICKHOUSE_PASSWORD"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('ch_add', 'clickhouse', 'batch', 5000, 4000000,
  'SELECT id AS _key, id, name, price FROM products WHERE id IN (:keys)',
  'POST /?query=INSERT%20INTO%20shop.products%20FORMAT%20JSONEachRow&wait_end_of_query=1 HTTP/1.1
Content-Type: application/x-ndjson

{{#each docs}}{{#unless @first}}
{{/unless}}{{json doc}}{{/each}}'),
 ('ch_del', 'clickhouse', 'batch', 5000, 200000,
  NULL,
  'POST /?query=DELETE%20FROM%20shop.products%20WHERE%20id%20IN%20({{#each keys}}{{#unless @first}},{{/unless}}{{this}}{{/each}})&wait_end_of_query=1 HTTP/1.1
');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('ch_add', 'products', 'insert,update', 'name,price', '{{new.id}}'),
 ('ch_del', 'products', 'delete',        NULL,         '{{new.id}}');
```

```text
POST /?query=INSERT%20INTO%20shop.products%20FORMAT%20JSONEachRow&wait_end_of_query=1 HTTP/1.1
Content-Type: application/x-ndjson
User-Agent: lazywf/<version>
Host: analytics:8123
Authorization: Basic <base64 of ingest:<password>>
Content-Length: 69

{"id":1,"name":"Bowl","price":12.50}
{"id":2,"name":"Mat","price":30}
```

* **The empty body is the success signal**, and it needs `wait_end_of_query=1` next to it. The
  vendor's own caveat: a 200 does not guarantee the query succeeded, because an error raised after
  the status and headers were already sent is written at the end of the body. Buffering the
  response delays the headers until the query is resolved, so `text = ''` can be trusted — up to
  the buffer settings, which is why a check of the status alone is not enough either.
* **`FORMAT JSONEachRow` over `FORMAT CSV`**: `{{json doc}}` is already the right shape and needs
  no quoting rules to reason about. CSV invites the mismatch between JSON's `\"` and CSV's `""`.
* **`{{this}}` in the delete is raw**: the delete list works because the keys come from
  `{{new.id}}` and are digits. A key that is not a number changes the statement; anything with a
  space or a control character is refused at the request line. Validate the key template (or keep
  the filter in the body) if ids can ever be text.
* **A 200 means the mutation was accepted**, not that the rows are gone: a lightweight delete
  marks rows and the removal happens at a merge, and the statement waits for the marking by
  default (`lightweight_deletes_sync`). Treat a delete as eventually visible.
* **Only `*MergeTree` tables** take a lightweight `DELETE`.

## Cloudflare: purge by URL

| | |
|---|---|
| **Endpoint** | `POST /client/v4/zones/{zone_id}/purge_cache`, one request per zone |
| **Auth** | `kind = 'http.bearer'`, `vars.token` → `env:CLOUDFLARE_API_TOKEN` (an API token is the preferred scheme; the legacy pair is `X-Auth-Email` + `X-Auth-Key`, which two `http.header`-style headers would need — one of them written by the template, and it is a secret, so use a token) |
| **Renders** | `{"files":["https://…","https://…"]}`. One of `files`, `tags`, `hosts`, `prefixes` or `purge_everything` per request; `purge_everything` cannot be combined with the others |
| **`expect`** | `status BETWEEN 200 AND 299 AND JSON_CONTAINS(body, 'true', '$.success')` — the API answers HTTP 200 with `{"success":true,"errors":[],"messages":[]}`, and a `success: false` answer looks healthy at the status band alone |
| **Batch limit** | 100 operations per request on every plan (500 for a single-file purge on Enterprise) → `batch_max_keys = 100`; the request *rate* is plan-dependent too (Free 5 per minute up to Enterprise 50 per second), which one lane per rule absorbs |
| **Deletes** | Nothing to un-purge. Expiring one product page means putting it in the next request's list; `purge_everything` is the only "all" |

`success: true` means Cloudflare **accepted** the purge. It does not confirm the content was ever
cached or that any edge dropped it — Cloudflare's own check is to fetch the URL afterwards and see
`CF-Cache-Status: MISS`.

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, vars) VALUES
 ('cloudflare', 'http.bearer', 'https://api.cloudflare.com',
  'status BETWEEN 200 AND 299 AND JSON_CONTAINS(body, ''true'', ''$.success'')',
  '{"token": "env:CLOUDFLARE_API_TOKEN"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, vars, template, debounce_ms, max_delay_ms) VALUES
 ('cf_purge', 'cloudflare', 'batch', 100, '{"zone": "0a1b2c3d4e5f60718293a4b5c6d7e8f9"}',
  'POST /client/v4/zones/{{url_encode vars.zone}}/purge_cache HTTP/1.1
Content-Type: application/json

{"files": [{{#each keys}}{{#unless @first}}, {{/unless}}{{json this}}{{/each}}]}',
  5000, 30000);

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('cf_purge', 'products', 'update', 'name,price', 'https://www.example.com/products/{{new.slug}}');
```

```text
POST /client/v4/zones/0a1b2c3d4e5f60718293a4b5c6d7e8f9/purge_cache HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: api.cloudflare.com
Authorization: Bearer <what env:CLOUDFLARE_API_TOKEN holds>
Content-Length: 92

{"files": ["https://www.example.com/products/bowl", "https://www.example.com/products/mat"]}
```

* **The keys are the URLs**, so the rule has no select and the trigger builds them
  (`https://www.example.com/products/{{new.slug}}`). A key is one line of text, and `{{json this}}`
  makes it a JSON string; `{{url_encode vars.zone}}` keeps the zone out of the request line's
  whitespace rules.
* **`JSON_CONTAINS(body, 'true', '$.success')`** is the form that behaves identically on MySQL and
  MariaDB; a `JSON_VALUE(… ) = 'true'` comparison does not.
* **Purge everything** is the same target with a different template, and it belongs on a rule no
  trigger can reach (`reset_requested = 1` and a `once` reset step, or a rule you start by hand):
  `POST /client/v4/zones/{{url_encode vars.zone}}/purge_cache HTTP/1.1` with
  `{"purge_everything": true}`.
* **A 429 is transient** here, so exceeding the plan's request rate waits and retries instead of
  dead-lettering; `debounce_ms` 5000 and `max_delay_ms` 30000 turn a burst of changes into fewer,
  fuller requests of 100 URLs each.

## Fastly: purge surrogate keys

| | |
|---|---|
| **Endpoint** | `POST /service/{service_id}/purge` (a batch of keys), also `POST /service/{service_id}/purge/{surrogate_key}`, `POST /purge/{cached_url}` and `POST /service/{service_id}/purge_all` |
| **Auth** | `kind = 'http.header'`, `vars.header` = `Fastly-Key`, `vars.value` → `env:FASTLY_API_TOKEN` |
| **Renders** | `{"surrogate_keys":["a","b"]}` — or no body at all with the keys in a `Surrogate-Key: a b c` header line the template writes |
| **`expect`** | NULL: rely on the 2xx band. The purge endpoints are documented by their request shape and their headers; no response-body contract is given, so there is nothing to assert beyond the status |
| **Batch limit** | **256 surrogate keys per request** → `batch_max_keys = 256` |
| **Deletes** | The purge is the delete. `Fastly-Soft-Purge: 1` marks objects stale instead of removing them (not for purge-all), and `Fastly-Purge-Environment: staging` targets the staging environment |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars) VALUES
 ('fastly', 'http.header', 'https://api.fastly.com',
  '{"header": "Fastly-Key", "value": "env:FASTLY_API_TOKEN"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, vars, template, debounce_ms) VALUES
 ('fastly_purge', 'fastly', 'batch', 256, '{"service": "SERVICE_ID"}',
  'POST /service/{{url_encode vars.service}}/purge HTTP/1.1
Content-Type: application/json
Fastly-Soft-Purge: 1

{"surrogate_keys": [{{#each keys}}{{#unless @first}}, {{/unless}}{{json this}}{{/each}}]}',
  2000);

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('fastly_purge', 'products',   'update', 'name,price', 'product_{{new.slug}}'),
 ('fastly_purge', 'categories', 'update', 'name',       'category_{{new.slug}}');
```

```text
POST /service/SERVICE_ID/purge HTTP/1.1
Content-Type: application/json
Fastly-Soft-Purge: 1
User-Agent: lazywf/<version>
Host: api.fastly.com
Fastly-Key: <what env:FASTLY_API_TOKEN holds>
Content-Length: 51

{"surrogate_keys": ["product_bowl", "product_mat"]}
```

* **Keys in the body, not in a credential header.** The vendor documents both shapes; the body is
  the primary one and keeps `vars.value` a pure credential. If you prefer
  `Surrogate-Key: {{join keys " "}}`, write it as a template header line — `{{join keys " "}}` in
  `vars.value` would be sent literally, because `vars` values are resolved as credentials and not
  rendered as templates.
* **Two triggers, one rule**: both mark keys of the same rule, so a change to a product and to its
  category arrive as one purge of both keys, deduplicated by key within the flush.
* **Soft purge** is a header the template writes; drop the line for a hard purge. A purge-all
  cannot be soft.
* **`{{url_encode vars.service}}`**: a service id is alphanumeric, and the request line refuses
  extra words, so encoding is cheap insurance against a `vars` edit.

## A signed webhook to an automation hub

| | |
|---|---|
| **Endpoint** | the hub's inbound webhook path, e.g. `POST /webhooks/lazywf` with `endpoints` = `https://hub.example` |
| **Auth** | Whatever the hub itself requires, on top of the signature: `http.bearer` (`vars.token`) or `http.header` (`vars.header`, `vars.value`). Signing and auth are independent settings — keep both when the hub checks both |
| **Renders** | One JSON object per action naming the rule and carrying the batch's keys: `{"rule":"hub_orders","keys":[42,43]}`. The hub decides what to run |
| **`expect`** | Mirror whatever the handler replies, exactly: `status = 200 AND text = 'ok'`. A 200 that is an HTML error page must not count as delivered |
| **Batch limit** | Yours to set: keep `batch_max_bytes` under the smallest body limit on the path (hub, reverse proxy, or the front end's `client_max_body_size`) |
| **Deletes** | A delete is an event, not a verb: put the op on the payload, or send a `DELETE` request line from its own rule. Nothing sent can be recalled |

Signing is set in the target's `vars`, applied after the template is rendered and after the auth
headers, over the exact bytes the request sends
([reference → webhook signing](reference.md#webhook-signing)):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, vars) VALUES
 ('hub', 'http', 'https://hub.example', 'status = 200 AND text = ''ok''',
  '{"sign": "standard", "sign_secret_ref": "file:/run/secrets/hub_whsec"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, template) VALUES
 ('hub_orders', 'hub', 'batch', 200, 200000,
  'POST /webhooks/lazywf HTTP/1.1
Content-Type: application/json

{"rule": {{json rule}}, "keys": [{{#each keys}}{{#unless @first}}, {{/unless}}{{json this}}{{/each}}]}');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, `key`) VALUES
 ('hub_orders', 'orders', '*', '{{new.id}}');
```

```text
POST /webhooks/lazywf HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: hub.example
Webhook-Id: hub-0000000000000011
Webhook-Timestamp: 1762300800
Webhook-Signature: v1,<base64 HMAC-SHA256 of "hub-0000000000000011.1762300800.<the body>">
Content-Length: 40

{"rule": "hub_orders", "keys": [42, 43]}
```

* **Two schemes.** `standard` is [Standard Webhooks](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md):
  the tool adds `Webhook-Id`, `Webhook-Timestamp` and `Webhook-Signature: v1,<base64>`, the base64
  HMAC-SHA256 of `<id>.<timestamp>.<body>`, and the secret must be the spec's `whsec_…` form.
  `hmac` is one header — by default `X-Signature-256: sha256=<hex>`, the GitHub-style form — with
  `vars.sign_header`, `vars.sign_prefix` and `vars.sign_encoding` (`hex` or `base64`) to match what
  the receiver expects. `vars.sign_secret_ref` is a reference only (`env:` or `file:`), never a
  literal, and it stays out of templates and logs.
* **The signature covers the bytes on the wire**, so nothing re-formats the body between signing
  and sending. A receiver that recomputes it must read the raw request body, not a re-serialised
  one.
* **Verifying on the hub's side is the hub's job.** Nothing here claims that any particular
  workflow tool checks an HMAC natively: the webhook inputs of the tools people point at this
  usually authenticate with Basic, header, JWT or nothing and accept an arbitrary payload.
  Verification is either the hub's own signature step or a small script in the flow that
  recomputes HMAC-SHA256 over the raw bytes, compares in constant time, and rejects a
  `Webhook-Timestamp` outside the tolerance it chooses (the tool sends the timestamp; it does not
  enforce one).
* **`on = '*'`** marks keys on inserts, updates and deletes. To tell the hub which is which, use
  `fire_per = 'key'` and a select with `NULL AS _op`, then `{{op}}` in the payload — a key no
  select returns arrives with op `delete`.

## Slack: post to a channel

| | |
|---|---|
| **Endpoint** | `POST` to the incoming-webhook URL: `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX` |
| **Auth** | **None — leave `vars` empty for this target.** The URL is the credential: it contains a secret, and Slack revokes a leaked one |
| **Renders** | `{"text":"…"}`, optionally with `blocks`, as `Content-Type: application/json` |
| **`expect`** | `status = 200 AND text = 'ok'` — a posted message answers 200 with the plain string `ok`, which is not JSON, so `body` is NULL and only `text` can confirm it |
| **Batch limit** | One message per request and about one message per second per channel → `fire_per = 'key'`, no batching |
| **Deletes** | Incoming webhooks cannot delete a message once posted; deleting means the Web API (`chat.delete`) with a bot token, i.e. a different target with real auth |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms) VALUES
 ('slack', 'http', 'https://hooks.slack.com', 'status = 200 AND text = ''ok''', 10000);

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms, max_delay_ms) VALUES
 ('stock_alerts', 'slack', 'key',
  'SELECT id AS _key, note FROM alerts WHERE id IN (:keys)',
  'POST /services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX HTTP/1.1
Content-Type: application/json

{{#each docs}}{"text": {{json doc.note}}}{{/each}}',
  5000, 60000);

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, `key`) VALUES
 ('stock_alerts', 'alerts', 'insert', '{{new.id}}');
```

```text
POST /services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: hooks.slack.com
Content-Length: 32

{"text": "Bowl is out of stock"}
```

* **The URL is a credential sitting in a plain column.** Values in `endpoints` are text, not
  references, so anyone who can read `lazy_wire_fire_target` can post to that channel, and a
  configuration snapshot copies it. Keep the tables admin-only, give each channel its own webhook
  rather than sharing one, and remember rotation means editing this row — Slack's own guidance is
  that it hunts for and revokes leaked webhook URLs.
* **The whole path is in the request line**, not in `endpoints`: `endpoints` is the origin and the
  template's request line carries `/services/…`, which avoids the trailing slash a prefix plus
  `POST /` would produce.
* **`fire_per = 'key'` and one lane** put the limiter on the sender's side. Exceeding about one
  message per second answers **HTTP 429 with a `Retry-After` header**, which is transient: the
  action waits and retries rather than dead-lettering, so a fast source drains slowly instead of
  losing alerts. Raise `debounce_ms` / `max_delay_ms` to space a burst out further.

## What these recipes do not claim

The vendor pages consulted do not settle everything a recipe needs, so these are verification
steps, not instructions:

* **Meilisearch**: no per-request document count is documented, only the instance's payload-size
  limit — nothing above assumes a maximum batch. Whether a quoted string id matches a numeric
  primary key in `delete-batch` is not documented either, so check your primary key's type before
  trusting a delete batch.
* **Typesense**: whether `filter_by=id: [1,2]` accepts the space-free form, and whether
  `num_deleted` counts ids that never existed. The `expect` above tolerates both spacings after
  `"success":`.
* **ClickHouse**: the quoting rules that apply to string literals inside a `DELETE … WHERE` sent
  in the `query` URL parameter. The recipe uses integer ids; for text ids, verify escaping in your
  own deployment or move the filter into the body.
* **Cloudflare**: the exact status code for exceeding a per-request or per-rate limit, and the
  name of the token permission a purge needs. Use `JSON_CONTAINS` on `success` and let the 429 /
  5xx band handle the rest.
* **Fastly**: no response-body contract is documented for the purge endpoints, so `expect` is
  NULL, and no purge rate-limit figures are given for a service.
* **Automation hub**: no third-party workflow tool is claimed here to verify an HMAC natively. The
  signing contract is this tool's and the Standard Webhooks convention.
* **Slack**: a webhook URL cannot be rotated from the configuration tables, and an incoming webhook
  cannot delete or edit what it posted.
