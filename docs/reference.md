# Reference

* [Targets](#targets): where actions go
* [Rules](#rules): what fires, rendered how
* [Triggers](#triggers): which changes mark which keys
* [Documents](#documents-from-path-named-columns): how select rows become `doc`
* [Templates](#templates): context, helpers, what to render
* [Reset steps](#reset-steps): when continuity is lost, or on request
* [Status](#status)
* [Security](#security)
* [Examples](#examples)

The tables are created on start in the DSN's schema. Text columns holding lists accept a JSON
array or items separated by commas, spaces or newlines. JSON columns are `TEXT` holding a JSON
object.

## Targets

`lazy_wire_fire_target`

| column | meaning |
|---|---|
| `name` | referenced by rules |
| `kind` | `http`, `http.basic` (`vars.username`, `vars.password`), `http.bearer` (`vars.token`), `http.header` (`vars.header`, `vars.value`), `http.s3` (SigV4, below), `redis` (`vars.password`, optional `vars.username` for ACLs), `mysql` / `pgsql` (`vars.username`, `vars.password`; [SQL targets](#sql-targets)) |
| `endpoints` | list. http: base URLs `http(s)://host:port[/prefix]` (the prefix is put before every path). redis: `host:port/db` (or `redis://…`, `rediss://…` for TLS). mysql / pgsql: `host:port/database`. `http.s3`: optional, the service URL; derived from `vars` when empty. An action goes to every endpoint |
| `tls` | empty (TLS for `https://` / `rediss://`, verified), `disabled`, `required` (no certificate checks), `verify-ca`, `verify-full`, or `{"mode": "verify-full", "ca": "/path/ca.pem", "server_name": "…"}` |
| `timeout_ms` | per action and endpoint, default 10000 |
| `expect` | success criterion: a SQL condition over the response, below |
| `vars` | JSON object: credentials for `kind` and anything templates need (`target.vars`). Credentials never reach templates |

A credential in `vars` is a literal or a reference: `env:NAME`, or `file:/path` (trailing newline
dropped, read on every use so rotated secrets apply). References are recommended when other users
can read the database.

**`http.s3`**: `vars.bucket`, `vars.region`, optional `vars.endpoint` (MinIO, R2, …),
`vars.path_style` (default false: virtual-hosted, `bucket.host`), `vars.access_key_ref`,
`vars.secret_key_ref`, optional `vars.session_token_ref`. The refs must be references. Without
them the default AWS chain is used: environment, shared credentials file (`AWS_PROFILE`), web
identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`), container credentials (ECS, EKS Pod
Identity), EC2 instance metadata. The template renders paths inside the bucket
(`PUT /products/1.json`); the tool adds `Host`, `x-amz-date`, `x-amz-content-sha256`,
`Authorization` (and `x-amz-security-token`), and the bucket for path style.

**`expect`**: a SQL condition the source database evaluates over the response, as the row `r`
of a derived table (`SELECT 1 FROM (SELECT … ) AS r WHERE (<expect>)`). The action succeeded
when it returns the row. It is MySQL / MariaDB SQL: operators, `JSON_*` functions, `CASE`, …

| column | http | redis | mysql / pgsql |
|---|---|---|---|
| `status` | status code | NULL | NULL |
| `headers` | JSON object, names in lower case (repeated headers joined with `, `) | NULL | NULL |
| `body` | the body when it is JSON, else NULL | the reply as JSON | NULL |
| `text` | the body as text | a string or integer reply as text, else NULL | NULL |
| `affected_rows` | NULL | NULL | rows the statement changed |

* Empty: http needs a 2xx status, redis any reply but an error, a SQL target a statement that
  ran. A condition that does not name `status` keeps the 2xx rule; one that does replaces it.
* Redis error replies always fail. An unmet condition on an http 5xx, 408 or 429 is transient
  (retried); on any other answer it is a refusal.
* Comparisons follow SQL: NULL is not met (a missing JSON field gives NULL), text compares in
  the connection's collation. `JSON_VALUE` of a JSON boolean differs between MySQL (`true`) and
  MariaDB (`1`); `JSON_CONTAINS` and `JSON_UNQUOTE(JSON_EXTRACT(…))` behave alike on both.
* Checked when rules load (run once against an empty answer), so syntax errors, unknown columns
  and (on MySQL) invalid JSON paths show up as the rule's error (log, `/status`, and
  `last_error` once it fires). A condition that cannot be evaluated
  later (the database unreachable) is a transient failure, never a refusal.
* Only the columns the condition names are sent. A condition that reads nothing but `r` (no
  subquery, variable or function outside the `JSON_*` and common string / number functions) is
  cached per distinct answer up to 1 KB, so repeated answers cost no query.

| target | `expect` |
|---|---|
| search `_bulk` | `JSON_CONTAINS(body, 'false', '$.errors')` |
| S3 `PUT` / `DELETE` | `status IN (200, 204)` |
| a webhook where a 404 is fine | `status BETWEEN 200 AND 299 OR status = 404` |
| an API that answers 200 with a state | `JSON_UNQUOTE(JSON_EXTRACT(body, '$.state')) = 'stored'` |
| a header | `JSON_UNQUOTE(JSON_EXTRACT(headers, '$."x-ack"')) = 'yes'` |
| redis `SET` | `text = 'OK'` |
| SQL targets | `affected_rows > 0` |

### SQL targets

`mysql` and `pgsql`: the template renders one SQL statement, run on the target database (a statement followed by another is refused; PostgreSQL runs it through the
extended protocol, which refuses batches too). Each connect pins the session so quoting is sound:
MySQL `SET NAMES utf8mb4` with backslash escapes on, PostgreSQL UTF-8 with
`standard_conforming_strings`. Put values in with `sql_quote` (a string literal of the target's
dialect, `NULL` for null; numbers are quoted too and converted by the server) and names with
`sql_ident`. Deadlocks, lock waits, timeouts and lost connections are transient; other SQL errors
count as refusals (see [security](#security)). Give the account only the rights the template
needs.

```
{{#each docs}}{{#if (eq op "delete")}}DELETE FROM copy WHERE id = {{sql_quote key}}
{{else}}INSERT INTO copy (id, name, doc) VALUES ({{sql_quote key}}, {{sql_quote doc.name}}, {{sql_quote (json doc)}})
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, doc = EXCLUDED.doc{{/if}}{{/each}}
```

## Rules

`lazy_wire_fire_rule`

| column | meaning |
|---|---|
| `name`, `enabled` | identity and switch |
| `target` | a target's `name` |
| `selects` | zero or more queries: SQL separated by `;`, or a JSON array of strings or `{"name", "sql"}`. Each returns `_key` plus [path-named columns](#documents-from-path-named-columns) and uses `:keys` for the changed keys (`WHERE id IN (:keys)`). They run concurrently, each on its own connection |
| `snapshot_keys_sql` | returns every key (`_key`), for `for_each` reset steps |
| `template` | the action, [below](#templates) |
| `fire_per` | `batch` (one action per batch of keys) or `key` (one per key; `docs` has one entry) |
| `batch_max_keys`, `batch_max_bytes` | split a flush above these limits. Bytes count the rendered action. Without `batch_max_keys`, a batch holds at most 10000 keys |
| `debounce_ms`, `max_delay_ms` | default 1000 and 5000: fire when nothing was marked for `debounce_ms`, at the latest `max_delay_ms` after the oldest pending change |
| `reset_steps` | JSON array, [below](#reset-steps) |
| `reset_requested` | set to 1 to run the reset steps; set back to 0 when they succeeded |
| `vars` | JSON object, `vars` in templates |

`:keys` becomes a list of parameters (padded to a power of two with the last key, so use it
inside `IN (…)`). Keys are strings; MySQL compares them with integer columns as numbers and still
uses the index.

Registration problems are reported, not fatal. A rule whose selects, template or target are broken
keeps marking keys (its position holds) and fires them once fixed. One path written by two selects
is such a problem.

## Triggers

`lazy_wire_fire_trigger`

| column | meaning |
|---|---|
| `rule` | the rule whose keys it marks; many triggers may feed one rule |
| `enabled` | switch, default 1 |
| `source_table` | `table` (the tool's schema) or `schema.table` |
| `on` | which events: list of `insert`, `update`, `delete` (default all) |
| `changed_columns` | optional list: only updates that change one of these columns |
| `key` | template over the row image giving the key; one key per line. `{{new.id}}`, `cat_p_{{old.entity_id}}` |
| `keys_sql` | alternative (or in addition): a query with `:old.<col>` / `:new.<col>` parameters returning `_key` rows, e.g. the parents of a child |

`old` and `new` both name the image being looked at. An insert has the new image, a delete the
old one, and an update is rendered for both, so a key that moved marks the old and the new key.
A line referring to a NULL column marks nothing. Simple templates (text and `{{new.col}}`) skip
Handlebars entirely; others get `old`, `new`, `row`, `op` and `table`.

`keys_sql` runs at flush time, once per distinct set of parameters, after the changes are
visible. It reads the current state: for a delete, rows already gone are not found (have a
trigger on that table too).

## Documents from path-named columns

The rows of all selects for a batch are merged by `_key`:

| column name | in `doc` |
|---|---|
| `name` | top-level field |
| `price.final` | nested field |
| `array[1].value` | element 1 (0-based) of an array, then a field |
| `category_ids[]` | collect: every row of the key appends its value, in row order |
| `items[].sku`, `items[].qty` | collect objects: one element per row |
| `_path`, `_value` | sets the path held in `_path` to `_value`, one row per field (`tags[]` appends) |
| `_op` | `delete` marks the key deleted. When any select has an `_op` column, a key no select returns is deleted too |
| `name:json` | the value is JSON text and is parsed (MySQL `JSON` columns always are) |

* One owner per path: a path written by two selects is a registration error.
* NULL means empty: a scalar becomes `null`; a NULL collect value is dropped, so an empty set is
  `[]`. To report "nothing left", return one row of NULLs for the key (a `LEFT JOIN`).
* Values: integers and floats are numbers, `DECIMAL` keeps its exact digits (`12.50`), dates are
  `YYYY-MM-DD[ HH:MM:SS[.ffffff]]`, strings and blobs are text.

## Templates

Handlebars, never HTML-escaped. Context:

| variable | content |
|---|---|
| `keys` | the batch's keys |
| `docs` | per key `{key, doc, op}`; `doc` is `{}` when the rule has no selects |
| `rule` | the rule name |
| `target` | `{name, vars}` (vars without credentials) |
| `vars` | the rule's vars |
| `endpoint` | `{host, port, url}` (redis also `db`) of the endpoint the action goes to |
| `reset` | inside reset steps `{step, keys}` (stray steps: the stray keys; list templates also `next`, `page`); `null` otherwise |

Inside `{{#each}}` / `{{#with}}` the names above still mean the top level (as with Handlebars.js
compat mode), so `{{target.vars.prefix}}` works inside `{{#each keys}}`. A field of the current
element with one of these names is `this.<name>`.

Helpers: `join list sep`, `upper`, `lower`, `regex_escape`, `url_encode`, `json`,
`replace s regex replacement`, `redis_quote s…` (its arguments concatenated, as one Redis
argument), `sql_quote v`, `sql_ident name` (in the dialect of the rule's target: PostgreSQL for
`pgsql`, MySQL otherwise), plus the built-ins `if`, `unless`, `each`, `with`,
`lookup`, `eq`, `ne`, `gt`, `lt`, `and`, `or`, `not`, `len`. String literals in templates take
JSON escapes: write `"\\d"` for the regex `\d`.

What a template renders:

* **http**: the raw request: request line (any method, `HTTP/1.1` optional), header lines, blank
  line, optional body. The tool sets `Content-Length`, adds `Host` (unless given), `User-Agent`
  and the authentication. Blank output sends nothing.
* **redis**: one command, split like `redis-cli` arguments (`"…"` with `\n`, `\"`, `\xHH`;
  `'…'`). `redis_quote` makes any string a safe argument. Blank output sends nothing.
* **mysql / pgsql**: one SQL statement. Blank output sends nothing.

## Reset steps

A JSON array, run in order; a failed step is retried, and the next one never starts before it
succeeded. They run when continuity is lost (first start, purged binlog, another server, a rule
enabled again, a watched table replaced without row events) or on `reset_requested = 1`, in the
background while streaming continues. An interrupted reset starts over after a restart.

```json
[
  {"once": true, "template": "PURGE / HTTP/1.1\nX-Tags-Pattern: .*\n"},
  {"for_each": "snapshot"},
  {"for_each": "stray", "list_template": "…", "keys": "JSON_EXTRACT(body, '$.hits.hits[*]._id')",
   "next": "JSON_EXTRACT(body, '$.hits.hits[last].sort')", "template": "…"}
]
```

* `once`: one action from `template`.
* `for_each: snapshot`: every key of `snapshot_keys_sql`, in batches, through the rule's selects
  and its template (or the step's `template`).
* `for_each: stray`: pages through the target's keys with `list_template` (context
  `reset.next`: the previous page's `next` value, `null` first; `reset.page`). `keys` and
  `next` are SQL expressions over the listing response, the row `r` of [`expect`](#targets)
  (a SQL target's listing gives its rows as a JSON array in `body`): `keys` the page's keys (a
  JSON array, one value, or NULL for none), `next` the cursor. JSON results (`JSON_EXTRACT`) are
  read as JSON, other text as a string. Paging stops when the cursor is NULL, `null` or `0` /
  `"0"` (Redis `SCAN`), when an http page brings no new keys, or when a Redis cursor repeats.
  The keys missing from the snapshot, read after the listing, are rendered with op `delete`
  (`reset.keys` holds them), per endpoint. Keys marked while the step runs are never treated as
  strays.

| listing | `keys` | `next` |
|---|---|---|
| OpenSearch / Elasticsearch 7, `search_after` by `_id` | `JSON_EXTRACT(body, '$.hits.hits[*]._id')` | `JSON_EXTRACT(body, '$.hits.hits[last].sort')` |
| Elasticsearch 8, scroll | `JSON_EXTRACT(body, '$.hits.hits[*]._id')` | `JSON_EXTRACT(body, '$._scroll_id')` |
| Redis `SCAN` | `JSON_EXTRACT(body, '$[1]')` | `JSON_EXTRACT(body, '$[0]')` |
| a SQL target, `SELECT id … ORDER BY id LIMIT 1000` | `JSON_EXTRACT(body, '$[*].id')` | `JSON_EXTRACT(body, '$[last].id')` |

## Status

`lazy_wire_fire_status`, one row per instance and rule, plus `rule = '*'` for the instance:

| column | meaning |
|---|---|
| `binlog_file`, `binlog_pos` | every action for changes before this point met `expect`. `*`: where the stream resumes |
| `lag_ms` | age of the oldest change not yet confirmed (or how far the stream is behind) |
| `reset_state` | `none`, `pending: <reason>`, `running: step i/n …`, `done: <time>`, `missing: <reason>` (continuity lost, the rule has no reset steps) |
| `last_error`, `last_error_at` | the last failure |
| `server_identity` | `*` only: the server the positions belong to |
| `updated_at` | written when the position moves, and at least every 30 s |

## Security

Templates are not escaped for you (there is no context the tool could know), so **values from
rows go through the helper of the place they land in**:

| where | write | not |
|---|---|---|
| a JSON string or value | `"_id":{{json key}}`, `{{json doc}}` | `"_id":"{{key}}"`: a key `1"}}…` adds `_bulk` actions |
| a path or query | `/items/{{url_encode key}}.json` | `/items/{{key}}.json`: a key `a?x=` writes another object |
| a regex (Varnish tags) | `{{regex_escape this}}` | `{{this}}`: a key `.*` purges everything |
| a Redis argument | `{{redis_quote this}}`, or `ARGV` of a script | splicing keys into a Lua string |
| SQL | `{{sql_quote key}}`, `{{sql_ident name}}` | `'{{key}}'`: a key `x'); DROP …` runs SQL |

What the tool enforces on its own:

* **Keys are one line of text.** A key with a control character (from a key template, `keys_sql`,
  the snapshot or a stray listing) is ignored and reported in `last_error`; a key template is
  rendered line by line, so a newline in a value cannot mark extra keys.
* **Interpolated values hold no control characters**: `{{x}}` turns them into spaces, so data
  never starts a new header, request, NDJSON line or command (`{{{x}}}` opts out). A request line
  or header with a control character is refused.
* **A refused key does not stall its rule.** An action the target refuses (4xx, an unmet
  `expect`, a Redis error) is split until the refused keys stand alone; the others go through.
  A key refused 5 times while others went through is dropped and named in `last_error`. When
  everything is refused, nothing is dropped (the template or the target is at fault). Transient
  failures (connection, timeout, 5xx, 429, Redis `LOADING`/`READONLY`/…) only wait.
* **Bounded parsing**: response bodies, Redis replies and path indexes (`_path`, `a[n]`, at most
  10000) are capped. Partials (`{{> …}}`) are refused.
* **Secret references are confined**: `env:` cannot name lazywf's own settings (`LAZYWF_*`), and
  `file:` must resolve (symlinks followed) inside `LAZYWF_SECRET_DIRS` (default
  `/run/secrets:/var/run/secrets`).

What it cannot enforce, so treat the tool's tables as **admin-only**:

* a writer of `lazy_wire_fire_rule` / `_trigger` runs SQL as lazywf's account (give it `SELECT`
  only where rules need it), and sends the result anywhere a target points;
* a writer of `lazy_wire_fire_target` chooses where requests (with the credentials of that target)
  go, including internal addresses.

## Examples

**Cache purge by tag, several hosts** (no selects: the keys are the tags):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints) VALUES ('varnish', 'http', 'http://cache1:6081, http://cache2:6081');
INSERT INTO lazy_wire_fire_rule (name, target, batch_max_bytes, template, reset_steps) VALUES ('purge', 'varnish', 7000,
'PURGE / HTTP/1.1
Host: {{endpoint.host}}
X-Tags-Pattern: {{#each keys}}((^|,){{regex_escape this}}(,|$)){{#unless @last}}|{{/unless}}{{/each}}
', '[{"once": true, "template": "PURGE / HTTP/1.1\nX-Tags-Pattern: .*\n"}]');
INSERT INTO lazy_wire_fire_trigger (rule, source_table, `key`) VALUES
 ('purge', 'stock', 'product_{{new.product_id}}'),
 ('purge', 'category_product', 'category_{{new.category_id}}\nproduct_{{new.product_id}}');
```

**Redis**, one command per key:

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars)
VALUES ('cache', 'redis', 'redis:6379/1', '{"password": "env:REDIS_PASSWORD", "prefix": "cust:"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template) VALUES ('customer_cache', 'cache', 'key',
 'SELECT id AS _key, name, email, IF(active, NULL, ''delete'') AS _op FROM customers WHERE id IN (:keys)',
 '{{#each docs}}{{#if (eq op "delete")}}DEL {{redis_quote target.vars.prefix key}}{{else}}SET {{redis_quote target.vars.prefix key}} {{redis_quote (json doc)}}{{/if}}{{/each}}');
```

**JSON objects in S3**, kept in sync (a `PUT` overwrites, a `DELETE` of a missing object succeeds):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, expect, vars) VALUES ('export', 'http.s3', 'status IN (200, 204)',
 '{"bucket": "exports", "region": "eu-west-1", "access_key_ref": "env:AWS_ACCESS_KEY_ID", "secret_key_ref": "env:AWS_SECRET_ACCESS_KEY"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, snapshot_keys_sql, template, reset_steps) VALUES
 ('customers_json', 'export', 'key',
  'SELECT id AS _key, name, email, NULL AS _op FROM customers WHERE id IN (:keys)',
  'SELECT id AS _key FROM customers',
  '{{#each docs}}{{#if (eq op "delete")}}DELETE /customers/{{url_encode key}}.json HTTP/1.1

{{else}}PUT /customers/{{url_encode key}}.json HTTP/1.1
Content-Type: application/json

{{json doc}}{{/if}}{{/each}}',
  '[{"for_each": "snapshot"}]');
```

The `_op` column is what turns a deleted customer into a `DELETE`: a key that no select returns,
while a select has `_op`, is deleted.

**Search index re-sync in place** (OpenSearch / Elasticsearch): upsert everything, then delete
what the index has and the database does not.

```json
[
  {"for_each": "snapshot"},
  {"for_each": "stray",
   "list_template": "POST /{{vars.alias}}/_search HTTP/1.1\nContent-Type: application/json\n\n{\"size\":1000,\"_source\":false,\"sort\":[{\"_id\":\"asc\"}]{{#if reset.next}},\"search_after\":{{json reset.next}}{{/if}}}",
   "keys": "JSON_EXTRACT(body, '$.hits.hits[*]._id')", "next": "JSON_EXTRACT(body, '$.hits.hits[last].sort')"}
]
```

The stray step has no `template`, so the rule's own `_bulk` template renders the deletes (every
`docs` entry has op `delete`).

## Notes

* Set `binlog_row_metadata=FULL` (MySQL 8) to name row images exactly while a DDL is in flight;
  otherwise names come from `information_schema` and a column-count mismatch resets the rules of
  that table.
* A DDL that changes values without row events (`ALTER TABLE … ADD COLUMN … DEFAULT …` read by a
  select) is not detected: request a reset (`reset_requested = 1`).
* Tables replaced by a swap (`RENAME TABLE new TO watched`) reset the rules watching them, since
  their rows changed without row events; avoid watching tables that are rebuilt this way often.
