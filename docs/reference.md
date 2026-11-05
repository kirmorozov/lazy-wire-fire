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
| `kind` | `http` (also `https`), `http.basic` (`vars.username`, `vars.password`), `http.bearer` (`vars.token`), `http.header` (`vars.header`, `vars.value`), `http.s3` (SigV4, below), `sqs` (SigV4, an Amazon
SQS queue, below), `kinesis` (SigV4, an Amazon Kinesis stream, below), `lambda` (SigV4, an Amazon
Lambda function, below), `amqp` (an AMQP 0-9-1
broker with publisher confirms, below), `pubsub` (a Google Cloud Pub/Sub topic, below), `redis` (`vars.password`, optional `vars.username` for ACLs), `mysql` (also `mariadb`) / `pgsql` (also `postgres`, `postgresql`) (`vars.username`, `vars.password`; [SQL targets](#sql-targets)) |
| `endpoints` | list. http: base URLs `http(s)://host:port[/prefix]` (the prefix is put before every path). redis: `host:port/db` (or `redis://…`, `rediss://…` for TLS). mysql / pgsql: `host:port/database` (the scheme, `mysql://` / `postgres://`, … is optional). `http.s3`: optional, the service URL; derived from `vars` when empty. `sqs`: exactly one: `aws-sqs://[<region>/]<account-id>/<queue-name>`. `kinesis`: exactly one: `aws-kinesis://[<region>/]<stream-name>`. `lambda`: exactly one: `aws-lambda://[<region>/]<function-name>[:<qualifier>]` (the `aws-` URL form, `?endpoint=`, `?invocation=` and the region: below). `amqp`: `amqp(s)://host[:port][/vhost]` (no credentials in the URL). `pubsub`: exactly one: `gc-pubsub://<project>/<topic>` (`?endpoint=` for the emulator: below). An action goes to every endpoint |
| `tls` | empty (TLS for `https://` / `rediss://` / `amqps://`, verified), `disabled`, `required` (no certificate checks), `verify-ca`, `verify-full`, or `{"mode": "verify-full", "ca": "/path/ca.pem", "server_name": "…"}` (`amqp` takes only `verify-full` / empty / `disabled` on `amqp://`: [below](#amqp-targets)) |
| `timeout_ms` | per action and endpoint, default 10000 |
| `expect` | success criterion: a SQL condition over the response, below |
| `vars` | JSON object: credentials for `kind` and anything templates need (`target.vars`). Credentials never reach templates |

A credential in `vars` is a literal or a reference: `env:NAME`, or `file:/path` (trailing newline
dropped, read on every use so rotated secrets apply). References are recommended when other users
can read the database.

**`http.s3`**: `vars.bucket`, `vars.region`, optional `vars.endpoint` (MinIO, R2, …),
`vars.path_style` (default false: virtual-hosted, `bucket.host`), `vars.access_key_ref`,
`vars.secret_key_ref`, optional `vars.session_token_ref` ([AWS credentials](#aws-credentials)).
The template renders paths inside the bucket (`PUT /products/1.json`); the tool adds `Host`,
`x-amz-date`, `x-amz-content-sha256`, `Authorization` (and `x-amz-security-token`), and the
bucket for path style.

**`sqs`**: the endpoint is the queue, as `aws-sqs://[<region>/]<account-id>/<queue-name>` (the
`aws-` URL form: below), and one action is one `SendMessageBatch` request to the service root
(`POST /`): the template names the action (`X-Amz-Target: AmazonSQS.SendMessageBatch`) and the
queue in the body (`QueueUrl`, from `{{json endpoint.url}}` — the queue URL built from the
address). `vars.access_key_ref`, `vars.secret_key_ref` (a pair) and optional
`vars.session_token_ref` must be references, and without them the [AWS credentials](#aws-credentials)
default chain is used; the tool signs the request SigV4 for service `sqs` (adds `Host`,
`x-amz-date`, `x-amz-content-sha256`, `Authorization`, and `x-amz-security-token` with a session
token). One entry per key, the answer per entry: [below](#sqs-targets).

**`kinesis`**: the endpoint is the stream, as `aws-kinesis://[<region>/]<stream-name>` (the
`aws-` URL form: below). One action is one `PutRecords` request to the service root (`POST /`):
the template names the action (`X-Amz-Target: Kinesis_20131202.PutRecords`) and the stream in
the body (`StreamName`, from `{{json endpoint.stream}}`). `vars.access_key_ref`,
`vars.secret_key_ref` (a pair) and optional `vars.session_token_ref` must be references, and
without them the [AWS credentials](#aws-credentials) default chain is used; the tool signs the
request SigV4 for service `kinesis` (adds `Host`, `x-amz-date`, `x-amz-content-sha256`,
`Authorization`, and `x-amz-security-token` with a session token). One record per key, the
answer per record: [below](#kinesis-targets).

**`lambda`**: the endpoint is the function, as
`aws-lambda://[<region>/]<function-name>[:<qualifier>]` (`?invocation=sync|event`, the `aws-`
URL form: below). One action is one `Invoke`: the template renders the payload only (the request
body); the tool writes the request (`POST /2015-03-31/functions/<function>/invocations`,
`X-Amz-Invocation-Type`), signs it SigV4 for service `lambda` and sends it. Credentials as
`sqs` and `kinesis`: [AWS credentials](#aws-credentials). One invocation per action, the answer
per item when the function lists `batchItemFailures`: [below](#lambda-targets).

**`pubsub`**: the endpoint is the topic, as `gc-pubsub://<project>/<topic>` (`?endpoint=` for
the emulator, below). Credentials: `vars.credentials_ref`, a reference to a service-account key
(never a literal), else [Application Default Credentials](#pubsub-targets). One action is one
`topics.publish` request, rendered by the template: [below](#pubsub-targets).

**`amqp`**: the endpoint is `amqp(s)://host[:port][/vhost]` (default ports 5672 / 5671; the
vhost is the path, `/` the default vhost), without credentials: `vars.username` and
`vars.password`, references or literals (as `redis` takes them), resolved on each connect. One
message per action with a publisher confirm: [below](#amqp-targets).

**The `aws-sqs://`, `aws-kinesis://` and `aws-lambda://` URL form** (the `endpoints` of the kinds
`sqs`, `kinesis` and `lambda`, and of no other kind):

* `sqs`: `aws-sqs://[<region>/]<account-id>/<queue-name>` — the account id is twelve digits, the
  queue name 1 to 80 letters, digits, `-` or `_`, optionally ending in `.fifo` (a FIFO queue).
* `kinesis`: `aws-kinesis://[<region>/]<stream-name>` — the stream name is 1 to 128 letters,
  digits, `-`, `_` or `.`.
* `lambda`: `aws-lambda://[<region>/]<function-name>[:<qualifier>]` — the function name is 1 to
  64 letters, digits, `-`, `_` or `.` (a function ARN does not fit the form and is refused); the
  qualifier is a version or an alias (letters, digits, `-`, `_`), `$LATEST` or
  `$LATEST.PUBLISHED`. `?invocation=sync` (the default: the answer is the function's result) or
  `?invocation=event` (queued; answered 202 before the function runs), once each.
* The region comes from the URL when the URL gives one; otherwise from the AWS default settings:
  `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `region` in the shared config file's profile
  (`AWS_CONFIG_FILE`, default `~/.aws/config`; the profile is `AWS_PROFILE`, default
  `default`). When none of these gives a region, the target does not load (the error names both
  ways to set one). `vars.region` is refused by the three kinds (`vars.stream_name` and
  `vars.stream_arn` by `kinesis`); `http.s3` is unchanged and still takes `vars.region`.
* `?endpoint=http(s)://host:port` (the only other query parameter, once, no path) sends the
  request to that address instead of the regional one — an emulator or a VPC endpoint. The
  signing still uses the region and the service name; for `sqs`, `endpoint.url` is the queue URL
  built from that address.
* Credentials never go in the URL: a `user:password@` in it is a load error, as is a scheme that
  does not match the kind (`https://` for `sqs` or `kinesis`, `aws-kinesis://` for `sqs`, an
  `aws-` URL for any other kind); each refusal names the expected form.

The same rules, minus the region in the URL, hold for `pubsub`'s `gc-pubsub://` endpoint: no
userinfo, and a `gc-pubsub://` URL is for `kind pubsub` only (and `pubsub` takes only that
scheme); `?endpoint=` there is the only query parameter.

### AWS credentials

The kinds `http.s3`, `sqs`, `kinesis` and `lambda` sign their requests SigV4 with one of two
sources, chosen per target:

* **References in the target's `vars`**: `vars.access_key_ref` and `vars.secret_key_ref`
  (together; `vars.session_token_ref` optional). They must be references (`env:NAME` or
  `file:/path`, the `file:` rule of `LAZYWF_SECRET_DIRS`), never literals, and they are read on
  every use — a rotated mounted secret file applies at the next request, without a restart.
* **The default credentials chain**, when the target gives no references. It is tried in this
  order; the first source that yields credentials wins:

  | # | source | settings |
  |---|---|---|
  | 1 | environment | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (optional) |
  | 2 | shared credentials and config files | `AWS_SHARED_CREDENTIALS_FILE` (default `~/.aws/credentials`) and `AWS_CONFIG_FILE` (default `~/.aws/config`), profile `AWS_PROFILE` (default `default`); an assume-role profile (`role_arn` with `source_profile`) is followed |
  | 3 | web identity (for example EKS IRSA) | `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`; the role is assumed through STS |
  | 4 | container credentials (for example ECS, EKS Pod Identity) | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or `AWS_CONTAINER_CREDENTIALS_FULL_URI`, with `AWS_CONTAINER_AUTHORIZATION_TOKEN` or `…_TOKEN_FILE` |
  | 5 | EC2 instance metadata (IMDSv2) | off with `AWS_EC2_METADATA_DISABLED=true` |

  `credential_process` and SSO profiles are not part of this build (compiled out). Temporary
  credentials are fetched again five minutes before they expire. When no source yields
  credentials, the first request fails: `no AWS credentials: set access_key_ref /
  secret_key_ref in the target's vars, or provide them through the environment (tried: …)`.

The chain's own TLS calls (the STS call of web identity) trust the system's root CAs, not the
Mozilla roots lazywf's own HTTP client uses; the instance-metadata and container-credentials
calls are plain HTTP to link-local or task addresses and need none. The published image's
distroless base ships the CA bundle; a custom runtime image needs one for web identity. The region of the chain's own calls (the STS call of web identity) is `AWS_REGION`, then
`AWS_DEFAULT_REGION`, then the target's region; with none of these, the profile's. The signing
region is always the target's.

**`expect`**: a SQL condition the source database evaluates over the response, as the row `r`
of a derived table (`SELECT 1 FROM (SELECT … ) AS r WHERE (<expect>)`). The action succeeded
when it returns the row. It is MySQL / MariaDB SQL: operators, `JSON_*` functions, `CASE`, …

| column | http | redis | amqp | lambda | pubsub | mysql / pgsql |
|---|---|---|---|---|---|---|
| `status` | status code | NULL | 200 on an ack, 312 on a return | 200 on a sync answer, 202 on an event | status code | NULL |
| `headers` | JSON object, names in lower case (repeated headers joined with `, `) | NULL | NULL | JSON object | JSON object | NULL |
| `body` | the body when it is JSON, else NULL | the reply as JSON | the return as JSON | the function's result, when it is JSON | the publish answer, when it is JSON | NULL |
| `text` | the body as text | a string or integer reply as text, else NULL | `ack`, `return` or `nack` | the result as text | the answer as text | NULL |
| `affected_rows` | NULL | NULL | NULL | NULL | NULL | rows the statement changed |

* Empty: http needs a 2xx status, redis any reply but an error, amqp an ack, lambda a 200
  without a function error (an event is met on its 202), pubsub a 2xx whose `messageIds` reach
  every message, a SQL target a statement that ran. A condition that does not name `status`
  keeps the 2xx rule; one that does replaces it.
* Redis error replies always fail. An unmet condition on an http 5xx, 408 or 429 is transient
  (retried); on any other answer it is a refusal.
* For `sqs`, the answer of one `SendMessageBatch` names its entries, so `expect` runs once per
  entry: `status` is the request's HTTP status and `body` is the entry's own JSON (its record in
  `Successful`). An entry whose `expect` is not met is refused on its own.
* For `kinesis`, the answer of one `PutRecords` gives its records in request order, so `expect`
  runs once per record: `status` is the request's HTTP status and `body` is the record's own
  JSON. A record whose `expect` is not met is refused on its own.
* For `amqp`, `expect` is a condition over the broker's confirm ([the rows](#amqp-targets)): an
  ack with an unmet condition is refused on its own; a returned message (312) is refused and a
  nack is transient, whatever the condition says.
* For `lambda`, a synchronous invocation's `expect` runs over the result (`body` the result,
  `status` 200); a function error (`X-Amz-Function-Error`) refuses the invocation whatever the
  condition says, and an event invocation is met on its 202 with no `expect`
  ([below](#lambda-targets)).
* For `pubsub`, `expect` runs over the whole publish answer (`status`, `headers`, `body` the
  `{"messageIds": […]}` object); the outcome is whole-action ([below](#pubsub-targets)).
* On `http.s3`, `sqs`, `kinesis` and `lambda`, an error answer is transient when its code (the
  `x-amzn-errortype` header, else the body's `__type`) is `ThrottlingException`,
  `RequestThrottled`, `ProvisionedThroughputExceededException`, `ServiceUnavailable` or
  `InternalFailure`, whatever the status (AWS JSON errors come as HTTP 400); every other error
  code is a refusal.
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
| SQS `SendMessageBatch` (per entry) | `JSON_EXTRACT(body, '$.MessageId') IS NOT NULL` |
| Kinesis `PutRecords` (per record) | `JSON_EXTRACT(body, '$.SequenceNumber') IS NOT NULL` |
| an AMQP publish (an ack) | `status = 200 AND text = 'ack'` |
| a Lambda invoke (the result names the stored id) | `JSON_EXTRACT(body, '$.id') IS NOT NULL` |
| a Pub/Sub publish (at least one message id) | `JSON_LENGTH(JSON_EXTRACT(body, '$.messageIds')) > 0` |
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

### SQS targets

`sqs` sends to an Amazon SQS queue: one `SendMessageBatch` per action, one entry per key (`Id` is
the key's index in the batch), and the answer reports each entry by that `Id` — the kind's answer
is per item, not per action.

* **Batch limits.** With `fire_per = batch`, `batch_max_keys` is required and holds 1 to 10 (the
  most entries a `SendMessageBatch` takes); a rule without it, or with more, does not load.
  `batch_max_bytes` defaults to 262144 (256 KiB, the most a request takes) and cannot be set
  higher. With `fire_per = key` each key is its own one-entry request, so no batch is sized.
* **Outcomes.** An entry in `Successful` whose `expect` is met is confirmed, and its key is never
  sent again. An entry in `Failed` with `SenderFault: true` is refused on its own (no bisection:
  the answer names the entry); five such refusals, while other keys went through, drop the key and
  name it in `last_error`. Any other `Failed` entry, and an entry the answer does not name, is
  retried — only the entries that did not go through wait, and the batches not yet sent wait with
  them. A whole-request refusal (an error code that is not transient) is bisected down to the key
  that fails alone, as with `http`.
* **FIFO queues.** A queue whose name ends in `.fifo` is FIFO: the rule's selects must return a
  `version` column (refused at load without it), which the template writes as the entry's
  `MessageDeduplicationId`; the key is the `MessageGroupId` (`{{json key}}`). Use a version that
  changes with each state you want delivered: the queue's deduplication drops an entry whose id it
  already saw, so a version shared by two keys drops one of them; a NULL version leaves the entry
  without an id, which the queue refuses on its own (and drops after five refusals).
* **Reset steps.** The kind only appends: a sent message cannot be recalled or deleted by key. A
  `for_each: stray` step does not load ("a stray cleanup needs a target that can delete keys; kind
  sqs only appends"), and neither does a `once` step whose template purges the queue
  (`PurgeQueue`; recognized by text, so a purge built from variables is not caught). Still
  allowed: `once` (an announcement message), `for_each: snapshot` (every key is sent again;
  consumers must tolerate that) and `reset_requested`.
* **Contract.** The queue receives the latest state of each key, not a log of every change, and
  delivery is at-least-once: an entry can arrive twice (a retry after a lost answer; a snapshot
  reset), and only a FIFO queue's deduplication filters repeats. Consumers apply an entry as the
  key's current state and drop what they applied already. On a FIFO queue one key's entries keep
  their order (they share its `MessageGroupId`); across keys, and on a standard queue, no order is
  guaranteed.

### Kinesis targets

`kinesis` sends to an Amazon Kinesis stream: one `PutRecords` per action, one record per key, and
the answer gives the records back in request order — record *i* of the answer is record *i* of
the request, so the kind's answer is per item, not per action.

* **Batch limits.** With `fire_per = batch`, `batch_max_keys` is required and holds 1 to 500 (the
  most records a `PutRecords` takes); a rule without it, or with more, does not load.
  `batch_max_bytes` defaults to 5242880 (5 MiB, the most a request takes, the base64 `Data`
  counted) and cannot be set higher. With `fire_per = key` each key is its own one-record request,
  so no batch is sized.
* **Outcomes.** A record without an `ErrorCode` was stored, and `expect` is evaluated for it: met,
  its key is confirmed and never sent again; not met, the record is refused on its own. A record
  whose `ErrorCode` is `ProvisionedThroughputExceededException`, `InternalFailure` or
  `ServiceUnavailable` is transient: its key waits and is sent again later with its newest
  document. Any other `ErrorCode` refuses
  that record on its own (no bisection: the answer names it by position); five such refusals,
  while other keys went through, drop the key and name it in `last_error`. A record the answer
  does not name is retried — only the records that did not go through wait, and the requests not
  yet sent wait with them. A whole-request refusal (an error code that is not transient) is
  bisected down to the key that fails alone, as with `http`.
* **Reset steps.** The kind only appends: a put record cannot be recalled, so a stray cleanup has
  no effect on the stream. A `for_each: stray` step does not load ("a stray cleanup needs a target
  that can delete keys; kind kinesis only appends"), and neither does a `once` step whose template
  deletes the stream (`DeleteStream`; recognized by text, so a delete built from variables is not
  caught). Still allowed: `once` (an announcement record), `for_each: snapshot` (every key is put
  again as a new record; consumers must tolerate that) and `reset_requested`.
* **Contract.** The stream receives the latest state of each key, not a log of every change, and
  delivery is at-least-once: a record can arrive twice (a retry after a lost answer; a snapshot
  reset). With `PartitionKey` set to the key, one key's records land on one shard, in order;
  across keys, no order is guaranteed. The key must be 1 to 256 bytes. Consumers apply a record
  as the key's state as of the time it was put, and tolerate repeats.

### Lambda targets

`lambda` invokes an AWS Lambda function: one `Invoke` per action, signed SigV4 for service
`lambda` to `/2015-03-31/functions/<function>/invocations` (with `?Qualifier=` when the endpoint
gives one). The template renders the payload only — the request body; the tool writes the
request line and headers (`X-Amz-Invocation-Type: RequestResponse` or `Event`), signs it and
sends it. The kind's answer is per item: a synchronous result may name the items that do not go
through in `batchItemFailures`.

* **The endpoint and invocation.** `aws-lambda://[<region>/]<function-name>[:<qualifier>]`
  (`?invocation=sync` the default — RequestResponse, the answer is the function's result;
  `?invocation=event` — Event, queued, answered 202 before the function runs), and
  `?endpoint=` for emulators and VPC endpoints, as for `sqs` and `kinesis`. A function given as
  an ARN is refused: name it in the endpoint.
* **Payload limits.** One synchronous Invoke takes 6 MB, one event Invoke 1 MB (Lambda's MB is
  1,024 KB). `batch_max_bytes` cannot be set above the invocation's limit (refused at rule
  load), and a batch is split to it when unset; a payload over the limit is refused on its own.
* **Outcomes.** Synchronous: a 200 without `X-Amz-Function-Error` is judged by `expect` over the
  result (`body` the result); a function error (`Handled` or `Unhandled`) refuses the invocation
  on its own (strikes; a batch is bisected down to the failing key). `batchItemFailures` in the
  result lists the items that do not go through: only those keys wait (they are retried with
  their newest state), the others are met. An identifier the function lists that is not a key of
  the batch is a wrong answer: nothing from it is trusted. Event: a 202 is met (no `expect`;
  fire-and-forget — Lambda may deliver the event more than once, so the function must be
  idempotent). A non-2xx answer follows the usual rules: 5xx, 408, 429 and the AWS throttling
  codes are transient, every other error is a refusal.
* **Reset steps.** A `for_each: stray` step does not load ("a stray cleanup needs a target that
  can list keys; kind lambda cannot"): a function cannot list what it was invoked with. Still
  allowed: `once` (one invocation, an announcement for example), `for_each: snapshot` (every
  key is invoked again; the function must tolerate that) and `reset_requested`.
* **Contract.** The function receives what the template renders, at least once: a synchronous
  invocation whose answer was lost is sent again with the newest state, and an event invocation
  may be delivered more than once. A long function raises the target's `timeout_ms`
  (synchronous invocations wait for their result).

### AMQP targets

`amqp` sends to an AMQP 0-9-1 broker (RabbitMQ): one message per action — one `mandatory`
publish, the message persistent — and the broker's publisher confirm is the answer: an ack is
judged by `expect`, a `basic.return` (the message was not routed anywhere) is refused on its own,
and a nack or a lost confirm is transient.

* **One message per action.** `fire_per` must be `key`: one action is one message, so there is no
  batch to size. `fire_per = batch` is refused at rule load ("kind amqp sends one message per
  action: set `fire_per = key`").
* **The rendered message.** The first line is `PUBLISH /<exchange>/<routing-key>`, then a
  `Message-Id: <key>` header (the only header; the key, required), a blank line, and a JSON body
  (one JSON value, sent as written):

  ```text
  PUBLISH /orders/order.changed
  Message-Id: 42

  {"key":42,"op":null,"doc":{"sku":"a","qty":3}}
  ```

  The exchange is the path segment before the first `/` (`//queue` is the default exchange); the
  routing key is everything after it (it may hold `/`); both are taken as written. Exchange,
  routing key and `Message-Id` are at most 255 bytes each (AMQP short strings). A blank render
  sends nothing.
* **The confirm.** With `expect` empty, an ack is met; else `expect` runs over the confirm's row
  (`status` 200 / `text` `ack` on an ack; `status` 312 / `text` `return` / `body`
  `{"reply_code", "reply_text"}` on a return; `status` NULL / `text` `nack` on a nack), and an
  ack whose condition is not met is refused on its own. A returned message is refused whatever
  the condition says (no queue is bound to the routing key); a nack is never judged by `expect`
  (it is transient).
* **Outcomes.** An unmet `expect` on an ack, and a `basic.return` (reply 312, `NO_ROUTE`), refuse
  the key on its own; five such refusals, while
  other keys went through, drop the key and name it in `last_error`. A nack, a failed publish, a
  lost connection, and no confirm within `timeout_ms` are transient: the message may or may not
  have reached the broker, so the key is sent again (at-least-once). A failed publish drops the
  broker session; the next send opens a new one.
* **One publish in flight.** Publishes to one endpoint run one at a time (a publish holds the
  endpoint's session until its confirm arrives; lapin can hand a confirm to the wrong publish
  when several are in flight on one channel); different endpoints of the target run in parallel.
* **TLS.** `amqps://` is TLS, `verify-full` by default: the chain against the Mozilla roots and
  the `tls.ca` files, and the host name. The modes the client cannot express are refused at load,
  naming the mode: `required` and `verify-ca`, `tls.server_name`, and `disabled` on `amqps://`.
* **Reset steps.** The kind only appends: a sent message cannot be recalled. A `for_each: stray`
  step does not load ("a stray cleanup needs a target that can delete keys; kind amqp only
  appends"). Still allowed: `once` (it publishes one message, an announcement for example) and
  `for_each: snapshot` (every key is published again, as new messages).
* **Contract.** The exchange receives the latest state of each key, not a log of every change,
  and delivery is at-least-once: a message whose confirm was lost (a connection drop, a broker
  restart, a timeout) is published again, so a consumer accepts duplicates — the message id is
  the key, and the body carries the key's current state (here `op` and `doc`), so a consumer
  keeps the newest message per key. Several changes of one key within the debounce window arrive
  as one message, and a delete is the key's last state. Across keys no order is guaranteed.

### Pub/Sub targets

`pubsub` sends to a Google Cloud Pub/Sub topic: one `topics.publish` request per action, rendered
by the template. The outcome is whole-action: the publish answer names its messages by order,
not by key, so one publish is either met, refused or transient for all of its messages.

* **The endpoint.** `gc-pubsub://<project>/<topic>` (each name 1 to 255 letters, digits or
  `._~+-`), exactly one. `?endpoint=http(s)://host:port` (the only query parameter) is the
  service address — the emulator or a private endpoint. Credentials never go in the URL (a
  `@` is a load error); a `gc-pubsub://` endpoint is for `kind pubsub` only, and `pubsub` takes
  only `gc-pubsub://` endpoints.
* **Credentials.** `vars.credentials_ref` (optional): a reference (`env:NAME` or `file:/path`)
  to a service-account key (JSON), read on every use — never a literal key. Without it,
  Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS` (a service-account key or an
  `external_account` file), then the gcloud ADC file, then the metadata server (GCE, GKE,
  Workload Identity). With `?endpoint=` and no reference, no credentials are sent (the emulator
  takes anonymous requests). The access token is reused until 300 s before its one-hour
  lifetime ends; a 401 from the service drops it.
* **The rendered request.** The tool sends only `POST
  /v1/projects/<project>/topics/<topic>:publish` for the endpoint's own project and topic (any
  other rendered request is refused); the template gives the rest: `Content-Type:
  application/json` and a `{"messages": […]}` body. Each message's `data` is the document's JSON
  in base64 (the `base64` helper), its `attributes` carry `key` and `op` (strings, checked), and
  `orderingKey` is the key only when `vars.ordering` is `true` (a boolean, checked at load). The
  endpoint's project and topic are `endpoint.project` / `endpoint.topic`.
* **Batch limits.** `topics.publish` takes at most 1000 messages and 10 MB per request: with
  `fire_per = batch`, `batch_max_keys` is required and holds 1 to 1000; `batch_max_bytes` cannot
  exceed 10000000. A rendered body over the limits is refused on its own.
* **Ordering.** With `vars.ordering`, the key is the message's `orderingKey`, and a subscriber
  with message ordering on receives one key's messages in publish order. lazywf keeps that order
  across retries: one lane per rule, and a failed publish is retried with the same key and the
  same `orderingKey` (there is no resume step: `resumePublish` is a client-library method, and
  lazywf publishes over REST). Without `vars.ordering`, no per-key order is promised.
* **Outcomes.** A 2xx whose `messageIds` list reaches every message sent is judged by `expect`
  over the answer (a shorter list waits — it is not known to be stored). A 400
  `INVALID_ARGUMENT` is the only refusal: it is bisected down to the failing key (strikes; five
  drop the key). 401 / 403 / 404, 429, 5xx, a 400 `FAILED_PRECONDITION` and every other non-2xx
  answer, and a transport error, are transient (retried with backoff, indefinitely).
* **Reset steps.** The topic only appends: a `for_each: stray` step does not load ("a stray
  cleanup needs a target that can delete keys; kind pubsub only appends"), and neither does a
  `once` or `snapshot` step whose template names a seek, a detach, a delete, a purge, a
  subscription, or a topic other than the publish path ("pubsub targets only publish"). Still
  allowed: `once` (it publishes one message, an announcement for example) and `for_each:
  snapshot` (every key is published again; consumers dedupe by the `key` attribute).
* **Contract.** The topic receives the latest state of each key, not a log of every change, and
  delivery is at-least-once: a publish whose answer was lost is sent again, so a consumer
  dedupes by the `key` attribute and keeps the newest message per key. Several changes of one
  key within the debounce window arrive as one message, and a delete is the key's last state.

## Rules

`lazy_wire_fire_rule`

| column | meaning |
|---|---|
| `name`, `enabled` | identity and switch |
| `target` | a target's `name` |
| `selects` | zero or more queries: SQL separated by `;`, or a JSON array of strings or `{"name", "sql"}`. Each returns `_key` plus [path-named columns](#documents-from-path-named-columns) and uses `:keys` for the changed keys (`WHERE id IN (:keys)`). They run concurrently, each on its own connection |
| `snapshot_keys_sql` | returns every key (`_key`), for `for_each` reset steps |
| `template` | the action, [below](#templates) |
| `fire_per` | `batch` (one action per batch of keys) or `key` (one per key; `docs` has one entry). `amqp` takes `key` only (one message per action: [AMQP targets](#amqp-targets)) |
| `batch_max_keys`, `batch_max_bytes` | split a flush above these limits. Bytes count the rendered action. Without `batch_max_keys`, a batch holds at most 10000 keys. `sqs`: `batch_max_keys` is required with `fire_per = batch` and holds 1 to 10, and `batch_max_bytes` cannot exceed 262144 ([SQS targets](#sqs-targets)). `kinesis`: `batch_max_keys`
is required with `fire_per = batch` and holds 1 to 500, and `batch_max_bytes` cannot exceed
5242880 ([Kinesis targets](#kinesis-targets)). `pubsub`: `batch_max_keys` is required with
`fire_per = batch` and holds 1 to 1000, and `batch_max_bytes` cannot exceed 10000000
([Pub/Sub targets](#pubsub-targets)) |
| `debounce_ms`, `max_delay_ms` | default 1000 and 5000: fire when nothing was marked for `debounce_ms`, at the latest `max_delay_ms` after the oldest pending change |
| `reset_steps` | JSON array, [below](#reset-steps) |
| `reset_requested` | set to 1 to run the reset steps; set back to 0 when they succeeded |
| `vars` | JSON object, `vars` in templates |

`:keys` becomes a list of parameters (padded to a power of two with the last key, so use it
inside `IN (…)`). Keys are strings; MySQL compares them with integer columns as numbers and still
uses the index.

Unqualified table names in `selects`, `keys_sql` and `snapshot_keys_sql` resolve in the DSN's
schema; when the source tables live in another schema, qualify them (`FROM app.orders o`).

Registration problems are reported, not fatal. A rule whose selects, template or target are broken
keeps marking keys (its position holds) and fires them once fixed. One path written by two selects
is such a problem.

## Triggers

`lazy_wire_fire_trigger`

| column | meaning |
|---|---|
| `id` | auto-increment identifier; the trigger cited in errors |
| `rule` | the rule whose keys it marks; many triggers may feed one rule |
| `enabled` | switch, default 1 |
| `source_table` | `table` (the tool's schema) or `schema.table` |
| `on` | which events: `insert`, `update`, `delete` (or `*` / `all` for everything), default all |
| `changed_columns` | optional list: only updates that change one of these columns |
| `key` | template over the row image giving the key; one key per line. `{{new.id}}`, `item_{{old.entity_id}}` |
| `keys_sql` | alternative (or in addition): a query with `:old.<col>`, `:new.<col>` or `:row.<col>` parameters returning `_key` rows, e.g. the parents of a child |

An insert looks at the new image only and a delete at the old one only. `old` and `new` both name
the image being looked at, so on a delete `{{new.id}}` gives the deleted row's id; an update is
rendered for both images, so a key that moved marks the old and the new key. A line referring to
a NULL column marks nothing. Simple templates (text and `{{new.col}}`) skip Handlebars entirely;
others get `old`, `new`, `row`, `op` and `table`.

The same holds for `keys_sql`: `:old.<col>`, `:new.<col>` and `:row.<col>` all bind the image
being looked at, so on a delete they bind the old row's values, and an update runs the query with
the old image and with the new one. `keys_sql` runs at flush time, once per distinct set of
parameters, after the changes are visible. It reads the current state: for a delete, rows already
gone are not found (have a trigger on that table too).

### Foreign key cascades

Whether lazywf sees the changes a foreign key's `ON DELETE CASCADE` (or `ON UPDATE CASCADE`,
`SET NULL`) makes in a child table depends on whether the server writes them to the binary
log:

- MySQL 8.0 / 8.4: not logged — "the cascading deletes are handled internally by the InnoDB
  storage engine, which means that none of the changes are logged" (§17.19 "InnoDB and MySQL
  Replication",
  [8.0](https://dev.mysql.com/doc/refman/8.0/en/innodb-and-mysql-replication.html),
  [8.4](https://dev.mysql.com/doc/refman/8.4/en/innodb-and-mysql-replication.html)).
- Percona Server 8.4: not verified.
- MariaDB: not logged — "cascading deletes or updates based on foreign key relations are an
  internal mechanism, and are not written to the binary log"
  ([Replication and Foreign Keys](https://mariadb.com/docs/server/ha-and-performance/standard-replication/replication-and-foreign-keys));
  [MDEV-38243](https://jira.mariadb.org/browse/MDEV-38243) adds an opt-in session variable
  `rpl_use_binlog_events_for_fk_cascade`, default `OFF`.
- MySQL 9.6 and later: logged by default — as of 9.6 the SQL layer handles InnoDB foreign keys
  by default (`innodb_native_foreign_keys` default `FALSE`), and "SQL-layer cascades generate
  row events for affected child tables"
  ([9.7 §15.1.25.5](https://dev.mysql.com/doc/refman/9.7/en/create-table-foreign-keys.html));
  with `innodb_native_foreign_keys=ON` the cascades stay in InnoDB and "do not generate
  separate row events"
  ([26.10 §19.5.1.37](https://dev.mysql.com/doc/refman/26.10/en/replication-features-foreign-keys.html)).

Where the child changes are not logged, only the parent's change reaches lazywf:

- a document keyed by the parent (one per parent row, the children included by the rule's
  select) stays correct: the parent's change marks its key, and the select reads the current
  state, where the children are already gone;
- a document keyed by the child (one per child row) goes stale: the child table's trigger
  never sees the cascaded change, and `keys_sql` on the parent cannot list the children
  either — it runs at flush time, after the changes are visible, when they are already gone
  (the last paragraph above).

Workarounds: a rule triggered on the parent's deletes whose template removes the children by
parent id in the target (OpenSearch / Elasticsearch: `_delete_by_query` on the parent field;
a SQL target: `DELETE … WHERE parent_id = …`); a reset step with stray cleanup
([Reset steps](#reset-steps)); or deleting the children explicitly in the application, so
each delete is binlogged.

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
| `docs` | per key `{key, doc, op}`; empty when the rule has no selects (the keys are the data) |
| `rule` | the rule name |
| `target` | `{name, vars}` (vars without credentials) |
| `vars` | the rule's vars |
| `endpoint` | `{host, port, url}` (redis and the SQL targets also `db`; `amqp` also `vhost`; `sqs` also `queue` and `region`; `kinesis` also `stream` and `region`; `lambda` also `function`, `qualifier`, `invocation` and `region`; `pubsub` also `project` and `topic`) of the endpoint the action goes to |
| `reset` | inside reset steps `{step, keys}` (stray steps: the stray keys; list templates also `next`, `page`); `null` otherwise |

Inside `{{#each}}` / `{{#with}}` the names above still mean the top level (as with Handlebars.js
compat mode), so `{{target.vars.prefix}}` works inside `{{#each keys}}`. A field of the current
element with one of these names is `this.<name>`.

Helpers: `join list sep`, `upper`, `lower`, `regex_escape`, `url_encode`, `json`,
`base64 s` (the standard base64 alphabet, padded: a Kinesis record's `Data`, a Pub/Sub
message's `data`),
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
* **amqp**: one message: `PUBLISH /<exchange>/<routing-key>`, a `Message-Id` header (the key), a
  blank line, a JSON body. Blank output sends nothing.
* **lambda**: the Invoke payload only (one JSON value); the tool writes and signs the request.
  Blank output sends nothing.
* **pubsub**: the raw request as for `http`, but only the topic's `:publish` path is sent
  ([Pub/Sub targets](#pubsub-targets)). Blank output sends nothing.

## Reset steps

A JSON array, run in order; a failed step is retried, and the next one never starts before it
succeeded. They run when continuity is lost (first start, purged binlog, another server, a new
rule, a watched table replaced without row events) or on `reset_requested = 1`, in the background
while streaming continues. A rule enabled again replays the binlog from where it stopped instead,
and resets only when that part of the binlog was purged. An interrupted reset starts over after a restart.

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
  `"0"` (Redis `SCAN`), when an http page brings no new keys, or when a non-http cursor repeats.
  The keys missing from the snapshot, read after the listing, are rendered with op `delete`
  (`reset.keys` holds them), per endpoint. Keys marked while the step runs are never treated as
  strays.
* An append-only kind (`sqs`, `kinesis`, `amqp`, `pubsub`) refuses a `for_each: stray` step at
  load (a sent message cannot be recalled), `lambda` refuses one too (a function cannot list
  what it was invoked with), `sqs` and `kinesis` refuse a `once` step that purges what it has
  sent (a queue's `PurgeQueue`, a stream's `DeleteStream`), and `pubsub` a step that does
  anything but publish — [SQS targets](#sqs-targets), [Kinesis targets](#kinesis-targets),
  [Lambda targets](#lambda-targets), [AMQP targets](#amqp-targets),
  [Pub/Sub targets](#pubsub-targets).

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

`/status` is the same per rule (its `rules` map), plus what the table does not hold:

| field | meaning |
|---|---|
| `pending_keys` | the keys in memory that still wait for a flush whose actions met `expect` |
| `firing` | the rule's one lane is busy: a flush or a reset batch |
| `select_ms` | the time of the rule's selects in its last flush |
| `events` | the row changes that matched one of the rule's triggers since start (a row matching two triggers counts twice) |
| `actions_ok`, `actions_failed` | the actions sent since start: one action may carry a batch of keys, so they are not key counts |
| `binlog_filtered_schemas` | instance only, not per rule: the checked schemas the server's binlog filters drop (sorted); [Binlog filters](server-setup.md#binlog-filters) |

The counters are in memory only: they start at 0 at each start and are not written to
`lazy_wire_fire_status`, which keeps only what a restart needs.

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
  When the target has several endpoints, a refusal is judged only once every endpoint has
  answered: a transient failure on any endpoint makes the whole action transient, so the refused
  keys collect no strike. A key refused 5 times while others went through is dropped, named in
  `last_error`, and logged on the event target `lazywf::undelivered` ([the log event](../README.md#undelivered-keys)).
  When everything is refused, nothing is dropped (the template or the target is at fault).
  Transient failures (connection, timeout, 5xx, 429, Redis `LOADING`/`READONLY`/…) only wait.
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

To check that a purge reached the cache, rely on the target's `expect`, the tool's only check of
the response (by default a 2xx on http): an action counts in the rule's `actions_ok` in
`/status`, and its position passes the change, only when `expect` held on every endpoint. A purge
the cache did not accept shows in `actions_failed` and in `last_error` (transient answers are
retried; refusals are split down to the offending keys).

**Redis**, one command per key:

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars)
VALUES ('cache', 'redis', 'redis:6379/1', '{"password": "env:REDIS_PASSWORD", "prefix": "cust:"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template) VALUES ('customer_cache', 'cache', 'key',
 'SELECT id AS _key, name, email, IF(active, NULL, ''delete'') AS _op FROM customers WHERE id IN (:keys)',
 '{{#each docs}}{{#if (eq op "delete")}}DEL {{redis_quote target.vars.prefix key}}{{else}}SET {{redis_quote target.vars.prefix key}} {{redis_quote (json doc)}}{{/if}}{{/each}}');
```

**Redis Streams**, one `XADD` per key — a consumer reads the stream as a feed of changes:

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars)
VALUES ('stream', 'redis', 'redis:6379/1', '{"password": "env:REDIS_PASSWORD"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES ('orders_changes', 'stream', 'key',
 'SELECT id AS _key, status, total, NULL AS _op FROM orders WHERE id IN (:keys)',
 '{{#each docs}}XADD orders:changes * key {{redis_quote key}} op {{op}} doc {{redis_quote (json doc)}}{{/each}}', 100);
```

A trigger on `orders` marks `{{new.id}}`; a deleted order reaches the consumer as `op = delete`
(because the select carries `NULL AS _op`). Each entry has the fields `key`, `op` and `doc`. The
reply to `XADD` is the new entry id, which `expect` would see in `text` (`body` holds it as a JSON
string); here `expect` is empty, which confirms any reply that is not an error.

The delivery contract is latest state per key, at-least-once: the select runs at flush time, so
the last entry for a key is its current state; and a consumer may see an entry twice — when the
reply is lost after the server stored the `XADD`, the key stays pending and is re-sent. Within
one rule, entries for one key come in change order (the rule's single lane); entries for
different keys in the same flush have no defined order.

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

**Messages to an SQS queue**, one entry per key (a consumer applies each message as the key's
latest state):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, vars) VALUES ('queue', 'sqs',
 'aws-sqs://eu-west-1/123456789012/orders',
 'JSON_EXTRACT(body, ''$.MessageId'') IS NOT NULL',
 '{"access_key_ref": "env:ORDERS_AK", "secret_key_ref": "env:ORDERS_SK"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template) VALUES
 ('orders_queue', 'queue', 'batch', 10,
  'SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)',
  'POST / HTTP/1.1
X-Amz-Target: AmazonSQS.SendMessageBatch
Content-Type: application/x-amz-json-1.0

{"QueueUrl": {{json endpoint.url}}, "Entries": [{{#each docs}}{"Id": "{{@index}}", "MessageBody": {{json (json doc)}}}{{#unless @last}}, {{/unless}}{{/each}}]}');
```

A trigger on `customers` marks `{{new.id}}`. Up to 10 keys go out as one `SendMessageBatch`, and
the answer's entries are checked one by one: `expect` runs per entry with the entry's own JSON as
`body` (here it confirms the `MessageId` SQS returns for the message). For a FIFO queue (a queue
name ending in `.fifo`, `aws-sqs://eu-west-1/123456789012/orders.fifo`) the select adds a
`version` column, one value per state you want delivered (`CONCAT(c.id,
'#', c.rev) AS version` here), and the entry carries `"MessageGroupId": {{json key}}` and
`"MessageDeduplicationId": {{json doc.version}}`.

**Records to a Kinesis stream**, one per key (a consumer applies each record as the key's latest
state):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, vars) VALUES ('stream', 'kinesis',
 'aws-kinesis://eu-west-1/customer-changes',
 'JSON_EXTRACT(body, ''$.SequenceNumber'') IS NOT NULL',
 '{"access_key_ref": "env:AWS_ACCESS_KEY_ID", "secret_key_ref": "env:AWS_SECRET_ACCESS_KEY"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms)
 VALUES ('stream', 'stream', 'batch', 500,
  'SELECT c.id AS _key, c.id, c.name, IF(c.active, NULL, ''delete'') AS _op
     FROM customers c WHERE c.id IN (:keys)',
  'POST / HTTP/1.1
X-Amz-Target: Kinesis_20131202.PutRecords
Content-Type: application/x-amz-json-1.1

{"StreamName": {{json endpoint.stream}}, "Records": [{{#each docs}}{"Data": "{{#if (eq op "delete")}}{{base64 ''{"op":"delete"}''}}{{else}}{{base64 (json doc)}}{{/if}}", "PartitionKey": {{json key}}}{{#unless @last}}, {{/unless}}{{/each}}]}',
  100);
```

A trigger on `customers` marks `{{new.id}}`. Up to 500 keys go out as one `PutRecords`, and the
answer's records are checked one by one: `expect` runs per record with the record's own JSON as
`body` (here it confirms the `SequenceNumber` Kinesis returns for the stored record). `Data` is
the document in base64 (the `base64` helper); a deleted key's record carries `{"op":"delete"}`,
and the reader keys on `PartitionKey`. The endpoint names the stream and the region
(`aws-kinesis://[<region>/]<stream-name>`); without a region in it, one must come from the AWS
default settings. An emulator or a VPC endpoint is given with `?endpoint=`, e.g.
`aws-kinesis://eu-west-1/customer-changes?endpoint=http://kinesis-mock:4568`.

**Messages to an AMQP broker**, one publish per key (a consumer keeps the newest message per
key):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars) VALUES ('bus', 'amqp',
 'amqp://broker:5672/', '{"username": "lwf", "password": "env:BUS_PASSWORD"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms)
 VALUES ('orders', 'bus', 'key',
  'SELECT id AS _key, sku, qty, NULL AS _op FROM orders WHERE id IN (:keys)',
  '{{#each docs}}PUBLISH /orders/order.changed
Message-Id: {{key}}

{"key":{{json key}},"op":{{json op}},"doc":{{json doc}}}{{/each}}',
  100);
```

A trigger on `orders` marks `{{new.id}}`. Each changed key is one message: exchange `orders`,
routing key `order.changed`, `Message-Id` the key, and the envelope `{"key","op","doc"}` as the
JSON body. A deleted row arrives with `"op": "delete"`, an upsert with `null`. The broker's
confirm decides: `expect` is empty, so an ack confirms the key; a `basic.return` (312, no queue
is bound to the routing key) refuses the key on its own; a nack is retried with backoff. The
consumer binds a durable queue to the `orders` exchange with the routing key `order.changed`.

**Invoking a Lambda function**, one invocation per key (the function applies each payload as the
key's latest state):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars) VALUES
 ('order-sync', 'lambda', 'aws-lambda://eu-west-1/order-sync:live',
  '{"access_key_ref": "env:AWS_ACCESS_KEY_ID", "secret_key_ref": "env:AWS_SECRET_ACCESS_KEY"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES
 ('orders', 'order-sync', 'key',
  'SELECT c.id AS _key, c.id, c.name FROM customers c WHERE c.id IN (:keys)',
  '{"orders": [{{#each docs}}{"key": {{json key}}, "doc": {{json doc}}}{{#unless @last}}, {{/unless}}{{/each}}]}',
  100);
```

A trigger on `customers` marks `{{new.id}}`. The template is the Invoke payload only (here: the
key and its document); the tool writes and signs the request, so the function is invoked for
each key on the qualifier `live`. With `fire_per = batch` one invocation carries the whole batch
(up to the 6 MB payload limit; 1 MB for `?invocation=event`). `expect` judges the result (here
`JSON_EXTRACT(body, '$.id') IS NOT NULL`); in a batch, the result's `batchItemFailures` lists
the keys that retry, and the others are met. An event invocation is met on its 202 (no
`expect`): the function must be idempotent.

**Messages to a Pub/Sub topic**, one publish per batch (a subscriber keeps the newest message
per key):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, vars) VALUES
 ('orders-topic', 'pubsub', 'gc-pubsub://lwf-test/orders', '{"ordering": true}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, selects, template, debounce_ms)
 VALUES ('orders', 'orders-topic', 'batch', 1000,
  'SELECT c.id AS _key, c.id, c.name, NULL AS _op FROM customers c WHERE c.id IN (:keys)',
  'POST /v1/projects/{{endpoint.project}}/topics/{{endpoint.topic}}:publish HTTP/1.1
Content-Type: application/json

{"messages": [{{#each docs}}{"data": {{json (base64 (json doc))}}, "attributes": {"key": {{json key}}, "op": {{json op}}}{{#if @root.target.vars.ordering}}, "orderingKey": {{json key}}{{/if}}}{{#unless @last}}, {{/unless}}{{/each}}]}',
  100);
```

A trigger on `customers` marks `{{new.id}}`. One publish carries up to 1000 messages: `data` is
the document's JSON in base64, `attributes` the `key` and the `op` (a hard-deleted row arrives
with `"op": "delete"`, the select's `_op` column making keys no select returns deletions), and
`orderingKey` the key (here, `vars.ordering`), so a subscriber with message ordering on receives
each key's messages in publish order. For the emulator, `?endpoint=` in the endpoint names its
address (with no `credentials_ref`, no credentials are sent).

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

The index itself must exist: create it and its mapping before the first action — lazywf does not
create it, and a search engine that auto-creates indexes would otherwise do so with a dynamic
mapping. When a target refuses actions instead (auto-creation off), the rule's position holds,
the keys are retried after backoff, and the error is in `last_error`.

## Notes

* Set `binlog_row_metadata=FULL` (MySQL 8.0+, MariaDB 10.5+) to name row images exactly while a DDL is in flight;
  otherwise names come from `information_schema` and a column-count mismatch resets the rules of
  that table.
* A DDL that changes values without row events (`ALTER TABLE … ADD COLUMN … DEFAULT …` read by a
  select) is not detected: request a reset (`reset_requested = 1`).
* Tables replaced by a swap (`RENAME TABLE new TO watched`) reset the rules watching them, since
  their rows changed without row events; avoid watching tables that are rebuilt this way often.
  The exception is an online schema change (gh-ost, pt-online-schema-change): when the table
  swapped in was created `LIKE` the watched one while lazywf was streaming, and the same `RENAME`
  moves the watched one away, it holds the same rows (filled through row events), so the rules
  keep their continuity. If you rebuild a table with other rows that way, set
  `reset_requested = 1`. After a lazywf restart during the copy, the swap resets as before.
