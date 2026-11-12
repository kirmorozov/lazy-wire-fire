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
| `kind` | `http` (also `https`), `http.basic` (`vars.username`, `vars.password`), `http.bearer` (`vars.token`), `http.header` (`vars.header`, `vars.value`) (these four can sign their requests: [Webhook signing](#webhook-signing)), `http.s3` (SigV4, below), `sqs` (SigV4, an Amazon
SQS queue, below), `kinesis` (SigV4, an Amazon Kinesis stream, below), `lambda` (SigV4, an Amazon
Lambda function, below), `eventbridge` (SigV4, an Amazon EventBridge event bus, below), `sns`
(SigV4, an Amazon SNS topic, below), `amqp` (an AMQP 0-9-1
broker with publisher confirms, below), `smtp` (an SMTP server, one mail per action, below), `pubsub` (a Google Cloud Pub/Sub topic, below), `kafka` (a broker of the Kafka protocol — Apache Kafka, Redpanda, MSK, Event Hubs — below), `rabbitmq-stream` (a RabbitMQ stream or super stream over the native stream protocol, below), `grpc` (a server of the gRPC protocol, reached over one long-lived bidirectional stream, below), `servicebus` (an Azure Service Bus queue or topic over AMQP 1.0, below), `redis` (`vars.password`, optional `vars.username` for ACLs), `mysql` (also `mariadb`) / `pgsql` (also `postgres`, `postgresql`) (`vars.username`, `vars.password`; [SQL targets](#sql-targets)) |
| `endpoints` | list. http: base URLs `http(s)://host:port[/prefix]` (the prefix is put before every path), or, for the four plain http kinds only, one `env:` / `file:` reference whose value is the whole URL ([secret http endpoints](#secret-http-endpoints)). redis: `host:port/db` (or `redis://…`, `rediss://…` for TLS). mysql / pgsql: `host:port/database` (the scheme, `mysql://` / `postgres://`, … is optional). `http.s3`: optional, the service URL; derived from `vars` when empty. `sqs`: exactly one: `aws-sqs://[<region>/]<account-id>/<queue-name>`. `kinesis`: exactly one: `aws-kinesis://[<region>/]<stream-name>`. `lambda`: exactly one: `aws-lambda://[<region>/]<function-name>[:<qualifier>]` (the `aws-` URL form, `?endpoint=`, `?invocation=` and the region: below). `eventbridge`: exactly one: `aws-eventbridge://[<region>/]<event-bus-name-or-arn>` (the `aws-` URL form and the region: below). `sns`: exactly one: `aws-sns://[<region>/]<account-id>/<topic-name>` (the `aws-` URL form and the region: below). `amqp`: `amqp(s)://host[:port][/vhost]` (no credentials in the URL). `smtp`: `smtp(s)://host[:port]` (no credentials in the URL, no path or query; port 587 for `smtp://`, 465 for `smtps://`). `pubsub`: exactly one: `gc-pubsub://<project>/<topic>` (`?endpoint=` for the emulator: below). `kafka`: exactly one: `kafka(s)://<broker>[:port][,<broker>[:port]…]/<topic>` (no credentials in the URL: below). `rabbitmq-stream`: exactly one: `rabbitmq-stream://<host>[:port]/[<vhost>/]<stream>` or `rabbitmq-stream+tls://…` (ports 5552 / 5551; no credentials in the URL: below). `servicebus`: exactly one: `sb://<namespace>.servicebus.windows.net/<queue-or-topic>` (no credentials in the URL, TLS on port 5671, the local-broker form: below). `grpc`: exactly one: `grpc(s)://<host>[:port]/<package.Service>/<Method>` (default ports 80 / 443; no credentials, no query string, no fragment and no bracketed IPv6 literal: below). An action goes to every endpoint |
| `tls` | empty (TLS for `https://` / `rediss://` / `amqps://` / `smtps://` / `kafkas://`, verified), `disabled`, `required` (no certificate checks), `verify-ca`, `verify-full`, or `{"mode": "verify-full", "ca": "/path/ca.pem", "server_name": "…"}` (`amqp` takes only `verify-full` / empty / `disabled` on `amqp://`: [below](#amqp-targets); `smtp` takes them all but refuses `disabled` on `smtps://`: [below](#smtp-targets); `kafka` takes them all but refuses `disabled` on `kafkas://` and any `tls.server_name`: [below](#kafka-targets); `rabbitmq-stream` takes only `verify-full` with exactly one `tls.ca` file: [below](#rabbitmq-stream-targets); `servicebus` takes them all but refuses `disabled` on a `servicebus.windows.net` endpoint and any `tls.server_name`: [below](#service-bus-targets); `grpc` takes them all but refuses `disabled` on a `grpcs://` endpoint: [below](#grpc-targets)) |
| `timeout_ms` | per action and endpoint, default 10000 |
| `expect` | success criterion: a SQL condition over the response, below |
| `vars` | JSON object: credentials for `kind` and anything templates need (`target.vars`). Credentials never reach templates |

A credential in `vars` is a literal or a reference: `env:NAME`, or `file:/path` (trailing newline
dropped, read on every use so rotated secrets apply). References are recommended when other users
can read the database. For the four plain http kinds an `endpoints` entry may be a reference too,
and there it is the **whole** URL — for an incoming webhook the URL below the host *is* the
credential: [secret http endpoints](#secret-http-endpoints). Every other kind refuses a reference
endpoint at load: it takes its address in plain text and keeps its credentials in `vars`.

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

**`eventbridge`**: the endpoint is the event bus, as
`aws-eventbridge://[<region>/]<event-bus-name-or-arn>` (the bus a name or its ARN, which names
its own region; the `aws-` URL form: below). One action is one `PutEvents` request to the service
root (`POST /`): the template names the action (`X-Amz-Target: AWSEvents.PutEvents`) and lists
the entries in the body, one per key. Credentials as `sqs`, `kinesis` and `lambda`:
[AWS credentials](#aws-credentials). The tool signs the request SigV4 for service `events`. The
answer per entry: [below](#eventbridge-targets).

**`sns`**: the endpoint is the topic, as
`aws-sns://[<region>/]<account-id>/<topic-name>` (the `aws-` URL form: below). One action is one
`PublishBatch` request to the service root (`POST /`, the query protocol: a form-encoded
`Action=PublishBatch` body): the template renders the whole request and names the topic by its
ARN (`{{json endpoint.topic_arn}}`). Credentials as `sqs`, `kinesis`, `lambda` and
`eventbridge`: [AWS credentials](#aws-credentials). The tool signs the request SigV4 for service
`sns`. The answer per entry: [below](#sns-targets).

**`pubsub`**: the endpoint is the topic, as `gc-pubsub://<project>/<topic>` (`?endpoint=` for
the emulator, below). Credentials: `vars.credentials_ref`, a reference to a service-account key
(never a literal), else [Application Default Credentials](#pubsub-targets). One action is one
`topics.publish` request, rendered by the template: [below](#pubsub-targets).

**`amqp`**: the endpoint is `amqp(s)://host[:port][/vhost]` (default ports 5672 / 5671; the
vhost is the path, `/` the default vhost), without credentials: `vars.username` and
`vars.password`, references or literals (as `redis` takes them), resolved on each connect. One
message per action with a publisher confirm: [below](#amqp-targets).

**`smtp`**: the endpoint is `smtp(s)://host[:port]` (default ports 587 / 465), without
credentials: `vars.username` and `vars.password`, references or literals (as `redis` and `amqp`
take them), resolved on each connect, or no credentials at all for an anonymous session. One
mail per action, the server's reply the answer: [below](#smtp-targets).

**`kafka`**: the endpoint is one broker set and the topic it writes to, as
`kafka(s)://<broker>[:port][,<broker>[:port]…]/<topic>` (default ports 9092 / 9093; `kafkas://`
is TLS), without credentials: `vars.sasl` (`plain`, `scram-sha-256` or `scram-sha-512`) with
`vars.username` (a reference or a literal) and `vars.password` (a reference, never a literal),
or no SASL at all for an anonymous connection. One action is one produce of a batch of records,
record *i* answered by key *i*: [below](#kafka-targets).

**`rabbitmq-stream`**: the endpoint is the RabbitMQ stream or super stream its batches go to,
as `rabbitmq-stream://<host>[:port]/[<vhost>/]<stream>` or `rabbitmq-stream+tls://…` for TLS
(default ports 5552 / 5551), without credentials: `vars.username` (a reference or a literal,
as `amqp` takes it) and `vars.password` (a reference, never a literal) are the SASL PLAIN
account the stream protocol always asks for. One action is one publish batch of records,
record *i* answered by key *i*: [below](#rabbitmq-stream-targets).

**`servicebus`**: the endpoint is the queue or topic, as
`sb://<namespace>.servicebus.windows.net/<queue-or-topic>` (exactly one, AMQP 1.0 over TLS on
port 5671, no credentials in the URL). Credentials are one SAS rule: `vars.key_name` with
`vars.key` (a reference, never a literal), put to the connection's `$cbs` node; Microsoft Entra
ID is not supported. One action is one batch of messages, a disposition awaited for every
message, message *i* answered by key *i*: [below](#service-bus-targets).

**`grpc`**: the endpoint is the method to call, as
`grpc(s)://<host>[:port]/<package.Service>/<Method>` (exactly one, default ports 80 / 443, no
credentials in the URL). The schema is a protobuf
descriptor set the target names: `vars.descriptor_set` (a `file:` reference) with `vars.item_type`
(the message a document becomes), both required. One action is one `lazywf.v1.Envelope` of items
written onto the target's one long-lived bidirectional stream, the server's acknowledgements
answering it item by item: [below](#grpc-targets).

**The `aws-sqs://`, `aws-kinesis://`, `aws-lambda://`, `aws-eventbridge://` and `aws-sns://` URL
form** (the `endpoints` of the kinds `sqs`, `kinesis`, `lambda`, `eventbridge` and `sns`, and of
no other kind):

* `sqs`: `aws-sqs://[<region>/]<account-id>/<queue-name>` — the account id is twelve digits, the
  queue name 1 to 80 letters, digits, `-` or `_`, optionally ending in `.fifo` (a FIFO queue).
* `kinesis`: `aws-kinesis://[<region>/]<stream-name>` — the stream name is 1 to 128 letters,
  digits, `-`, `_` or `.`.
* `lambda`: `aws-lambda://[<region>/]<function-name>[:<qualifier>]` — the function name is 1 to
  64 letters, digits, `-`, `_` or `.` (a function ARN does not fit the form and is refused); the
  qualifier is a version or an alias (letters, digits, `-`, `_`), `$LATEST` or
  `$LATEST.PUBLISHED`. `?invocation=sync` (the default: the answer is the function's result) or
  `?invocation=event` (queued; answered 202 before the function runs), once each.
* `eventbridge`: `aws-eventbridge://[<region>/]<event-bus>` — the event bus is a name (1 to 256
  letters, digits, `-`, `_` or `.`) or its ARN
  (`arn:<partition>:events:<region>:<account-id>:event-bus/<name>`, the partition `aws`,
  `aws-cn` or `aws-us-gov`, the account id twelve digits). An ARN names its own region and is
  written alone: a region in front of it is refused, naming the reason.
* `sns`: `aws-sns://[<region>/]<account-id>/<topic-name>` — the account id is twelve digits, the
  topic name 1 to 256 letters, digits, `-` or `_`, optionally ending in `.fifo` (a FIFO topic).
* The region comes from the URL when the URL gives one; otherwise from the AWS default settings:
  `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `region` in the shared config file's profile
  (`AWS_CONFIG_FILE`, default `~/.aws/config`; the profile is `AWS_PROFILE`, default
  `default`). When none of these gives a region, the target does not load (the error names both
  ways to set one). `vars.region` is refused by the five kinds (`vars.stream_name` and
  `vars.stream_arn` by `kinesis`, `vars.event_bus_name` and `vars.event_bus_arn` by
  `eventbridge`, `vars.account_id`, `vars.topic_name` and `vars.topic_arn` by `sns`); `http.s3`
  is unchanged and still takes `vars.region`.
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

`servicebus`'s `sb://` endpoint follows the same rules with no query parameter at all: no
userinfo, no query string and no fragment, and a `sb://` URL is for `kind servicebus` only (and
`servicebus` takes only that scheme).

`grpc`'s `grpc(s)://` endpoint follows them too — no userinfo, no query string, no fragment, no
bracketed IPv6 literal — and a `grpc://` or `grpcs://` URL is for `kind grpc` only (and `grpc`
takes only those two schemes); each refusal names the expected form.

### Secret http endpoints

For the four plain http kinds (`http`, `http.basic`, `http.bearer`, `http.header`) an entry of
`endpoints` may itself be a reference — `env:NAME` or `file:/path` — whose value is the **whole**
http(s) URL. It is for an incoming webhook whose path or query *is* the credential (Slack,
Microsoft Teams Workflows, Google Chat, Discord), where keeping the origin in `endpoints` and
putting the secret path in the template leaves the credential in a plain column that anyone who
can read the rule table can see. No other kind takes a reference endpoint: it names its address
in plain text and keeps its secrets in `vars`, so a reference there would only hide the address.

* **The value is a full URL**: `http://host/…` or `https://host/…`. Userinfo
  (`https://user:pass@host/x`) is a load error — a credential belongs in the target's `vars`, and
  here it would be a second secret; so are a fragment, a space or a control character in the path
  or query, and a host or port that does not parse. A refusal names the reference and never the
  value it holds.
* **Confined like any other secret reference**: the file must resolve (symlinks followed) inside
  `LAZYWF_SECRET_DIRS` (default `/run/secrets:/var/run/secrets`) and `env:` may not name a
  `LAZYWF_*` variable ([Security](#security)).
* **Resolved on every request**, so rewriting the file *is* the rotation: the next request uses
  the new URL, with no reload and no restart, and the value is not kept between requests (the
  keep-alive pool is keyed by the origin, never by the URL).
* **Nothing below the origin gets out**: the log at every `RUST_LOG` level, `/status`, a rule's
  `last_error` and the template's `endpoint.*` show the scheme, the host, the port and an
  ellipsis — `https://hooks.example.test/…`. `endpoint.url` is that string; `endpoint.host` and
  `endpoint.port` are the origin's. The path and the query reach only the bytes of the request
  line, written as they are sent.
* **`LAZYWF_ALLOWED_HOSTS` checks the resolved host** of every request, not the reference
  ([Security](#security)): a rewritten file cannot move a target to a host the operator did not
  allow, and such a host is refused, not waited out.
* **How the request line is made**: `POST /` asks for the endpoint itself, so the URL is called
  exactly as given, with no stray trailing slash; any other target goes *below* the endpoint's
  path (`/media/1.png` on `https://hooks.example.test/hooks/a` is requested as
  `/hooks/a/media/1.png`); the endpoint's query comes last, joined with `&` to a query the
  template wrote; `*` stays `*`. The endpoint's path is no prefix: a base URL's `/prefix` is put
  in front of every path, whereas here the path belongs to the request line.

So the recommended form of an incoming webhook is `endpoints = 'file:/run/secrets/slack_webhook'`,
the file holding `https://hooks.example.test/services/T00000000/B00000000/PLACEHOLDER-TOKEN`, and
`POST / HTTP/1.1` in the template ([the Slack recipe](recipes.md#slack-post-to-a-channel),
[Teams](recipes-apps.md#microsoft-teams-post-to-a-channel) and
[Google Chat](recipes-apps.md#google-chat-post-to-a-space), and among the automation hubs
[Zapier](recipes-apps.md#zapier-trigger-a-zap-from-a-catch-hook) and
[Power Automate](recipes-apps.md#microsoft-power-automate-run-an-instant-cloud-flow)).

### AWS credentials

The kinds `http.s3`, `sqs`, `kinesis`, `lambda`, `eventbridge` and `sns` sign their requests
SigV4 with one of two sources, chosen per target:

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

| column | http | redis | amqp | smtp | lambda | pubsub | mysql / pgsql |
|---|---|---|---|---|---|---|---|
| `status` | status code | NULL | 200 on an ack, 312 on a return | the reply code | 200 on a sync answer, 202 on an event | status code | NULL |
| `headers` | JSON object, names in lower case (repeated headers joined with `, `) | NULL | NULL | NULL | JSON object | JSON object | NULL |
| `body` | the body when it is JSON, else NULL | the reply as JSON | the return as JSON | NULL | the function's result, when it is JSON | the publish answer, when it is JSON | NULL |
| `text` | the body as text | a string or integer reply as text, else NULL | `ack`, `return` or `nack` | the reply as text | the result as text | the answer as text | NULL |
| `affected_rows` | NULL | NULL | NULL | NULL | NULL | NULL | rows the statement changed |

* Empty: http needs a 2xx status, redis any reply but an error, amqp an ack, smtp an accepted
  (2xx) reply, lambda a 200 without a function error (an event is met on its 202), pubsub a 2xx
  whose `messageIds` reach every message, a SQL target a statement that ran. A condition that
  does not name `status` keeps the 2xx rule; one that does replaces it.
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
* For `smtp`, `expect` is a condition over the server's reply ([the rows](#smtp-targets)): the
  reply code as `status`, the reply as `text`. Only an accepted message (a 2xx after the data)
  is judged, and an unmet condition refuses it on its own; a 4xx is transient and a 5xx is
  refused whatever the condition says.
* For `lambda`, a synchronous invocation's `expect` runs over the result (`body` the result,
  `status` 200); a function error (`X-Amz-Function-Error`) refuses the invocation whatever the
  condition says, and an event invocation is met on its 202 with no `expect`
  ([below](#lambda-targets)).
* For `pubsub`, `expect` runs over the whole publish answer (`status`, `headers`, `body` the
  `{"messageIds": […]}` object); the outcome is whole-action ([below](#pubsub-targets)).
* For `kafka`, the produce answer maps per partition onto that partition's records, so `expect`
  runs once per record: `body` is the record's `{"topic": …, "partition": …, "offset": …}`
  object, the other columns NULL. A record whose `expect` is not met is refused on its own.
* For `rabbitmq-stream`, the publish answer of the batch is per record, so `expect` runs once
  per record: `body` is the record's `{"stream": …, "publishing_id": …}` object (the partition
  stream of a super stream), `status` 200, the other columns NULL. A record whose `expect` is
  not met is refused on its own.
* For `grpc`, `expect` runs over each acknowledged item: `status` 200, `text` `ok` and `body` the
  acknowledgement's `{"seq", "hash", "status", "reason"}` JSON. An item whose `expect` is not met
  refuses its own key and only that key; a `REFUSED` or `RETRY` acknowledgement never asks
  `expect` ([below](#grpc-targets)).
* For `eventbridge`, the answer of one `PutEvents` gives its entries in request order, so
  `expect` runs once per entry: `status` is the request's HTTP status and `body` is the entry's
  own JSON (its `EventId`, or its `ErrorCode` and `ErrorMessage`). An entry whose `expect` is
  not met is refused on its own.
* For `sns`, the answer of one `PublishBatch` names its entries by their `Id` (what the
  template wrote there: the key's index in the batch, from 0), so `expect` runs once per
  entry: `status` is the request's HTTP status and `body` is the entry's own JSON (its
  `MessageId`, or its `Code`, `Message` and `SenderFault`). An entry whose `expect` is not
  met is refused on its own.
* On `http.s3`, `sqs`, `kinesis`, `lambda`, `eventbridge` and `sns`, an error answer is
  transient when its code (the `x-amzn-errortype` header, else the body's `__type`, else — on
  `sns`, whose answer is the query protocol's XML — its error document's `<Code>`) is
  `ThrottlingException`, `RequestThrottled`, `ProvisionedThroughputExceededException`,
  `ServiceUnavailable` or `InternalFailure` — plus, on `sns`, `InternalError` and
  `KMSThrottling` — whatever the status (AWS JSON errors come as HTTP 400); every other error
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
| an SMTP mail (the server accepted it) | `status = 250` |
| a Lambda invoke (the result names the stored id) | `JSON_EXTRACT(body, '$.id') IS NOT NULL` |
| a Pub/Sub publish (at least one message id) | `JSON_LENGTH(JSON_EXTRACT(body, '$.messageIds')) > 0` |
| a Kafka produce (per record, the offset answered) | `JSON_EXTRACT(body, '$.offset') IS NOT NULL` |
| a RabbitMQ stream publish (per record, the publish stored) | `JSON_EXTRACT(body, '$.publishing_id') IS NOT NULL` |
| a gRPC item (per item, the server took it) | `status = 200 AND JSON_UNQUOTE(JSON_EXTRACT(body, '$.status')) = 'OK'` |
| an EventBridge entry (per entry, the event stored) | `JSON_EXTRACT(body, '$.EventId') IS NOT NULL` |
| an SNS entry (per entry, the message stored) | `JSON_EXTRACT(body, '$.MessageId') IS NOT NULL` |
| a webhook where a 404 is fine | `status BETWEEN 200 AND 299 OR status = 404` |
| an API that answers 200 with a state | `JSON_UNQUOTE(JSON_EXTRACT(body, '$.state')) = 'stored'` |
| a header | `JSON_UNQUOTE(JSON_EXTRACT(headers, '$."x-ack"')) = 'yes'` |
| redis `SET` | `text = 'OK'` |
| SQL targets | `affected_rows > 0` |

### Webhook signing

The four plain http kinds (`http`, `http.basic`, `http.bearer`, `http.header`) can sign the
requests they send, so the receiver can tell they came from lazywf and that nothing in them
changed. Signing is settings in the target's `vars`, applied after the template is rendered and
after the kind's authentication headers, over the exact body the request sends — the bytes the
receiver reads (a stray-cleanup listing of these kinds is signed too):

| `vars` | meaning |
|---|---|
| `vars.sign` | `standard` or `hmac`: the scheme, below. The setting that turns signing on; a signing setting without it is a load error naming the setting |
| `vars.sign_secret_ref` | the signing secret: a reference only (`env:NAME` or `file:/path`), never a literal, resolved on every request like the other credentials |
| `vars.sign_header` | `hmac` only: the header that carries the signature, `X-Signature-256` by default |
| `vars.sign_encoding` | `hmac` only: `hex` (the default) or `base64`, the form of the digest |
| `vars.sign_prefix` | `hmac` only: the text before the digest, `sha256=` by default |

* **`standard`** — [Standard Webhooks](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md),
  its symmetric scheme: the tool adds `Webhook-Id` (unique per request: the target's name and a
  counter), `Webhook-Timestamp` (Unix seconds) and `Webhook-Signature: v1,<base64>`, the base64
  HMAC-SHA256 of `<id>.<timestamp>.<body>`. The secret is the spec's `whsec_` form and the key is
  its base64 part; a secret without the prefix, that is not base64, or whose key is empty fails
  when the request is signed, naming the setting (the secret is a reference, so its content is
  known only then).
* **`hmac`** — one header (`vars.sign_header`) with `vars.sign_prefix` and the HMAC-SHA256 of the
  body in `vars.sign_encoding`, keyed by the secret's own bytes: with the defaults the
  GitHub-style `X-Signature-256: sha256=<hex>`.
* Everything else is checked at load and names the setting: an unknown scheme, a missing or
  literal `sign_secret_ref`, a signing setting without `vars.sign`, a header name that is not a
  header name, an encoding that is neither `hex` nor `base64`, a `sign_prefix` that is not a
  string, and the three `hmac`-only settings on `standard` (that scheme fixes its headers).
* A signing setting on any other kind — `http.s3`, `sqs`, `kinesis`, `lambda`, `eventbridge`,
  `sns`, `pubsub`, `kafka`, `rabbitmq-stream`, `grpc`, `amqp`, `smtp`, `redis`, `mysql`, `pgsql` —
  is a load error too:
  `http.s3` signs SigV4 and the queue kinds their own protocols, so the setting would point at
  nothing.
* The secret is a credential of the kind: it stays out of the template context (`target.vars`
  holds the scheme and the header form, not the secret) and out of the logs
  ([security](#security)).

### SQL targets

`mysql` and `pgsql`: the template renders one SQL statement, run on the target database. One
action is one statement, enforced twice: lazywf refuses a top-level `;` followed by more (the
error names the statement count) and an executable comment (`/*! … */`, `/*!NNNNN … */`,
`/*M! … */` — the server would run its body as SQL); optimizer hints (`/*+ … */`) stay allowed.
The server enforces it too: MySQL and MariaDB run the statement as a prepared statement, which
the server refuses to run as more than one, and PostgreSQL runs it through the extended protocol,
which refuses batches as well. A server refusal is not transient: it is a refusal in `last_error`
(MySQL's error 1295 for a kind that cannot be prepared, 1064 for a second statement after the
`;`).

On MySQL, the kinds that cannot be prepared cannot run through a `mysql` target:
`CREATE` / `DROP TRIGGER`, `CREATE` / `DROP PROCEDURE`, `BEGIN` / `START TRANSACTION`,
`SAVEPOINT`, `USE`, `LOCK TABLES` / `UNLOCK TABLES`, `SHOW WARNINGS`, `HANDLER`, `XA START`,
`LOAD DATA` and `PREPARE` / `EXECUTE` (on MariaDB, only `PREPARE` / `EXECUTE` is refused).
`COMMIT` and `ROLLBACK` still run, so a statement must not open a transaction on MySQL
([SQL syntax permitted in prepared statements](https://dev.mysql.com/doc/refman/8.0/en/sql-prepared-statements.html#prepared-statements-permitted)).

Each connect pins the session so quoting is sound: MySQL `SET NAMES utf8mb4` with backslash
escapes on, PostgreSQL UTF-8 with
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

### EventBridge targets

`eventbridge` sends to an Amazon EventBridge event bus: one `PutEvents` per action (AWS JSON
1.1, `X-Amz-Target: AWSEvents.PutEvents`, signed SigV4 for service `events`), one entry per key,
and the answer gives the entries back in request order — entry *i* of the answer is entry *i* of
the request, so the kind's answer is per item, not per action.

* **The endpoint.** `aws-eventbridge://[<region>/]<event-bus>`: the bus a name (1 to 256
  letters, digits, `-`, `_` or `.`) or its ARN, which names its own region; exactly one endpoint,
  and `?endpoint=` for emulators and VPC endpoints, as for `sqs` and `kinesis`. The region comes
  from the URL or the AWS default settings (above). `vars.region`, `vars.event_bus_name` and
  `vars.event_bus_arn` are refused: the region and the bus go in the endpoint. Credentials: as
  `sqs`, `kinesis` and `lambda` — [AWS credentials](#aws-credentials).
* **Batch limits.** With `fire_per = batch`, `batch_max_keys` is required and holds 1 to 10 (the
  most entries a `PutEvents` takes); a rule without it, or with more, does not load.
  `batch_max_bytes` defaults to 1048576 (1 MB, the most the entries of one request sum to, the
  JSON around them counted) and cannot be set higher. With `fire_per = key` each key is its own
  one-entry request, so no batch is sized.
* **The rendered request.** The template renders the whole request (as for `sqs` and `kinesis`):
  the `POST /` line with `X-Amz-Target: AWSEvents.PutEvents`, `Content-Type:
  application/x-amz-json-1.1`, and a `{"Entries": […]}` body — exactly one entry per key
  (`Entries[i]` is answered by position), each entry holding `Source`, `DetailType` and `Detail`
  as non-empty strings (PutEvents requires all three on every entry; `Detail` is the document's
  JSON in `{{json (json doc)}}`), the bus from the endpoint (`"EventBusName": {{json
  endpoint.bus}}`), and the optional `Resources`, `Time` and `TraceHeader`. The entries are
  checked before the request is signed and sent: a batch whose entries are wrong is a
  whole-request refusal, bisected down to the key that fails alone (as with `http`).
  `endpoint.bus` and `endpoint.region` name the bus and the region.
* **Outcomes.** An entry with an `EventId` was stored, and `expect` is evaluated for it: met,
  its key is confirmed and never sent again; not met, the entry is refused on its own. An entry
  whose `ErrorCode` is `ThrottlingException`, `ProvisionedThroughputExceededException`,
  `RequestThrottled`, `ServiceUnavailable` or `InternalFailure` is transient: its key waits and
  is sent again later with its newest document. Any other `ErrorCode` refuses that entry on its
  own (no bisection: the answer names it by position); five such refusals, while other keys went
  through, drop the key and name it in `last_error`. An entry the answer says nothing about
  (neither `EventId` nor `ErrorCode`) waits — only the entries that did not go through wait.
  A non-2xx answer follows the usual rules: 5xx, 408, 429 and the AWS throttling codes are
  transient, every other error is a refusal; an answer that is not JSON fails the whole action
  as transient.
* **Reset steps.** The kind only appends: an event that went out cannot be recalled. A
  `for_each: stray` step does not load ("a stray cleanup needs a target that can delete keys;
  kind eventbridge only appends"), and neither does a `once` step whose template deletes the bus
  (`DeleteEventBus`, which would take the bus and every rule on it; recognized by text, so a
  delete built from variables is not caught). Still allowed: `once` (an announcement entry),
  `for_each: snapshot` (every key is sent again; consumers must tolerate that) and
  `reset_requested`.
* **Contract.** The bus receives the latest state of each key, not a log of every change, and
  delivery is at-least-once: an entry can arrive twice (a retry after a lost answer; a snapshot
  reset). Consumers apply an entry as the key's current state and drop what they applied already.
  No order is guaranteed, within one key or across keys.

### SNS targets

`sns` publishes to an Amazon SNS topic: one `PublishBatch` per action (the query protocol: a
form-encoded `POST /` with `Action=PublishBatch&Version=2010-03-31`, signed SigV4 for service
`sns`), one entry per key, and the answer reports each entry by its `Id` — the kind's answer is
per item, not per action. AWS answers the JSON form of the operation too; the query protocol is
the one used and tested (the emulator the tests run against answers the JSON form with a 500).

* **The endpoint.** `aws-sns://[<region>/]<account-id>/<topic-name>`: the account id twelve
  digits, the topic name 1 to 256 letters, digits, `-` or `_`, optionally ending in `.fifo`
  (a FIFO topic); exactly one endpoint, and `?endpoint=` for emulators and VPC endpoints, as
  for `sqs`. The region comes from the URL or the AWS default settings (above). `vars.region`,
  `vars.account_id`, `vars.topic_name` and `vars.topic_arn` are refused: they go in the
  endpoint. Credentials: as `sqs`, `kinesis`, `lambda` and `eventbridge` —
  [AWS credentials](#aws-credentials).
* **Batch limits.** With `fire_per = batch`, `batch_max_keys` is required and holds 1 to 10
  (the most entries a `PublishBatch` takes); a rule without it, or with more, does not load.
  `batch_max_bytes` defaults to 262144 and cannot be set higher: that is the topic's
  `MaximumMessageSize` by default, and the same number bounds one message and all the
  messages of one request together. `vars.maximum_message_size` states the topic's value for
  the target, as kafka's `vars.message_max_bytes` states the broker's: 1 to 1048576 (1 MiB,
  the most AWS gives a topic), refused otherwise; a smaller value only tightens the check.
  With `fire_per = key` each key is its own one-entry request, so no batch is sized.
* **The rendered request.** The template renders the whole request (as for `sqs`, `kinesis`
  and `eventbridge`): the `POST /` line with `Content-Type: application/x-www-form-urlencoded`,
  and the form body — `Action=PublishBatch`, `Version=2010-03-31`, the topic
  (`TopicArn={{url_encode endpoint.topic_arn}}`), and one
  `PublishBatchRequestEntries.member.<n>.<Field>` group per key. Members are numbered from 1
  (`{{add @index 1}}`); each group holds `Id` (the key's index in the batch, from 0:
  `{{@index}}` — what the answer names the entry by) and `Message` (the document's JSON,
  `{{url_encode (json doc)}}`) as non-empty values, and on a FIFO topic the required
  `MessageGroupId`. The entries are checked before the request is signed and sent: a batch
  whose entries are wrong is a whole-request refusal, bisected down to the key that fails
  alone (as with `http`). `endpoint.topic_arn`, `endpoint.topic`, `endpoint.region` and
  `endpoint.fifo` name the topic.
* **Outcomes.** An entry in `Successful` was stored and `expect` is evaluated for it (`body`
  holds its `MessageId`, `status` is the request's HTTP status): met, its key is confirmed and
  never sent again; not met, the entry is refused on its own. An entry in `Failed` whose
  `SenderFault` is true is refused on its own (no bisection: the answer names it by its `Id`);
  five such refusals, while other keys went through, drop the key and name it in `last_error`.
  Any other `Failed` entry — one without `SenderFault` true — and an entry the answer does not
  name is transient: its key waits and is sent again later with its newest document. A
  non-2xx answer follows the usual rules, its error code read from the query protocol's error
  document too (the kind's transient codes are the common ones plus `InternalError` and
  `KMSThrottling`, the latter as HTTP 400).
* **FIFO topics.** A topic whose name ends in `.fifo` is FIFO: the rule's selects must return a
  `version` column (refused at load without it), which the template writes as the entry's
  `MessageDeduplicationId`; the key is the `MessageGroupId` (`{{json key}}`). As for a FIFO
  queue: the topic's deduplication drops an entry whose id it has already seen, so a version
  shared by two keys drops one of them, and a NULL version leaves the entry without an id,
  which the topic refuses on its own (and drops after five refusals).
* **Reset steps.** The kind only appends: a published message cannot be recalled. A
  `for_each: stray` step does not load ("a stray cleanup needs a target that can delete keys;
  kind sns only appends"), and neither does a `once` step whose template deletes the topic
  (`DeleteTopic`, which would take the topic and every subscription on it; recognized by text,
  so a delete built from variables is not caught). Still allowed: `once` (an announcement
  message), `for_each: snapshot` (every key is sent again; subscribers must tolerate that) and
  `reset_requested`.
* **Contract.** The topic receives the latest state of each key, not a log of every change,
  and delivery is at-least-once: an entry can arrive twice (a retry after a lost answer; a
  snapshot reset). Subscribers apply an entry as the key's current state and drop what they
  applied already. On a FIFO topic one key's messages keep their order (they share its
  `MessageGroupId`); across keys, and on a standard topic, no order is guaranteed.

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
* **Into a stream queue.** A queue declared with `x-queue-type: stream` and bound to the
  exchange and routing key the target publishes to takes this kind's messages as they are —
  the same envelope, the same publisher confirms, the same outcomes — onto RabbitMQ's
  append-only log. The integration suite does exactly this: the target is such a queue, every
  key arrives with its document (insert, update, delete), and after the broker is cut off and
  lazywf restarts over it the log holds every key again — at-least-once, since a re-send can
  store a message twice. A stream is read by consuming it, not with `basic.get`: a consumer
  with the `x-stream-offset` argument (`first` starts at the beginning of the log), a prefetch
  and acknowledgements (the acks are the credit that advances the consumer's offset) reads the
  log without removing anything from it. For the native stream protocol — throughput, super
  streams — the kind is another one: [RabbitMQ stream targets](#rabbitmq-stream-targets).

### SMTP targets

`smtp` sends to an SMTP server: one mail per action, written and addressed by the template, over
one connection per endpoint that stays open while the server accepts. The server's reply is the
answer: an accepted message is judged by `expect`, a 4xx is transient, a 5xx is refused on its
own.

* **One message per action.** `fire_per` must be `key`: one action is one mail, so there is no
  batch to size. `fire_per = batch` is refused at rule load ("kind smtp sends one message per
  action: set `fire_per = key`").
* **The rendered message.** The header lines, a blank line, the body:

  ```text
  From: sender@example.com
  To: a@example.com, b@example.com
  Cc: c@example.com
  Subject: order 42 changed
  Content-Type: text/plain; charset=utf-8

  {"key":42,"op":null,"doc":{"sku":"a","qty":3}}
  ```

  The headers are `From`, `To`, `Cc`, `Subject`, `Reply-To` and `Content-Type`, each exactly
  once: an unknown or repeated header, a folded (continuation) line, or a control character in a
  header line is refused, as http refuses it in a request line — data cannot start an SMTP
  conversation. Addresses are one line each, `user@domain`, comma-separated; `Content-Type` is
  `text/plain` or `text/html`, its optional `charset` parameter must be `utf-8`. The message
  goes on the wire as it was rendered (its line endings turned into CRLF): nothing of it is
  rebuilt, so the server sees what the template wrote. A blank render sends nothing.
* **The envelope.** The sender is `From` and the recipients are `To` plus `Cc`, at most
  `vars.recipients` of them (a number, 1 or more; 50 by default). A `Bcc` has no envelope to go
  into, so the header is refused.
* **The reply.** `expect` sees one row: the reply code as `status`, the reply as `text`
  ([above](#targets)). With `expect` empty, a 2xx after the message is met. A 4xx (421 and 451
  among them: throttling, greylisting) is transient: the key waits and is sent again with
  backoff. A 5xx refuses the key on its own (strikes; five of them while other keys go through
  drop the key and name it in `last_error`). A connection, TLS or STARTTLS failure is transient
  too: the mail may or may not have reached the server, so it is sent again (at-least-once).
* **One connection per endpoint.** A send holds the endpoint's connection until its reply is
  read; the connection is kept while the server accepts and dropped on every other answer (a
  4xx, a 5xx, a broken or a timed-out session), so a refused mail costs one reconnect and
  accepted mails reuse the session. The whole send is bounded by the target's `timeout_ms`;
  different endpoints of the target run in parallel.
* **Credentials.** The session authenticates with `vars.username` and `vars.password`, which go
  together or not at all and are resolved on every connect. Each is a literal or a reference to
  where the value really lives — `env:NAME` for an environment variable, `file:/path` for a file
  under `LAZYWF_SECRET_DIRS` — and a reference is what to use when others can read the target
  table ([above](#targets)). The auth itself is PLAIN, then LOGIN, whichever the server offers
  first; without credentials the mail goes anonymously, when the server takes it. They never go
  in the endpoint: a `user:password@` in the URL is a load error, as are a path, a query, a
  fragment and an IPv6 literal.
* **TLS.** `smtp://` connects plain and upgrades with STARTTLS, which is mandatory unless `tls`
  says `disabled` (port 587 by default); `smtps://` is implicit TLS from the connection (465).
  Both default to `verify-full`: the chain against the Mozilla roots and the `tls.ca` files, and
  the host name. `verify-ca` checks the chain only, `required` encrypts without checking the
  certificate, and `disabled` skips TLS — on `smtp://` only: `smtps://` with `disabled` is a
  load error. `tls.server_name` is refused: the endpoint's host name is the one checked.
* **Reset steps.** The kind only appends: a sent mail cannot be recalled. A `for_each: stray`
  step does not load ("a stray cleanup needs a target that can delete keys; kind smtp only
  appends"). Still allowed: `once` (it sends one mail, an announcement for example) and
  `for_each: snapshot` (every key is mailed again, as new messages).
* **Contract.** The mailbox receives the latest state of each key, not a log of every change,
  and delivery is at-least-once: a mail whose reply was lost (a connection drop, a timeout) is
  sent again, so a reader keeps the newest mail per key. Several changes of one key within the
  debounce window arrive as one mail, and a delete is the key's last state. Across keys no order
  is guaranteed.

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

### Kafka targets

`kafka` produces to a broker of the Kafka protocol (Apache Kafka, Redpanda, Amazon MSK and
Azure Event Hubs all speak it): one action is one produce of a batch of records, one record per
key, and the answer maps the produce answer of each partition onto that partition's records, so
the kind's answer is per item, not per action. The client is `rskafka`, pure Rust: no librdkafka,
so the image grows no native Kafka dependency.

* **The endpoint.** `kafka://<broker>[:port][,<broker>[:port]…]/<topic>`, or `kafkas://` for
  TLS (default ports 9092 / 9093); exactly one endpoint: one broker set and the one topic it
  writes to (a batch is answered per item, which the engine sends to a single endpoint — as
  `sqs`, `kinesis` and `lambda` take one endpoint too). The topic is 1 to 249 letters, digits,
  `-` or `.`; the target never creates one: a topic the broker does not list is refused (below).
  No credentials in the URL (a `@` is a load error), no query string and no fragment; a
  `kafka://` or `kafkas://` URL belongs to `kind kafka` only. The endpoint context names the
  topic (`endpoint.topic`) and the broker set (`endpoint.brokers`: `{host, port}` per broker),
  and [LAZYWF_ALLOWED_HOSTS](#security) matches every broker of the set.
* **Credentials and TLS.** `vars.sasl` selects the SASL mechanism — `plain`, `scram-sha-256` or
  `scram-sha-512` — and needs `vars.username` (a reference or a literal, as `amqp` takes it) and
  `vars.password` (a reference, never a literal; it is a secret of the kind); both are resolved
  on every connect, so a rotated secret file applies at the next connection. Without
  `vars.sasl` the connection is anonymous. `kafkas://` is TLS, `verify-full` by default: the
  chain against the Mozilla roots and the `tls.ca` files, and the host name of every broker;
  `tls.server_name` is refused (each broker's own name is checked) and so is `disabled` on
  `kafkas://`.
* **The rendered action.** One action, one produce: the template renders `POST /` with a body
  of

  ```text
  POST / HTTP/1.1
  Content-Type: application/json

  {"records": [{"value": …, "headers": {"op": "upsert"}}, {"value": …}]}
  ```

  Record *i* takes the document key `keys[i]` as its record key, its rendered `value` (one JSON
  value, serialized compactly; null is refused — render an empty object for a delete) and
  optional `headers` (string to string); a record carries `value` and `headers` only, and the
  records' count must match the keys'. A `once` reset step has no keys: its records are keyless
  and land on partition 0. A blank render sends nothing; a body that is not that JSON is
  refused.
* **Partition choice.** The record key is the document key, and the partition is what the
  brokers' own default partitioner makes of it — the murmur2 hash of the key, made positive and
  taken modulo the topic's partition count — so one key always lands on the same partition, and
  its records arrive in order. Across keys no order is guaranteed.
* **Compression and the batch limit.** The produce's batch is compressed per target:
  `vars.compression` is `none`, `gzip`, `lz4` or `zstd` (zstd by default). The largest batch a
  target takes is the broker's `message.max.bytes`, 1 MiB by default, raised per target by
  `vars.message_max_bytes` (1 to 4294967295); with `fire_per = batch`, `batch_max_bytes` above
  the target's limit does not load, and a batch is split to it when unset.
* **Outcomes.** The produce answer of a partition maps onto that partition's records. A base
  offset: sent — `expect` then sees, per record, `body` `{"topic": …, "partition": …, "offset":
  …}` (no `expect`: met), and a record whose condition is not met is refused on its own.
  Retriable broker errors (leader not available, not leader for the partition, a timeout,
  throttling, the network), a whole-call connection failure, a produce's
  `UnknownTopicOrPartition` (stale routing: the failed step clears the cached partitions, so the
  next attempt re-reads them) and any code the client cannot judge (the broker's
  `UnknownServerError`, an unrecognised code) are transient: a retry cannot drop a key. Record
  too large, message too large, an unknown topic at the metadata check (the topic must exist:
  the target does not create one), an invalid record, an authorization or a SASL failure and an
  unsupported compression are refused. The broker refuses a whole record set over one record of
  it and names none — so a batch refused over a size or a format code is split in two and both
  halves produced again: the record the broker will not have is refused alone and its neighbours
  go through (bounded by 64 extra produces per action; a batch refused whole costs one produce).
  A failed step drops the connection and its cached partitions; the next action reconnects and
  re-reads the metadata.
* **One produce at a time.** Produces to one endpoint run one at a time (the endpoint's lock is
  held over all of an action's partition produces: one batch of records per partition, in one
  produce each, keeps the order of the action), as `amqp` holds its session; different targets
  and rules run in parallel. Every partition step is bounded by the target's `timeout_ms`.
* **Reset steps.** The kind only appends: a produced record cannot be recalled. A
  `for_each: stray` step does not load ("a stray cleanup needs a target that can delete keys;
  kind kafka only appends"), and the target cannot list keys either. Still allowed: `once` (one
  produce of keyless records, an announcement for example) and `for_each: snapshot` (every key
  is produced again, as new records) and `reset_requested`.
* **Contract.** The topic receives the latest state of each key, not a log of every change, and
  delivery is at-least-once: a record can arrive twice (a retry after a lost answer; a snapshot
  reset), so a consumer reads the partition log and keeps the newest record per key. Several
  changes of one key within the debounce window arrive as one record, and a delete is the key's
  last state. One key's records keep their order (one key, one partition); across keys no order
  is guaranteed.

### RabbitMQ stream targets

`rabbitmq-stream` writes to a RabbitMQ stream — or to the partitions of a super stream — over
RabbitMQ's native stream protocol, port 5552 (5551 over TLS): one action is one publish batch
of records, one record per key, and the broker's publish confirm answers each record, so the
kind's answer is per item and not per action. The client is `rabbitmq-stream-client`, the
official pure-Rust one, used through its raw `Client`: the kind owns its connection and waits
for the confirms of a publish itself, exactly the kafka "kind owns its connection" shape. For a
stream queue on the AMQP 0-9-1 port the kind is `amqp` ([AMQP targets](#amqp-targets)); this
one is for throughput and super streams.

* **The endpoint.** `rabbitmq-stream://<host>[:port]/[<vhost>/]<stream>`, or
  `rabbitmq-stream+tls://` for TLS (default ports 5552 / 5551 — RabbitMQ's own stream ports);
  exactly one endpoint per target: one node and the stream its batches go to (a batch is
  answered per item, which the engine sends to a single endpoint, as `kafka` and `servicebus`
  take one too). The path is `<stream>` (the default vhost) or `<vhost>/<stream>`; `%2F`
  names the default vhost. The stream name is 1 to 255 letters, digits, `-`, `.`, `_` or `:`
  (as the broker names queues); the target never creates one, and neither creates the
  partitions of a super stream. No credentials in the URL (a `@` is a load error: they belong
  in `vars`), no query string and no fragment (TLS is the target's `tls`), and a
  `rabbitmq-stream://` or `rabbitmq-stream+tls://` URL belongs to `kind rabbitmq-stream` only.
  The endpoint context names the vhost and the stream (`endpoint.vhost`, `endpoint.stream`)
  next to `host`, `port` and `url`, and [LAZYWF_ALLOWED_HOSTS](#security) matches the host.
* **Credentials.** The stream protocol always asks for a SASL PLAIN account: `vars.username`
  (a reference or a literal, as `amqp` takes it) and `vars.password` (a reference, `env:` or
  `file:`, never a literal; a secret of the kind), and both are required — without them the
  target does not load. They are resolved on every connect, so a rotated secret file applies
  at the next connection, and neither reaches a log at any level.
* **The rendered action.** One action, one publish batch: the template renders `POST /` with a
  body of

  ```text
  POST / HTTP/1.1
  Content-Type: application/json

  {"records": [{"value": …, "routing_key": "order-1"}, {"value": …, "routing_key": null}]}
  ```

  Record *i* takes the document key `keys[i]` — it travels on the broker's message as the
  application property `lazywf_key` (the counterpart of `amqp`'s `Message-Id`; a `once` step
  has no keys and its records carry none) — its rendered `value` (one JSON value, serialized
  compactly; null is refused — render an empty object for a delete) and its optional
  `routing_key` (a string or null). A record carries `value` and `routing_key` only, and the
  records' count must match the keys'. A blank render sends nothing; a body that is not that
  JSON is refused.
* **Super streams and the partition choice.** A plain stream takes every record of the batch.
  The first batch asks the broker whether the named stream is a super stream (its partition
  list, cached per connection); if it is, each record's `routing_key` chooses the partition
  stream it goes to: **murmur3 of the routing key (seed 104729) modulo the length of the
  broker's partition list** — the rule `rabbitmq-stream-client`'s own super-stream producer
  uses, so the same key lands on the same partition either way — and one publisher is declared
  per partition stream, all of it on the connection to the endpoint's host. A super stream
  record without a routing key is refused on its own (nothing to route by); on a plain stream
  the routing key is carried and unused. A partition whose leader is another node answers
  `StreamNotAvailable`, which is transient here: front the cluster with a stream load
  balancer, or name the node that hosts the partitions.
* **Size.** The largest batch a target takes is one stream frame: 1 MiB (the client sends a
  publish batch as one frame and does not split it; there is no var to raise the limit). With
  `fire_per = batch`, a `batch_max_bytes` above it does not load, and a batch is split to it
  when unset; a batch that still goes over the frame limit is refused whole before anything
  goes on the wire, and the error names the template or the setting.
* **Outcomes.** A publish confirm for the record is **sent**: `expect` then sees, per record,
  `body` `{"stream": …, "publishing_id": …}` and status 200 (no `expect`: met), and a record
  whose condition is not met is refused on its own. Of the broker's publish-error codes only
  `FrameTooLarge` speaks positively about this one record and nothing else: it refuses that
  record **alone**, so its neighbours of the batch still arrive and only that key is struck.
  `AccessRefused` — the account may not publish to this stream — refuses the **whole call**
  and strikes no key: a super stream's partitions are separate streams with separate rights,
  and a per-record verdict would let a permissions mistake quietly drop the keys of the
  forbidden partition after five strikes while the others went through. A stream the broker
  does not know (refused at the publisher's declare — the target never creates one) and a
  refused SASL or vhost account (at the connect) are likewise a **refusal of the whole call**,
  found at link level once — never a per-message strike: with nothing confirmed in the batch
  the answer says nothing about one key on its own, so the keys stay pending and the rule
  retries until someone creates the stream or fixes the target — exactly `kafka`'s unknown
  topic at the metadata check. Everything that does not positively refuse this record is
  transient — a lost connection, no confirm within the target's `timeout_ms`, and every code
  that says "not now" or nothing (`StreamNotAvailable`, `InternalError`, a publisher gone
  stale, an unknown code): the connection drops and the next action reconnects; a retry cannot
  drop a key.
* **One batch at a time.** One publish batch at a time per endpoint: the endpoint's lock is
  held over all of an action's publishes; different targets and rules run in parallel (as
  `kafka` and `amqp` hold theirs).
* **TLS.** `rabbitmq-stream+tls://` is TLS, and this client verifies one way only:
  `verify-full` with `tls.ca` naming exactly one PEM file — the client trusts that file and
  nothing else (no Mozilla roots, no OS store) and checks the endpoint host's name itself, so
  a `+tls://` target without `tls.ca` is a load error: name the CA. The modes the client
  cannot express are refused at load, naming the mode: `required`, `verify-ca` (no host name
  check), `tls.server_name` (the endpoint's own host is always the one checked) and `disabled`
  on `rabbitmq-stream+tls://`; the crate's no-verification mode is never reachable.
  Handshakes run on the ring rustls provider, which the tool installs as the process default
  at startup.
* **Reset steps.** The kind only appends: a published record cannot be recalled and the target
  cannot list keys, so a `for_each: stray` step does not load. Still allowed: `once` (one
  publish of keyless records, an announcement for example), `for_each: snapshot` (every key
  published again, as new records) and `reset_requested`.
* **Contract.** The stream receives the latest state of each key, not a log of every change.
  Delivery is **at-least-once, with no broker-side deduplication yet**: the producer is
  unnamed and the publishing ids are the client's own per connection, so after a lost confirm
  or a restart re-send a consumer may see the same record twice (a record is never lost by
  that). A consumer reads the log and keeps the newest record per key — the document key
  travels as the `lazywf_key` application property. Several changes of one key within the
  debounce window arrive as one record, and a delete is the key's last state. Across keys no
  order is guaranteed.

### Service Bus targets

`servicebus` writes to an Azure Service Bus queue or topic over AMQP 1.0: one action is one batch
of messages sent over one sender link, with a disposition awaited for every transfer, so the
kind's answer is per item and not per action. The client is `fe2o3-amqp`, pure Rust, so the image
grows no native AMQP dependency.

* **The endpoint.** `sb://<namespace>.servicebus.windows.net/<queue-or-topic>`: exactly one per
  target (a batch is answered per item, which the engine sends to a single endpoint, as `kafka`,
  `sqs` and `sns` do), AMQP 1.0 over TLS on port 5671, always. No credentials in the URL (a `@`
  is a load error: they belong in `vars`), no query string, no fragment, and no
  `tls.server_name` (the endpoint's own host is the one checked). The entity is 1 to 260 letters,
  digits, `-`, `.` or `_`, does not start with `$` (the system nodes) and carries no
  `/subscriptions/` segment: a sender writes to the topic and the namespace fans the copy out.
  The target creates nothing: a queue or topic that is not there refuses the target (below).
  The endpoint context names the entity (`endpoint.entity`) next to `host`, `port` and `url`, and
  [LAZYWF_ALLOWED_HOSTS](#security) matches the host.
* **The local broker.** Any host that is not `<namespace>.servicebus.windows.net` is the override
  for tests and self-hosted brokers — `sb://<host>:<port>/<address>`, the integration suite
  pointing it at its own RabbitMQ — where the path is the AMQP 1.0 address the broker is
  given verbatim (its leading `/` kept, `%` allowed for a percent-encoded segment: RabbitMQ's own
  form is `/queues/<name>`) and `tls: disabled` is allowed. The `$` and subscription refusals
  stand there too, so a typo cannot reach a system node.
* **Credentials: a SAS rule.** `vars.key_name` (the authorization rule name) together with
  `vars.key` (the key: a reference, `env:` or `file:`, never a literal; one without the other is
  a load error). The token is put to the connection's `$cbs` node and refreshed before it
  expires, and the key is resolved on every put, so a rotated secret file applies at the next
  refresh. **Microsoft Entra ID is not supported** by this kind. For a broker that speaks
  AMQP 1.0 but not CBS, `vars.username` (a reference or a literal) with `vars.password` (a
  reference, never a literal) select SASL PLAIN instead; the two forms cannot be combined (Service
  Bus accepts CBS only on a SASL ANONYMOUS handshake) and neither at all means an anonymous
  connection, which only the local override may use.
* **The rendered action.** One action, one batch: the template renders `POST /` with a body of

  ```text
  POST / HTTP/1.1
  Content-Type: application/json

  {"messages": [{"value": …, "session": "orders-7", "properties": {"op": "upsert"}}, {"value": …}]}
  ```

  Message *i* takes the document key `keys[i]`: its `value` (one JSON value, serialized compactly
  into an AMQP Data section with `content_type: application/json` and a durable header; null is
  refused — render an empty object for a delete), its optional `session` and its optional
  `properties` (string to string: the application properties). A message carries `value`,
  `session` and `properties` only, and the messages' count must match the keys'. A `once` reset
  step has no keys, so its messages are keyless. A blank render sends nothing; a body that is not
  that JSON is refused.
* **Sessions.** `vars.session` states that the entity takes sessions; then every message must
  render its `session` — the AMQP group id, what the entity groups by — or that one message is
  refused while its neighbours go through.
* **Size.** The most one message takes is the entity's maximum message size, 256 KiB by default,
  raised per target by `vars.max_message_size` to the entity's own limit (1 to 104857600 bytes:
  100 MiB, the biggest tier's). A `value` over it is refused on its own, before anything goes on
  the wire. The same number bounds the batch: with `fire_per = batch`, a `batch_max_bytes` above
  the target's limit does not load, and a batch the link's own limits will not take is halved and
  sent again, as for `kafka`.
* **Duplicate detection.** `vars.dedup` (off by default) gives every message the `MessageId`
  `hex(sha256(key + NUL + value))`, which is what the entity's own duplicate-detection window
  filters on. It is opt-in: a document that legitimately returns to an earlier value inside that
  window would otherwise be dropped as a duplicate.
* **Outcomes.** An `Accepted` disposition is the only answer that counts as sent: `expect` then
  sees, per message, `body` `{"entity": …, "message_id": …}` (no `expect`: met), and a message
  whose condition is not met is refused on its own. A `Rejected` whose condition is a verdict on
  that message — `amqp:link:message-size-exceeded`, `amqp:decode-error`, `amqp:invalid-field` —
  refuses that message alone. Everything else is transient, because nothing there says the message
  did not arrive: `Released`, `Modified`, a server-busy or throttling condition, a condition this
  kind does not know, a timeout with no disposition, a missing ack, a lost link. Only a positive
  verdict about a message costs that key a strike. A missing entity or an attach refused for lack
  of the Send right (`amqp:not-found`, `amqp:unauthorized-access`) refuses the **whole target**
  once per attempt — the target creates no entities and holds no rights — and so does a CBS reply
  that refuses the token (400, 401, 403 or 404: a malformed token, a wrong key or rule name, no
  right, an unknown audience, none of which a retry can fix) and a failed SASL PLAIN handshake;
  any other CBS reply (5xx, a malformed answer, 408 or 429) says nothing about the token and stays
  transient.
* **One send at a time.** One connection and one sender link per endpoint, held across actions
  behind the endpoint's lock, so one action at a time per endpoint (as `kafka` and `amqp` hold
  theirs) while different targets and rules run in parallel. Any failure that leaves the state
  unknown drops the connection: the next action reconnects, re-puts the token and re-attaches.
  Every step of an action is bounded by the target's `timeout_ms`.
* **Reset steps.** The kind only appends: a message sent cannot be recalled and the target cannot
  list keys, so a `for_each: stray` step does not load. Still allowed: `once` (one batch of
  keyless messages, an announcement for example), `for_each: snapshot` (every key sent again, as
  new messages) and `reset_requested`.
* **Contract.** The entity receives the latest state of each key, not a log of every change, and
  delivery is at-least-once: a message can arrive twice (a retry after an answer that never came,
  a snapshot reset), so a reader keeps the newest message per key. Several changes of one key
  within the debounce window arrive as one message, and a delete is the key's last state.

### gRPC targets

`grpc` writes to a server of the gRPC protocol over HTTP/2: one action is one `lazywf.v1.Envelope`
of items written onto the target's one long-lived bidirectional stream, and the server's
`lazywf.v1.Acks` answer it item by item — the kind's answer is per item, not per action. The
contract is lazywf's own (`proto/lazywf/v1/lazywf.proto`: `Item`, `Envelope`, `Ack`, `Acks`, and a
`Sink` service whose one method takes a stream of envelopes and returns a stream of acks); a server
implements that signature under a service and method name of its own, which the endpoint's path
names. The client is `tonic` over lazywf's own TLS connector, so `grpcs://` gets the same CA bundle
and verify modes as every other kind and the image grows no second libcrypto.

* **The endpoint.** `grpc(s)://<host>[:port]/<package.Service>/<Method>`: exactly one per target
  (the engine holds one stream per endpoint), default ports 80 / 443. No credentials in the URL (an
  `@` is a load error: the bearer belongs in `vars.token`), no query string, no fragment, no
  bracketed IPv6 literal; the service and method are protobuf names. `grpcs://` is TLS: the `tls`
  column holds the modes as for `http`, and `disabled` on a `grpcs://` endpoint is refused.
  [LAZYWF_ALLOWED_HOSTS](#security) matches the host and `endpoint.url` holds the endpoint as it
  should be written.
* **The schema.** `vars.descriptor_set` is a `file:` reference to a protobuf descriptor set and
  `vars.item_type` names the message a document becomes — both are required, and a
  `descriptor_set` written any other way is refused ("a schema is a file, never a value in the
  database"). The set must hold the endpoint's service and method — whose signature is
  `rpc (stream lazywf.v1.Envelope) returns (stream lazywf.v1.Acks)` — and the item message, all
  checked when the rule loads; the rule's payload columns are checked against the item message's
  fields where they can be, and an item field no column covers is a warning. The vars an owner
  reaches for when the schema lives elsewhere — `schema`, `message`, `message_type`,
  `request_type`, `response_type`, `service`, `method` — are refused at load, naming the two this
  kind reads.
* **The mapping.** Protobuf's own JSON mapping, restricted to what a document is: a field name or
  its lowerCamelCase JSON name, a number into any numeric field, a string into a string, `bytes`
  (base64) or an enum, an object into a message or a map, an array into a repeated field. Anything
  else is refused with the field named — at rule load where the columns say enough to tell, at
  send time where they do not.
* **The rendered action.** One action, one batch: the template renders the `docs` array itself
  (no request line — anything that is not that JSON is refused as a body that is not the `docs`
  array), and lazywf reads that JSON back rather than shipping the rendered text — element *i*
  must be `{"key": "<the key>", "doc": {…}}`, `doc` becomes the item message field by field, and
  an element count that is not the key count, a `key` that is not the one the engine gave, or a
  missing `doc` is refused (document *i* is the item of key *i*). A blank render sends nothing.
* **Credentials.** `vars.token` is a bearer credential — a reference (`env:` or `file:`), never a
  literal — resolved on every call, so a rotated file applies without a reload, and sent as
  `authorization: Bearer …` marked sensitive, which keeps it out of the log, a `Debug` and a
  panic. Without it the call carries no `authorization` metadata.
* **Size and compression.** The largest message the server takes is `vars.max_message_bytes`
  (4 194 304 bytes by default, at least 64); with `fire_per = batch`, a `batch_max_bytes` above it
  does not load. Messages of at least `vars.compress_min_bytes` (1024 by default) are compressed:
  `vars.compression` is `auto` (the default — zstd, stepping down to gzip and to identity when
  the server says it lacks the codec), `none`, `gzip` or `zstd`; a pinned codec the server lacks
  does not step down: the flush is a moment whose message quotes the server's own words, so the
  target keeps the codec it was told and says who lacks it.
* **The stream.** One long-lived bidirectional stream per endpoint, opened by the first flush that
  needs it and kept while it is healthy: nothing waits to fill a batch, sequence numbers restart
  at 1 with each stream, and up to 512 items / 8 MiB ride unacknowledged at a time (HTTP/2's own
  flow control beside them). An item is settled by the ack naming its sequence number, and only
  when its hash — xxh3-64 of the key and the encoded body — matches, so a stale ack from a dead
  stream confirms nothing. A stream that ends before an item is acknowledged leaves that item
  transient; the next flush reconnects (backoff from 100 ms, doubling to 30 s) and sends it again.
* **Outcomes.** An `OK` ack settles its key once the rule's `expect` agrees: the row it reads
  has `status` 200, `text` `ok` and `body` the acknowledgement's JSON (`seq` and `hash` as
  strings, `status` `OK` and the server's `reason`), so a rule can hold the server to its own
  answer. A `REFUSED` ack refuses exactly that key (its row is 400 / `refused`), a `RETRY` ack
  is that key's own moment, and an ack with no status or an unknown one says nothing —
  transient, never a strike. The whole call is refused only by a gRPC status that judges the
  request (`INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`, `PERMISSION_DENIED`,
  `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `UNAUTHENTICATED`) or, from a peer that answered plain
  HTTP rather than gRPC, by the statuses that are their own no (400, 401, 403, 409 and the
  404-family); everything else — 408, 425, 429, any 5xx, `UNAVAILABLE`, `DEADLINE_EXCEEDED`
  and the rest — is a moment. Only a positive verdict about an item can cost that key a
  strike.
* **Reset steps.** The kind only appends: a sent item cannot be recalled and the target cannot
  list keys, so a `for_each: stray` step does not load. Still allowed: `once` (whatever the
  template rendered goes out; its `key` fields are the server's deduplication label, and an item
  the server will not take fails the step), `for_each: snapshot` (every key sent again) and
  `reset_requested`.
* **Contract.** The server receives the latest state of each key, not a log of every change, and
  delivery is at-least-once: an item can arrive twice (a retry after an answer that never came, a
  snapshot reset), which is what the `key` and the hash are for — a server that has already
  stored a key may acknowledge the duplicate without processing it again. Several changes of one
  key within the debounce window arrive as one item, and a delete is the key's last state.

## Rules

`lazy_wire_fire_rule`

| column | meaning |
|---|---|
| `name`, `enabled` | identity and switch |
| `target` | a target's `name` |
| `selects` | zero or more queries: SQL separated by `;`, or a JSON array of strings or `{"name", "sql"}`. Each returns `_key` plus [path-named columns](#documents-from-path-named-columns) and uses `:keys` for the changed keys (`WHERE id IN (:keys)`). They run concurrently, each on its own connection |
| `snapshot_keys_sql` | returns every key (`_key`), for `for_each` reset steps |
| `template` | the action, [below](#templates) |
| `fire_per` | `batch` (one action per batch of keys) or `key` (one per key; `docs` has one entry). `amqp` and `smtp` take `key` only (one message / one mail per action: [AMQP targets](#amqp-targets), [SMTP targets](#smtp-targets)); `sqs`, `kinesis`, `lambda`, `eventbridge`, `sns`, `kafka`, `rabbitmq-stream`, `grpc` and `servicebus` answer per item ([SQS targets](#sqs-targets), [Kinesis targets](#kinesis-targets), [EventBridge targets](#eventbridge-targets), [SNS targets](#sns-targets), [Kafka targets](#kafka-targets), [RabbitMQ stream targets](#rabbitmq-stream-targets), [gRPC targets](#grpc-targets), [Service Bus targets](#service-bus-targets)) |
| `batch_max_keys`, `batch_max_bytes` | split a flush above these limits. Bytes count the rendered action. Without `batch_max_keys`, a batch holds at most 10000 keys. `sqs`: `batch_max_keys` is required with `fire_per = batch` and holds 1 to 10, and `batch_max_bytes` cannot exceed 262144 ([SQS targets](#sqs-targets)). `kinesis`: `batch_max_keys`
is required with `fire_per = batch` and holds 1 to 500, and `batch_max_bytes` cannot exceed
5242880 ([Kinesis targets](#kinesis-targets)). `eventbridge`: `batch_max_keys` is required with
`fire_per = batch` and holds 1 to 10, and `batch_max_bytes` cannot exceed 1048576
([EventBridge targets](#eventbridge-targets)). `sns`: `batch_max_keys` is required with
`fire_per = batch` and holds 1 to 10, and `batch_max_bytes` cannot exceed 262144, the topic's
`MaximumMessageSize`, which `vars.maximum_message_size` states up to 1048576
([SNS targets](#sns-targets)). `pubsub`: `batch_max_keys` is required with
`fire_per = batch` and holds 1 to 1000, and `batch_max_bytes` cannot exceed 10000000
([Pub/Sub targets](#pubsub-targets)). `kafka`: `batch_max_bytes` cannot exceed the target's
produce limit, the broker's `message.max.bytes` (1 MiB by default) raised by its
`vars.message_max_bytes` ([Kafka targets](#kafka-targets)). `rabbitmq-stream`: `batch_max_bytes`
cannot exceed the one stream frame a publish is, 1048576 (1 MiB; there is no var to raise it:
[RabbitMQ stream targets](#rabbitmq-stream-targets)). `grpc`: `batch_max_bytes` cannot exceed the
target's `vars.max_message_bytes` (4 194 304 bytes by default: [gRPC targets](#grpc-targets)).
`servicebus`: `batch_max_bytes`
cannot exceed the target's entity message size, 262144 (256 KiB) by default, raised by its
`vars.max_message_size` up to 104857600 ([Service Bus targets](#service-bus-targets)) |
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
| `key` | template over the row image giving the key; one key per line. `{{new.id}}`, `item_{{old.item_id}}` |
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
| `endpoint` | `{host, port, url}` (redis and the SQL targets also `db`; `amqp` also `vhost`; `sqs` also `queue` and `region`; `kinesis` also `stream` and `region`; `lambda` also `function`, `qualifier`, `invocation` and `region`; `eventbridge` also `bus` and `region`; `sns` also `topic`, `topic_arn`, `region` and `fifo`; `pubsub` also `project` and `topic`; `kafka` also `topic` and `brokers` (one `{host, port}` per broker); `rabbitmq-stream` also `vhost` and `stream`; `servicebus` also `entity`; `grpc` also `service`,
`method` and `item_type`) of the endpoint the action goes to. For an endpoint whose URL is a reference these three are the origin only — `url` is `https://hooks.example.test/…` and nothing below the origin reaches a template ([secret http endpoints](#secret-http-endpoints)) |
| `reset` | inside reset steps `{step, keys}` (stray steps: the stray keys; list templates also `next`, `page`); `null` otherwise |

Inside `{{#each}}` / `{{#with}}` the names above still mean the top level (as with Handlebars.js
compat mode), so `{{target.vars.prefix}}` works inside `{{#each keys}}`. A field of the current
element with one of these names is `this.<name>`.

Helpers: `join list sep`, `upper`, `lower`, `regex_escape`, `url_encode`, `json`, `yaml v` (the
value as a YAML document: block style, two-space indents, the value's own key order, no `---`
header), `base64 s` (the standard base64 alphabet, padded: a Kinesis record's `Data`, a
Pub/Sub message's `data`), `replace s regex replacement`, `add a b` (the sum of two whole
numbers, a value that is not one being an error rather than a blank; it numbers the entries of
an `sns` batch, `…member.{{add @index 1}}`), `redis_quote s…` (its arguments
concatenated, as one Redis argument), `sql_quote v`, `sql_ident name` (in the dialect of the
rule's target: PostgreSQL for `pgsql`, MySQL otherwise), plus the built-ins `if`, `unless`,
`each`, `with`, `lookup`, `eq`, `ne`, `gt`, `lt`, `and`, `or`, `not`, `len`. String literals in
templates take JSON escapes: write `"\\d"` for the regex `\d`.

**`{{yaml v}}`** writes its argument as a YAML document, for a body: an empty object or list is
`{}` or `[]`, an object's entries are one per line and a nested block indents two spaces, a
list's items take a `- ` at the item's column. It is the one helper whose output the escape
function does not touch, and `{{yaml doc}}` and `{{{yaml doc}}}` render the same document: a
document is multi-line by nature, and the helper keeps every control character of the data
inside an escaped double-quoted scalar, so the only raw line breaks in its output are YAML's own
structure. The read-back is guaranteed: a string that a YAML 1.1 or 1.2 reader could resolve as
a number, a boolean (`y`, `no`, `on` among them), a null or a date (`2026-11-07`,
`2026-11-07 10:00`) goes out double-quoted, so the receiver gets the document's values with their
types (a `DATE` column reaches the template as a string and comes back a string, not a date), a
float carries the dot and the exponent sign a 1.1 reader needs (`1.0e+20`, not the `1e20` JSON
prints), and a key whose written form — quotes and `\n` escapes counted, not the raw key —
passes 1024 characters goes on its own line in the explicit `? key` / `: value` form: a reader
refuses the `:` right after a key that long, quoted or not, so such a key cannot share its line
with its value. Use it in a body only: elsewhere a line break is not data, so in a request line,
a header, a mail header or a one-line command the document's lines would arrive as extra headers,
extra arguments or a refusal — put `{{json doc}}` or a single value in those places. The template
writes the body's `Content-Type` itself (`application/yaml`), as it does for a JSON body: the tool
adds no content type to an http request.

What a template renders:

* **http**: the raw request: request line (any method, `HTTP/1.1` optional), header lines, blank
  line, optional body. The tool sets `Content-Length`, adds `Host` (unless given), `User-Agent`
  and the authentication. Blank output sends nothing.
* **redis**: one command, split like `redis-cli` arguments (`"…"` with `\n`, `\"`, `\xHH`;
  `'…'`). `redis_quote` makes any string a safe argument. Blank output sends nothing.
* **mysql / pgsql**: one SQL statement. Blank output sends nothing.
* **amqp**: one message: `PUBLISH /<exchange>/<routing-key>`, a `Message-Id` header (the key), a
  blank line, a JSON body. Blank output sends nothing.
* **smtp**: one mail: the header lines (`From`, `To`, `Cc`, `Subject`, `Reply-To`,
  `Content-Type`), a blank line, the body; the envelope comes from `From`, `To` and `Cc`
  ([SMTP targets](#smtp-targets)). Blank output sends nothing.
* **lambda**: the Invoke payload only (one JSON value); the tool writes and signs the request.
  Blank output sends nothing.
* **pubsub**: the raw request as for `http`, but only the topic's `:publish` path is sent
  ([Pub/Sub targets](#pubsub-targets)). Blank output sends nothing.
* **kafka**: `POST /` with the `{"records": […]}` produce body (record *i* takes key *i*); the
  tool groups the records by partition and produces them
  ([Kafka targets](#kafka-targets)). Blank output sends nothing.
* **rabbitmq-stream**: `POST /` with the `{"records": […]}` publish body (record *i* takes key
  *i*, with its `routing_key` for a super stream); the tool publishes the batch — a super
  stream's records grouped per partition stream — and awaits the publish confirm of every
  record ([RabbitMQ stream targets](#rabbitmq-stream-targets)). Blank output sends nothing.
* **eventbridge**: the raw request as for `http` — the `PutEvents` call — whose `Entries` are
  checked before the request is signed and sent
  ([EventBridge targets](#eventbridge-targets)). Blank output sends nothing.
* **sns**: the raw request as for `http` — the form-encoded `PublishBatch` call — whose
  entries are checked before the request is signed and sent
  ([SNS targets](#sns-targets)). Blank output sends nothing.
* **servicebus**: `POST /` (any other request line is refused) with the `{"messages": […]}` batch
  body (message *i* takes key *i*); the tool opens the connection and the sender link, puts the
  SAS token to `$cbs` and sends the batch, awaiting a disposition per message
  ([Service Bus targets](#service-bus-targets)). Blank output sends nothing.
* **grpc**: the `docs` array itself, and nothing else — element *i* is the `{"key": …, "doc": …}`
  of key *i*; the tool reads that JSON back, encodes each `doc` onto the target's item message
  with the descriptor set's own schema, writes the envelope onto the target's stream and lets the
  server's acknowledgements answer it (a request line or header lines in the render are refused:
  there is no request to parse — [gRPC targets](#grpc-targets)). Blank output sends nothing.

## Reset steps

A JSON array, run in order; a failed step is retried, and the next one never starts before it
succeeded. They run when continuity is lost (first start, purged binlog, another server, a new
rule, a watched table replaced without row events) or on `reset_requested = 1`, in the background
while streaming continues. A rule enabled again replays the binlog from where it stopped instead,
and resets only when that part of the binlog was purged. A server 1236 on the binlog dump resets
only when its own message says the requested position cannot be served: a purged or missing
binlog file, a position outside the file, or GTIDs the position needs are gone. Any other 1236
keeps the position: the dump is retried with the capped backoff, the server's message is logged
as a WARN and shown as `stream_error` on `/status` until the stream delivers events again
([Status](#status)). An interrupted reset starts over after a restart.

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
* An append-only kind (`sqs`, `kinesis`, `eventbridge`, `sns`, `amqp`, `smtp`, `pubsub`, `kafka`,
  `rabbitmq-stream`, `grpc`, `servicebus`)
  refuses a `for_each: stray` step at load (a sent message cannot be recalled), `lambda` refuses
  one too (a function cannot list what it was invoked with), `sqs`, `kinesis`, `eventbridge` and
  `sns` refuse a `once` step that purges what it has sent (a queue's `PurgeQueue`, a stream's
  `DeleteStream`, an event bus's `DeleteEventBus`, a topic's `DeleteTopic`), and `pubsub` a step
  that does anything but publish — [SQS targets](#sqs-targets), [Kinesis targets](#kinesis-targets),
  [Lambda targets](#lambda-targets), [EventBridge targets](#eventbridge-targets),
  [SNS targets](#sns-targets), [AMQP targets](#amqp-targets), [Pub/Sub targets](#pubsub-targets),
  [RabbitMQ stream targets](#rabbitmq-stream-targets), [gRPC targets](#grpc-targets),
  [Service Bus targets](#service-bus-targets).

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
| `binlog_file`, `binlog_pos` | every action for changes before this point met `expect`. `*`: where the stream resumes. The position is a transaction boundary: the end of the last event that closed a transaction (`Xid`, `XA PREPARE`, `COMMIT` / `ROLLBACK`, a statement on its own), never the position of a statement inside one — the events that open a group differ per server, the ones that close it do not, so the boundary is the same rule whether GTIDs are on or off |
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
| `stream_error` | instance only, not per rule: a binlog dump 1236 whose message says nothing about the requested position: the server's message with the position it failed at, shown while the dump is retried with that position kept, until the stream delivers events again; [Reset steps](#reset-steps) |
| `binlog_filtered_schemas` | instance only, not per rule: the checked schemas the server's binlog filters drop (sorted); [Binlog filters](server-setup.md#binlog-filters) |

The counters are in memory only: they start at 0 at each start and are not written to
`lazy_wire_fire_status`, which keeps only what a restart needs.

## Security

Templates are not escaped for you (there is no context the tool could know), so **values from
rows go through the helper of the place they land in**:

| where | write | not |
|---|---|---|
| a JSON string or value | `"_id":{{json key}}`, `{{json doc}}` | `"_id":"{{key}}"`: a key `1"}}…` adds `_bulk` actions |
| a YAML value | `{{yaml doc}}` | `key: {{value}}`: a value `1` reads back a number, a value `k: 2` leaves the line unparseable |
| a path or query | `/items/{{url_encode key}}.json` | `/items/{{key}}.json`: a key `a?x=` writes another object |
| a regex (Varnish tags) | `{{regex_escape this}}` | `{{this}}`: a key `.*` purges everything |
| a Redis argument | `{{redis_quote this}}`, or `ARGV` of a script | splicing keys into a Lua string |
| SQL | `{{sql_quote key}}`, `{{sql_ident name}}`; an action is one statement, a second is refused ([SQL targets](#sql-targets)) | `'{{key}}'`: a key `x'); DROP …` starts a second statement |

What the tool enforces on its own:

* **Keys are one line of text.** A key with a control character (from a key template, `keys_sql`,
  the snapshot or a stray listing) is ignored and reported in `last_error`; a key template is
  rendered line by line, so a newline in a value cannot mark extra keys.
* **Interpolated values hold no control characters**: `{{x}}` turns them into spaces, so data
  never starts a new header, request, NDJSON line or command (`{{{x}}}` opts out, and
  `{{yaml doc}}` is exempt the same way: its line breaks are the document's own structure and
  every control character of the data sits inside an escaped double-quoted scalar, so use it in a
  body only — [Templates](#templates)). A request line or header with a control character is
  refused; a rendered mail refuses, besides that, an unknown or repeated header, a folded line,
  and more recipients than `vars.recipients` ([SMTP targets](#smtp-targets)).
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
* **A SQL target action is one statement.** A rendered statement holding a top-level `;`
  followed by more, or an executable comment (`/*! … */`, `/*!NNNNN … */`, `/*M! … */`), is
  refused before it is sent (the error names the statement count or the comment); the server
  enforces it too — MySQL / MariaDB run the statement as a prepared statement, which refuses
  more than one, and PostgreSQL's extended protocol refuses batches. Optimizer hints
  (`/*+ … */`) stay allowed ([SQL targets](#sql-targets)).
* **Secret references are confined**: `env:` cannot name lazywf's own settings (`LAZYWF_*`), and
  `file:` must resolve (symlinks followed) inside `LAZYWF_SECRET_DIRS` (default
  `/run/secrets:/var/run/secrets`). An `endpoints` entry of the four plain http kinds is confined
  the same way and held to the same rule: its value is a whole webhook URL, read on every request
  and shown only as its origin ([secret http endpoints](#secret-http-endpoints)).
* **The webhook signing secret is a reference.** `vars.sign_secret_ref` is confined like any
  other secret reference and a literal is a load error; it is a credential of the kind, so it
  stays out of the template context and is read only where the signature is computed: it never
  reaches a template or a log ([Webhook signing](#webhook-signing)).

What it cannot enforce, so treat the tool's tables as **admin-only**:

* a writer of `lazy_wire_fire_rule` / `_trigger` runs SQL as the account that runs the
  configuration's queries: lazywf's own account when `LAZYWF_QUERY_*` is unset (give it `SELECT`
  only where rules need it), or the query account — for which a SELECT-only one on the source
  tables is recommended ([server setup](server-setup.md#query-account)) — and sends the result
  anywhere a target points;
* a writer of `lazy_wire_fire_target` chooses where requests (with the credentials of that target)
  go, including internal addresses — unless `LAZYWF_ALLOWED_HOSTS` pins the endpoint hosts.

Where the tables cannot be kept admin-only, `LAZYWF_ALLOWED_HOSTS` pins the endpoint hosts a
target may connect to: a comma-separated list of patterns, each an exact host name
(case-insensitive), a `*.` suffix (the subdomains of the name, not the name itself), an IP
literal, or a CIDR range, with an optional `:port` (an IPv6 pattern with a port in brackets,
`[2001:db8::1]:443`). Unset or blank: no restriction.

Every endpoint's effective host and port — the name lazywf connects to, after a `?endpoint=`
override, the derived AWS and Google service hosts, and the S3 virtual-hosted bucket host (the
bucket name in front of the service host) — must match a pattern. A `kafka` endpoint lists every
broker of its set: the check matches all of them, not only the first, and the one off the list is
the host the error names. Matching is on that name:
nothing is resolved in DNS, so an IP pattern matches IP-literal endpoints only, and `localhost`
never matches `127.0.0.1`. An unmatched endpoint is a target load error that names the host and
the setting; it shows in the rule's `last_error`, and the check runs when a target is loaded —
at start, and on a config reload whose target row changed (an unchanged target keeps its checked
load). Not covered: the AWS and Google credential calls (lazywf does not take them from the
target table) and redirects (the HTTP client does not follow them). At start the log says whether
the list is set and its pattern count.

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

**A YAML body over http**, the same customers as a document a receiver parses (plain tables, one
request per key):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints) VALUES ('collector', 'http', 'http://collector:9090');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms) VALUES
 ('customers_yaml', 'collector', 'key',
  'SELECT id AS _key, name, note, qty FROM customers WHERE id IN (:keys)',
  '{{#each docs}}POST /customers/{{url_encode key}} HTTP/1.1
Content-Type: application/yaml

{{yaml doc}}{{/each}}', 100);
```

Customer `42` with `name = 'Ana'`, a `note` of two lines and `qty = 3` sends this body, its keys
in the select's column order:

```text
name: Ana
note: "left at the door\nring the bell"
qty: 3
```

`note` is one column holding a line break: it goes out double-quoted with `\n`, so the
receiver's parser hands back the same two lines, `qty` comes back the number the column is, and
a `name` of `2026-11-07` would go out quoted too and stay a string rather than becoming a date.
The template writes the `Content-Type` line itself — the tool adds `Content-Length`, `Host` and
`User-Agent` and no content type of its own — and a `{{yaml doc}}` needs no triple stash.

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

**Messages to an Azure Service Bus queue**, one batch per flush (a reader keeps the newest
message per key):

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, vars) VALUES
 ('orders-sb', 'servicebus', 'sb://lwf-test-ns.servicebus.windows.net/orders',
  'JSON_EXTRACT(body, ''$.entity'') IS NOT NULL',
  '{"key_name": "send-rule", "key": "env:SB_SEND_KEY"}');
INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template, debounce_ms)
 VALUES ('orders', 'orders-sb', 'batch', 500, 200000,
  'SELECT c.id AS _key, c.id, c.name, NULL AS _op FROM customers c WHERE c.id IN (:keys)',
  'POST / HTTP/1.1
Content-Type: application/json

{"messages": [{{#each docs}}{"value": {{json doc}}}{{#unless @last}}, {{/unless}}{{/each}}]}',
  100);
```

A trigger on `customers` marks `{{new.id}}`: one batch per flush, message *i* the document of key
*i*, and the disposition awaited for that transfer is its answer — `expect` sees
`{"entity": …, "message_id": …}` per message. `vars.key_name` names the shared access rule that
holds the Send right and `vars.key` is its key, a reference only; `vars.session` says the entity
takes sessions (every message then renders its `session`), `vars.dedup` puts a content id on the
wire for the entity's duplicate detection, and `vars.max_message_size` raises the 256 KiB default
to the entity's own limit — which is also the ceiling `batch_max_bytes` must stay under.

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
