# Business app recipes

Copy-ready recipes for the `http` kinds pointed at hosted business applications: two chat
platforms, an event pipeline, an email platform, three systems of record and four automation
hubs — Microsoft Teams, Google Chat, Segment, Klaviyo, HubSpot, Airtable, Notion, and Zapier,
Make, n8n and Microsoft Power Automate. Same kinds and the same three tables as
[the http recipes](recipes.md). What changes is which vendor limit binds, which
header a service wants besides its auth header, which id a delete has to be keyed by, and how
much a 2xx is actually worth.

> **These are examples; CI does not run them.** The test suite exercises the built-in sinks and
> the `http` kinds against OpenSearch, Elasticsearch and Varnish containers, never against
> Teams, Google Chat, Segment, Klaviyo, HubSpot, Airtable, Notion or the four automation hubs.
> The endpoint forms, header names and `expect` clauses below come from each vendor's own
> documentation and from the request-building code. Try one against your own account before
> production data drives it.

Three things are the same in every recipe here:

* **`endpoints` is the origin** and the template's request line carries the path and the query —
  for every recipe here except the incoming webhooks (Teams, Google Chat, Zapier, Power Automate,
  and n8n with its authentication left at None), where `endpoints` is a `file:` reference
  whose value is the whole URL and the request line is `POST /`
  ([reference → secret http endpoints](reference.md#secret-http-endpoints)). A prefix in
  `endpoints` is put in front of every path, so a prefix plus `POST /` would gain a trailing
  slash; the services that give their address in plain text take their path in the template
  instead.
* **The credential is a reference** (`env:NAME` or `file:/path`), resolved on every request, never
  rendered by a template: in `vars` for the recipes that send a credential (and for the
  automation hub that sends an API-key header), and in `endpoints` for the incoming webhooks
  whose secret is the URL below the origin (a reference endpoint is for the four
  plain http kinds, and only their `endpoints` column resolves one).
* **`expect` is a target column**, evaluated as a SQL condition over the response row
  (`status`, `headers`, `body`, `text`). Left empty it means "any 2xx". A condition that names
  `status` replaces the 2xx band, so write `status BETWEEN 200 AND 299 AND …` when you want both
  ([reference → targets](reference.md#targets)).

The rest of this file is one section per service, each with the endpoint form, the auth kind,
the request as it goes on the wire, what `expect` can and cannot prove, the batch and rate
limits that bind, and how a delete is expressed — or the fact that none exists.

* [How these differ from the http recipes](#how-these-differ-from-the-http-recipes)
* [Microsoft Teams: post to a channel](#microsoft-teams-post-to-a-channel)
* [Google Chat: post to a space](#google-chat-post-to-a-space)
* [Segment: send tracked events in a batch](#segment-send-tracked-events-in-a-batch)
* [Klaviyo: create profile events](#klaviyo-create-profile-events)
* [HubSpot: upsert contacts and archive them](#hubspot-upsert-contacts-and-archive-them)
* [Airtable: create, update and delete records](#airtable-create-update-and-delete-records)
* [Notion: create a page and archive it](#notion-create-a-page-and-archive-it)
* [Zapier: trigger a Zap from a catch hook](#zapier-trigger-a-zap-from-a-catch-hook)
* [Make: trigger a scenario from its custom webhook](#make-trigger-a-scenario-from-its-custom-webhook)
* [n8n: run a workflow from its Webhook node](#n8n-run-a-workflow-from-its-webhook-node)
* [Microsoft Power Automate: run an instant cloud flow](#microsoft-power-automate-run-an-instant-cloud-flow)
* [What these recipes do not claim](#what-these-recipes-do-not-claim)
* [Not covered here](#not-covered-here)

## How these differ from the http recipes

**Two headers, one kind.** `http.basic`, `http.bearer` and `http.header` write exactly one
credential header. Half the services below also require a version header of their own, and a
template header is the only other option: `http.header` takes `vars.header` and `vars.value` for
one header, so Klaviyo gets `Authorization` from `vars` and `revision` from the template, and
Notion gets `Authorization` from `http.bearer` and `Notion-Version` from the template. A template
header is *rendered*, so it is the right place for a version string and the wrong place for a
secret.

**Order on the wire.** The template's own header lines go first, then `User-Agent`, then `Host`,
then the kind's credential header, then `Content-Length` computed from the body. Nothing else is
added: no `Accept-Encoding`, no `Connection`, no content type — the template writes
`Content-Type` itself, including where the service wants something other than
`application/json`.

**Deletes are keyed by the id the vendor issued.** HubSpot and Airtable delete by their own
record ids, so those ids have to live in the source table and the delete rule's `key` template
reads them (`{{new.hs_contact_id}}`, `{{new.at_record_id}}`). A delete rule keyed by the row's
own primary key can only be used where the vendor accepts that value as its identifier.

**A 2xx that means "accepted" or "some of them".** Klaviyo answers 202 for an event that is
still queued and can silently discard duplicates of it. Segment answers 200 to a batch whose
events it then drops. HubSpot can answer 207 Multi-Status, a code inside the 2xx band that
reports successes and errors together, when partial-error handling is enabled. The `expect`
column can pin the status and read the body, but none of it can prove the row is where you wanted
it; each recipe says what its answer is worth.

**Rate limits are per space, per base, per account.** A 429 is transient here as everywhere: the
action is retried rather than dead-lettered, so one lane per rule plus a wider `debounce_ms`
drains a burst instead of losing it. Five strikes on the same key drops that key
([reference → status](reference.md#status)).

## Microsoft Teams: post to a channel

| | |
|---|---|
| **Endpoint** | the callback URL the Workflows app prints when the flow is saved, whole: `endpoints` holds a `file:` reference to it and the template's request line is `POST /` |
| **Auth** | **None — leave `vars` empty.** The URL is the credential, so it stays out of the configuration: the file the reference names holds it, no column does, and a reader of `lazy_wire_fire_target` sees the reference and nothing of the URL |
| **Renders** | one of the two shapes the page shows: `{"text":"…"}`, or an Adaptive Card envelope `{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","contentUrl":null,"content":{…}}]}`, as `Content-Type: application/json` |
| **`expect`** | NULL — every 2xx passes. The page documents the request and not the answer, so there is no status or body to assert |
| **Batch and rate limit** | 28 KB per message; beyond four requests in a second the connection is throttled until the fixed window refreshes → `fire_per = 'key'`, one message per request, one lane, `debounce_ms` / `max_delay_ms` widened for bursts |
| **Deletes** | None. A workflow webhook posts; there is no edit, no delete and no read-back. Turning the flow off or deleting it is done in the Workflows app |

Use the **Workflows app**, not an Office 365 connector: connector-based incoming webhooks were
retired in May 2026, and the trigger to build the flow around is `When a Teams webhook request
is received`.

```sql
-- /run/secrets/teams_webhook, one line, no trailing slash: the callback URL the flow prints,
-- verbatim (its host and path depend on the flow; nothing below the origin is in a column):
-- https://workflow-host.example/<the rest of the callback URL>

INSERT INTO lazy_wire_fire_target (name, kind, endpoints, timeout_ms) VALUES
 ('teams', 'http', 'file:/run/secrets/teams_webhook', 10000);

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms, max_delay_ms) VALUES
 ('teams_order_updates', 'teams', 'key',
  'SELECT id AS _key, CONCAT(''Order '', id, '' is '', status) AS line FROM orders WHERE id IN (:keys)',
  'POST / HTTP/1.1
Content-Type: application/json

{{#each docs}}{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","contentUrl":null,"content":{"type":"AdaptiveCard","version":"1.2","body":[{"type":"TextBlock","text":{{json doc.line}}}]}}]}{{/each}}',
  5000, 60000);

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('teams_order_updates', 'orders', 'update', 'status', '{{new.id}}');
```

```text
POST /<the rest of the callback URL> HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: workflow-host.example
Content-Length: 221

{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","contentUrl":null,"content":{"type":"AdaptiveCard","version":"1.2","body":[{"type":"TextBlock","text":"Order 4213 is shipped"}]}}]}
```

* **Copy the callback URL into the file; do not rebuild it.** Its host and path depend on the
  flow. One line, no trailing slash: `POST /` asks for the endpoint itself, so the URL is called
  exactly as given. The file must sit inside `LAZYWF_SECRET_DIRS` (default
  `/run/secrets:/var/run/secrets`) and an `env:` reference may not name a `LAZYWF_*` variable;
  rewriting it **is** the rotation, applied on the next request with no reload and no restart.
  The log, `/status` and `last_error` name the request `POST https://workflow-host.example/…`, and
  a template's `endpoint.url` is that same origin ([reference → secret http endpoints](reference.md#secret-http-endpoints)).
* **The fallback is weaker.** Keeping the origin in `endpoints` and the callback path in the
  template's request line is what this recipe taught before endpoint references existed, and it is
  what to fall back to only if the URL must stay in a row: the credential then sits in
  `lazy_wire_fire_rule.template`, where anyone who can read that table can post to the channel, a
  configuration snapshot copies it, and rotation means editing the rule.
* **The flow, not the request, decides what the message says.** The trigger hands the payload to
  the flow, which posts it after the response has already gone back, so a 2xx means the trigger
  accepted the body. Text fields in a card take basic Markdown; HTML in them is ignored.
* **Card or text.** Both shapes are documented for a workflow webhook. A MessageCard renders but
  has no interactive action buttons; an Adaptive Card does, with the flow configured to handle
  the response. One message per request keeps the 28 KB limit and the four-per-second window
  trivial to stay inside.
* **A throttled request is not lost.** Beyond the window the connection is throttled and the
  vendor's own advice is exponential backoff; a 429 lands on the transient path here and is
  retried, so a burst of order updates arrives late rather than never.

## Google Chat: post to a space

| | |
|---|---|
| **Endpoint** | the space's webhook URL whole — `https://chat.googleapis.com/v1/spaces/<space>/messages?key=<key>&token=<token>`: `endpoints` holds a `file:` reference to it and the template's request line is `POST /` |
| **Auth** | **None — leave `vars` empty.** The `token` in the query is the secret; `key` is a value shared by all webhooks. Both travel in the URL, so both live in the file the reference names and in no column |
| **Renders** | `{"text":"…"}` as `Content-Type: application/json`; `threadKey` in the body replies into a thread instead of starting one |
| **`expect`** | `json_extract(body, '$.name') IS NOT NULL` — deliberately without `status`, so the 2xx band stays and no success code is asserted |
| **Batch and rate limit** | One message per request, and the per-space quota for `spaces.messages.create` is 1 request per second shared among all webhooks in that space → `fire_per = 'key'`, one lane, wide `debounce_ms` |
| **Deletes** | None from a webhook. Removing a message is `spaces.messages.delete`, an OAuth call with a bot's credentials: a different target kind of problem |

```sql
-- /run/secrets/gchat_webhook, one line, no trailing slash. The credential is the query, so the
-- file holds it whole (`<key>` and `<token>` are placeholders, never values to publish):
-- https://chat.googleapis.com/v1/spaces/<space>/messages?key=<key>&token=<token>

INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms) VALUES
 ('gchat', 'http', 'file:/run/secrets/gchat_webhook',
  'json_extract(body, ''$.name'') IS NOT NULL', 10000);

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms, max_delay_ms) VALUES
 ('gchat_order_updates', 'gchat', 'key',
  'SELECT id AS _key, CONCAT(''Order '', id, '' is '', status) AS line FROM orders WHERE id IN (:keys)',
  'POST / HTTP/1.1
Content-Type: application/json

{{#each docs}}{"text": {{json doc.line}}}{{/each}}',
  10000, 120000);

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('gchat_order_updates', 'orders', 'update', 'status', '{{new.id}}');
```

```text
POST /v1/spaces/<space>/messages?key=<key>&token=<token> HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: chat.googleapis.com
Content-Length: 33

{"text": "Order 4213 is shipped"}
```

* **The URL is the credential, so it lives outside the tables** — and here the credential is the
  **query**, which makes it the clearest case for a reference endpoint: the file holds
  `?key=<key>&token=<token>` and the template's request line is simply `POST /`, the endpoint's own
  query being joined to it last, with `&` after any query the template wrote. `endpoints` holds the
  `file:` reference; the file must sit inside `LAZYWF_SECRET_DIRS` (default
  `/run/secrets:/var/run/secrets`) and an `env:` reference may not name a `LAZYWF_*` variable.
  Rewriting the file **is** the rotation, applied on the next request with no reload and no
  restart, and the log, `/status`, `last_error` and a template's `endpoint.url` name only
  `https://chat.googleapis.com/…`
  ([reference → secret http endpoints](reference.md#secret-http-endpoints)). Keep one webhook per
  space, and keep the tables admin-only all the same.
* **The fallback is weaker.** Keeping `https://chat.googleapis.com` in `endpoints` and writing
  `/v1/spaces/<space>/messages?key=…&token=…` in the template's request line is what this recipe
  taught before endpoint references existed: the secret then sits in
  `lazy_wire_fire_rule.template`, so anyone who can read that table can post to the space, a
  configuration snapshot copies it, and rotation means editing the rule.
* **The answer is a stub of a message.** The response populates only `name` and `thread.name`, so
  `expect` checks that a name came back and nothing more: reading `$.text` out of it would fail a
  message that was posted perfectly well. A non-JSON answer leaves `body` NULL, which the same
  condition rejects.
* **The one-per-second limit is per space, not per webhook.** Two rules posting to the same space
  share it, and a third rule from another service does too. Widen `debounce_ms` and
  `max_delay_ms` rather than adding lanes, and let the transient 429 retry do the waiting.

## Segment: send tracked events in a batch

| | |
|---|---|
| **Endpoint** | `POST /v1/batch` on `https://api.segment.io` (EU workspaces: `https://events.eu1.segmentapis.com`) |
| **Auth** | `kind = 'http.basic'`, `vars.username` → `env:SEGMENT_WRITE_KEY`, `vars.password` → `""`. Basic is documented as the write key as username with an empty password; the alternative is the write key in the body (`"writeKey":"…"`) with `kind = 'http'` |
| **Renders** | `{"batch":[{…},{…}]}` — one event object per changed row. `Content-Type: application/json` is required by the API, and lazywf sends it only because the template writes it |
| **`expect`** | NULL — any 2xx. See the first bullet: a 200 is what Segment answers for almost everything, including events it then drops |
| **Batch and rate limit** | 500 KB per request, 32 KB per event, at most 2500 events per batch; the API recommends staying under 1000 requests per second per workspace, and requests over the limit can come back 429 with `Retry-After` → `batch_max_keys = 200` and `batch_max_bytes` under half a megabyte |
| **Deletes** | None. The HTTP tracking API has no delete: a write key sends events and nothing else. Correcting an event means sending another one that the downstream tool reconciles |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, timeout_ms, vars) VALUES
 ('segment', 'http.basic', 'https://api.segment.io', 10000,
  '{"username": "env:SEGMENT_WRITE_KEY", "password": ""}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('segment_order_events', 'segment', 'batch', 200, 400000,
  'SELECT id AS _key, user_id, id, status,
          CONCAT(''orders:'', id, '':'', status) AS mid
     FROM orders WHERE id IN (:keys)',
  'POST /v1/batch HTTP/1.1
Content-Type: application/json

{"batch": [{{#each docs}}{{#unless @first}}, {{/unless}}{"type": "track", "event": "Order Status Changed", "userId": {{json doc.user_id}}, "properties": {"order_id": {{json doc.id}}, "status": {{json doc.status}}}, "messageId": {{json doc.mid}}}{{/each}}]}');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('segment_order_events', 'orders', 'insert,update', 'status', '{{new.id}}');
```

```text
POST /v1/batch HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: api.segment.io
Authorization: Basic <base64 of <write key>:>
Content-Length: 174

{"batch": [{"type": "track", "event": "Order Status Changed", "userId": "u-7781", "properties": {"order_id": 4213, "status": "shipped"}, "messageId": "orders:4213:shipped"}]}
```

* **A 200 is the default answer, not a verdict.** The API returns 200 for every request except a
  malformed JSON body or an oversized payload, which are 400 — and it returns 200 while rejecting
  events whose batch is over 2500 entries, whose `userId` / `anonymousId` is missing, whose track
  `event` name is missing, or whose `messageId` repeats. The 2xx band catches the 400s on its own
  and nothing here can catch the rest; that half is the Segment Debugger's job, which is why
  `expect` above is left empty instead of pretending to check more.
* **`vars.password` is an empty string, not a missing key.** `http.basic` base64-encodes
  `username:password` as one string, and the documented form is the write key, a colon, and
  nothing after it. A reference that resolves to nothing is a valid credential value; omitting
  `vars.password` is not — the kind requires both names.
* **`messageId` is the deduplication key**, matched on values under 100 characters, so the
  example builds it from the row rather than leaving it out. The same value on a retry is what
  makes a resend harmless; the same value across two genuinely different states of a row is what
  makes the second one disappear. If an order can be `shipped`, `returned` and `shipped` again,
  put something in `mid` that distinguishes them.
* **Size the batch by bytes, not by rows.** 32 KB per event and 500 KB per request are the
  documented limits; `batch_max_bytes` counts the rendered request, so it is the one to set from
  what the template actually produces.

## Klaviyo: create profile events

| | |
|---|---|
| **Endpoint** | `POST /api/event-bulk-create-jobs` on `https://a.klaviyo.com` (one event at a time: `POST /api/events`) |
| **Auth** | `kind = 'http.header'`, `vars.header` = `Authorization`, `vars.value` → `env:KLAVIYO_PRIVATE_KEY`. The header is **not** `Bearer …`: Klaviyo's private key goes out as `Klaviyo-API-Key <key>`, and `http.header` sends `vars.value` verbatim, so the environment variable holds the whole `Klaviyo-API-Key …` string |
| **Renders** | a JSON:API envelope, `{"data":{"type":"event-bulk-create-job","attributes":{"events-bulk-create":{"data":[…groups…]}}}}`, with `Content-Type: application/vnd.api+json`, `Accept: application/vnd.api+json` and the `revision:` header, which the reference lists as required for this endpoint and defaults to `2026-10-15` — all three written by the template |
| **`expect`** | `status = 202` — the documented success code for this endpoint. Pinning a status replaces the 2xx band, so a 429 or a 500 becomes a failed action that is retried instead of a success |
| **Batch and rate limit** | 1000 events per request, 5 MB payload, 100 KB per string value; this endpoint is limited to a burst of 10/s and a steady 150/m → `batch_max_keys = 200`, one lane |
| **Deletes** | None listed. The Events API creates and reads events; there is no endpoint that removes one |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('klaviyo', 'http.header', 'https://a.klaviyo.com', 'status = 202', 15000,
  '{"header": "Authorization", "value": "env:KLAVIYO_PRIVATE_KEY"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('klaviyo_order_events', 'klaviyo', 'batch', 200, 1000000,
  'SELECT id AS _key, customer_email, id, total,
          CONCAT(''orders:'', id) AS uid
     FROM orders WHERE id IN (:keys)',
  'POST /api/event-bulk-create-jobs HTTP/1.1
Content-Type: application/vnd.api+json
Accept: application/vnd.api+json
revision: 2026-10-15

{"data": {"type": "event-bulk-create-job", "attributes": {"events-bulk-create": {"data": [{{#each docs}}{{#unless @first}}, {{/unless}}{"type": "event-bulk-create", "attributes": {"profile": {"data": {"type": "profile", "attributes": {"email": {{json doc.customer_email}}}}}, "events": {"data": [{"type": "event", "attributes": {"properties": {"order_id": {{json doc.id}}, "total": {{json doc.total}}}, "unique_id": {{json doc.uid}}, "metric": {"data": {"type": "metric", "attributes": {"name": "Order Synced"}}}}}]}}}}{{/each}}]}}}}');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, `key`) VALUES
 ('klaviyo_order_events', 'orders', 'insert', '{{new.id}}');
```

```text
POST /api/event-bulk-create-jobs HTTP/1.1
Content-Type: application/vnd.api+json
Accept: application/vnd.api+json
revision: 2026-10-15
User-Agent: lazywf/<version>
Host: a.klaviyo.com
Authorization: Klaviyo-API-Key <the whole value env:KLAVIYO_PRIVATE_KEY resolves to>
Content-Length: 440

{"data": {"type": "event-bulk-create-job", "attributes": {"events-bulk-create": {"data": [{"type": "event-bulk-create", "attributes": {"profile": {"data": {"type": "profile", "attributes": {"email": "ada@example.com"}}}, "events": {"data": [{"type": "event", "attributes": {"properties": {"order_id": 4213, "total": 12.5}, "unique_id": "orders:4213", "metric": {"data": {"type": "metric", "attributes": {"name": "Order Synced"}}}}}]}}}]}}}}
```

* **`vars.value` is the whole header value.** The kind writes `<header>: <value>` with the value
  resolved as a credential and otherwise untouched, so the secret has to include the
  `Klaviyo-API-Key ` prefix. An invalid or missing key is reported as a 400, which is a refusal
  and not a retry, so this mistake fails loudly with five strikes on each key rather than
  quietly. Writing the header in the template instead would work, and would put the key in a
  plain column that every template reader can see.
* **`revision` is required and comes from the template.** One header per `http.header` target is
  the limit, so the version header is the template's job. `2026-10-15` is the revision this
  endpoint currently defaults to; pin the one you tested against, because the value picks the
  shape of the request as much as the shape of the answer.
* **202 means queued, and duplicates go unnoticed.** The success answer says the event was
  validated and submitted for processing, not processed. Events are deduplicated on
  (`profile`, `metric`, `unique_id`), so a 202 can be a batch half-discarded — and inside one
  bulk request a repeated `unique_id` is worse than that: it answers 400 and processes nothing,
  which is why `uid` above is built from the order id and not left to default to the timestamp to
  the second.
* **`unique_id` also buys you a safe retry.** Resending the same event with the same value is a
  no-op once the first has landed, which is what makes a transient 5xx worth retrying; without
  it, a retry can create a second event.

## HubSpot: upsert contacts and archive them

| | |
|---|---|
| **Endpoint** | `POST /crm/v3/objects/contacts/batch/upsert` on `https://api.hubapi.com`; deletes are `POST /crm/v3/objects/contacts/batch/archive` on their own target |
| **Auth** | `kind = 'http.bearer'`, `vars.token` → `env:HUBSPOT_ACCESS_TOKEN`. The batch endpoints document `Authorization: Bearer <token>`; the token of a **privately distributed app** — HubSpot's name for an app distributed only inside your own account — is the static-token option, and it is the kind the rate limits below are written for |
| **Renders** | `{"inputs":[{"id":"…","idProperty":"email","properties":{"email":"…","firstname":"…"}}]}` — `id` is the value of the property named by `idProperty` |
| **`expect`** | `status = 200` for the upsert, `status = 204` for the archive. Pinning them keeps a 207 from counting as a whole-batch success |
| **Batch and rate limit** | 100 inputs per request on the batch endpoints → `batch_max_keys = 100`. A privately distributed app is limited per 10 seconds (100 per app on Free and Starter, 190 on Professional and Enterprise) and per day per account (250,000 / 625,000 / 1,000,000 by tier) |
| **Deletes** | `batch/archive` takes record ids only — `{"inputs":[{"id":"430001"}]}` — and answers 204 with no body. Archived contacts are restorable for 90 days |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('hubspot',       'http.bearer', 'https://api.hubapi.com', 'status = 200', 15000,
  '{"token": "env:HUBSPOT_ACCESS_TOKEN"}'),
 ('hubspot_delete', 'http.bearer', 'https://api.hubapi.com', 'status = 204', 15000,
  '{"token": "env:HUBSPOT_ACCESS_TOKEN"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('hubspot_upsert_contacts', 'hubspot', 'batch', 100, 200000,
  'SELECT id AS _key, email, first_name, last_name FROM customers WHERE id IN (:keys)',
  'POST /crm/v3/objects/contacts/batch/upsert HTTP/1.1
Content-Type: application/json

{"inputs": [{{#each docs}}{{#unless @first}}, {{/unless}}{"id": {{json doc.email}}, "idProperty": "email", "properties": {"email": {{json doc.email}}, "firstname": {{json doc.first_name}}, "lastname": {{json doc.last_name}}}}{{/each}}]}'),
 ('hubspot_archive_contacts', 'hubspot_delete', 'batch', 100, 50000,
  NULL,
  'POST /crm/v3/objects/contacts/batch/archive HTTP/1.1
Content-Type: application/json

{"inputs": [{{#each keys}}{{#unless @first}}, {{/unless}}{"id": {{json this}}}{{/each}}]}');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('hubspot_upsert_contacts',   'customers', 'insert,update', 'email,first_name,last_name', '{{new.id}}'),
 ('hubspot_archive_contacts',  'customers', 'delete',        NULL, '{{new.hs_contact_id}}');
```

```text
POST /crm/v3/objects/contacts/batch/upsert HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: api.hubapi.com
Authorization: Bearer <the token env:HUBSPOT_ACCESS_TOKEN resolves to>
Content-Length: 150

{"inputs": [{"id": "ada@example.com", "idProperty": "email", "properties": {"email": "ada@example.com", "firstname": "Ada", "lastname": "Lovelace"}}]}
```

```text
POST /crm/v3/objects/contacts/batch/archive HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: api.hubapi.com
Authorization: Bearer <the token env:HUBSPOT_ACCESS_TOKEN resolves to>
Content-Length: 30

{"inputs": [{"id": "430001"}]}
```

* **The delete rule is keyed by HubSpot's id, not by email.** `batch/archive` accepts record ids
  and has no `idProperty`, so `hs_contact_id` has to be a column on `customers`, filled when the
  contact is created — the upsert's answer carries the `id` it assigned. On a delete the key
  template reads the old image, so `{{new.hs_contact_id}}` is the deleted row's value. A row
  whose `hs_contact_id` is NULL marks nothing, which is the right outcome for a customer that was
  never in HubSpot.
* **207 is the status to think about.** A batch answers 207 Multi-Status only when multi-status
  error handling is enabled for the request; without it there are no per-input statuses to read.
  Pinning `status = 200` means a partial answer is treated as a failure of the batch and retried,
  which is the safe reading of an upsert.
* **`423 Locked` says slow down, not stop.** It comes from pushing a large volume of writes in a
  short window, and the documented advice is at least two seconds between requests. It is not on
  the transient list, so it is refused rather than waited on: a refusal charges a strike to a key
  only when other keys of the same action got through, and when the whole batch is refused the
  keys simply wait for the rule to be fixed. Keep `batch_max_keys` at 100 and `debounce_ms`
  generous rather than firing continuously. A `429` is transient, and its body names the policy
  hit in `policyName`.
* **`idProperty` is what makes this an upsert**: a record with that property's value exists, it
  is updated; not, it is created. Use a unique property, and check how your account deduplicates
  contacts on that property before trusting a batch of them.

## Airtable: create, update and delete records

| | |
|---|---|
| **Endpoint** | `POST /v0/<baseId>/<tableId>` to create and `PATCH /v0/<baseId>/<tableId>` to update or upsert on `https://api.airtable.com`; deletes are `DELETE /v0/<baseId>/<tableId>?records[]=…` on their own target |
| **Auth** | `kind = 'http.bearer'`, `vars.token` → `env:AIRTABLE_TOKEN`. A PAT — an access token minted in your own account — is the static option, and Bearer is the only scheme; the token needs `data.records:write`, which covers creating, editing and deleting records in the bases granted to it |
| **Renders** | `{"records":[{"fields":{"…":…}}]}` for both create and update; upserts add `"performUpsert":{"fieldsToMergeOn":["…"]}` |
| **`expect`** | `status = 200` for all three. The body is worth reading by hand, not by `expect`: a create can answer 200 with a per-record `details.message` of `partialSuccess` or `cellsRedactedDueToLimitedPermissions` |
| **Batch and rate limit** | 5 requests per second per base and 50 per second per token owner, and a 429 asks for a 30 second pause → `batch_max_keys = 50` and one lane. The API reference prints no record count per write request, so the rate limits are what `batch_max_keys` is set against here |
| **Deletes** | `DELETE` with the record ids **in the query string** — `?records[]=recA&records[]=recB` — answering `{"records":[{"id":"recA","deleted":true}]}` |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('airtable',        'http.bearer', 'https://api.airtable.com', 'status = 200', 15000,
  '{"token": "env:AIRTABLE_TOKEN"}'),
 ('airtable_delete', 'http.bearer', 'https://api.airtable.com', 'status = 200', 15000,
  '{"token": "env:AIRTABLE_TOKEN"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('airtable_items', 'airtable', 'batch', 50, 100000,
  'SELECT id AS _key, id, name, status FROM items WHERE id IN (:keys)',
  'POST /v0/appXXXXXXXXXXXXXX/tblYYYYYYYYYYYYYY HTTP/1.1
Content-Type: application/json

{"records": [{{#each docs}}{{#unless @first}}, {{/unless}}{"fields": {"Order": {{json doc.id}}, "Name": {{json doc.name}}, "Status": {{json doc.status}}}}{{/each}}]}'),
 ('airtable_delete_items', 'airtable_delete', 'batch', 50, 20000,
  NULL,
  'DELETE /v0/appXXXXXXXXXXXXXX/tblYYYYYYYYYYYYYY?{{#each keys}}{{#unless @first}}&{{/unless}}records[]={{url_encode this}}{{/each}} HTTP/1.1
');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('airtable_items',        'items', 'insert,update', 'name,status', '{{new.id}}'),
 ('airtable_delete_items', 'items', 'delete',        NULL,          '{{new.at_record_id}}');
```

```text
POST /v0/appXXXXXXXXXXXXXX/tblYYYYYYYYYYYYYY HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: api.airtable.com
Authorization: Bearer <the token env:AIRTABLE_TOKEN resolves to>
Content-Length: 79

{"records": [{"fields": {"Order": 4213, "Name": "Bowl", "Status": "shipped"}}]}
```

```text
DELETE /v0/appXXXXXXXXXXXXXX/tblYYYYYYYYYYYYYY?records[]=rec560UJdUtocSouk HTTP/1.1
User-Agent: lazywf/<version>
Host: api.airtable.com
Authorization: Bearer <the token env:AIRTABLE_TOKEN resolves to>
```

* **Table ids beat table names.** The path takes either, and the vendor recommends the id so a
  rename in the base does not silently turn every action into a 404. Put it in the template
  literally rather than in an `endpoints` prefix, which would gain a trailing slash before `POST /`.
* **`PATCH` versus `PUT` matters more than it looks.** `PATCH` leaves the fields you did not send
  alone; `PUT` clears every cell you left out. The create above is a `POST`; the update rule is
  the same body sent as `PATCH`. Use `PUT` only when "whatever is not in this row is gone" is what
  you mean.
* **`performUpsert` matches on unique fields**, one to three of them, and a record that matches
  more than one existing row fails the request. It is a property of the request body, next to
  `records`, and once it is set the `id` of each record becomes optional: records without one are
  matched by `fieldsToMergeOn` instead — created on no match, updated on one.
* **The delete ids are in the query string, not the body**, which is why that rule renders a
  `DELETE` with no body at all — a `DELETE` with an empty body goes out without `Content-Length`.
  The ids are the Airtable ones (`rec…`), stored in `at_record_id`; `url_encode` is there because
  a key that turns out not to be a record id should be escaped rather than break the request line.
* **A 200 can carry a caveat.** The response shape allows `records[]` entries with a `details`
  object whose `message` says `partialSuccess` or `cellsRedactedDueToLimitedPermissions`. One
  condition over one row cannot judge that per record, so the check is left as a step to run
  against your own base.

## Notion: create a page and archive it

| | |
|---|---|
| **Endpoint** | `POST /v1/pages` on `https://api.notion.com`; archiving is `PATCH /v1/pages/<page_id>` on its own target |
| **Auth** | `kind = 'http.bearer'`, `vars.token` → `env:NOTION_TOKEN` (an internal integration's secret). The required `Notion-Version` header is written by the template |
| **Renders** | `{"parent":{"database_id":"…"},"properties":{"Name":{"title":[{"text":{"content":"…"}}]}}}` as `Content-Type: application/json`; other property types follow the shapes on the same reference page |
| **`expect`** | `status = 200 AND json_unquote(json_extract(body, '$.object')) = 'page'` for the create, `status = 200 AND json_contains(body, 'true', '$.in_trash')` for the archive. A create that carries `markdown` with `allow_async` answers 202 with an `async_task` object instead, which the first condition refuses |
| **Batch and rate limit** | One page per request → `fire_per = 'key'`. Per connection: 180 requests per 60 second window (an average of 3 per second), 600 on Business and Enterprise plans; over the limit answers 429 with `Retry-After` |
| **Deletes** | There is no delete endpoint. A page is archived with `PATCH /v1/pages/<page_id>` and `{"in_trash":true}`, which answers 200 with the page |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('notion',         'http.bearer', 'https://api.notion.com',
  'status = 200 AND json_unquote(json_extract(body, ''$.object'')) = ''page''', 15000,
  '{"token": "env:NOTION_TOKEN"}'),
 ('notion_archive', 'http.bearer', 'https://api.notion.com',
  'status = 200 AND json_contains(body, ''true'', ''$.in_trash'')', 15000,
  '{"token": "env:NOTION_TOKEN"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms, max_delay_ms) VALUES
 ('notion_order_pages', 'notion', 'key',
  'SELECT id AS _key, CONCAT(''Order '', id, '' is '', status) AS title FROM orders WHERE id IN (:keys)',
  'POST /v1/pages HTTP/1.1
Content-Type: application/json
Notion-Version: 2026-03-11

{{#each docs}}{"parent": {"database_id": "0a1b2c3d4e5f4a5b8c9d0e1f2a3b4c5d"}, "properties": {"Name": {"title": [{"text": {"content": {{json doc.title}}}}]}}}{{/each}}',
  5000, 60000),
 ('notion_archive_pages', 'notion_archive', 'key',
  NULL,
  'PATCH /v1/pages/{{#each keys}}{{url_encode this}}{{/each}} HTTP/1.1
Content-Type: application/json
Notion-Version: 2026-03-11

{"in_trash": true}',
  5000, 60000);

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('notion_order_pages',   'orders', 'update', 'status', '{{new.id}}'),
 ('notion_archive_pages', 'orders', 'delete', NULL,     '{{new.notion_page_id}}');
```

```text
POST /v1/pages HTTP/1.1
Content-Type: application/json
Notion-Version: 2026-03-11
User-Agent: lazywf/<version>
Host: api.notion.com
Authorization: Bearer <the token env:NOTION_TOKEN resolves to>
Content-Length: 148

{"parent": {"database_id": "0a1b2c3d4e5f4a5b8c9d0e1f2a3b4c5d"}, "properties": {"Name": {"title": [{"text": {"content": "Order 4213 is shipped"}}]}}}
```

* **Two headers, and only one of them is a credential.** `Authorization` comes from the kind;
  `Notion-Version` is required by every endpoint and has to be written by the template, since a
  target carries one credential header. It is a version string, so a template header is the right
  place for it. `2026-03-11` is the latest version the reference lists; pin the one you tested.
* **`parent` decides where the page lands** — `database_id` for a row of a database, and a page
  parent for a page under a page. A database row is a page whose `properties` are the database's
  properties, so the property names in the template are your column titles, spelled as they are
  in the database.
* **Archiving is a PATCH on the page id**, so the delete rule is keyed by `notion_page_id` — the
  id Notion returned when the page was created. `in_trash` is the only spelling accepted from API
  version `2026-03-11` on (`archived` was its deprecated alias before that), and the same column
  lets a later rule write `{"in_trash": false}` to bring a page back.
* **429 is per connection and per 60 second window**, with `Retry-After` telling you how long, so
  the burst is allowed and one lane with a sane `debounce_ms` is enough; the transient retry waits
  it out.

## Zapier: trigger a Zap from a catch hook

| | |
|---|---|
| **Endpoint** | the hook URL the Zap's Test tab prints, whole — `https://hooks.zapier.com/hooks/catch/<user-id>/<hook>`: `endpoints` holds a `file:` reference to it and the template's request line is `POST /` |
| **Auth** | **None — leave `vars` empty.** Zapier's article for the catch triggers documents no key and no signature: the URL is the only access control, and it embeds the owner's Zapier ID (a transferred Zap gets a new URL, and every sender has to be told). The `file:` keeps it out of the configuration file and out of `lazy_wire_fire_target` |
| **Renders** | one JSON array of the rows' documents, `[{…},{…}]`: [Zapier documents](https://help.zapier.com/hc/en-us/articles/8496288690317-Trigger-Zap-workflows-from-webhooks) an array of JSON objects as triggering the Zap **once per object** — one request per batch, one Zap run per row |
| **`expect`** | `status = 200` — a live Zap's hook answers 200 and a deleted or turned-off one answers 404; the delay between the two is the first bullet |
| **Batch and rate limit** | 10 MB per request (2 MB for a Catch Raw Hook); 20,000 requests per 5 minutes per user and 1,000 per 5 minutes per Zap on legacy routes → `batch_max_keys = 50`, `batch_max_bytes = 65536`, one lane. Past the limit Zapier answers 429, which is transient |
| **Deletes** | None. A hook receives; turning the Zap off or deleting it happens in Zapier |

The trigger is **Webhooks by Zapier** with the **Catch Hook** event (the body arrives parsed);
**Catch Raw Hook** is the variant whose body arrives unparsed, headers included, and it carries
the 2 MB limit.

```sql
-- /run/secrets/zapier_hook, one line, no trailing slash: the trigger URL the Zap's Test tab
-- prints, verbatim (the user id and hook below are placeholders):
-- https://hooks.zapier.com/hooks/catch/123456/abcdef

INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms) VALUES
 ('zapier', 'http', 'file:/run/secrets/zapier_hook', 'status = 200', 10000);

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('zapier_order_events', 'zapier', 'batch', 50, 65536,
  'SELECT id AS _key, id, status, updated_at FROM orders WHERE id IN (:keys)',
  'POST / HTTP/1.1
Content-Type: application/json

[{{#each docs}}{{#unless @first}}, {{/unless}}{{json doc}}{{/each}}]');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('zapier_order_events', 'orders', 'insert,update', 'status', '{{new.id}}');
```

```text
POST /hooks/catch/123456/abcdef HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: hooks.zapier.com
Content-Length: 130

[{"id":4213,"status":"shipped","updated_at":"2026-11-11 09:15:02"},{"id":4214,"status":"paid","updated_at":"2026-11-11 09:15:04"}]
```

* **One array, one run per object.** Zapier's own example posts three objects and runs the Zap
  three times, which is why the body here is the plain array of documents and nothing wraps it:
  `batch_max_keys` doubles as the number of Zap runs one flush buys.
* **The 404 of a dead Zap comes late.** Zapier: with the Zap turned off or deleted, incoming
  requests get 404 — "however, there is a system update delay of up to several hours before the
  404 response takes effect, during which the URL will continue to return a 200 response."
  Through that window lazywf keeps settling rows as though the Zap ran: a retired Zap is an
  operational fact the rows cannot see.
* **A 200 is accepted, not run.** "Webhooks may delay passing on data to other steps if there are
  a lot of requests coming at the same time", and in busy periods Zapier "may return a 200 status
  code but delay webhook processing by several minutes": nothing in the row says the Zap's actions
  ran.
* **The limits are per user and per Zap.** [Zapier's rate-limits
  article](https://help.zapier.com/hc/en-us/articles/29972220283789-Webhooks-by-Zapier-rate-limits)
  gives 20,000 requests per 5 minutes per user and 1,000 per 5 minutes per Zap for legacy webhook
  routes (those without the user id in the URL), answered 429 past the limit — transient here, so
  one lane and a sane `debounce_ms` ride a burst out. Confirm the current numbers for your plan.
* **The URL is the bearer credential, so it lives outside the tables** — one line in a file
  inside `LAZYWF_SECRET_DIRS`, rewriting it is the rotation, and
  `LAZYWF_ALLOWED_HOSTS=hooks.zapier.com` stops a rewritten file from moving the target
  ([reference → secret http endpoints](reference.md#secret-http-endpoints)). Zapier documents no
  signature for the catch triggers, so there is nothing to verify and nothing to set: a receiver
  that can check an HMAC is what [the signed-webhook
  recipe](recipes.md#a-signed-webhook-to-an-automation-hub) is for.

## Make: trigger a scenario from its custom webhook

| | |
|---|---|
| **Endpoint** | `https://hook.eu2.make.com` (the region host your webhook uses) in `endpoints`, the [Custom webhook](https://apps.make.com/gateway)'s 32-character string in the request line — plain text, because `http.header` is not one of the four plain http kinds and takes no reference endpoint; the API key below is what stops the URL from being the only credential |
| **Auth** | `kind = 'http.header'`, `vars.header` = `x-make-apikey`, `vars.value` → `file:/run/secrets/make_apikey`. Make documents an optional API key per webhook — "In requests, include API keys in the `x-make-apikey` header" — created in the webhook's settings, at most 512 ASCII characters, shown once |
| **Renders** | one JSON array of the rows' documents, `[{…},{…}]`. Declare the webhook's data structure (Make recommends it) as an array of objects matching the select: without one "Make accepts all incoming data without validation", with one a request that fails validation is "rejected with HTTP status code 400" |
| **`expect`** | `status = 200 AND text = 'Accepted'` — Make's documented default answer while the call enters the scenario's queue; its other default answers are in the first bullet |
| **Batch and rate limit** | 5 MB (5 242 880 bytes) per request "regardless of the subscription tier" → `batch_max_keys = 50`, `batch_max_bytes = 262144`, one lane. A full webhook queue answers 400 — a refusal, so a scenario that cannot drain strikes keys rather than waiting |
| **Deletes** | None. The webhook receives; deactivating the scenario happens in Make |

```sql
-- /run/secrets/make_apikey, one line: the value you typed into the webhook's keychain when you
-- created it (Make shows it once and never again).

INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('make', 'http.header', 'https://hook.eu2.make.com',
  'status = 200 AND text = ''Accepted''', 10000,
  '{"header": "x-make-apikey", "value": "file:/run/secrets/make_apikey"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('make_order_events', 'make', 'batch', 50, 262144,
  'SELECT id AS _key, id, status, updated_at FROM orders WHERE id IN (:keys)',
  'POST /<the-32-character-webhook-string> HTTP/1.1
Content-Type: application/json

[{{#each docs}}{{#unless @first}}, {{/unless}}{{json doc}}{{/each}}]');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('make_order_events', 'orders', 'insert,update', 'status', '{{new.id}}');
```

```text
POST /<the-32-character-webhook-string> HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: hook.eu2.make.com
x-make-apikey: <the key file:/run/secrets/make_apikey holds>
Content-Length: 130

[{"id":4213,"status":"shipped","updated_at":"2026-11-11 09:15:02"},{"id":4214,"status":"paid","updated_at":"2026-11-11 09:15:04"}]
```

* **The answer is about the queue, not the scenario.** Make's own table: `200 Accepted` when the
  call enters the queue, `400 Queue is full.` when it will not fit, and — only when the scenario
  contains the **Webhook Response** module — `500 Scenario failed to complete.` when the scenario
  errors, with that module's own status and body answered instead of the defaults and `200
  Accepted` if the scenario has not answered within 180 seconds. A 500 is transient here, so the
  whole batch is retried and the scenario runs again for the same rows: make the scenario
  idempotent per document, or have the Webhook Response module answer a 4xx of your choosing for
  a failure you do not want replayed.
* **The data structure is a 400 switch.** With one, a payload that fails validation is refused
  with 400 — a refusal here, so a drifted template strikes keys instead of waiting — and without
  one everything is accepted and the error surfaces late, in a module two steps downstream.
  Declare it as the rendered array, and use Make's **Detect new values** to confirm the shape.
* **`x-make-apikey` is reserved inside Make.** With request headers exposed to the scenario, that
  header's value is sanitized away before the scenario can map it: it is a transport credential
  to Make, not payload content, which is what `vars.value` (resolved per request, never rendered
  by a template) is for.
* **The account is rate limited too.** Make's webhook article gives 300 requests per 10 seconds
  across an account's webhooks and answers 429 past it — transient — and the article, not this
  recipe, is where the current number for a plan lives.

## n8n: run a workflow from its Webhook node

| | |
|---|---|
| **Endpoint** | the node's **Production URL**, `https://<n8n-host>/webhook/<path>` (the path is randomly generated by default), in plain `endpoints` — with the node's Authentication left at None, take the Zapier shape instead: `kind = 'http'` with a `file:` reference holding the whole URL |
| **Auth** | `kind = 'http.header'`, `vars.header` = `x-api-key` — any name, as long as it equals the **Name** of the [Header Auth credential](https://docs.n8n.io/integrations/builtin/credentials/webhook.html) — `vars.value` → `file:/run/secrets/n8n_webhook_key`. In the node set Authentication to **Header Auth** and create that credential with the same Name and Value |
| **Renders** | one JSON array of the rows' documents, `[{…},{…}]`; the workflow sees the request as `{ body, headers, params, query }`, so the array arrives at `$json.body` |
| **`expect`** | `status = 200` — with **Respond: Immediately** the node answers its Response Code (200 by default) and the message `Workflow got started` the moment the execution begins; mirror any Response Code you change here |
| **Batch and rate limit** | 16 MB per request (`N8N_PAYLOAD_SIZE_MAX`, self-hosted) → `batch_max_keys = 50`, `batch_max_bytes = 262144`. One webhook per path-and-method; on n8n Cloud an answer slower than 100 seconds becomes a Cloudflare 524, which Respond: Immediately avoids by construction |
| **Deletes** | None. The workflow runs; unpublishing it in n8n stops the production URL |

```sql
-- /run/secrets/n8n_webhook_key, one line: the Value of the Header Auth credential in n8n.

INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms, vars) VALUES
 ('n8n', 'http.header', 'https://automation.example', 'status = 200', 10000,
  '{"header": "x-api-key", "value": "file:/run/secrets/n8n_webhook_key"}');

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('n8n_order_events', 'n8n', 'batch', 50, 262144,
  'SELECT id AS _key, id, status, updated_at FROM orders WHERE id IN (:keys)',
  'POST /webhook/6f4d2a9c HTTP/1.1
Content-Type: application/json

[{{#each docs}}{{#unless @first}}, {{/unless}}{{json doc}}{{/each}}]');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('n8n_order_events', 'orders', 'insert,update', 'status', '{{new.id}}');
```

```text
POST /webhook/6f4d2a9c HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: automation.example
x-api-key: <the value file:/run/secrets/n8n_webhook_key holds>
Content-Length: 130

[{"id":4213,"status":"shipped","updated_at":"2026-11-11 09:15:02"},{"id":4214,"status":"paid","updated_at":"2026-11-11 09:15:04"}]
```

* **Production means published.** The test URL answers only after you select **Listen for test
  event**, and for 120 seconds; the production URL is registered when you publish the workflow and
  lives until it is unpublished. Point lazywf at the production URL and build against the test
  URL — and check what your n8n answers for a production URL whose workflow is unpublished before
  trusting `expect` to catch that mistake.
* **Only Run If is a 200 that runs nothing.** A request whose expression is false "receive[s] a
  200 response without creating an execution", and an expression that fails to evaluate lets the
  request through with a logged warning. A wrong filter therefore settles every row here: test
  the expression before the rule goes near production data.
* **The IP allowlist answers 403, which refuses.** Outside the allowlist n8n throws 403 — a
  refusal here, charging strikes rather than waiting — and behind a reverse proxy even allowed
  IPs fail until `N8N_PROXY_HOPS` counts the proxies correctly.
* **The auth options map unevenly.** Header Auth is the one this recipe uses; Basic Auth maps to
  `http.basic` (`vars.username`, `vars.password`); JWT auth and n8n User Auth (OAuth2) have no
  counterpart among the four plain http kinds. Left at **None**, the random path *is* the
  credential — then use `kind = 'http'` with the whole URL in a `file:` reference
  ([reference → secret http endpoints](reference.md#secret-http-endpoints)) instead of a header.

## Microsoft Power Automate: run an instant cloud flow

| | |
|---|---|
| **Endpoint** | the **HTTP POST URL** the trigger prints once the flow is saved, whole: `endpoints` holds a `file:` reference to it and the template's request line is `POST /` |
| **Auth** | **None — leave `vars` empty.** The trigger has no shared secret: what Microsoft documents is *who can trigger the flow* — Anyone, Any user in my tenant, or Specific users in my tenant — and with **Anyone** (the form [Microsoft's lead-management instructions](https://learn.microsoft.com/en-us/partner-center/marketplace-offers/commercial-marketplace-lead-management-instructions-https) use) the URL is the credential, so it lives in a file and in no column. Check which setting your flow carries |
| **Renders** | one JSON array of the rows' documents, `[{…},{…}]`; the trigger's **Request Body JSON Schema** is free text you write: declare `"type": "array"` whose `items` object lists the columns the select returns |
| **`expect`** | `status = 200`, with a `Response` action at the end of the trigger branch answering 200. Without a Response action the same family of request trigger is answered as soon as the trigger fires — Microsoft's Logic Apps documentation of it says 202 Accepted: started, not finished |
| **Batch and rate limit** | the request-trigger pages consulted print no payload or rate limit for the trigger → `batch_max_keys = 50`, `batch_max_bytes = 65536`, one lane, and check the limits shown for your flow |
| **Deletes** | None. The flow runs; turning it off or deleting it happens in Power Automate |

```sql
-- /run/secrets/powerautomate_flow, one line, no trailing slash: the HTTP POST URL the trigger
-- shows after the flow is saved, verbatim (the host and path below are placeholders):
-- https://flow-host.example/<the path and query the trigger generated>

INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms) VALUES
 ('powerautomate', 'http', 'file:/run/secrets/powerautomate_flow', 'status = 200', 10000);

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, batch_max_keys, batch_max_bytes, selects, template) VALUES
 ('powerautomate_order_events', 'powerautomate', 'batch', 50, 65536,
  'SELECT id AS _key, id, status, updated_at FROM orders WHERE id IN (:keys)',
  'POST / HTTP/1.1
Content-Type: application/json

[{{#each docs}}{{#unless @first}}, {{/unless}}{{json doc}}{{/each}}]');

INSERT INTO lazy_wire_fire_trigger (rule, source_table, `on`, changed_columns, `key`) VALUES
 ('powerautomate_order_events', 'orders', 'insert,update', 'status', '{{new.id}}');
```

```text
POST /<the path and query the trigger generated> HTTP/1.1
Content-Type: application/json
User-Agent: lazywf/<version>
Host: flow-host.example
Content-Length: 130

[{"id":4213,"status":"shipped","updated_at":"2026-11-11 09:15:02"},{"id":4214,"status":"paid","updated_at":"2026-11-11 09:15:04"}]
```

* **The URL exists only after the first save.** The instant cloud flow's trigger prints its HTTP
  POST URL when the flow is saved — that is the moment the file gets its content; on the lazywf
  side rewriting the file is the rotation, as for every secret endpoint.
* **The answer is yours to define.** Put a `Response` action at the end of the trigger branch and
  pin `expect` to exactly what it answers; without one, the platform answers for you (Microsoft's
  Logic Apps documentation of the same request trigger: no Response action means the call is
  answered 202 Accepted as soon as the trigger fires, and a beyond-timeout request gets 408).
  Test the flow in the designer and mirror what you see.
* **Premium is Microsoft's call.** The request trigger appears marked `[PREMIUM]` in Microsoft's
  documentation; whether an instant flow with an HTTP trigger runs on your licensing is a check in
  your own tenant, made before the rule goes near production data.
* **Nothing here is claimed about retries.** The pages consulted say nothing about whether the
  platform redelivers a request whose answer never arrived; lazywf is at-least-once on its side,
  so the flow must be idempotent per document — an upsert keyed on the order id, say — or both
  sides' retries will duplicate.

## What these recipes do not claim

The vendor pages consulted do not settle everything a recipe would want, so these are
verification steps against your own account, not instructions:

* **Microsoft Teams**: no success status code or response body is documented for a workflow
  webhook, so `expect` is NULL and any 2xx counts. The callback URL's host and path are per-flow,
  which is why they are not written out above, and which of the documented bodies a given flow
  takes is whatever its trigger was configured to accept.
* **Google Chat**: no success status code is documented for a webhook post, so the `expect` above
  leaves the 2xx band alone and checks only the returned `name`.
* **Segment**: the response body of a batch is not documented on the HTTP API page, so nothing
  above reads it — the documented facts are the 200-for-almost-everything answer, the 400 for bad
  JSON and oversized payloads, and the silently rejected events over 2500 in a batch.
* **Klaviyo**: that no delete endpoint exists is what the Events API's own list of endpoints and
  use cases shows (creating and fetching events, and their related profiles and metrics); the
  absence is stated that way rather than as a guarantee about every version of the API.
* **HubSpot**: how a batch behaves when one input is refused without multi-status error handling
  enabled is not written down, so the recipe pins `status = 200` and lets a partial answer be a
  failure. Which property you can safely use as `idProperty` depends on your account's unique and
  deduplication settings.
* **Airtable**: no per-request record count for create, update or delete appears in the API
  reference, and the API reference does not say what a delete answers for an id that does not
  exist. `batch_max_keys = 50` is sized against the documented request rates, not against a
  documented cap; verify an oversized batch's failure mode in your own base.
* **Notion**: the `title` element in the example is the shape the reference page shows, and other
  property types are written as their own pages write them. What an unknown property name answers
  is not stated, so the names above are the ones the recipe was read from the database's own
  columns; check them against yours.
* **Zapier**: the 200 answer is documented as a status, not a body, so `expect` pins the status
  and reads nothing else; and that the catch triggers document no signature is a statement about
  the article consulted, not about every Zapier product. Confirm both in your Zap's Test tab.
* **Make**: the 300-requests-per-10-seconds figure is the one printed in Make's webhook article —
  verify the current number for your plan. The `Accepted` and `Queue is full.` answers are the
  documented defaults; what your scenario answers once a Webhook Response module is in it is
  yours, and only you can pin it.
* **n8n**: what the production URL answers while the workflow is unpublished is not stated in the
  pages consulted, and neither is how Header Auth answers a wrong value — both are checks to make
  in your own n8n before trusting `expect` to notice the mistake.
* **Power Automate**: no claim about retries, duplicates or a trigger timeout — the pages
  consulted print none; the `[PREMIUM]` marking is quoted from Microsoft's documentation and your
  licensing is a tenant check; and which of the three "who can trigger" settings your flow carries
  is not something the rows can show you.

## Not covered here

Google Sheets, Microsoft Excel and Salesforce are left out of this file: they authenticate through
OAuth flows, and lazywf has no OAuth client — no authorization code exchange, no token refresh.
Their APIs are reachable only with a static token or a webhook URL, which is what every recipe
above uses.
