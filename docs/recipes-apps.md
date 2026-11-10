# Business app recipes

Copy-ready recipes for the `http` kinds pointed at hosted business applications: two chat
platforms, an event pipeline, an email platform and three systems of record — Microsoft Teams,
Google Chat, Segment, Klaviyo, HubSpot, Airtable and Notion. Same kinds and the same three
tables as [the http recipes](recipes.md). What changes is which vendor limit binds, which
header a service wants besides its auth header, which id a delete has to be keyed by, and how
much a 2xx is actually worth.

> **These are examples; CI does not run them.** The test suite exercises the built-in sinks and
> the `http` kinds against OpenSearch, Elasticsearch and Varnish containers, never against
> Teams, Google Chat, Segment, Klaviyo, HubSpot, Airtable or Notion. The endpoint forms, header
> names and `expect` clauses below come from each vendor's own documentation and from the
> request-building code. Try one against your own account before production data drives it.

Three things are the same in every recipe here:

* **`endpoints` is the origin** and the template's request line carries the path and the query.
  A prefix in `endpoints` is put in front of every path, so a prefix plus `POST /` would gain a
  trailing slash; the services below all take their path in the template instead.
* **The credential is a reference in `vars`** (`env:NAME` or `file:/path`), resolved on every
  request, never rendered by a template. The two webhook recipes are the exception, and they are
  spelled out: there the secret is inside the URL, which is a plain column.
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
| **Endpoint** | the callback URL the Workflows app prints when the flow is saved. `endpoints` is its origin; the request line is the rest of that URL, copied verbatim |
| **Auth** | **None — leave `vars` empty.** The URL is the credential, and it sits in the configuration as plain text: whoever can read the row can post to that channel |
| **Renders** | one of the two shapes the page shows: `{"text":"…"}`, or an Adaptive Card envelope `{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","contentUrl":null,"content":{…}}]}`, as `Content-Type: application/json` |
| **`expect`** | NULL — every 2xx passes. The page documents the request and not the answer, so there is no status or body to assert |
| **Batch and rate limit** | 28 KB per message; beyond four requests in a second the connection is throttled until the fixed window refreshes → `fire_per = 'key'`, one message per request, one lane, `debounce_ms` / `max_delay_ms` widened for bursts |
| **Deletes** | None. A workflow webhook posts; there is no edit, no delete and no read-back. Turning the flow off or deleting it is done in the Workflows app |

Use the **Workflows app**, not an Office 365 connector: connector-based incoming webhooks were
retired in May 2026, and the trigger to build the flow around is `When a Teams webhook request
is received`.

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, timeout_ms) VALUES
 ('teams', 'http', 'https://workflow-host.example', 10000);

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms, max_delay_ms) VALUES
 ('teams_order_updates', 'teams', 'key',
  'SELECT id AS _key, CONCAT(''Order '', id, '' is '', status) AS line FROM orders WHERE id IN (:keys)',
  'POST /<the rest of the callback URL> HTTP/1.1
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

* **Copy the callback URL; do not rebuild it.** Its host and path depend on the flow, and only
  the origin belongs in `endpoints` — the whole path and query go in the request line, which is
  also how the Slack recipe in [the http recipes](recipes.md#slack-post-to-a-channel) avoids the
  trailing slash a prefix plus `POST /` produces. The URL is stored as plain text in the
  configuration, like the Slack webhook: values in `endpoints` are text and not references, so
  anyone who can read `lazy_wire_fire_target` can post to this channel, and a configuration
  snapshot copies it; keep the tables admin-only. When the endpoint can be a reference, use it.
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
| **Endpoint** | `POST /v1/spaces/<space>/messages?key=<key>&token=<token>` on `https://chat.googleapis.com` — the URL the space's webhook gives you, split at the origin |
| **Auth** | **None — leave `vars` empty.** The `token` in the query is the secret; `key` is a value shared by all webhooks. Both travel in the URL |
| **Renders** | `{"text":"…"}` as `Content-Type: application/json`; `threadKey` in the body replies into a thread instead of starting one |
| **`expect`** | `json_extract(body, '$.name') IS NOT NULL` — deliberately without `status`, so the 2xx band stays and no success code is asserted |
| **Batch and rate limit** | One message per request, and the per-space quota for `spaces.messages.create` is 1 request per second shared among all webhooks in that space → `fire_per = 'key'`, one lane, wide `debounce_ms` |
| **Deletes** | None from a webhook. Removing a message is `spaces.messages.delete`, an OAuth call with a bot's credentials: a different target kind of problem |

```sql
INSERT INTO lazy_wire_fire_target (name, kind, endpoints, expect, timeout_ms) VALUES
 ('gchat', 'http', 'https://chat.googleapis.com',
  'json_extract(body, ''$.name'') IS NOT NULL', 10000);

INSERT INTO lazy_wire_fire_rule (name, target, fire_per, selects, template, debounce_ms, max_delay_ms) VALUES
 ('gchat_order_updates', 'gchat', 'key',
  'SELECT id AS _key, CONCAT(''Order '', id, '' is '', status) AS line FROM orders WHERE id IN (:keys)',
  'POST /v1/spaces/<space>/messages?key=<key>&token=<token> HTTP/1.1
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

* **The URL is a credential sitting in a plain column**, exactly as with the Slack webhook:
  values in `endpoints` are text and not references, so anyone who can read
  `lazy_wire_fire_target` can post to that space, and a configuration snapshot copies it. Keep
  the tables admin-only, one webhook per space, and treat rotation as an edit to this row.
  `key` and `token` above are placeholders, never values to publish. When the endpoint can be a
  reference, use it.
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

## Not covered here

Google Sheets, Microsoft Excel and Salesforce are left out of this file: they authenticate through
OAuth flows, and lazywf has no OAuth client — no authorization code exchange, no token refresh.
Their APIs are reachable only with a static token or a webhook URL, which is what every recipe
above uses.
