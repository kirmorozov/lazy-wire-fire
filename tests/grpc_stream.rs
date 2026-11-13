//! The `grpc` kind end to end: lazywf's tonic-based client against a server of lazywf's own,
//! in this process, on 127.0.0.1.
//!
//! The server and the schema it serves are `common::grpc_server`, shared with the `grpc` flow of
//! `tests/log_secrets.rs`: hand-rolled on `h2` rather than a gRPC framework, both because what
//! the kind has to be shown is the parts a framework hides and because a framework would not let
//! a test *do* what these tests do — a proxy answering HTTP 404 of its own, a stream that ends
//! without a `grpc-status`, a stream reset mid-flight, an acknowledgement echoing a hash it was
//! not sent, a server without zstd. The client half is exactly the one the tool ships; the wire
//! between them is plain gRPC, so the server says nothing the client could not meet in a real
//! deployment.
//!
//! Nothing here needs a database or a container: an item's outcome is what `send_items` answers,
//! and the `expect` clause of the target is unset, so no row is written.

// A `Policy` is a dozen knobs and a test names the two it is about; spelling all of them out
// as a struct literal at every call site would bury the knob under the noise it exists to turn.
#![allow(clippy::field_reassign_with_default)]

mod common;

use std::sync::OnceLock;

use common::grpc_server::{self, doc, rendered, Policy, Server};
use lazy_wire_fire::model::{RawTarget, TargetDef};
use lazy_wire_fire::response::Expect;
use lazy_wire_fire::targets::grpc::{self, contract};
use serde_json::{json, Value};

/// The token `vars.token` holds by reference, for the calls that must carry it.
const TOKEN: &str = "lwf-grpc-test-token-6f2a";

/// A directory the secret references of these tests may read, with `LAZYWF_SECRET_DIRS` pointed
/// at it once for the whole binary (the var is process-wide, and every test here wants the same
/// directory).
fn secrets_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("lazywf-grpc-e2e-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("LAZYWF_SECRET_DIRS", &dir);
        std::fs::write(dir.join("token"), format!("{TOKEN}\n")).unwrap();
        dir
    })
    .clone()
}

/// The test schema — an item of the app's own package and a service of the contract's shape in
/// another, the way a real deployment's descriptor set is laid out — compiled into the secrets
/// directory. Once for the whole binary: the `.proto` files live in one directory every test
/// shares, and protox must not read them while another test is still writing them. Returns the
/// path of the descriptor set.
fn descriptor_set() -> String {
    static DS: OnceLock<String> = OnceLock::new();
    DS.get_or_init(|| grpc_server::write_schema(&secrets_dir()))
        .clone()
}

/// The vars of a target of these tests: the schema, the item type, and the call options the test
/// asks for.
fn vars(extra: &[(&str, Value)]) -> String {
    let mut m = json!({
        "descriptor_set": format!("file:{}", descriptor_set()),
        "item_type": grpc_server::ITEM_TYPE,
    });
    for (k, v) in extra {
        m.as_object_mut()
            .unwrap()
            .insert((*k).to_string(), v.clone());
    }
    m.to_string()
}

/// A `grpc` target's definition: one endpoint, one item type, a short timeout so that a server
/// which says nothing is a quick answer rather than a slow one.
fn target(endpoint: String, extra: &[(&str, Value)]) -> TargetDef {
    target_at(endpoint, extra, 2_000)
}

/// The same, with the timeout a test needs: the one a silent server is judged by.
fn target_at(endpoint: String, extra: &[(&str, Value)], timeout_ms: u64) -> TargetDef {
    let raw = RawTarget {
        name: "orders".into(),
        kind: Some("grpc".into()),
        endpoints: Some(endpoint),
        timeout_ms: Some(timeout_ms),
        vars: Some(vars(extra)),
        ..Default::default()
    };
    TargetDef::from_row(&raw).expect("loads")
}

use lazy_wire_fire::target::{is_rejected, Item};

/// One flush of `keys` at a server with this policy: what the kind answered for each key, and the
/// server itself, for the test to read what really arrived.
async fn flush(policy: Policy, keys: &[&str], extra: &[(&str, Value)]) -> (Vec<Item>, Server) {
    flush_at(policy, keys, extra, 2_000).await
}

/// The same, with the target's timeout.
async fn flush_at(
    policy: Policy,
    keys: &[&str],
    extra: &[(&str, Value)],
    timeout_ms: u64,
) -> (Vec<Item>, Server) {
    let server = Server::start(policy).await;
    let def = target_at(server.endpoint(), extra, timeout_ms);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).expect("loads");
    let items = grpc::send_items(
        &client,
        &rendered(keys),
        &owns(keys),
        &Expect::new(None).unwrap(),
        &pool(),
    )
    .await
    .expect("the call itself went through");
    (items, server)
}

/// One flush whose whole call is expected to fail outright: a verdict of the whole call that
/// refuses the action (the whole-call `Err` lazywf gives when the keys take a strike together).
async fn failure(policy: Policy, keys: &[&str], extra: &[(&str, Value)]) -> anyhow::Error {
    let server = Server::start(policy).await;
    let def = target_at(server.endpoint(), extra, 2_000);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).expect("loads");
    let e = grpc::send_items(
        &client,
        &rendered(keys),
        &owns(keys),
        &Expect::new(None).unwrap(),
        &pool(),
    )
    .await
    .expect_err("the call was expected to fail");
    server.stop();
    e
}

/// The keys as the kind takes them: the engine hands `Vec<String>` down, a test spells them
/// as literals.
fn owns(v: &[&str]) -> Vec<String> {
    v.iter().map(|k| k.to_string()).collect()
}

/// The reference `vars.token` holds for a server that wants a token.
fn token_var() -> Value {
    Value::String(format!("file:{}/token", secrets_dir().display()))
}

/// `ok`, or the reason the key is waiting or gone: one string per key.
fn outcomes(items: &[Item]) -> Vec<String> {
    items
        .iter()
        .map(|i| match i {
            Item::Ok => "ok".to_string(),
            Item::Rejected(w) => format!("refused: {w}"),
            Item::Transient(w) => format!("transient: {w}"),
        })
        .collect()
}

#[tokio::test]
async fn the_items_arrive_and_every_key_is_acked() {
    let keys = ["a", "b", "c"];
    let (items, server) = flush(Policy::default(), &keys, &[]).await;
    assert_eq!(outcomes(&items), vec!["ok", "ok", "ok"]);
    let seen = server.seen();
    assert_eq!(seen.len(), 3);
    // The sequence numbers are the stream's: from 1, by ones, in the order the engine passed the
    // keys; the key travels with its item; the hash is the item's own content.
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(seen[i].seq, i as u64 + 1, "{k}");
        assert_eq!(seen[i].key, *k);
        let body = body_of(&keys, i);
        assert_eq!(seen[i].item, body, "{k} was not the document rendered");
        assert_eq!(seen[i].hash, contract::hash(k.as_bytes(), &body), "{k}");
    }
    // What a gRPC call is: the path of the method, the protobuf content type, trailers. tonic
    // names the type `application/grpc` (`+proto` is the spec's optional spelling of the same
    // thing), so that is what lazywf's client puts on the wire.
    assert_eq!(server.header("te").as_deref(), Some("trailers"));
    let ct = server.header("content-type").unwrap_or_default();
    assert!(ct.starts_with("application/grpc"), "content-type: {ct}");
    assert!(server
        .header("user-agent")
        .is_some_and(|u| u.starts_with("lazywf/")));
    server.stop();
}

#[tokio::test]
async fn one_stream_serves_every_flush_and_the_numbers_run_on() {
    let mut policy = Policy::default();
    policy.one_by_one = true;
    let server = Server::start(policy).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let expect = Expect::new(None).unwrap();
    let keys = ["a", "b"];
    for round in 0..3 {
        let items = grpc::send_items(&client, &rendered(&keys), &owns(&keys), &expect, &pool())
            .await
            .unwrap();
        assert_eq!(outcomes(&items), vec!["ok", "ok"], "round {round}");
    }
    // One call, not three: the stream was opened once and kept.
    assert_eq!(server.calls(), 1, "the stream was not kept across flushes");
    let seqs: Vec<u64> = server.seen().iter().map(|i| i.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
    server.stop();
}

#[tokio::test]
async fn a_refused_ack_refuses_that_key_and_only_that_key() {
    let mut policy = Policy::default();
    policy.refuse = vec![("b".into(), "the sku is unknown".into())];
    let (items, server) = flush(policy, &["a", "b", "c"], &[]).await;
    let out = outcomes(&items);
    assert_eq!(out[0], "ok");
    assert_eq!(out[2], "ok");
    assert!(out[1].starts_with("refused:"), "{out:?}");
    assert!(out[1].contains("the sku is unknown"), "{out:?}");
    // The server was asked about all three, whatever it made of one of them.
    assert_eq!(server.seen().len(), 3);
    server.stop();
}

#[tokio::test]
async fn a_retry_ack_is_that_key_waiting() {
    let mut policy = Policy::default();
    policy.retry = vec!["a".into()];
    let (items, server) = flush(policy, &["a", "b"], &[]).await;
    let out = outcomes(&items);
    assert!(out[0].starts_with("transient:"), "{out:?}");
    assert!(out[0].contains("not now"), "{out:?}");
    assert_eq!(out[1], "ok");
    server.stop();
}

#[tokio::test]
async fn an_item_never_acknowledged_is_transient_never_a_strike() {
    let mut policy = Policy::default();
    policy.silent = vec!["b".into()];
    let (items, server) = flush_at(policy, &["a", "b"], &[], 400).await;
    let out = outcomes(&items);
    assert_eq!(out[0], "ok");
    assert!(out[1].starts_with("transient:"), "{out:?}");
    assert!(
        out[1].contains("no acknowledgement") && out[1].contains("400ms"),
        "{out:?}"
    );
    server.stop();
}

#[tokio::test]
async fn a_stream_that_dies_mid_flight_costs_no_key_and_the_next_flush_reconnects() {
    let mut policy = Policy::default();
    // The server reads both items and acknowledges the first, then resets the stream: the
    // second item's answer will never come, and nothing may be lost to that.
    policy.reset_after = 2;
    policy.silent = vec!["b".into()];
    let server = Server::start(policy).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let expect = Expect::new(None).unwrap();

    let items = grpc::send_items(
        &client,
        &rendered(&["a", "b"]),
        &owns(&["a", "b"]),
        &expect,
        &pool(),
    )
    .await
    .unwrap();
    let out = outcomes(&items);
    // The item the server had already taken stays taken; the other one waits.
    assert_eq!(out[0], "ok", "{out:?}");
    assert!(out[1].starts_with("transient:"), "{out:?}");

    // And the next flush opens a stream of its own, whose numbers start at 1 again: the hash is
    // what tells the two streams' items apart, not the number.
    let items = grpc::send_items(&client, &rendered(&["c"]), &owns(&["c"]), &expect, &pool())
        .await
        .unwrap();
    assert_eq!(outcomes(&items), vec!["ok"]);
    assert_eq!(server.calls(), 2, "the stream was not opened again");
    let seqs: Vec<u64> = server.seen().iter().map(|i| i.seq).collect();
    assert_eq!(seqs, vec![1, 2, 1], "the numbers did not restart");
    assert_eq!(server.seen()[2].key, "c");
    server.stop();
}

#[tokio::test]
async fn an_acknowledgement_with_the_wrong_hash_is_not_believed() {
    let mut policy = Policy::default();
    policy.wrong_hash = vec![1];
    let (items, server) = flush_at(policy, &["a", "b"], &[], 500).await;
    let out = outcomes(&items);
    // The poisoned answer buys nothing: the item waits out the deadline rather than trusting a
    // hash that is not the one it was sent with. The other key, answered honestly, is fine.
    assert!(out[0].starts_with("transient:"), "{out:?}");
    assert_eq!(out[1], "ok", "{out:?}");
    server.stop();
}

#[tokio::test]
async fn an_acknowledgement_for_an_item_never_sent_is_not_believed() {
    let mut policy = Policy::default();
    // The only answer the key gets is an acknowledgement for a sequence number that was never
    // sent: believing it could lose the key twice over, so the deadline answers for it instead.
    policy.ghost_ack = true;
    policy.silent = vec!["a".into()];
    let (items, server) = flush_at(policy, &["a"], &[], 500).await;
    let out = outcomes(&items);
    assert!(out[0].starts_with("transient:"), "{out:?}");
    server.stop();
}

#[tokio::test]
async fn a_missing_status_cannot_take_an_acknowledgement_back() {
    let mut policy = Policy::default();
    policy.no_status = true;
    let (items, server) = flush(policy, &["a"], &[]).await;
    let out = outcomes(&items);
    // The server positively acknowledged the item before the body ended: that answer stands,
    // and an end without `grpc-status` cannot take it back. What the missing status costs is
    // the stream — a flush that was still waiting would be transient, and the next one here
    // opens a stream of its own (the `one_stream` test shows what a stream of its own means).
    assert_eq!(out, vec!["ok"], "{out:?}");
    assert!(!out[0].contains("refused"), "{out:?}");
    server.stop();
}

#[tokio::test]
async fn the_token_goes_as_bearer_metadata_and_nowhere_else() {
    let mut policy = Policy::default();
    policy.token = Some(TOKEN.to_string());
    let (items, server) = flush(policy, &["a"], &[("token", token_var())]).await;
    assert_eq!(outcomes(&items), vec!["ok"]);
    assert_eq!(
        server.header("authorization").as_deref(),
        Some(&format!("Bearer {TOKEN}")[..])
    );
    server.stop();
}

#[tokio::test]
async fn an_env_token_is_read_at_open_and_reaches_the_peer_as_bearer() {
    // `check` accepts both reference forms at load; this pins the other one through the wire —
    // the flush really reads the variable. Were it read as a file, every flush would fail with
    // "must be a `file:` reference" before a byte was sent, and the stream would never open.
    std::env::set_var("LWF_E2E_GRPC_TOKEN", format!("{TOKEN}-from-env"));
    let mut policy = Policy::default();
    policy.token = Some(format!("{TOKEN}-from-env"));
    let (items, server) = flush(
        policy,
        &["a"],
        &[("token", Value::String("env:LWF_E2E_GRPC_TOKEN".into()))],
    )
    .await;
    assert_eq!(outcomes(&items), vec!["ok"]);
    assert_eq!(
        server.header("authorization").as_deref(),
        Some(&format!("Bearer {TOKEN}-from-env")[..])
    );
    server.stop();
}

#[tokio::test]
async fn a_server_that_wants_another_token_refuses_the_action() {
    let mut policy = Policy::default();
    // The server wants a token lazywf was not given: the credentials are wrong, and asking again
    // will not change that.
    policy.token = Some("a-different-token".to_string());
    let e = failure(policy, &["a"], &[("token", token_var())]).await;
    let msg = format!("{e:#}");
    assert!(is_rejected(&e), "{msg}");
    assert!(msg.contains("UNAUTHENTICATED"), "{msg}");
    assert!(!msg.contains(TOKEN), "the token was repeated: {msg}");
}

#[tokio::test]
async fn gzip_round_trips_both_ways() {
    let mut policy = Policy::default();
    policy.accept = vec!["gzip".into()];
    policy.respond_with = Some("gzip".into());
    let (items, server) = flush(policy, &["a", "b"], &[("compression", json!("gzip"))]).await;
    assert_eq!(outcomes(&items), vec!["ok", "ok"]);
    assert_eq!(server.asked(), vec!["gzip".to_string()]);
    assert_eq!(server.seen().len(), 2);
    server.stop();
}

#[tokio::test]
async fn zstd_round_trips_both_ways() {
    let mut policy = Policy::default();
    policy.accept = vec!["zstd".into()];
    policy.respond_with = Some("zstd".into());
    let (items, server) = flush(policy, &["a", "b"], &[("compression", json!("zstd"))]).await;
    assert_eq!(outcomes(&items), vec!["ok", "ok"]);
    assert_eq!(server.asked(), vec!["zstd".to_string()]);
    server.stop();
}

#[tokio::test]
async fn a_server_without_zstd_is_sent_gzip_instead() {
    // `auto` asks for zstd first; a server without it says `UNIMPLEMENTED` naming the codec, and
    // lazywf is the one that asked for something the server has not got.
    let mut policy = Policy::default();
    policy.accept = vec!["identity".into(), "gzip".into()];
    let (items, server) = flush(policy, &["a", "b"], &[]).await;
    assert_eq!(outcomes(&items), vec!["ok", "ok"]);
    assert_eq!(
        server.asked(),
        vec!["zstd".to_string(), "gzip".to_string()],
        "the ladder did not walk"
    );
    server.stop();
}

#[tokio::test]
async fn a_target_told_to_use_zstd_hears_about_a_server_without_it() {
    // The other half of the ladder: an owner who named the codec should hear that the server
    // does not have it, not be quietly sent as identity. The keys wait (the server took nothing),
    // and what they wait for says why.
    let mut policy = Policy::default();
    policy.accept = vec!["identity".into()];
    let (items, server) = flush(policy, &["a"], &[("compression", json!("zstd"))]).await;
    let out = outcomes(&items);
    assert!(out[0].starts_with("transient:"), "{out:?}");
    assert!(out[0].contains("UNIMPLEMENTED"), "{out:?}");
    assert_eq!(server.asked(), vec!["zstd".to_string()]);
    server.stop();
}

#[tokio::test]
async fn none_sends_the_messages_as_they_are() {
    let server = Server::start(Policy::default()).await;
    let def = target(server.endpoint(), &[("compression", json!("none"))]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let items = grpc::send_items(
        &client,
        &rendered(&["a"]),
        &owns(&["a"]),
        &Expect::new(None).unwrap(),
        &pool(),
    )
    .await
    .unwrap();
    assert_eq!(outcomes(&items), vec!["ok"]);
    // No `grpc-encoding` at all: identity is the absence of one.
    assert_eq!(server.header("grpc-encoding"), None);
    server.stop();
}

#[tokio::test]
async fn a_proxy_that_answers_http_404_refuses_the_action() {
    let mut policy = Policy::default();
    policy.http_status = Some(404);
    let e = failure(policy, &["a"], &[]).await;
    let msg = format!("{e:#}");
    assert!(is_rejected(&e), "{msg}");
    assert!(msg.contains("404") && msg.contains("not gRPC"), "{msg}");
    assert!(
        msg.contains("no such backend"),
        "the body was not quoted: {msg}"
    );
}

/// A proxy that answers the call with a 401 of its own — the credential is not wanted here — is
/// refused as a whole action, the way an `UNAUTHENTICATED` from the service itself is: the
/// configuration is what is wrong, and a target that retried it forever would hide that from the
/// owner. The endpoint is named, so the owner knows which one to fix.
#[tokio::test]
async fn a_proxy_that_answers_http_401_refuses_the_action() {
    let mut policy = Policy::default();
    policy.http_status = Some(401);
    let e = failure(policy, &["a"], &[]).await;
    let msg = format!("{e:#}");
    assert!(is_rejected(&e), "{msg}");
    assert!(msg.contains("401") && msg.contains("not gRPC"), "{msg}");
    assert!(
        msg.contains("no such backend"),
        "the body was not quoted: {msg}"
    );
    assert!(msg.contains(grpc_server::METHOD), "{msg}");
}

#[tokio::test]
async fn a_proxy_that_answers_http_503_is_a_moment() {
    let mut policy = Policy::default();
    policy.http_status = Some(503);
    let (items, server) = flush(policy, &["a"], &[]).await;
    let out = outcomes(&items);
    assert!(out[0].starts_with("transient:"), "{out:?}");
    assert!(out[0].contains("503"), "{out:?}");
    server.stop();
}

#[tokio::test]
async fn a_status_that_judges_the_request_refuses_and_one_that_is_a_moment_waits() {
    let mut policy = Policy::default();
    policy.status = Some(3); // INVALID_ARGUMENT
    let e = failure(policy, &["a"], &[]).await;
    assert!(is_rejected(&e), "{}", format!("{e:#}"));
    assert!(format!("{e:#}").contains("INVALID_ARGUMENT"));

    let mut policy = Policy::default();
    policy.status = Some(14); // UNAVAILABLE: the keys wait, they do not take a strike.
    let (items, server) = flush(policy, &["a"], &[]).await;
    let out = outcomes(&items);
    assert!(out[0].starts_with("transient:"), "{out:?}");
    assert!(out[0].contains("UNAVAILABLE"), "{out:?}");
    server.stop();
}

#[tokio::test]
async fn a_message_over_the_target_limit_is_reported_and_nothing_is_sent() {
    let mut policy = Policy::default();
    // The server would take anything: the target's own limit is what stops the flush.
    policy.accept = vec!["identity".into(), "gzip".into(), "zstd".into()];
    let server = Server::start(policy).await;
    let def = target_at(
        server.endpoint(),
        &[("max_message_bytes", json!(64))],
        1_000,
    );
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let items = grpc::send_items(
        &client,
        &rendered(&["a", "b", "c"]),
        &owns(&["a", "b", "c"]),
        &Expect::new(None).unwrap(),
        &pool(),
    )
    .await
    .unwrap();
    let out = outcomes(&items);
    assert!(out.iter().all(|o| o.starts_with("transient:")), "{out:?}");
    assert!(out[0].contains("max_message_bytes"), "{out:?}");
    assert_eq!(server.seen().len(), 0, "an over-large message was sent");
    server.stop();
}

#[tokio::test]
async fn a_flush_bigger_than_the_window_still_goes() {
    // The window bounds how much may be unacknowledged, not how big one flush may be: a batch
    // over the window is the batch limits' business, and waiting for room that can never open
    // would stall the target.
    let keys: Vec<String> = (0..700).map(|i| format!("k{i}")).collect();
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let (items, server) = flush(Policy::default(), &keys, &[]).await;
    assert!(
        items.iter().all(|i| *i == Item::Ok),
        "{} lost",
        outcomes(&items).iter().filter(|o| *o != "ok").count()
    );
    assert_eq!(server.seen().len(), 700);
    server.stop();
}

#[tokio::test]
async fn a_body_that_is_not_the_docs_array_is_refused() {
    let server = Server::start(Policy::default()).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let e = grpc::send_items(
        &client,
        r#"{"docs": []}"#,
        &owns(&["a"]),
        &Expect::new(None).unwrap(),
        &pool(),
    )
    .await
    .expect_err("an object is not the docs array");
    assert!(is_rejected(&e), "{}", format!("{e:#}"));
    assert!(format!("{e:#}").contains("docs"));
    server.stop();
}

#[tokio::test]
async fn a_document_that_does_not_fit_the_item_message_is_refused() {
    let server = Server::start(Policy::default()).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let body = json!([{"key": "a", "op": "update", "doc": {"nosuch": 1}}]).to_string();
    let e = grpc::send_items(
        &client,
        &body,
        &owns(&["a"]),
        &Expect::new(None).unwrap(),
        &pool(),
    )
    .await
    .expect_err("no such field in orders.v1.Item");
    assert!(is_rejected(&e), "{}", format!("{e:#}"));
    assert!(format!("{e:#}").contains("nosuch"), "{}", format!("{e:#}"));
    // And nothing went on the stream: a document that cannot be built is caught before the call.
    assert_eq!(server.seen().len(), 0);
    server.stop();
}

#[tokio::test]
async fn the_key_of_a_document_is_the_key_the_engine_gave() {
    let server = Server::start(Policy::default()).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let body = json!([{"key": "other", "op": "update", "doc": {"id": 1}}]).to_string();
    let e = grpc::send_items(
        &client,
        &body,
        &owns(&["a"]),
        &Expect::new(None).unwrap(),
        &pool(),
    )
    .await
    .expect_err("the document is not the engine's key");
    assert!(format!("{e:#}").contains("other"), "{}", format!("{e:#}"));
    server.stop();
}

#[tokio::test]
async fn a_once_step_with_nothing_rendered_is_skipped() {
    let server = Server::start(Policy::default()).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let sent = grpc::send(&client, "", &Expect::new(None).unwrap(), &pool())
        .await
        .unwrap();
    assert_eq!(sent, lazy_wire_fire::target::Sent::Skipped);
    assert_eq!(server.calls(), 0);
    server.stop();
}

#[tokio::test]
async fn a_once_step_pushes_what_it_rendered() {
    let server = Server::start(Policy::default()).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let body =
        json!([{"key": "flushed", "op": "update", "doc": {"id": 7, "sku": "s"}}]).to_string();
    let sent = grpc::send(&client, &body, &Expect::new(None).unwrap(), &pool())
        .await
        .unwrap();
    assert_eq!(sent, lazy_wire_fire::target::Sent::Ok);
    assert_eq!(server.seen().len(), 1);
    assert_eq!(server.seen()[0].key, "flushed");
    server.stop();
}

#[tokio::test]
async fn a_once_step_the_server_refuses_is_a_refusal() {
    let mut policy = Policy::default();
    policy.refuse = vec![("flushed".into(), "no".into())];
    let server = Server::start(policy).await;
    let def = target(server.endpoint(), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let body =
        json!([{"key": "flushed", "op": "update", "doc": {"id": 7, "sku": "s"}}]).to_string();
    let e = grpc::send(&client, &body, &Expect::new(None).unwrap(), &pool())
        .await
        .expect_err("the server refused it");
    assert!(is_rejected(&e), "{}", format!("{e:#}"));
    assert!(format!("{e:#}").contains("no"), "{}", format!("{e:#}"));
    server.stop();
}

/// The pool an `expect`-less target never touches.
fn pool() -> mysql_async::Pool {
    mysql_async::Pool::new("mysql://u:p@127.0.0.1:1/lazywf_unused")
}

/// What the item of key `k` is on the wire: the same document `rendered` holds, so a test can say
/// what the server should have been given.
fn body_of(keys: &[&str], i: usize) -> Vec<u8> {
    let def = target(format!("grpc://h:1/{}", grpc_server::METHOD), &[]);
    let (_, client) = grpc::Client::new(&def, &def.endpoints[0]).unwrap();
    let body = client.method.body.as_ref().unwrap();
    let document = doc(keys, i);
    let msg =
        lazy_wire_fire::targets::grpc::schema::build(body, document.as_object().unwrap()).unwrap();
    prost::Message::encode_to_vec(&msg)
}
