use super::*;
use serde_json::json;

fn render(src: &str, ctx: Value) -> String {
    let mut t = Templates::new();
    t.add("t", src, ACTION_ROOTS).unwrap();
    t.render("t", &ctx).unwrap()
}

#[test]
fn varnish_purge() {
    let src = "PURGE / HTTP/1.1\nHost: {{endpoint.host}}\nX-Cache-Tags-Pattern: {{#each keys}}((^|,){{this}}(,|$)){{#unless @last}}|{{/unless}}{{/each}}\n";
    let out = render(
        src,
        json!({"keys": ["item_1", "group_2"], "endpoint": {"host": "varnish"}}),
    );
    assert_eq!(
            out,
            "PURGE / HTTP/1.1\nHost: varnish\nX-Cache-Tags-Pattern: ((^|,)item_1(,|$))|((^|,)group_2(,|$))\n"
        );
}

#[test]
fn redis_clear_by_tag() {
    let src = "EVAL \"for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end\" 0 {{#each keys}}{{target.vars.prefix}}{{upper (replace this \"[^A-Za-z0-9_]\" \"_\")}} {{/each}}";
    let out = render(
        src,
        json!({"keys": ["item_1", "a-b.c"], "target": {"vars": {"prefix": "69d_"}}}),
    );
    assert_eq!(
            out,
            "EVAL \"for _,t in ipairs(ARGV) do redis.call('DEL','zc:ti:'..t) end\" 0 69d_ITEM_1 69d_A_B_C "
        );
}

#[test]
fn search_bulk() {
    let src = "POST /{{vars.alias}}/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{{#each docs}}{{#if (eq op \"delete\")}}{\"delete\":{\"_id\":\"{{key}}\"}}\n{{else}}{\"update\":{\"_id\":\"{{key}}\"}}\n{\"doc\":{{json doc}},\"doc_as_upsert\":true}\n{{/if}}{{/each}}";
    let out = render(
        src,
        json!({"vars": {"alias": "items_1"}, "docs": [
            {"key": "1", "op": "upsert", "doc": {"sku": "a<b&\"c\"", "price": 1.5}},
            {"key": "2", "op": "delete", "doc": {}}
        ]}),
    );
    assert_eq!(
            out,
            "POST /items_1/_bulk HTTP/1.1\nContent-Type: application/x-ndjson\n\n{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"sku\":\"a<b&\\\"c\\\"\",\"price\":1.5},\"doc_as_upsert\":true}\n{\"delete\":{\"_id\":\"2\"}}\n"
        );
}

#[test]
fn s3_per_key() {
    let src = "{{#each docs}}{{#if (eq op \"delete\")}}DELETE /products/{{key}}.json HTTP/1.1\n\n{{else}}PUT /products/{{key}}.json HTTP/1.1\nContent-Type: application/json\n\n{{json doc}}{{/if}}{{/each}}";
    let put = render(
        src,
        json!({"docs": [{"key": "7", "op": "upsert", "doc": {"a": [1]}}]}),
    );
    assert_eq!(
        put,
        "PUT /products/7.json HTTP/1.1\nContent-Type: application/json\n\n{\"a\":[1]}"
    );
    let del = render(
        src,
        json!({"docs": [{"key": "7", "op": "delete", "doc": {}}]}),
    );
    assert_eq!(del, "DELETE /products/7.json HTTP/1.1\n\n");
}

#[test]
fn helpers() {
    assert_eq!(
        render("{{join keys \"|\"}}", json!({"keys": ["a", 1, null]})),
        "a|1|"
    );
    assert_eq!(render("{{join keys}}", json!({"keys": ["a", "b"]})), "a,b");
    assert_eq!(render("{{lower x}}{{upper x}}", json!({"x": "aB"})), "abAB");
    assert_eq!(
        render("{{regex_escape x}}", json!({"x": "a.b*"})),
        "a\\.b\\*"
    );
    assert_eq!(
        render("{{url_encode x}}", json!({"x": "a b/ü"})),
        "a%20b%2F%C3%BC"
    );
    assert_eq!(
        render("{{json x}}", json!({"x": {"a": [1, "b"]}})),
        "{\"a\":[1,\"b\"]}"
    );
    assert_eq!(render("{{json missing}}", json!({})), "null");
    assert_eq!(
        render("{{replace x \"(\\\\d+)\" \"<$1>\"}}", json!({"x": "a1b22"})),
        "a<1>b<22>"
    );
    assert_eq!(
        render("{{redis_quote x}}", json!({"x": "a \"b\"\n\\"})),
        "\"a \\\"b\\\"\\n\\\\\""
    );
    assert_eq!(
        render("{{#if (eq a 1)}}y{{else}}n{{/if}}", json!({"a": 1})),
        "y"
    );
    assert_eq!(render("{{x}}", json!({"x": "<&>"})), "<&>");
}

#[test]
fn add_helper() {
    // The reason it exists: the query protocol numbers the entries of a batch from 1, where
    // `each` counts from 0 (aws::SNS_MAX_ENTRIES and the `sns` kind's templates).
    assert_eq!(
        render(
            "{{#each docs}}m{{add @index 1}}={{json this}} {{/each}}",
            json!({"docs": [{"a": 1}, {"a": 2}, {"a": 3}]})
        ),
        "m1={\"a\":1} m2={\"a\":2} m3={\"a\":3} "
    );
    assert_eq!(render("{{add 2 3}}", json!({})), "5");
    assert_eq!(render("{{add x 1}}", json!({"x": 41})), "42");
    // A column can hold the number as text.
    assert_eq!(render("{{add x 1}}", json!({"x": " 7 "})), "8");
    assert_eq!(render("{{add x -1}}", json!({"x": 7})), "6");
    assert_eq!(render("{{add 0 0}}", json!({})), "0");
    // What it cannot add is an error, not an empty string.
    let bad = |src: &str, ctx: Value| -> String {
        let mut t = Templates::new();
        t.add("t", src, ACTION_ROOTS).unwrap();
        format!("{:#}", t.render("t", &ctx).unwrap_err())
    };
    for (src, ctx) in [
        ("{{add x 1}}", json!({"x": "seven"})),
        ("{{add x 1}}", json!({"x": true})),
        ("{{add x 1}}", json!({"x": {"a": 1}})),
        ("{{add x 1}}", json!({"x": 1.5})),
        ("{{add x 1}}", json!({})),
    ] {
        let e = bad(src, ctx);
        assert!(e.contains("not a whole number"), "{src}: {e}");
    }
    // And so is one number instead of two: a template that cannot count is not sent.
    let e = bad("{{add x}}", json!({"x": 1}));
    assert!(e.contains("index 1"), "{e}");
}

#[test]
fn base64_helper() {
    // Standard alphabet with padding; the text form of the value is encoded.
    assert_eq!(render("{{base64 x}}", json!({"x": "hello"})), "aGVsbG8=");
    assert_eq!(render("{{base64 x}}", json!({"x": "a?b>c"})), "YT9iPmM=");
    assert_eq!(render("{{base64 x}}", json!({"x": "ab"})), "YWI=");
    assert_eq!(render("{{base64 x}}", json!({"x": ""})), "");
    // Non-ASCII: the UTF-8 bytes are encoded.
    assert_eq!(render("{{base64 x}}", json!({"x": "ü"})), "w7w=");
    // A document, as a Kinesis record would carry it.
    assert_eq!(
        render("{{base64 (json doc)}}", json!({"doc": {"a": 1}})),
        "eyJhIjoxfQ=="
    );
    // Decodes back to the same text.
    use base64::Engine as _;
    let enc = render("{{base64 x}}", json!({"x": "{\"k\":\"v/+=\"}"}));
    let dec = base64::engine::general_purpose::STANDARD
        .decode(enc)
        .unwrap();
    assert_eq!(String::from_utf8(dec).unwrap(), "{\"k\":\"v/+=\"}");
}

#[test]
fn yaml_helper() {
    let y = |v: Value| render("{{{yaml doc}}}", json!({"doc": v}));

    // The document's key order, scalars verbatim.
    assert_eq!(
        y(json!({"b": 1, "a": "two", "c": true, "d": null})),
        "b: 1\na: two\nc: true\nd: null"
    );
    assert_eq!(
        y(json!({"i": 7, "f": 1.5, "neg": -3})),
        "i: 7\nf: 1.5\nneg: -3"
    );
    // A key is quoted by the same rule as a value: unquoted, `n:` is a 1.1 boolean, so the
    // receiver would lose the string the document's key is.
    assert_eq!(
        y(json!({"y": 1, "n": 2, "null": 3})),
        "\"y\": 1\n\"n\": 2\n\"null\": 3"
    );
    // A float serde_json prints without a dot takes the dot and the exponent sign a YAML
    // reader needs to see a float: `1e20` alone reads back as a string.
    assert_eq!(
        y(json!({"a": 1e20, "b": 1.5e30, "c": 1e-7, "d": 1.0, "e": -0.5, "f": 3})),
        "a: 1.0e+20\nb: 1.5e+30\nc: 1.0e-7\nd: 1.0\ne: -0.5\nf: 3"
    );
    // 1.1's float resolver reads a sexagesimal with a fraction as a number, and a line
    // that starts with `...` ends the document: both take quotes.
    assert_eq!(
        y(json!({"a": "1:20.5", "b": "190:20:30.15", "c": "... x", "d": "..."})),
        "a: \"1:20.5\"\nb: \"190:20:30.15\"\nc: \"... x\"\nd: \"...\""
    );
    // A key past the 1.1 simple-key length is refused unquoted *and* quoted, so it takes
    // the explicit form; a value of the same length needs nothing of the sort.
    let long = "k".repeat(1100);
    let mut keyed = serde_json::Map::new();
    keyed.insert(long.clone(), json!(1));
    assert_eq!(y(Value::Object(keyed)), format!("? {long}\n: 1"));
    assert_eq!(y(json!({"k": long})), format!("k: {}", "k".repeat(1100)));
    // …inside a block and as a list item's key too, where both lines keep the entry's
    // column.
    assert_eq!(
        y(json!({"m": {long.as_str(): 1}, "rows": [{ long.as_str(): 2 }]})),
        format!("m:\n  ? {long}\n  : 1\nrows:\n  - ? {long}\n    : 2")
    );
    // The limit counts the key *as written* — the quotes and the `\n` escapes are what the
    // reader scans, not the raw key. So 1022 characters of key that needs quoting (its
    // leading `-`) still rides its line at 1024 written, and one raw character more, and a
    // key of exactly 1024 whose 24 line breaks take two characters each, do not.
    let fits = format!("-{}", "k".repeat(1021));
    let over = format!("-{}", "k".repeat(1022));
    assert_eq!(y(json!({ fits.as_str(): 1 })), format!("\"{fits}\": 1"));
    assert_eq!(y(json!({ over.as_str(): 1 })), format!("? \"{over}\"\n: 1"));
    let breaks = format!("{}{}{}", "m".repeat(500), "\n".repeat(24), "n".repeat(500));
    assert_eq!(
        y(json!({ breaks.as_str(): 1 })),
        format!(
            "? \"{}{}{}\"\n: 1",
            "m".repeat(500),
            "\\n".repeat(24),
            "n".repeat(500)
        )
    );
    // Top-level values of every shape.
    assert_eq!(y(json!([1, 2])), "- 1\n- 2");
    assert_eq!(y(json!({})), "{}");
    assert_eq!(y(json!([])), "[]");
    assert_eq!(y(json!(null)), "null");
    assert_eq!(y(json!("hi")), "hi");
    assert_eq!(y(json!("")), "\"\"");
    // Nesting, two spaces a level.
    assert_eq!(
        y(json!({"outer": {"inner": 1}, "list": [1, "two", true]})),
        "outer:\n  inner: 1\nlist:\n  - 1\n  - two\n  - true"
    );
    // A sequence item that is a mapping: the first entry rides the dash's line.
    assert_eq!(
        y(json!({"rows": [{"id": 1, "name": "a"}, {"id": 2}]})),
        "rows:\n  - id: 1\n    name: a\n  - id: 2"
    );
    // A sequence item that is a sequence: the dash stands alone.
    assert_eq!(
        y(json!({"a": [[1, 2], [3]]})),
        "a:\n  -\n    - 1\n    - 2\n  -\n    - 3"
    );
    // A sequence item that is a mapping holding a block of its own: the entries ride the
    // dash and its block indents past them.
    assert_eq!(
        y(json!({"rows": [{"a": {"b": 1}, "c": [2]}]})),
        "rows:\n  - a:\n      b: 1\n    c:\n      - 2"
    );
    // Empty collections inside a document.
    assert_eq!(y(json!({"o": {}, "a": []})), "o: {}\na: []");
    // A string a YAML reader would resolve as something else stays a string:
    // numbers (whole, decimal, exponent, underscores, sexagesimal, hex),
    // booleans (1.1's included), null, .inf / .nan and dates.
    assert_eq!(
            y(json!({"a": "123", "b": "1.5", "c": "1.5e3", "d": "0x1F", "e": "1:2", "f": "true", "g": "yes", "h": "On", "i": "null", "j": "~", "k": ".inf", "l": "2026-11-07", "m": "2026-11-07 10:00:00"})),
            "a: \"123\"\nb: \"1.5\"\nc: \"1.5e3\"\nd: \"0x1F\"\ne: \"1:2\"\nf: \"true\"\ng: \"yes\"\nh: \"On\"\ni: \"null\"\nj: \"~\"\nk: \".inf\"\nl: \"2026-11-07\"\nm: \"2026-11-07 10:00:00\""
        );
    // Indicators and the comment / colon contexts are quoted; the rest stay plain.
    assert_eq!(
        y(
            json!({"a": "-lead", "b": "?q", "c": "x #y", "d": "a: b", "e": "...", "f": "it's", "g": "a b"})
        ),
        "a: \"-lead\"\nb: \"?q\"\nc: \"x #y\"\nd: \"a: b\"\ne: \"...\"\nf: it's\ng: a b"
    );
    // Control characters are escaped: a value holds no raw line break.
    assert_eq!(
        y(json!({"a": "l1\nl2", "b": "a\tb", "c": "x\u{0}y"})),
        "a: \"l1\\nl2\"\nb: \"a\\tb\"\nc: \"x\\u0000y\""
    );
    // The C1 controls and the characters YAML 1.1 reads as a line break or a byte-order
    // mark are escaped too: is_control does not cover the last three.
    assert_eq!(
        y(
            json!({"a": "x\u{85}y", "b": "x\u{9f}y", "c": "x\u{2028}y", "d": "x\u{2029}y", "e": "x\u{feff}y"})
        ),
        "a: \"x\\u0085y\"\nb: \"x\\u009Fy\"\nc: \"x\\u2028y\"\nd: \"x\\u2029y\"\ne: \"x\\uFEFFy\""
    );
    // 1.1's one-letter booleans, the merge key, an underscored number and a timestamp that
    // stops at the minute are quoted as well.
    assert_eq!(
        y(
            json!({"a": "y", "b": "N", "c": "<<", "d": "1_000", "e": "2026-11-07 10:00", "f": "1e5"})
        ),
        "a: \"y\"\nb: \"N\"\nc: \"<<\"\nd: \"1_000\"\ne: \"2026-11-07 10:00\"\nf: \"1e5\""
    );
    // Unicode stays as is, quoted or not.
    assert_eq!(
        y(json!({"a": "日本", "b": "x\nü"})),
        "a: 日本\nb: \"x\\nü\""
    );
    // Keys take the same treatment.
    assert_eq!(
        y(json!({": x": 1, "123": 2, "": 3})),
        r#"": x": 1
"123": 2
"": 3"#
    );
    // Both stashes give the document: the helper writes it to the output itself, so the
    // escape function never sees it (and a value with a line break stays quoted, so the
    // exemption cannot smuggle one in).
    let ctx = json!({"doc": {"a": 1, "b": "x\ny"}});
    assert_eq!(render("{{yaml doc}}", ctx.clone()), "a: 1\nb: \"x\\ny\"");
    assert_eq!(render("{{{yaml doc}}}", ctx.clone()), "a: 1\nb: \"x\\ny\"");
    // Every other helper keeps its escaping: the exemption is this helper's alone. `json`
    // escapes a line break itself; `join` and a bare value still go through the escape.
    assert_eq!(
        render("{{json doc}}", json!({"doc": ["a\nb"]})),
        "[\"a\\nb\"]"
    );
    assert_eq!(render("{{join list}}", json!({"list": ["a\nb"]})), "a b");
    assert_eq!(render("{{this}}", json!("x\ny")), "x y");
}

/// What a YAML reader makes of the helper's output: exactly the document, with its types.
/// The strings are the ones item by item kept out of plain scalars, plus the shapes the
/// emitter writes around them.
#[test]
fn yaml_helper_round_trips_through_a_parser() {
    let long_key = "k".repeat(1100);
    let cases = vec![
        json!(null),
        json!(true),
        json!(7),
        json!(-3),
        json!(1.5),
        // Floats serde_json prints without a dot, with and without an exponent: the
        // reader must still resolve them as numbers.
        json!(1e20),
        json!(1.5e30),
        json!(1e-7),
        json!(1.0),
        json!(-0.0),
        json!(1e308),
        json!("hi"),
        json!(""),
        json!([]),
        json!({}),
        json!({"a": [1, {"b": [true, null, "x"]}], "c": {"d": {}}, "e": [[1], []]}),
        // A list item that is a mapping with a block of its own: the entries ride the
        // dash's line and the block indents past them.
        json!({"rows": [{"a": {"b": 1}, "c": [2, {"d": 3}], "e": "x: y"}, {"f": null}]}),
        // The keys as well: unquoted, `n:`, `123:` or `<<:` are a boolean, a number and a
        // merge key to a reader, not the strings the document's keys are.
        Value::Object(
            [
                ("y", 1),
                ("n", 2),
                ("123", 3),
                ("true", 4),
                ("null", 5),
                ("~", 6),
                ("2026-11-07", 7),
                ("<<", 8),
                ("a: b", 9),
                ("- x", 10),
                ("", 11),
                (" lead", 12),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), json!(v)))
            .collect(),
        ),
        // A key past 1.1's simple-key length: a reader refuses the `:` that follows it,
        // plain or quoted, so such a key cannot share its line with its value.
        Value::Object(
            [
                (long_key.clone(), 1),
                (long_key.clone() + " x", 2),
                ("k".repeat(1024), 3),
            ]
            .into_iter()
            .map(|(k, v)| (k, json!(v)))
            .collect(),
        ),
        // …and the same key inside a block and as a list item's key.
        json!({"m": {long_key.as_str(): {"deep": 1}}, "rows": [{ long_key.as_str(): [1, "x"] }]}),
        // Keys at the boundary of the 1024-character limit *as written*: the two `-` keys
        // are 1022 and 1023 raw and take quotes for their leading `-`, so it is the two
        // quote characters that decide which side of the limit they land on; the third is
        // 1024 raw but 1050 written, its 24 line breaks escaping to two characters each.
        {
            let mut keyed = serde_json::Map::new();
            keyed.insert(format!("-{}", "k".repeat(1021)), json!(1));
            keyed.insert(format!("-{}", "k".repeat(1022)), json!(2));
            keyed.insert(
                format!("{}{}{}", "m".repeat(500), "\n".repeat(24), "n".repeat(500)),
                json!(3),
            );
            Value::Object(keyed)
        },
        Value::Object(
            [
                ("int", "123"),
                ("dec", "1.5"),
                ("exp", "1e5"),
                ("hex", "0x1F"),
                ("oct", "0777"),
                ("oct2", "0o17"),
                ("bin", "0b101"),
                ("sex", "1:2:3"),
                // 1.1's float resolver reads a sexagesimal with a fraction as a number.
                ("sex_f", "1:20.5"),
                ("sex_f2", "190:20:30.15"),
                ("und", "1_000"),
                ("dot", ".5"),
                ("neg", "-3"),
                ("bool1", "true"),
                ("bool2", "yes"),
                ("bool3", "y"),
                ("bool4", "n"),
                ("bool5", "On"),
                ("nil", "null"),
                ("tilde", "~"),
                ("inf", ".inf"),
                ("nan", "-.nan"),
                ("date", "2026-11-07"),
                ("ts", "2026-11-07 10:00"),
                ("ts2", "2026-11-07T10:00:00.5-05:00"),
                ("merge", "<<"),
                ("dots", "..."),
                // A document-end marker at the start of a line, and a plain key past the
                // 1.1 implicit-key length, both refused by a reader unquoted.
                ("dots_x", "... x"),
                ("long_key", long_key.as_str()),
                ("dash", "- x"),
                ("quest", "? q"),
                ("cmt", "a #b"),
                ("colon", "a: b"),
                ("colon_end", "end:"),
                ("amp", "&anchor"),
                ("star", "*alias"),
                ("tag", "!!str"),
                ("pipe", "|"),
                ("fold", ">"),
                ("pct", "%YAML"),
                ("at", "@a"),
                ("tick", "`x"),
                ("brace", "{a}"),
                ("brack", "[a]"),
                ("comma", "a,b"),
                ("sq", "'x"),
                ("dq", "\"x"),
                ("lead", " lead"),
                ("trail", "trail "),
                ("tab", "a\tb"),
                ("nl", "l1\nl2"),
                ("cr", "a\rb"),
                ("nul", "a\0b"),
                ("del", "a\u{7f}b"),
                ("c1", "a\u{85}b"),
                ("c1b", "a\u{9f}b"),
                ("ls", "a\u{2028}b"),
                ("ps", "a\u{2029}b"),
                ("bom", "a\u{feff}b"),
                ("jp", "日本語"),
                ("emoji", "🎉"),
                ("acute", "e\u{301}"),
                ("inject", "x\nInjected: yes\n\nGET / HTTP/1.1"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.into())))
            .collect(),
        ),
    ];
    for v in cases {
        for src in ["{{yaml doc}}", "{{{yaml doc}}}"] {
            let doc = render(src, json!({"doc": v.clone()}));
            // The document's own line breaks are the only control characters in it.
            assert!(
                !doc.chars().any(|c| c != '\n' && c.is_control()),
                "a raw control character in {src}:\n{doc:?}"
            );
            assert_eq!(yaml_parse(&doc), v, "read back from {src}:\n{doc}");
        }
    }
}

/// The document as a YAML reader sees it, as JSON.
fn yaml_parse(doc: &str) -> Value {
    let v: yaml_serde::Value = yaml_serde::from_str(doc)
        .unwrap_or_else(|e| panic!("the document is not YAML ({e}):\n{doc}"));
    serde_json::to_value(v).unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn roots_resolve_inside_blocks() {
    let r = |s: &str| resolve_roots(s, ACTION_ROOTS);
    assert_eq!(
        r("{{vars.a}}{{#each keys}}{{vars.a}}{{this}}{{/each}}"),
        "{{vars.a}}{{#each keys}}{{@root.vars.a}}{{this}}{{/each}}"
    );
    assert_eq!(
            r("{{#each docs}}{{key}}{{upper (replace target.vars.p \"x\" \"y\")}}{{#if (eq op \"vars\")}}{{/if}}{{/each}}"),
            "{{#each docs}}{{key}}{{upper (replace @root.target.vars.p \"x\" \"y\")}}{{#if (eq op \"vars\")}}{{/if}}{{/each}}"
        );
    assert_eq!(
        r("{{#each docs as |d|}}{{d.key}}{{this.vars}}{{@root.rule}}{{../rule}}{{/each}}"),
        "{{#each docs as |d|}}{{d.key}}{{this.vars}}{{@root.rule}}{{../rule}}{{/each}}"
    );
    assert_eq!(
        r("{{#each keys}}{{!-- {{vars}} --}}{{{vars.x}}}{{/each}}"),
        "{{#each keys}}{{!-- {{vars}} --}}{{{@root.vars.x}}}{{/each}}"
    );
    assert_eq!(
        r("{{#with target}}{{x k=vars}}{{/with}}{{vars}}"),
        "{{#with target}}{{x k=@root.vars}}{{/with}}{{vars}}"
    );
}

#[test]
fn root_resolution_edge_cases() {
    let r = |s: &str| resolve_roots(s, ACTION_ROOTS);
    // An inverse section is a block of its own: closing it does not end the `each`.
    // `{{^}}` (else) keeps the `each` open.
    assert_eq!(
        r("{{#each keys}}{{this}}{{^}}{{rule}}{{/each}}{{rule}}"),
        "{{#each keys}}{{this}}{{^}}{{@root.rule}}{{/each}}{{rule}}"
    );
    assert_eq!(
        render(
            "{{#each keys}}{{this}}{{^}}none of {{rule}}{{/each}}",
            json!({"keys": [], "rule": "r"})
        ),
        "none of r"
    );
    // A block helper's parameters still resolve.
    assert_eq!(
        r("{{#each docs}}{{#each target.vars.list}}{{/each}}{{/each}}"),
        "{{#each docs}}{{#each @root.target.vars.list}}{{/each}}{{/each}}"
    );
    // Block parameters named like roots stay parameters.
    assert_eq!(
        r("{{#each docs as |rule|}}{{rule.key}}{{/each}}"),
        "{{#each docs as |rule|}}{{rule.key}}{{/each}}"
    );
    // Raw blocks are left alone, content included.
    assert_eq!(
        r("{{#each keys}}{{{{raw}}}}{{vars}}{{{{/raw}}}}{{/each}}"),
        "{{#each keys}}{{{{raw}}}}{{vars}}{{{{/raw}}}}{{/each}}"
    );
}

#[test]
fn interpolated_control_characters_are_neutralized() {
    let ctx = json!({"t": "x\r\nX-Evil: 1", "doc": {"a": "l1\nl2"}});
    assert_eq!(
        render("X-Title: {{t}}\n", ctx.clone()),
        "X-Title: x  X-Evil: 1\n"
    );
    assert_eq!(render("{{json doc}}", ctx.clone()), "{\"a\":\"l1\\nl2\"}");
    assert_eq!(render("{{{t}}}", ctx), "x\r\nX-Evil: 1");
}

#[test]
fn partials_are_refused() {
    for src in [
        "{{> action}}",
        "{{~> step0}}",
        "{{#> layout}}x{{/layout}}",
        "{{#*inline \"p\"}}x{{/inline}}",
        "{{ > x }}",
    ] {
        assert!(
            Templates::new().add("t", src, ACTION_ROOTS).is_err(),
            "{src}"
        );
    }
    Templates::new()
        .add("t", "{{#each keys}}{{this}}{{/each}} a > b", ACTION_ROOTS)
        .unwrap();
}

#[test]
fn redis_quote_joins_its_arguments() {
    assert_eq!(
        render("{{redis_quote p k}}", json!({"p": "cust:", "k": "a \"b"})),
        "\"cust:a \\\"b\""
    );
}

#[test]
fn decimals_stay_exact() {
    let v: Value = serde_json::from_str("{\"p\": 12.340000}").unwrap();
    assert_eq!(render("{{json p}} {{p}}", v), "12.340000 12.340000");
}

#[test]
fn key_fast_path() {
    assert_eq!(
        simple_key_template("item_{{new.product_id}}\n  group_{{ old.category_id }}x\n"),
        Some(vec![
            vec![Seg::Lit("item_".into()), Seg::Col("product_id".into())],
            vec![
                Seg::Lit("group_".into()),
                Seg::Col("category_id".into()),
                Seg::Lit("x".into())
            ],
        ])
    );
    assert_eq!(simple_key_template("{{upper new.a}}"), None);
    assert_eq!(simple_key_template("{{#if new.a}}x{{/if}}"), None);
    assert_eq!(simple_key_template("{{{new.a}}}"), None);
    assert_eq!(simple_key_template("{{vars.a}}"), None);
}
