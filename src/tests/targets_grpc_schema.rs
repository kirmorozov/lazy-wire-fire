use super::*;
use crate::targets::grpc::contract::PROTO_DIR;
use serde_json::json;

/// Compiles a schema the way a deployment's `.desc` was made: protox, no protoc. The
/// shipped contract is on the include path, so a test schema can import it.
///
/// One directory per call: the tests run in parallel, several of them compiling the same
/// name, and two sharing a file would let one read it while another is still writing it.
fn compile(name: &str, src: &str) -> Vec<u8> {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "lazywf-grpc-{}-{}-{name}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::write(dir.join(name), src).expect("write .proto");
    protox::Compiler::new([dir.as_path(), PROTO_DIR.as_ref()])
        .expect("compiler")
        .include_source_info(false)
        .include_imports(true)
        .open_files([dir.join(name)])
        .expect("compiles")
        .encode_file_descriptor_set()
}

fn pool_of(name: &str, src: &str) -> DescriptorPool {
    DescriptorPool::decode(compile(name, src).as_slice()).expect("decodes")
}

/// The order form of the design notes, with the shapes a real schema would have, and a
/// service that speaks lazywf's own contract.
const ORDER: &str = r#"
        syntax = "proto3";
        package orders.v1;
        import "lazywf/v1/lazywf.proto";

        message Line {
          string sku = 1;
          int32 qty = 2;
        }
        message Customer {
          string name = 1;
          string city = 2;
        }
        enum State {
          STATE_UNSPECIFIED = 0;
          STATE_NEW = 1;
          STATE_SHIPPED = 2;
        }
        message Item {
          int64 id = 1;
          string sku = 2;
          double price = 3;
          bool paid = 4;
          bytes signature = 5;
          repeated Line lines = 6;
          map<string, string> labels = 7;
          Customer customer = 8;
          State state = 9;
        }
        service Sink {
          rpc Push(stream lazywf.v1.Envelope) returns (stream lazywf.v1.Acks);
        }
        "#;

fn body() -> MessageDescriptor {
    pool_of("orders.proto", ORDER)
        .get_message_by_name("orders.v1.Item")
        .expect("Item")
}

fn doc(v: Json) -> JsonMap<String, Json> {
    v.as_object().expect("an object").clone()
}

fn get(msg: &DynamicMessage, name: &str) -> Value {
    msg.get_field_by_name(name).expect("set").into_owned()
}

fn col(column: &str, key: &str, kind: ValueKind) -> docs::Field {
    docs::Field {
        column: column.into(),
        key: key.into(),
        kind,
    }
}

#[test]
fn method_is_checked_against_the_contract() {
    let p = pool(None).expect("embedded");
    let m = check_method(&p, contract::SERVICE, contract::METHOD, None).expect("Push");
    assert_eq!(m.name(), contract::FULL_METHOD);
    assert!(m.body.is_none(), "no item_type means a JSON body");

    let e = check_method(&p, "orders.v1.Sink", "Push", None)
        .unwrap_err()
        .to_string();
    assert!(e.contains("no service"), "{e}");
    let e = check_method(&p, contract::SERVICE, "Pull", None)
        .unwrap_err()
        .to_string();
    assert!(e.contains("no method"), "{e}");
    let e = check_method(
        &p,
        contract::SERVICE,
        contract::METHOD,
        Some("orders.v1.Item"),
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("`item_type`"), "{e}");
}

/// A deployment's own descriptor set is added to the contract: its service is callable,
/// and lazywf's own method still is too.
#[test]
fn user_set_keeps_the_contract() {
    let p = pool(Some(&compile("orders.proto", ORDER))).expect("both files");
    let m = check_method(&p, "orders.v1.Sink", "Push", Some("orders.v1.Item")).expect("Push");
    assert_eq!(m.name(), "orders.v1.Sink/Push");
    assert_eq!(m.body.expect("item_type").full_name(), "orders.v1.Item");
    check_method(&p, contract::SERVICE, contract::METHOD, None).expect("still callable");

    // A method that is not bidi, or does not speak the contract, is refused by name.
    let bad = r#"
            syntax = "proto3";
            package orders.v1;
            import "lazywf/v1/lazywf.proto";
            message Item { string sku = 1; }
            service Sink { rpc Push(lazywf.v1.Envelope) returns (lazywf.v1.Acks); }
            service Sink2 { rpc Push(stream Item) returns (stream lazywf.v1.Acks); }
            "#;
    let p = pool_of("bad.proto", bad);
    let e = check_method(&p, "orders.v1.Sink", "Push", None)
        .unwrap_err()
        .to_string();
    assert!(e.contains("stream of envelopes"), "{e}");
    let e = check_method(&p, "orders.v1.Sink2", "Push", None)
        .unwrap_err()
        .to_string();
    assert!(e.contains("orders.v1.Item"), "{e}");
}

#[test]
fn scalars() {
    let msg = build(
        &body(),
        &doc(json!({
            "id": 42,
            "sku": "A-1",
            "price": 19.9,
            "paid": true,
            "signature": "aGVsbG8=",
        })),
    )
    .expect("builds");
    assert_eq!(get(&msg, "id"), Value::I64(42));
    assert_eq!(get(&msg, "sku"), Value::String("A-1".into()));
    assert_eq!(get(&msg, "price"), Value::F64(19.9));
    assert_eq!(get(&msg, "paid"), Value::Bool(true));
    assert_eq!(get(&msg, "signature"), Value::Bytes("hello".into()));
}

/// A number fills any numeric field and a quoted number fills a number, and an enum takes
/// its name or its number: what protobuf's own JSON mapping does, so a `VARCHAR` id is not
/// a schema error.
#[test]
fn numbers_and_strings_fill_numbers() {
    let item = body();
    let msg = build(
        &item,
        &doc(json!({"id": "42", "sku": "A", "state": "STATE_SHIPPED"})),
    )
    .expect("a quoted number is a number");
    assert_eq!(get(&msg, "id"), Value::I64(42));
    assert_eq!(get(&msg, "state"), Value::EnumNumber(2));

    // and text that holds no number is refused, naming what the field wanted
    let e = build(&item, &doc(json!({"id": "A-1"})))
        .unwrap_err()
        .to_string();
    assert!(e.contains(".id") && e.contains("not a number"), "{e}");

    let msg = build(&item, &doc(json!({"state": "STATE_SHIPPED"}))).expect("by name");
    assert_eq!(get(&msg, "state"), Value::EnumNumber(2));
    let msg = build(&item, &doc(json!({"state": 1}))).expect("by number");
    assert_eq!(get(&msg, "state"), Value::EnumNumber(1));

    let e = build(&item, &doc(json!({"state": 9})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a value of"), "{e}");
    let e = build(&item, &doc(json!({"state": "PAID"})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a value of"), "{e}");
}

/// A fraction does not fill an integer field, and an integer does fill a double one.
#[test]
fn fractions_are_not_integers() {
    let e = build(&body(), &doc(json!({"id": 19.90})))
        .unwrap_err()
        .to_string();
    assert!(e.contains(".id") && e.contains("whole number"), "{e}");
    let msg = build(&body(), &doc(json!({"price": 20}))).expect("int into double");
    assert_eq!(get(&msg, "price"), Value::F64(20.0));
}

#[test]
fn nested_and_repeated_and_maps() {
    let msg = build(
        &body(),
        &doc(json!({
            "customer": {"name": "Ada", "city": "Bath"},
            "lines": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 3}],
            "labels": {"channel": "web", "region": "eu"},
        })),
    )
    .expect("builds");
    let Value::Message(c) = get(&msg, "customer") else {
        panic!("customer is a message")
    };
    assert_eq!(
        c.get_field_by_name("city").unwrap().as_str().unwrap(),
        "Bath"
    );
    let Value::List(lines) = get(&msg, "lines") else {
        panic!("lines is a list")
    };
    assert_eq!(lines.len(), 2);
    let qty = lines[1].as_message().unwrap().get_field_by_name("qty");
    assert_eq!(qty.unwrap().as_i32(), Some(3));
    let Value::Map(labels) = get(&msg, "labels") else {
        panic!("labels is a map")
    };
    assert_eq!(
        labels.get(&MapKey::String("channel".into())).unwrap(),
        &Value::String("web".into())
    );
}

/// The ways a document can be wrong, each naming the path and what the field wanted.
#[test]
fn refusals_name_the_path() {
    let item = body();
    let e = build(&item, &doc(json!({"skux": "A"})))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains(".skux") && e.contains("not a field") && e.contains("sku"),
        "{e}"
    );

    let e = build(&item, &doc(json!({"lines": [{"sku": "A", "qty": "two"}]})))
        .unwrap_err()
        .to_string();
    assert!(e.contains(".lines[0].qty"), "{e}");

    let e = build(&item, &doc(json!({"customer": "Ada"})))
        .unwrap_err()
        .to_string();
    assert!(e.contains(".customer") && e.contains("object"), "{e}");

    let e = build(&item, &doc(json!({"lines": "A"})))
        .unwrap_err()
        .to_string();
    // A repeated field of messages still wants an array, not the object a message wants.
    assert!(
        e.contains(".lines") && e.contains("repeated") && e.contains("send an array"),
        "{e}"
    );

    let e = build(&item, &doc(json!({"id": null})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("null") && e.contains("default"), "{e}");

    let e = build(&item, &doc(json!({"labels": {"a": []}})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("[a]"), "{e}");

    let e = build(&item, &doc(json!({"signature": "not base64!!"})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("base64"), "{e}");
}

/// protobuf's JSON mapping accepts the lowerCamelCase name of a field as well as its proto
/// name, so a rule that writes `orderId` for a field `order_id` is not an error.
#[test]
fn json_names_are_accepted() {
    let src = r#"
            syntax = "proto3";
            package orders.v1;
            message Item { int64 order_id = 1; }
            "#;
    let item = pool_of("jsonname.proto", src)
        .get_message_by_name("orders.v1.Item")
        .expect("Item");
    assert_eq!(field_of(&item, "order_id").unwrap().json_name(), "orderId");
    assert!(build(&item, &doc(json!({"orderId": 7}))).is_ok());
    assert!(build(&item, &doc(json!({"order_id": 7}))).is_ok());
    // and the check accepts both too
    assert!(column_errors(&item, &[col("o", "orderId", ValueKind::Int)]).is_empty());
}

/// A column that names no field, and one whose type cannot fill the field it names, are
/// both refused with the column and the field in the message.
#[test]
fn columns_are_checked_against_the_schema() {
    let item = body();
    assert!(column_errors(
        &item,
        &[
            col("id", "id", ValueKind::Int),
            col("sku", "sku", ValueKind::Str),
            col("price", "price", ValueKind::Double),
            col("lines[]", "lines", ValueKind::Array),
            col("customer.name", "customer", ValueKind::Object),
            col("meta:json", "labels", ValueKind::Unknown),
        ]
    )
    .is_empty());

    let e = column_errors(&item, &[col("skux", "skux", ValueKind::Str)]);
    assert_eq!(e.len(), 1);
    assert!(
        e[0].contains("column `skux`") && e[0].contains("not a field") && e[0].contains("sku"),
        "{e:?}"
    );

    let e = column_errors(&item, &[col("price", "id", ValueKind::Double)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("CAST(col AS SIGNED)"), "{e:?}");

    // A binary column into a bytes field would arrive mangled: refused, with the fix.
    let e = column_errors(&item, &[col("sig", "signature", ValueKind::Blob)]);
    assert_eq!(e.len(), 1);
    assert!(e[0].contains("TO_BASE64"), "{e:?}");
    // as text, which is what `TO_BASE64` returns, it is fine.
    assert!(column_errors(&item, &[col("sig", "signature", ValueKind::Str)]).is_empty());

    // A collect column fills a repeated field and nothing else.
    assert!(column_errors(&item, &[col("lines[]", "lines", ValueKind::Array)]).is_empty());
    let e = column_errors(&item, &[col("sku[]", "sku", ValueKind::Array)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("repeated"), "{e:?}");

    // An integer column into a string field is refused; text into a number is not.
    let e = column_errors(&item, &[col("id", "sku", ValueKind::Int)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(column_errors(&item, &[col("id", "id", ValueKind::Str)]).is_empty());
}

/// A field no column fills is worth a warning, and names the field.
#[test]
fn uncovered_fields_warn() {
    let item = body();
    let w = uncovered(
        &item,
        &[
            col("id", "id", ValueKind::Int),
            col("sku", "sku", ValueKind::Str),
        ],
    );
    assert!(
        w.iter().any(|w| w.starts_with("`price` gets nothing")),
        "{w:?}"
    );
    assert!(!w.iter().any(|w| w.contains("`id`")), "{w:?}");
    assert_eq!(w.len(), item.fields().count() - 2);
}

/// The `fits` table, as every column kind against every field type.
#[test]
fn fits_table() {
    for f in [
        "int32", "int64", "uint32", "uint64", "float", "double", "enum",
    ] {
        assert!(fits(ValueKind::Int, f), "int into {f}");
    }
    for f in ["string", "bytes", "bool", "message", "map", "repeated"] {
        assert!(!fits(ValueKind::Int, f), "int into {f}");
    }
    assert!(fits(ValueKind::Double, "double") && fits(ValueKind::Double, "float"));
    for f in ["int64", "bool", "string"] {
        assert!(!fits(ValueKind::Double, f), "double into {f}");
    }
    assert!(fits(ValueKind::Str, "bytes") && fits(ValueKind::Str, "int64"));
    for f in ["bool", "message", "map", "repeated"] {
        assert!(!fits(ValueKind::Str, f), "string into {f}");
    }
    assert!(fits(ValueKind::Blob, "string"));
    for f in ["bytes", "int32", "message"] {
        assert!(!fits(ValueKind::Blob, f), "blob into {f}");
    }
    assert!(fits(ValueKind::Array, "repeated"));
    for f in ["map", "string", "message"] {
        assert!(!fits(ValueKind::Array, f), "array into {f}");
    }
    assert!(fits(ValueKind::Object, "map") && fits(ValueKind::Object, "message"));
    assert!(!fits(ValueKind::Object, "repeated"));
    for f in [
        "bool", "int32", "string", "bytes", "enum", "message", "map", "repeated",
    ] {
        assert!(fits(ValueKind::Unknown, f), "json into {f}");
    }
}

// ——— the arms the order form does not reach: map keys of every type, text of
// every numeric width, and what a field says it wants —————————————————————————

/// A schema of the shapes the order form lacks: bool and unsigned map keys, a `float`,
/// a `uint64`, an enum and `bytes` next to a plain bool.
const WIDE: &str = r#"
        syntax = "proto3";
        package shapes.v1;
        enum State {
          STATE_UNSPECIFIED = 0;
          STATE_NEW = 1;
        }
        message Wide {
          map<bool, string> flags = 1;
          map<uint32, string> codes = 2;
          map<uint64, string> bigs = 3;
          uint64 count = 4;
          float rate = 5;
          double amount = 6;
          bool paid = 7;
          string sku = 8;
          State st = 9;
          bytes blob = 10;
        }
        "#;

fn wide() -> MessageDescriptor {
    pool_of("wide.proto", WIDE)
        .get_message_by_name("shapes.v1.Wide")
        .expect("Wide")
}

#[test]
fn map_keys_are_read_as_their_type_and_refused_as_wrong_ones() {
    let m = build(
        &wide(),
        &doc(json!({
            "flags": {"true": "yes", "false": "no"},
            "codes": {"7": "a"},
            "bigs": {"9": "b"},
        })),
    )
    .expect("builds");
    let Value::Map(flags) = get(&m, "flags") else {
        panic!("flags is a map")
    };
    assert_eq!(
        flags.get(&MapKey::Bool(true)).unwrap(),
        &Value::String("yes".into())
    );
    assert_eq!(
        flags.get(&MapKey::Bool(false)).unwrap(),
        &Value::String("no".into())
    );
    let Value::Map(codes) = get(&m, "codes") else {
        panic!("codes is a map")
    };
    assert_eq!(
        codes.get(&MapKey::U32(7)).unwrap(),
        &Value::String("a".into())
    );
    let Value::Map(bigs) = get(&m, "bigs") else {
        panic!("bigs is a map")
    };
    assert_eq!(
        bigs.get(&MapKey::U64(9)).unwrap(),
        &Value::String("b".into())
    );

    // A key the type cannot hold, each naming the key and why.
    let e = build(&wide(), &doc(json!({"flags": {"yes": "x"}})))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("flags[yes]") && e.contains("not `true` or `false`"),
        "{e}"
    );
    let e = build(&wide(), &doc(json!({"codes": {"-1": "x"}})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("codes[-1]") && e.contains("non-negative"), "{e}");
    let e = build(&wide(), &doc(json!({"codes": {"x": "y"}})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a non-negative integer"), "{e}");
    let e = build(&wide(), &doc(json!({"bigs": {"-2": "y"}})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("bigs[-2]") && e.contains("non-negative"), "{e}");
}

#[test]
fn text_of_every_numeric_width_and_a_number_in_a_field_that_is_not_one() {
    // Quoted numbers fill the numeric field of their width.
    let m = build(
        &wide(),
        &doc(json!({"count": "42", "rate": "1.5", "amount": "2.25"})),
    )
    .expect("builds");
    assert_eq!(get(&m, "count").as_u64(), Some(42));
    assert_eq!(get(&m, "rate").as_f32(), Some(1.5));
    assert_eq!(get(&m, "amount").as_f64(), Some(2.25));
    // Text that is not a number of that width, per width.
    for (field, value, want) in [
        ("amount", "wide", "a `double` field"),
        ("rate", "wide", "a `float` field"),
        ("count", "wide", "a `uint64` field"),
    ] {
        let e = build(&wide(), &doc(json!({field: value})))
            .unwrap_err()
            .to_string();
        assert!(e.contains("which is not a number that fits"), "{e}");
        assert!(e.contains(want), "{e}");
    }
    // A number in a field that is not one: the strict JSON mapping refuses it.
    let e = build(&wide(), &doc(json!({"paid": 1})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("is a number and `paid` is a bool field"), "{e}");
    // And an unsigned field still refuses a fraction.
    let e = build(&wide(), &doc(json!({"count": 1.5})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a whole number"), "{e}");
}

#[test]
fn a_field_names_what_it_wants_and_a_shape_names_itself() {
    for (field, value, want) in [
        ("blob", json!(true), "send base64 text"),
        ("st", json!(true), "send the name or the number"),
        ("paid", json!([]), "send true or false"),
    ] {
        let e = build(&wide(), &doc(json!({field: value})))
            .unwrap_err()
            .to_string();
        assert!(e.contains(want), "{field}={value}: {e}");
    }
    // What a JSON value is, in the error of the map that cannot hold it.
    for (value, want) in [
        (json!(null), "is null and `flags` is a map"),
        (json!(true), "is a bool and `flags` is a map"),
        (json!(5), "is a number and `flags` is a map"),
    ] {
        let e = build(&wide(), &doc(json!({"flags": value})))
            .unwrap_err()
            .to_string();
        assert!(e.contains(want), "{value}: {e}");
    }
}

#[test]
fn column_errors_name_every_column_type_and_its_fix() {
    // A decimal column into an unsigned field: named by type, with the SQL fix.
    let e = column_errors(&wide(), &[col("c", "count", ValueKind::Double)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("decimal or floating-point column"), "{e:?}");
    assert!(e[0].contains("uint64 field"), "{e:?}");
    assert!(e[0].contains("CAST(col AS SIGNED)"), "{e:?}");
    // A collect column into a float: named as a repeated mismatch.
    let e = column_errors(&wide(), &[col("r", "rate", ValueKind::Array)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(
        e[0].contains("float field") && e[0].contains("collect column"),
        "{e:?}"
    );
    // Text into a bool: the one hint that names true or false.
    let e = column_errors(&wide(), &[col("p", "paid", ValueKind::Str)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("a bool field wants true or false"), "{e:?}");
    // A JSON object column into a plain string field: it wants a real object field.
    let e = column_errors(&wide(), &[col("s", "sku", ValueKind::Object)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(
        e[0].contains("column with a path") && e[0].contains("wants a JSON column"),
        "{e:?}"
    );
    // A binary column into a field that holds no object: it says to send text instead.
    let e = column_errors(&wide(), &[col("b", "paid", ValueKind::Blob)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(e[0].contains("binary column holds no object"), "{e:?}");
}

/// The three arms that were `unreachable!` refuse now instead of panicking: a descriptor
/// that disagrees with itself, a field that is not a map handed to the map route, and a
/// non-number asked to parse text — each an `Err` naming the field and the kind, so a bad
/// descriptor is a load-time refusal or a transient, never a panic on the first send.
#[test]
fn the_arms_that_cannot_be_reached_refuse_now() {
    let item = body();
    let line_item = pool_of("line.proto", ORDER)
        .get_message_by_name("orders.v1.Line")
        .expect("Line");
    let qty = line_item.get_field_by_name("qty").expect("qty");
    let path = vec![".qty".to_string()];

    // The arm `fill` cannot reach: prost-reflect's `try_set_field` only ever answers
    // `InvalidType`, so `NotFound` is built directly — and the answer is a named refusal,
    // not a panic, if a descriptor ever does disagree with its message.
    let e = set_field_error(&item, &qty, &path, SetFieldError::NotFound).to_string();
    assert!(
        e.contains("qty") && e.contains("orders.v1.Item"),
        "names the field and the message: {e}"
    );

    // A field that is no map, asked as one: refused naming the field and its kind.
    let e = map_value(&qty, &Json::Object(JsonMap::new()), &path)
        .expect_err("an int32 is no entry message");
    let e = e.to_string();
    assert!(e.contains("qty") && e.contains("int32"), "{e}");

    // A field that is no number, asked to read a number from text.
    let sku = item.get_field_by_name("sku").expect("sku");
    let e = number_text("7", &sku, ".sku")
        .expect_err("a string field holds no numbers")
        .to_string();
    assert!(e.contains("string") && e.contains("sku"), "{e}");
}

// ——— the shapes the order form and the wide one both lack: the signed map-key widths,
// a `uint32`, a message with no fields at all, and the two contract checks that a
// deployment's own descriptor set is the one to fail ——————————————————————————————

/// Signed map keys of both widths, an unsigned scalar, a plain message and an empty one.
const NARROW: &str = r#"
        syntax = "proto3";
        package narrow.v1;
        message Nested { string a = 1; }
        message Empty {}
        message Narrow {
          map<int32, string> signed32 = 1;
          map<int64, string> signed64 = 2;
          uint32 code = 3;
          int32 qty = 4;
          float rate = 5;
          string sku = 6;
          Nested nested = 7;
        }
        "#;

fn narrow() -> MessageDescriptor {
    pool_of("narrow.proto", NARROW)
        .get_message_by_name("narrow.v1.Narrow")
        .expect("Narrow")
}

#[test]
fn the_signed_map_key_widths_read_a_number_and_refuse_text() {
    let m = build(
        &narrow(),
        &doc(json!({"signed32": {"-7": "a"}, "signed64": {"-9000000000": "b"}})),
    )
    .expect("a signed key of either width");
    let Value::Map(s32) = get(&m, "signed32") else {
        panic!("signed32 is a map")
    };
    assert_eq!(
        s32.get(&MapKey::I32(-7)).unwrap(),
        &Value::String("a".into())
    );
    let Value::Map(s64) = get(&m, "signed64") else {
        panic!("signed64 is a map")
    };
    assert_eq!(
        s64.get(&MapKey::I64(-9_000_000_000)).unwrap(),
        &Value::String("b".into())
    );
    // Text where a signed key of either width belongs: the width is named by the refusal, so
    // an author sees `integer` and not `non-negative integer` (which is what an unsigned key
    // of the same digits says).
    for (field, key) in [("signed32", "x"), ("signed64", "x")] {
        let e = build(&narrow(), &doc(json!({field: {key: "v"}})))
            .unwrap_err()
            .to_string();
        assert!(
            e.contains(&format!("{field}[{key}]")) && e.contains("the key is not an integer"),
            "{field}: {e}"
        );
    }
    // Out of the width of the key type: the same refusal, because the key is text.
    let e = build(&narrow(), &doc(json!({"signed32": {"3000000000": "v"}})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("the key is not an integer"), "{e}");
}

#[test]
fn a_map_key_of_a_type_no_map_takes_is_refused_by_name() {
    // The one arm of the key reader no schema can reach, because protobuf allows a map key to
    // be only text, bool or an integer: called with the field a descriptor cannot give it, it
    // refuses with what a key may be rather than guessing.
    let rate = narrow().get_field_by_name("rate").expect("rate");
    let e = map_key(&rate, "1", &[".rate".to_string()])
        .expect_err("a float is no map key type")
        .to_string();
    assert!(
        e.contains(".rate[1]") && e.contains("a map key is text or a number"),
        "{e}"
    );
}

#[test]
fn an_unsigned_scalar_reads_text_and_a_number_of_its_width() {
    let m = build(&narrow(), &doc(json!({"code": "4294967295"}))).expect("text into uint32");
    assert_eq!(get(&m, "code").as_u32(), Some(4_294_967_295));
    let m = build(&narrow(), &doc(json!({"code": 7}))).expect("a number into uint32");
    assert_eq!(get(&m, "code").as_u32(), Some(7));
    // Text that is not a number of that width, and a number that is not one of it: both name
    // the field's own kind, `uint32`, so the fix is visible in the message.
    let e = build(&narrow(), &doc(json!({"code": "-1"})))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("is `-1`") && e.contains("a `uint32` field"),
        "{e}"
    );
    let e = build(&narrow(), &doc(json!({"code": 1.5})))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("not a whole number") && e.contains("`code` is a uint32 field"),
        "{e}"
    );
    // A number with a fraction fills the float field it belongs to.
    let m = build(&narrow(), &doc(json!({"rate": 1.5}))).expect("a number into float");
    assert_eq!(get(&m, "rate").as_f32(), Some(1.5));
}

#[test]
fn an_object_in_a_field_that_holds_no_object_says_so() {
    // `shape` names a JSON object in the error of a scalar field that was handed one.
    let e = build(&narrow(), &doc(json!({"sku": {"a": 1}})))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("is an object and `sku` is a string field"),
        "{e}"
    );
    // A numeric field handed a list says what it wants rather than nothing.
    let e = build(&narrow(), &doc(json!({"code": []})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("send a number"), "{e}");
}

#[test]
fn a_message_with_no_fields_says_it_has_none() {
    // The error says which fields the message does have. A message with none must say so,
    // or the author reads an empty list as a broken message.
    let empty = pool_of("narrow.proto", NARROW)
        .get_message_by_name("narrow.v1.Empty")
        .expect("Empty");
    let e = build(&empty, &doc(json!({"anything": 1})))
        .unwrap_err()
        .to_string();
    assert!(e.contains("is not a field of `narrow.v1.Empty`"), "{e}");
    assert!(e.contains("(it has: none)"), "{e}");
    // The other side of the same line: a message with fields lists them, sorted.
    let e = build(&narrow(), &doc(json!({"nope": 1})))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("it has: code, nested, qty, rate, signed32, signed64, sku"),
        "{e}"
    );
}

#[test]
fn a_column_that_cannot_fill_a_field_without_a_recipe_says_make_them_agree() {
    // Every mismatch has a hint. The ones with no specific fix (a text column into a
    // message, which has no SQL that makes it one) still say what to do in general, and the
    // name of the column kind is there too.
    let e = column_errors(&narrow(), &[col("n", "nested", ValueKind::Str)]);
    assert_eq!(e.len(), 1, "{e:?}");
    assert!(
        e[0].contains("make the column and the field agree"),
        "{e:?}"
    );
    assert!(e[0].contains("text column"), "{e:?}");
    // A JSON column is named by what it is, wherever it turns up.
    assert_eq!(column_kind(ValueKind::Unknown), "JSON column");
}

#[test]
fn a_field_the_message_holds_is_refused_as_the_wrong_type_by_name() {
    // The other arm of the same error: prost-reflect refuses a value of another type, and the
    // message says what the field is, not merely that something was wrong. `narrow` is the
    // message that carries a top-level `int32 qty`; the envelope's own message does not.
    let item = narrow();
    let qty = item.get_field_by_name("qty").expect("qty");
    let e = set_field_error(
        &item,
        &qty,
        &[".qty".to_string()],
        SetFieldError::InvalidType {
            field: qty.clone(),
            value: Value::Bool(true),
        },
    )
    .to_string();
    assert!(
        e.contains(".qty") && e.contains("is not a int32 value"),
        "{e}"
    );
}

#[test]
fn a_method_of_the_wrong_answer_is_refused_by_its_own_name() {
    // The contract check that looks at the answer: bidi, and it takes an Envelope — but it
    // returns something else. The refusal names both what it must return and what it does.
    let src = r#"
        syntax = "proto3";
        package shape.v1;
        import "lazywf/v1/lazywf.proto";
        message Wrong { string x = 1; }
        service Ok { rpc Push(stream lazywf.v1.Envelope) returns (stream lazywf.v1.Acks); }
        service WrongOut { rpc Push(stream lazywf.v1.Envelope) returns (stream Wrong); }
        "#;
    let p = pool_of("shape.proto", src);
    check_method(&p, "shape.v1.Ok", "Push", None).expect("the contract shape");
    let e = check_method(&p, "shape.v1.WrongOut", "Push", None)
        .unwrap_err()
        .to_string();
    assert!(e.contains("must return `lazywf.v1.Acks`"), "{e}");
    assert!(e.contains("it returns `shape.v1.Wrong`"), "{e}");
}

#[test]
fn an_envelope_of_its_own_that_holds_no_items_is_refused() {
    // A deployment's descriptor set may know a message of the contract's own name. It is
    // checked against the contract by name, and the check that the envelope holds the items
    // is the one that must refuse it — not a panic on a missing field.
    let src = r#"
        syntax = "proto3";
        package lazywf.v1;
        message Envelope { string key = 1; }
        message Acks {}
        service Sink { rpc Push(stream Envelope) returns (stream Acks); }
        "#;
    let p = pool_of("own.proto", src);
    let e = check_method(&p, "lazywf.v1.Sink", "Push", None)
        .unwrap_err()
        .to_string();
    assert!(e.contains("`items`"), "{e}");
    assert!(e.contains("lazywf.v1.Envelope"), "{e}");
}
