//! The schema of a `grpc` target: the descriptor set it names, the method it asks for, and
//! the mapping from a rendered document to a protobuf message.
//!
//! A descriptor set is a schema lazywf reads, not a contract it must be given: the pool every
//! target's method is looked up in always holds lazywf's own [`crate::grpc::contract`] first,
//! so `grpcs://host/lazywf.v1.Sink/Push` works with no configuration at all, and a target that
//! ships a `.desc` only adds to it. The envelope and the acks are fixed by that contract; the
//! body of an item is what the target's `item_type` says, which is the only part of the wire a
//! deployment chooses. With no `item_type` the body is the document as JSON, so the kind works
//! with no schema at all and a schema is what buys the smaller wire.
//!
//! The mapping is protobuf's own JSON mapping, restricted to the shape lazywf produces: keys
//! are field names or their lowerCamelCase JSON names, a number fills any numeric field, a
//! string fills a string, `bytes` (base64) or an enum, an object fills a message or a map, an
//! array fills a repeated field. Anything else is refused with the field named — at load time
//! where the rule's columns say enough to tell, at run time where they do not.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor,
    MethodDescriptor, SetFieldError, Value,
};
use serde_json::{Map as JsonMap, Number, Value as Json};

use crate::docs::{self, ValueKind};
use crate::grpc::contract;

/// What a target's endpoint names, checked against the descriptor set.
#[derive(Debug)]
pub struct Method {
    /// The method, for the call itself.
    pub descriptor: MethodDescriptor,
    /// The message an item's body is built from, if the target named an `item_type`. `None`
    /// means the body is the document as JSON.
    pub body: Option<MessageDescriptor>,
}

impl Method {
    /// The method's full name, as the endpoint names it and as the HTTP/2 path carries it.
    pub fn name(&self) -> String {
        format!(
            "{}/{}",
            self.descriptor.parent_service().full_name(),
            self.descriptor.name()
        )
    }
}

/// lazywf's own contract, as a pool. Built once: it is a committed constant, and a test checks
/// that it decodes and holds the method.
fn embedded() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(contract::DESCRIPTOR)
            .expect("the committed contract descriptor decodes")
    })
}

/// The pool a target's method is looked up in: lazywf's own contract, then the descriptor set
/// the target names, if it names one.
///
/// The contract is seeded first for two reasons: a server can implement
/// `lazywf.v1.Sink/Push` with no descriptor of its own, and a deployment's `.desc` that imports
/// lazywf's `.proto` resolves whether or not it was compiled with `--include_imports` (a file of
/// a name already in the pool is skipped, so shipping lazywf's own file back is harmless).
pub fn pool(descriptor_set: Option<&[u8]>) -> Result<DescriptorPool, anyhow::Error> {
    let mut pool = embedded().clone();
    if let Some(bytes) = descriptor_set {
        pool.decode_file_descriptor_set(bytes)
            .context("`descriptor_set` is not a protobuf descriptor set")?;
    }
    Ok(pool)
}

/// Checks the method an endpoint names, and resolves the message an item's body is built from.
///
/// The call must be bidi streaming with lazywf's own envelope in and acks out: that is the
/// contract, and it is checked here because both mistakes are configuration errors that would
/// otherwise look like a server that never answers. `item_type` is the target's own choice; it
/// has to exist in the descriptor set, which is also checked here.
pub fn check_method(
    pool: &DescriptorPool,
    service: &str,
    method: &str,
    item_type: Option<&str>,
) -> Result<Method, anyhow::Error> {
    let svc = pool
        .get_service_by_name(service)
        .ok_or_else(|| anyhow!("the descriptor set has no service `{service}`"))?;
    let m = svc
        .methods()
        .find(|m| m.name() == method)
        .ok_or_else(|| anyhow!("service `{service}` has no method `{method}`"))?;
    anyhow::ensure!(
        m.is_client_streaming(),
        "`{service}/{method}` must take a stream of envelopes: it is not a client-streaming method"
    );
    anyhow::ensure!(
        m.is_server_streaming(),
        "`{service}/{method}` must return a stream of acks: it is not a server-streaming method"
    );
    let input = m.input();
    anyhow::ensure!(
        input.full_name() == ENVELOPE,
        "`{service}/{method}` must take `{ENVELOPE}`: it takes `{}`",
        input.full_name()
    );
    let output = m.output();
    anyhow::ensure!(
        output.full_name() == ACKS,
        "`{service}/{method}` must return `{ACKS}`: it returns `{}`",
        output.full_name()
    );
    let items = input.get_field_by_name("items").ok_or_else(|| {
        anyhow!("`{ENVELOPE}` as the descriptor set knows it has no `items` field")
    })?;
    anyhow::ensure!(
        items.cardinality() == Cardinality::Repeated,
        "`{ENVELOPE}.items` must be repeated"
    );
    anyhow::ensure!(
        matches!(items.kind(), Kind::Message(_)),
        "`{ENVELOPE}.items` must be messages"
    );
    let body =
        match item_type {
            None => None,
            Some(name) => Some(pool.get_message_by_name(name).ok_or_else(|| {
                anyhow!("the descriptor set has no message `{name}` (`item_type`)")
            })?),
        };
    Ok(Method {
        descriptor: m,
        body,
    })
}

const ENVELOPE: &str = "lazywf.v1.Envelope";
const ACKS: &str = "lazywf.v1.Acks";

/// Builds the message one item's document is meant to become.
///
/// `doc` is the document the rule rendered, so its keys are the rule's columns. A key that is
/// not a field, or a value that cannot fill the field it names, is an error naming the path:
/// the caller sends nothing and reports it as the item's status text.
pub fn build(body: &MessageDescriptor, doc: &JsonMap<String, Json>) -> Result<DynamicMessage> {
    build_at(body, doc, &[])
}

/// Builds a message whose values came from `doc`, itself at path `path` in the document: every
/// error names the whole path, so a value three levels down is still findable.
fn build_at(
    body: &MessageDescriptor,
    doc: &JsonMap<String, Json>,
    path: &[String],
) -> Result<DynamicMessage> {
    let mut msg = DynamicMessage::new(body.clone());
    fill(body, doc, &mut msg, path)?;
    Ok(msg)
}

fn fill(
    body: &MessageDescriptor,
    doc: &JsonMap<String, Json>,
    msg: &mut DynamicMessage,
    path: &[String],
) -> Result<()> {
    for (name, value) in doc {
        let p = extend(path, name);
        let field = field_of(body, name).ok_or_else(|| unknown_field(body, &p))?;
        let v = to_value(&field, value, &p)?;
        msg.try_set_field(&field, v).map_err(|e| match e {
            SetFieldError::InvalidType { .. } => {
                anyhow!("`{}` is not a {} value", p.join(""), kind_name(&field))
            }
            SetFieldError::NotFound => unreachable!("the field was just found on this message"),
        })?;
    }
    Ok(())
}

/// A field by its proto name or by its lowerCamelCase JSON name, which is what protobuf's own
/// JSON mapping accepts.
fn field_of(body: &MessageDescriptor, name: &str) -> Option<FieldDescriptor> {
    body.get_field_by_name(name)
        .or_else(|| body.get_field_by_json_name(name))
}

fn unknown_field(body: &MessageDescriptor, path: &[String]) -> anyhow::Error {
    anyhow!(
        "`{}` is not a field of `{}` (it has: {})",
        path.join(""),
        body.full_name(),
        field_names(body)
    )
}

/// The message's field names, for an error that says what it does have.
fn field_names(body: &MessageDescriptor) -> String {
    let mut names: Vec<String> = body.fields().map(|f| f.name().to_string()).collect();
    names.sort_unstable();
    names.truncate(20);
    if names.is_empty() {
        "none".to_string()
    } else {
        names.join(", ")
    }
}

fn extend(path: &[String], name: &str) -> Vec<String> {
    let mut p = path.to_vec();
    p.push(format!(".{name}"));
    p
}

fn indexed(path: &[String], i: usize) -> Vec<String> {
    let mut p = path.to_vec();
    p.push(format!("[{i}]"));
    p
}

/// One value as its field's protobuf type.
fn to_value(field: &FieldDescriptor, value: &Json, path: &[String]) -> Result<Value> {
    if field.is_map() {
        return map_value(field, value, path);
    }
    if field.cardinality() == Cardinality::Repeated {
        let Json::Array(items) = value else {
            bail!(
                "`{}` is {} and `{}` is a repeated field{}",
                path.join(""),
                shape(value),
                field.name(),
                wants(field)
            );
        };
        let mut out = Vec::with_capacity(items.len());
        for (i, item) in items.iter().enumerate() {
            out.push(scalar(field, item, &indexed(path, i))?);
        }
        return Ok(Value::List(out));
    }
    scalar(field, value, path)
}

/// One key of a map field. The JSON object's keys are the map's keys as text, as in
/// protobuf's JSON mapping.
fn map_value(field: &FieldDescriptor, value: &Json, path: &[String]) -> Result<Value> {
    let entry = match field.kind() {
        Kind::Message(m) => m,
        _ => unreachable!("a map field's kind is its entry message"),
    };
    let not_a_map = || anyhow!("`{}` is not a map field", path.join(""));
    let key_field = entry.get_field_by_name("key").ok_or_else(not_a_map)?;
    let value_field = entry.get_field_by_name("value").ok_or_else(not_a_map)?;
    let Json::Object(map) = value else {
        bail!(
            "`{}` is {} and `{}` is a map",
            path.join(""),
            shape(value),
            field.name()
        );
    };
    let mut out = HashMap::with_capacity(map.len());
    for (k, v) in map {
        let key = map_key(&key_field, k, path)?;
        let mut p = path.to_vec();
        p.push(format!("[{k}]"));
        out.insert(key, scalar(&value_field, v, &p)?);
    }
    Ok(Value::Map(out))
}

fn map_key(field: &FieldDescriptor, key: &str, path: &[String]) -> Result<MapKey> {
    let bad = |why: &str| {
        anyhow!(
            "`{}[{key}]`: a map of this key type cannot hold it ({why})",
            path.join("")
        )
    };
    Ok(match field.kind() {
        Kind::String => MapKey::String(key.to_string()),
        Kind::Bool => match key {
            "true" => MapKey::Bool(true),
            "false" => MapKey::Bool(false),
            _ => return Err(bad("not `true` or `false`")),
        },
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            MapKey::I32(key.parse().map_err(|_| bad("the key is not an integer"))?)
        }
        Kind::Uint32 | Kind::Fixed32 => MapKey::U32(
            key.parse()
                .map_err(|_| bad("the key is not a non-negative integer"))?,
        ),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            MapKey::I64(key.parse().map_err(|_| bad("the key is not an integer"))?)
        }
        Kind::Uint64 | Kind::Fixed64 => MapKey::U64(
            key.parse()
                .map_err(|_| bad("the key is not a non-negative integer"))?,
        ),
        _ => return Err(bad("a map key is text or a number")),
    })
}

/// One value as a scalar of its field's type: no lists, no maps.
fn scalar(field: &FieldDescriptor, value: &Json, path: &[String]) -> Result<Value> {
    let here = path.join("");
    let kind = field.kind();
    match (value, &kind) {
        // protobuf has no null: leaving the key out is how a default is meant.
        (Json::Null, _) => bail!(
            "`{here}` is null and `{}` is a {} field; leave the key out to mean its default",
            field.name(),
            kind_name(field)
        ),
        (Json::Bool(b), Kind::Bool) => Ok(Value::Bool(*b)),
        (Json::String(s), Kind::String) => Ok(Value::String(s.clone())),
        (Json::String(s), Kind::Bytes) => base64(s, &here).map(|b| Value::Bytes(b.into())),
        (Json::String(s), Kind::Enum(e)) => e
            .get_value_by_name(s)
            .map(|v| Value::EnumNumber(v.number()))
            .ok_or_else(|| {
                anyhow!(
                    "`{here}` is `{s}`, which is not a value of `{}`",
                    e.full_name()
                )
            }),
        (Json::Number(n), Kind::Enum(e)) => n
            .as_i64()
            .and_then(|i| i32::try_from(i).ok())
            .and_then(|i| e.get_value(i))
            .map(|v| Value::EnumNumber(v.number()))
            .ok_or_else(|| {
                anyhow!(
                    "`{here}` is {n}, which is not a value of `{}`",
                    e.full_name()
                )
            }),
        // A quoted number fills a number field, as it does in protobuf's own JSON mapping:
        // an id kept in a `VARCHAR` is the common case.
        (Json::String(s), _) if numeric(&kind) => number_text(s, field, &here),
        (Json::Object(o), Kind::Message(m)) => Ok(Value::Message(build_at(m, o, path)?)),
        (Json::Number(n), _) => number(n, field, &here),
        (v, _) => bail!(
            "`{here}` is {} and `{}` is a {} field{}",
            shape(v),
            field.name(),
            kind_name(field),
            wants(field)
        ),
    }
}

/// What a field of this type wants to be handed, in the error that names it: the shape an owner
/// has to write in the document, not just the type the field has.
fn wants(field: &FieldDescriptor) -> &'static str {
    if field.cardinality() == Cardinality::Repeated {
        ": send an array"
    } else if field.is_map() || matches!(field.kind(), Kind::Message(_)) {
        ": send an object"
    } else {
        match field.kind() {
            Kind::Bytes => ": send base64 text",
            Kind::Enum(_) => ": send the name or the number of one of its values",
            Kind::Bool => ": send true or false",
            Kind::String => ": send text",
            _ => ": send a number",
        }
    }
}

/// Whether a field's type is a number, which is the one type JSON reaches by more than one
/// route: a number, or a string holding one.
fn numeric(kind: &Kind) -> bool {
    matches!(
        kind,
        Kind::Double
            | Kind::Float
            | Kind::Int32
            | Kind::Sint32
            | Kind::Sfixed32
            | Kind::Uint32
            | Kind::Fixed32
            | Kind::Int64
            | Kind::Sint64
            | Kind::Sfixed64
            | Kind::Uint64
            | Kind::Fixed64
    )
}

/// A number the column handed over as text.
fn number_text(text: &str, field: &FieldDescriptor, here: &str) -> Result<Value> {
    let bad = || {
        anyhow!(
            "`{here}` is `{text}`, which is not a number that fits a `{}` field",
            kind_name(field)
        )
    };
    Ok(match field.kind() {
        Kind::Double => text.parse::<f64>().map(Value::F64).map_err(|_| bad())?,
        Kind::Float => text.parse::<f32>().map(Value::F32).map_err(|_| bad())?,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            text.parse::<i32>().map(Value::I32).map_err(|_| bad())?
        }
        Kind::Uint32 | Kind::Fixed32 => text.parse::<u32>().map(Value::U32).map_err(|_| bad())?,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            text.parse::<i64>().map(Value::I64).map_err(|_| bad())?
        }
        Kind::Uint64 | Kind::Fixed64 => text.parse::<u64>().map(Value::U64).map_err(|_| bad())?,
        _ => unreachable!("only number fields get here"),
    })
}

/// `bytes` in JSON is base64, as everywhere in protobuf's JSON mapping.
fn base64(s: &str, here: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| anyhow!("`{here}` is not base64, which is how JSON carries bytes"))
}

/// A JSON number as its field's numeric type. A number with a fraction does not fill an
/// integer field: protobuf's own JSON mapping is as strict about that.
fn number(n: &Number, field: &FieldDescriptor, here: &str) -> Result<Value> {
    let not_here = || anyhow!("`{here}` is {n}, which is not a number");
    let not_int = || {
        anyhow!(
            "`{here}` is {n}, which is not a whole number, and `{}` is a {} field",
            field.name(),
            kind_name(field)
        )
    };
    Ok(match field.kind() {
        Kind::Double => Value::F64(n.as_f64().ok_or_else(not_here)?),
        Kind::Float => Value::F32(n.as_f64().ok_or_else(not_here)? as f32),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            Value::I32(i32::try_from(n.as_i64().ok_or_else(not_int)?).map_err(|_| not_int())?)
        }
        Kind::Uint32 | Kind::Fixed32 => {
            Value::U32(u32::try_from(n.as_u64().ok_or_else(not_int)?).map_err(|_| not_int())?)
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Value::I64(n.as_i64().ok_or_else(not_int)?),
        Kind::Uint64 | Kind::Fixed64 => Value::U64(n.as_u64().ok_or_else(not_int)?),
        _ => bail!(
            "`{here}` is a number and `{}` is a {} field",
            field.name(),
            kind_name(field)
        ),
    })
}

/// What a JSON value is, for an error message.
fn shape(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "a bool",
        Json::Number(_) => "a number",
        Json::String(_) => "a string",
        Json::Array(_) => "an array",
        Json::Object(_) => "an object",
    }
}

/// A field's type as a name an owner can match against a column type.
fn kind_name(field: &FieldDescriptor) -> String {
    if field.is_map() {
        return "map".to_string();
    }
    if field.cardinality() == Cardinality::Repeated {
        return "repeated".to_string();
    }
    match field.kind() {
        Kind::Bool => "bool",
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => "int32",
        Kind::Uint32 | Kind::Fixed32 => "uint32",
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => "int64",
        Kind::Uint64 | Kind::Fixed64 => "uint64",
        Kind::Float => "float",
        Kind::Double => "double",
        Kind::String => "string",
        Kind::Bytes => "bytes",
        Kind::Enum(_) => "enum",
        Kind::Message(_) => "message",
    }
    .to_string()
}

/// Whether a column that renders as `kind` can fill a field of this type.
fn fits(kind: ValueKind, field: &str) -> bool {
    match kind {
        // The value is only known when the row is read: nothing to check.
        ValueKind::Unknown => true,
        // A whole number fills any number field, and an enum by its number.
        ValueKind::Int => matches!(
            field,
            "int32" | "int64" | "uint32" | "uint64" | "float" | "double" | "enum"
        ),
        // A fraction is not an integer, so it goes only where a fraction fits.
        ValueKind::Double => matches!(field, "double" | "float"),
        // protobuf's JSON mapping takes a quoted number for a number field, reads a string as
        // UTF-8 for `bytes`, and takes an enum by the name of one of its values.
        ValueKind::Str => matches!(
            field,
            "string"
                | "bytes"
                | "enum"
                | "int32"
                | "int64"
                | "uint32"
                | "uint64"
                | "float"
                | "double"
        ),
        // `to_json` renders a binary column as lossy UTF-8 text, and a `bytes` field reads its
        // JSON as base64: the SQL has to say `TO_BASE64(col)`.
        ValueKind::Blob => matches!(field, "string" | "enum"),
        ValueKind::Array => field == "repeated",
        ValueKind::Object => matches!(field, "message" | "map"),
    }
}

/// Columns that cannot fill the schema: one whose document key is no field, or whose type
/// cannot fill the field it names.
///
/// Both are fixed by changing the rule or the `.proto`, so both are refused when the rule
/// loads, where the owner is told, rather than at run time where a key would fail. What the
/// check cannot see is the content of a value: an enum filled from a text column is only known
/// to be wrong when a row is read, and then it is a transient failure, so the key is retried
/// rather than dropped.
pub fn column_errors(body: &MessageDescriptor, fields: &[docs::Field]) -> Vec<String> {
    let mut out = Vec::new();
    for f in fields {
        let Some(field) = field_of(body, &f.key) else {
            out.push(format!(
                "column `{}` sends `{}` to `{}`, which is not a field of `{}` (it has: {})",
                f.column,
                f.key,
                body.name(),
                body.full_name(),
                field_names(body)
            ));
            continue;
        };
        let kind = kind_name(&field);
        if !fits(f.kind, &kind) {
            out.push(format!(
                "column `{}` is a {} and `{}` is a {kind} field: {}",
                f.column,
                column_kind(f.kind),
                f.key,
                hint(f.kind, &kind)
            ));
        }
    }
    out
}

/// Schema fields no column fills, for a warning when the rule loads.
///
/// A warning and not an error: an absent field takes its proto3 default, which is often what
/// the server wants. It is worth one line of log because the common cause is a typo in a column
/// name, and a typo here looks exactly like a document that arrived with a hole in it.
pub fn uncovered(body: &MessageDescriptor, fields: &[docs::Field]) -> Vec<String> {
    let given: HashSet<String> = fields
        .iter()
        .filter_map(|f| field_of(body, &f.key).map(|fd| fd.name().to_string()))
        .collect();
    body.fields()
        .filter(|f| !given.contains(f.name()))
        .map(|f| {
            format!(
                "`{}` gets nothing: no column of the rule fills it, so it takes its default",
                f.name()
            )
        })
        .collect()
}

/// A column type as a name an owner can match against the schema.
fn column_kind(kind: ValueKind) -> &'static str {
    match kind {
        ValueKind::Int => "whole-number column",
        ValueKind::Double => "decimal or floating-point column",
        ValueKind::Str => "text column",
        ValueKind::Blob => "binary column",
        ValueKind::Array => "collect column (`[]`)",
        ValueKind::Object => "column with a path",
        ValueKind::Unknown => "JSON column",
    }
}

/// What to do about a column that cannot fill a field, in the error that names it.
fn hint(kind: ValueKind, field: &str) -> &'static str {
    match (kind, field) {
        (ValueKind::Double, "int32" | "int64" | "uint32" | "uint64") => {
            "cast it in SQL (`CAST(col AS SIGNED)`), or make the field a double"
        }
        (ValueKind::Blob, "bytes") => {
            "a bytes field wants base64: send `TO_BASE64(col)`, or make the field a string"
        }
        (ValueKind::Blob, _) => "a binary column holds no object: send text instead",
        (ValueKind::Array, _) => "only a repeated field takes a collect column (`path[]`)",
        (ValueKind::Object, _) => {
            "a message or map field wants a JSON column, or several columns of one path"
        }
        (ValueKind::Int, "string" | "bytes") => {
            "a string field wants text: cast it in SQL (`CAST(col AS CHAR)`)"
        }
        (ValueKind::Str | ValueKind::Int | ValueKind::Double, "bool") => {
            "a bool field wants true or false"
        }
        _ => "make the column and the field agree",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::contract::PROTO_DIR;
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
}
