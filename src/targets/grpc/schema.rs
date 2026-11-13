//! The schema of a `grpc` target: the descriptor set it names, the method it asks for, and
//! the mapping from a rendered document to a protobuf message.
//!
//! A descriptor set is a schema lazywf reads, not a contract it must be given: the pool every
//! target's method is looked up in always holds lazywf's own [`crate::targets::grpc::contract`] first,
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
use crate::targets::grpc::contract;

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
#[path = "../../tests/targets_grpc_schema.rs"]
mod tests;
