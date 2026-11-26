//! A fake Microsoft Service Bus endpoint, in this process: a hand-written AMQP 1.0 peer
//! that answers exactly what the client of `targets::servicebus` needs and nothing more.
//!
//! The real emulator is off the table (its EULA has never been accepted on this machine), so
//! the CBS and link-failure paths are driven against a peer written from the protocol: it
//! speaks the frames the shipped client sends (SASL ANONYMOUS, `$cbs`, a session, the entity
//! sender) and — the point of the whole file — answers them the way a real Service Bus does
//! when a token, an entity or a single message is refused.
//!
//! It is deliberately *not* a protocol implementation:
//! * no TLS, no SASL mechanism but ANONYMOUS, no echo of flow states;
//! * the couple of sessions and links the client opens, one frame per performative;
//! * it never splits a frame, so the client always sees complete frames;
//! * every wait has a deadline: a peer that hangs forever would hang the test suite, so the
//!   client's own timeout is what decides an unanswered case.
//!
//! The scripts name what a peer may answer, so a test says "this message is rejected with
//! `amqp:link:message-size-exceeded`" and not "the frame stream is bytes 0x15…".

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// The CBS node a token is put to (the client names it as the source and target of its pair).
const CBS: &str = "$cbs";
/// How long one connection of the fake peer may live. A test gives the client a far shorter
/// budget; this one only exists so a script that never answers cannot hang the suite.
const PEER_BUDGET: Duration = Duration::from_secs(20);

// -------------------------------------------------------------------------------- value model

/// The slice of the AMQP type system the client's messages and the CBS replies are made of.
/// A described value keeps its descriptor, which is how a section is told apart.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum V {
    Null,
    Bool(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Int(i32),
    Long(i64),
    Binary(Vec<u8>),
    Str(String),
    Sym(String),
    List(Vec<V>),
    Map(Vec<(V, V)>),
    /// An array: the format code of its element type, then the elements.
    Array(u8, Vec<V>),
    /// A described value: the descriptor's code and the value under it (every descriptor the
    /// protocol uses for these frames is a ulong).
    Desc(u64, Box<V>),
}

impl V {
    fn text(&self) -> Option<&str> {
        match self {
            V::Str(s) | V::Sym(s) => Some(s),
            _ => None,
        }
    }

    /// The code a section carries: `0x74` application-properties, `0x77` amqp-value, …
    fn section_code(&self) -> Option<u64> {
        match self {
            V::Desc(c, _) => Some(*c),
            _ => None,
        }
    }

    /// The value a described section carries.
    fn described(&self) -> Option<&V> {
        match self {
            V::Desc(_, inner) => Some(inner.as_ref()),
            _ => None,
        }
    }

    /// The entries of a map section (application properties).
    fn map_entries(&self) -> Option<&[(V, V)]> {
        match self.described()? {
            V::Map(m) => Some(m),
            _ => None,
        }
    }

    /// The entries of a list section (a composite: header, properties, a terminus, …).
    fn described_list(&self) -> Option<&[V]> {
        match self.described()? {
            V::List(l) => Some(l),
            _ => None,
        }
    }
}

// ------------------------------------------------------------------------------------ encoder

fn put_sized(out: &mut Vec<u8>, short: u8, long: u8, b: &[u8]) {
    if b.len() < 256 {
        out.push(short);
        out.push(b.len() as u8);
    } else {
        out.push(long);
        out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    }
    out.extend_from_slice(b);
}

fn put_ulong(out: &mut Vec<u8>, n: u64) {
    if n == 0 {
        out.push(0x44);
    } else if n < 256 {
        out.push(0x53);
        out.push(n as u8);
    } else {
        out.push(0x80);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn put_uint(out: &mut Vec<u8>, n: u32) {
    if n == 0 {
        out.push(0x43);
    } else if n < 256 {
        out.push(0x52);
        out.push(n as u8);
    } else {
        out.push(0x70);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

/// Encodes one value. A composite field the peer leaves unset is `V::Null` (0x40), which is
/// what the spec means by an absent optional field.
fn enc(out: &mut Vec<u8>, v: &V) {
    match v {
        V::Null => out.push(0x40),
        V::Bool(true) => out.push(0x41),
        V::Bool(false) => out.push(0x42),
        V::Ubyte(b) => {
            out.push(0x50);
            out.push(*b);
        }
        V::Ushort(n) => {
            out.push(0x60);
            out.extend_from_slice(&n.to_be_bytes());
        }
        V::Uint(n) => put_uint(out, *n),
        V::Ulong(n) => put_ulong(out, *n),
        V::Int(n) => {
            out.push(0x71);
            out.extend_from_slice(&n.to_be_bytes());
        }
        V::Long(n) => {
            out.push(0x81);
            out.extend_from_slice(&n.to_be_bytes());
        }
        V::Binary(b) => put_sized(out, 0xa0, 0xb0, b),
        V::Str(s) => put_sized(out, 0xa1, 0xb1, s.as_bytes()),
        V::Sym(s) => put_sized(out, 0xa3, 0xb3, s.as_bytes()),
        V::List(items) => {
            let mut body = Vec::new();
            for i in items {
                enc(&mut body, i);
            }
            if items.len() < 256 && body.len() < 254 {
                out.push(0xc0);
                out.push((body.len() + 1) as u8);
                out.push(items.len() as u8);
            } else {
                out.push(0xd0);
                out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
                out.extend_from_slice(&(items.len() as u32).to_be_bytes());
            }
            out.extend_from_slice(&body);
        }
        V::Map(entries) => {
            let mut body = Vec::new();
            for (k, val) in entries {
                enc(&mut body, k);
                enc(&mut body, val);
            }
            if entries.len() < 128 && body.len() < 254 {
                out.push(0xc1);
                out.push((body.len() + 1) as u8);
                out.push((entries.len() * 2) as u8);
            } else {
                out.push(0xd1);
                out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
                out.extend_from_slice(&((entries.len() * 2) as u32).to_be_bytes());
            }
            out.extend_from_slice(&body);
        }
        V::Array(code, items) => {
            // An array names its element type once, after the count, and the elements then
            // carry no format code of their own — only the payload the type implies.
            let mut body = Vec::new();
            for i in items {
                enc_element(&mut body, *code, i);
            }
            // array8: the code, the number of bytes that follow (count byte, element-type
            // byte, elements), the count, the element type, the payloads.
            debug_assert!(body.len() + 2 < 256, "an array of {} bytes", body.len());
            out.push(0xe0);
            out.push((body.len() + 2) as u8);
            out.push(items.len() as u8);
            out.push(*code);
            out.extend_from_slice(&body);
        }
        V::Desc(code, inner) => {
            out.push(0x00);
            put_ulong(out, *code);
            enc(out, inner);
        }
    }
}

/// The payload of one array element, which carries no format code: the array's own element
/// type (`code`) says what every element is. Only the types the fake peer actually puts in an
/// array are here — the SASL mechanisms are the one array of the handshake.
fn enc_element(out: &mut Vec<u8>, code: u8, v: &V) {
    match (code, v) {
        (0xa3, V::Sym(s)) | (0xa1, V::Str(s)) => {
            let b = s.as_bytes();
            assert!(
                b.len() < 256,
                "a {} of {} bytes",
                if code == 0xa3 { "sym8" } else { "str8" },
                b.len()
            );
            out.push(b.len() as u8);
            out.extend_from_slice(b);
        }
        (0x50, V::Ubyte(b)) => out.push(*b),
        (0x52, V::Uint(n)) => out.push(*n as u8),
        (0x53, V::Ulong(n)) => out.push(*n as u8),
        (0x70, V::Uint(n)) => out.extend_from_slice(&n.to_be_bytes()),
        (0x80, V::Ulong(n)) => out.extend_from_slice(&n.to_be_bytes()),
        _ => unimplemented!("the fake peer has no array of elements coded {code:#04x}"),
    }
}

/// A composite performative or section: `0x00 <descriptor> <fields>`, with the trailing
/// fields the peer does not use trimmed off (an absent field is simply not there).
pub(super) fn composite(code: u64, fields: &[Option<V>]) -> Vec<u8> {
    let mut n = fields.len();
    while n > 0 && fields[n - 1].is_none() {
        n -= 1;
    }
    let list = fields
        .iter()
        .take(n)
        .map(|f| f.clone().unwrap_or(V::Null))
        .collect();
    let mut out = vec![0x00];
    put_ulong(&mut out, code);
    enc(&mut out, &V::List(list));
    out
}

/// A composite whose body is not a list: a section such as `application-properties` (a map) or
/// `amqp-value` (any single value) is the descriptor followed by that value itself.
pub(super) fn described(code: u64, value: V) -> Vec<u8> {
    let mut out = vec![0x00];
    put_ulong(&mut out, code);
    enc(&mut out, &value);
    out
}

/// One AMQP frame.
pub(super) fn frame(channel: u16, body: &[u8]) -> Vec<u8> {
    header(8 + body.len(), 2, 0, channel, body)
}

/// One SASL frame: the frame type the handshake reads (the client refuses any other).
pub(super) fn sasl_frame(body: &[u8]) -> Vec<u8> {
    header(8 + body.len(), 2, 1, 0, body)
}

fn header(size: usize, doff: u8, kind: u8, channel: u16, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.push(doff);
    out.push(kind);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(body);
    out
}

// ------------------------------------------------------------------------------------ decoder

struct Cur<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cur<'a> {
    fn byte(&mut self) -> Result<u8, String> {
        let b = *self.b.get(self.i).ok_or("frame ended")?;
        self.i += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.i + n > self.b.len() {
            return Err("frame ended".into());
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }

    fn u16(&mut self) -> Result<u16, String> {
        let a = self.take(2)?;
        Ok(u16::from_be_bytes([a[0], a[1]]))
    }

    fn u32(&mut self) -> Result<u32, String> {
        let a = self.take(4)?;
        Ok(u32::from_be_bytes([a[0], a[1], a[2], a[3]]))
    }

    fn u64(&mut self) -> Result<u64, String> {
        let a = self.take(8)?;
        let mut x = [0u8; 8];
        x.copy_from_slice(a);
        Ok(u64::from_be_bytes(x))
    }

    /// One value, with the described form kept as [`V::Desc`]. The 0.14 codec reads a list8
    /// as size-then-count (the spec's order swapped), which is what this reads too.
    fn value(&mut self) -> Result<V, String> {
        let c = self.byte()?;
        match c {
            0x00 => {
                let d = self.value()?;
                let code = match d {
                    V::Ulong(c) => c,
                    other => return Err(format!("a described value with a {other:?} descriptor")),
                };
                let inner = self.value()?;
                Ok(V::Desc(code, Box::new(inner)))
            }
            0x40 => Ok(V::Null),
            0x41 => Ok(V::Bool(true)),
            0x42 => Ok(V::Bool(false)),
            0x43 => Ok(V::Uint(0)),
            0x44 => Ok(V::Ulong(0)),
            0x45 => Ok(V::List(Vec::new())), // list0: a terminus the client sends as an empty list
            0x50 | 0x51 => Ok(V::Ubyte(self.byte()?)),
            0x52 => Ok(V::Uint(u32::from(self.byte()?))),
            0x53 => Ok(V::Ulong(u64::from(self.byte()?))),
            0x54 => Ok(V::Int(i32::from(self.byte()? as i8))),
            0x55 => Ok(V::Long(i64::from(self.byte()? as i8))),
            0x56 => Ok(V::Bool(self.byte()? != 0)),
            0x60 => Ok(V::Ushort(self.u16()?)),
            0x61 => Ok(V::Int(i32::from(self.u16()? as i16))),
            0x70 => Ok(V::Uint(self.u32()?)),
            0x71 => Ok(V::Int(self.u32()? as i32)),
            0x80 => Ok(V::Ulong(self.u64()?)),
            0x81 => Ok(V::Long(self.u64()? as i64)),
            0x82 => Ok(V::Long(self.u64()? as i64)),
            0x83 => Ok(V::Long(self.u64()? as i64)),
            0xa0 | 0xb0 | 0xa1 | 0xb1 | 0xa3 | 0xb3 => {
                let (short, kind) = match c {
                    0xa0 => (true, 0),
                    0xb0 => (false, 0),
                    0xa1 => (true, 1),
                    0xb1 => (false, 1),
                    0xa3 => (true, 2),
                    _ => (false, 2),
                };
                let n = if short {
                    usize::from(self.byte()?)
                } else {
                    self.u32()? as usize
                };
                let raw = self.take(n)?.to_vec();
                Ok(match kind {
                    0 => V::Binary(raw),
                    1 => V::Str(String::from_utf8_lossy(&raw).into_owned()),
                    _ => V::Sym(String::from_utf8_lossy(&raw).into_owned()),
                })
            }
            0xc0 | 0xd0 | 0xc1 | 0xd1 => {
                let (wide, map) = match c {
                    0xc0 => (false, false),
                    0xd0 => (true, false),
                    0xc1 => (false, true),
                    _ => (true, true),
                };
                let size = if wide {
                    self.u32()? as usize
                } else {
                    usize::from(self.byte()?)
                };
                if size == 0 {
                    return Err("an empty list8/list32".into());
                }
                // `size` counts everything after its own bytes, the count included.
                let end = self.i + size;
                let count = if wide {
                    self.u32()? as usize
                } else {
                    usize::from(self.byte()?)
                };
                if map {
                    let mut out = Vec::with_capacity(count / 2);
                    for _ in 0..count / 2 {
                        let k = self.value()?;
                        let v = self.value()?;
                        out.push((k, v));
                    }
                    if self.i != end {
                        return Err("a map8/map32 that does not end where its size says".into());
                    }
                    Ok(V::Map(out))
                } else {
                    let mut out = Vec::with_capacity(count);
                    for _ in 0..count {
                        out.push(self.value()?);
                    }
                    if self.i != end {
                        return Err("a list8/list32 that does not end where its size says".into());
                    }
                    Ok(V::List(out))
                }
            }
            0xe0 | 0xf0 => {
                let wide = c == 0xf0;
                let size = if wide {
                    self.u32()? as usize
                } else {
                    usize::from(self.byte()?)
                };
                if size == 0 {
                    return Err("an empty array8".into());
                }
                // Both widths carry a one-byte count, which is how the client's own encoder
                // writes them (its `array8` is size, count, element type, elements).
                let count = if wide {
                    self.u32()? as usize
                } else {
                    usize::from(self.byte()?)
                };
                let code = self.byte()?; // the element type, as a format code
                let mut out = Vec::with_capacity(count);
                for _ in 0..count {
                    out.push(self.element(code)?);
                }
                Ok(V::Array(code, out))
            }
            _ => Err(format!("unknown format code {c:#04x}")),
        }
    }

    /// One element of an array: the payload its element type says, with no format code of its
    /// own (see [`enc_element`]).
    fn element(&mut self, code: u8) -> Result<V, String> {
        match code {
            0xa3 | 0xa1 => {
                let n = usize::from(self.byte()?);
                let s = String::from_utf8(self.take(n)?.to_vec())
                    .map_err(|_| "a non-utf8 array element".to_string())?;
                Ok(if code == 0xa3 { V::Sym(s) } else { V::Str(s) })
            }
            0x50 => Ok(V::Ubyte(self.byte()?)),
            0x52 => Ok(V::Uint(u32::from(self.byte()?))),
            0x53 => Ok(V::Ulong(u64::from(self.byte()?))),
            0x70 => Ok(V::Uint(self.u32()?)),
            0x80 => Ok(V::Ulong(self.u64()?)),
            _ => Err(format!("the fake peer does not read arrays of {code:#04x}")),
        }
    }
}

/// The performatives the client sends to us. Everything else arrives as [`In::Other`] and is
/// ignored: a fake peer has no use for an echo of flow or a `received`. The fields the peer
/// does not act on are kept because they are what a failing test reads in [`Peer::trace`].
#[derive(Debug)]
#[allow(dead_code)]
pub(super) enum In {
    Open,
    Begin {
        remote_channel: Option<u16>,
    },
    Attach {
        name: String,
        handle: u32,
        role: bool,
        source: Option<String>,
        target: Option<String>,
        snd_settle: Option<u8>,
        rcv_settle: Option<u8>,
    },
    Flow {
        handle: Option<u32>,
        link_credit: Option<u32>,
    },
    Transfer {
        handle: u32,
        delivery_id: Option<u64>,
        settled: bool,
        sections: Vec<V>,
    },
    Disposition {
        first: Option<u64>,
    },
    Detach {
        handle: u32,
        closed: bool,
        /// The condition the detach names, when it names one.
        condition: Option<String>,
    },
    End,
    Close,
    Other(u64),
}

fn nth(list: &[V], i: usize) -> Option<&V> {
    list.get(i)
}

fn as_str(v: Option<&V>) -> Option<String> {
    v.and_then(|v| match v {
        V::Str(s) | V::Sym(s) => Some(s.clone()),
        _ => None,
    })
}

fn as_u64(v: Option<&V>) -> Option<u64> {
    v.and_then(|v| match v {
        V::Ulong(n) => Some(*n),
        V::Uint(n) => Some(u64::from(*n)),
        V::Ushort(n) => Some(u64::from(*n)),
        V::Ubyte(n) => Some(u64::from(*n)),
        _ => None,
    })
}

fn as_u8(v: Option<&V>) -> Option<u8> {
    as_u64(v).and_then(|n| u8::try_from(n).ok())
}

fn as_u32(v: Option<&V>) -> Option<u32> {
    as_u64(v).and_then(|n| u32::try_from(n).ok())
}

fn as_bool(v: Option<&V>) -> Option<bool> {
    v.and_then(|v| match v {
        V::Bool(b) => Some(*b),
        _ => None,
    })
}

/// The address of a source or target terminus: a described list whose field 0 is the address.
fn terminus(v: Option<&V>) -> Option<String> {
    match v? {
        V::Desc(_, inner) => match inner.as_ref() {
            V::List(l) => as_str(nth(l, 0)),
            _ => None,
        },
        _ => None,
    }
}

/// The condition an `error` composite names, as the wire wrote it (`None`: a detach that
/// names nothing, which is a different answer from a condition we failed to read).
fn error_condition(v: Option<&V>) -> Option<String> {
    match v? {
        V::Desc(_, inner) => match inner.as_ref() {
            V::List(l) => as_str(nth(l, 0)),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn decode(body: &[u8]) -> Result<In, String> {
    let mut c = Cur { b: body, i: 0 };
    let d = c.value()?;
    let (code, list) = match &d {
        V::Desc(code, inner) => match inner.as_ref() {
            V::List(l) => (*code, l.clone()),
            other => return Err(format!("a performative described as a {other:?}")),
        },
        other => return Err(format!("not a performative: {other:?}")),
    };
    Ok(match code {
        0x10 => In::Open,
        0x11 => In::Begin {
            remote_channel: as_u32(nth(&list, 0)).and_then(|n| u16::try_from(n).ok()),
        },
        0x12 => In::Attach {
            name: as_str(nth(&list, 0)).unwrap_or_default(),
            handle: as_u32(nth(&list, 1)).unwrap_or(0),
            role: as_bool(nth(&list, 2)).unwrap_or(false),
            // The attach names the source first (field 5) and the target second (field 6), the
            // order the client's own attach writes them in.
            source: terminus(nth(&list, 5)),
            target: terminus(nth(&list, 6)),
            snd_settle: as_u8(nth(&list, 3)),
            rcv_settle: as_u8(nth(&list, 4)),
        },
        0x13 => In::Flow {
            handle: as_u32(nth(&list, 4)),
            link_credit: as_u32(nth(&list, 6)),
        },
        0x14 => {
            let mut sections = Vec::new();
            while c.i < c.b.len() {
                sections.push(c.value()?);
            }
            In::Transfer {
                handle: as_u32(nth(&list, 0)).unwrap_or(0),
                delivery_id: as_u64(nth(&list, 1)),
                settled: as_bool(nth(&list, 4)).unwrap_or(false),
                sections,
            }
        }
        0x15 => In::Disposition {
            first: as_u64(nth(&list, 1)),
        },
        0x16 => In::Detach {
            handle: as_u32(nth(&list, 0)).unwrap_or(0),
            closed: as_bool(nth(&list, 1)).unwrap_or(false),
            condition: error_condition(nth(&list, 2)),
        },
        0x17 => In::End,
        0x18 => In::Close,
        other => In::Other(other),
    })
}

// ------------------------------------------------------------------------------------- script

/// How the fake `$cbs` node answers a `put-token`.
#[derive(Clone, Debug)]
pub enum Cbs {
    /// Settles the request `accepted` and answers `201`: a token the namespace takes.
    Ok,
    /// Neither settles the request nor answers it.
    Silent,
    /// Answers a status the client has to judge (`403` refuses the token, `500` says nothing).
    Status(i64, Option<String>),
    /// Settles the request `released`: the node saw it and did not take the token.
    Released,
}

/// How the fake entity answers one transfer.
#[derive(Clone, Debug)]
pub enum Answer {
    /// `accepted` — the message is in.
    Accept,
    /// `rejected` with an error condition and optionally a description.
    Reject(String, Option<String>),
    /// `received`: the delivery is known but not settled, so this message has no outcome.
    Received,
    /// Answers a delivery number that is not this one (someone else's message).
    AcceptOther,
    /// Never answers this message.
    Silence,
}

impl Answer {
    pub fn reject(condition: &str) -> Self {
        Answer::Reject(condition.to_string(), None)
    }
}

/// The answer sequence of one link: entry `i` for the `(i % len)`-th transfer of that link.
#[derive(Clone, Default)]
pub struct Answers(Vec<Answer>);

impl Answers {
    pub fn one(a: Answer) -> Self {
        Answers(vec![a])
    }
    pub fn seq(v: Vec<Answer>) -> Self {
        Answers(v)
    }
    fn get(&self, n: usize) -> Answer {
        if self.0.is_empty() {
            return Answer::Accept;
        }
        self.0[n % self.0.len()].clone()
    }
}

/// The script of one connection.
#[derive(Clone)]
pub struct Script {
    pub cbs: Cbs,
    /// How the CBS node answers the SECOND `put-token` — the one a token that aged out brings
    /// (`None`: it answers that one as it answers the first). A node that takes a token at the
    /// open and refuses it later is what the refresh path has to survive.
    pub cbs_refresh: Option<Cbs>,
    /// The answers of the entity link (the link that is not the CBS node).
    pub answers: Answers,
    /// After this many transfers of the entity link the peer detaches it.
    pub detach_after: Option<usize>,
    /// The error condition of that detach (`None`: a detach that names nothing).
    pub detach_condition: Option<String>,
    /// Whether that detach carries `closed`.
    pub detach_closed: bool,
    /// Drop the entity link as soon as it stands: the client's next send over it is the one
    /// that finds the link gone (a link the broker closed while the worker was idle).
    pub detach_on_attach: bool,
    /// Refuse the entity sender attach with this condition instead of attaching it.
    pub refuse_attach: Option<String>,
    /// Answer nothing at all after the protocol header: the client times out on its own.
    pub silent: bool,
    /// Answer the SASL handshake with a failure (a credential the namespace will not take).
    pub refuse_sasl: bool,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            cbs: Cbs::Ok,
            cbs_refresh: None,
            answers: Answers::default(),
            detach_after: None,
            detach_condition: None,
            detach_closed: true,
            detach_on_attach: false,
            refuse_attach: None,
            silent: false,
            refuse_sasl: false,
        }
    }
}

impl Script {
    /// The peer takes the token and answers every message `accepted`.
    pub fn happy() -> Self {
        Self::default()
    }

    /// A CBS node with a script of its own.
    pub fn cbs(mut self, c: Cbs) -> Self {
        self.cbs = c;
        self
    }

    /// A CBS node that answers the refreshed token (the second `put-token`) differently from
    /// the first: the token the open took is refused, ignored or left unanswered once it ages.
    pub fn cbs_refresh(mut self, c: Cbs) -> Self {
        self.cbs_refresh = Some(c);
        self
    }

    /// What the entity link answers.
    pub fn answers(mut self, a: Answers) -> Self {
        self.answers = a;
        self
    }

    /// The peer drops the entity link after `n` transfers, optionally naming a condition.
    pub fn detach_after(mut self, n: usize, condition: Option<&str>, closed: bool) -> Self {
        self.detach_after = Some(n);
        self.detach_condition = condition.map(String::from);
        self.detach_closed = closed;
        self
    }

    /// The entity sender attach is refused with this condition.
    pub fn refuse_attach(mut self, condition: &str) -> Self {
        self.refuse_attach = Some(condition.to_string());
        self
    }

    /// The peer drops the entity link as soon as it stands, optionally naming a condition:
    /// the client's next send is the one that finds the link gone, as after a broker that
    /// closed the link while the worker was idle.
    pub fn detach_on_attach(mut self, condition: Option<&str>, closed: bool) -> Self {
        self.detach_on_attach = true;
        self.detach_condition = condition.map(String::from);
        self.detach_closed = closed;
        self
    }

    /// No answer to anything after the protocol header.
    pub fn silent() -> Self {
        Self {
            silent: true,
            ..Default::default()
        }
    }

    /// The SASL handshake fails.
    pub fn refuse_sasl() -> Self {
        Self {
            refuse_sasl: true,
            ..Default::default()
        }
    }
}

/// What the peer saw, for a test to assert on.
#[derive(Default, Debug, Clone)]
pub struct Seen {
    /// The tokens put to `$cbs`, in the order they arrived.
    pub put_tokens: Vec<String>,
    /// The `put-token` application properties, one map per request.
    pub cbs_props: Vec<HashMap<String, String>>,
    /// The entity messages, rendered as `key=value;` of their properties and body.
    pub bodies: Vec<String>,
    /// One line per frame the peer read; kept out of [`Peer::seen`] and printed on a failure.
    pub trace: Vec<String>,
}

/// A fake peer listening on the loopback interface. It is held for the whole test: the port is
/// bound once and the listener lives in the peer's task, so a connection that arrives late is
/// still accepted (a listener dropped and re-bound loses connections — the #201 lesson).
pub struct Peer {
    port: u16,
    seen: Arc<Mutex<Seen>>,
    _task: tokio::task::JoinHandle<()>,
}

impl Peer {
    /// Binds an ephemeral port on 127.0.0.1 and answers every connection with `script`.
    pub async fn start(script: Script) -> Peer {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the fake peer binds an ephemeral port on the loopback interface");
        let port = listener.local_addr().expect("a local address").port();
        let seen = Arc::new(Mutex::new(Seen::default()));
        let task = {
            let seen = seen.clone();
            tokio::spawn(async move {
                loop {
                    match timeout(PEER_BUDGET, listener.accept()).await {
                        Ok(Ok((sock, _))) => {
                            let seen = seen.clone();
                            let script = script.clone();
                            let logged = seen.clone();
                            tokio::spawn(async move {
                                // A peer that could not make sense of the conversation says so
                                // in the trace: the client only sees the socket close.
                                if let Ok(Err(e)) =
                                    timeout(PEER_BUDGET, serve(sock, script, seen)).await
                                {
                                    note(&logged, |s| {
                                        s.trace.push(format!("the peer gave up: {e}"))
                                    });
                                }
                            });
                        }
                        // A budget that ran out or a failed accept: the peer is done.
                        _ => return,
                    }
                }
            })
        };
        Peer {
            port,
            seen,
            _task: task,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// What the peer saw, without the frame trace.
    pub fn seen(&self) -> Seen {
        let mut s = self.seen.lock().expect("the peer's log").clone();
        s.trace.clear();
        s
    }

    /// The whole log of frames, for a failing assertion to read.
    pub fn trace(&self) -> String {
        self.seen.lock().expect("the peer's log").trace.join("\n")
    }
}

fn note(seen: &Arc<Mutex<Seen>>, f: impl FnOnce(&mut Seen)) {
    let mut g = seen.lock().expect("the peer's log");
    f(&mut g);
}

// ------------------------------------------------------------------------------------- server

/// Reads one frame of the socket: its channel and its body, after the 8-byte header.
pub(super) async fn read_frame(sock: &mut TcpStream) -> Result<Option<(u16, Vec<u8>)>, String> {
    let mut head = [0u8; 8];
    match sock.read_exact(&mut head).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(format!("the frame header: {e}")),
    }
    let size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let channel = u16::from_be_bytes([head[6], head[7]]);
    if size < 8 {
        return Err(format!("a frame of {size} bytes"));
    }
    let mut body = vec![0u8; size - 8];
    sock.read_exact(&mut body)
        .await
        .map_err(|e| format!("the frame body: {e}"))?;
    Ok(Some((channel, body)))
}

async fn send_frame(
    sock: &mut TcpStream,
    seen: &Arc<Mutex<Seen>>,
    channel: u16,
    body: &[u8],
) -> Result<(), String> {
    let bytes = frame(channel, body);
    let shown = decode(body)
        .map(|i| format!("{i:?}"))
        .unwrap_or_else(|e| format!("<{e} {}>", hex(body)));
    note(seen, |s| {
        s.trace.push(format!("OUT ch{channel} {shown}"));
    });
    sock.write_all(&bytes).await.map_err(|e| e.to_string())
}

/// Answers one connection: the SASL handshake, the open, then the sessions, links and traffic.
async fn serve(mut sock: TcpStream, script: Script, seen: Arc<Mutex<Seen>>) -> Result<(), String> {
    // The SASL header, or a plain AMQP 1.0 header from a client with no SASL layer.
    let mut head = [0u8; 8];
    if sock.read_exact(&mut head).await.is_err() {
        return Ok(());
    }
    note(&seen, |s| s.trace.push(format!("header {}", hex(&head))));
    if &head[0..4] != b"AMQP" {
        return Err("no protocol header".into());
    }
    if head[4] == 3 {
        // The SASL layer's header: "AMQP" and protocol id 3.
        send_header(&mut sock, b"AMQP\x03\x01\x00\x00").await?;
        // The mechanisms we offer, then the client's response, then our outcome.
        let mech = composite(
            0x40,
            &[
                Some(V::Array(0xa3, vec![V::Sym("ANONYMOUS".into())])),
                None,
                None,
                None,
            ],
        );
        sock.write_all(&sasl_frame(&mech))
            .await
            .map_err(|e| e.to_string())?;
        // The client's SASL init (its mechanism choice and its response): ANONYMOUS is the
        // only one we offered, and the token authenticates the links, not this handshake.
        let (_ch, _body) = match timeout(Duration::from_secs(2), read_frame(&mut sock)).await {
            Err(_) => return Err("the SASL init timed out".into()),
            Ok(r) => r?.ok_or("the SASL init never arrived")?,
        };
        note(&seen, |s| s.trace.push("sasl-init".into()));
        let code = if script.refuse_sasl { 3 } else { 0 }; // 0 ok, 3 sys-perm-error
        let outcome = composite(0x44, &[Some(V::Ubyte(code)), None]);
        sock.write_all(&sasl_frame(&outcome))
            .await
            .map_err(|e| e.to_string())?;
        if script.refuse_sasl {
            return Ok(());
        }
        // The client sends the AMQP header once the handshake stands.
        let mut h = [0u8; 8];
        sock.read_exact(&mut h)
            .await
            .map_err(|e| format!("the AMQP header after SASL: {e}"))?;
        if &h[0..4] != b"AMQP" {
            return Err("no AMQP header after the SASL outcome".into());
        }
        // The header exchange of the AMQP layer itself: the client waits for ours before it
        // sends its open.
        send_header(&mut sock, b"AMQP\x00\x01\x00\x00").await?;
    } else {
        send_header(&mut sock, b"AMQP\x00\x01\x00\x00").await?;
    }
    if script.silent {
        // Nothing at all: the client's own timeouts have to be what ends the test.
        let _ = timeout(PEER_BUDGET, std::future::pending::<()>()).await;
        return Ok(());
    }

    let mut links: Vec<Link> = Vec::new();
    // The `put-token`s of THIS connection: the node answers the first one by `script.cbs` and
    // every later one — the token a refresh brings — by `script.cbs_refresh`, when it has one.
    let put_tokens = AtomicUsize::new(0);
    loop {
        let Some((channel, body)) = read_frame(&mut sock).await? else {
            return Ok(());
        };
        if body.is_empty() {
            continue; // an empty frame is a heartbeat
        }
        let came = decode(&body).map_err(|e| format!("{e} in [{}]", hex(&body)))?;
        note(&seen, |s| s.trace.push(format!("ch{channel} {came:?}")));
        match came {
            In::Open => {
                let open = composite(
                    0x10,
                    &[
                        Some(V::Str("lwf-fake-peer".into())),
                        Some(V::Str("127.0.0.1".into())),
                        Some(V::Uint(65536)),   // max-frame-size
                        Some(V::Ushort(65535)), // channel-max
                        Some(V::Uint(0)),       // idle-time-out
                        None,
                        None,
                        None,
                    ],
                );
                send_frame(&mut sock, &seen, channel, &open).await?;
            }
            In::Begin { remote_channel } => {
                // The responder names the initiator's channel as its remote channel; the one
                // the client names for us (none, as the initiator) is not what we answer with.
                let _ = remote_channel;
                let begin = composite(
                    0x11,
                    &[
                        Some(V::Ushort(channel)),
                        Some(V::Uint(0)), // next-outgoing-id (sequence-no: a uint here)
                        Some(V::Uint(100)), // incoming-window
                        Some(V::Uint(100)), // outgoing-window
                        Some(V::Uint(4294967295)), // handle-max
                        None,
                        None,
                        None,
                    ],
                );
                send_frame(&mut sock, &seen, channel, &begin).await?;
            }
            In::Attach {
                name,
                handle,
                role,
                source,
                target,
                snd_settle,
                rcv_settle,
            } => {
                // Both ends of a link carry the same handle: the one the client named is the
                // one our transfers and detaches of this link are read under, so the peer keeps
                // it as its own.
                let mine = handle;
                let is_cbs = source.as_deref() == Some(CBS) || target.as_deref() == Some(CBS);
                // A broker that will not create the entity answers the attach with no terminus
                // at all and then drops the link naming why — the AMQP way to refuse a node.
                // It is also the only way the shipped client's attach failure can carry a
                // condition: `SenderAttachError::RemoteClosedWithError` is built from the
                // detach the peer answers the failed exchange with.
                if let Some(c) = script.refuse_attach.as_deref().filter(|_| !is_cbs) {
                    let fields = vec![
                        Some(V::Str(name.clone())),
                        Some(V::Uint(mine)),
                        Some(V::Bool(!role)),
                        Some(V::Ubyte(snd_settle.unwrap_or(2))),
                        Some(V::Ubyte(rcv_settle.unwrap_or(0))),
                        Some(source_node(source.as_deref().unwrap_or(""))),
                        None, // no target: the node is not created
                        None,
                        None,
                        None,
                    ];
                    send_frame(&mut sock, &seen, channel, &composite(0x12, &fields)).await?;
                    send_frame(
                        &mut sock,
                        &seen,
                        channel,
                        &detach_frame(mine, Some(c), true),
                    )
                    .await?;
                    note(&seen, |s| {
                        s.trace.push(format!("refused the attach of {name}: {c}"));
                    });
                    // The link is not kept: a transfer over a refused link is a client bug,
                    // and the unknown-handle error is the way to see it.
                    continue;
                }
                let fields = vec![
                    Some(V::Str(name.clone())),
                    Some(V::Uint(mine)),
                    Some(V::Bool(!role)), // our role is the client's opposite
                    Some(V::Ubyte(snd_settle.unwrap_or(2))),
                    Some(V::Ubyte(rcv_settle.unwrap_or(0))),
                    // The attach names the source before the target, whatever side attached.
                    Some(source_node(source.as_deref().unwrap_or(""))),
                    Some(target_node(target.as_deref().unwrap_or(""))),
                    None,
                    None,
                    Some(V::Uint(0)), // initial-delivery-count
                ];
                send_frame(&mut sock, &seen, channel, &composite(0x12, &fields)).await?;
                // The link may be dropped as soon as it stands: the client's next send over it
                // is then the one that finds the link gone.
                let drop_now = (!is_cbs && !role && script.detach_on_attach)
                    .then(|| (script.detach_condition.clone(), script.detach_closed));
                links.push(Link {
                    name,
                    channel,
                    client_handle: handle,
                    peer_handle: mine,
                    is_cbs,
                    sent: AtomicI64::new(0),
                    detached: AtomicI64::new(i64::from(drop_now.is_some())),
                });
                if let Some((condition, closed)) = drop_now {
                    send_frame(
                        &mut sock,
                        &seen,
                        channel,
                        &detach_frame(mine, condition.as_deref(), closed),
                    )
                    .await?;
                    continue;
                }
                if !role {
                    // The client is the sender of this link and starts with no credit: without a
                    // flow of ours its transfer would never be allowed. The 0.14 codec's `Flow`
                    // has no next-hop field, so its fields sit one position lower than the spec's
                    // (and `incoming-window` is mandatory — leaving it out is a decode error).
                    let flow = composite(
                        0x13,
                        &[
                            None,                // next-incoming-id
                            Some(V::Uint(100)),  // incoming-window
                            Some(V::Uint(0)),    // next-outgoing-id
                            Some(V::Uint(100)),  // outgoing-window
                            Some(V::Uint(mine)), // handle
                            Some(V::Uint(0)),    // delivery-count
                            Some(V::Uint(200)),  // link-credit
                            None,                // available
                            None,                // drain
                            None,                // echo
                            None,                // properties
                        ],
                    );
                    send_frame(&mut sock, &seen, channel, &flow).await?;
                }
            }
            In::Flow { .. } => {}
            In::Transfer {
                handle,
                delivery_id,
                settled,
                sections,
            } => {
                let Some(link) = links
                    .iter()
                    .find(|l| l.channel == channel && l.client_handle == handle)
                else {
                    let known = links
                        .iter()
                        .map(|l| format!("{}={}", l.name, l.client_handle))
                        .collect::<Vec<_>>()
                        .join(" ");
                    return Err(format!(
                        "a transfer on unknown handle {handle} of channel {channel} (links: {known})"
                    ));
                };
                let n = link.sent.fetch_add(1, Ordering::SeqCst);
                let id = match delivery_id {
                    Some(d) => d,
                    // A presettled transfer has no disposition to answer.
                    None if settled => continue,
                    None => return Err("a transfer without a delivery id".into()),
                };
                if link.is_cbs {
                    note(&seen, |s| {
                        let (token, props) = cbs_request(&sections);
                        s.put_tokens.push(token);
                        s.cbs_props.push(props);
                    });
                    let nth = put_tokens.fetch_add(1, Ordering::SeqCst);
                    let cbs = if nth == 0 {
                        &script.cbs
                    } else {
                        script.cbs_refresh.as_ref().unwrap_or(&script.cbs)
                    };
                    let (settle, reply) = match cbs {
                        Cbs::Ok => (Some(accepted()), Some((201, Some("ok".to_string())))),
                        Cbs::Status(code, desc) => (Some(accepted()), Some((*code, desc.clone()))),
                        Cbs::Silent => (None, None),
                        Cbs::Released => (Some(released()), None),
                    };
                    if let Some(state) = settle {
                        send_frame(&mut sock, &seen, channel, &disposition(id, state)).await?;
                    }
                    if let Some((code, desc)) = reply {
                        // The answer goes to the client's receiver link on the CBS node.
                        let Some(rx) = links.iter().find(|l| {
                            l.channel == channel && l.is_cbs && l.peer_handle != link.peer_handle
                        }) else {
                            return Err("the CBS reply link is not attached".into());
                        };
                        send_frame(
                            &mut sock,
                            &seen,
                            channel,
                            // The client reads our transfer under the handle it gave the link.
                            &cbs_reply(rx.client_handle, id, code, desc.as_deref()),
                        )
                        .await?;
                    }
                    continue;
                }
                note(&seen, |s| s.bodies.push(body_text(&sections)));
                if let Some(after) = script.detach_after {
                    if n >= after as i64 && link.detached.swap(1, Ordering::SeqCst) == 0 {
                        // The link goes away instead of answering this message (and the ones
                        // after it, which the client then has no answer for either).
                        let d = detach_frame(
                            link.peer_handle,
                            script.detach_condition.as_deref(),
                            script.detach_closed,
                        );
                        send_frame(&mut sock, &seen, channel, &d).await?;
                        continue;
                    }
                }
                let out = match script.answers.get(n as usize) {
                    Answer::Accept => Some(disposition(id, accepted())),
                    Answer::Reject(c, d) => Some(disposition(id, rejected(&c, d.as_deref()))),
                    Answer::Received => Some(disposition(id, received())),
                    Answer::AcceptOther => Some(disposition(id + 100, accepted())),
                    Answer::Silence => None,
                };
                if let Some(f) = out {
                    send_frame(&mut sock, &seen, channel, &f).await?;
                }
            }
            In::Disposition { .. } => {}
            In::Detach { handle, closed, .. } => {
                // The client ended one of our links: answer its detach, and when it closed the
                // link, answer its end and finish the conversation.
                send_frame(
                    &mut sock,
                    &seen,
                    channel,
                    &detach_frame(handle, None, false),
                )
                .await?;
                if closed {
                    send_frame(&mut sock, &seen, channel, &composite(0x17, &[None, None])).await?;
                    return Ok(());
                }
            }
            In::End => {
                send_frame(&mut sock, &seen, channel, &composite(0x17, &[None, None])).await?;
            }
            In::Close => return Ok(()),
            In::Other(c) => note(&seen, |s| s.trace.push(format!("ignored {c:#04x}"))),
        }
    }
}

async fn send_header(sock: &mut TcpStream, header: &[u8; 8]) -> Result<(), String> {
    sock.write_all(header).await.map_err(|e| e.to_string())
}

/// One link of the peer: the client's handle (what its transfers name) and the peer's own.
struct Link {
    name: String,
    channel: u16,
    client_handle: u32,
    peer_handle: u32,
    is_cbs: bool,
    sent: AtomicI64,
    detached: AtomicI64,
}

// ----------------------------------------------------------------------------------- builders

fn error(condition: &str, description: Option<&str>) -> V {
    V::Desc(
        0x1d,
        Box::new(V::List(vec![
            V::Sym(condition.to_string()),
            description
                .map(|d| V::Str(d.to_string()))
                .unwrap_or(V::Null),
            V::Null,
        ])),
    )
}

fn target_node(address: &str) -> V {
    terminus_node(0x29, address)
}

fn source_node(address: &str) -> V {
    terminus_node(0x28, address)
}

fn terminus_node(code: u64, address: &str) -> V {
    V::Desc(
        code,
        Box::new(V::List(vec![
            V::Str(address.to_string()),
            V::Null,
            V::Null,
            V::Null,
            V::Bool(false),
            V::Null,
        ])),
    )
}

fn accepted() -> V {
    V::Desc(0x24, Box::new(V::List(vec![])))
}

fn released() -> V {
    V::Desc(0x26, Box::new(V::List(vec![])))
}

fn received() -> V {
    // `received` carries the mandatory section-number and section-offset.
    V::Desc(0x23, Box::new(V::List(vec![V::Uint(0), V::Ulong(0)])))
}

fn rejected(condition: &str, description: Option<&str>) -> V {
    V::Desc(0x25, Box::new(V::List(vec![error(condition, description)])))
}

/// A disposition settling one delivery of the client's sender (role: receiver, settled).
/// The delivery numbers are `sequence-no`, which the shipped client reads as a uint (the spec
/// says ulong — the client's own frames write uint, so the peer answers in kind).
fn disposition(id: u64, state: V) -> Vec<u8> {
    composite(
        0x15,
        &[
            Some(V::Bool(true)),
            Some(V::Uint(id as u32)),
            Some(V::Uint(id as u32)),
            Some(V::Bool(true)),
            Some(state),
            None,
        ],
    )
}

fn detach_frame(handle: u32, condition: Option<&str>, closed: bool) -> Vec<u8> {
    composite(
        0x16,
        &[
            Some(V::Uint(handle)),
            Some(V::Bool(closed)),
            condition.map(|c| error(c, None)),
        ],
    )
}

/// A settled transfer on the client's CBS receiver link: the `status-code` /
/// `status-description` application properties and the amqp-value body the client decodes.
fn cbs_reply(handle: u32, delivery_id: u64, status: i64, description: Option<&str>) -> Vec<u8> {
    let mut props = vec![(V::Str("status-code".into()), V::Int(status as i32))];
    if let Some(d) = description {
        props.push((V::Str("status-description".into()), V::Str(d.to_string())));
    }
    let head = composite(
        0x14,
        &[
            Some(V::Uint(handle)),
            Some(V::Uint(delivery_id as u32)), // sequence-no: the client reads it as a uint
            // The client's receiver keys the delivery by its tag: a reply without one is an
            // error, not a settled delivery.
            Some(V::Binary(delivery_id.to_be_bytes().to_vec())),
            Some(V::Uint(0)),    // message-format
            Some(V::Bool(true)), // settled
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    );
    // `application-properties` is a described map and `amqp-value` a described single value:
    // neither is the described list the performatives are.
    let mut body = head;
    body.extend_from_slice(&described(0x74, V::Map(props)));
    body.extend_from_slice(&described(
        0x77,
        V::Str(description.unwrap_or("ok").to_string()),
    ));
    body
}

fn hex(b: &[u8]) -> String {
    let mut s = String::new();
    for x in b.iter().take(96) {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

/// The token and the request properties of a `put-token` message.
fn cbs_request(sections: &[V]) -> (String, HashMap<String, String>) {
    let mut token = String::new();
    let mut props = HashMap::new();
    for s in sections {
        match s.section_code() {
            Some(0x74) => {
                if let Some(m) = s.map_entries() {
                    for (k, v) in m {
                        if let (Some(k), Some(v)) = (k.text(), v.text()) {
                            props.insert(k.to_string(), v.to_string());
                        }
                    }
                }
            }
            Some(0x77) => {
                if let Some(V::Str(t)) = s.described() {
                    token = t.clone();
                }
            }
            _ => {}
        }
    }
    (token, props)
}

/// A short, assertable rendering of an entity message: its application properties and its
/// body, so a test can tell which message a verdict belongs to.
fn body_text(sections: &[V]) -> String {
    let mut out = String::new();
    for s in sections {
        match s.section_code() {
            Some(0x73) => {
                // The properties the client is judged by: its dedup id (field 0) and its group
                // id (field 10, after the two nulls of the trimmed tail's neighbours).
                if let Some(l) = s.described_list() {
                    out.push_str(&format!(
                        "mid={};sid={};",
                        value_text(nth(l, 0).unwrap_or(&V::Null)),
                        value_text(nth(l, 10).unwrap_or(&V::Null))
                    ));
                }
            }
            Some(0x74) => {
                if let Some(m) = s.map_entries() {
                    for (k, v) in m {
                        out.push_str(&format!("{}={};", k.text().unwrap_or("?"), value_text(v)));
                    }
                }
            }
            Some(0x77) => {
                if let Some(inner) = s.described() {
                    out.push_str(&format!("value={};", value_list(inner)));
                }
            }
            Some(0x75) => {
                if let Some(V::Binary(b)) = s.described() {
                    out.push_str(&format!("data={};", String::from_utf8_lossy(b)));
                }
            }
            _ => {}
        }
    }
    out
}

fn value_text(v: &V) -> String {
    match v {
        V::Desc(_, inner) => value_text(inner),
        V::Str(s) | V::Sym(s) => s.clone(),
        V::Ulong(n) => n.to_string(),
        V::Uint(n) => n.to_string(),
        V::Long(n) => n.to_string(),
        V::Int(n) => n.to_string(),
        V::Bool(b) => b.to_string(),
        V::Null => "null".into(),
        other => format!("{other:?}"),
    }
}

fn value_list(v: &V) -> String {
    match v {
        V::Map(m) => m
            .iter()
            .map(|(k, v)| format!("{}={}", value_text(k), value_text(v)))
            .collect::<Vec<_>>()
            .join(","),
        V::List(l) => l.iter().map(value_text).collect::<Vec<_>>().join(","),
        other => value_text(other),
    }
}
