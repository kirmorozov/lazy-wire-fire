use super::*;
use tonic::metadata::MetadataMap;

fn codec_ask(ask: Option<Codec>) -> Arc<Shared> {
    Arc::new(Shared::new(Endpoint0 {
        url: "grpc://ingest.example.test:9000/catalog.v1.Ingest/Push".into(),
        path: "/catalog.v1.Ingest/Push".into(),
        secure: false,
        host: "ingest.example.test".into(),
        port: 9000,
        connector: tls::Connector::new(
            "ingest.example.test",
            9000,
            &crate::tls::TlsSpec::parse(None).unwrap(),
            false,
            &[b"h2"],
        )
        .unwrap(),
        token: None,
        compression: ask,
        min_bytes: 1024,
        max_message_bytes: 4 << 20,
        timeout: Duration::from_secs(2),
    }))
}

/// `auto` walks the ladder and stops at the bottom; a target that was told a codec hears
/// about a server without it rather than being sent as identity quietly.
#[test]
fn the_ladder_goes_down_to_nothing() {
    let s = codec_ask(None);
    assert_eq!(s.codec(), Codec::Zstd);
    assert!(s.step_down());
    assert_eq!(s.codec(), Codec::Gzip);
    assert!(s.step_down());
    assert_eq!(s.codec(), Codec::Identity);
    assert!(!s.step_down());
    assert_eq!(s.codec(), Codec::Identity);

    let s = codec_ask(Some(Codec::Zstd));
    assert_eq!(s.codec(), Codec::Zstd);
    assert!(!s.step_down());
    assert_eq!(s.codec(), Codec::Zstd);
}

fn outbound(keys: &[&str]) -> Vec<Outbound> {
    keys.iter()
        .map(|k| Outbound {
            key: k.to_string(),
            body: format!(r#"{{"sku":"{k}"}}"#).into_bytes(),
        })
        .collect()
}

/// The ring answers by `seq`, out of order, exactly once; and every acknowledgement
/// lazywf cannot believe — a number it never sent, a slot whose item has moved on, a
/// hash that is not the one sent — is ignored, never a verdict, while a status it has
/// never heard of can at most be the key's wait.
#[tokio::test]
async fn the_ring_believes_only_answers_it_can_stand_behind() {
    let shared = codec_ask(Some(Codec::Identity));
    let tail = Arc::new(Tail::new(Arc::clone(&shared)));
    let items = outbound(&["a", "b", "c"]);
    let r = tail.reserve(&items).expect("room");
    assert_eq!(r.envelope.items.len(), 3);
    assert_eq!(
        r.envelope.items.iter().map(|i| i.seq).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the numbers run on one stream from 1"
    );
    let hashes: Vec<u64> = r.envelope.items.iter().map(|i| i.hash).collect();

    // Out of order: item 3 first, then 1, then 2.
    tail.answer(&Ack {
        seq: 3,
        hash: hashes[2],
        status: AckStatus::Ok as i32,
        reason: String::new(),
    });
    assert!(!r.batch.answered());
    tail.answer(&Ack {
        seq: 1,
        hash: hashes[0],
        status: AckStatus::Refused as i32,
        reason: "no such sku".into(),
    });
    // A second answer for 1 changes nothing (a server may repeat itself).
    tail.answer(&Ack {
        seq: 1,
        hash: hashes[0],
        status: AckStatus::Ok as i32,
        reason: String::new(),
    });
    // A number never sent: ignored.
    tail.answer(&Ack {
        seq: 77,
        hash: 9,
        status: AckStatus::Ok as i32,
        reason: String::new(),
    });
    // The right number, the wrong content — the stale acknowledgement of a reconnect:
    // believed, and lazywf would be confirming a document it never sent.
    tail.answer(&Ack {
        seq: 2,
        hash: hashes[0],
        status: AckStatus::Ok as i32,
        reason: String::new(),
    });
    assert_eq!(r.batch.unanswered(), 1, "only item 2 has no verdict");
    // A status from another universe cannot refuse a key, so it cannot be lost to
    // disbelief either: it is what it reads as, RETRY, the key's own wait.
    tail.answer(&Ack {
        seq: 2,
        hash: hashes[1],
        status: 44,
        reason: "?".into(),
    });
    assert!(r.batch.answered(), "and that wait answers the flush");

    let v = r.batch.take();
    assert_eq!(
        v,
        vec![
            Verdict::Ack {
                seq: 1,
                hash: hashes[0],
                status: AckStatus::Refused,
                reason: "no such sku".into()
            },
            Verdict::Ack {
                seq: 2,
                hash: hashes[1],
                status: AckStatus::Retry,
                reason: "?".into()
            },
            Verdict::Ack {
                seq: 3,
                hash: hashes[2],
                status: AckStatus::Ok,
                reason: String::new()
            },
        ],
        "the answers land where the items were sent"
    );

    // After the flush is forgotten, its numbers are the duplicates the ring ignores.
    tail.forget(&r.batch);
    tail.answer(&Ack {
        seq: 3,
        hash: hashes[2],
        status: AckStatus::Ok as i32,
        reason: String::new(),
    });
    assert_eq!(tail.pending(), 0, "the window came back");
}

/// The window bounds items and bytes, and frees when the flush is done with its answers.
#[tokio::test]
async fn the_window_fills_and_frees() {
    let shared = codec_ask(Some(Codec::Identity));
    let tail = Arc::new(Tail::new(Arc::clone(&shared)));
    // One flush of the whole window fills it; the next waits.
    let many: Vec<String> = (0..WINDOW_ITEMS).map(|i| format!("k{i}")).collect();
    let items: Vec<Outbound> = many.iter().map(|k| outbound(&[k])[0].clone()).collect();
    let big = tail
        .reserve(&items)
        .expect("the window is exactly this deep");
    assert_eq!(tail.pending(), WINDOW_ITEMS);
    assert!(
        tail.reserve(&outbound(&["one more"])).is_none(),
        "a window full is a flush that waits"
    );
    // An item answered is not yet free: the flush itself lets go when it has them all.
    let hash = big.envelope.items[0].hash;
    tail.answer(&Ack {
        seq: 1,
        hash,
        status: AckStatus::Ok as i32,
        reason: String::new(),
    });
    assert!(tail.reserve(&outbound(&["one more"])).is_none());
    tail.forget(&big.batch);
    assert_eq!(tail.pending(), 0);
    assert!(tail.reserve(&outbound(&["one more"])).is_some());

    // A flush bigger than the window still goes — a flush must not deadlock on itself —
    // but only while nothing else is in flight.
    let tail = Arc::new(Tail::new(Arc::clone(&shared)));
    let huge: Vec<Outbound> = (0..(WINDOW_ITEMS + 64))
        .map(|i| Outbound {
            key: format!("h{i}"),
            body: vec![b'x'; 8],
        })
        .collect();
    assert!(tail.reserve(&huge).is_some(), "the big flush goes");
    assert!(tail.reserve(&outbound(&["late"])).is_none());
}

/// When the stream ends, everyone waiting is told and every unanswered item is lost — a
/// loss is never a verdict, whoever ended the stream.
#[tokio::test]
async fn a_closed_stream_leaves_every_item_waiting() {
    let shared = codec_ask(Some(Codec::Identity));
    let tail = Arc::new(Tail::new(Arc::clone(&shared)));
    let r = tail.reserve(&outbound(&["a", "b"])).unwrap();
    let waiter = {
        let tail = Arc::clone(&tail);
        let batch = Arc::clone(&r.batch);
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while !batch.answered() {
                if !wait(&tail, deadline).await {
                    break;
                }
            }
            batch.take()
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    tail.close(wire::failure(tonic::Code::Unavailable, "gone"));
    let v = waiter.await.unwrap();
    assert!(
        v.iter()
            .all(|v| matches!(v, Verdict::Lost(w) if w.contains("UNAVAILABLE"))),
        "{v:?}"
    );
    // The next look says the stream is over, with the same reason.
    let f = tail.over_now().unwrap();
    assert!(!f.refuse && f.why.contains("UNAVAILABLE"));
}

/// The packer is the whole compression decision in one place: messages under the threshold
/// travel byte for byte as they were, messages over it travel compressed and flagged, and
/// frames split across chunks still come out whole.
#[tokio::test]
async fn the_packer_compresses_above_the_threshold_only() {
    let body_small = br#"{"sku":"a"}"#.to_vec();
    let body_big: Vec<u8> = br#"{"sku":"a","note":"the same note, repeated to be compressible "}"#
        .iter()
        .cycle()
        .take(4096)
        .copied()
        .collect();
    let min = 1024;
    let mut raw = BytesMut::new();
    for body in [&body_small, &body_big, &body_small] {
        raw.put_u8(0);
        raw.put_u32(body.len() as u32);
        raw.extend_from_slice(body);
    }
    // Feed it in chunks that split the frames apart, to show the carry works.
    let chunks: Vec<Bytes> = raw.chunks(17).map(Bytes::copy_from_slice).collect();
    let body = GrpcBody::new(FakeBody { chunks, at: 0 });
    let packer = Packer {
        inner: Box::pin(body),
        codec: Codec::Gzip,
        min,
        carry: BytesMut::new(),
        out: VecDeque::new(),
        done: false,
    };
    let out = http_body_util::BodyExt::collect(packer)
        .await
        .unwrap()
        .to_bytes();

    // Walk the frames out and check each against what went in.
    let mut at = &out[..];
    let mut smalls = 0;
    let mut bigs = 0;
    while !at.is_empty() {
        let flag = at[0];
        let len = u32::from_be_bytes(at[1..5].try_into().unwrap()) as usize;
        let (payload, rest) = at[5..].split_at(len);
        if len == body_small.len() {
            assert_eq!(flag, 0, "the small message travels as it is");
            assert_eq!(payload, &body_small[..]);
            smalls += 1;
        } else {
            assert_eq!(flag, 1, "the big message is compressed");
            let back = zstd::decode_all(payload).ok().or_else(|| {
                use std::io::Read as _;
                let mut d = flate2::read::GzDecoder::new(payload);
                let mut o = Vec::new();
                d.read_to_end(&mut o).ok().map(|_| o)
            });
            assert_eq!(back.unwrap(), body_big, "and it reads back");
            bigs += 1;
        }
        at = rest;
    }
    assert_eq!((smalls, bigs), (2, 1));
}

struct FakeBody {
    chunks: Vec<Bytes>,
    at: usize,
}

impl HttpBody for FakeBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.at >= this.chunks.len() {
            Poll::Ready(None)
        } else {
            let c = this.chunks[this.at].clone();
            this.at += 1;
            Poll::Ready(Some(Ok(Frame::data(c))))
        }
    }
}

/// A metadata value the server must not be able to make lazywf print: the token goes as a
/// sensitive header, so a debug of the map never holds it.
#[test]
fn the_authorization_header_is_sensitive() {
    let mut map = MetadataMap::new();
    let mut v: MetadataValue<_> = "Bearer top-secret".try_into().unwrap();
    v.set_sensitive(true);
    map.insert("authorization", v);
    let shown = format!("{map:?}");
    assert!(!shown.contains("top-secret"), "{shown}");
}
