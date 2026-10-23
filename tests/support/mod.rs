#![allow(dead_code)]
use flare_cache_manager::{
    Key, disk,
    protocol::{self, Decoder, Encoder, Frame, tag},
    record::{self, Descriptor, Header, RankSizes},
    server::{self, Config},
};
use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime},
};

static SERIAL: AtomicU64 = AtomicU64::new(0);
pub fn key(tokens: u64) -> Key {
    Key {
        consumed_tokens: tokens,
        digest: (tokens as u128 * 7919).to_le_bytes(),
    }
}
pub fn descriptor() -> Descriptor {
    Descriptor {
        identity: [7; 32],
        fingerprint: [3; 16],
        page_tokens: 64,
        ranks: vec![RankSizes {
            planes: vec![4000],
            row_bytes: 1000,
        }],
    }
}
pub fn base(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "cm-{}-{}-{label}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ))
}
pub fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(5));
    }
}
pub struct Host {
    pub base: PathBuf,
    pub config: Config,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Host {
    pub fn new(memory: usize, nvme: u64) -> Self {
        let base = base("host");
        let mut config = Config::new(base.join("records"), base.join("run/m.sock"), memory, nvme);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        config.metrics = Some(listener.local_addr().unwrap().to_string());
        drop(listener);
        Self {
            base,
            config,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }
    pub fn start(&mut self) {
        self.stop.store(false, Ordering::Release);
        let config = self.config.clone();
        let stop = self.stop.clone();
        self.thread = Some(thread::spawn(move || server::run(config, stop).unwrap()));
        wait(|| self.config.socket.exists());
    }
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
        // Worker channels drain after the state owner exits.
        wait(|| disk::RootLock::acquire(&self.config.root).is_ok());
    }
    pub fn client(&self) -> Client {
        Client::connect(&self.config, descriptor())
    }
    pub fn metrics(&self) -> String {
        let mut stream = TcpStream::connect(self.config.metrics.as_ref().unwrap()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut text = String::new();
        stream.read_to_string(&mut text).unwrap();
        text
    }
    pub fn gauge(&self, name: &str) -> u64 {
        self.metrics()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("cm_{name} ")))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }
    pub fn durable(&self, count: usize) {
        wait(|| {
            self.metrics()
                .lines()
                .filter(|line| line.starts_with("cm_records{") && line.contains("tier=\"disk\""))
                .filter_map(|line| line.rsplit(' ').next().unwrap().parse::<usize>().ok())
                .sum::<usize>()
                == count
        });
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        self.stop();
        let _ = fs::remove_dir_all(&self.base);
        if !self.config.root.starts_with(&self.base) {
            let _ = fs::remove_dir_all(&self.config.root);
        }
    }
}
#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Key,
    pub parent: Option<Key>,
    pub pages: u32,
    pub row: bool,
    pub tier: u8,
    pub durable: bool,
    pub bytes: u64,
}
pub fn entry(bytes: &[u8]) -> Entry {
    let mut d = Decoder::new(bytes);
    let key = d.key().unwrap();
    let parent = d.parent().unwrap();
    d.take(16).unwrap();
    let pages = d.u32().unwrap();
    let row = d.u8().unwrap() != 0;
    let tier = d.u8().unwrap();
    let durable = d.u8().unwrap() != 0;
    let bytes = d.u64().unwrap();
    d.end().unwrap();
    Entry {
        key,
        parent,
        pages,
        row,
        tier,
        durable,
        bytes,
    }
}
pub struct Client {
    pub stream: UnixStream,
    pub generation: u64,
    pub next_op: u64,
    pub descriptor: Descriptor,
    pub entries: HashMap<Key, Entry>,
    pub seq: u64,
    control: u64,
    pub events: VecDeque<Frame>,
    pub snapshots: Vec<Vec<Key>>,
    snapshot: Vec<Key>,
}
impl Client {
    pub fn connect(config: &Config, descriptor: Descriptor) -> Self {
        let stream = UnixStream::connect(&config.socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut c = Self {
            stream,
            generation: 0,
            next_op: 2,
            descriptor,
            entries: HashMap::new(),
            seq: 0,
            control: 1,
            events: VecDeque::new(),
            snapshots: Vec::new(),
            snapshot: Vec::new(),
        };
        let mut p = Encoder::default();
        p.u32(protocol::VERSION);
        p.text("l0");
        p.u32(std::process::id());
        p.descriptor(&c.descriptor);
        c.send(tag::HELLO, 1, p.0);
        let welcome = Frame::read(&mut c.stream).unwrap();
        assert_eq!(welcome.tag, tag::WELCOME, "{welcome:?}");
        c.generation = welcome.generation;
        loop {
            let f = c.read();
            if f.tag == tag::SNAPSHOT_END {
                break;
            }
        }
        c
    }
    pub fn op(&mut self) -> u64 {
        let op = self.next_op;
        self.next_op += 1;
        op
    }
    pub fn send(&mut self, tag: u8, op: u64, payload: Vec<u8>) {
        Frame {
            tag,
            generation: self.generation,
            op,
            payload,
        }
        .write(&mut self.stream)
        .unwrap();
    }
    pub fn read(&mut self) -> Frame {
        let f = Frame::read(&mut self.stream).unwrap();
        assert_eq!(f.generation, self.generation);
        if matches!(
            f.tag,
            tag::SNAPSHOT | tag::ENTRY | tag::SNAPSHOT_END | tag::DELTA | tag::ROOM
        ) && f.op != self.control
        {
            return f;
        }
        match f.tag {
            tag::SNAPSHOT => {
                let mut d = Decoder::new(&f.payload);
                self.seq = d.u64().unwrap();
                let _count = d.u64().unwrap();
                self.entries.clear();
                self.snapshot.clear();
            }
            tag::ENTRY => {
                let e = entry(&f.payload);
                assert!(
                    e.parent
                        .is_none_or(|parent| self.entries.contains_key(&parent)),
                    "snapshot child before parent"
                );
                self.snapshot.push(e.key);
                self.entries.insert(e.key, e);
            }
            tag::SNAPSHOT_END => self.snapshots.push(self.snapshot.clone()),
            tag::DELTA => {
                let mut d = Decoder::new(&f.payload);
                let seq = d.u64().unwrap();
                if seq != self.seq + 1 {
                    self.entries.clear();
                    self.control = self.op();
                    self.send(tag::RESYNC, self.control, self.seq.to_le_bytes().to_vec());
                    return f;
                }
                self.seq = seq;
                let kind = d.u8().unwrap();
                if kind == 2 {
                    let k = d.key().unwrap();
                    assert!(
                        !self.entries.values().any(|entry| entry.parent == Some(k)),
                        "parent removed before child"
                    );
                    self.entries.remove(&k);
                } else {
                    let e = entry(&f.payload[9..]);
                    assert!(
                        e.parent
                            .is_none_or(|parent| self.entries.contains_key(&parent)),
                        "delta child before parent"
                    );
                    self.entries.insert(e.key, e);
                }
            }
            _ => {}
        }
        f
    }
    pub fn until(&mut self, tag: u8, op: u64) -> Frame {
        if let Some(index) = self.events.iter().position(|f| f.tag == tag && f.op == op) {
            return self.events.remove(index).unwrap();
        }
        loop {
            let f = self.read();
            if f.tag == tag && f.op == op {
                return f;
            }
            if !matches!(
                f.tag,
                tag::DELTA | tag::ENTRY | tag::SNAPSHOT | tag::SNAPSHOT_END
            ) {
                self.events.push_back(f);
            }
        }
    }
    pub fn put(&mut self, key: Key, parent: Option<Key>) -> (u64, Frame) {
        let op = self.op();
        let mut p = Encoder::default();
        p.key(key);
        p.parent(parent);
        p.bytes(&[4; 16]);
        let pages = (key.consumed_tokens / self.descriptor.page_tokens) as u32;
        p.u32(pages);
        p.u32(
            pages
                - parent.map_or(0, |parent| {
                    (parent.consumed_tokens / self.descriptor.page_tokens) as u32
                }),
        );
        p.u64(self.descriptor.page_bytes());
        p.u64(self.descriptor.row_bytes());
        self.send(tag::PUT, op, p.0);
        loop {
            let f = self.read();
            if f.op == op
                && matches!(
                    f.tag,
                    tag::ACCEPT | tag::PRESENT | tag::RETRY | tag::OVERSIZE
                )
            {
                return (op, f);
            }
        }
    }
    pub fn content(
        &self,
        key: Key,
        parent: Option<Key>,
        first: u32,
    ) -> (Header, Vec<u8>, Vec<u64>) {
        let pages = key.consumed_tokens / self.descriptor.page_tokens - u64::from(first);
        let mut header = Header::new(
            &self.descriptor,
            self.descriptor.identity,
            self.descriptor.fingerprint,
            key,
            parent,
            [4; 16],
            pages as u32,
            SystemTime::UNIX_EPOCH,
        )
        .unwrap();
        let mut bytes = vec![0; header.length_with_row() as usize];
        let page_bytes = header.page_record_bytes() as usize;
        let mut hashes = Vec::new();
        for page in 0..pages as usize {
            let offset = header.page_segment as usize + page * page_bytes;
            let data = &mut bytes[offset..offset + page_bytes];
            for (index, byte) in data.iter_mut().enumerate() {
                *byte = ((page + first as usize) * 31 + index * 7) as u8;
            }
            hashes.push(record::checksum(data));
        }
        for (index, byte) in bytes[header.row_segment as usize..].iter_mut().enumerate() {
            *byte = (key.consumed_tokens as usize + index * 13) as u8;
        }
        header.row_checksum = record::checksum(&bytes[header.row_segment as usize..]);
        bytes[..4096].copy_from_slice(&header.encode().unwrap());
        for (index, hash) in hashes.iter().enumerate() {
            bytes[4096 + index * 8..4096 + index * 8 + 8].copy_from_slice(&hash.to_le_bytes());
        }
        (header, bytes, hashes)
    }
    pub fn upload(&mut self, op: u64, header: &Header, bytes: &[u8], hashes: &[u64]) {
        let page_bytes = header.page_record_bytes() as usize;
        for first in (0..hashes.len()).step_by(64) {
            let end = (first + 64).min(hashes.len());
            let mut p = Encoder::default();
            p.u32(first as u32);
            p.u32((end - first) as u32);
            for hash in &hashes[first..end] {
                p.u64(*hash);
            }
            self.send(tag::WINDOW, op, p.0);
            let start = header.page_segment as usize + first * page_bytes;
            let end = header.page_segment as usize + end * page_bytes;
            self.data(op, start as u64, &bytes[start..end]);
        }
        self.send(tag::ROW, op, header.row_checksum.to_le_bytes().to_vec());
        self.data(
            op,
            header.row_segment,
            &bytes[header.row_segment as usize..],
        );
        for (index, chunk) in bytes[..header.page_segment as usize]
            .chunks(4096)
            .enumerate()
        {
            let mut payload = Encoder::default();
            payload.u64((index * 4096) as u64);
            payload.bytes(chunk);
            self.send(tag::COMMIT, op, payload.0);
        }
    }
    pub fn data(&mut self, op: u64, offset: u64, bytes: &[u8]) {
        for (index, chunk) in bytes.chunks(protocol::MAX_DATA).enumerate() {
            let mut p = Encoder::default();
            p.u64(offset + (index * protocol::MAX_DATA) as u64);
            p.bytes(chunk);
            self.send(tag::DATA, op, p.0);
        }
    }
    pub fn store(&mut self, key: Key, parent: Option<Key>) -> Vec<u8> {
        let (op, answer) = self.put(key, parent);
        assert_eq!(answer.tag, tag::ACCEPT, "{answer:?}");
        let mut accepted = Decoder::new(&answer.payload);
        let first = accepted.u32().unwrap();
        let shaped_parent = accepted.parent().unwrap();
        accepted.end().unwrap();
        let (header, bytes, hashes) = self.content(key, shaped_parent, first);
        self.upload(op, &header, &bytes, &hashes);
        let stored = self.until(tag::STORED, op);
        assert_eq!(stored.payload, [0], "{stored:?}");
        assert!(self.entries.contains_key(&key), "Stored before Added");
        bytes
    }
    pub fn get_start(&mut self, keys: &[Key], skip: u32, pages: u32) -> u64 {
        let op = self.op();
        let mut p = Encoder::default();
        p.u32(keys.len() as u32);
        for key in keys {
            p.key(*key);
        }
        p.u32(skip);
        p.u32(pages);
        p.u8(1);
        self.send(tag::GET, op, p.0);
        op
    }
    pub fn fetch(&mut self, keys: &[Key], skip: u32, pages: u32) -> Vec<(u32, u64, Vec<u8>)> {
        let op = self.get_start(keys, skip, pages);
        let mut data = Vec::new();
        loop {
            let f = self.read();
            if f.op != op {
                continue;
            }
            if f.tag == tag::DONE {
                assert_eq!(f.payload, [0], "{f:?}");
                break;
            }
            assert_eq!(f.tag, tag::DATA);
            let mut d = Decoder::new(&f.payload);
            let index = d.u32().unwrap();
            let offset = d.u64().unwrap();
            data.push((index, offset, f.payload[12..].to_vec()));
        }
        data
    }
    pub fn resync(&mut self) {
        let op = self.op();
        self.control = op;
        self.send(tag::RESYNC, op, self.seq.to_le_bytes().to_vec());
        self.until(tag::SNAPSHOT_END, op);
    }
}
