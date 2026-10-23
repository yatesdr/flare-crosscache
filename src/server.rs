use crate::{
    Key, Result,
    arena::{Arena, Extent},
    disk::{self, DiskExtent, RecordData, Task},
    invalid,
    protocol::{self, Decoder, Encoder, Frame, tag},
    record::{self, Descriptor, Header},
    tree::{Eviction, Tree},
};
use std::hash::Hasher;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{self, Read, Write},
    net::TcpListener,
    os::{
        fd::AsRawFd,
        unix::{
            fs::{FileTypeExt, PermissionsExt},
            net::{UnixListener, UnixStream},
        },
    },
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, Instant, SystemTime},
};
use twox_hash::XxHash3_64;

const MAX_CONNECTIONS: usize = 64;
const CLEANUP_BATCH: usize = 16;
const DEADLINE: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct Config {
    pub root: PathBuf,
    pub socket: PathBuf,
    pub memory: usize,
    pub nvme: u64,
    pub staging: usize,
    pub metrics: Option<String>,
    pub faults: disk::Faults,
}

impl Config {
    pub fn new(root: PathBuf, socket: PathBuf, memory: usize, nvme: u64) -> Self {
        Self {
            root,
            socket,
            memory,
            nvme,
            staging: 64 * 1024 * 1024,
            metrics: None,
            faults: disk::Faults::default(),
        }
    }
}

#[derive(Clone)]
struct Location {
    header: Header,
    memory: Option<Arc<Extent>>,
    disk: Option<Arc<DiskExtent>>,
    durable: bool,
    writing: bool,
    retry: Instant,
    bytes: u64,
}

impl Location {
    fn reading(&self) -> bool {
        self.memory
            .as_ref()
            .is_some_and(|extent| Arc::strong_count(extent) > 1)
            || self
                .disk
                .as_ref()
                .is_some_and(|extent| Arc::strong_count(extent) > 1)
    }
}

struct Namespace {
    directory: PathBuf,
    descriptor: Descriptor,
    tree: Tree<Location>,
    seq: u64,
    disk: disk::Worker,
    inflight: HashSet<Key>,
}

impl Namespace {
    fn path(&self, key: Key) -> PathBuf {
        self.directory.join(record::file_name(&key))
    }
    fn temporary(&self, id: u64) -> Arc<DiskExtent> {
        let extent = Arc::new(DiskExtent::new(
            self.directory.join(format!(".write-{id}.tmp")),
        ));
        extent.retire();
        extent
    }
    fn parent_durable(&self, parent: Option<Key>) -> bool {
        parent.is_none_or(|key| self.tree[&key].location.durable)
    }
}

struct Put {
    header: Header,
    memory: Option<Arc<Extent>>,
    temporary: Arc<DiskExtent>,
    checksums: Vec<u64>,
    cursor: u64,
    window_end: u64,
    verified_pages: usize,
    page_hash: Option<XxHash3_64>,
    row_hash: Option<u64>,
    row_started: bool,
    committed: bool,
    pending: usize,
    failed: bool,
    pins: VecDeque<Key>,
    replacement: bool,
    metadata: Option<Arc<Vec<u8>>>,
    commit_bytes: Vec<u8>,
    finalizing: bool,
}

struct Lease {
    nodes: VecDeque<Key>,
    cancelled: Arc<AtomicBool>,
}
enum Operation {
    Put(Box<Put>),
    Get(Lease),
}
struct Open {
    operation: Operation,
    progress: Instant,
}

struct Connection {
    generation: u64,
    control: u64,
    highest_op: u64,
    namespace: Option<String>,
    reader: Receiver<ReadEvent>,
    writer: SyncSender<Output>,
    socket: UnixStream,
    ops: HashMap<u64, Open>,
    held: Option<Frame>,
    blocked: bool,
    recycle: SyncSender<Vec<u8>>,
    refused: u64,
    dead: bool,
    pid: u32,
    connected: Instant,
}

impl Connection {
    fn send(&mut self, output: Output) {
        self.dead |= self.writer.try_send(output).is_err();
    }
    fn reply(&mut self, tag: u8, op: u64, payload: Vec<u8>) {
        self.send(Output::Frame(Frame::new(tag, self.generation, op, payload)));
    }
}

enum ReadEvent {
    Frame(Frame),
    Closed,
}
enum Output {
    Close,
    Frame(Frame),
    Snapshot {
        generation: u64,
        op: u64,
        seq: u64,
        entries: Vec<Vec<u8>>,
    },
    Get {
        generation: u64,
        op: u64,
        records: Vec<ReadRecord>,
        cancelled: Arc<AtomicBool>,
    },
}
struct ReadRecord {
    memory: Option<Arc<Extent>>,
    _disk: Option<Arc<DiskExtent>>,
    ranges: Vec<(u64, usize)>,
    header: Header,
    row: bool,
    file: Option<Arc<File>>,
    fill: Option<Extent>,
}
enum WriterEvent {
    Filled(u64, Key, Box<Header>, Arc<Extent>),
    Progress(u64, u64),
    Finished(u64, u64, bool),
    Closed(u64),
}

#[derive(Default)]
struct Metrics {
    dropped: u64,
    cancels: u64,
    puts_ok: u64,
    puts_error: u64,
    present: u64,
    retry_queue: u64,
    retry_budget: u64,
    retry_inflight: u64,
    oversize: u64,
    gets_ok: u64,
    gets_error: u64,
    evictions: u64,
    compactions: u64,
    nvme_bytes: u64,
    nvme_seconds: f64,
    nvme_errors: u64,
    hits: HashMap<(String, &'static str), u64>,
}

enum Job {
    Write {
        namespace: String,
        key: Key,
    },
    Chunk {
        connection: u64,
        op: u64,
    },
    Finish {
        connection: u64,
        op: u64,
        final_extent: Arc<DiskExtent>,
    },
    Compact {
        namespace: String,
        key: Key,
        bytes: u64,
    },
}

struct Subscription {
    namespace: String,
    generation: u64,
    op: u64,
    writer: SyncSender<Output>,
    failed: bool,
}

struct State {
    config: Config,
    lock: Arc<disk::RootLock>,
    memory: Arena,
    staging: Arena,
    namespaces: HashMap<String, Namespace>,
    connections: HashMap<u64, Connection>,
    order: VecDeque<u64>,
    cleanup: VecDeque<(String, VecDeque<Key>)>,
    jobs: HashMap<u64, Job>,
    job_counter: u64,
    clock: u64,
    boot: u64,
    counter: u64,
    metrics: Metrics,
    writers: Receiver<WriterEvent>,
    writer_sender: SyncSender<WriterEvent>,
    charged: u64,
    deferred_disk: Vec<Arc<DiskExtent>>,
    retired_puts: Vec<(String, Key, Arc<DiskExtent>, u64)>,
    retired_records: Vec<(String, Key, Location)>,
    subscribers: HashMap<u64, Subscription>,
}

pub fn run(config: Config, stop: Arc<AtomicBool>) -> Result<()> {
    if config.memory < 4096 || config.staging < 2 * protocol::MAX_DATA || config.nvme < 8192 {
        return Err(invalid(
            "memory >= 4096, staging >= 8 MiB and nvme >= 8192 required",
        ));
    }
    let lock = Arc::new(disk::RootLock::acquire(&config.root)?);
    let recovered = disk::recover(&config.root)?;
    let memory = Arena::new(config.memory)?;
    let staging = Arena::new(config.staging)?;
    let mut nonce = [0; 8];
    File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    let (writer_sender, writers) = mpsc::sync_channel(256);
    let mut state = State {
        config: config.clone(),
        lock,
        memory,
        staging,
        namespaces: HashMap::new(),
        connections: HashMap::new(),
        order: VecDeque::new(),
        cleanup: VecDeque::new(),
        jobs: HashMap::new(),
        job_counter: 0,
        clock: 0,
        boot: u64::from_le_bytes(nonce),
        counter: 0,
        metrics: Metrics::default(),
        writers,
        writer_sender,
        charged: 0,
        deferred_disk: Vec::new(),
        retired_puts: Vec::new(),
        retired_records: Vec::new(),
        subscribers: HashMap::new(),
    };
    let mut recovered_age = Vec::new();
    for recovered in recovered {
        let mut namespace = state.new_namespace(recovered.descriptor);
        for (header, row, extent) in recovered.records {
            let modified = fs::metadata(&extent.path)?
                .modified()
                .unwrap_or(SystemTime::UNIX_EPOCH);
            recovered_age.push((modified, namespace.descriptor.name(), header.key));
            let bytes = disk::ENVELOPE_BYTES + header.length(row);
            namespace.tree.insert(
                header.key,
                header.parent,
                header.bucket,
                header.tail_pages as usize,
                row,
                Location {
                    header: header.clone(),
                    memory: None,
                    disk: Some(extent),
                    durable: true,
                    writing: false,
                    retry: Instant::now(),
                    bytes,
                },
            )?;
            state.charged += bytes;
        }
        state
            .namespaces
            .insert(namespace.descriptor.name(), namespace);
    }
    recovered_age.sort_unstable();
    for (_, name, key) in recovered_age {
        state.clock += 1;
        state
            .namespaces
            .get_mut(&name)
            .unwrap()
            .tree
            .access_at(&key, state.clock);
    }
    let directory = config
        .socket
        .parent()
        .ok_or_else(|| invalid("socket directory"))?;
    fs::create_dir_all(directory)?;
    fs::set_permissions(directory, fs::Permissions::from_mode(0o770))?;
    if config.socket.exists() {
        if !fs::symlink_metadata(&config.socket)?
            .file_type()
            .is_socket()
        {
            return Err(invalid("socket path is not a socket"));
        }
        // Refuse to unlink a live manager even when it uses another disk root.
        if UnixStream::connect(&config.socket).is_ok() {
            return Err(invalid("socket is already in use"));
        }
        fs::remove_file(&config.socket)?;
    }
    let listener = UnixListener::bind(&config.socket)?;
    fs::set_permissions(&config.socket, fs::Permissions::from_mode(0o660))?;
    listener.set_nonblocking(true)?;
    let (accepted, arrivals) = mpsc::sync_channel(16);
    let accept_stop = stop.clone();
    let accept_thread = thread::spawn(move || {
        while !accept_stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let _ = accepted.try_send(stream);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2))
                }
                Err(_) => break,
            }
        }
    });
    let metrics_text = Arc::new(Mutex::new(String::new()));
    let metrics_thread = config
        .metrics
        .as_ref()
        .map(|address| start_metrics(address, metrics_text.clone(), stop.clone()))
        .transpose()?;
    let mut metrics_at = Instant::now();
    while !stop.load(Ordering::Acquire) {
        if let Ok(stream) = arrivals.try_recv() {
            state.connect(stream)?;
        }
        for _ in 0..64 {
            let Ok(event) = state.writers.try_recv() else {
                break;
            };
            state.writer_event(event);
        }
        state.disk_events();
        let count = state.order.len();
        let mut active = false;
        for _ in 0..count {
            let id = state.order.pop_front().unwrap();
            if let Some(mut connection) = state.connections.remove(&id) {
                if connection.namespace.is_none() && connection.connected.elapsed() >= DEADLINE {
                    connection.dead = true;
                }
                if !connection.dead {
                    let frame =
                        connection
                            .held
                            .take()
                            .or_else(|| match connection.reader.try_recv() {
                                Ok(ReadEvent::Frame(frame)) => Some(frame),
                                Ok(ReadEvent::Closed) | Err(mpsc::TryRecvError::Disconnected) => {
                                    connection.dead = true;
                                    None
                                }
                                Err(mpsc::TryRecvError::Empty) => None,
                            });
                    if let Some(frame) = frame {
                        active = true;
                        connection.blocked = false;
                        if let Err(error) = state.frame(id, &mut connection, &frame) {
                            let _ = error;
                            connection.dead = true;
                        }
                        connection.dead |= state
                            .subscribers
                            .get(&id)
                            .is_some_and(|subscriber| subscriber.failed);
                        if connection.blocked {
                            connection.held = Some(frame);
                        } else if frame.payload.len() > 4096 {
                            let _ = connection.recycle.try_send(frame.payload);
                        }
                    }
                    let expired: Vec<u64> = connection
                        .ops
                        .iter()
                        .filter(|(_, open)| open.progress.elapsed() >= DEADLINE)
                        .map(|(op, _)| *op)
                        .collect();
                    for op in expired {
                        connection.reply(tag::CANCEL, op, Vec::new());
                        state.cancel(&mut connection, op);
                    }
                }
                connection.dead |= state
                    .subscribers
                    .get(&id)
                    .is_some_and(|subscriber| subscriber.failed);
                if connection.dead {
                    state.subscribers.remove(&id);
                    let _ = connection.socket.shutdown(std::net::Shutdown::Both);
                    let ops: Vec<u64> = connection.ops.keys().copied().collect();
                    for op in ops {
                        state.cancel(&mut connection, op);
                    }
                } else {
                    state.connections.insert(id, connection);
                    state.order.push_back(id);
                }
            }
        }
        state.cleanup_turn();
        state.schedule();
        state.room();
        if metrics_at.elapsed() >= Duration::from_millis(20) {
            *metrics_text.lock().unwrap() = state.metrics_text();
            metrics_at = Instant::now();
        }
        if !active {
            thread::park_timeout(Duration::from_micros(200));
        }
    }
    for connection in state.connections.values() {
        let _ = connection.socket.shutdown(std::net::Shutdown::Both);
    }
    accept_thread.join().unwrap();
    if let Some(thread) = metrics_thread {
        thread.join().unwrap();
    }
    fs::remove_file(&config.socket)?;
    Ok(())
}

impl State {
    fn new_namespace(&self, descriptor: Descriptor) -> Namespace {
        Namespace {
            directory: self.config.root.join(descriptor.name()),
            tree: Tree::new(descriptor.page_tokens),
            descriptor,
            seq: 0,
            disk: disk::Worker::start(self.config.faults.clone(), self.lock.clone()),
            inflight: HashSet::new(),
        }
    }
    fn connect(&mut self, socket: UnixStream) -> Result<()> {
        if self.connections.len() >= MAX_CONNECTIONS {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        let pid = {
            let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
            let mut length = std::mem::size_of_val(&credentials) as libc::socklen_t;
            let result = unsafe {
                libc::getsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_PEERCRED,
                    (&mut credentials as *mut libc::ucred).cast(),
                    &mut length,
                )
            };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            if credentials.uid != unsafe { libc::geteuid() }
                || credentials.gid != unsafe { libc::getegid() }
            {
                return Ok(());
            }
            credentials.pid as u32
        };
        #[cfg(not(target_os = "linux"))]
        return Err(invalid("SO_PEERCRED requires Linux"));
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| invalid("connection counter exhausted"))?;
        let id = self.counter;
        let generation = self.boot ^ id;
        let (events, reader) = mpsc::sync_channel(1);
        let (recycle, buffers) = mpsc::sync_channel(2);
        let (writer, output) = mpsc::sync_channel(8);
        let mut read_socket = socket.try_clone()?;
        let mut write_socket = socket.try_clone()?;
        write_socket.set_write_timeout(Some(DEADLINE))?;
        let writer_events = self.writer_sender.clone();
        let staging = self.staging.clone();
        let writer_lock = self.lock.clone();
        let state_thread = thread::current();
        thread::spawn(move || {
            while let Ok((mut frame, length)) = Frame::read_header(&mut read_socket) {
                if length > 4096 {
                    frame.payload = buffers.try_recv().unwrap_or_default();
                }
                frame.payload.resize(length - 17, 0);
                if read_socket.read_exact(&mut frame.payload).is_err() {
                    break;
                }
                if events.send(ReadEvent::Frame(frame)).is_err() {
                    return;
                }
                state_thread.unpark();
            }
            let _ = events.send(ReadEvent::Closed);
            state_thread.unpark();
        });
        thread::spawn(move || {
            let _lock = writer_lock;
            for command in output {
                if output_command(&mut write_socket, command, &staging, id, &writer_events).is_err()
                {
                    break;
                }
            }
            let _ = write_socket.shutdown(std::net::Shutdown::Both);
            let _ = writer_events.send(WriterEvent::Closed(id));
        });
        self.connections.insert(
            id,
            Connection {
                generation,
                control: 0,
                highest_op: 0,
                namespace: None,
                reader,
                writer,
                socket,
                ops: HashMap::new(),
                held: None,
                blocked: false,
                recycle,
                refused: 0,
                dead: false,
                pid,
                connected: Instant::now(),
            },
        );
        self.order.push_back(id);
        Ok(())
    }
    fn result(&self, connection: &mut Connection, tag: u8, op: u64, result: Result<()>) {
        let mut payload = Encoder::default();
        match result {
            Ok(()) => payload.u8(0),
            Err(error) => {
                payload.u8(1);
                payload.text(&error.to_string());
            }
        }
        connection.reply(tag, op, payload.0);
    }
    fn retry(&mut self, connection: &mut Connection, op: u64, reason: u8) {
        match reason {
            0 => self.metrics.retry_queue += 1,
            1 => self.metrics.retry_budget += 1,
            _ => self.metrics.retry_inflight += 1,
        }
        connection.reply(tag::RETRY, op, vec![reason]);
    }
    fn frame(&mut self, id: u64, connection: &mut Connection, frame: &Frame) -> Result<()> {
        if connection.namespace.is_none() {
            if frame.tag != tag::HELLO || frame.op == 0 {
                return Err(invalid("expected Hello"));
            }
            return self.hello(id, connection, frame);
        }
        if frame.generation != connection.generation || frame.op == 0 {
            self.metrics.dropped += 1;
            return Ok(());
        }
        let new = matches!(frame.tag, tag::PUT | tag::GET | tag::RESYNC);
        if new {
            if frame.op <= connection.highest_op {
                self.metrics.dropped += 1;
                return Ok(());
            }
            connection.highest_op = frame.op;
        } else if !connection.ops.contains_key(&frame.op) {
            self.metrics.dropped += 1;
            return Ok(());
        }
        match frame.tag {
            tag::PUT => self.put(connection, frame),
            tag::GET => self.get(connection, frame),
            tag::RESYNC => {
                let mut decoder = Decoder::new(&frame.payload);
                let _last_seq = decoder.u64()?;
                decoder.end()?;
                connection.control = frame.op;
                self.subscribers.get_mut(&id).unwrap().op = frame.op;
                self.snapshot(connection);
                Ok(())
            }
            tag::CANCEL => {
                if !frame.payload.is_empty() {
                    return Err(invalid("Cancel payload"));
                }
                self.metrics.cancels += 1;
                self.cancel(connection, frame.op);
                Ok(())
            }
            tag::WINDOW | tag::ROW | tag::DATA | tag::COMMIT => {
                let op = frame.op;
                match self.put_frame(id, connection, frame) {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        self.metrics.puts_error += 1;
                        self.cancel(connection, op);
                        self.result(connection, tag::STORED, op, Err(error));
                        Ok(())
                    }
                }
            }
            _ => {
                self.metrics.dropped += 1;
                Ok(())
            }
        }
    }
    fn hello(&mut self, id: u64, connection: &mut Connection, frame: &Frame) -> Result<()> {
        let parsed = (|| {
            let mut decoder = Decoder::new(&frame.payload);
            if decoder.u32()? != protocol::VERSION {
                return Err(invalid("protocol version"));
            }
            let name = decoder.text()?;
            if name.is_empty() || name.len() > 256 {
                return Err(invalid("replica name"));
            }
            if decoder.u32()? != connection.pid {
                return Err(invalid("pid differs from SO_PEERCRED"));
            }
            let descriptor = decoder.descriptor()?;
            decoder.end()?;
            let namespace = descriptor.name();
            if let Some(stored) = self.namespaces.get(&namespace) {
                if stored.descriptor != descriptor {
                    return Err(invalid("namespace descriptor mismatch"));
                }
            } else {
                disk::descriptor(&self.config.root.join(&namespace), &descriptor)?;
                self.namespaces
                    .insert(namespace.clone(), self.new_namespace(descriptor));
            }
            Ok(namespace)
        })();
        match parsed {
            Ok(namespace) => {
                self.subscribers.insert(
                    id,
                    Subscription {
                        namespace: namespace.clone(),
                        generation: connection.generation,
                        op: frame.op,
                        writer: connection.writer.clone(),
                        failed: false,
                    },
                );
                connection.namespace = Some(namespace);
                connection.control = frame.op;
                connection.highest_op = frame.op;
                let mut limits = Encoder::default();
                limits.u32(protocol::VERSION);
                limits.u32(protocol::MAX_FRAME as u32);
                limits.u32(4);
                limits.u32(4);
                limits.u32(64);
                limits.u32(2);
                limits.u32(2);
                limits.u32(3000);
                connection.reply(tag::WELCOME, frame.op, limits.0);
                self.snapshot(connection);
            }
            Err(error) => {
                let mut reason = Encoder::default();
                reason.text(&error.to_string());
                connection.reply(tag::REJECT, frame.op, reason.0);
                // The writer closes after sending the rejection.
                connection.send(Output::Close);
            }
        }
        Ok(())
    }
    fn entry(namespace: &Namespace, key: Key) -> Vec<u8> {
        let node = &namespace.tree[&key];
        let mut entry = Encoder::default();
        entry.key(key);
        entry.parent(node.parent);
        entry.bytes(&node.bucket);
        entry.u32(node.tail_pages as u32);
        entry.u8(node.row as u8);
        entry.u8(if node.location.memory.is_some() { 0 } else { 1 });
        entry.u8(node.location.durable as u8);
        entry.u64(node.location.bytes);
        entry.0
    }
    fn snapshot(&self, connection: &mut Connection) {
        let namespace = &self.namespaces[connection.namespace.as_ref().unwrap()];
        let mut keys: Vec<Key> = namespace.tree.nodes().map(|node| node.key).collect();
        keys.sort_unstable();
        let entries = keys
            .into_iter()
            .map(|key| Self::entry(namespace, key))
            .collect();
        connection.send(Output::Snapshot {
            generation: connection.generation,
            op: connection.control,
            seq: namespace.seq,
            entries,
        });
    }

    fn delta(&mut self, name: &str, kind: u8, key: Key) {
        let namespace = self.namespaces.get_mut(name).unwrap();
        namespace.seq += 1;
        let mut payload = Encoder::default();
        payload.u64(namespace.seq);
        payload.u8(kind);
        if kind == 2 {
            payload.key(key);
        } else {
            payload.bytes(&Self::entry(namespace, key));
        }
        for subscriber in self
            .subscribers
            .values_mut()
            .filter(|subscriber| subscriber.namespace == name)
        {
            let frame = Frame::new(
                tag::DELTA,
                subscriber.generation,
                subscriber.op,
                payload.0.clone(),
            );
            subscriber.failed |= subscriber.writer.try_send(Output::Frame(frame)).is_err();
        }
    }
    fn put(&mut self, connection: &mut Connection, frame: &Frame) -> Result<()> {
        let mut decoder = Decoder::new(&frame.payload);
        let key = decoder.key()?;
        let published_parent = decoder.parent()?;
        let bucket = decoder.array()?;
        let chain_pages = decoder.u32()?;
        let published_tail = decoder.u32()?;
        let page_bytes = decoder.u64()?;
        let row_bytes = decoder.u64()?;
        decoder.end()?;
        let name = connection.namespace.clone().unwrap();
        let namespace = &self.namespaces[&name];
        let descriptor = &namespace.descriptor;
        if published_tail > chain_pages
            || (published_parent.is_none() && published_tail != chain_pages)
            || descriptor.page_bytes() != page_bytes
            || descriptor.row_bytes() != row_bytes
            || key.consumed_tokens == 0
            || key.consumed_tokens / descriptor.page_tokens != u64::from(chain_pages)
            || published_parent.is_some_and(|parent| {
                parent.consumed_tokens >= key.consumed_tokens
                    || key.consumed_tokens / descriptor.page_tokens
                        - parent.consumed_tokens / descriptor.page_tokens
                        != u64::from(published_tail)
            })
        {
            return Err(invalid("Put geometry or chain"));
        }
        let old = namespace.tree.node(&key);
        if namespace.inflight.contains(&key) || old.is_some_and(|node| !node.row && node.pinned()) {
            self.retry(connection, frame.op, 2);
            return Ok(());
        }
        if old.is_some_and(|node| node.row) {
            self.metrics.present += 1;
            connection.reply(tag::PRESENT, frame.op, Vec::new());
            return Ok(());
        }
        if connection
            .ops
            .values()
            .filter(|open| matches!(open.operation, Operation::Put(_)))
            .count()
            >= 4
        {
            self.retry(connection, frame.op, 0);
            return Ok(());
        }
        let replacement = old.is_some();
        let parent = match old {
            Some(node) => node.parent,
            None => published_parent
                .filter(|parent| namespace.tree.node(parent).is_some_and(|node| node.row)),
        };
        let first = parent.map_or(0, |parent| parent.consumed_tokens / descriptor.page_tokens);
        let tail = chain_pages - first as u32;
        let header = Header::new(descriptor, key, parent, bucket, tail)?;
        let bytes = disk::ENVELOPE_BYTES + header.length_with_row();
        if bytes > self.config.nvme {
            self.metrics.oversize += 1;
            connection.reply(tag::OVERSIZE, frame.op, Vec::new());
            return Ok(());
        }
        let pins: VecDeque<Key> = parent
            .into_iter()
            .chain(replacement.then_some(key))
            .collect();
        for key in &pins {
            self.namespaces.get_mut(&name).unwrap().tree.pin(key)?;
        }
        let fits_memory =
            header.length_with_row().next_power_of_two() <= self.memory.capacity() as u64;
        let room = self.make_room(bytes);
        let memory = if room && fits_memory {
            self.memory_room(header.length_with_row() as usize)
        } else {
            None
        };
        if !room || (fits_memory && memory.is_none()) {
            for key in &pins {
                self.namespaces.get_mut(&name).unwrap().tree.unpin(key)?;
            }
            connection.refused = connection.refused.max(bytes);
            self.retry(connection, frame.op, 1);
            return Ok(());
        }
        let cursor = header.page_segment;
        self.charged += bytes;
        let directory = &self.namespaces[&name].directory;
        self.job_counter += 1;
        let temporary = Arc::new(DiskExtent::new(directory.join(format!(
            ".{}-{}.tmp",
            record::file_name(&key),
            self.job_counter
        ))));
        temporary.retire();
        self.namespaces.get_mut(&name).unwrap().inflight.insert(key);
        connection.ops.insert(
            frame.op,
            Open {
                progress: Instant::now(),
                operation: Operation::Put(Box::new(Put {
                    header,
                    memory: memory.map(Arc::new),
                    temporary,
                    checksums: Vec::new(),
                    cursor,
                    window_end: cursor,
                    verified_pages: 0,
                    page_hash: None,
                    row_hash: None,
                    row_started: false,
                    committed: false,
                    pending: 0,
                    failed: false,
                    pins,
                    replacement,
                    metadata: None,
                    commit_bytes: Vec::new(),
                    finalizing: false,
                })),
            },
        );
        let mut payload = Encoder::default();
        payload.u32(first as u32);
        payload.parent(parent);
        connection.reply(tag::ACCEPT, frame.op, payload.0);
        Ok(())
    }
    fn oldest(
        &self,
        eligible: impl Fn(&crate::tree::Node<Location>) -> bool,
    ) -> Option<(String, Key)> {
        self.namespaces
            .iter()
            .flat_map(|(name, namespace)| {
                namespace
                    .tree
                    .nodes()
                    .filter(|node| {
                        !node.pinned()
                            && node.location.durable
                            && !node.location.writing
                            && eligible(node)
                    })
                    .map(move |node| (node.accessed(), node.key, name))
            })
            .min_by_key(|(accessed, key, _)| (*accessed, key.consumed_tokens))
            .map(|(_, key, name)| (name.clone(), key))
    }
    fn memory_room(&mut self, bytes: usize) -> Option<Extent> {
        loop {
            if let Some(extent) = self.memory.allocate(bytes) {
                return Some(extent);
            }
            let (name, key) = self.oldest(|node| {
                node.location
                    .memory
                    .as_ref()
                    .is_some_and(|data| Arc::strong_count(data) == 1)
            })?;
            self.namespaces.get_mut(&name).unwrap().tree[&key]
                .location
                .memory = None;
            self.delta(&name, 1, key);
        }
    }
    fn make_room(&mut self, bytes: u64) -> bool {
        if bytes > self.config.nvme {
            return false;
        }
        while self.charged.saturating_add(bytes) > self.config.nvme {
            let candidate = self.oldest(|node| {
                !node.location.reading()
                    && (node.children() == 0 || (node.row && node.children() == 1))
            });
            let Some((name, key)) = candidate else {
                return false;
            };
            let namespace = self.namespaces.get_mut(&name).unwrap();
            // Select exactly the candidate chosen by the global LRU.
            match namespace.tree.evict(true, |node| node.key == key) {
                Some(Eviction::Leaves(nodes)) => {
                    for node in nodes {
                        self.metrics.evictions += 1;
                        if let Some(extent) = node.location.disk {
                            extent.retire();
                            self.deferred_disk.push(extent);
                        }
                        self.charged -= node.location.bytes;
                        self.delta(&name, 2, node.key);
                    }
                }
                Some(Eviction::Compacted(key)) => {
                    let node = &self.namespaces[&name].tree[&key];
                    let source = node.location.disk.as_ref().unwrap().clone();
                    if source.reader().is_err() {
                        self.namespaces.get_mut(&name).unwrap().tree[&key].row = true;
                        return false;
                    }
                    let header = node.location.header.clone();
                    let freed = node.location.bytes - disk::ENVELOPE_BYTES - header.row_segment;
                    self.job_counter += 1;
                    let id = self.job_counter;
                    let temporary = Arc::new(DiskExtent::new(
                        source.path.with_file_name(format!(".compact-{id}.tmp")),
                    ));
                    temporary.retire();
                    let task = Task::Compact {
                        id,
                        source,
                        temporary,
                        header,
                    };
                    match self.namespaces[&name].disk.sender.try_send(task) {
                        Ok(()) => {
                            self.jobs.insert(
                                id,
                                Job::Compact {
                                    namespace: name.clone(),
                                    key,
                                    bytes: freed,
                                },
                            );
                            let node = &mut self.namespaces.get_mut(&name).unwrap().tree[&key];
                            node.location.writing = true;
                            self.namespaces
                                .get_mut(&name)
                                .unwrap()
                                .tree
                                .pin(&key)
                                .unwrap();
                            self.metrics.compactions += 1;
                            self.delta(&name, 1, key);
                        }
                        Err(_) => {
                            self.namespaces.get_mut(&name).unwrap().tree[&key].row = true;
                            return false;
                        }
                    }
                    // The disk bytes remain charged until truncation has completed.
                    return false;
                }
                None => return false,
            }
        }
        true
    }
    fn put_frame(&mut self, id: u64, connection: &mut Connection, frame: &Frame) -> Result<()> {
        let name = connection.namespace.clone().unwrap();
        let open = connection.ops.get_mut(&frame.op).unwrap();
        let Operation::Put(put) = &mut open.operation else {
            return Err(invalid("Put frame on Get"));
        };
        if put.committed || (!put.commit_bytes.is_empty() && frame.tag != tag::COMMIT) {
            return Err(invalid("frame after Commit"));
        }
        let mut decoder = Decoder::new(&frame.payload);
        match frame.tag {
            tag::WINDOW => {
                if put.row_started || put.cursor != put.window_end {
                    return Err(invalid("overlapping window"));
                }
                let index = decoder.u32()?;
                let pages = decoder.u32()?;
                if pages == 0
                    || pages > 64
                    || index != put.checksums.len() as u32
                    || index
                        .checked_add(pages)
                        .is_none_or(|end| end > put.header.tail_pages)
                {
                    return Err(invalid("window pages or index"));
                }
                for _ in 0..pages {
                    put.checksums.push(decoder.u64()?);
                }
                decoder.end()?;
                put.window_end = put.cursor + u64::from(pages) * put.header.page_record_bytes();
                put.page_hash = None;
            }
            tag::ROW => {
                if put.row_started
                    || put.cursor != put.header.row_segment
                    || put.cursor != put.window_end
                    || put.checksums.len() != put.header.tail_pages as usize
                {
                    return Err(invalid("row order"));
                }
                put.row_hash = Some(decoder.u64()?);
                decoder.end()?;
                put.row_started = true;
                put.window_end = put.header.length_with_row();
                put.page_hash = None;
            }
            tag::DATA => {
                let offset = decoder.u64()?;
                let count = frame
                    .payload
                    .len()
                    .checked_sub(8)
                    .ok_or_else(|| invalid("Data length"))?;
                if count == 0
                    || count > protocol::MAX_DATA
                    || count % 4096 != 0
                    || offset != put.cursor
                    || offset
                        .checked_add(count as u64)
                        .is_none_or(|end| end > put.window_end)
                {
                    return Err(invalid("Data offset or length"));
                }
                let data = decoder.take(count)?;
                if let Some(memory) = &mut put.memory {
                    Arc::get_mut(memory).unwrap().bytes_mut()
                        [offset as usize..offset as usize + count]
                        .copy_from_slice(data);
                } else {
                    if !self.namespaces[&name].parent_durable(put.header.parent) {
                        connection.blocked = true;
                        return Ok(());
                    }
                    let Some(mut extent) = self.staging.allocate(count) else {
                        connection.blocked = true;
                        return Ok(());
                    };
                    extent.bytes_mut().copy_from_slice(data);
                    self.job_counter += 1;
                    let job = self.job_counter;
                    let task = Task::Chunk {
                        id: job,
                        temporary: put.temporary.clone(),
                        offset,
                        data: Arc::new(extent),
                    };
                    match self.namespaces[&name].disk.sender.try_send(task) {
                        Ok(()) => {
                            put.pending += 1;
                            self.jobs.insert(
                                job,
                                Job::Chunk {
                                    connection: id,
                                    op: frame.op,
                                },
                            );
                        }
                        Err(TrySendError::Full(_)) => {
                            connection.blocked = true;
                            return Ok(());
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            return Err(invalid("disk worker stopped"));
                        }
                    }
                }
                let unit_bytes = if put.row_started {
                    put.header.row_record_bytes()
                } else {
                    put.header.page_record_bytes()
                };
                let base = if put.row_started {
                    put.header.row_segment
                } else {
                    put.header.page_segment
                };
                let mut cursor = 0;
                while cursor < data.len() {
                    let unit_offset = (put.cursor - base) % unit_bytes;
                    let bytes =
                        (unit_bytes - unit_offset).min((data.len() - cursor) as u64) as usize;
                    let slice = &data[cursor..cursor + bytes];
                    let hash = if unit_offset == 0 && bytes as u64 == unit_bytes {
                        Some(record::checksum(slice))
                    } else {
                        put.page_hash
                            .get_or_insert_with(XxHash3_64::default)
                            .write(slice);
                        (unit_offset + bytes as u64 == unit_bytes)
                            .then(|| put.page_hash.take().unwrap().finish())
                    };
                    put.cursor += bytes as u64;
                    cursor += bytes;
                    if let Some(hash) = hash {
                        let expected = if put.row_started {
                            put.row_hash.unwrap()
                        } else {
                            put.checksums[put.verified_pages]
                        };
                        if hash != expected {
                            return Err(invalid("checksum mismatch"));
                        }
                        if !put.row_started {
                            put.verified_pages += 1;
                        }
                        put.page_hash = None;
                    }
                }
                self.config.faults.checkpoint("data");
            }
            tag::COMMIT => {
                if !put.row_started || put.cursor != put.header.length_with_row() || put.failed {
                    return Err(invalid("short or failed payload"));
                }
                let offset = decoder.u64()? as usize;
                let count = frame
                    .payload
                    .len()
                    .checked_sub(8)
                    .ok_or_else(|| invalid("Commit length"))?;
                if offset != put.commit_bytes.len()
                    || count == 0
                    || count % 4096 != 0
                    || offset
                        .checked_add(count)
                        .is_none_or(|end| end > put.header.page_segment as usize)
                {
                    return Err(invalid("Commit offset or length"));
                }
                put.commit_bytes.extend_from_slice(decoder.take(count)?);
                decoder.end()?;
                open.progress = Instant::now();
                if put.commit_bytes.len() < put.header.page_segment as usize {
                    return Ok(());
                }
                let header = Header::decode(&put.commit_bytes[..4096])?;
                let descriptor = &self.namespaces[&name].descriptor;
                if header.key != put.header.key
                    || header.parent != put.header.parent
                    || header.bucket != put.header.bucket
                    || header.tail_pages != put.header.tail_pages
                    || header.identity != descriptor.identity
                    || header.fingerprint != descriptor.fingerprint
                    || !header.matches(descriptor)
                    || header.row_checksum != put.row_hash.unwrap()
                    || header.flags != 0
                {
                    return Err(invalid("Commit header disagrees with Accept"));
                }
                let checksum_segment = &put.commit_bytes[4096..];
                for (index, hash) in put.checksums.iter().enumerate() {
                    if checksum_segment[index * 8..index * 8 + 8] != hash.to_le_bytes() {
                        return Err(invalid("Commit checksum table"));
                    }
                }
                put.header = header;
                if let Some(memory) = &mut put.memory {
                    let bytes = Arc::get_mut(memory).unwrap().bytes_mut();
                    bytes[..4096].copy_from_slice(&put.commit_bytes[..4096]);
                    bytes[4096..put.header.page_segment as usize].copy_from_slice(checksum_segment);
                } else {
                    put.metadata = Some(Arc::new(std::mem::take(&mut put.commit_bytes)));
                }
                put.committed = true;
                if put.memory.is_some() {
                    self.publish(connection, frame.op, false)?;
                }
            }
            _ => unreachable!(),
        }
        if let Some(open) = connection.ops.get_mut(&frame.op) {
            open.progress = Instant::now();
        }
        Ok(())
    }
    fn publish(&mut self, connection: &mut Connection, op: u64, durable: bool) -> Result<()> {
        let open = connection.ops.remove(&op).unwrap();
        let Operation::Put(put) = open.operation else {
            unreachable!()
        };
        let name = connection.namespace.clone().unwrap();
        let key = put.header.key;
        let namespace = self.namespaces.get_mut(&name).unwrap();
        let path = namespace.path(key);
        let location = Location {
            header: put.header.clone(),
            memory: put.memory,
            disk: durable.then(|| Arc::new(DiskExtent::new(path))),
            durable,
            writing: false,
            retry: Instant::now(),
            bytes: disk::ENVELOPE_BYTES + put.header.length_with_row(),
        };
        if put.replacement {
            let node = &mut namespace.tree[&key];
            let old = std::mem::replace(&mut node.location, location);
            self.retired_records.push((name.clone(), key, old));
            node.row = true;
        } else {
            namespace.tree.insert(
                key,
                put.header.parent,
                put.header.bucket,
                put.header.tail_pages as usize,
                true,
                location,
            )?;
        }
        for key in &put.pins {
            namespace.tree.unpin(key)?;
        }
        namespace.inflight.remove(&key);
        self.clock += 1;
        namespace.tree.access_at(&key, self.clock);
        self.metrics.puts_ok += 1;
        self.delta(&name, if put.replacement { 1 } else { 0 }, key);
        self.result(connection, tag::STORED, op, Ok(()));
        Ok(())
    }
    fn get(&mut self, connection: &mut Connection, frame: &Frame) -> Result<()> {
        let parsed = (|| {
            if connection
                .ops
                .values()
                .filter(|open| matches!(open.operation, Operation::Get(_)))
                .count()
                >= 4
            {
                return Err(invalid("Get queue full"));
            }
            let mut decoder = Decoder::new(&frame.payload);
            let count = decoder.u32()?;
            if count == 0 || count > 65536 {
                return Err(invalid("Get node count"));
            }
            let mut keys = Vec::new();
            for _ in 0..count {
                keys.push(decoder.key()?);
            }
            let skip = decoder.u32()?;
            let expected = decoder.u32()?;
            if decoder.u8()? != 1 {
                return Err(invalid("Get requires endpoint row"));
            }
            decoder.end()?;
            let name = connection.namespace.as_ref().unwrap();
            let namespace = &self.namespaces[name];
            let mut total = 0u64;
            let mut records = Vec::new();
            for (index, key) in keys.iter().enumerate() {
                let node = namespace
                    .tree
                    .node(key)
                    .ok_or_else(|| invalid("Get key missing"))?;
                if (index > 0 && node.parent != Some(keys[index - 1]))
                    || (index + 1 == keys.len() && !node.row)
                {
                    return Err(invalid("Get chain"));
                }
                if index == 0 && skip as usize > node.tail_pages {
                    return Err(invalid("Get skip"));
                }
                total += node.tail_pages as u64;
                let header = node.location.header.clone();
                let skipped = if index == 0 { u64::from(skip) } else { 0 };
                let mut ranges = vec![(0, header.page_segment as usize)];
                let start = header.page_segment + skipped * header.page_record_bytes();
                if start < header.row_segment {
                    ranges.push((start, (header.row_segment - start) as usize));
                }
                if index + 1 == keys.len() {
                    ranges.push((header.row_segment, header.row_record_bytes() as usize));
                }
                records.push(ReadRecord {
                    memory: node.location.memory.clone(),
                    _disk: node.location.disk.clone(),
                    ranges,
                    header,
                    row: node.row,
                    fill: None,
                    file: node
                        .location
                        .disk
                        .as_ref()
                        .map(|extent| extent.reader())
                        .transpose()?,
                });
            }
            if total - u64::from(skip) != u64::from(expected) {
                return Err(invalid("Get page count"));
            }
            Ok((keys, records))
        })();
        let (keys, mut records) = match parsed {
            Ok(value) => value,
            Err(error) => {
                self.metrics.gets_error += 1;
                self.result(connection, tag::DONE, frame.op, Err(error));
                return Ok(());
            }
        };
        let name = connection.namespace.clone().unwrap();
        let namespace = self.namespaces.get_mut(&name).unwrap();
        for key in &keys {
            namespace.tree.pin(key)?;
            self.clock += 1;
            namespace.tree.access_at(key, self.clock);
        }
        for record in &mut records {
            if record.memory.is_none() {
                let length = record.header.length(record.row);
                record.fill = self.memory_room(length as usize);
            }
        }
        for record in &records {
            *self
                .metrics
                .hits
                .entry((
                    name.clone(),
                    if record.memory.is_some() {
                        "memory"
                    } else {
                        "disk"
                    },
                ))
                .or_default() += 1;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        connection.ops.insert(
            frame.op,
            Open {
                progress: Instant::now(),
                operation: Operation::Get(Lease {
                    nodes: keys.into(),
                    cancelled: cancelled.clone(),
                }),
            },
        );
        connection.send(Output::Get {
            generation: connection.generation,
            op: frame.op,
            records,
            cancelled,
        });
        Ok(())
    }
    fn cancel(&mut self, connection: &mut Connection, op: u64) {
        let Some(open) = connection.ops.remove(&op) else {
            return;
        };
        let name = connection.namespace.clone().unwrap();
        match open.operation {
            Operation::Put(put) => {
                self.cleanup.push_back((name.clone(), put.pins));
                self.retired_puts.push((
                    name.clone(),
                    put.header.key,
                    put.temporary.clone(),
                    disk::ENVELOPE_BYTES + put.header.length_with_row(),
                ));
                // Disk tasks own their buffers and temporary file until their last I/O completes.
            }
            Operation::Get(lease) => {
                lease.cancelled.store(true, Ordering::Release);
                self.cleanup.push_back((name, lease.nodes));
            }
        }
    }
    fn cleanup_turn(&mut self) {
        let mut budget = CLEANUP_BATCH;
        while budget > 0 {
            let Some((name, mut keys)) = self.cleanup.pop_front() else {
                break;
            };
            let namespace = self.namespaces.get_mut(&name).unwrap();
            while budget > 0 {
                let Some(key) = keys.pop_front() else {
                    break;
                };
                namespace.tree.unpin(&key).unwrap();
                budget -= 1;
            }
            if !keys.is_empty() {
                self.cleanup.push_back((name, keys));
            }
        }
        self.deferred_disk
            .retain(|extent| Arc::strong_count(extent) > 1);
        self.retired_puts.retain(|(name, key, extent, bytes)| {
            if Arc::strong_count(extent) > 1 {
                true
            } else {
                self.charged -= bytes;
                self.namespaces.get_mut(name).unwrap().inflight.remove(key);
                false
            }
        });
        self.retired_records.retain(|(name, key, location)| {
            let replaced_on_disk = self.namespaces[name]
                .tree
                .node(key)
                .is_none_or(|node| node.location.durable);
            if !replaced_on_disk || location.reading() {
                true
            } else {
                self.charged -= location.bytes;
                false
            }
        });
    }
    fn writer_event(&mut self, event: WriterEvent) {
        match event {
            WriterEvent::Filled(id, key, header, extent) => {
                let name = self
                    .connections
                    .get(&id)
                    .and_then(|connection| connection.namespace.clone());
                if let Some(name) = name {
                    let node = self.namespaces.get_mut(&name).unwrap().tree.node_mut(&key);
                    if let Some(node) = node {
                        if node.location.header == *header
                            && node.location.memory.is_none()
                            && node.location.durable
                            && extent.bytes().len() as u64 == node.location.header.length(node.row)
                        {
                            node.location.memory = Some(extent);
                            self.delta(&name, 1, key);
                        }
                    }
                }
            }
            WriterEvent::Progress(id, op) => {
                if let Some(open) = self
                    .connections
                    .get_mut(&id)
                    .and_then(|connection| connection.ops.get_mut(&op))
                {
                    open.progress = Instant::now();
                }
            }
            WriterEvent::Finished(id, op, success) => {
                if let Some(mut connection) = self.connections.remove(&id) {
                    if connection.ops.contains_key(&op) {
                        if success {
                            self.metrics.gets_ok += 1;
                        } else {
                            self.metrics.gets_error += 1;
                        }
                        self.cancel(&mut connection, op);
                    }
                    self.connections.insert(id, connection);
                }
            }
            WriterEvent::Closed(id) => {
                if let Some(connection) = self.connections.get_mut(&id) {
                    connection.dead = true;
                }
            }
        }
    }
    fn disk_events(&mut self) {
        let mut completions = Vec::new();
        for namespace in self.namespaces.values() {
            for _ in 0..16 {
                let Ok(completion) = namespace.disk.receiver.try_recv() else {
                    break;
                };
                completions.push(completion);
            }
        }
        for completion in completions {
            self.metrics.nvme_bytes += completion.bytes;
            self.metrics.nvme_seconds += completion.elapsed.as_secs_f64();
            if completion.result.is_err() {
                self.metrics.nvme_errors += 1;
            }
            let Some(job) = self.jobs.remove(&completion.id) else {
                continue;
            };
            match job {
                Job::Write { namespace, key } => {
                    let namespace_state = self.namespaces.get_mut(&namespace).unwrap();
                    let path = namespace_state.path(key);
                    let node = &mut namespace_state.tree[&key];
                    node.location.writing = false;
                    if completion.result.is_ok() {
                        node.location.durable = true;
                        node.location.disk = Some(Arc::new(DiskExtent::new(path)));
                        self.delta(&namespace, 1, key);
                    } else {
                        node.location.retry = Instant::now() + Duration::from_millis(100);
                    }
                }
                Job::Chunk { connection, op } => {
                    if let Some(open) = self
                        .connections
                        .get_mut(&connection)
                        .and_then(|connection| connection.ops.get_mut(&op))
                    {
                        if let Operation::Put(put) = &mut open.operation {
                            put.pending -= 1;
                            put.failed |= completion.result.is_err();
                            open.progress = Instant::now();
                        }
                    }
                }
                Job::Finish {
                    connection: id,
                    op,
                    final_extent,
                } => {
                    if let Some(mut connection) = self.connections.remove(&id) {
                        if connection.ops.contains_key(&op) {
                            if completion.result.is_ok() {
                                final_extent.keep();
                                if self.publish(&mut connection, op, true).is_err() {
                                    connection.dead = true;
                                }
                            } else {
                                self.metrics.puts_error += 1;
                                self.cancel(&mut connection, op);
                                self.result(
                                    &mut connection,
                                    tag::STORED,
                                    op,
                                    Err(invalid("streaming disk write failed")),
                                );
                            }
                        }
                        self.connections.insert(id, connection);
                    }
                }
                Job::Compact {
                    namespace,
                    key,
                    bytes,
                } => {
                    let path = self.namespaces[&namespace].path(key);
                    self.namespaces
                        .get_mut(&namespace)
                        .unwrap()
                        .tree
                        .unpin(&key)
                        .unwrap();
                    let node = &mut self.namespaces.get_mut(&namespace).unwrap().tree[&key];
                    node.location.writing = false;
                    if completion.result.is_ok() {
                        node.location.bytes -= bytes;
                        self.charged -= bytes;
                        // Drop the cached row as well; future reads use the compacted file.
                        node.location.memory = None;
                        node.location.disk = Some(Arc::new(DiskExtent::new(path)));
                        self.delta(&namespace, 1, key);
                    } else {
                        node.row = true;
                        self.delta(&namespace, 1, key);
                    }
                }
            }
        }
    }
    fn schedule(&mut self) {
        let names: Vec<String> = self.namespaces.keys().cloned().collect();
        for name in names {
            let namespace = &self.namespaces[&name];
            let mut keys: Vec<Key> = namespace
                .tree
                .nodes()
                .filter(|node| {
                    !node.location.durable
                        && !node.location.writing
                        && node.location.retry <= Instant::now()
                        && namespace.parent_durable(node.parent)
                })
                .map(|node| node.key)
                .collect();
            keys.sort_unstable();
            for key in keys {
                self.job_counter += 1;
                let id = self.job_counter;
                let node = &self.namespaces[&name].tree[&key];
                let header = node.location.header.clone();
                let temporary = self.namespaces[&name].temporary(id);
                let task = Task::Publish {
                    id,
                    temporary,
                    destination: Arc::new(DiskExtent::new(self.namespaces[&name].path(key))),
                    data: RecordData::Memory(node.location.memory.as_ref().unwrap().clone()),
                    header,
                    row: node.row,
                };
                if self.namespaces[&name].disk.sender.try_send(task).is_ok() {
                    self.jobs.insert(
                        id,
                        Job::Write {
                            namespace: name.clone(),
                            key,
                        },
                    );
                    self.namespaces.get_mut(&name).unwrap().tree[&key]
                        .location
                        .writing = true;
                } else {
                    break;
                }
            }
        }
        let ids: Vec<u64> = self.connections.keys().copied().collect();
        for id in ids {
            let mut connection = self.connections.remove(&id).unwrap();
            let name = connection.namespace.clone();
            let ops: Vec<u64> = connection.ops.iter().filter(|(_, open)| matches!(&open.operation, Operation::Put(put) if put.committed && put.memory.is_none() && put.pending == 0 && !put.finalizing))
                .map(|(op, _)| *op).collect();
            for op in ops {
                let name = name.as_ref().unwrap();
                let open = connection.ops.get_mut(&op).unwrap();
                let Operation::Put(put) = &mut open.operation else {
                    unreachable!()
                };
                if put.failed {
                    self.cancel(&mut connection, op);
                    self.metrics.puts_error += 1;
                    self.result(
                        &mut connection,
                        tag::STORED,
                        op,
                        Err(invalid("streaming disk write failed")),
                    );
                    continue;
                }
                if !self.namespaces[name].parent_durable(put.header.parent) {
                    continue;
                }
                self.job_counter += 1;
                let job = self.job_counter;
                let final_extent =
                    Arc::new(DiskExtent::new(self.namespaces[name].path(put.header.key)));
                let task = Task::Publish {
                    id: job,
                    temporary: put.temporary.clone(),
                    destination: final_extent.clone(),
                    data: RecordData::Metadata(put.metadata.as_ref().unwrap().clone()),
                    header: put.header.clone(),
                    row: true,
                };
                if self.namespaces[name].disk.sender.try_send(task).is_ok() {
                    put.finalizing = true;
                    self.jobs.insert(
                        job,
                        Job::Finish {
                            connection: id,
                            op,
                            final_extent,
                        },
                    );
                }
            }
            self.connections.insert(id, connection);
        }
    }
    fn room(&mut self) {
        let largest = self
            .connections
            .values()
            .map(|connection| connection.refused)
            .max()
            .unwrap_or(0);
        if largest > 0 {
            self.make_room(largest);
        }
        for connection in self.connections.values_mut() {
            if connection.refused == 0 {
                continue;
            }
            let reclaimable: usize = self
                .namespaces
                .values()
                .flat_map(|namespace| namespace.tree.nodes())
                .filter(|node| !node.pinned() && node.location.durable && !node.location.writing)
                .filter_map(|node| node.location.memory.as_ref())
                .filter(|extent| Arc::strong_count(extent) == 1)
                .map(|extent| extent.capacity())
                .sum();
            let memory_available = self.memory.capacity() - self.memory.used() + reclaimable;
            if self.charged.saturating_add(connection.refused) <= self.config.nvme
                && (connection.refused > self.memory.capacity() as u64
                    || memory_available as u64
                        >= connection
                            .refused
                            .saturating_sub(disk::ENVELOPE_BYTES)
                            .next_power_of_two())
            {
                connection.refused = 0;
                let frame = Frame::new(
                    tag::ROOM,
                    connection.generation,
                    connection.control,
                    Vec::new(),
                );
                connection.send(Output::Frame(frame));
            }
        }
    }
    fn metrics_text(&self) -> String {
        let mut text = String::new();
        for (name, value) in [
            ("unknown_frames_total", self.metrics.dropped),
            ("cancels_total", self.metrics.cancels),
            ("evictions_total", self.metrics.evictions),
            ("compactions_total", self.metrics.compactions),
            ("nvme_bytes_total", self.metrics.nvme_bytes),
            ("nvme_errors_total", self.metrics.nvme_errors),
            ("connections", self.connections.len() as u64),
            (
                "leases",
                self.connections
                    .values()
                    .flat_map(|connection| connection.ops.values())
                    .filter(|open| matches!(open.operation, Operation::Get(_)))
                    .count() as u64,
            ),
            (
                "open_ops",
                self.connections
                    .values()
                    .map(|connection| connection.ops.len() as u64)
                    .sum(),
            ),
            ("memory_allocated_bytes", self.memory.used() as u64),
            ("staging_allocated_bytes", self.staging.used() as u64),
            ("nvme_reserved_bytes", self.charged),
            (
                "cleanup_records",
                self.cleanup.iter().map(|(_, keys)| keys.len() as u64).sum(),
            ),
        ] {
            text.push_str(&format!("cm_{name} {value}\n"));
        }
        text.push_str(&format!(
            "cm_nvme_seconds_total {}\n",
            self.metrics.nvme_seconds
        ));
        for (outcome, count) in [
            ("ok", self.metrics.puts_ok),
            ("error", self.metrics.puts_error),
            ("present", self.metrics.present),
            ("queue", self.metrics.retry_queue),
            ("budget", self.metrics.retry_budget),
            ("inflight", self.metrics.retry_inflight),
            ("oversize", self.metrics.oversize),
        ] {
            text.push_str(&format!("cm_puts_total{{outcome=\"{outcome}\"}} {count}\n"));
        }
        for (outcome, count) in [
            ("ok", self.metrics.gets_ok),
            ("error", self.metrics.gets_error),
        ] {
            text.push_str(&format!("cm_gets_total{{outcome=\"{outcome}\"}} {count}\n"));
        }
        for (name, namespace) in &self.namespaces {
            for tier in ["memory", "disk"] {
                let mut count = 0;
                let mut bytes = 0;
                for node in namespace.tree.nodes() {
                    let retained = if tier == "memory" {
                        node.location.memory.is_some()
                    } else {
                        node.location.durable
                    };
                    if retained {
                        count += 1;
                        bytes += if tier == "memory" {
                            node.location.header.length(node.row)
                        } else {
                            node.location.bytes
                        };
                    }
                }
                text.push_str(&format!("cm_records{{namespace=\"{name}\",tier=\"{tier}\"}} {count}\ncm_bytes{{namespace=\"{name}\",tier=\"{tier}\"}} {bytes}\n"));
                let hits = self
                    .metrics
                    .hits
                    .get(&(name.clone(), tier))
                    .copied()
                    .unwrap_or(0);
                text.push_str(&format!(
                    "cm_hits_total{{namespace=\"{name}\",tier=\"{tier}\"}} {hits}\n"
                ));
            }
        }
        text
    }
}

fn output_command(
    socket: &mut UnixStream,
    command: Output,
    staging: &Arena,
    id: u64,
    events: &SyncSender<WriterEvent>,
) -> Result<()> {
    match command {
        Output::Close => Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")),
        Output::Frame(frame) => frame.write(socket),
        Output::Snapshot {
            generation,
            op,
            seq,
            entries,
        } => {
            let mut start = Encoder::default();
            start.u64(seq);
            start.u64(entries.len() as u64);
            Frame::new(tag::SNAPSHOT, generation, op, start.0).write(socket)?;
            for payload in entries {
                Frame::new(tag::ENTRY, generation, op, payload).write(socket)?;
            }
            Frame::new(
                tag::SNAPSHOT_END,
                generation,
                op,
                seq.to_le_bytes().to_vec(),
            )
            .write(socket)
        }
        Output::Get {
            generation,
            op,
            mut records,
            cancelled,
        } => {
            let result = stream_get(
                socket,
                generation,
                op,
                &mut records,
                &cancelled,
                staging,
                id,
                events,
            );
            let success = result.is_ok() && !cancelled.load(Ordering::Acquire);
            if !cancelled.load(Ordering::Acquire) {
                let mut payload = Encoder::default();
                match &result {
                    Ok(()) => payload.u8(0),
                    Err(error) => {
                        payload.u8(1);
                        payload.text(&error.to_string());
                    }
                }
                Frame::new(tag::DONE, generation, op, payload.0).write(socket)?;
            }
            let _ = events.send(WriterEvent::Finished(id, op, success));
            if result.as_ref().is_err_and(|error| {
                matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                )
            }) {
                result
            } else {
                Ok(())
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn stream_get(
    socket: &mut UnixStream,
    generation: u64,
    op: u64,
    records: &mut [ReadRecord],
    cancelled: &AtomicBool,
    staging: &Arena,
    id: u64,
    events: &SyncSender<WriterEvent>,
) -> Result<()> {
    for (index, record) in records.iter_mut().enumerate() {
        if cancelled.load(Ordering::Acquire) {
            return Err(invalid("cancelled"));
        }
        if let Some(mut extent) = record.fill.take() {
            let file = record.file.as_ref().unwrap();
            for (index, chunk) in extent
                .bytes_mut()
                .chunks_mut(protocol::MAX_DATA)
                .enumerate()
            {
                if cancelled.load(Ordering::Acquire) {
                    return Err(invalid("cancelled"));
                }
                disk::read_at(
                    file,
                    chunk,
                    disk::ENVELOPE_BYTES + (index * protocol::MAX_DATA) as u64,
                )?;
                let _ = events.send(WriterEvent::Progress(id, op));
            }
            // Verify before retaining a disk read as a DRAM copy.
            verify_record(extent.bytes(), &record.header, record.row)?;
            let extent = Arc::new(extent);
            record.memory = Some(extent.clone());
            let _ = events.send(WriterEvent::Filled(
                id,
                record.header.key,
                Box::new(record.header.clone()),
                extent,
            ));
        }
        let file = record.file.as_ref();
        for &(offset, length) in &record.ranges {
            let mut done = 0;
            while done < length {
                if cancelled.load(Ordering::Acquire) {
                    return Err(invalid("cancelled"));
                }
                let position = offset + done as u64;
                let mut count = (length - done).min(protocol::MAX_DATA);
                if offset >= record.header.page_segment && offset < record.header.row_segment {
                    let window_bytes = 64 * record.header.page_record_bytes();
                    count = count.min((window_bytes - done as u64 % window_bytes) as usize);
                }
                let mut prefix = [0; 12];
                prefix[..4].copy_from_slice(&(index as u32).to_le_bytes());
                prefix[4..].copy_from_slice(&position.to_le_bytes());
                let mut buffer;
                let data = if let Some(memory) = &record.memory {
                    &memory.bytes()[position as usize..position as usize + count]
                } else {
                    let start = Instant::now();
                    buffer = loop {
                        if let Some(buffer) = staging.allocate(count) {
                            break buffer;
                        }
                        if cancelled.load(Ordering::Acquire) || start.elapsed() > DEADLINE {
                            return Err(invalid("staging deadline"));
                        }
                        thread::sleep(Duration::from_micros(200));
                    };
                    disk::read_at(
                        file.unwrap(),
                        buffer.bytes_mut(),
                        disk::ENVELOPE_BYTES + position,
                    )?;
                    buffer.bytes()
                };
                if position == 0 && Header::decode(data)? != record.header {
                    return Err(invalid("disk header changed"));
                }
                Frame::write_parts(socket, tag::DATA, generation, op, &prefix, data)?;
                let _ = events.send(WriterEvent::Progress(id, op));
                done += count;
            }
        }
    }
    Ok(())
}

fn start_metrics(
    address: &str,
    text: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
) -> Result<thread::JoinHandle<()>> {
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let handle = thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
                    let mut request = [0; 1024];
                    let count = stream.read(&mut request).unwrap_or(0);
                    let (status, body) = if request[..count].starts_with(b"GET /metrics ") {
                        ("200 OK", text.lock().unwrap().clone())
                    } else {
                        ("404 Not Found", String::new())
                    };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(_) => break,
            }
        }
    });
    Ok(handle)
}

fn verify_record(bytes: &[u8], header: &Header, row: bool) -> Result<()> {
    let length = header.length(row);
    if Header::decode(bytes)? != *header || bytes.len() as u64 != length {
        return Err(invalid("record header or length"));
    }
    for page in 0..header.tail_pages as usize {
        let start = header.page_segment as usize + page * header.page_record_bytes() as usize;
        let expected = u64::from_le_bytes(
            bytes[4096 + page * 8..4096 + page * 8 + 8]
                .try_into()
                .unwrap(),
        );
        if record::checksum(&bytes[start..start + header.page_record_bytes() as usize]) != expected
        {
            return Err(invalid("disk page checksum"));
        }
    }
    if row && record::checksum(&bytes[header.row_segment as usize..]) != header.row_checksum {
        return Err(invalid("disk row checksum"));
    }
    Ok(())
}
