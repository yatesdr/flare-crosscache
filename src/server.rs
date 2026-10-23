use crate::{
    Key, Result,
    arena::Arena,
    codec::{Decoder, Encoder},
    disk, invalid,
    protocol::{self, Frame, tag},
    record::{self, Header},
    store::{
        self, DispatchOutcome, GetBeginOutcome, GetRequest, PutBeginOutcome, PutFrameInput,
        PutFrameOutcome, PutRequest, Store, StoreEvent,
    },
};
use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{self, Read, Write},
    net::TcpListener,
    os::{
        fd::AsRawFd,
        unix::{
            fs::{FileTypeExt, PermissionsExt},
            net::{UnixListener, UnixStream},
        },
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

pub use crate::store::Config;

const MAX_CONNECTIONS: usize = 64;
const DEADLINE: Duration = Duration::from_secs(3);

struct Connection {
    generation: u64,
    control: u64,
    highest_op: u64,
    reader: Receiver<ReadEvent>,
    writer: SyncSender<Output>,
    socket: UnixStream,
    held: Option<Frame>,
    blocked: bool,
    recycle: SyncSender<Vec<u8>>,
    dead: bool,
    pid: u32,
    connected: Instant,
    helloed: bool,
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
        records: Vec<store::ReadRecord>,
        cancelled: Arc<AtomicBool>,
    },
}
enum WriterEvent {
    Filled(u64, Key, Box<Header>, Arc<crate::arena::Extent>),
    Progress(u64, u64),
    Finished(u64, u64, bool),
    Closed(u64),
}

struct Subscription {
    namespace: record::NamespaceId,
    generation: u64,
    op: u64,
    writer: SyncSender<Output>,
    failed: bool,
}

pub fn run(config: Config, stop: Arc<AtomicBool>) -> Result<()> {
    let mut store = Store::open(config.clone())?;
    let staging = store.staging();
    let lock = store.lock();
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
    let mut connections: HashMap<u64, Connection> = HashMap::new();
    let mut subscribers: HashMap<u64, Subscription> = HashMap::new();
    let mut order: VecDeque<u64> = VecDeque::new();
    let (writer_sender, writers) = mpsc::sync_channel(256);
    let mut counter = 0u64;
    let mut metrics_at = Instant::now();
    while !stop.load(Ordering::Acquire) {
        if let Ok(stream) = arrivals.try_recv() {
            if connections.len() < MAX_CONNECTIONS {
                counter += 1;
                connect(
                    counter,
                    stream,
                    &mut connections,
                    &mut order,
                    &staging,
                    writer_sender.clone(),
                    lock.clone(),
                )?;
            }
        }
        for _ in 0..64 {
            let Ok(event) = writers.try_recv() else {
                break;
            };
            writer_event(&mut store, &mut connections, event);
        }
        store.disk_events();
        let events = store.take_events();
        flush_events(events, &mut connections, &mut subscribers);
        let count = order.len();
        let mut active = false;
        for _ in 0..count {
            let id = order.pop_front().unwrap();
            if let Some(mut connection) = connections.remove(&id) {
                if !connection.helloed && connection.connected.elapsed() >= DEADLINE {
                    connection.dead = true;
                }
                if !connection.dead {
                    let expired = store.expired(id, DEADLINE);
                    for op in expired {
                        connection.reply(tag::CANCEL, op, Vec::new());
                        store.cancel(id, op);
                    }
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
                        connection.blocked = false;
                        if let Err(_error) =
                            frame_in(&mut store, &mut subscribers, id, &mut connection, &frame)
                        {
                            connection.dead = true;
                        }
                        connection.dead |= subscribers
                            .get(&id)
                            .is_some_and(|subscriber| subscriber.failed);
                        if connection.blocked {
                            connection.held = Some(frame);
                        } else {
                            active = true;
                            if frame.payload.len() > 4096 {
                                let _ = connection.recycle.try_send(frame.payload);
                            }
                        }
                    }
                }
                connection.dead |= subscribers
                    .get(&id)
                    .is_some_and(|subscriber| subscriber.failed);
                if connection.dead {
                    subscribers.remove(&id);
                    let _ = connection.socket.shutdown(std::net::Shutdown::Both);
                    store.connection_closed(id);
                } else {
                    connections.insert(id, connection);
                    order.push_back(id);
                }
                let events = store.take_events();
                flush_events(events, &mut connections, &mut subscribers);
            }
        }
        let outcomes = store.dispatch();
        for outcome in outcomes {
            handle_dispatch(&mut store, &mut connections, outcome);
        }
        store.cleanup_turn();
        store.schedule();
        store.tick();
        let events = store.take_events();
        flush_events(events, &mut connections, &mut subscribers);
        if metrics_at.elapsed() >= Duration::from_millis(20) {
            *metrics_text.lock().unwrap() = store.metrics_text();
            metrics_at = Instant::now();
        }
        if !active {
            thread::park_timeout(Duration::from_micros(200));
        }
    }
    for connection in connections.values() {
        let _ = connection.socket.shutdown(std::net::Shutdown::Both);
    }
    accept_thread.join().unwrap();
    if let Some(thread) = metrics_thread {
        thread.join().unwrap();
    }
    fs::remove_file(&config.socket)?;
    Ok(())
}

fn connect(
    id: u64,
    socket: UnixStream,
    connections: &mut HashMap<u64, Connection>,
    order: &mut VecDeque<u64>,
    staging: &Arena,
    writer_sender: SyncSender<WriterEvent>,
    lock: Arc<disk::RootLock>,
) -> Result<()> {
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
    let (events, reader) = mpsc::sync_channel(1);
    let (recycle, buffers) = mpsc::sync_channel(2);
    let (writer, output) = mpsc::sync_channel(8);
    let mut read_socket = socket.try_clone()?;
    let mut write_socket = socket.try_clone()?;
    write_socket.set_write_timeout(Some(DEADLINE))?;
    let staging = staging.clone();
    let writer_lock = lock;
    let writer_events = writer_sender;
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
            if output_command(&mut write_socket, command, &staging, id, &writer_events).is_err() {
                break;
            }
        }
        let _ = write_socket.shutdown(std::net::Shutdown::Both);
        let _ = writer_events.send(WriterEvent::Closed(id));
    });
    connections.insert(
        id,
        Connection {
            generation: 0,
            control: 0,
            highest_op: 0,
            reader,
            writer,
            socket,
            held: None,
            blocked: false,
            recycle,
            dead: false,
            pid,
            connected: Instant::now(),
            helloed: false,
        },
    );
    order.push_back(id);
    Ok(())
}

fn writer_event(store: &mut Store, connections: &mut HashMap<u64, Connection>, event: WriterEvent) {
    match event {
        WriterEvent::Filled(id, key, header, extent) => {
            store.get_filled(id, key, &header, extent);
        }
        WriterEvent::Progress(id, op) => store.progress(id, op),
        WriterEvent::Finished(id, op, success) => store.get_finished(id, op, success),
        WriterEvent::Closed(id) => {
            if let Some(connection) = connections.get_mut(&id) {
                connection.dead = true;
            }
        }
    }
}

fn frame_in(
    store: &mut Store,
    subscribers: &mut HashMap<u64, Subscription>,
    id: u64,
    connection: &mut Connection,
    frame: &Frame,
) -> Result<()> {
    if !connection.helloed {
        if frame.tag != tag::HELLO || frame.op == 0 {
            return Err(invalid("expected Hello"));
        }
        return hello(store, subscribers, id, connection, frame);
    }
    if frame.generation != connection.generation || frame.op == 0 {
        store.count_dropped();
        return Ok(());
    }
    let new = matches!(frame.tag, tag::PUT | tag::GET | tag::RESYNC);
    if new {
        if frame.op <= connection.highest_op {
            store.count_dropped();
            return Ok(());
        }
        connection.highest_op = frame.op;
    } else if !store.has_op(id, frame.op) {
        store.count_dropped();
        return Ok(());
    }
    match frame.tag {
        tag::PUT => {
            let mut decoder = Decoder::new(&frame.payload);
            let key = decoder.key()?;
            let published_parent = decoder.parent()?;
            let bucket = decoder.array()?;
            let chain_pages = decoder.u32()?;
            let published_tail_pages = decoder.u32()?;
            let page_bytes = decoder.u64()?;
            let row_bytes = decoder.u64()?;
            decoder.end()?;
            store.submit_put(
                id,
                frame.op,
                PutRequest {
                    key,
                    published_parent,
                    bucket,
                    chain_pages,
                    published_tail_pages,
                    page_bytes,
                    row_bytes,
                    row: true,
                },
            );
        }
        tag::GET => {
            let request = (|| {
                let mut decoder = Decoder::new(&frame.payload);
                let count = decoder.u32()?;
                if count == 0 || count > 65536 {
                    return Err(invalid("Get node count"));
                }
                let mut keys = Vec::new();
                for _ in 0..count {
                    keys.push(decoder.key()?);
                }
                let skip_pages = decoder.u32()?;
                let expected_pages = decoder.u32()?;
                if decoder.u8()? != 1 {
                    return Err(invalid("Get requires endpoint row"));
                }
                decoder.end()?;
                Ok(GetRequest {
                    keys,
                    skip_pages,
                    expected_pages,
                    row: true,
                })
            })();
            match request {
                Ok(request) => store.submit_get(id, frame.op, request),
                Err(error) => {
                    store.count_get_error();
                    result(connection, tag::DONE, frame.op, Err(error));
                }
            }
        }
        tag::RESYNC => {
            let mut decoder = Decoder::new(&frame.payload);
            let _last_seq = decoder.u64()?;
            decoder.end()?;
            connection.control = frame.op;
            if let Some(subscriber) = subscribers.get_mut(&id) {
                subscriber.op = frame.op;
            }
            snapshot(store, id, connection);
        }
        tag::CANCEL => {
            if !frame.payload.is_empty() {
                return Err(invalid("Cancel payload"));
            }
            store.count_cancel();
            store.cancel(id, frame.op);
        }
        tag::WINDOW | tag::ROW | tag::DATA | tag::COMMIT => {
            let op = frame.op;
            let input = (|| -> Result<PutFrameInput<'_>> {
                Ok(match frame.tag {
                    tag::WINDOW => {
                        let mut decoder = Decoder::new(&frame.payload);
                        let index = decoder.u32()?;
                        let count = decoder.u32()?;
                        if count == 0 || count > 64 {
                            return Err(invalid("window pages"));
                        }
                        let mut pages = Vec::with_capacity(count as usize);
                        for _ in 0..count {
                            pages.push(decoder.u64()?);
                        }
                        decoder.end()?;
                        PutFrameInput::Window { index, pages }
                    }
                    tag::ROW => {
                        let mut decoder = Decoder::new(&frame.payload);
                        let checksum = decoder.u64()?;
                        decoder.end()?;
                        PutFrameInput::Row { checksum }
                    }
                    tag::DATA => {
                        let count = frame
                            .payload
                            .len()
                            .checked_sub(8)
                            .ok_or_else(|| invalid("Data length"))?;
                        let offset = u64::from_le_bytes(frame.payload[..8].try_into().unwrap());
                        PutFrameInput::Data {
                            offset,
                            data: &frame.payload[8..8 + count],
                        }
                    }
                    _ => {
                        let count = frame
                            .payload
                            .len()
                            .checked_sub(8)
                            .ok_or_else(|| invalid("Commit length"))?;
                        let offset = u64::from_le_bytes(frame.payload[..8].try_into().unwrap());
                        PutFrameInput::Commit {
                            offset,
                            metadata: &frame.payload[8..8 + count],
                        }
                    }
                })
            })();
            let input = match input {
                Ok(input) => input,
                Err(error) => {
                    // A malformed payload on an accepted Put finishes that operation only.
                    store.count_put_error();
                    store.cancel(id, op);
                    result(connection, tag::STORED, op, Err(error));
                    return Ok(());
                }
            };
            match store.put_frame(id, op, input) {
                PutFrameOutcome::Ok => {}
                PutFrameOutcome::Blocked => connection.blocked = true,
                PutFrameOutcome::Error(error) => {
                    store.count_put_error();
                    store.cancel(id, op);
                    result(connection, tag::STORED, op, Err(error));
                }
            }
        }
        _ => {
            store.count_dropped();
        }
    }
    Ok(())
}

fn hello(
    store: &mut Store,
    subscribers: &mut HashMap<u64, Subscription>,
    id: u64,
    connection: &mut Connection,
    frame: &Frame,
) -> Result<()> {
    let parsed = (|| {
        let mut decoder = Decoder::new(&frame.payload);
        if decoder.u32()? != protocol::VERSION {
            return Err(invalid("protocol version"));
        }
        let name = decoder.text()?.to_owned();
        if decoder.u32()? != connection.pid {
            return Err(invalid("pid differs from SO_PEERCRED"));
        }
        let descriptor = decoder.descriptor()?;
        decoder.end()?;
        Ok((name, descriptor))
    })();
    match parsed.and_then(|(name, descriptor)| {
        let generation = store.hello(id, name, connection.pid, descriptor)?;
        Ok(generation)
    }) {
        Ok(generation) => {
            connection.generation = generation;
            connection.control = frame.op;
            connection.highest_op = frame.op;
            connection.helloed = true;
            let namespace = store.session(id).unwrap().namespace.unwrap();
            subscribers.insert(
                id,
                Subscription {
                    namespace,
                    generation,
                    op: frame.op,
                    writer: connection.writer.clone(),
                    failed: false,
                },
            );
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
            snapshot(store, id, connection);
        }
        Err(error) => {
            let mut reason = Encoder::default();
            reason.text(&error.to_string());
            connection.reply(tag::REJECT, frame.op, reason.0);
            connection.send(Output::Close);
        }
    }
    Ok(())
}

fn snapshot(store: &Store, id: u64, connection: &mut Connection) {
    let Some(namespace) = store.session(id).and_then(|session| session.namespace) else {
        return;
    };
    let Some((seq, entries)) = store.snapshot(namespace) else {
        return;
    };
    let entries = entries.iter().map(entry_bytes).collect();
    connection.send(Output::Snapshot {
        generation: connection.generation,
        op: connection.control,
        seq,
        entries,
    });
}

fn entry_bytes(entry: &store::EntryData) -> Vec<u8> {
    let mut bytes = Encoder::default();
    bytes.key(entry.key);
    bytes.parent(entry.parent);
    bytes.bytes(&entry.bucket);
    bytes.u32(entry.tail_pages);
    bytes.u8(entry.row as u8);
    bytes.u8(entry.tier);
    bytes.u8(entry.durable as u8);
    bytes.u64(entry.bytes);
    bytes.0
}

fn result(connection: &mut Connection, tag: u8, op: u64, result: Result<()>) {
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

fn handle_dispatch(
    store: &mut Store,
    connections: &mut HashMap<u64, Connection>,
    outcome: DispatchOutcome,
) {
    match outcome {
        DispatchOutcome::Put {
            connection,
            op,
            outcome,
        } => {
            let Some(conn) = connections.get_mut(&connection) else {
                return;
            };
            match outcome {
                Err(_) => conn.dead = true,
                Ok(PutBeginOutcome::Accept { first_page, parent }) => {
                    let mut payload = Encoder::default();
                    payload.u32(first_page);
                    payload.parent(parent);
                    conn.reply(tag::ACCEPT, op, payload.0);
                }
                Ok(PutBeginOutcome::Present) => conn.reply(tag::PRESENT, op, Vec::new()),
                Ok(PutBeginOutcome::Retry(reason)) => {
                    conn.reply(tag::RETRY, op, vec![reason]);
                }
                Ok(PutBeginOutcome::Oversize) => conn.reply(tag::OVERSIZE, op, Vec::new()),
            }
        }
        DispatchOutcome::Get {
            connection,
            op,
            outcome,
        } => {
            let Some(conn) = connections.get_mut(&connection) else {
                return;
            };
            match outcome {
                GetBeginOutcome::Error(error) => {
                    result(conn, tag::DONE, op, Err(error));
                }
                GetBeginOutcome::Plan(plan) => {
                    let cancelled = store
                        .get_cancelled(connection, op)
                        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
                    conn.send(Output::Get {
                        generation: conn.generation,
                        op,
                        records: plan.records,
                        cancelled,
                    });
                }
            }
        }
    }
}

fn flush_events(
    events: Vec<StoreEvent>,
    connections: &mut HashMap<u64, Connection>,
    subscribers: &mut HashMap<u64, Subscription>,
) {
    for event in events {
        match event {
            StoreEvent::Catalog {
                namespace,
                seq,
                kind,
                key,
                entry,
            } => {
                let mut payload = Encoder::default();
                payload.u64(seq);
                payload.u8(kind);
                if kind == 2 {
                    payload.key(key);
                } else if let Some(entry) = entry {
                    payload.bytes(&entry_bytes(&entry));
                }
                for subscriber in subscribers
                    .values_mut()
                    .filter(|subscriber| subscriber.namespace == namespace)
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
            StoreEvent::Room { connection } => {
                if let Some(subscriber) = subscribers.get(&connection) {
                    let frame =
                        Frame::new(tag::ROOM, subscriber.generation, subscriber.op, Vec::new());
                    let failed = subscriber.writer.try_send(Output::Frame(frame)).is_err();
                    if failed {
                        if let Some(subscriber) = subscribers.get_mut(&connection) {
                            subscriber.failed = true;
                        }
                    }
                }
            }
            StoreEvent::PutResult {
                connection,
                op,
                result: outcome,
            } => {
                if let Some(conn) = connections.get_mut(&connection) {
                    result(conn, tag::STORED, op, outcome);
                }
            }
        }
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
    records: &mut [store::ReadRecord],
    cancelled: &AtomicBool,
    staging: &Arena,
    id: u64,
    events: &SyncSender<WriterEvent>,
) -> Result<()> {
    for (index, record) in records.iter_mut().enumerate() {
        if cancelled.load(Ordering::Acquire) {
            return Err(invalid("cancelled"));
        }
        if let Some(extent) = record.fill.take() {
            let file = record.file.as_ref().unwrap();
            let extent = record.reader.read(file, disk::ENVELOPE_BYTES, extent)?;
            let _ = events.send(WriterEvent::Progress(id, op));
            // Verify before retaining a disk read as a DRAM copy.
            record::verify_record(extent.bytes(), &record.header, record.row)?;
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
                let data;
                if let Some(memory) = &record.memory {
                    data = &memory.bytes()[position as usize..position as usize + count];
                } else {
                    let start = Instant::now();
                    let mut buffer = loop {
                        if let Some(buffer) = staging.allocate(count) {
                            break buffer;
                        }
                        if cancelled.load(Ordering::Acquire) || start.elapsed() > DEADLINE {
                            return Err(invalid("staging deadline"));
                        }
                        thread::sleep(Duration::from_micros(200));
                    };
                    buffer = record.reader.read(
                        file.unwrap(),
                        disk::ENVELOPE_BYTES + position,
                        buffer,
                    )?;
                    // Keep the buffer alive while its bytes are sent.
                    data = buffer.bytes();
                    if position == 0 && Header::decode(data)? != record.header {
                        return Err(invalid("disk header changed"));
                    }
                    Frame::write_parts(socket, tag::DATA, generation, op, &prefix, data)?;
                    let _ = events.send(WriterEvent::Progress(id, op));
                    done += count;
                    continue;
                }
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
