use crate::{
    Key, Result,
    arena::{Arena, Extent},
    crosscache,
    disk::{self, DiskExtent, RecordData, Task},
    invalid,
    record::{self, Descriptor, Header, NamespaceId},
    tree::{Eviction, Tree},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    hash::Hasher,
    io::{self, Read},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use twox_hash::XxHash3_64;

pub const WINDOW_CREDITS: u32 = 2;

#[derive(Clone)]
pub struct Config {
    pub root: PathBuf,
    pub socket: PathBuf,
    pub memory: usize,
    pub nvme: u64,
    pub staging: usize,
    pub metrics: Option<String>,
    pub faults: disk::Faults,
    pub write_budget: u64,
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
            write_budget: 0,
        }
    }
}

#[derive(Clone)]
pub struct Location {
    pub header: Header,
    pub memory: Option<Arc<Extent>>,
    pub disk: Option<Arc<DiskExtent>>,
    pub durable: bool,
    pub writing: bool,
    pub retry: Instant,
    pub bytes: u64,
}

impl Location {
    pub fn reading(&self) -> bool {
        self.memory
            .as_ref()
            .is_some_and(|extent| Arc::strong_count(extent) > 1)
            || self
                .disk
                .as_ref()
                .is_some_and(|extent| Arc::strong_count(extent) > 1)
    }
}

pub struct Namespace {
    pub directory: PathBuf,
    pub descriptor: Descriptor,
    pub tree: Tree<Location>,
    pub seq: u64,
    pub disk: disk::Worker,
    pub inflight: HashSet<Key>,
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

pub struct Put {
    pub header: Header,
    pub memory: Option<Arc<Extent>>,
    pub temporary: Arc<DiskExtent>,
    pub checksums: Vec<u64>,
    pub cursor: u64,
    pub window_end: u64,
    pub verified_pages: usize,
    pub page_hash: Option<XxHash3_64>,
    pub row_hash: Option<u64>,
    pub row_started: bool,
    pub committed: bool,
    pub pending: usize,
    pub staged: Option<(u64, Arc<Extent>)>,
    pub failed: bool,
    pub pins: VecDeque<Key>,
    pub replacement: bool,
    pub metadata: Option<Arc<Vec<u8>>>,
    pub commit_bytes: Vec<u8>,
    pub finalizing: bool,
}

pub struct Lease {
    pub nodes: VecDeque<Key>,
    pub cancelled: Arc<AtomicBool>,
}

pub enum Operation {
    Put(Box<Put>),
    Get(Lease),
}

pub struct Open {
    pub operation: Operation,
    pub progress: Instant,
}

pub struct TokenBucket {
    rate: u64,
    burst: u64,
    balance: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: u64, now: Instant) -> Self {
        let burst = rate.max(1024 * 1024);
        Self {
            rate,
            burst,
            balance: burst as f64,
            last: now,
        }
    }
    fn refill(&mut self, now: Instant) {
        if self.rate == 0 {
            self.balance = self.burst as f64;
            return;
        }
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.balance = (self.balance + elapsed * self.rate as f64).min(self.burst as f64);
    }
    fn admit(&mut self, charge: u64, now: Instant) -> bool {
        if self.rate == 0 {
            return true;
        }
        self.refill(now);
        if self.balance < 0.0 {
            return false;
        }
        self.balance -= charge as f64;
        true
    }
    pub fn balance(&self) -> i64 {
        self.balance as i64
    }
}

pub struct Session {
    pub generation: u64,
    pub namespace: Option<NamespaceId>,
    pub ops: HashMap<u64, Open>,
    pub armed: u64,
    pub budget: TokenBucket,
}

pub enum Job {
    Write {
        namespace: NamespaceId,
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
        namespace: NamespaceId,
        key: Key,
        bytes: u64,
    },
}

#[derive(Default)]
pub struct Metrics {
    pub dropped: u64,
    pub cancels: u64,
    pub puts_ok: u64,
    pub puts_error: u64,
    pub present: u64,
    pub retry_queue: u64,
    pub retry_budget: u64,
    pub retry_inflight: u64,
    pub oversize: u64,
    pub gets_ok: u64,
    pub gets_error: u64,
    pub evictions: u64,
    pub compactions: u64,
    pub nvme_bytes: u64,
    pub nvme_seconds: f64,
    pub nvme_errors: u64,
    pub hits: HashMap<(String, &'static str), u64>,
}

#[derive(Clone, Debug)]
pub struct EntryData {
    pub key: Key,
    pub parent: Option<Key>,
    pub bucket: [u8; 16],
    pub tail_pages: u32,
    pub row: bool,
    pub tier: u8,
    pub durable: bool,
    pub bytes: u64,
}

pub enum StoreEvent {
    Catalog {
        namespace: NamespaceId,
        seq: u64,
        kind: u8,
        key: Key,
        entry: Option<EntryData>,
    },
    Room {
        connection: u64,
    },
    PutResult {
        connection: u64,
        op: u64,
        result: Result<()>,
    },
}

pub struct ReadRecord {
    pub header: Header,
    pub memory: Option<Arc<Extent>>,
    pub disk: Option<Arc<DiskExtent>>,
    pub ranges: Vec<(u64, usize)>,
    pub row: bool,
    pub file: Option<Arc<File>>,
    pub fill: Option<Extent>,
    pub reader: disk::Reader,
}

pub struct GetPlan {
    pub keys: Vec<Key>,
    pub records: Vec<ReadRecord>,
}

pub struct GetRequest {
    pub keys: Vec<Key>,
    pub skip_pages: u32,
    pub expected_pages: u32,
    pub row: bool,
}

pub struct PutRequest {
    pub key: Key,
    pub published_parent: Option<Key>,
    pub bucket: [u8; 16],
    pub chain_pages: u32,
    pub published_tail_pages: u32,
    pub page_bytes: u64,
    pub row_bytes: u64,
    pub row: bool,
}

pub enum PutBeginOutcome {
    Accept {
        first_page: u32,
        parent: Option<Key>,
    },
    Present,
    Retry(u8),
    Oversize,
}

pub enum GetBeginOutcome {
    Plan(GetPlan),
    Error(io::Error),
}

pub enum PutFrameOutcome {
    Ok,
    Blocked,
    Error(io::Error),
}

pub enum DispatchOutcome {
    Put {
        connection: u64,
        op: u64,
        outcome: Result<PutBeginOutcome>,
    },
    Get {
        connection: u64,
        op: u64,
        outcome: GetBeginOutcome,
    },
}

struct PendingPut {
    connection: u64,
    op: u64,
    request: PutRequest,
}

struct PendingGet {
    connection: u64,
    op: u64,
    request: GetRequest,
}

pub struct Store {
    config: Config,
    lock: Arc<disk::RootLock>,
    memory: Arena,
    staging: Arena,
    namespaces: HashMap<NamespaceId, Namespace>,
    sessions: HashMap<u64, Session>,
    cleanup: VecDeque<(NamespaceId, VecDeque<Key>)>,
    jobs: HashMap<u64, Job>,
    job_counter: u64,
    clock: u64,
    boot: u64,
    counter: u64,
    metrics: Metrics,
    charged: u64,
    deferred_disk: Vec<Arc<DiskExtent>>,
    retired_puts: Vec<(NamespaceId, Key, Arc<DiskExtent>, u64)>,
    retired_records: Vec<(NamespaceId, Key, Location)>,
    events: Vec<StoreEvent>,
    pending_puts: HashMap<u64, VecDeque<(u64, PutRequest)>>,
    pending_gets: HashMap<u64, VecDeque<(u64, GetRequest)>>,
    put_order: VecDeque<u64>,
    get_order: VecDeque<u64>,
}

impl Store {
    pub fn open(config: Config) -> Result<Self> {
        if config.memory < 4096
            || config.staging < 2 * crate::protocol::MAX_DATA
            || config.nvme < 8192
        {
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
        let mut store = Self {
            config: config.clone(),
            lock,
            memory,
            staging,
            namespaces: HashMap::new(),
            sessions: HashMap::new(),
            cleanup: VecDeque::new(),
            jobs: HashMap::new(),
            job_counter: 0,
            clock: 0,
            boot: u64::from_le_bytes(nonce),
            counter: 0,
            metrics: Metrics::default(),
            charged: 0,
            deferred_disk: Vec::new(),
            retired_puts: Vec::new(),
            retired_records: Vec::new(),
            events: Vec::new(),
            pending_puts: HashMap::new(),
            pending_gets: HashMap::new(),
            put_order: VecDeque::new(),
            get_order: VecDeque::new(),
        };
        let mut recovered_age = Vec::new();
        for recovered in recovered {
            let mut namespace = store.new_namespace(recovered.descriptor);
            let id = namespace.descriptor.namespace_id();
            for (header, row, extent) in recovered.records {
                let modified = fs::metadata(&extent.path)?
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                recovered_age.push((modified, id, header.key));
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
                store.charged += bytes;
            }
            store.namespaces.insert(id, namespace);
        }
        recovered_age.sort_unstable();
        for (_, id, key) in recovered_age {
            store.clock += 1;
            let clock = store.clock;
            store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .access_at(&key, clock);
        }
        Ok(store)
    }

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

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn staging(&self) -> Arena {
        self.staging.clone()
    }

    pub fn lock(&self) -> Arc<disk::RootLock> {
        self.lock.clone()
    }

    pub fn count_dropped(&mut self) {
        self.metrics.dropped += 1;
    }

    pub fn count_cancel(&mut self) {
        self.metrics.cancels += 1;
    }

    pub fn count_put_error(&mut self) {
        self.metrics.puts_error += 1;
    }

    pub fn count_get_error(&mut self) {
        self.metrics.gets_error += 1;
    }

    pub fn namespace_id(&self) -> Option<NamespaceId> {
        self.namespaces.keys().next().copied()
    }

    pub fn session(&self, connection: u64) -> Option<&Session> {
        self.sessions.get(&connection)
    }

    pub fn session_generation(&self, connection: u64) -> Option<u64> {
        self.sessions
            .get(&connection)
            .map(|session| session.generation)
    }

    pub fn is_helloed(&self, connection: u64) -> bool {
        self.sessions.contains_key(&connection)
    }

    pub fn has_op(&self, connection: u64, op: u64) -> bool {
        self.sessions
            .get(&connection)
            .is_some_and(|session| session.ops.contains_key(&op))
    }

    pub fn budget_balance(&self, connection: u64) -> Option<i64> {
        self.sessions
            .get(&connection)
            .map(|session| session.budget.balance())
    }

    pub fn hello(
        &mut self,
        connection: u64,
        replica: String,
        pid: u32,
        descriptor: Descriptor,
    ) -> Result<u64> {
        let _ = pid;
        if replica.is_empty() || replica.len() > 256 {
            return Err(invalid("replica name"));
        }
        descriptor.validate()?;
        let id = descriptor.namespace_id();
        if let Some(stored) = self.namespaces.get(&id) {
            if stored.descriptor != descriptor {
                return Err(invalid("namespace descriptor mismatch"));
            }
        } else {
            disk::descriptor(&self.config.root, &descriptor)?;
            let namespace = self.new_namespace(descriptor);
            self.namespaces.insert(id, namespace);
        }
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| invalid("connection counter exhausted"))?;
        let generation = self.boot ^ self.counter;
        self.sessions.insert(
            connection,
            Session {
                generation,
                namespace: Some(id),
                ops: HashMap::new(),
                armed: 0,
                budget: TokenBucket::new(self.config.write_budget, Instant::now()),
            },
        );
        Ok(generation)
    }

    pub fn take_events(&mut self) -> Vec<StoreEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    fn push_event(&mut self, event: StoreEvent) {
        self.events.push(event);
    }

    fn entry(namespace: &Namespace, key: Key) -> EntryData {
        let node = &namespace.tree[&key];
        EntryData {
            key,
            parent: node.parent,
            bucket: node.bucket,
            tail_pages: node.tail_pages as u32,
            row: node.row,
            tier: if node.location.memory.is_some() { 0 } else { 1 },
            durable: node.location.durable,
            bytes: node.location.bytes,
        }
    }

    pub fn snapshot(&self, id: NamespaceId) -> Option<(u64, Vec<EntryData>)> {
        let namespace = self.namespaces.get(&id)?;
        let mut keys: Vec<Key> = namespace.tree.nodes().map(|node| node.key).collect();
        keys.sort_unstable();
        let entries = keys
            .into_iter()
            .map(|key| Self::entry(namespace, key))
            .collect();
        Some((namespace.seq, entries))
    }

    fn delta(&mut self, id: NamespaceId, kind: u8, key: Key) {
        let namespace = self.namespaces.get_mut(&id).unwrap();
        namespace.seq += 1;
        let seq = namespace.seq;
        let entry = (kind != 2).then(|| Self::entry(namespace, key));
        self.push_event(StoreEvent::Catalog {
            namespace: id,
            seq,
            kind,
            key,
            entry,
        });
    }

    pub fn submit_put(&mut self, connection: u64, op: u64, request: PutRequest) {
        let queue = self.pending_puts.entry(connection).or_default();
        let was_empty = queue.is_empty();
        queue.push_back((op, request));
        if was_empty {
            self.put_order.push_back(connection);
        }
    }

    pub fn submit_get(&mut self, connection: u64, op: u64, request: GetRequest) {
        let queue = self.pending_gets.entry(connection).or_default();
        let was_empty = queue.is_empty();
        queue.push_back((op, request));
        if was_empty {
            self.get_order.push_back(connection);
        }
    }

    fn pop_get(&mut self) -> Option<PendingGet> {
        while let Some(connection) = self.get_order.pop_front() {
            let Some(queue) = self.pending_gets.get_mut(&connection) else {
                continue;
            };
            let Some((op, request)) = queue.pop_front() else {
                self.pending_gets.remove(&connection);
                continue;
            };
            if queue.is_empty() {
                self.pending_gets.remove(&connection);
            } else {
                self.get_order.push_back(connection);
            }
            return Some(PendingGet {
                connection,
                op,
                request,
            });
        }
        None
    }

    fn pop_put(&mut self) -> Option<PendingPut> {
        while let Some(connection) = self.put_order.pop_front() {
            let Some(queue) = self.pending_puts.get_mut(&connection) else {
                continue;
            };
            let Some((op, request)) = queue.pop_front() else {
                self.pending_puts.remove(&connection);
                continue;
            };
            if queue.is_empty() {
                self.pending_puts.remove(&connection);
            } else {
                self.put_order.push_back(connection);
            }
            return Some(PendingPut {
                connection,
                op,
                request,
            });
        }
        None
    }

    pub fn dispatch(&mut self) -> Vec<DispatchOutcome> {
        let mut outcomes = Vec::new();
        for _ in 0..4 {
            let Some(pending) = self.pop_get() else {
                break;
            };
            let outcome = self.begin_get(pending.connection, pending.op, pending.request);
            outcomes.push(DispatchOutcome::Get {
                connection: pending.connection,
                op: pending.op,
                outcome,
            });
        }
        if let Some(pending) = self.pop_put() {
            let outcome = self.begin_put(pending.connection, pending.op, pending.request);
            outcomes.push(DispatchOutcome::Put {
                connection: pending.connection,
                op: pending.op,
                outcome,
            });
        }
        outcomes
    }

    fn begin_put(
        &mut self,
        connection: u64,
        op: u64,
        request: PutRequest,
    ) -> Result<PutBeginOutcome> {
        let id = self
            .sessions
            .get(&connection)
            .and_then(|session| session.namespace)
            .ok_or_else(|| invalid("Put before Hello"))?;
        let namespace = &self.namespaces[&id];
        let descriptor = &namespace.descriptor;
        if request.published_tail_pages > request.chain_pages
            || (request.published_parent.is_none()
                && request.published_tail_pages != request.chain_pages)
            || descriptor.page_bytes() != request.page_bytes
            || descriptor.row_bytes() != request.row_bytes
            || request.key.consumed_tokens == 0
            || request.key.consumed_tokens / descriptor.page_tokens
                != u64::from(request.chain_pages)
            || request.published_parent.is_some_and(|parent| {
                parent.consumed_tokens >= request.key.consumed_tokens
                    || request.key.consumed_tokens / descriptor.page_tokens
                        - parent.consumed_tokens / descriptor.page_tokens
                        != u64::from(request.published_tail_pages)
            })
        {
            return Err(invalid("Put geometry or chain"));
        }
        let old = namespace.tree.node(&request.key);
        if namespace.inflight.contains(&request.key)
            || old.is_some_and(|node| !node.row && node.pinned())
        {
            self.retry(connection, 2);
            return Ok(PutBeginOutcome::Retry(2));
        }
        if old.is_some_and(|node| node.row) {
            self.metrics.present += 1;
            return Ok(PutBeginOutcome::Present);
        }
        if self.sessions[&connection]
            .ops
            .values()
            .filter(|open| matches!(open.operation, Operation::Put(_)))
            .count()
            >= 4
        {
            self.retry(connection, 0);
            return Ok(PutBeginOutcome::Retry(0));
        }
        let replacement = old.is_some();
        let parent = match old {
            Some(node) => node.parent,
            None => request
                .published_parent
                .filter(|parent| namespace.tree.node(parent).is_some_and(|node| node.row)),
        };
        let first = parent.map_or(0, |parent| parent.consumed_tokens / descriptor.page_tokens);
        let tail = request.chain_pages - first as u32;
        let header = Header::new(descriptor, request.key, parent, request.bucket, tail)?;
        let bytes = disk::ENVELOPE_BYTES + header.length_with_row();
        if bytes > self.config.nvme {
            self.metrics.oversize += 1;
            return Ok(PutBeginOutcome::Oversize);
        }
        let pins: VecDeque<Key> = parent
            .into_iter()
            .chain(replacement.then_some(request.key))
            .collect();
        for key in &pins {
            self.namespaces.get_mut(&id).unwrap().tree.pin(key)?;
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
                self.namespaces.get_mut(&id).unwrap().tree.unpin(key)?;
            }
            self.arm(connection, bytes);
            self.retry(connection, 1);
            return Ok(PutBeginOutcome::Retry(1));
        }
        if !self.admit(connection, bytes) {
            for key in &pins {
                self.namespaces.get_mut(&id).unwrap().tree.unpin(key)?;
            }
            drop(memory);
            self.arm(connection, bytes);
            self.retry(connection, 1);
            return Ok(PutBeginOutcome::Retry(1));
        }
        let cursor = header.page_segment;
        self.charged += bytes;
        let directory = &self.namespaces[&id].directory;
        self.job_counter += 1;
        let temporary = Arc::new(DiskExtent::new(directory.join(format!(
            ".{}-{}.tmp",
            record::file_name(&request.key),
            self.job_counter
        ))));
        temporary.retire();
        let namespace = self.namespaces.get_mut(&id).unwrap();
        namespace.inflight.insert(request.key);
        self.sessions.get_mut(&connection).unwrap().ops.insert(
            op,
            Open {
                progress: Instant::now(),
                operation: Operation::Put(Box::new(Put {
                    header: header.clone(),
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
                    staged: None,
                    failed: false,
                    pins,
                    replacement,
                    metadata: None,
                    commit_bytes: Vec::new(),
                    finalizing: false,
                })),
            },
        );
        Ok(PutBeginOutcome::Accept {
            first_page: first as u32,
            parent,
        })
    }

    fn admit(&mut self, connection: u64, bytes: u64) -> bool {
        let now = Instant::now();
        self.sessions
            .get_mut(&connection)
            .unwrap()
            .budget
            .admit(bytes, now)
    }

    fn arm(&mut self, connection: u64, bytes: u64) {
        let session = self.sessions.get_mut(&connection).unwrap();
        session.armed = session.armed.max(bytes);
    }

    fn retry(&mut self, _connection: u64, reason: u8) {
        match reason {
            0 => self.metrics.retry_queue += 1,
            1 => self.metrics.retry_budget += 1,
            _ => self.metrics.retry_inflight += 1,
        }
    }

    fn oldest(
        &self,
        eligible: impl Fn(&crate::tree::Node<Location>) -> bool,
    ) -> Option<(NamespaceId, Key)> {
        self.namespaces
            .iter()
            .flat_map(|(id, namespace)| {
                namespace
                    .tree
                    .nodes()
                    .filter(|node| {
                        !node.pinned()
                            && node.location.durable
                            && !node.location.writing
                            && eligible(node)
                    })
                    .map(move |node| (node.accessed(), node.key, *id))
            })
            .min_by_key(|(accessed, key, _)| (*accessed, key.consumed_tokens))
            .map(|(_, key, id)| (id, key))
    }

    fn memory_room(&mut self, bytes: usize) -> Option<Extent> {
        loop {
            if let Some(extent) = self.memory.allocate(bytes) {
                return Some(extent);
            }
            let (id, key) = self.oldest(|node| {
                node.location
                    .memory
                    .as_ref()
                    .is_some_and(|data| Arc::strong_count(data) == 1)
            })?;
            self.namespaces.get_mut(&id).unwrap().tree[&key]
                .location
                .memory = None;
            self.delta(id, 1, key);
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
            let Some((id, key)) = candidate else {
                return false;
            };
            let namespace = self.namespaces.get_mut(&id).unwrap();
            match namespace.tree.evict(true, |node| node.key == key) {
                Some(Eviction::Leaves(nodes)) => {
                    for node in nodes {
                        self.metrics.evictions += 1;
                        if let Some(extent) = node.location.disk {
                            extent.retire();
                            self.deferred_disk.push(extent);
                        }
                        self.charged -= node.location.bytes;
                        self.delta(id, 2, node.key);
                    }
                }
                Some(Eviction::Compacted(key)) => {
                    let node = &self.namespaces[&id].tree[&key];
                    let source = node.location.disk.as_ref().unwrap().clone();
                    if source.reader().is_err() {
                        self.namespaces.get_mut(&id).unwrap().tree[&key].row = true;
                        return false;
                    }
                    let header = node.location.header.clone();
                    let freed = node.location.bytes - disk::ENVELOPE_BYTES - header.row_segment;
                    self.job_counter += 1;
                    let job = self.job_counter;
                    let temporary = Arc::new(DiskExtent::new(
                        source.path.with_file_name(format!(".compact-{job}.tmp")),
                    ));
                    temporary.retire();
                    let task = Task::Compact {
                        id: job,
                        source,
                        temporary,
                        header,
                    };
                    match self.namespaces[&id].disk.sender.try_send(task) {
                        Ok(()) => {
                            self.jobs.insert(
                                job,
                                Job::Compact {
                                    namespace: id,
                                    key,
                                    bytes: freed,
                                },
                            );
                            let node = &mut self.namespaces.get_mut(&id).unwrap().tree[&key];
                            node.location.writing = true;
                            self.namespaces
                                .get_mut(&id)
                                .unwrap()
                                .tree
                                .pin(&key)
                                .unwrap();
                            self.metrics.compactions += 1;
                            self.delta(id, 1, key);
                        }
                        Err(_) => {
                            self.namespaces.get_mut(&id).unwrap().tree[&key].row = true;
                            return false;
                        }
                    }
                    return false;
                }
                None => return false,
            }
        }
        true
    }

    pub fn put_frame(
        &mut self,
        connection: u64,
        op: u64,
        input: PutFrameInput<'_>,
    ) -> PutFrameOutcome {
        let Some(mut open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.remove(&op))
        else {
            return PutFrameOutcome::Error(invalid("unknown Put"));
        };
        if !matches!(open.operation, Operation::Put(_)) {
            self.sessions
                .get_mut(&connection)
                .unwrap()
                .ops
                .insert(op, open);
            return PutFrameOutcome::Error(invalid("Put frame on Get"));
        }
        let Operation::Put(mut put) = open.operation else {
            unreachable!()
        };
        let id = self.sessions[&connection].namespace.unwrap();
        let result = self.apply_put_frame(connection, op, id, &mut put, input);
        open.operation = Operation::Put(put);
        open.progress = Instant::now();
        let publish =
            matches!(&open.operation, Operation::Put(put) if put.committed && put.memory.is_some());
        self.sessions
            .get_mut(&connection)
            .unwrap()
            .ops
            .insert(op, open);
        if publish {
            return match self.publish(connection, op, false) {
                Ok(()) => PutFrameOutcome::Ok,
                Err(error) => PutFrameOutcome::Error(error),
            };
        }
        result
    }

    fn apply_put_frame(
        &mut self,
        connection: u64,
        op: u64,
        id: NamespaceId,
        put: &mut Put,
        input: PutFrameInput<'_>,
    ) -> PutFrameOutcome {
        match input {
            PutFrameInput::Window { index, pages } => {
                if put.row_started || put.cursor != put.window_end {
                    return PutFrameOutcome::Error(invalid("overlapping window"));
                }
                if pages.is_empty()
                    || pages.len() > 64
                    || index != put.checksums.len() as u32
                    || index
                        .checked_add(pages.len() as u32)
                        .is_none_or(|end| end > put.header.tail_pages)
                {
                    return PutFrameOutcome::Error(invalid("window pages or index"));
                }
                let count = pages.len() as u64;
                put.checksums.extend(pages);
                put.window_end = put.cursor + count * put.header.page_record_bytes();
                put.page_hash = None;
                PutFrameOutcome::Ok
            }
            PutFrameInput::Row { checksum } => {
                if put.row_started
                    || put.cursor != put.header.row_segment
                    || put.cursor != put.window_end
                    || put.checksums.len() != put.header.tail_pages as usize
                {
                    return PutFrameOutcome::Error(invalid("row order"));
                }
                put.row_hash = Some(checksum);
                put.row_started = true;
                put.window_end = put.header.length_with_row();
                put.page_hash = None;
                PutFrameOutcome::Ok
            }
            PutFrameInput::Data { offset, data } => {
                let count = data.len();
                if count == 0
                    || count > crate::protocol::MAX_DATA
                    || count % 4096 != 0
                    || offset != put.cursor
                    || offset
                        .checked_add(count as u64)
                        .is_none_or(|end| end > put.window_end)
                {
                    return PutFrameOutcome::Error(invalid("Data offset or length"));
                }
                if let Some(memory) = &mut put.memory {
                    Arc::get_mut(memory).unwrap().bytes_mut()
                        [offset as usize..offset as usize + count]
                        .copy_from_slice(data);
                } else {
                    if !self.namespaces[&id].parent_durable(put.header.parent) {
                        return PutFrameOutcome::Blocked;
                    }
                    let (job, extent) = match put.staged.take() {
                        Some(staged) => staged,
                        None => {
                            let Some(mut extent) = self.staging.allocate(count) else {
                                return PutFrameOutcome::Blocked;
                            };
                            extent.bytes_mut().copy_from_slice(data);
                            self.job_counter += 1;
                            (self.job_counter, Arc::new(extent))
                        }
                    };
                    let task = Task::Chunk {
                        id: job,
                        temporary: put.temporary.clone(),
                        offset,
                        data: extent.clone(),
                    };
                    match self.namespaces[&id].disk.sender.try_send(task) {
                        Ok(()) => {
                            put.pending += 1;
                            self.jobs.insert(job, Job::Chunk { connection, op });
                        }
                        Err(std::sync::mpsc::TrySendError::Full(_)) => {
                            put.staged = Some((job, extent));
                            return PutFrameOutcome::Blocked;
                        }
                        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                            return PutFrameOutcome::Error(invalid("disk worker stopped"));
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
                            return PutFrameOutcome::Error(invalid("checksum mismatch"));
                        }
                        if !put.row_started {
                            put.verified_pages += 1;
                        }
                        put.page_hash = None;
                    }
                }
                self.config.faults.checkpoint("data");
                PutFrameOutcome::Ok
            }
            PutFrameInput::Commit { offset, metadata } => {
                if !put.row_started || put.cursor != put.header.length_with_row() || put.failed {
                    return PutFrameOutcome::Error(invalid("short or failed payload"));
                }
                let count = metadata.len();
                if offset as usize != put.commit_bytes.len()
                    || count == 0
                    || count % 4096 != 0
                    || offset
                        .checked_add(count as u64)
                        .is_none_or(|end| end > put.header.page_segment)
                {
                    return PutFrameOutcome::Error(invalid("Commit offset or length"));
                }
                put.commit_bytes.extend_from_slice(metadata);
                if put.commit_bytes.len() < put.header.page_segment as usize {
                    return PutFrameOutcome::Ok;
                }
                let header = match Header::decode(&put.commit_bytes[..4096]) {
                    Ok(header) => header,
                    Err(error) => return PutFrameOutcome::Error(error),
                };
                let descriptor = &self.namespaces[&id].descriptor;
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
                    return PutFrameOutcome::Error(invalid("Commit header disagrees with Accept"));
                }
                let checksum_segment = &put.commit_bytes[4096..];
                for (index, hash) in put.checksums.iter().enumerate() {
                    if checksum_segment[index * 8..index * 8 + 8] != hash.to_le_bytes() {
                        return PutFrameOutcome::Error(invalid("Commit checksum table"));
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
                PutFrameOutcome::Ok
            }
        }
    }

    fn publish(&mut self, connection: u64, op: u64, durable: bool) -> Result<()> {
        let open = self
            .sessions
            .get_mut(&connection)
            .unwrap()
            .ops
            .remove(&op)
            .unwrap();
        let Operation::Put(put) = open.operation else {
            unreachable!()
        };
        let id = self.sessions[&connection].namespace.unwrap();
        let key = put.header.key;
        let namespace = self.namespaces.get_mut(&id).unwrap();
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
            self.retired_records.push((id, key, old));
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
        let clock = self.clock;
        namespace.tree.access_at(&key, clock);
        self.metrics.puts_ok += 1;
        self.delta(id, if put.replacement { 1 } else { 0 }, key);
        self.push_event(StoreEvent::PutResult {
            connection,
            op,
            result: Ok(()),
        });
        Ok(())
    }

    fn begin_get(&mut self, connection: u64, op: u64, request: GetRequest) -> GetBeginOutcome {
        let Some(id) = self
            .sessions
            .get(&connection)
            .and_then(|session| session.namespace)
        else {
            self.metrics.gets_error += 1;
            return GetBeginOutcome::Error(invalid("Get before Hello"));
        };
        let parsed = (|| {
            if self.sessions[&connection]
                .ops
                .values()
                .filter(|open| matches!(open.operation, Operation::Get(_)))
                .count()
                >= 4
            {
                return Err(invalid("Get queue full"));
            }
            if request.keys.is_empty() || request.keys.len() > 65536 {
                return Err(invalid("Get node count"));
            }
            if !request.row {
                return Err(invalid("Get requires endpoint row"));
            }
            let namespace = &self.namespaces[&id];
            let mut total = 0u64;
            let mut records = Vec::new();
            let reader = namespace.disk.reader();
            for (index, key) in request.keys.iter().enumerate() {
                let node = namespace
                    .tree
                    .node(key)
                    .ok_or_else(|| invalid("Get key missing"))?;
                if (index > 0 && node.parent != Some(request.keys[index - 1]))
                    || (index + 1 == request.keys.len() && !node.row)
                {
                    return Err(invalid("Get chain"));
                }
                if index == 0 && request.skip_pages as usize > node.tail_pages {
                    return Err(invalid("Get skip"));
                }
                total += node.tail_pages as u64;
                let header = node.location.header.clone();
                let skipped = if index == 0 {
                    u64::from(request.skip_pages)
                } else {
                    0
                };
                let mut ranges = vec![(0, header.page_segment as usize)];
                let start = header.page_segment + skipped * header.page_record_bytes();
                if start < header.row_segment {
                    ranges.push((start, (header.row_segment - start) as usize));
                }
                if index + 1 == request.keys.len() {
                    ranges.push((header.row_segment, header.row_record_bytes() as usize));
                }
                records.push(ReadRecord {
                    memory: node.location.memory.clone(),
                    disk: node.location.disk.clone(),
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
                    reader: reader.clone(),
                });
            }
            if total - u64::from(request.skip_pages) != u64::from(request.expected_pages) {
                return Err(invalid("Get page count"));
            }
            Ok((request.keys, records))
        })();
        let (keys, mut records) = match parsed {
            Ok(value) => value,
            Err(error) => {
                self.metrics.gets_error += 1;
                return GetBeginOutcome::Error(error);
            }
        };
        let namespace = self.namespaces.get_mut(&id).unwrap();
        for key in &keys {
            namespace.tree.pin(key).unwrap();
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
                    record::hex(&id),
                    if record.memory.is_some() {
                        "memory"
                    } else {
                        "disk"
                    },
                ))
                .or_default() += 1;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        self.sessions.get_mut(&connection).unwrap().ops.insert(
            op,
            Open {
                progress: Instant::now(),
                operation: Operation::Get(Lease {
                    nodes: keys.clone().into(),
                    cancelled,
                }),
            },
        );
        GetBeginOutcome::Plan(GetPlan { keys, records })
    }

    pub fn get_cancelled(&self, connection: u64, op: u64) -> Option<Arc<AtomicBool>> {
        match &self.sessions.get(&connection)?.ops.get(&op)?.operation {
            Operation::Get(lease) => Some(lease.cancelled.clone()),
            Operation::Put(_) => None,
        }
    }

    pub fn cancel(&mut self, connection: u64, op: u64) {
        let Some(session) = self.sessions.get_mut(&connection) else {
            return;
        };
        let Some(open) = session.ops.remove(&op) else {
            return;
        };
        let id = session.namespace.unwrap();
        match open.operation {
            Operation::Put(put) => {
                self.cleanup.push_back((id, put.pins));
                self.retired_puts.push((
                    id,
                    put.header.key,
                    put.temporary.clone(),
                    disk::ENVELOPE_BYTES + put.header.length_with_row(),
                ));
            }
            Operation::Get(lease) => {
                lease.cancelled.store(true, Ordering::Release);
                self.cleanup.push_back((id, lease.nodes));
            }
        }
    }

    pub fn connection_closed(&mut self, connection: u64) {
        self.pending_gets.remove(&connection);
        self.pending_puts.remove(&connection);
        self.get_order.retain(|id| *id != connection);
        self.put_order.retain(|id| *id != connection);
        let ops = match self.sessions.get(&connection) {
            Some(session) => session.ops.keys().copied().collect::<Vec<_>>(),
            None => return,
        };
        for op in ops {
            self.cancel(connection, op);
        }
        self.sessions.remove(&connection);
    }

    pub fn expired(&self, connection: u64, timeout: Duration) -> Vec<u64> {
        let Some(session) = self.sessions.get(&connection) else {
            return Vec::new();
        };
        session
            .ops
            .iter()
            .filter(|(_, open)| open.progress.elapsed() >= timeout)
            .map(|(op, _)| *op)
            .collect()
    }

    pub fn progress(&mut self, connection: u64, op: u64) {
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&op))
        {
            open.progress = Instant::now();
        }
    }

    pub fn get_filled(&mut self, connection: u64, key: Key, header: &Header, extent: Arc<Extent>) {
        let Some(id) = self
            .sessions
            .get(&connection)
            .and_then(|session| session.namespace)
        else {
            return;
        };
        let node = self.namespaces.get_mut(&id).unwrap().tree.node_mut(&key);
        if let Some(node) = node {
            if node.location.header == *header
                && node.location.memory.is_none()
                && node.location.durable
                && extent.bytes().len() as u64 == node.location.header.length(node.row)
            {
                node.location.memory = Some(extent);
                self.delta(id, 1, key);
            }
        }
    }

    pub fn get_finished(&mut self, connection: u64, op: u64, success: bool) {
        if self.has_op(connection, op) {
            if success {
                self.metrics.gets_ok += 1;
            } else {
                self.metrics.gets_error += 1;
            }
            self.cancel(connection, op);
        }
    }

    pub fn cleanup_turn(&mut self) {
        let mut budget = 16;
        while budget > 0 {
            let Some((id, mut keys)) = self.cleanup.pop_front() else {
                break;
            };
            let namespace = self.namespaces.get_mut(&id).unwrap();
            while budget > 0 {
                let Some(key) = keys.pop_front() else {
                    break;
                };
                namespace.tree.unpin(&key).unwrap();
                budget -= 1;
            }
            if !keys.is_empty() {
                self.cleanup.push_back((id, keys));
            }
        }
        self.deferred_disk
            .retain(|extent| Arc::strong_count(extent) > 1);
        self.retired_puts.retain(|(id, key, extent, bytes)| {
            if Arc::strong_count(extent) > 1 {
                true
            } else {
                self.charged -= bytes;
                self.namespaces.get_mut(id).unwrap().inflight.remove(key);
                false
            }
        });
        self.retired_records.retain(|(id, key, location)| {
            let replaced_on_disk = self.namespaces[id]
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

    pub fn disk_events(&mut self) {
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
                    let state = self.namespaces.get_mut(&namespace).unwrap();
                    let path = state.path(key);
                    let node = &mut state.tree[&key];
                    node.location.writing = false;
                    if completion.result.is_ok() {
                        node.location.durable = true;
                        node.location.disk = Some(Arc::new(DiskExtent::new(path)));
                        self.delta(namespace, 1, key);
                    } else {
                        node.location.retry = Instant::now() + Duration::from_millis(100);
                    }
                }
                Job::Chunk { connection, op } => {
                    if let Some(open) = self
                        .sessions
                        .get_mut(&connection)
                        .and_then(|session| session.ops.get_mut(&op))
                    {
                        if let Operation::Put(put) = &mut open.operation {
                            put.pending -= 1;
                            put.failed |= completion.result.is_err();
                            open.progress = Instant::now();
                        }
                    }
                }
                Job::Finish {
                    connection,
                    op,
                    final_extent,
                } => {
                    if self.has_op(connection, op) {
                        if completion.result.is_ok() {
                            final_extent.keep();
                            if let Err(error) = self.publish(connection, op, true) {
                                let _ = error;
                                self.cancel(connection, op);
                                self.push_event(StoreEvent::PutResult {
                                    connection,
                                    op,
                                    result: Err(invalid("streaming publish failed")),
                                });
                            }
                        } else {
                            self.metrics.puts_error += 1;
                            self.cancel(connection, op);
                            self.push_event(StoreEvent::PutResult {
                                connection,
                                op,
                                result: Err(invalid("streaming disk write failed")),
                            });
                        }
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
                        node.location.memory = None;
                        node.location.disk = Some(Arc::new(DiskExtent::new(path)));
                        self.delta(namespace, 1, key);
                    } else {
                        node.row = true;
                        self.delta(namespace, 1, key);
                    }
                }
            }
        }
    }

    pub fn schedule(&mut self) {
        let ids: Vec<NamespaceId> = self.namespaces.keys().copied().collect();
        for id in ids {
            let namespace = &self.namespaces[&id];
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
                let job = self.job_counter;
                let namespace = &self.namespaces[&id];
                let node = &namespace.tree[&key];
                let header = node.location.header.clone();
                let temporary = namespace.temporary(job);
                let task = Task::Publish {
                    id: job,
                    temporary,
                    destination: Arc::new(DiskExtent::new(namespace.path(key))),
                    data: RecordData::Memory(node.location.memory.as_ref().unwrap().clone()),
                    header,
                    row: node.row,
                };
                if namespace.disk.sender.try_send(task).is_ok() {
                    self.jobs.insert(job, Job::Write { namespace: id, key });
                    self.namespaces.get_mut(&id).unwrap().tree[&key]
                        .location
                        .writing = true;
                } else {
                    break;
                }
            }
        }
        let connections: Vec<u64> = self.sessions.keys().copied().collect();
        for connection in connections {
            let id = self.sessions[&connection].namespace.unwrap();
            let ops: Vec<u64> = self.sessions[&connection]
                .ops
                .iter()
                .filter(|(_, open)| matches!(&open.operation, Operation::Put(put) if put.committed && put.memory.is_none() && put.pending == 0 && !put.finalizing))
                .map(|(op, _)| *op)
                .collect();
            for op in ops {
                let open = self
                    .sessions
                    .get_mut(&connection)
                    .unwrap()
                    .ops
                    .get_mut(&op)
                    .unwrap();
                let Operation::Put(put) = &mut open.operation else {
                    unreachable!()
                };
                if put.failed {
                    self.metrics.puts_error += 1;
                    self.cancel(connection, op);
                    self.push_event(StoreEvent::PutResult {
                        connection,
                        op,
                        result: Err(invalid("streaming disk write failed")),
                    });
                    continue;
                }
                if !self.namespaces[&id].parent_durable(put.header.parent) {
                    continue;
                }
                self.job_counter += 1;
                let job = self.job_counter;
                let final_extent =
                    Arc::new(DiskExtent::new(self.namespaces[&id].path(put.header.key)));
                let task = Task::Publish {
                    id: job,
                    temporary: put.temporary.clone(),
                    destination: final_extent.clone(),
                    data: RecordData::Metadata(put.metadata.as_ref().unwrap().clone()),
                    header: put.header.clone(),
                    row: true,
                };
                if self.namespaces[&id].disk.sender.try_send(task).is_ok() {
                    put.finalizing = true;
                    self.jobs.insert(
                        job,
                        Job::Finish {
                            connection,
                            op,
                            final_extent,
                        },
                    );
                }
            }
        }
    }

    pub fn tick(&mut self) {
        self.tick_at(Instant::now());
    }

    pub fn tick_at(&mut self, now: Instant) {
        for session in self.sessions.values_mut() {
            session.budget.refill(now);
        }
        self.room();
    }

    pub fn room(&mut self) {
        let largest = self
            .sessions
            .values()
            .map(|session| session.armed)
            .max()
            .unwrap_or(0);
        if largest > 0 {
            self.make_room(largest);
        }
        let mut ready = Vec::new();
        for (connection, session) in &self.sessions {
            if session.armed == 0 {
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
            let capacity = self.charged.saturating_add(session.armed) <= self.config.nvme
                && (session.armed > self.memory.capacity() as u64
                    || memory_available as u64
                        >= session
                            .armed
                            .saturating_sub(disk::ENVELOPE_BYTES)
                            .next_power_of_two());
            if capacity && session.budget.balance() >= 0 {
                ready.push(*connection);
            }
        }
        for connection in ready {
            self.sessions.get_mut(&connection).unwrap().armed = 0;
            self.push_event(StoreEvent::Room { connection });
        }
    }

    pub fn metrics_text(&self) -> String {
        let mut text = String::new();
        for (name, value) in [
            ("unknown_frames_total", self.metrics.dropped),
            ("cancels_total", self.metrics.cancels),
            ("evictions_total", self.metrics.evictions),
            ("compactions_total", self.metrics.compactions),
            ("nvme_bytes_total", self.metrics.nvme_bytes),
            ("nvme_errors_total", self.metrics.nvme_errors),
            ("connections", self.sessions.len() as u64),
            (
                "leases",
                self.sessions
                    .values()
                    .flat_map(|session| session.ops.values())
                    .filter(|open| matches!(open.operation, Operation::Get(_)))
                    .count() as u64,
            ),
            (
                "open_ops",
                self.sessions
                    .values()
                    .map(|session| session.ops.len() as u64)
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
        let mut ids: Vec<&NamespaceId> = self.namespaces.keys().collect();
        ids.sort_unstable();
        for id in ids {
            let name = record::hex(id);
            let namespace = &self.namespaces[id];
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

pub enum PutFrameInput<'a> {
    Window { index: u32, pages: Vec<u64> },
    Row { checksum: u64 },
    Data { offset: u64, data: &'a [u8] },
    Commit { offset: u64, metadata: &'a [u8] },
}

pub type Reply<T> = tokio::sync::oneshot::Sender<T>;

pub enum Transport {
    Stream,
    Rdma,
}

pub struct PeerRegions {
    pub arena_bytes: u64,
    pub staging_bytes: u64,
    pub rdma: Vec<u8>,
}

pub struct PeerSession {
    pub generation: u64,
    pub transport: Transport,
    pub stream_addr: String,
    pub token: Vec<u8>,
    pub regions: PeerRegions,
}

pub enum PeerEvent {
    Connecting,
    Connected {
        node: String,
        hello: PeerSession,
    },
    Catalog {
        node: String,
        event: crosscache::CatalogEvent,
    },
    Disconnected {
        node: String,
    },
    Forwarded {
        key: Key,
        result: Result<()>,
    },
    Data {
        node: String,
        transfer: u64,
        result: Result<()>,
    },
}

pub struct HelloReply {
    pub generation: u64,
    pub namespace: NamespaceId,
}

pub struct PutAccept {
    pub transfer: u64,
    pub first_page: u32,
    pub parent: Option<Key>,
    pub record_bytes: u64,
}

pub struct WindowGrant {
    pub transfer: u64,
    pub index: u32,
    pub record: u32,
    pub record_offset: u64,
    pub length: u64,
    pub page_first: u64,
    pub page_count: u32,
    pub row: bool,
}

pub struct Refused {
    pub reason: u8,
    pub detail: String,
}

pub enum PutAnswer {
    Accept(PutAccept),
    Grant(WindowGrant),
    Stored { bytes: u64 },
    Refused(Refused),
}

pub enum GetAnswer {
    Leased {
        transfer: u64,
        record_bytes: Vec<u64>,
    },
    Meta {
        record: u32,
        metadata_offset: u64,
        metadata: Vec<u8>,
    },
    Ready(WindowGrant),
    Done,
    Refused(Refused),
}

pub enum StoreCommand {
    Hello {
        connection: u64,
        replica: String,
        pid: u32,
        descriptor: Descriptor,
        reply: Reply<Result<HelloReply>>,
    },
    Subscribe {
        connection: u64,
        namespace: NamespaceId,
        generation: u64,
        descriptor: Descriptor,
        events: tokio::sync::mpsc::Sender<crosscache::CatalogEvent>,
        reply: Reply<Result<()>>,
    },
    PutBegin {
        connection: u64,
        namespace: NamespaceId,
        descriptor: Descriptor,
        key: Key,
        published_parent: Option<Key>,
        bucket: [u8; 16],
        chain_pages: u32,
        published_tail_pages: u32,
        row: bool,
        events: tokio::sync::mpsc::Sender<Result<PutAnswer>>,
        reply: Reply<Result<()>>,
    },
    WindowFilled {
        connection: u64,
        transfer: u64,
        index: u32,
        page_xxh3: Vec<u64>,
        row_xxh3: Option<u64>,
        reply: Reply<Result<()>>,
    },
    PutCommit {
        connection: u64,
        transfer: u64,
        metadata_offset: u64,
        metadata: Vec<u8>,
        reply: Reply<Result<()>>,
    },
    GetBegin {
        connection: u64,
        namespace: NamespaceId,
        chain: Vec<Key>,
        skip_pages: u32,
        expected_pages: u32,
        row: bool,
        destinations: Vec<crosscache::GetDestination>,
        transfer: Option<u64>,
        events: tokio::sync::mpsc::Sender<Result<GetAnswer>>,
        reply: Reply<Result<()>>,
    },
    WindowReleased {
        connection: u64,
        transfer: u64,
        index: u32,
        reply: Reply<Result<()>>,
    },
    Cancel {
        connection: u64,
        transfer: u64,
    },
    ConnectionClosed {
        connection: u64,
    },
    ProcessExited {
        connection: u64,
    },
    PeerHello {
        connection: u64,
        node: String,
        transport: Transport,
        regions: PeerRegions,
        reply: Reply<Result<PeerSession>>,
    },
    Peer(PeerEvent),
    Tick,
}

impl Store {
    pub fn handle(&mut self, command: StoreCommand) {
        match command {
            StoreCommand::Hello {
                connection,
                replica,
                pid,
                descriptor,
                reply,
            } => {
                let result = self
                    .hello(connection, replica, pid, descriptor)
                    .map(|generation| HelloReply {
                        generation,
                        namespace: self
                            .sessions
                            .get(&connection)
                            .and_then(|session| session.namespace)
                            .unwrap(),
                    });
                let _ = reply.send(result);
            }
            StoreCommand::Subscribe {
                connection,
                namespace,
                generation,
                descriptor,
                reply,
                ..
            } => {
                let result = (|| {
                    descriptor.validate()?;
                    if descriptor.namespace_id() != namespace {
                        return Err(invalid("descriptor does not match namespace"));
                    }
                    let session = self
                        .sessions
                        .get(&connection)
                        .ok_or_else(|| invalid("Subscribe before Hello"))?;
                    if session.generation != generation || session.namespace != Some(namespace) {
                        return Err(invalid("Subscribe generation or namespace"));
                    }
                    if self.namespaces[&namespace].descriptor != descriptor {
                        return Err(invalid("namespace descriptor mismatch"));
                    }
                    Ok(())
                })();
                let _ = reply.send(result);
            }
            StoreCommand::PutBegin {
                connection,
                namespace,
                descriptor,
                reply,
                ..
            } => {
                let result = (|| {
                    if descriptor.namespace_id() != namespace
                        || self.sessions[&connection].namespace != Some(namespace)
                    {
                        return Err(invalid("PutBegin namespace"));
                    }
                    Err(invalid(
                        "the grant-based Put path is provided by the M2 region service",
                    ))
                })();
                let _ = reply.send(result);
            }
            StoreCommand::WindowFilled { reply, .. }
            | StoreCommand::PutCommit { reply, .. }
            | StoreCommand::WindowReleased { reply, .. } => {
                let _ = reply.send(Err(invalid(
                    "window grants are provided by the M2 region service",
                )));
            }
            StoreCommand::GetBegin { reply, .. } => {
                let _ = reply.send(Err(invalid(
                    "window grants are provided by the M2 region service",
                )));
            }
            StoreCommand::Cancel {
                connection,
                transfer,
            } => self.cancel(connection, transfer),
            StoreCommand::ConnectionClosed { connection }
            | StoreCommand::ProcessExited { connection } => self.connection_closed(connection),
            StoreCommand::PeerHello { reply, .. } => {
                let _ = reply.send(Err(invalid("peer support arrives in M4")));
            }
            StoreCommand::Peer(_) => {}
            StoreCommand::Tick => self.tick(),
        }
    }
}
