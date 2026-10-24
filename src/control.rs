use crate::{
    Key, Result, invalid, proto,
    proto::{
        get_message, get_reply, local_cache_server::LocalCache,
        local_cache_server::LocalCacheServer, put_message, put_reply,
    },
    record::{Descriptor, RankSizes},
    store::{self, Config, GetAnswer, NO_PROGRESS, PutAnswer, Store, StoreCommand},
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, Read, Write},
    net::TcpListener,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::Path,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, oneshot},
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream, wrappers::UnixListenerStream};
use tonic::{
    Request, Response, Status, Streaming, transport::Server, transport::server::Connected,
};

const MAX_MESSAGE: usize = 16 * 1024 * 1024;
const GENERATION: &str = "x-selene-generation";

#[derive(Clone)]
pub struct PeerInfo {
    pub connection: u64,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
}

impl Connected for PeerStream {
    type ConnectInfo = PeerInfo;

    fn connect_info(&self) -> PeerInfo {
        self.info.clone()
    }
}

pub struct PeerStream {
    inner: tokio::net::UnixStream,
    info: PeerInfo,
    commands: mpsc::Sender<StoreCommand>,
    runtime: tokio::runtime::Handle,
    generations: Arc<Mutex<HashMap<u64, Generation>>>,
    over_limit: bool,
    _slot: ConnectionSlot,
}

struct ConnectionSlot(Arc<AtomicUsize>);

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl AsyncRead for PeerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.over_limit {
            // A clean EOF closes the excess transport without failing the
            // whole incoming stream.
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.over_limit {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection limit",
            )));
        }
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl Drop for PeerStream {
    fn drop(&mut self) {
        self.generations
            .lock()
            .unwrap()
            .remove(&self.info.connection);
        let command = StoreCommand::ConnectionClosed {
            connection: self.info.connection,
        };
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let commands = self.commands.clone();
                self.runtime.spawn(async move {
                    let _ = commands.send(command).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

#[derive(Default)]
struct Generation {
    current: u64,
    issued: HashSet<u64>,
}

#[derive(Clone)]
pub struct Service {
    commands: mpsc::Sender<StoreCommand>,
    generations: Arc<Mutex<HashMap<u64, Generation>>>,
    transfers: Arc<AtomicU64>,
    epochs: Arc<Mutex<HashMap<u64, Arc<AtomicU64>>>>,
}

impl Service {
    fn epoch(&self, connection: u64) -> Arc<AtomicU64> {
        let mut epochs = self.epochs.lock().unwrap();
        epochs
            .entry(connection)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }
}

pub fn key_to_proto(key: &Key) -> proto::Key {
    proto::Key {
        consumed_tokens: key.consumed_tokens,
        digest: key.digest.to_vec(),
    }
}

fn key_from_proto(key: Option<proto::Key>) -> std::result::Result<Key, Status> {
    let key = key.ok_or_else(|| Status::invalid_argument("missing key"))?;
    let digest: [u8; 16] = key
        .digest
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("key digest must be 16 bytes"))?;
    if key.consumed_tokens == 0 {
        return Err(Status::invalid_argument("zero consumed_tokens"));
    }
    Ok(Key {
        consumed_tokens: key.consumed_tokens,
        digest,
    })
}

fn descriptor_from_proto(
    value: Option<proto::Descriptor>,
) -> std::result::Result<Descriptor, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("missing descriptor"))?;
    let identity: [u8; 32] = value
        .identity
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("descriptor identity must be 32 bytes"))?;
    let fingerprint: [u8; 16] = value
        .fingerprint
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("descriptor fingerprint must be 16 bytes"))?;
    let ranks = value
        .ranks
        .into_iter()
        .map(|rank| RankSizes {
            planes: rank.plane_bytes,
            row_bytes: rank.row_bytes,
        })
        .collect();
    let descriptor = Descriptor {
        identity,
        fingerprint,
        page_tokens: value.page_tokens,
        ranks,
        tenant: value.tenant,
    };
    descriptor
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if value.page_record_bytes != 0 && value.page_record_bytes != descriptor.page_bytes()
        || value.row_record_bytes != 0 && value.row_record_bytes != descriptor.row_bytes()
    {
        return Err(Status::invalid_argument("descriptor record sizes"));
    }
    Ok(descriptor)
}

pub fn entry_to_proto(entry: &store::EntryData) -> proto::CatalogEntry {
    proto::CatalogEntry {
        key: Some(key_to_proto(&entry.key)),
        parent: entry.parent.map(|parent| key_to_proto(&parent)),
        has_parent: entry.parent.is_some(),
        bucket: entry.bucket.to_vec(),
        tail_pages: u64::from(entry.tail_pages),
        row: entry.row,
        tier: if entry.tier == 0 {
            proto::Tier::Dram as i32
        } else {
            proto::Tier::Nvme as i32
        },
        durable: entry.durable,
        bytes: entry.bytes,
        unreadable: false,
    }
}

fn map_hello_error(error: io::Error) -> Status {
    match error.kind() {
        io::ErrorKind::AlreadyExists => Status::failed_precondition(error.to_string()),
        io::ErrorKind::WouldBlock => Status::resource_exhausted(error.to_string()),
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
            Status::invalid_argument(error.to_string())
        }
        _ => Status::internal(error.to_string()),
    }
}

fn put_reply(answer: PutAnswer) -> proto::PutReply {
    use put_reply::R;
    let r = match answer {
        PutAnswer::Accept(accept) => R::Accept(proto::PutAccept {
            transfer: accept.transfer,
            first_page: u64::from(accept.first_page),
            parent: accept.parent.map(|parent| key_to_proto(&parent)),
            has_parent: accept.parent.is_some(),
            record_bytes: accept.record_bytes,
        }),
        PutAnswer::Grant(grant) => R::Grant(window_grant(grant)),
        PutAnswer::Stored { bytes } => R::Stored(proto::Stored { bytes }),
        PutAnswer::Refused(refused) => R::Refused(proto::Refused {
            reason: i32::from(refused.reason),
            detail: refused.detail,
        }),
    };
    proto::PutReply { r: Some(r) }
}

fn get_reply(answer: GetAnswer) -> proto::GetReply {
    use get_reply::R;
    let r = match answer {
        GetAnswer::Leased {
            transfer,
            record_bytes,
        } => R::Leased(proto::GetLeased {
            transfer,
            record_bytes,
        }),
        GetAnswer::Meta {
            record,
            metadata_offset,
            metadata,
            source,
        } => R::Meta(proto::RecordMeta {
            record,
            metadata_offset,
            metadata,
            source,
        }),
        GetAnswer::Ready(grant) => R::Ready(window_grant(grant)),
        GetAnswer::Done => R::Done(proto::GetDone {}),
        GetAnswer::Refused(refused) => R::Refused(proto::Refused {
            reason: i32::from(refused.reason),
            detail: refused.detail,
        }),
    };
    proto::GetReply { r: Some(r) }
}

fn window_grant(grant: store::WindowGrant) -> proto::WindowGrant {
    proto::WindowGrant {
        transfer: grant.transfer,
        index: grant.index,
        record: grant.record,
        record_offset: grant.record_offset,
        length: grant.length,
        memory: Some(proto::WindowRef {
            region: grant.region,
            offset: grant.offset,
            length: grant.length,
        }),
        page_first: grant.page_first,
        page_count: grant.page_count,
        row: grant.row,
    }
}

fn refused(reason: proto::Reason) -> proto::PutReply {
    proto::PutReply {
        r: Some(put_reply::R::Refused(proto::Refused {
            reason: reason as i32,
            detail: String::new(),
        })),
    }
}

fn refused_get(reason: proto::Reason) -> proto::GetReply {
    proto::GetReply {
        r: Some(get_reply::R::Refused(proto::Refused {
            reason: reason as i32,
            detail: String::new(),
        })),
    }
}

impl Service {
    fn peer<T>(request: &Request<T>) -> std::result::Result<PeerInfo, Status> {
        request
            .extensions()
            .get::<PeerInfo>()
            .cloned()
            .ok_or_else(|| Status::internal("missing peer credentials"))
    }

    fn generation<T>(
        &self,
        request: &Request<T>,
        peer: &PeerInfo,
    ) -> std::result::Result<u64, Status> {
        let value = request
            .metadata()
            .get(GENERATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| Status::unauthenticated("missing generation"))?;
        let generations = self.generations.lock().unwrap();
        let Some(generation) = generations.get(&peer.connection) else {
            return Err(Status::unauthenticated("no Hello on this connection"));
        };
        if value == generation.current {
            Ok(value)
        } else if generation.issued.contains(&value) {
            Err(Status::aborted("generation superseded"))
        } else {
            Err(Status::unauthenticated("unknown generation"))
        }
    }

    async fn send(&self, command: StoreCommand) -> std::result::Result<(), Status> {
        self.commands
            .send(command)
            .await
            .map_err(|_| Status::unavailable("store stopped"))
    }
}

#[tonic::async_trait]
impl LocalCache for Service {
    type SubscribeStream = Pin<
        Box<
            dyn tokio_stream::Stream<Item = std::result::Result<proto::CatalogEvent, Status>>
                + Send,
        >,
    >;
    type PutStream = Pin<
        Box<dyn tokio_stream::Stream<Item = std::result::Result<proto::PutReply, Status>> + Send>,
    >;
    type GetStream = Pin<
        Box<dyn tokio_stream::Stream<Item = std::result::Result<proto::GetReply, Status>> + Send>,
    >;

    async fn hello(
        &self,
        request: Request<proto::HelloRequest>,
    ) -> std::result::Result<Response<proto::HelloReply>, Status> {
        let peer = Self::peer(&request)?;
        let message = request.into_inner();
        if message.protocol != 1 {
            return Err(Status::failed_precondition("protocol mismatch"));
        }
        let descriptor = descriptor_from_proto(message.descriptor)?;
        let (reply, answer) = oneshot::channel();
        self.send(StoreCommand::Hello {
            connection: peer.connection,
            replica: message.replica,
            pid: peer.pid,
            descriptor,
            reply,
        })
        .await?;
        let answer = answer
            .await
            .map_err(|_| Status::internal("store dropped Hello"))?
            .map_err(map_hello_error)?;
        {
            let mut generations = self.generations.lock().unwrap();
            let generation = generations.entry(peer.connection).or_default();
            if generation.current != 0 {
                generation.issued.insert(generation.current);
            }
            generation.current = answer.generation;
            generation.issued.insert(answer.generation);
        }
        Ok(Response::new(proto::HelloReply {
            namespace_id: answer.namespace.to_vec(),
            generation: answer.generation,
            arena_path: answer.arena_path,
            arena_bytes: answer.arena_bytes,
            staging_path: answer.staging_path,
            staging_bytes: answer.staging_bytes,
            max_open_puts: store::OPEN_PUTS as u32,
            max_open_gets: store::OPEN_GETS as u32,
            max_window_bytes: answer.max_window_bytes,
            window_credits: store::WINDOW_CREDITS,
        }))
    }

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        let peer = Self::peer(&request)?;
        let generation = self.generation(&request, &peer)?;
        let message = request.into_inner();
        let namespace: [u8; 32] = message
            .namespace_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("namespace id must be 32 bytes"))?;
        if message.generation != generation {
            return Err(Status::unauthenticated("generation mismatch"));
        }
        let descriptor = descriptor_from_proto(message.descriptor)?;
        let epoch = self.epoch(peer.connection);
        let this_epoch = epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let (events, receiver) = mpsc::channel(65_536);
        let (reply, answer) = oneshot::channel();
        self.send(StoreCommand::Subscribe {
            connection: peer.connection,
            namespace,
            generation,
            descriptor,
            events,
            reply,
        })
        .await?;
        answer
            .await
            .map_err(|_| Status::internal("store dropped Subscribe"))?
            .map_err(|error| match error.kind() {
                io::ErrorKind::NotConnected | io::ErrorKind::PermissionDenied => {
                    Status::unauthenticated(error.to_string())
                }
                _ => Status::failed_precondition(error.to_string()),
            })?;
        let current = epoch.clone();
        Ok(Response::new(Box::pin(
            ReceiverStream::new(receiver)
                .take_while(move |_| current.load(Ordering::Acquire) == this_epoch)
                .map(Ok::<_, Status>),
        )))
    }

    async fn locate(
        &self,
        _request: Request<proto::LocateRequest>,
    ) -> std::result::Result<Response<proto::LocateReply>, Status> {
        Err(Status::unimplemented("Locate is P2"))
    }

    async fn put(
        &self,
        request: Request<Streaming<proto::PutMessage>>,
    ) -> std::result::Result<Response<Self::PutStream>, Status> {
        let peer = Self::peer(&request)?;
        let generation = self.generation(&request, &peer)?;
        let stream = request.into_inner();
        let transfer = self.transfers.fetch_add(1, Ordering::Relaxed);
        let commands = self.commands.clone();
        let (output, receiver) = mpsc::channel(8);
        tokio::spawn(put_task(
            peer, stream, commands, transfer, generation, output,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn get(
        &self,
        request: Request<Streaming<proto::GetMessage>>,
    ) -> std::result::Result<Response<Self::GetStream>, Status> {
        let peer = Self::peer(&request)?;
        let generation = self.generation(&request, &peer)?;
        let stream = request.into_inner();
        let transfer = self.transfers.fetch_add(1, Ordering::Relaxed);
        let commands = self.commands.clone();
        let (output, receiver) = mpsc::channel(8);
        tokio::spawn(get_task(
            peer, stream, commands, transfer, generation, output,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

async fn pending_put_terminal(
    events: &mut mpsc::Receiver<std::result::Result<PutAnswer, io::Error>>,
) -> Option<PutAnswer> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(Ok(answer)))
                if matches!(answer, PutAnswer::Stored { .. } | PutAnswer::Refused(_)) =>
            {
                return Some(answer);
            }
            Ok(Some(Ok(_))) => continue,
            _ => return None,
        }
    }
}

async fn pending_get_terminal(
    events: &mut mpsc::Receiver<std::result::Result<GetAnswer, io::Error>>,
) -> Option<GetAnswer> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(Ok(answer))) if matches!(answer, GetAnswer::Done | GetAnswer::Refused(_)) => {
                return Some(answer);
            }
            Ok(Some(Ok(_))) => continue,
            _ => return None,
        }
    }
}

async fn put_task(
    peer: PeerInfo,
    mut stream: Streaming<proto::PutMessage>,
    commands: mpsc::Sender<StoreCommand>,
    transfer: u64,
    generation: u64,
    output: mpsc::Sender<std::result::Result<proto::PutReply, Status>>,
) {
    let first = match stream.message().await {
        Ok(Some(message)) => message,
        Ok(None) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
        Err(status) => {
            let _ = output.send(Err(status)).await;
            return;
        }
    };
    let begin = match first.m {
        Some(put_message::M::Begin(begin)) => begin,
        _ => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let namespace: [u8; 32] = match begin.namespace_id.as_slice().try_into() {
        Ok(namespace) => namespace,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let descriptor = match descriptor_from_proto(begin.descriptor) {
        Ok(descriptor) => descriptor,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let key = match key_from_proto(begin.key) {
        Ok(key) => key,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let bucket: [u8; 16] = match begin.bucket.as_slice().try_into() {
        Ok(bucket) => bucket,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let chain_pages = match u32::try_from(begin.chain_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let tail_pages = match u32::try_from(begin.published_tail_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let published_parent =
        if begin.has_published_parent {
            if begin.published_parent.as_ref().is_some_and(|key| {
                key.consumed_tokens == 0 && key.digest.iter().all(|byte| *byte == 0)
            }) {
                let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                return;
            }
            match key_from_proto(begin.published_parent) {
                Ok(key) => Some(key),
                Err(_) => {
                    let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                    return;
                }
            }
        } else {
            None
        };
    let (events, event_stream) = mpsc::channel(4);
    let (reply, answer) = oneshot::channel();
    if commands
        .send(StoreCommand::PutBegin {
            connection: peer.connection,
            transfer,
            generation,
            namespace,
            descriptor,
            key,
            published_parent,
            bucket,
            chain_pages,
            published_tail_pages: tail_pages,
            row: begin.row,
            events,
            reply,
        })
        .await
        .is_err()
    {
        let _ = output.send(Err(Status::unavailable("store stopped"))).await;
        return;
    }
    match answer.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let status = match error.kind() {
                io::ErrorKind::PermissionDenied => Status::aborted(error.to_string()),
                io::ErrorKind::AlreadyExists | io::ErrorKind::NotConnected => {
                    Status::failed_precondition(error.to_string())
                }
                _ => Status::invalid_argument(error.to_string()),
            };
            let _ = output.send(Err(status)).await;
            return;
        }
        Err(_) => {
            let _ = output
                .send(Err(Status::internal("store dropped PutBegin")))
                .await;
            return;
        }
    }
    let mut event_stream = event_stream;
    loop {
        tokio::select! {
            event = event_stream.recv() => {
                match event {
                    Some(Ok(answer)) => {
                        let terminal = matches!(answer, PutAnswer::Stored { .. } | PutAnswer::Refused(_));
                        if output.send(Ok(put_reply(answer))).await.is_err() {
                            return;
                        }
                        if terminal {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        let _ = output.send(Err(Status::internal(error.to_string()))).await;
                        break;
                    }
                    None => break,
                }
            }
            message = stream.message() => {
                match message {
                    Ok(Some(message)) => match message.m {
                        Some(put_message::M::Filled(filled)) => {
                            if filled.transfer != transfer {
                                let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                break;
                            }
                            let (reply, ack) = oneshot::channel();
                            if commands.send(StoreCommand::WindowFilled {
                                connection: peer.connection,
                                generation,
                                transfer,
                                index: filled.index,
                                page_xxh3: filled.page_xxh3,
                                row_xxh3: filled.has_row.then_some(filled.row_xxh3),
                                reply,
                            }).await.is_err() {
                                break;
                            }
                            let ack = ack.await;
                            if matches!(&ack, Ok(Err(error)) if error.kind() == io::ErrorKind::PermissionDenied) {
                                let _ = output.send(Err(Status::aborted("generation superseded"))).await;
                                break;
                            }
                            if !matches!(ack, Ok(Ok(()))) {
                                match pending_put_terminal(&mut event_stream).await {
                                    Some(answer) => {
                                        let _ = output.send(Ok(put_reply(answer))).await;
                                    }
                                    None => {
                                        let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                    }
                                }
                                break;
                            }
                        }
                        Some(put_message::M::Commit(commit)) => {
                            if commit.transfer != transfer {
                                let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                break;
                            }
                            let (reply, ack) = oneshot::channel();
                            if commands.send(StoreCommand::PutCommit {
                                connection: peer.connection,
                                generation,
                                transfer,
                                metadata_offset: commit.metadata_offset,
                                metadata: commit.metadata,
                                reply,
                            }).await.is_err() {
                                break;
                            }
                            let ack = ack.await;
                            if matches!(&ack, Ok(Err(error)) if error.kind() == io::ErrorKind::PermissionDenied) {
                                let _ = output.send(Err(Status::aborted("generation superseded"))).await;
                                break;
                            }
                            if !matches!(ack, Ok(Ok(()))) {
                                match pending_put_terminal(&mut event_stream).await {
                                    Some(answer) => {
                                        let _ = output.send(Ok(put_reply(answer))).await;
                                    }
                                    None => {
                                        let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                    }
                                }
                                break;
                            }
                        }
                        Some(put_message::M::Cancel(_)) => {
                            let _ = commands.send(StoreCommand::Cancel {
                                connection: peer.connection,
                                generation,
                                transfer,
                            }).await;
                            if let Ok(Some(Ok(answer))) = tokio::time::timeout(
                                Duration::from_millis(500),
                                event_stream.recv(),
                            )
                            .await
                            {
                                let _ = output.send(Ok(put_reply(answer))).await;
                            }
                            break;
                        }
                        Some(put_message::M::Begin(_)) | None => {
                            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                            let _ = commands.send(StoreCommand::Cancel {
                                connection: peer.connection,
                                generation,
                                transfer,
                            }).await;
                            break;
                        }
                    },
                    Ok(None) => {
                        let _ = commands.send(StoreCommand::Cancel {
                            connection: peer.connection,
                            generation,
                            transfer,
                        }).await;
                        break;
                    }
                    Err(_) => {
                        let _ = commands.send(StoreCommand::Cancel {
                            connection: peer.connection,
                            generation,
                            transfer,
                        }).await;
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(NO_PROGRESS) => {
                let _ = output.send(Err(Status::deadline_exceeded("no progress"))).await;
                let _ = commands.send(StoreCommand::Cancel {
                    connection: peer.connection,
                    generation,
                    transfer,
                }).await;
                break;
            }
        }
    }
}

async fn get_task(
    peer: PeerInfo,
    mut stream: Streaming<proto::GetMessage>,
    commands: mpsc::Sender<StoreCommand>,
    transfer: u64,
    generation: u64,
    output: mpsc::Sender<std::result::Result<proto::GetReply, Status>>,
) {
    let first = match stream.message().await {
        Ok(Some(message)) => message,
        Ok(None) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
        Err(status) => {
            let _ = output.send(Err(status)).await;
            return;
        }
    };
    let begin = match first.m {
        Some(get_message::M::Begin(begin)) => begin,
        _ => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    let namespace: [u8; 32] = match begin.namespace_id.as_slice().try_into() {
        Ok(namespace) => namespace,
        Err(_) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    if !begin.destinations.is_empty() || begin.transfer != 0 {
        let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
        return;
    }
    let mut chain = Vec::with_capacity(begin.chain.len());
    for key in begin.chain {
        match key_from_proto(Some(key)) {
            Ok(key) => chain.push(key),
            Err(_) => {
                let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                return;
            }
        }
    }
    if chain.is_empty() || chain.len() > store::MAX_CHAIN {
        let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
        return;
    }
    let skip_pages = match u32::try_from(begin.skip_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    let expected_pages = match u32::try_from(begin.expected_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    let (events, event_stream) = mpsc::channel(4);
    let (reply, answer) = oneshot::channel();
    if commands
        .send(StoreCommand::GetBegin {
            connection: peer.connection,
            transfer,
            generation,
            namespace,
            chain,
            skip_pages,
            expected_pages,
            row: begin.row,
            events,
            reply,
        })
        .await
        .is_err()
    {
        let _ = output.send(Err(Status::unavailable("store stopped"))).await;
        return;
    }
    match answer.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let status = match error.kind() {
                io::ErrorKind::PermissionDenied => Status::aborted(error.to_string()),
                io::ErrorKind::NotConnected => Status::unauthenticated(error.to_string()),
                _ => Status::invalid_argument(error.to_string()),
            };
            let _ = output.send(Err(status)).await;
            return;
        }
        Err(_) => {
            let _ = output
                .send(Err(Status::internal("store dropped GetBegin")))
                .await;
            return;
        }
    }
    let mut event_stream = event_stream;
    loop {
        tokio::select! {
            event = event_stream.recv() => {
                match event {
                    Some(Ok(answer)) => {
                        let terminal = matches!(answer, GetAnswer::Done | GetAnswer::Refused(_));
                        if output.send(Ok(get_reply(answer))).await.is_err() {
                            return;
                        }
                        if terminal {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        let _ = output.send(Err(Status::internal(error.to_string()))).await;
                        break;
                    }
                    None => break,
                }
            }
            message = stream.message() => {
                match message {
                    Ok(Some(message)) => match message.m {
                        Some(get_message::M::Released(released)) => {
                            if released.transfer != transfer {
                                let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                                break;
                            }
                            let (reply, ack) = oneshot::channel();
                            if commands.send(StoreCommand::WindowReleased {
                                connection: peer.connection,
                                generation,
                                transfer,
                                index: released.index,
                                reply,
                            }).await.is_err() {
                                break;
                            }
                            let ack = ack.await;
                            if matches!(&ack, Ok(Err(error)) if error.kind() == io::ErrorKind::PermissionDenied) {
                                let _ = output.send(Err(Status::aborted("generation superseded"))).await;
                                break;
                            }
                            if !matches!(ack, Ok(Ok(()))) {
                                match pending_get_terminal(&mut event_stream).await {
                                    Some(answer) => {
                                        let _ = output.send(Ok(get_reply(answer))).await;
                                    }
                                    None => {
                                        let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                                    }
                                }
                                break;
                            }
                        }
                        Some(get_message::M::Cancel(_)) => {
                            let _ = commands.send(StoreCommand::Cancel {
                                connection: peer.connection,
                                generation,
                                transfer,
                            }).await;
                            break;
                        }
                        Some(get_message::M::Begin(_)) | None => {
                            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                            let _ = commands.send(StoreCommand::Cancel {
                                connection: peer.connection,
                                generation,
                                transfer,
                            }).await;
                            break;
                        }
                    },
                    Ok(None) => {
                        let _ = commands.send(StoreCommand::Cancel {
                            connection: peer.connection,
                            generation,
                            transfer,
                        }).await;
                        break;
                    }
                    Err(_) => {
                        let _ = commands.send(StoreCommand::Cancel {
                            connection: peer.connection,
                            generation,
                            transfer,
                        }).await;
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(NO_PROGRESS) => {
                let _ = output.send(Err(Status::deadline_exceeded("no progress"))).await;
                let _ = commands.send(StoreCommand::Cancel {
                    connection: peer.connection,
                    generation,
                    transfer,
                }).await;
                break;
            }
        }
    }
}

pub fn run(config: Config, stop: Arc<AtomicBool>) -> Result<()> {
    let store = Store::open(config.clone())?;
    let directory = config
        .socket
        .parent()
        .ok_or_else(|| invalid("socket directory"))?;
    prepare_socket_directory(directory)?;
    if config.socket.exists() {
        if !fs::symlink_metadata(&config.socket)?
            .file_type()
            .is_socket()
        {
            return Err(invalid("socket path is not a socket"));
        }
        if std::os::unix::net::UnixStream::connect(&config.socket).is_ok() {
            return Err(invalid("socket is already in use"));
        }
        fs::remove_file(&config.socket)?;
    }
    let metrics_text = Arc::new(Mutex::new(String::new()));
    let owner_metrics = metrics_text.clone();
    let (commands, mut receiver) = mpsc::channel::<StoreCommand>(64);
    let owner = thread::spawn(move || {
        let mut store = store;
        'outer: while let Some(mut command) = receiver.blocking_recv() {
            // Drain the batch before dispatching so queued Gets and Puts are
            // ordered by the scheduler rather than by arrival.
            let mut batch = 64;
            loop {
                let shutdown = matches!(command, StoreCommand::Shutdown);
                store.handle(command);
                if shutdown {
                    break 'outer;
                }
                batch -= 1;
                if batch == 0 {
                    break;
                }
                match receiver.try_recv() {
                    Ok(next) => command = next,
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break 'outer,
                }
            }
            store.pump();
            *owner_metrics.lock().unwrap() = store.metrics_text();
        }
    });
    let tick_commands = commands.clone();
    let tick_stop = stop.clone();
    let tick = thread::spawn(move || {
        while !tick_stop.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(10));
            let _ = tick_commands.try_send(StoreCommand::Tick);
        }
    });
    let metrics = start_metrics(
        config.metrics.as_deref(),
        metrics_text.clone(),
        stop.clone(),
    )?;
    let generations = Arc::new(Mutex::new(HashMap::new()));
    let service = Service {
        commands: commands.clone(),
        generations: generations.clone(),
        transfers: Arc::new(AtomicU64::new(1)),
        epochs: Arc::new(Mutex::new(HashMap::new())),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let listener = std::os::unix::net::UnixListener::bind(&config.socket).map_err(|error| {
        invalid(format!(
            "cannot bind socket {} in {}: {error}",
            config.socket.display(),
            directory.display()
        ))
    })?;
    fs::set_permissions(&config.socket, fs::Permissions::from_mode(0o660))?;
    listener.set_nonblocking(true)?;
    let socket_path = config.socket.clone();
    let incoming_commands = commands.clone();
    let incoming_generations = generations.clone();
    let counter = Arc::new(AtomicU64::new(1));
    let connections = Arc::new(AtomicUsize::new(0));
    let async_stop = stop.clone();
    let run_stop = stop.clone();
    let result = runtime.block_on(async move {
        let listener = tokio::net::UnixListener::from_std(listener)?;
        let incoming = UnixListenerStream::new(listener).map(move |stream| {
            let stream = stream?;
            let credentials = stream
                .peer_cred()
                .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
            if credentials.uid() != uid || credentials.gid() != gid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "peer uid/gid differs from the manager",
                ));
            }
            let connection = counter.fetch_add(1, Ordering::Relaxed);
            let open = connections.fetch_add(1, Ordering::AcqRel) + 1;
            Ok(PeerStream {
                inner: stream,
                over_limit: open > store::MAX_CONNECTIONS,
                _slot: ConnectionSlot(connections.clone()),
                info: PeerInfo {
                    connection,
                    pid: credentials.pid().unwrap_or(0) as u32,
                    uid,
                    gid,
                },
                commands: incoming_commands.clone(),
                runtime: tokio::runtime::Handle::current(),
                generations: incoming_generations.clone(),
            })
        });
        let shutdown_stop = run_stop.clone();
        let server = Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(Duration::from_secs(20)))
            .add_service(
                LocalCacheServer::new(service)
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
            );
        let signal_stop = shutdown_stop.clone();
        tokio::spawn(async move {
            let terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
            let interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt());
            match (terminate, interrupt) {
                (Ok(mut terminate), Ok(mut interrupt)) => {
                    tokio::select! {
                        _ = terminate.recv() => {}
                        _ = interrupt.recv() => {}
                    }
                }
                (Ok(mut terminate), Err(_)) => {
                    terminate.recv().await;
                }
                (Err(_), Ok(mut interrupt)) => {
                    interrupt.recv().await;
                }
                (Err(_), Err(_)) => std::future::pending::<()>().await,
            }
            signal_stop.store(true, Ordering::Release);
        });
        let shutdown = async move {
            while !async_stop.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        server
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
            .map_err(|error| invalid(format!("server: {error}")))
    });
    stop.store(true, Ordering::Release);
    runtime.block_on(async {
        let _ = commands.send(StoreCommand::Shutdown).await;
    });
    drop(commands);
    let _ = owner.join();
    let _ = tick.join();
    if let Some(metrics) = metrics {
        let _ = metrics.join();
    }
    let _ = fs::remove_file(socket_path);
    result
}

/// Creates the socket's directory when it is missing. A directory that already
/// exists keeps its mode and ownership: the manager only sets the mode of a
/// directory it created, so pointing `--socket` into a host directory such as
/// `/tmp` or `/run` cannot change that directory's mode.
fn prepare_socket_directory(directory: &Path) -> Result<()> {
    if !directory.exists() {
        fs::create_dir_all(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o770))?;
    }
    Ok(())
}

fn start_metrics(
    address: Option<&str>,
    text: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
) -> Result<Option<thread::JoinHandle<()>>> {
    let Some(address) = address else {
        return Ok(None);
    };
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
    Ok(Some(handle))
}
