use crate::{
    Result,
    arena::{Arena, Extent},
    invalid,
    protocol::{Decoder, Encoder},
    record::{self, Descriptor, Header},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

pub const ENVELOPE_BYTES: u64 = 4096;
const MAGIC: &[u8; 8] = b"FLCMREC1";

pub struct RootLock {
    _file: File,
}
impl RootLock {
    pub fn acquire(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root.join(".lock"))?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { _file: file })
    }
}

pub struct DiskExtent {
    pub path: PathBuf,
    retired: AtomicBool,
    file: std::sync::Mutex<Option<Arc<File>>>,
}
impl DiskExtent {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            retired: AtomicBool::new(false),
            file: std::sync::Mutex::new(None),
        }
    }
    pub fn reader(&self) -> Result<Arc<File>> {
        let mut file = self.file.lock().unwrap();
        if file.is_none() {
            *file = Some(Arc::new(open_read(&self.path)?));
        }
        Ok(file.as_ref().unwrap().clone())
    }
    pub fn keep(&self) {
        self.retired.store(false, Ordering::Release);
    }
    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
}
impl Drop for DiskExtent {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Clone, Default)]
pub struct Faults {
    pub pause_at: Option<String>,
    pub signal: Option<PathBuf>,
    pub disk_full: Option<PathBuf>,
    pub delay_ms: u64,
}
impl Faults {
    fn checkpoint(&self, point: &str) {
        if self.pause_at.as_deref() == Some(point) {
            if let Some(path) = &self.signal {
                fs::write(path, point).unwrap();
                while path.exists() {
                    thread::sleep(Duration::from_millis(2));
                }
            }
        }
    }
    fn before_io(&self) -> Result<()> {
        if self.delay_ms > 0 {
            thread::sleep(Duration::from_millis(self.delay_ms));
        }
        if self.disk_full.as_ref().is_some_and(|path| path.exists()) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        Ok(())
    }
}

pub enum Task {
    Write {
        id: u64,
        temporary: Arc<DiskExtent>,
        final_path: PathBuf,
        data: Arc<Extent>,
        header: Header,
        row: bool,
    },
    Chunk {
        id: u64,
        temporary: Arc<DiskExtent>,
        offset: u64,
        data: Arc<Extent>,
    },
    Finish {
        id: u64,
        temporary: Arc<DiskExtent>,
        final_extent: Arc<DiskExtent>,
        metadata: Arc<Vec<u8>>,
        header: Header,
        row: bool,
    },
    Compact {
        id: u64,
        source: Arc<DiskExtent>,
        temporary: Arc<DiskExtent>,
        header: Header,
    },
}

pub struct Completion {
    pub id: u64,
    pub result: Result<()>,
    pub bytes: u64,
    pub elapsed: Duration,
}

pub struct Worker {
    pub sender: SyncSender<Task>,
    pub receiver: Receiver<Completion>,
}
impl Worker {
    pub fn start(faults: Faults, lock: Arc<RootLock>) -> Self {
        let (sender, tasks) = mpsc::sync_channel(8);
        let (done, receiver) = mpsc::sync_channel(16);
        thread::spawn(move || {
            let _lock = lock;
            for task in tasks {
                let started = Instant::now();
                let id = match &task {
                    Task::Write { id, .. }
                    | Task::Chunk { id, .. }
                    | Task::Finish { id, .. }
                    | Task::Compact { id, .. } => *id,
                };
                let result = execute(task, &faults);
                let bytes = result.as_ref().copied().unwrap_or(0);
                if done
                    .send(Completion {
                        id,
                        result: result.map(|_| ()),
                        bytes,
                        elapsed: started.elapsed(),
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        Self { sender, receiver }
    }
}

fn open(path: &Path, write: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_DIRECT);
    if write {
        options.create(true).truncate(false);
    }
    options.open(path)
}

pub fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> Result<()> {
    let mut done = 0;
    while done < bytes.len() {
        match file.read_at(&mut bytes[done..], offset + done as u64) {
            Ok(0) => return Err(invalid("short disk read")),
            Ok(count) if count % 4096 == 0 => done += count,
            Ok(_) => return Err(invalid("unaligned disk read")),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
fn write_at(file: &File, bytes: &[u8], offset: u64) -> Result<()> {
    let mut done = 0;
    while done < bytes.len() {
        match file.write_at(&bytes[done..], offset + done as u64) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "disk write")),
            Ok(count) if count % 4096 == 0 => done += count,
            Ok(_) => return Err(invalid("unaligned disk write")),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn execute(task: Task, faults: &Faults) -> Result<u64> {
    faults.before_io()?;
    match task {
        Task::Chunk {
            temporary,
            offset,
            data,
            ..
        } => {
            let file = open(&temporary.path, true)?;
            write_at(&file, data.bytes(), ENVELOPE_BYTES + offset)?;
            Ok(data.bytes().len() as u64)
        }
        Task::Write {
            temporary,
            final_path,
            data,
            header,
            row,
            ..
        } => {
            faults.checkpoint("parent_write");
            let file = open(&temporary.path, true)?;
            write_at(&file, data.bytes(), ENVELOPE_BYTES)?;
            finish(
                &file,
                &temporary.path,
                &final_path,
                data.bytes(),
                &header,
                row,
                faults,
            )?;
            faults.checkpoint("after_rename");
            Ok(data.bytes().len() as u64 + ENVELOPE_BYTES)
        }
        Task::Finish {
            temporary,
            final_extent,
            metadata,
            header,
            row,
            ..
        } => {
            let file = open(&temporary.path, true)?;
            let arena = Arena::new(4 * 1024 * 1024)?;
            for (index, chunk) in metadata.chunks(4 * 1024 * 1024).enumerate() {
                let mut buffer = arena.allocate(chunk.len()).unwrap();
                buffer.bytes_mut().copy_from_slice(chunk);
                write_at(
                    &file,
                    buffer.bytes(),
                    ENVELOPE_BYTES + (index * 4 * 1024 * 1024) as u64,
                )?;
            }
            finish(
                &file,
                &temporary.path,
                &final_extent.path,
                &metadata,
                &header,
                row,
                faults,
            )?;
            final_extent.retire();
            faults.checkpoint("after_rename");
            Ok(metadata.len() as u64 + ENVELOPE_BYTES)
        }
        Task::Compact {
            source,
            temporary,
            header,
            ..
        } => {
            let input = open(&source.path, false)?;
            let output = open(&temporary.path, true)?;
            let arena = Arena::new(4 * 1024 * 1024)?;
            let mut offset = ENVELOPE_BYTES;
            let mut metadata = Vec::new();
            while offset < ENVELOPE_BYTES + header.row_segment {
                let length =
                    (ENVELOPE_BYTES + header.row_segment - offset).min(4 * 1024 * 1024) as usize;
                let mut buffer = arena.allocate(length).unwrap();
                read_at(&input, buffer.bytes_mut(), offset)?;
                if metadata.len() < header.page_segment as usize {
                    let count = (header.page_segment as usize - metadata.len()).min(length);
                    metadata.extend_from_slice(&buffer.bytes()[..count]);
                }
                write_at(&output, buffer.bytes(), offset)?;
                offset += length as u64;
            }
            finish(
                &output,
                &temporary.path,
                &source.path,
                &metadata,
                &header,
                false,
                faults,
            )?;
            Ok(header.row_segment + ENVELOPE_BYTES)
        }
    }
}

fn finish(
    file: &File,
    temporary: &Path,
    final_path: &Path,
    metadata: &[u8],
    header: &Header,
    row: bool,
    faults: &Faults,
) -> Result<()> {
    let length = if row {
        header.length_with_row()
    } else {
        header.row_segment
    };
    let arena = Arena::new(4096)?;
    let mut envelope = arena.allocate(4096).unwrap();
    envelope.bytes_mut().fill(0);
    envelope.bytes_mut()[..8].copy_from_slice(MAGIC);
    envelope.bytes_mut()[8..12].copy_from_slice(&1u32.to_le_bytes());
    envelope.bytes_mut()[12..16].copy_from_slice(&(row as u32).to_le_bytes());
    envelope.bytes_mut()[16..24].copy_from_slice(&length.to_le_bytes());
    let crc = crc32(&metadata[..header.page_segment as usize]);
    envelope.bytes_mut()[24..28].copy_from_slice(&crc.to_le_bytes());
    let crc = crc32(&envelope.bytes()[..28]);
    envelope.bytes_mut()[28..32].copy_from_slice(&crc.to_le_bytes());
    write_at(file, envelope.bytes(), 0)?;
    file.set_len(ENVELOPE_BYTES + length)?;
    file.sync_data()?;
    faults.checkpoint("before_rename");
    fs::rename(temporary, final_path)?;
    File::open(final_path.parent().unwrap())?.sync_all()?;
    Ok(())
}

pub fn open_read(path: &Path) -> Result<File> {
    open(path, false)
}

pub struct Recovered {
    pub descriptor: Descriptor,
    pub records: Vec<(Header, bool, Arc<DiskExtent>)>,
}

pub fn descriptor(directory: &Path, value: &Descriptor) -> Result<()> {
    fs::create_dir_all(directory)?;
    let path = directory.join("descriptor");
    if path.exists() {
        if load_descriptor(&path)? != *value {
            return Err(invalid("namespace descriptor mismatch"));
        }
        return Ok(());
    }
    let mut bytes = Encoder::default();
    bytes.descriptor(value);
    let crc = crc32(&bytes.0);
    bytes.u32(crc);
    let temp = directory.join(".descriptor.tmp");
    let mut file = File::create(&temp)?;
    file.write_all(&bytes.0)?;
    file.sync_data()?;
    fs::rename(temp, path)?;
    File::open(directory)?.sync_all()
}
fn load_descriptor(path: &Path) -> Result<Descriptor> {
    let mut bytes = Vec::new();
    File::open(path)?.take(8193).read_to_end(&mut bytes)?;
    if bytes.len() < 4 || bytes.len() > 8192 {
        return Err(invalid("descriptor length"));
    }
    let tail = bytes.len() - 4;
    if crc32(&bytes[..tail]) != u32::from_le_bytes(bytes[tail..].try_into().unwrap()) {
        return Err(invalid("descriptor CRC"));
    }
    let mut decoder = Decoder::new(&bytes[..tail]);
    let descriptor = decoder.descriptor()?;
    decoder.end()?;
    Ok(descriptor)
}

pub fn recover(root: &Path) -> Result<Vec<Recovered>> {
    let mut namespaces = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let directory = entry.path();
        if !directory.join("descriptor").exists() {
            continue;
        }
        let descriptor = load_descriptor(&directory.join("descriptor"))?;
        if entry.file_name().to_str() != Some(&descriptor.name()) {
            return Err(invalid("namespace directory name"));
        }
        let mut records = Vec::new();
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let path = entry.path();
            if name.starts_with('.') && name.ends_with(".tmp") {
                fs::remove_file(path)?;
                continue;
            }
            if !name.ends_with(".rec") {
                continue;
            }
            match inspect(&path, name, &descriptor) {
                Ok((header, row)) => records.push((header, row, Arc::new(DiskExtent::new(path)))),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    fs::remove_file(path)?
                }
                Err(error) => return Err(error),
            }
        }
        records.sort_by_key(|(header, _, _)| header.key);
        let mut kept = std::collections::HashSet::new();
        records.retain(|(header, _, extent)| {
            if header.parent.is_none_or(|parent| kept.contains(&parent)) {
                kept.insert(header.key);
                true
            } else {
                extent.retire();
                false
            }
        });
        namespaces.push(Recovered {
            descriptor,
            records,
        });
    }
    Ok(namespaces)
}

fn inspect(path: &Path, name: &str, descriptor: &Descriptor) -> Result<(Header, bool)> {
    let mut file = File::open(path)?;
    let mut prefix = [0; 4096];
    file.read_exact(&mut prefix)?;
    if &prefix[..8] != MAGIC
        || u32::from_le_bytes(prefix[8..12].try_into().unwrap()) != 1
        || crc32(&prefix[..28]) != u32::from_le_bytes(prefix[28..32].try_into().unwrap())
    {
        return Err(invalid("envelope magic, version or CRC"));
    }
    let row = match u32::from_le_bytes(prefix[12..16].try_into().unwrap()) {
        0 => false,
        1 => true,
        _ => return Err(invalid("envelope flags")),
    };
    let length = u64::from_le_bytes(prefix[16..24].try_into().unwrap());
    let mut block = [0; 4096];
    file.read_exact(&mut block)?;
    let header = Header::decode(&block)?;
    if record::parse_file_name(name) != Some(header.key)
        || header.identity != descriptor.identity
        || header.fingerprint != descriptor.fingerprint
        || !header.matches(descriptor)
        || length
            != if row {
                header.length_with_row()
            } else {
                header.row_segment
            }
        || file.metadata()?.len() != ENVELOPE_BYTES + length
    {
        return Err(invalid("envelope record mismatch"));
    }
    // Hash the variable checksum segment without allocating in proportion to an untrusted length.
    let mut crc = !0u32;
    crc_update(&mut crc, &block);
    let mut remaining = header.checksum_bytes();
    while remaining > 0 {
        file.read_exact(&mut block)?;
        crc_update(&mut crc, &block);
        remaining -= 4096;
    }
    if !crc != u32::from_le_bytes(prefix[24..28].try_into().unwrap()) {
        return Err(invalid("metadata CRC"));
    }
    Ok((header, row))
}

fn crc_update(crc: &mut u32, bytes: &[u8]) {
    for byte in bytes {
        *crc ^= u32::from(*byte);
        for _ in 0..8 {
            *crc = (*crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(*crc & 1));
        }
    }
}
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    crc_update(&mut crc, bytes);
    !crc
}
