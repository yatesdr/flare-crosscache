use crate::{Key, Result, invalid};
use std::{
    fmt::Write as _,
    time::{SystemTime, UNIX_EPOCH},
};

pub const BLOCK_BYTES: u64 = 4096;
pub const HEADER_BYTES: usize = 4096;
pub const MAGIC: [u8; 8] = *b"FLARECKP";
pub const VERSION: u32 = 1;
pub const EXTENSION: &str = "rec";
const SUFFIX: &str = ".rec";
const FIXED_FIELD_BYTES: usize = 177;
const FILE_NAME_LENGTH: usize = 16 + 1 + 32 + SUFFIX.len();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Descriptor {
    pub identity: [u8; 32],
    pub fingerprint: [u8; 16],
    pub page_tokens: u64,
    pub ranks: Vec<RankSizes>,
}

impl Descriptor {
    pub fn validate(&self) -> Result<()> {
        let header = Header {
            identity: self.identity,
            fingerprint: self.fingerprint,
            flags: 0,
            key: Key {
                consumed_tokens: 1,
                digest: [0; 16],
            },
            parent: None,
            bucket: [0; 16],
            page_tokens: self.page_tokens,
            tail_pages: 0,
            ranks: self.ranks.clone(),
            page_segment: 4096,
            row_segment: 4096,
            row_checksum: 0,
            created: 0,
        };
        if self.page_tokens == 0 {
            return Err(invalid("zero page tokens"));
        }
        let mut header = header;
        header.tail_pages = u32::try_from(1 / self.page_tokens).map_err(|_| invalid("pages"))?;
        let offsets = header.derive()?;
        header.page_segment = offsets.page_segment;
        header.row_segment = offsets.row_segment;
        header.validate()
    }
    pub fn name(&self) -> String {
        directory_name(&self.identity, &self.fingerprint)
    }
    pub fn page_bytes(&self) -> u64 {
        self.ranks.iter().map(RankSizes::page_record_bytes).sum()
    }
    pub fn row_bytes(&self) -> u64 {
        self.ranks.iter().map(RankSizes::row_record_bytes).sum()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankSizes {
    pub planes: Vec<u64>,
    pub row_bytes: u64,
}

impl RankSizes {
    fn page_record_bytes(&self) -> u64 {
        self.planes.iter().map(|size| padded(*size)).sum()
    }

    fn row_record_bytes(&self) -> u64 {
        padded(self.row_bytes)
    }

    fn encoded_bytes(&self) -> usize {
        4 + 8 * self.planes.len() + 8
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub identity: [u8; 32],
    pub fingerprint: [u8; 16],
    pub flags: u32,
    pub key: Key,
    pub parent: Option<Key>,
    pub bucket: [u8; 16],
    pub page_tokens: u64,
    pub tail_pages: u32,
    pub ranks: Vec<RankSizes>,
    pub page_segment: u64,
    pub row_segment: u64,
    pub row_checksum: u64,
    pub created: u64,
}

impl Header {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        descriptor: &Descriptor,
        identity: [u8; 32],
        fingerprint: [u8; 16],
        key: Key,
        parent: Option<Key>,
        bucket: [u8; 16],
        tail_pages: u32,
        created: SystemTime,
    ) -> Result<Self> {
        descriptor.validate()?;
        let ranks = descriptor.ranks.clone();
        let mut header = Self {
            identity,
            fingerprint,
            flags: 0,
            key,
            parent,
            bucket,
            page_tokens: descriptor.page_tokens,
            tail_pages,
            ranks,
            page_segment: 0,
            row_segment: 0,
            row_checksum: 0,
            created: created
                .duration_since(UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_secs()),
        };
        let derived = header.derive()?;
        header.page_segment = derived.page_segment;
        header.row_segment = derived.row_segment;
        header.validate()?;
        Ok(header)
    }

    pub fn encode(&self) -> Result<[u8; HEADER_BYTES]> {
        self.validate()?;
        let mut bytes = [0u8; HEADER_BYTES];
        let mut cursor = Cursor::new(&mut bytes);
        cursor.put(&MAGIC);
        cursor.put(&VERSION.to_le_bytes());
        cursor.put(&self.flags.to_le_bytes());
        cursor.put(&self.identity);
        cursor.put(&self.fingerprint);
        cursor.put(&self.key.consumed_tokens.to_le_bytes());
        cursor.put(&self.key.digest);
        cursor.put(&[self.parent.is_some() as u8]);
        let parent = self.parent.unwrap_or(Key {
            consumed_tokens: 0,
            digest: [0; 16],
        });
        cursor.put(&parent.consumed_tokens.to_le_bytes());
        cursor.put(&parent.digest);
        cursor.put(&self.bucket);
        cursor.put(&self.page_tokens.to_le_bytes());
        cursor.put(&self.tail_pages.to_le_bytes());
        cursor.put(&self.page_segment.to_le_bytes());
        cursor.put(&self.row_segment.to_le_bytes());
        cursor.put(&self.row_checksum.to_le_bytes());
        cursor.put(&self.created.to_le_bytes());
        cursor.put(&(self.ranks.len() as u32).to_le_bytes());
        for rank in &self.ranks {
            cursor.put(&(rank.planes.len() as u32).to_le_bytes());
            for size in &rank.planes {
                cursor.put(&size.to_le_bytes());
            }
            cursor.put(&rank.row_bytes.to_le_bytes());
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES {
            return Err(invalid(format!(
                "record header block is {} bytes; a header occupies {HEADER_BYTES}",
                bytes.len()
            )));
        }
        let mut cursor = Reader::new(&bytes[..HEADER_BYTES]);
        if cursor.array::<8>("magic")? != MAGIC {
            return Err(invalid("record header magic is not FLARECKP"));
        }
        let version = cursor.u32("version")?;
        if version != VERSION {
            return Err(invalid(format!(
                "record header version {version} is not the supported version {VERSION}"
            )));
        }
        let flags = cursor.u32("flags")?;
        let identity = cursor.array::<32>("identity")?;
        let fingerprint = cursor.array::<16>("fingerprint")?;
        let key = Key {
            consumed_tokens: cursor.u64("key consumed count")?,
            digest: cursor.array::<16>("key digest")?,
        };
        let parent_present = cursor.array::<1>("parent presence")?[0];
        let parent_key = Key {
            consumed_tokens: cursor.u64("parent consumed count")?,
            digest: cursor.array::<16>("parent digest")?,
        };
        let parent = match parent_present {
            0 => None,
            1 => Some(parent_key),
            other => {
                return Err(invalid(format!(
                    "record header parent presence flag is {other}; it must be 0 or 1"
                )));
            }
        };
        let bucket = cursor.array::<16>("bucket digest")?;
        let page_tokens = cursor.u64("page token count")?;
        let tail_pages = cursor.u32("tail page count")?;
        let page_segment = cursor.u64("page segment offset")?;
        let row_segment = cursor.u64("row segment offset")?;
        let row_checksum = cursor.u64("row checksum")?;
        let created = cursor.u64("creation time")?;
        let rank_count = cursor.u32("rank count")?;
        let mut ranks = Vec::new();
        for rank in 0..rank_count {
            let plane_count = cursor.u32("plane count")?;
            let mut planes = Vec::new();
            for _ in 0..plane_count {
                planes.push(cursor.u64("plane size")?);
            }
            let row_bytes = cursor.u64("row size")?;
            if planes.is_empty() {
                return Err(invalid(format!(
                    "record header rank {rank} declares no page planes"
                )));
            }
            ranks.push(RankSizes { planes, row_bytes });
        }
        let header = Self {
            identity,
            fingerprint,
            flags,
            key,
            parent,
            bucket,
            page_tokens,
            tail_pages,
            ranks,
            page_segment,
            row_segment,
            row_checksum,
            created,
        };
        header.validate()?;
        Ok(header)
    }

    pub fn validate(&self) -> Result<()> {
        if self.ranks.is_empty() {
            return Err(invalid("record header rank count is zero"));
        }
        let entry_bytes: usize = self.ranks.iter().map(RankSizes::encoded_bytes).sum();
        if FIXED_FIELD_BYTES + entry_bytes > HEADER_BYTES {
            return Err(invalid(format!(
                "record header for {} ranks and {} planes needs {} bytes, more than the \
                 {HEADER_BYTES}-byte header block",
                self.ranks.len(),
                self.ranks
                    .iter()
                    .map(|rank| rank.planes.len())
                    .sum::<usize>(),
                FIXED_FIELD_BYTES + entry_bytes
            )));
        }
        for (index, rank) in self.ranks.iter().enumerate() {
            if rank.planes.is_empty() {
                return Err(invalid(format!(
                    "record header rank {index} declares no page planes"
                )));
            }
            if rank.planes.contains(&0) {
                return Err(invalid(format!(
                    "record header rank {index} declares a zero-byte plane"
                )));
            }
            if rank.row_bytes == 0 {
                return Err(invalid(format!(
                    "record header rank {index} declares a zero-byte row"
                )));
            }
        }
        if self.page_tokens == 0 {
            return Err(invalid("record header page token count is zero"));
        }
        if self.key.consumed_tokens == 0 {
            return Err(invalid("record header key consumed count is zero"));
        }
        let parent_tokens = match self.parent {
            Some(parent) if parent.consumed_tokens >= self.key.consumed_tokens => {
                return Err(invalid(format!(
                    "record header parent consumed count {} is not below the key consumed count {}",
                    parent.consumed_tokens, self.key.consumed_tokens
                )));
            }
            Some(parent) => parent.consumed_tokens,
            None => 0,
        };
        let expected_tail =
            self.key.consumed_tokens / self.page_tokens - parent_tokens / self.page_tokens;
        if expected_tail != self.tail_pages as u64 {
            return Err(invalid(format!(
                "record header tail page count {} disagrees with the {expected_tail} complete \
                 pages between the parent and the key",
                self.tail_pages
            )));
        }
        let derived = self.derive()?;
        if self.page_segment != derived.page_segment {
            return Err(invalid(format!(
                "record header page segment offset {} is not the derived offset {}",
                self.page_segment, derived.page_segment
            )));
        }
        if self.row_segment != derived.row_segment {
            return Err(invalid(format!(
                "record header row segment offset {} is not the derived offset {}",
                self.row_segment, derived.row_segment
            )));
        }
        Ok(())
    }

    fn derive(&self) -> Result<Derived> {
        let mut page_record_bytes = 0u64;
        let mut row_record_bytes = 0u64;
        for (index, rank) in self.ranks.iter().enumerate() {
            for size in &rank.planes {
                page_record_bytes = checked_padded(*size)
                    .and_then(|padded| page_record_bytes.checked_add(padded))
                    .ok_or_else(|| {
                        invalid(format!(
                            "record header rank {index} plane size {size} overflows the page record"
                        ))
                    })?;
            }
            row_record_bytes = checked_padded(rank.row_bytes)
                .and_then(|padded| row_record_bytes.checked_add(padded))
                .ok_or_else(|| {
                    invalid(format!(
                        "record header rank {index} row size {} overflows the row record",
                        rank.row_bytes
                    ))
                })?;
        }
        let page_segment = self.checksum_segment() + self.checksum_bytes();
        let row_segment = (self.tail_pages as u64)
            .checked_mul(page_record_bytes)
            .and_then(|pages| page_segment.checked_add(pages))
            .ok_or_else(|| {
                invalid(format!(
                    "record header tail page count {} of {page_record_bytes}-byte page records \
                     overflows the row segment offset",
                    self.tail_pages
                ))
            })?;
        if row_segment.checked_add(row_record_bytes).is_none() {
            return Err(invalid(format!(
                "record header row record size {row_record_bytes} overflows the file length"
            )));
        }
        Ok(Derived {
            page_segment,
            row_segment,
        })
    }

    pub fn checksum_segment(&self) -> u64 {
        HEADER_BYTES as u64
    }

    pub fn checksum_bytes(&self) -> u64 {
        (self.tail_pages as u64 * 8).div_ceil(BLOCK_BYTES) * BLOCK_BYTES
    }

    pub fn page_record_bytes(&self) -> u64 {
        self.ranks.iter().map(RankSizes::page_record_bytes).sum()
    }

    pub fn row_record_bytes(&self) -> u64 {
        self.ranks.iter().map(RankSizes::row_record_bytes).sum()
    }

    pub fn length_with_row(&self) -> u64 {
        self.row_segment + self.row_record_bytes()
    }

    pub fn row_present(&self, file_length: u64) -> bool {
        file_length >= self.length_with_row()
    }

    pub fn matches(&self, descriptor: &Descriptor) -> bool {
        self.page_tokens == descriptor.page_tokens && self.ranks == descriptor.ranks
    }
}

struct Derived {
    page_segment: u64,
    row_segment: u64,
}

fn padded(bytes: u64) -> u64 {
    bytes.div_ceil(BLOCK_BYTES) * BLOCK_BYTES
}

fn checked_padded(bytes: u64) -> Option<u64> {
    bytes.div_ceil(BLOCK_BYTES).checked_mul(BLOCK_BYTES)
}

struct Cursor<'a> {
    bytes: &'a mut [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn put(&mut self, field: &[u8]) {
        self.bytes[self.position..self.position + field.len()].copy_from_slice(field);
        self.position += field.len();
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, count: usize, field: &str) -> Result<&'a [u8]> {
        let end = self.position + count;
        if end > self.bytes.len() {
            return Err(invalid(format!(
                "record header {field} extends past the header block"
            )));
        }
        let taken = &self.bytes[self.position..end];
        self.position = end;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self, field: &str) -> Result<[u8; N]> {
        let mut array = [0u8; N];
        array.copy_from_slice(self.take(N, field)?);
        Ok(array)
    }

    fn u32(&mut self, field: &str) -> Result<u32> {
        self.array::<4>(field).map(u32::from_le_bytes)
    }

    fn u64(&mut self, field: &str) -> Result<u64> {
        self.array::<8>(field).map(u64::from_le_bytes)
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}

fn hex_digits(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2)
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect()
}

pub fn directory_name(identity: &[u8; 32], fingerprint: &[u8; 16]) -> String {
    format!("{}-{}", hex(&identity[..8]), hex(fingerprint))
}

pub fn file_name(key: &Key) -> String {
    format!(
        "{:016x}-{}.{EXTENSION}",
        key.consumed_tokens,
        hex(&key.digest)
    )
}

pub fn temporary_name(key: &Key) -> String {
    format!(".{}.tmp", file_name(key))
}

pub fn parse_file_name(name: &str) -> Option<Key> {
    if name.len() != FILE_NAME_LENGTH || !name.is_ascii() {
        return None;
    }
    let (consumed, rest) = name.split_at(16);
    let rest = rest.strip_prefix('-')?;
    let (digest, suffix) = rest.split_at(32);
    if suffix != SUFFIX {
        return None;
    }
    let consumed = u64::from_be_bytes(hex_digits(consumed)?.try_into().ok()?);
    let digest: [u8; 16] = hex_digits(digest)?.try_into().ok()?;
    Some(Key {
        consumed_tokens: consumed,
        digest,
    })
}

pub fn checksum(bytes: &[u8]) -> u64 {
    twox_hash::XxHash3_64::oneshot(bytes)
}
