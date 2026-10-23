use crate::{
    Key, Result, invalid,
    protocol::{Decoder, Encoder},
};
use std::fmt::Write as _;

pub const BLOCK_BYTES: u64 = 4096;
pub const HEADER_BYTES: usize = 4096;
const MAGIC: &[u8; 8] = b"FLARECKP";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankSizes {
    pub planes: Vec<u64>,
    pub row_bytes: u64,
}

pub fn encode_ranks(encoder: &mut Encoder, ranks: &[RankSizes]) {
    encoder.u32(ranks.len() as u32);
    for rank in ranks {
        encoder.u32(rank.planes.len() as u32);
        for size in &rank.planes {
            encoder.u64(*size);
        }
        encoder.u64(rank.row_bytes);
    }
}
pub fn decode_ranks(decoder: &mut Decoder<'_>) -> Result<Vec<RankSizes>> {
    let count = decoder.u32()?;
    if count == 0 || count > 326 {
        return Err(invalid("rank count"));
    }
    let mut ranks = Vec::new();
    for _ in 0..count {
        let count = decoder.u32()?;
        if count == 0 || count > 488 {
            return Err(invalid("plane count"));
        }
        let mut planes = Vec::new();
        for _ in 0..count {
            planes.push(decoder.u64()?);
        }
        ranks.push(RankSizes {
            planes,
            row_bytes: decoder.u64()?,
        });
    }
    Ok(ranks)
}

fn layout(page_tokens: u64, ranks: &[RankSizes]) -> Result<(u64, u64)> {
    let entries: usize = ranks.iter().map(|rank| 12 + rank.planes.len() * 8).sum();
    if page_tokens == 0 || ranks.is_empty() || 177 + entries > HEADER_BYTES {
        return Err(invalid("zero page tokens, empty ranks or oversized header"));
    }
    let add = |total: u64, size: u64| {
        if size == 0 {
            return Err(invalid("zero unit size"));
        }
        size.div_ceil(BLOCK_BYTES)
            .checked_mul(BLOCK_BYTES)
            .and_then(|size| total.checked_add(size))
            .ok_or_else(|| invalid("layout overflow"))
    };
    let (mut pages, mut rows) = (0, 0);
    for rank in ranks {
        if rank.planes.is_empty() {
            return Err(invalid("empty planes"));
        }
        for size in &rank.planes {
            pages = add(pages, *size)?;
        }
        rows = add(rows, rank.row_bytes)?;
    }
    Ok((pages, rows))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Descriptor {
    pub identity: [u8; 32],
    pub fingerprint: [u8; 16],
    pub page_tokens: u64,
    pub ranks: Vec<RankSizes>,
}
impl Descriptor {
    pub fn validate(&self) -> Result<()> {
        layout(self.page_tokens, &self.ranks).map(|_| ())
    }
    pub fn name(&self) -> String {
        directory_name(&self.identity, &self.fingerprint)
    }
    pub fn page_bytes(&self) -> u64 {
        page_bytes(&self.ranks)
    }
    pub fn row_bytes(&self) -> u64 {
        row_bytes(&self.ranks)
    }
}
fn padded(size: u64) -> u64 {
    size.div_ceil(BLOCK_BYTES) * BLOCK_BYTES
}
fn page_bytes(ranks: &[RankSizes]) -> u64 {
    ranks
        .iter()
        .flat_map(|rank| &rank.planes)
        .map(|size| padded(*size))
        .sum()
}
fn row_bytes(ranks: &[RankSizes]) -> u64 {
    ranks.iter().map(|rank| padded(rank.row_bytes)).sum()
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
    pub fn new(
        descriptor: &Descriptor,
        key: Key,
        parent: Option<Key>,
        bucket: [u8; 16],
        tail_pages: u32,
    ) -> Result<Self> {
        let mut header = Self {
            identity: descriptor.identity,
            fingerprint: descriptor.fingerprint,
            flags: 0,
            key,
            parent,
            bucket,
            page_tokens: descriptor.page_tokens,
            tail_pages,
            ranks: descriptor.ranks.clone(),
            page_segment: 0,
            row_segment: 0,
            row_checksum: 0,
            created: 0,
        };
        (header.page_segment, header.row_segment) = header.offsets()?;
        header.validate()?;
        Ok(header)
    }
    fn offsets(&self) -> Result<(u64, u64)> {
        let (pages, rows) = layout(self.page_tokens, &self.ranks)?;
        let page_segment = HEADER_BYTES as u64 + self.checksum_bytes();
        let row_segment = u64::from(self.tail_pages)
            .checked_mul(pages)
            .and_then(|size| page_segment.checked_add(size))
            .ok_or_else(|| invalid("page segment overflow"))?;
        row_segment
            .checked_add(rows)
            .ok_or_else(|| invalid("row segment overflow"))?;
        Ok((page_segment, row_segment))
    }
    fn validate(&self) -> Result<()> {
        let (pages, rows) = self.offsets()?;
        let parent = self.parent.map_or(0, |key| key.consumed_tokens);
        if self.key.consumed_tokens <= parent
            || self.key.consumed_tokens / self.page_tokens - parent / self.page_tokens
                != u64::from(self.tail_pages)
            || self.page_segment != pages
            || self.row_segment != rows
        {
            return Err(invalid("record key, parent, tail or offsets"));
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(
            bytes
                .get(..HEADER_BYTES)
                .ok_or_else(|| invalid("short header"))?,
        );
        if decoder.array::<8>()? != *MAGIC || decoder.u32()? != 1 {
            return Err(invalid("record magic or version"));
        }
        let flags = decoder.u32()?;
        let identity = decoder.array()?;
        let fingerprint = decoder.array()?;
        let key = decoder.key()?;
        let present = decoder.u8()?;
        let parent = decoder.key()?;
        let parent = match present {
            0 => None,
            1 => Some(parent),
            _ => return Err(invalid("parent flag")),
        };
        let header = Self {
            identity,
            fingerprint,
            flags,
            key,
            parent,
            bucket: decoder.array()?,
            page_tokens: decoder.u64()?,
            tail_pages: decoder.u32()?,
            page_segment: decoder.u64()?,
            row_segment: decoder.u64()?,
            row_checksum: decoder.u64()?,
            created: decoder.u64()?,
            ranks: decode_ranks(&mut decoder)?,
        };
        header.validate()?;
        Ok(header)
    }
    pub fn checksum_bytes(&self) -> u64 {
        padded(u64::from(self.tail_pages) * 8)
    }
    pub fn page_record_bytes(&self) -> u64 {
        page_bytes(&self.ranks)
    }
    pub fn row_record_bytes(&self) -> u64 {
        row_bytes(&self.ranks)
    }
    pub fn length(&self, row: bool) -> u64 {
        self.row_segment + if row { self.row_record_bytes() } else { 0 }
    }
    pub fn length_with_row(&self) -> u64 {
        self.row_segment + self.row_record_bytes()
    }
    pub fn matches(&self, descriptor: &Descriptor) -> bool {
        self.page_tokens == descriptor.page_tokens && self.ranks == descriptor.ranks
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}
pub fn directory_name(identity: &[u8; 32], fingerprint: &[u8; 16]) -> String {
    format!("{}-{}", hex(&identity[..8]), hex(fingerprint))
}
pub fn file_name(key: &Key) -> String {
    format!("{:016x}-{}.rec", key.consumed_tokens, hex(&key.digest))
}
pub fn parse_file_name(name: &str) -> Option<Key> {
    if name.len() != 53 || !name.is_ascii() {
        return None;
    }
    let mut key = Key {
        consumed_tokens: u64::from_str_radix(&name[..16], 16).ok()?,
        digest: [0; 16],
    };
    for (index, byte) in key.digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&name[17 + index * 2..19 + index * 2], 16).ok()?;
    }
    (file_name(&key) == name).then_some(key)
}
pub fn checksum(bytes: &[u8]) -> u64 {
    twox_hash::XxHash3_64::oneshot(bytes)
}
