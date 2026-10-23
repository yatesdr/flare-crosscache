use crate::{
    Key, Result, invalid,
    record::{Descriptor, RankSizes},
};
use std::io::{Read, Write};

pub const VERSION: u32 = 1;
pub const MAX_DATA: usize = 4 * 1024 * 1024;
pub const MAX_FRAME: usize = MAX_DATA + 64;
pub const OPEN_LIMIT: usize = 4;
pub const WINDOW_PAGES: u32 = 64;

pub mod tag {
    pub const HELLO: u8 = 1;
    pub const WELCOME: u8 = 2;
    pub const REJECT: u8 = 3;
    pub const SNAPSHOT: u8 = 4;
    pub const ENTRY: u8 = 5;
    pub const SNAPSHOT_END: u8 = 6;
    pub const DELTA: u8 = 7;
    pub const ROOM: u8 = 8;
    pub const PUT: u8 = 9;
    pub const ACCEPT: u8 = 10;
    pub const PRESENT: u8 = 11;
    pub const RETRY: u8 = 12;
    pub const OVERSIZE: u8 = 13;
    pub const WINDOW: u8 = 14;
    pub const DATA: u8 = 15;
    pub const ROW: u8 = 16;
    pub const COMMIT: u8 = 17;
    pub const STORED: u8 = 18;
    pub const GET: u8 = 19;
    pub const DONE: u8 = 20;
    pub const CANCEL: u8 = 21;
    pub const RESYNC: u8 = 22;
}

#[derive(Clone, Debug)]
pub struct Frame {
    pub tag: u8,
    pub generation: u64,
    pub op: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn read(reader: &mut impl Read) -> Result<Self> {
        let mut length = [0; 4];
        reader.read_exact(&mut length)?;
        let length = u32::from_le_bytes(length) as usize;
        if !(17..=MAX_FRAME).contains(&length) {
            return Err(invalid("frame length"));
        }
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes)?;
        Ok(Self {
            tag: bytes[0],
            generation: u64::from_le_bytes(bytes[1..9].try_into().unwrap()),
            op: u64::from_le_bytes(bytes[9..17].try_into().unwrap()),
            payload: bytes[17..].to_vec(),
        })
    }
    pub fn write(&self, writer: &mut impl Write) -> Result<()> {
        let length = 17 + self.payload.len();
        if length > MAX_FRAME || self.op == 0 {
            return Err(invalid("frame length or op"));
        }
        let mut bytes = Vec::with_capacity(length + 4);
        bytes.extend_from_slice(&(length as u32).to_le_bytes());
        bytes.push(self.tag);
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&self.op.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        writer.write_all(&bytes)
    }
}

#[derive(Default)]
pub struct Encoder(pub Vec<u8>);
impl Encoder {
    pub fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    pub fn u32(&mut self, value: u32) {
        self.0.extend(value.to_le_bytes());
    }
    pub fn u64(&mut self, value: u64) {
        self.0.extend(value.to_le_bytes());
    }
    pub fn bytes(&mut self, value: &[u8]) {
        self.0.extend(value);
    }
    pub fn key(&mut self, key: Key) {
        self.u64(key.consumed_tokens);
        self.bytes(&key.digest);
    }
    pub fn parent(&mut self, parent: Option<Key>) {
        self.u8(parent.is_some() as u8);
        if let Some(key) = parent {
            self.key(key);
        }
    }
    pub fn text(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.bytes(value.as_bytes());
    }
    pub fn descriptor(&mut self, descriptor: &Descriptor) {
        self.bytes(&descriptor.identity);
        self.bytes(&descriptor.fingerprint);
        self.u64(descriptor.page_tokens);
        self.u32(descriptor.ranks.len() as u32);
        for rank in &descriptor.ranks {
            self.u32(rank.planes.len() as u32);
            for size in &rank.planes {
                self.u64(*size);
            }
            self.u64(rank.row_bytes);
        }
        self.u64(descriptor.page_bytes());
        self.u64(descriptor.row_bytes());
    }
}

pub struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    pub fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| invalid("length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("short payload"))?;
        self.offset = end;
        Ok(value)
    }
    pub fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub fn key(&mut self) -> Result<Key> {
        Ok(Key {
            consumed_tokens: self.u64()?,
            digest: self.array()?,
        })
    }
    pub fn parent(&mut self) -> Result<Option<Key>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.key()?)),
            _ => Err(invalid("parent flag")),
        }
    }
    pub fn text(&mut self) -> Result<&'a str> {
        let length = self.u32()? as usize;
        std::str::from_utf8(self.take(length)?).map_err(|_| invalid("UTF-8"))
    }
    pub fn descriptor(&mut self) -> Result<Descriptor> {
        let identity = self.array()?;
        let fingerprint = self.array()?;
        let page_tokens = self.u64()?;
        let count = self.u32()?;
        if count == 0 || count > 326 {
            return Err(invalid("rank count"));
        }
        let mut ranks = Vec::new();
        for _ in 0..count {
            let count = self.u32()?;
            if count == 0 || count > 488 {
                return Err(invalid("plane count"));
            }
            let mut planes = Vec::new();
            for _ in 0..count {
                planes.push(self.u64()?);
            }
            ranks.push(RankSizes {
                planes,
                row_bytes: self.u64()?,
            });
        }
        let descriptor = Descriptor {
            identity,
            fingerprint,
            page_tokens,
            ranks,
        };
        descriptor.validate()?;
        if self.u64()? != descriptor.page_bytes() || self.u64()? != descriptor.row_bytes() {
            return Err(invalid("record sizes disagree with ranks"));
        }
        Ok(descriptor)
    }
    pub fn end(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("trailing payload"))
        }
    }
}
