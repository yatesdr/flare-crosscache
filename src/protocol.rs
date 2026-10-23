use crate::{
    Key, Result, invalid,
    record::{Descriptor, RankSizes},
};
use std::io::{self, IoSlice, Read, Write};

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
        let mut frame = Self {
            tag: 0,
            generation: 0,
            op: 0,
            payload: Vec::new(),
        };
        frame.read_into(reader)?;
        Ok(frame)
    }
    pub fn read_into(&mut self, reader: &mut impl Read) -> Result<()> {
        let (header, length) = Self::read_header(reader)?;
        self.tag = header.tag;
        self.generation = header.generation;
        self.op = header.op;
        self.payload.resize(length - 17, 0);
        reader.read_exact(&mut self.payload)
    }
    pub fn read_header(reader: &mut impl Read) -> Result<(Self, usize)> {
        let mut header = [0; 21];
        reader.read_exact(&mut header)?;
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        if !(17..=MAX_FRAME).contains(&length) {
            return Err(invalid("frame length"));
        }
        Ok((
            Self {
                tag: header[4],
                generation: u64::from_le_bytes(header[5..13].try_into().unwrap()),
                op: u64::from_le_bytes(header[13..21].try_into().unwrap()),
                payload: Vec::new(),
            },
            length,
        ))
    }
    pub fn write(&self, writer: &mut impl Write) -> Result<()> {
        Self::write_parts(
            writer,
            self.tag,
            self.generation,
            self.op,
            &[],
            &self.payload,
        )
    }
    pub fn write_parts(
        writer: &mut impl Write,
        tag: u8,
        generation: u64,
        op: u64,
        prefix: &[u8],
        data: &[u8],
    ) -> Result<()> {
        let length = 17 + prefix.len() + data.len();
        if length > MAX_FRAME || op == 0 || prefix.len() > 12 {
            return Err(invalid("frame length or op"));
        }
        let mut header = [0; 33];
        header[..4].copy_from_slice(&(length as u32).to_le_bytes());
        header[4] = tag;
        header[5..13].copy_from_slice(&generation.to_le_bytes());
        header[13..21].copy_from_slice(&op.to_le_bytes());
        header[21..21 + prefix.len()].copy_from_slice(prefix);
        let mut slices = [
            IoSlice::new(&header[..21 + prefix.len()]),
            IoSlice::new(data),
        ];
        let mut remaining = &mut slices[..];
        while !remaining.is_empty() {
            match writer.write_vectored(remaining) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "frame write")),
                Ok(count) => IoSlice::advance_slices(&mut remaining, count),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
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
