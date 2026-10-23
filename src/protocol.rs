use crate::{Result, invalid};
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
    pub fn new(tag: u8, generation: u64, op: u64, payload: Vec<u8>) -> Self {
        Self {
            tag,
            generation,
            op,
            payload,
        }
    }

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
        reader.read_exact(&mut header[..4])?;
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        if !(17..=MAX_FRAME).contains(&length) {
            return Err(invalid("frame length"));
        }
        reader.read_exact(&mut header[4..])?;
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
