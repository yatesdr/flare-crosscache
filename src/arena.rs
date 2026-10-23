use crate::{Result, invalid};
use std::{
    collections::{BTreeMap, BTreeSet},
    ptr::NonNull,
    sync::{Arc, Mutex},
};

struct Mapping {
    pointer: NonNull<u8>,
    length: usize,
    free: Mutex<BTreeMap<usize, BTreeSet<usize>>>,
}

// Allocations never overlap; a mutable slice requires exclusive ownership of its allocation.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.pointer.as_ptr().cast(), self.length);
        }
    }
}

#[derive(Clone)]
pub struct Arena(Arc<Mapping>);

pub struct Extent {
    mapping: Arc<Mapping>,
    offset: usize,
    capacity: usize,
    length: usize,
}

impl Arena {
    pub fn new(length: usize) -> Result<Self> {
        let length = length / 4096 * 4096;
        if length == 0 {
            return Err(invalid("arena must hold at least 4096 bytes"));
        }
        let pointer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if pointer == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(pointer, length, libc::MADV_HUGEPAGE);
        }
        let mut free: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        let mut offset = 0;
        let mut remaining = length;
        while remaining > 0 {
            let size = 1usize << (usize::BITS - 1 - remaining.leading_zeros());
            free.entry(size).or_default().insert(offset);
            offset += size;
            remaining -= size;
        }
        Ok(Self(Arc::new(Mapping {
            pointer: NonNull::new(pointer.cast()).unwrap(),
            length,
            free: Mutex::new(free),
        })))
    }

    pub fn allocate(&self, length: usize) -> Option<Extent> {
        let capacity = length.max(4096).checked_next_power_of_two()?;
        let mut free = self.0.free.lock().unwrap();
        let (&size, _) = free
            .range(capacity..)
            .find(|(_, offsets)| !offsets.is_empty())?;
        let offset = free.get_mut(&size).unwrap().pop_first().unwrap();
        let mut split = size;
        while split > capacity {
            split /= 2;
            free.entry(split).or_default().insert(offset + split);
        }
        Some(Extent {
            mapping: self.0.clone(),
            offset,
            capacity,
            length,
        })
    }

    pub fn used(&self) -> usize {
        let free = self.0.free.lock().unwrap();
        self.0.length
            - free
                .iter()
                .map(|(size, offsets)| size * offsets.len())
                .sum::<usize>()
    }

    pub fn capacity(&self) -> usize {
        self.0.length
    }
}

impl Extent {
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.mapping.pointer.as_ptr().add(self.offset), self.length)
        }
    }
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.mapping.pointer.as_ptr().add(self.offset),
                self.length,
            )
        }
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for Extent {
    fn drop(&mut self) {
        let mut free = self.mapping.free.lock().unwrap();
        let mut offset = self.offset;
        let mut size = self.capacity;
        loop {
            let buddy = offset ^ size;
            if !free
                .get_mut(&size)
                .is_some_and(|offsets| offsets.remove(&buddy))
            {
                break;
            }
            offset = offset.min(buddy);
            size *= 2;
        }
        free.entry(size).or_default().insert(offset);
    }
}
