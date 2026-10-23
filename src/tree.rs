use crate::{Key, Result, invalid};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Node<L> {
    pub key: Key,
    pub parent: Option<Key>,
    pub bucket: [u8; 16],
    pub tail_pages: usize,
    pub row: bool,
    pub ready: bool,
    pub location: L,
    children: u32,
    pins: u32,
    accessed: u64,
}

impl<L> Node<L> {
    pub fn accessed(&self) -> u64 {
        self.accessed
    }
    pub fn children(&self) -> u32 {
        self.children
    }
    pub fn pins(&self) -> u32 {
        self.pins
    }
    pub fn pinned(&self) -> bool {
        self.pins > 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    pub key: Key,
    pub consumed_tokens: u64,
    pub resident_tokens: u64,
    pub nodes: Vec<Key>,
    pub skip_pages: usize,
    pub pages: usize,
}

#[derive(Debug)]
pub enum Eviction<L> {
    Leaves(Vec<Node<L>>),
    Compacted(Key),
}

pub struct Tree<L> {
    page_tokens: u64,
    nodes: HashMap<Key, Node<L>>,
    buckets: HashMap<[u8; 16], Vec<Key>>,
    clock: u64,
}

impl<L> Tree<L> {
    pub fn new(page_tokens: u64) -> Self {
        Self {
            page_tokens,
            nodes: HashMap::new(),
            buckets: HashMap::new(),
            clock: 0,
        }
    }

    pub fn page_tokens(&self) -> u64 {
        self.page_tokens
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn contains(&self, key: &Key) -> bool {
        self.nodes.contains_key(key)
    }

    pub fn node(&self, key: &Key) -> Option<&Node<L>> {
        self.nodes.get(key)
    }

    pub fn node_mut(&mut self, key: &Key) -> Option<&mut Node<L>> {
        self.nodes.get_mut(key)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node<L>> {
        self.nodes.values()
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        key: Key,
        parent: Option<Key>,
        bucket: [u8; 16],
        tail_pages: usize,
        row: bool,
        ready: bool,
        location: L,
    ) -> Result<()> {
        if key.consumed_tokens == 0 || self.nodes.contains_key(&key) {
            return Err(invalid("host record key is empty or already indexed"));
        }
        let parent_tokens = match parent {
            Some(parent) => {
                let node = self
                    .nodes
                    .get_mut(&parent)
                    .ok_or_else(|| invalid("host record parent is not indexed"))?;
                node.children += 1;
                parent.consumed_tokens
            }
            None => 0,
        };
        if key.consumed_tokens <= parent_tokens
            || key.consumed_tokens / self.page_tokens - parent_tokens / self.page_tokens
                != tail_pages as u64
        {
            if let Some(parent) = parent {
                self.nodes.get_mut(&parent).unwrap().children -= 1;
            }
            return Err(invalid(
                "host record must extend its parent by exactly its tail pages",
            ));
        }
        let accessed = self.tick();
        self.buckets.entry(bucket).or_default().push(key);
        self.nodes.insert(
            key,
            Node {
                key,
                parent,
                bucket,
                tail_pages,
                row,
                ready,
                location,
                children: 0,
                pins: 0,
                accessed,
            },
        );
        Ok(())
    }

    pub fn access_at(&mut self, key: &Key, tick: u64) {
        self.clock = self.clock.max(tick);
        if let Some(node) = self.nodes.get_mut(key) {
            node.accessed = tick;
        }
    }
    pub fn touch(&mut self, key: &Key) {
        let tick = self.tick();
        if let Some(node) = self.nodes.get_mut(key) {
            node.accessed = tick;
        }
    }

    pub fn pin(&mut self, key: &Key) -> Result<()> {
        let node = self
            .nodes
            .get_mut(key)
            .ok_or_else(|| invalid("cannot pin an unindexed host record"))?;
        node.pins = node
            .pins
            .checked_add(1)
            .ok_or_else(|| invalid("host record pin count exhausted"))?;
        Ok(())
    }

    pub fn unpin(&mut self, key: &Key) -> Result<()> {
        let node = self
            .nodes
            .get_mut(key)
            .ok_or_else(|| invalid("cannot unpin an unindexed host record"))?;
        node.pins = node
            .pins
            .checked_sub(1)
            .ok_or_else(|| invalid("host record has no pin to release"))?;
        Ok(())
    }

    pub fn complete(&self, key: &Key) -> bool {
        self.chain(key, 0, &|node| node.ready).is_some()
    }

    pub fn chain(
        &self,
        key: &Key,
        resident: u64,
        usable: &impl Fn(&Node<L>) -> bool,
    ) -> Option<Match> {
        let endpoint = self.nodes.get(key)?;
        if !endpoint.row || !usable(endpoint) {
            return None;
        }
        let mut nodes = vec![*key];
        let mut cursor = endpoint;
        let parent_tokens = loop {
            match cursor.parent {
                None => break 0,
                Some(parent) if parent.consumed_tokens <= resident => {
                    break parent.consumed_tokens;
                }
                Some(parent) => {
                    let node = self.nodes.get(&parent)?;
                    if !usable(node) {
                        return None;
                    }
                    nodes.push(parent);
                    cursor = node;
                }
            }
        };
        nodes.reverse();
        let pt = self.page_tokens;
        let available: usize = nodes.iter().map(|key| self.nodes[key].tail_pages).sum();
        let expected = (key.consumed_tokens / pt - parent_tokens / pt) as usize;
        if available != expected {
            return None;
        }
        let pages = (key.consumed_tokens / pt - resident / pt) as usize;
        Some(Match {
            key: *key,
            consumed_tokens: key.consumed_tokens,
            resident_tokens: resident,
            nodes,
            skip_pages: available - pages,
            pages,
        })
    }

    pub fn remove(&mut self, key: &Key) -> Result<Vec<Node<L>>> {
        let node = self
            .nodes
            .get(key)
            .ok_or_else(|| invalid("cannot remove an unindexed host record"))?;
        if node.children != 0 || node.pins != 0 {
            return Err(invalid(
                "cannot remove a pinned host record or one with children",
            ));
        }
        let mut removed = Vec::new();
        let mut next = Some(*key);
        while let Some(key) = next {
            let node = self.nodes.remove(&key).unwrap();
            if let Some(keys) = self.buckets.get_mut(&node.bucket) {
                keys.retain(|k| *k != key);
                if keys.is_empty() {
                    self.buckets.remove(&node.bucket);
                }
            }
            next = None;
            if let Some(parent) = node.parent {
                let parent_node = self.nodes.get_mut(&parent).unwrap();
                parent_node.children -= 1;
                if parent_node.children == 0 && !parent_node.row && parent_node.pins == 0 {
                    next = Some(parent);
                }
            }
            removed.push(node);
        }
        Ok(removed)
    }

    pub fn evict(
        &mut self,
        need_row: bool,
        evictable: impl Fn(&Node<L>) -> bool,
    ) -> Option<Eviction<L>> {
        let candidate = self
            .nodes
            .values()
            .filter(|node| {
                node.ready
                    && node.pins == 0
                    && evictable(node)
                    && (node.children == 0 || (need_row && node.row && node.children == 1))
            })
            .min_by_key(|node| (node.accessed, node.key.consumed_tokens))
            .map(|node| node.key)?;
        if self.nodes[&candidate].children == 0 {
            return self.remove(&candidate).ok().map(Eviction::Leaves);
        }
        self.nodes.get_mut(&candidate).unwrap().row = false;
        Some(Eviction::Compacted(candidate))
    }
}
