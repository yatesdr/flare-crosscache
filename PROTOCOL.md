# Flare cache manager protocol, version 1

This document defines the complete manager/client contract. All integers are
unsigned little-endian. Fields are packed, with no implicit alignment. A byte
array has exactly the stated length. `text` is a `u32` byte length followed by
UTF-8. A `key` is `u64 consumed_tokens, digest[16]`. An optional key is a `u8`
presence flag (0 or 1), followed by a key only when present. No other boolean
encoding is valid. Unsupported versions are rejected; changes to these layouts
or semantics require a new protocol version.

## Connection and framing

Use one Linux Unix stream socket per replica. The manager checks `SO_PEERCRED`:
both effective uid and effective gid must match. Hello's pid must equal the peer
pid. Run containers in the same PID namespace as their clients when sharing the
socket. The socket directory is mode 0770; the socket is 0660. There is no TCP
cache transport. `--metrics` is a separate HTTP endpoint.

Every frame is:

| Field | Bytes | Meaning |
|---|---:|---|
| length | 4 | Number of subsequent bytes; excludes this field |
| tag | 1 | Numeric tag below |
| generation | 8 | Connection generation |
| op | 8 | Operation number |
| payload | length − 17 | Tag-specific fields |

`17 <= length <= 4,194,368` (4 MiB + 64). Data bytes in a single Data or Commit
frame are at most 4,194,304. Short frames, invalid lengths and malformed requests
close the connection. A malformed payload belonging to an accepted Put instead
finishes that operation with `Stored{Err}`. There is no padding in framing.

The client assigns increasing op numbers, starting at 1, across Hello, Put, Get,
and Resync. Continuation frames reuse their operation's number. Zero is invalid;
there are no sentinel operation numbers. Op numbers must not wrap. The manager
assigns the generation in Welcome using a random boot nonce XOR an increasing
connection counter. Every u64 generation, including zero, is valid. Hello's
header generation is ignored; Welcome and Reject correlate by Hello's op.
After Welcome, both peers use its header generation for every frame. Clients
ignore previous generations. The manager drops and counts a wrong generation,
a continuation of an unknown/finished/cancelled op, a repeated new op number,
or an unknown tag. New operations must have a greater op than every earlier
new operation on the connection; rejected Put/Get requests also consume numbers.

Hello opens a connection control operation. Snapshot, Entry, SnapshotEnd, Delta
and Room use its op. Resync replaces that control operation with its own op.
The control operation lasts until Resync or EOF; data operations have independent
lifetimes. No Ping, Pong, Stats, or acknowledgements of notifications exist.

The manager admits at most 64 connections, with four open Puts and four open Gets
per connection. Reader channels hold one frame, writer channels eight commands,
and namespace disk queues eight tasks. Slow or overflowing writers disconnect;
clients discard their mirrors and reconnect. Each state-loop pass handles at
most one incoming frame per connection in round-robin order. Dead-client pin
cleanup releases at most 16 records per pass across all dead clients.

## Namespace descriptor

A descriptor encodes these fields in order:

| Field | Encoding |
|---|---|
| identity | 32 bytes, full model/offload identity |
| fingerprint | 16 bytes, layout fingerprint including rank count/order |
| page_tokens | u64, nonzero |
| rank_count | u32, nonzero |
| ranks | rank_count rank entries |
| page_record_bytes | u64 |
| row_record_bytes | u64 |

A rank entry is `u32 plane_count`, that many nonzero `u64 plane_unit_bytes`,
then nonzero `u64 row_unit_bytes`. Ranks and planes preserve logical geometry
order. Each unit occupies `ceil(unit_bytes / 4096) * 4096` bytes, including its
padding. A page record is page-major, then rank-major, then plane-major. A row
record is rank-major. The descriptor's last two fields must equal the sums of
the corresponding padded units. Sizes and derived offsets must not overflow
u64, and all rank entries must fit in the Flare header defined below.

The namespace directory is the lowercase hex of the 32-byte namespace id:
BLAKE2b-256 over `selene-ns-v1`, the full identity, the full fingerprint, the
tenant length byte and the tenant. The full descriptor is persisted and compared
alongside the name, so directory-name collisions are rejected. A namespace with
different sizes, full identity, fingerprint, tenant, or page_tokens cannot join
an existing descriptor. Different TP widths use different namespaces.

## Message layouts

An empty payload has zero bytes. `result` is `u8 status`: 0 means success with
no further bytes; 1 means failure followed by `text reason`. Reasons are diagnostic
strings, not stable enumerations. Receivers must not infer retry policy from text.

| Tag | Direction | Name | Payload in order |
|---:|---|---|---|
| 1 | C→M | Hello | u32 protocol_version, text replica_name, u32 pid, descriptor |
| 2 | M→C | Welcome | u32 version, u32 max_frame, u32 open_puts, u32 open_gets, u32 window_pages, u32 outbound_credits, u32 inbound_credits, u32 deadline_ms |
| 3 | M→C | Reject | text reason |
| 4 | M→C | Snapshot | u64 seq, u64 entry_count |
| 5 | M→C | Entry | entry, defined below |
| 6 | M→C | SnapshotEnd | u64 seq |
| 7 | M→C | Delta | u64 seq, u8 kind, entry for Added/Changed or key for Removed |
| 8 | M→C | Room | empty |
| 9 | C→M | Put | key, optional published_parent, bucket[16], u32 chain_pages, u32 tail_pages, u64 page_record_bytes, u64 row_record_bytes |
| 10 | M→C | Accept | u32 first_page, optional selected_parent |
| 11 | M→C | Present | empty |
| 12 | M→C | Retry | u8 reason: 0 Queue, 1 Budget, 2 Inflight |
| 13 | M→C | Oversize | empty |
| 14 | C→M | Window | u32 index, u32 pages, pages × u64 checksums |
| 15 | C→M | Data | u64 record_offset, remaining payload bytes |
| 15 | M→C | Data | u32 record_index, u64 record_offset, remaining payload bytes |
| 16 | C→M | Row | u64 checksum |
| 17 | C→M | Commit | u64 metadata_offset, remaining payload bytes |
| 18 | M→C | Stored | result |
| 19 | C→M | Get | u32 node_count, node_count × key, u32 skip_pages, u32 expected_pages, u8 row (must be 1) |
| 20 | M→C | Done | result |
| 21 | either | Cancel | empty; header op identifies the target operation |
| 22 | C→M | Resync | u64 last_good_seq |

Welcome currently advertises version 1, max_frame 4,194,368, four Puts, four Gets,
64 pages/window, two outbound and two inbound credits, and a 3000 ms deadline.
Replica names contain 1–256 UTF-8 bytes. A Reject is sent before closing.

An entry is `key, optional parent, bucket[16], u32 tail_pages, u8 row,
u8 tier, u8 durable, u64 bytes`. Tier 0 means DRAM, tier 1 means disk. A record
may have both copies; tier reports DRAM when retained there. `durable` means the
record and its entire ancestor chain have completed NVMe writes. `bytes` is the
NVMe reservation including the 4096-byte manager envelope; compaction reduces
it once the disk operation completes. Every entry is readable. Page-only
ancestors have row=0 and contribute pages but are never lookup endpoints.

Delta kind 0 is Added, 1 Changed, 2 Removed. Changed carries the complete current
entry, including row/tier/durable/bytes, so it also covers a compacted endpoint's
replacement. An Added parent precedes every Added child. Removed children precede
Removed parents. Sequences increase by exactly one per namespace mutation.

## Snapshot and mirror

After Welcome, the manager queues a snapshot at a single state boundary seq₀.
Exactly entry_count Entries follow, sorted by consumed count (parents first),
then SnapshotEnd with seq₀. No mutation is interleaved into that snapshot.
Subsequent Deltas start at seq₀+1, including mutations made while the writer
was sending the snapshot. There is no replay log exposed to clients.

The client builds a fresh index off the worker thread and swaps it at
SnapshotEnd. It applies at most 4096 queued delta entries per poll. A sequence
gap, decode error, or receiver backlog above 65,536 entries clears the mirror
and sends Resync with the last good sequence. Resync always produces a new full
snapshot and establishes a new control op; the last_good_seq is diagnostic and
does not request incremental replay. Ignore queued notifications from the old
control op after replacing the mirror. An empty mirror produces misses. The
manager confirms both Put deduplication and every Get independently of the mirror.

## Put lifecycle

`chain_pages == floor(key.consumed_tokens / page_tokens)`. The published parent,
if present, has a lower consumed count; tail_pages equals the difference of their
complete-page counts and never exceeds chain_pages. Without a parent, send the
whole chain as tail_pages. Put's byte sizes equal the descriptor.

The manager answers once with Accept, Present, Retry, or Oversize. Present means
an existing complete row endpoint; that op is finished. An open Put for the same
namespace/key yields Retry{Inflight}, including cancelled disk work still draining.
Queue limits yield Retry{Queue}. Budget admission, reservations and eviction
happen before Accept. Oversize means the manager envelope plus the selected Flare
record exceeds the NVMe byte limit; DRAM capacity alone never causes Oversize.

For a new key, a readable published parent with a row produces a child. Otherwise
the manager selects a root holding the whole chain. For replacement of an existing
page-only endpoint, its stored parent and page tail are preserved. `first_page`
is the selected parent's complete-page count, or zero for a root. The client
constructs its Header with the explicit selected_parent in Accept and
`chain_pages - first_page` tail pages. The selected parent is necessary even
when first_page is zero: a parent below one complete page is distinct from a
root. It also disambiguates preserved replacement parents. This is a wire
clarification of the design's shorthand `Accept{first_page}`.
The manager pins the parent across Accept→Stored/Cancel. Replacement uses a new
extent; old readers retain the original bytes/inode until their I/O ends.

Send windows in increasing page order. Window.index is the zero-based page index
within the selected record's tail, not within the full published chain. Each
window contains 1–64 pages; its checksums are xxh3-64 of each complete padded
page record. The Data frames that follow cover exactly those pages, in file
offset order, with no gaps, overlaps or extra bytes. All offsets and Data lengths
are 4096-aligned; Data is nonempty. A page may span frames. Finish a window's
Data before issuing the next Window. After all tail pages (including zero pages),
send Row with xxh3-64 of the complete padded row, then its Data. No next window,
row, or commit is valid while the current range is incomplete.

Finally send Commit fragments covering `[0, Header.page_segment)` in order:
4096-byte encoded Header followed by its padded variable checksum segment.
metadata_offset is relative to that metadata, begins at zero, and increases by
the previous fragment's byte count. Fragments are nonempty, 4096-aligned and at
most 4 MiB. The final fragment completes Commit; there is no separate end marker.
Fragmentation permits checksum tables larger than the frame limit. No further
Window, Row, or Data is allowed once Commit begins. The header's descriptor,
key, selected parent, bucket, tail and offsets must agree with Accept; its row
checksum must match Row; every checksum-table entry must match Window. Padding
is included in checksums. Header/checksum padding should be zero.

The manager verifies pages/row as Data lands. A mismatch, short committed stream,
bad offset, invalid header or checksum table yields Stored{Err}, with no new
publication. A valid DRAM record is published with Delta{Added/Changed} before
Stored{Ok}, then written to NVMe asynchronously. A larger record streams through
bounded staging into a temporary file and publishes once its disk write finishes.
Streaming children wait for their parent's durable completion before disk Data
writes. No child write can finish before its parent's write. Tier/durable changes
are sent as Changed deltas.

Stored{Ok} releases the client's publication pin. Present has the same successful
outcome with zero transferred bytes. Retry or Cancel/disconnect maps to Deferred
after the client's outstanding CUDA transfers drain; Oversize or Stored{Err}
maps to an error. A record acknowledged in DRAM can be lost on manager death
before it and its ancestor chain reach NVMe; clients do not wait for durability.

After Retry{Budget}, the manager tracks the largest refused reservation for that
connection. It sends one Room on the control op when freed/reclaimable capacity
can admit that reservation, then clears the notification. A new Budget refusal
can arm another Room. A retry may choose a different shape if its parent vanished.
A steady Budget refusal is not client progress. Client progress advances only
for snapshot application, Room, or freeing an open-op slot.

## Get lifecycle and flow control

Get names 1–65,536 ascending, consecutive chain nodes. Each node after the first
must name its predecessor as parent. The first may have a parent outside the
request. skip_pages cannot exceed the first node's tail. The sum of requested
nodes' tail pages minus skip_pages must equal expected_pages. The last node must
have a row. Missing keys, stale chain shape, or the Get concurrency limit yield
Done{Err} without payload data. All chain nodes are pinned for the lease.

For each record in request order (record_index starts at zero), Data sends:

1. Its full Header and checksum segment, offsets `[0, page_segment)`.
2. Its requested page range, skipping skip_pages only on the first record.
3. The row only for the final record.

Offsets refer to the Flare record, excluding the manager envelope. Skipped bytes
are not sent. Each range is contiguous and ascending. Page Data is divided into
windows of at most 64 pages from the first requested page in that record; a frame
never crosses a window boundary or includes more than 4 MiB. Metadata and row
also split into at most 4 MiB frames. Empty page ranges emit no page Data. Done{Ok}
follows the final Data. Records remain pinned until completion/Cancel/EOF; their
extents remain alive until the writer/disk reader's last reference disappears.
NVMe reads refill DRAM when an arena extent is available; corrupt bytes are never
retained by read-through.

The client validates Header, namespace/key/parent, geometry, each requested page's
xxh3 against its table, and the last row checksum before success. Done alone does
not finish a fetch: all H2D replies must also have arrived. A short stream,
mismatch, stale key, deadline or Done{Err} fails the fetch after H2D drain.

Credits are client-global across operations on this connection: two outbound
and two inbound windows of at most 64 pages, plus one row slab per credit, for
256 page slabs and four row slabs total. At most four operations per direction
may be open; the client issues/consumes windows only against these credits and
releases them after its corresponding D2H/send or H2D completion. Credits are
local scheduling state, not extra wire messages. Stream backpressure bounds the
manager writer while the client waits for an inbound credit. Never wait for a
whole Get in staging before submitting H2D windows.

## Cancellation and deadlines

Each data operation has a 3-second no-progress deadline. The manager counts a
valid operation frame, disk completion, or successful outbound chunk as progress.
The client counts its attributed frames, credits and CUDA replies. On expiry,
send Cancel using the target op in the header; the receiver answers nothing.
Cancel is idempotent via the unknown-op drop rule. No later continuation can
reopen that op. The manager may also cancel an expired operation.

On Cancel/EOF the manager releases leases and parent pins in bounded cleanup
steps and discards unpublished payloads. Disk/writer users retain references to
buffers and temporary/final extents until their I/O finishes. Cancelled keys stay
Inflight until that work drains, fencing delayed completion from a later Put.
The client stops issuing windows, drains outstanding CUDA replies, then releases
staging and publication pins or ticket ids. It discards old-generation work on
reconnect, builds a fresh snapshot, and reoffers Deferred publications still on
GPU. Recommended reconnect backoff is 100 ms increasing to 5 s.

## Flare record format (version 1)

All offsets below are relative to the Flare record. It begins 4096 bytes after
the start of the manager's NVMe file. Alignment A=4096. `padded(n)=ceil(n/A)*A`.
The checksum table is tail_pages little-endian u64 xxh3 values, padded to A.
Its offset is 4096 and its length is padded(tail_pages*8), including zero for a
zero-page tail. `page_segment = 4096 + checksum_bytes`;
`row_segment = page_segment + tail_pages*page_record_bytes`;
`length_with_row = row_segment + row_record_bytes`.

The Header occupies exactly 4096 bytes:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `FLARECKP` |
| 8 | 4 | record version = 1 |
| 12 | 4 | flags = 0 |
| 16 | 32 | identity |
| 48 | 16 | fingerprint |
| 64 | 8 | key consumed_tokens, nonzero |
| 72 | 16 | key digest |
| 88 | 1 | parent present, 0 or 1 |
| 89 | 8 | parent consumed_tokens, zero when absent |
| 97 | 16 | parent digest, zero when absent |
| 113 | 16 | bucket |
| 129 | 8 | page_tokens |
| 137 | 4 | tail_pages |
| 141 | 8 | page_segment |
| 149 | 8 | row_segment |
| 157 | 8 | row_checksum |
| 165 | 8 | created, Unix seconds |
| 173 | 4 | rank_count |
| 177 | variable | rank entries in the descriptor's rank-entry encoding |
| after ranks | remaining | zero padding |

Parent tokens are less than key tokens, and tail_pages equals the difference
of floor-divided complete-page counts. Roots use parent tokens zero. Zero tail
pages are legal when a checkpoint advances only the continuation row. A compacted
record has all metadata and pages but no row, ends at row_segment, and retains
its original Header/row checksum. It can supply pages to descendants but cannot
be fetched as an endpoint. Compaction and leaf eviction follow Flare's LRU:
choose the oldest ready, unpinned eligible leaf or single-child row ancestor;
compact the latter by dropping only its row. Removing a leaf cascades through
unpinned, childless page-only ancestors. Parents are never removed before children.

## Manager NVMe envelope and recovery

Each `<root>/<namespace>/<consumed:016x>-<digest:032hex>.rec` is a 4096-byte envelope
followed by the Flare record. The envelope contains:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `FLCMREC1` |
| 8 | 4 | envelope version = 1 |
| 12 | 4 | row present: 0 or 1 |
| 16 | 8 | Flare record length, excluding envelope |
| 24 | 4 | CRC32 of Header + complete padded checksum segment |
| 28 | 4 | CRC32 of envelope bytes 0..28 |
| 32 | 4064 | zero padding |

CRC32 is IEEE reflected polynomial 0xedb88320, initial 0xffffffff, final XOR
0xffffffff. CRC covers metadata; per-page xxh3 and row xxh3 protect payload.
The embedded metadata supplies the full namespace, key, parent, bucket, tail,
payload length and checksum table. Descriptor files contain the wire descriptor
followed by its CRC32. The root's `.lock` is exclusively flocked for its lifetime,
including outstanding disk users. Each namespace has one ordered disk writer.

Writes use aligned O_DIRECT buffers, fdatasync (`File::sync_data`), same-directory
rename, then directory fsync. Compaction atomically installs a page-only copy,
so an interrupted compaction keeps the previous valid record. Recovery ignores
symlinks, removes regular `.*.tmp` files, checks envelope/header/layout/name/length
and metadata CRC, sorts parents first, and removes orphan descendants. Unreadable
files fail startup without deletion. Recovered entries start in the disk tier;
DRAM copies are lost at restart. The pooled record files are manager-owned and
must not be opened as Flare's local `.ckpt` cache.
