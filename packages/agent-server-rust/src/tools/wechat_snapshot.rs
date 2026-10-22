//! Source-isolated reads of a WeChat SQLCipher database.
//!
//! The live shard is never handed to SQLite. Its bytes and its `-wal` are read
//! with ordinary `O_RDONLY` file reads, the committed WAL prefix is merged into
//! a private image, and only that image is opened by SQLCipher (`READ_ONLY`,
//! `URI`, `immutable=1`). The source is therefore never locked, never needs its
//! `-shm`, and is never checkpointed or rewritten.
//!
//! Two properties are deliberate and easy to get wrong:
//!
//! * **Original identity.** The copy is a byte image, not a logical export, so
//!   every rowid is preserved — including implicit rowids of tables without a
//!   primary key, which `sqlcipher_export` renumbers.
//! * **Page authentication.** SQLCipher verifies a page's HMAC when the page is
//!   read, so a corrupted page the query never touches is not noticed. Unlike
//!   the old host-side page-by-page decryptor, this does not authenticate every
//!   logical page up front; `PRAGMA cipher_integrity_check` can, on the private
//!   copy, but is not run per request.
//!
//! A bad source is reported, never silently downgraded: a WAL that is present
//! but malformed fails the read, and only an absent WAL means "no journal". The
//! deadline is observed between chunks and at each stage; it cannot preempt a
//! blocking kernel read or the key-derivation call. A source whose main file,
//! WAL, merge index and merged image together exceed
//! [`SNAPSHOT_MAX_INPUT_BYTES`] is refused rather than read.

use rusqlite::{Connection, OpenFlags};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use super::wechat_messages::CollectionCancel;

/// Combined ceiling for one snapshot: the main file, its WAL, the merge's page
/// index and the merged image, measured before the merged image is allocated.
///
/// A shard above this is refused rather than read. The limit is a temporary
/// safety bound for this stage, not a claim that larger histories work.
pub const SNAPSHOT_MAX_INPUT_BYTES: usize = 512 * 1024 * 1024;

/// Budget used when the caller has no deadline of its own (the shared,
/// non-cancellable read helpers). A never-ending read is not an option.
pub const SNAPSHOT_DEFAULT_BUDGET: Duration = Duration::from_secs(8);

/// SQLCipher's page size, fixed for WeChat databases and assumed by the merge.
const PAGE_SIZE: usize = 4096;
/// Chunk size of one source read, so a cancellation can land mid-file.
const READ_CHUNK_BYTES: usize = 1024 * 1024;
/// Chunk size of the private write, so a cancellation can land mid-write.
const WRITE_CHUNK_BYTES: usize = 1024 * 1024;
/// VDBE steps between two deadline checks inside one SQLite statement.
const SNAPSHOT_PROGRESS_STEPS: i32 = 1000;
/// WAL file format version.
const WAL_VERSION: u32 = 3_007_000;
/// WAL magic with little-endian checksums (`WAL_MAGIC` on a little-endian host).
const WAL_MAGIC_LE: u32 = 0x377f_0682;
/// WAL magic with big-endian checksums.
const WAL_MAGIC_BE: u32 = 0x377f_0683;
/// Conservative per-page cost of the merge's ordered page index, so the input
/// budget accounts for it and not only for page payloads.
const MERGE_INDEX_BYTES_PER_PAGE: usize = 96;
/// Worst-case per-frame cost of the parsed frame index, including the `Commit`
/// entries and the `BTreeMap` nodes one merged page implies.
const INDEX_ENTRY_BYTES_PER_FRAME_UPPER: u64 = 160;
/// A hex key longer than this is not a WeChat key.
const MAX_KEY_HEX_LEN: usize = 128;

/// Coarse, closed set of source-snapshot failures. Fieldless: no path, key or
/// SQL is carried, so the value is safe to log and to map onto an HTTP code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotError {
    /// The source file or its WAL could not be read at all (missing, not a
    /// regular file, permissions).
    CannotOpen,
    /// The merged image is not a database this key can read.
    NotADatabase,
    /// The source changed while it was being captured, or its WAL is torn.
    SourceChanged,
    /// Source + WAL + index + image would exceed [`SNAPSHOT_MAX_INPUT_BYTES`].
    BudgetExceeded,
    /// Any other read or merge failure, including an unrecognisable WAL.
    DatabaseRead,
}

/// Identity of a source file at one instant, with nanosecond timestamps.
///
/// `dev`/`ino` catch a replaced file; `size`/`mtime`/`ctime` catch an in-place
/// rewrite that a path-based `stat` would otherwise miss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fingerprint {
    dev: u64,
    ino: u64,
    size: u64,
    mtime_ns: i128,
    ctime_ns: i128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceFingerprint {
    main: Fingerprint,
    wal: Option<Fingerprint>,
}

fn fingerprint(metadata: &fs::Metadata) -> Fingerprint {
    Fingerprint {
        dev: metadata.dev(),
        ino: metadata.ino(),
        size: metadata.size(),
        mtime_ns: i128::from(metadata.mtime()) * 1_000_000_000 + i128::from(metadata.mtime_nsec()),
        ctime_ns: i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec()),
    }
}

/// Fingerprint a path only when it is a regular file. A FIFO, device or
/// directory is refused here rather than opened and read into a block.
fn stat_regular(path: &Path) -> std::io::Result<Fingerprint> {
    let metadata = fs::metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    Ok(fingerprint(&metadata))
}

fn source_fingerprint(db_path: &Path) -> Result<SourceFingerprint, SnapshotError> {
    let wal = wal_path(db_path);
    let main = stat_regular(db_path).map_err(|_| SnapshotError::CannotOpen)?;
    let wal = match stat_regular(&wal) {
        Ok(meta) => Some(meta),
        Err(error) if is_not_found(&error) => None,
        Err(_) => return Err(SnapshotError::CannotOpen),
    };
    Ok(SourceFingerprint { main, wal })
}

fn is_not_found(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
}

/// `<db>-wal`, appended to the whole name the way SQLite does it (a dotted
/// extension must not be replaced).
fn wal_path(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_os_string();
    name.push("-wal");
    PathBuf::from(name)
}

/// A key the `PRAGMA` can carry as statement text: hex only, even length.
fn valid_key(hex_key: &str) -> bool {
    !hex_key.is_empty()
        && hex_key.len() <= MAX_KEY_HEX_LEN
        && hex_key.len() % 2 == 0
        && hex_key.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Read the source and its WAL as ordinary file bytes, then prove neither
/// changed while it was being read.
///
/// Only a missing `-wal` counts as "no WAL"; any other stat failure is an open
/// failure, never a silent empty journal. `after_read` runs after the bytes are
/// in memory and before the closing stats, which is the seam a test uses to
/// change the source deterministically.
fn capture_bytes(
    db_path: &Path,
    cancel: &CollectionCancel,
    max_input: usize,
    after_read: &dyn Fn(),
) -> Result<(Vec<u8>, Vec<u8>), SnapshotError> {
    let wal = wal_path(db_path);

    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    let main_pre = stat_regular(db_path).map_err(|_| SnapshotError::CannotOpen)?;
    let wal_pre = match stat_regular(&wal) {
        Ok(meta) => Some(meta),
        Err(error) if is_not_found(&error) => None,
        Err(_) => return Err(SnapshotError::CannotOpen),
    };

    let mut total = main_pre.size;
    if let Some(meta) = wal_pre {
        total = total
            .checked_add(meta.size)
            .ok_or(SnapshotError::BudgetExceeded)?;
    }
    if total > max_input as u64 {
        return Err(SnapshotError::BudgetExceeded);
    }

    let main_file = File::open(db_path).map_err(|_| SnapshotError::CannotOpen)?;
    let held = fingerprint(&main_file.metadata().map_err(|_| SnapshotError::CannotOpen)?);
    if held != main_pre {
        return Err(SnapshotError::SourceChanged);
    }
    let mut main = Vec::new();
    read_bounded(&main_file, main_pre.size, cancel, &mut main)?;
    drop(main_file);

    let mut journal = Vec::new();
    if let Some(meta) = wal_pre {
        let wal_file = File::open(&wal).map_err(|_| SnapshotError::CannotOpen)?;
        let held = fingerprint(&wal_file.metadata().map_err(|_| SnapshotError::CannotOpen)?);
        if held != meta {
            return Err(SnapshotError::SourceChanged);
        }
        read_bounded(&wal_file, meta.size, cancel, &mut journal)?;
        drop(wal_file);
    }

    after_read();

    match stat_regular(db_path) {
        Ok(post) if post == main_pre => {}
        _ => return Err(SnapshotError::SourceChanged),
    }
    match (wal_pre, stat_regular(&wal)) {
        (Some(meta), Ok(post)) if post == meta => {}
        (None, Err(error)) if is_not_found(&error) => {}
        // A journal that appeared, vanished or changed during the read may hold
        // a commit we did not copy: the snapshot is stale, not a valid state.
        _ => return Err(SnapshotError::SourceChanged),
    }

    Ok((main, journal))
}

/// Read at most `expected` bytes, checking cancellation between chunks. A file
/// that ends before `expected` changed under the reader.
fn read_bounded(
    mut file: &File,
    expected: u64,
    cancel: &CollectionCancel,
    out: &mut Vec<u8>,
) -> Result<(), SnapshotError> {
    out.reserve(expected as usize);
    let mut buffer = vec![0u8; READ_CHUNK_BYTES];
    let mut remaining = expected;
    while remaining > 0 {
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        let want = remaining.min(READ_CHUNK_BYTES as u64) as usize;
        let read = file
            .read(&mut buffer[..want])
            .map_err(|_| SnapshotError::CannotOpen)?;
        if read == 0 {
            return Err(SnapshotError::SourceChanged);
        }
        out.extend_from_slice(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(())
}

fn be32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("four bytes"))
}

/// The rolling WAL checksum over 8-byte words.
fn wal_checksum(data: &[u8], a: u32, b: u32, big_endian: bool) -> (u32, u32) {
    let (mut a, mut b) = (a, b);
    for chunk in data.chunks_exact(8) {
        let (x, y) = if big_endian {
            (
                u32::from_be_bytes(chunk[0..4].try_into().expect("four bytes")),
                u32::from_be_bytes(chunk[4..8].try_into().expect("four bytes")),
            )
        } else {
            (
                u32::from_le_bytes(chunk[0..4].try_into().expect("four bytes")),
                u32::from_le_bytes(chunk[4..8].try_into().expect("four bytes")),
            )
        };
        a = a.wrapping_add(x).wrapping_add(b);
        b = b.wrapping_add(y).wrapping_add(a);
    }
    (a, b)
}

/// A commit boundary: the frame count after it and the database size in pages
/// after that transaction.
struct Commit {
    end: usize,
    pages: u64,
}

/// The structurally valid prefix of a WAL, up to its last commit.
struct WalPrefix {
    /// `(page number, offset of the page payload inside the WAL)`, in file order.
    frames: Vec<(u32, usize)>,
    commits: Vec<Commit>,
}

/// Parse a WAL far enough to find its complete committed prefix.
///
/// An absent or empty WAL has no frames. A present but malformed WAL is an
/// error, never a silent fallback to the older main file: a non-empty header
/// that is too short is a torn tail ([`SnapshotError::SourceChanged`]), and an
/// unrecognisable magic, version, page size or header checksum is
/// [`SnapshotError::DatabaseRead`]. Frame-level damage is different: a bad salt
/// or checksum ends the prefix at the last commit, which is a legal WAL tail.
fn parse_wal(wal: &[u8], cancel: &CollectionCancel) -> Result<WalPrefix, SnapshotError> {
    let mut prefix = WalPrefix {
        frames: Vec::new(),
        commits: Vec::new(),
    };
    if wal.is_empty() {
        return Ok(prefix);
    }
    if wal.len() < 32 {
        return Err(SnapshotError::SourceChanged);
    }
    let magic = be32(&wal[0..4]);
    let big_endian = if magic == WAL_MAGIC_LE {
        false
    } else if magic == WAL_MAGIC_BE {
        true
    } else {
        return Err(SnapshotError::DatabaseRead);
    };
    if be32(&wal[4..8]) != WAL_VERSION
        || be32(&wal[8..12]) as usize != PAGE_SIZE
        || (be32(&wal[24..28]), be32(&wal[28..32])) != wal_checksum(&wal[0..24], 0, 0, big_endian)
    {
        return Err(SnapshotError::DatabaseRead);
    }

    let salt = &wal[16..24];
    let mut running = (be32(&wal[24..28]), be32(&wal[28..32]));
    let mut offset = 32usize;
    while offset + 24 + PAGE_SIZE <= wal.len() {
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        let header = &wal[offset..offset + 24];
        let page = &wal[offset + 24..offset + 24 + PAGE_SIZE];
        if header[8..16] != *salt {
            break;
        }
        let mid = wal_checksum(&header[0..8], running.0, running.1, big_endian);
        let next = wal_checksum(page, mid.0, mid.1, big_endian);
        if next != (be32(&header[16..20]), be32(&header[20..24])) {
            break;
        }
        running = next;
        let page_number = be32(&header[0..4]);
        let db_size = be32(&header[4..8]);
        if page_number == 0 {
            break;
        }
        prefix.frames.push((page_number, offset + 24));
        if db_size != 0 {
            prefix.commits.push(Commit {
                end: prefix.frames.len(),
                pages: u64::from(db_size),
            });
        }
        offset += 24 + PAGE_SIZE;
    }
    Ok(prefix)
}

/// Precheck the merge's index from the WAL length alone, before parsing it.
///
/// `parse_wal` builds a frame vector and a commit vector proportional to the
/// frame count; `merge_wal` then keeps an ordered page map. All of that is
/// bounded here from the file sizes, so an oversized source is refused before
/// those allocations happen rather than after.
fn check_index_budget(
    wal_len: u64,
    main_len: u64,
    max_input: usize,
) -> Result<(), SnapshotError> {
    let frame_count = wal_len / (24 + PAGE_SIZE as u64);
    let index_upper = frame_count
        .checked_mul(INDEX_ENTRY_BYTES_PER_FRAME_UPPER)
        .ok_or(SnapshotError::BudgetExceeded)?;
    let reserved = main_len
        .checked_add(wal_len)
        .and_then(|total| total.checked_add(index_upper))
        .ok_or(SnapshotError::BudgetExceeded)?;
    if reserved > max_input as u64 {
        return Err(SnapshotError::BudgetExceeded);
    }
    Ok(())
}

/// Merge the committed WAL prefix into the database file image.
///
/// Truncation is applied per commit, in order. A page that an earlier
/// transaction wrote and a later commit shrinks away is no longer part of the
/// image: if a still-later commit grows past that point without writing the
/// page again, it is a missing page and the merge fails instead of reviving the
/// old main-file bytes under it. Physical length is never shrunk
/// (preallocation is preserved), and bytes past the logical end are zeroed so
/// no stale data sits inside the returned image. A page the logical image needs
/// but neither the main file nor the committed WAL provides is a hard error,
/// not a zero-filled hole.
fn merge_wal(
    main: &[u8],
    wal: &[u8],
    max_input: usize,
    cancel: &CollectionCancel,
) -> Result<Vec<u8>, SnapshotError> {
    if main.len() < PAGE_SIZE || main.len() % PAGE_SIZE != 0 {
        return Err(SnapshotError::DatabaseRead);
    }
    check_index_budget(wal.len() as u64, main.len() as u64, max_input)?;
    let main_pages = main.len() / PAGE_SIZE;

    let prefix = parse_wal(wal, cancel)?;
    let final_pages = match prefix.commits.last() {
        Some(commit) => commit.pages,
        None => main_pages as u64,
    };
    let usable_end = prefix
        .commits
        .last()
        .map(|commit| commit.end)
        .unwrap_or(0);

    // `main_safe_prefix` is the smallest page count any commit has truncated the
    // image to (the first truncation). Main pages above it are no longer part of
    // the image: if a later commit grows back into them, every such page needs a
    // frame written after that truncation, or the stale main bytes would revive.
    // Pages at or below it are still covered by the intact main prefix.
    let mut pages: BTreeMap<u32, usize> = BTreeMap::new();
    let mut highest: u32 = 0;
    let mut main_safe_prefix: Option<u32> = None;
    let mut next_commit = 0usize;
    for (index, &(page_number, offset)) in prefix.frames[..usable_end].iter().enumerate() {
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        pages.insert(page_number, offset);
        highest = highest.max(page_number);
        while next_commit < prefix.commits.len() && prefix.commits[next_commit].end == index + 1 {
            let limit = prefix.commits[next_commit].pages;
            // Truncate the WAL page map only when this commit drops pages we
            // actually hold, so a grow-only WAL stays linear.
            if u64::from(highest) > limit {
                pages.retain(|&page, _| u64::from(page) <= limit);
                highest = pages.keys().next_back().copied().unwrap_or(0);
            }
            // A commit that shrinks below the physical main file truncates main
            // pages. Keep the FIRST such limit: later commits that grow back into
            // those pages do not make the old main bytes valid again.
            if (main_pages as u64) > limit {
                let floor = (limit as u32).min(main_pages as u32);
                main_safe_prefix = Some(match main_safe_prefix {
                    Some(current) => current.min(floor),
                    None => floor,
                });
            }
            next_commit += 1;
        }
    }

    if final_pages == 0 || final_pages > (max_input / PAGE_SIZE) as u64 {
        return Err(SnapshotError::BudgetExceeded);
    }
    let page_count = (main_pages as u64).max(final_pages);
    let merged_len = (page_count as usize)
        .checked_mul(PAGE_SIZE)
        .ok_or(SnapshotError::BudgetExceeded)?;
    let index_bytes = prefix
        .frames
        .len()
        .checked_mul(std::mem::size_of::<(u32, usize)>())
        .and_then(|bytes| {
            bytes.checked_add(pages.len().saturating_mul(MERGE_INDEX_BYTES_PER_PAGE))
        })
        .ok_or(SnapshotError::BudgetExceeded)?;
    let reserved = main
        .len()
        .checked_add(wal.len())
        .and_then(|total| total.checked_add(index_bytes))
        .and_then(|total| total.checked_add(merged_len))
        .ok_or(SnapshotError::BudgetExceeded)?;
    if reserved > max_input {
        return Err(SnapshotError::BudgetExceeded);
    }

    let mut image = vec![0u8; merged_len];
    copy_chunked(
        |range| image[range.clone()].copy_from_slice(&main[range]),
        main.len(),
        cancel,
    )?;
    for (&page_number, &offset) in &pages {
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        if offset + PAGE_SIZE > wal.len() {
            return Err(SnapshotError::DatabaseRead);
        }
        let index = (page_number as usize - 1) * PAGE_SIZE;
        image[index..index + PAGE_SIZE].copy_from_slice(&wal[offset..offset + PAGE_SIZE]);
    }

    // Every logical page above the main prefix must have a committed frame, and
    // only pages that a truncation actually cut out of the main file need a
    // later frame. Pages beyond the logical size fall outside the image.
    let extended_start = main_pages as u64 + 1;
    let logical_end = final_pages;
    for page in (extended_start..=logical_end).map(|page| page as u32) {
        if !pages.contains_key(&page) {
            return Err(SnapshotError::DatabaseRead);
        }
    }
    if let Some(safe_prefix) = main_safe_prefix {
        // Every page above the smallest truncated main prefix, up to the logical
        // end, must have a frame written after that truncation — even when the
        // logical end grows past the old main file (the regrow case).
        for page in (u64::from(safe_prefix) + 1..=logical_end).map(|p| p as u32) {
            if !pages.contains_key(&page) {
                return Err(SnapshotError::DatabaseRead);
            }
        }
    }
    // Bytes past the logical end are not part of the image.
    let logical_bytes = (logical_end as usize)
        .checked_mul(PAGE_SIZE)
        .ok_or(SnapshotError::BudgetExceeded)?;
    if logical_bytes < image.len() {
        image[logical_bytes..].fill(0);
    }

    Ok(image)
}

/// Copy `len` bytes into `dest` in chunks, checking cancellation between them.
fn copy_chunked(
    mut dest: impl FnMut(std::ops::Range<usize>),
    len: usize,
    cancel: &CollectionCancel,
) -> Result<(), SnapshotError> {
    let mut at = 0usize;
    while at < len {
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        let end = (at + READ_CHUNK_BYTES).min(len);
        dest(at..end);
        at = end;
    }
    Ok(())
}

/// A private copy of the merged image: a 0700 directory holding one 0600 file,
/// removed on drop unless it was already released.
struct PrivateCopy {
    dir: PathBuf,
    file: PathBuf,
    armed: bool,
}

const SNAPSHOT_CACHE_LIMIT: usize = 4;

struct CachedCopy {
    path: PathBuf,
    source: SourceFingerprint,
    hex_key: String,
    copy: Arc<PrivateCopy>,
}

static SNAPSHOT_CACHE: std::sync::OnceLock<Mutex<Vec<CachedCopy>>> = std::sync::OnceLock::new();

fn snapshot_cache() -> &'static Mutex<Vec<CachedCopy>> {
    SNAPSHOT_CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

fn with_snapshot_cache<R>(action: impl FnOnce(&mut Vec<CachedCopy>) -> R) -> R {
    let mut cache = snapshot_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    action(&mut cache)
}

/// Release this process's retained snapshot paths after requests have drained.
pub(crate) fn clear_snapshot_cache() {
    with_snapshot_cache(Vec::clear);
}

fn cached_copy(
    db_path: &Path,
    source: SourceFingerprint,
    hex_key: &str,
) -> Option<Arc<PrivateCopy>> {
    with_snapshot_cache(|cache| {
        let index = cache.iter().position(|entry| {
            entry.path == db_path && entry.source == source && entry.hex_key == hex_key
        });
        match index {
            Some(index) => {
                let entry = cache.remove(index);
                let copy = Arc::clone(&entry.copy);
                cache.insert(0, entry);
                Some(copy)
            }
            None => {
                // Do not retain an older version while rebuilding this source.
                // An opened connection owns its own fd; Arc only keeps the cached path alive
                // until a new connection has opened it.
                cache.retain(|entry| entry.path != db_path || entry.hex_key != hex_key);
                None
            }
        }
    })
}

fn cache_copy(
    db_path: &Path,
    source: SourceFingerprint,
    hex_key: &str,
    copy: Arc<PrivateCopy>,
) {
    with_snapshot_cache(|cache| {
        cache.retain(|entry| entry.path != db_path || entry.hex_key != hex_key);
        cache.insert(
            0,
            CachedCopy {
                path: db_path.to_path_buf(),
                source,
                hex_key: hex_key.to_owned(),
                copy,
            },
        );
        cache.truncate(SNAPSHOT_CACHE_LIMIT);
    });
}

impl PrivateCopy {
    fn new(image: &[u8], cancel: &CollectionCancel) -> Result<Self, SnapshotError> {
        let root = std::env::temp_dir();
        for _ in 0..16 {
            let dir = root.join(format!("wechat-snapshot-{}", uuid::Uuid::new_v4().simple()));
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&dir) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(SnapshotError::DatabaseRead),
            }
            let file = dir.join("snapshot.db");
            let opened = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&file);
            let mut handle = match opened {
                Ok(handle) => handle,
                Err(_) => {
                    let _ = fs::remove_dir(&dir);
                    return Err(SnapshotError::DatabaseRead);
                }
            };
            let mut written = 0usize;
            while written < image.len() {
                if cancel.expired() {
                    drop(handle);
                    let _ = fs::remove_file(&file);
                    let _ = fs::remove_dir(&dir);
                    return Err(SnapshotError::DatabaseRead);
                }
                let end = (written + WRITE_CHUNK_BYTES).min(image.len());
                if handle.write_all(&image[written..end]).is_err() {
                    drop(handle);
                    let _ = fs::remove_file(&file);
                    let _ = fs::remove_dir(&dir);
                    return Err(SnapshotError::DatabaseRead);
                }
                written = end;
            }
            return Ok(Self {
                dir,
                file,
                armed: true,
            });
        }
        Err(SnapshotError::DatabaseRead)
    }

}

impl Drop for PrivateCopy {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.file);
            let _ = fs::remove_dir(&self.dir);
        }
    }
}

/// Percent-encode the bytes a SQLite URI cannot carry literally.
fn escape_uri_path(path: &Path) -> String {
    let bytes = path.as_os_str().as_bytes();
    let mut escaped = String::with_capacity(bytes.len());
    for &byte in bytes {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                escaped.push(byte as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(escaped, "%{byte:02X}");
            }
        }
    }
    escaped
}

fn classify_database_error(error: rusqlite::Error) -> SnapshotError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == rusqlite::ffi::ErrorCode::NotADatabase =>
        {
            SnapshotError::NotADatabase
        }
        _ => SnapshotError::DatabaseRead,
    }
}

/// Open the private copy with SQLCipher and prove the key derives.
///
/// An interrupt is installed before the first statement, so the one schema read
/// — which forces the first page to decrypt and turns a wrong key into
/// [`SnapshotError::NotADatabase`] — is itself bounded by the deadline.
fn open_copy_connection(
    copy: &PrivateCopy,
    hex_key: &str,
    cancel: &CollectionCancel,
) -> Result<Connection, SnapshotError> {
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    let uri = format!("file:{}?immutable=1", escape_uri_path(&copy.file));
    let connection = Connection::open_with_flags(
        &uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| SnapshotError::DatabaseRead)?;
    let guard = cancel.clone();
    connection.progress_handler(
        SNAPSHOT_PROGRESS_STEPS,
        Some(move || guard.expired()),
    );
    connection
        .execute_batch(&format!(
            "PRAGMA key = \"x'{hex_key}'\"; PRAGMA cipher_compatibility = 4; PRAGMA query_only = 1;"
        ))
        .map_err(classify_database_error)?;
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    connection
        .query_row("SELECT count(*) FROM sqlite_master", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(classify_database_error)?;
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    Ok(connection)
}

/// Open a read-only `Connection` on an isolated copy of `db_path`.
///
/// `cancel` is the caller's deadline; `None` gives this layer its own
/// [`SNAPSHOT_DEFAULT_BUDGET`]. The returned connection owns the only file
/// descriptor it opened on the private copy. The verified private copy remains
/// in the bounded in-process cache until it is replaced or evicted; its path
/// is then unlinked by [`PrivateCopy::drop`].
pub fn open_snapshot_conn(
    db_path: &Path,
    hex_key: &str,
    cancel: Option<&CollectionCancel>,
) -> Result<Connection, SnapshotError> {
    open_snapshot_conn_inner(db_path, hex_key, cancel, SNAPSHOT_MAX_INPUT_BYTES, &|| {})
}

/// Return true only when the new credential authenticates the isolated source,
/// the previous credential fails its schema read against that same source
/// version, and the source fingerprint remains unchanged across both checks.
pub(crate) fn verify_key_replacement(db_path: &Path, candidate: &str, previous: &str) -> bool {
    let Ok(source_before) = source_fingerprint(db_path) else { return false; };
    if open_snapshot_conn(db_path, candidate, None).is_err() { return false; }
    let previous_failed = matches!(
        open_snapshot_conn(db_path, previous, None),
        Err(SnapshotError::NotADatabase)
    );
    let Ok(source_after) = source_fingerprint(db_path) else { return false; };
    previous_failed && source_before == source_after
}

fn open_snapshot_conn_inner(
    db_path: &Path,
    hex_key: &str,
    cancel: Option<&CollectionCancel>,
    max_input: usize,
    after_read: &dyn Fn(),
) -> Result<Connection, SnapshotError> {
    // The source must exist before the key is even judged, so a missing or
    // unreadable source classifies as `CannotOpen` regardless of the key.
    stat_regular(db_path).map_err(|_| SnapshotError::CannotOpen)?;
    if !valid_key(hex_key) {
        return Err(SnapshotError::NotADatabase);
    }
    let owned;
    let cancel = match cancel {
        Some(cancel) => cancel,
        None => {
            owned = CollectionCancel::within(SNAPSHOT_DEFAULT_BUDGET);
            &owned
        }
    };
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }

    let source = source_fingerprint(db_path)?;

    if let Some(copy) = cached_copy(db_path, source, hex_key) {
        let connection = open_copy_connection(&copy, hex_key, cancel)?;
        if cancel.expired() {
            return Err(SnapshotError::DatabaseRead);
        }
        let current =
            source_fingerprint(db_path).map_err(|_| SnapshotError::SourceChanged)?;
        if current != source {
            return Err(SnapshotError::SourceChanged);
        }
        return Ok(connection);
    }

    let (main, journal) = capture_bytes(db_path, cancel, max_input, after_read)?;
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    let image = merge_wal(&main, &journal, max_input, cancel)?;
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    let copy = Arc::new(PrivateCopy::new(&image, cancel)?);
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    let connection = open_copy_connection(&copy, hex_key, cancel)?;
    if cancel.expired() {
        return Err(SnapshotError::DatabaseRead);
    }
    let current = source_fingerprint(db_path).map_err(|_| SnapshotError::SourceChanged)?;
    if current != source {
        return Err(SnapshotError::SourceChanged);
    }
    cache_copy(db_path, source, hex_key, copy);
    Ok(connection)
}
