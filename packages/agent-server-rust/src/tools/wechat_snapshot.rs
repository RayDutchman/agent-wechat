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
#[cfg(not(test))]
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

/// Test-only seams that trip the cancel at a chosen stage, so a test reaches a
/// mid-merge or mid-write expiry deterministically instead of racing the clock.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Merge,
    Copy,
    Write,
    Schema,
    BeforeReturn,
}

#[cfg(test)]
thread_local! {
    static TRIP: std::cell::RefCell<Option<(Stage, CollectionCancel)>> =
        const { std::cell::RefCell::new(None) };
    static LAST_COPY_DIR: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn set_trip(stage: Stage, cancel: &CollectionCancel) {
    TRIP.with(|slot| *slot.borrow_mut() = Some((stage, cancel.clone())));
}

#[cfg(test)]
fn clear_trip() {
    TRIP.with(|slot| *slot.borrow_mut() = None);
}

#[cfg(test)]
fn trip(stage: Stage) {
    TRIP.with(|slot| {
        if let Some((wanted, cancel)) = slot.borrow().as_ref() {
            if *wanted == stage {
                cancel.cancel();
            }
        }
    });
}

/// Let a caller in another module trip the merge stage. Test-only.
#[cfg(test)]
pub(crate) fn test_trip_on_merge(cancel: &CollectionCancel) {
    set_trip(Stage::Merge, cancel);
}

/// Clear any trip set by [`test_trip_on_merge`]. Test-only.
#[cfg(test)]
pub(crate) fn test_clear_trip() {
    clear_trip();
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
        #[cfg(test)]
        trip(Stage::Merge);
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
        #[cfg(test)]
        trip(Stage::Merge);
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
        #[cfg(test)]
        trip(Stage::Copy);
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
        #[cfg(test)]
        trip(Stage::Copy);
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

#[cfg(not(test))]
static SNAPSHOT_CACHE: std::sync::OnceLock<Mutex<Vec<CachedCopy>>> = std::sync::OnceLock::new();

#[cfg(not(test))]
fn snapshot_cache() -> &'static Mutex<Vec<CachedCopy>> {
    SNAPSHOT_CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(not(test))]
fn with_snapshot_cache<R>(action: impl FnOnce(&mut Vec<CachedCopy>) -> R) -> R {
    let mut cache = snapshot_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    action(&mut cache)
}

#[cfg(test)]
thread_local! {
    static TEST_SNAPSHOT_CACHE: std::cell::RefCell<Vec<CachedCopy>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

#[cfg(test)]
fn with_snapshot_cache<R>(action: impl FnOnce(&mut Vec<CachedCopy>) -> R) -> R {
    TEST_SNAPSHOT_CACHE.with(|cache| action(&mut cache.borrow_mut()))
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
            #[cfg(test)]
            LAST_COPY_DIR.with(|slot| *slot.borrow_mut() = Some(dir.clone()));
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
                #[cfg(test)]
                trip(Stage::Write);
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
    #[cfg(test)]
    trip(Stage::Schema);
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
        #[cfg(test)]
        trip(Stage::BeforeReturn);
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
    #[cfg(test)]
    trip(Stage::BeforeReturn);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::Duration;

    const TEST_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn within_seconds(seconds: u64) -> CollectionCancel {
        CollectionCancel::within(Duration::from_secs(seconds))
    }

    /// Real SQLCipher WAL database whose writer stays open: a checkpointed row,
    /// one committed `-wal` row and one uncommitted row.
    struct Fixture {
        _dir: tempfile::TempDir,
        path: PathBuf,
        writer: Connection,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("snapshot_messages.db");
            let writer = Connection::open(&path).unwrap();
            writer
                .execute_batch(&format!(
                    "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                     PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
                     CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT);
                     CREATE TABLE Name2Id (user_name TEXT PRIMARY KEY, is_session INTEGER);
                     CREATE TABLE VoiceInfo (chat_name_id INTEGER, voice_len INTEGER);
                     CREATE TABLE Plain (value TEXT);"
                ))
                .unwrap();
            writer
                .execute_batch(
                    "INSERT INTO messages VALUES (1, 'checkpointed');
                     INSERT INTO Name2Id (rowid, user_name, is_session) VALUES (2, 'self', 0), (58, 'peer', 1), (103, 'other', 1);
                     INSERT INTO VoiceInfo (rowid, chat_name_id, voice_len) VALUES (77, 58, 10), (9001, 58, 20);
                     INSERT INTO Plain (rowid, value) VALUES (7, 'a'), (900, 'b'), (5000, 'c');",
                )
                .unwrap();
            writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap();
            // Committed, still only in the -wal file.
            writer
                .execute("INSERT INTO messages VALUES (2, 'wal_only')", [])
                .unwrap();
            // Uncommitted: must never be visible to a snapshot.
            writer
                .execute_batch("BEGIN IMMEDIATE; INSERT INTO messages VALUES (3, 'uncommitted');")
                .unwrap();

            let fixture = Self {
                _dir: dir,
                path,
                writer,
            };
            assert!(
                std::fs::metadata(wal_path(&fixture.path)).unwrap().size() > 32,
                "fixture must keep the newest commit in the -wal file"
            );
            fixture
        }

        fn wal(&self) -> PathBuf {
            wal_path(&self.path)
        }

        fn shm(&self) -> PathBuf {
            let mut name = self.path.as_os_str().to_os_string();
            name.push("-shm");
            PathBuf::from(name)
        }
    }

    fn snapshot_rows(path: &Path, sql: &str) -> Vec<i64> {
        let cancel = within_seconds(60);
        let connection = open_snapshot_conn(path, TEST_KEY, Some(&cancel)).unwrap();
        let mut statement = connection.prepare(sql).unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<i64>, _>>()
            .unwrap()
    }

    fn last_copy_dir() -> Option<PathBuf> {
        LAST_COPY_DIR.with(|slot| slot.borrow().clone())
    }

    #[test]
    fn reads_committed_wal_rows_and_excludes_uncommitted_ones() {
        let fixture = Fixture::new();
        assert_eq!(
            snapshot_rows(&fixture.path, "SELECT id FROM messages ORDER BY id"),
            vec![1, 2],
            "the committed -wal row is visible and the uncommitted row is not"
        );
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn preserves_rowids_including_implicit_ones() {
        let fixture = Fixture::new();
        assert_eq!(
            snapshot_rows(&fixture.path, "SELECT rowid FROM Name2Id ORDER BY rowid"),
            vec![2, 58, 103]
        );
        assert_eq!(
            snapshot_rows(&fixture.path, "SELECT rowid FROM VoiceInfo ORDER BY rowid"),
            vec![77, 9001]
        );
        assert_eq!(
            snapshot_rows(&fixture.path, "SELECT rowid FROM Plain ORDER BY rowid"),
            vec![7, 900, 5000]
        );
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_join_through_a_preserved_rowid_still_resolves() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        let connection = open_snapshot_conn(&fixture.path, TEST_KEY, Some(&cancel)).unwrap();
        let name: String = connection
            .query_row(
                "SELECT n.user_name FROM VoiceInfo v JOIN Name2Id n ON n.rowid = v.chat_name_id \
                 WHERE v.rowid = 77",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "peer");
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn never_touches_the_source_bytes_or_sidecars() {
        let fixture = Fixture::new();
        let main_before = std::fs::read(&fixture.path).unwrap();
        let wal_before = std::fs::read(fixture.wal()).unwrap();
        let shm_before = std::fs::read(fixture.shm()).unwrap_or_default();

        assert_eq!(
            snapshot_rows(&fixture.path, "SELECT id FROM messages ORDER BY id"),
            vec![1, 2]
        );

        assert_eq!(std::fs::read(&fixture.path).unwrap(), main_before);
        assert_eq!(std::fs::read(fixture.wal()).unwrap(), wal_before);
        assert_eq!(std::fs::read(fixture.shm()).unwrap_or_default(), shm_before);
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_change_during_the_capture_is_refused() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        let wal = fixture.wal();
        let result = open_snapshot_conn_inner(
            &fixture.path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &move || {
                let mut handle = OpenOptions::new().append(true).open(&wal).unwrap();
                handle.write_all(&[0u8]).unwrap();
            },
        );
        assert!(matches!(result, Err(SnapshotError::SourceChanged)));
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn an_expiry_after_the_capture_is_refused() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        let trip_cancel = cancel.clone();
        let result = open_snapshot_conn_inner(
            &fixture.path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &move || trip_cancel.cancel(),
        );
        assert!(matches!(result, Err(SnapshotError::DatabaseRead)));
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_cancel_during_the_merge_creates_no_private_copy() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        set_trip(Stage::Merge, &cancel);
        let result = open_snapshot_conn(&fixture.path, TEST_KEY, Some(&cancel));
        clear_trip();
        assert!(matches!(result, Err(SnapshotError::DatabaseRead)));
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_cancel_during_the_write_cleans_up_the_private_copy() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        set_trip(Stage::Write, &cancel);
        let result = open_snapshot_conn(&fixture.path, TEST_KEY, Some(&cancel));
        clear_trip();
        assert!(matches!(result, Err(SnapshotError::DatabaseRead)));
        let dir = last_copy_dir().expect("the write stage must have created a private copy");
        assert!(!dir.exists(), "a cancelled write must not leave its private copy");
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_cancel_before_the_schema_read_cleans_up_the_private_copy() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        set_trip(Stage::Schema, &cancel);
        let result = open_snapshot_conn(&fixture.path, TEST_KEY, Some(&cancel));
        clear_trip();
        assert!(matches!(result, Err(SnapshotError::DatabaseRead)));
        let dir = last_copy_dir().expect("the schema stage must have created a private copy");
        assert!(!dir.exists(), "a cancelled schema read must not leave its private copy");
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_cancel_before_return_cleans_up_the_private_copy() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        set_trip(Stage::BeforeReturn, &cancel);
        let result = open_snapshot_conn(&fixture.path, TEST_KEY, Some(&cancel));
        clear_trip();
        assert!(matches!(result, Err(SnapshotError::DatabaseRead)));
        let dir = last_copy_dir().expect("the return stage must have created a private copy");
        assert!(!dir.exists(), "a cancelled return must not leave its private copy");
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_wrong_key_is_not_a_database_not_an_empty_success() {
        let fixture = Fixture::new();
        let wrong = "22".repeat(32);
        let cancel = within_seconds(60);
        assert!(matches!(
            open_snapshot_conn(&fixture.path, &wrong, Some(&cancel)),
            Err(SnapshotError::NotADatabase)
        ));
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn a_missing_source_is_cannot_open_and_creates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.db");
        let cancel = within_seconds(60);
        assert!(matches!(
            open_snapshot_conn(&path, TEST_KEY, Some(&cancel)),
            Err(SnapshotError::CannotOpen)
        ));
        assert!(!path.exists());
    }

    #[test]
    fn a_missing_source_is_reported_before_a_bad_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.db");
        let cancel = within_seconds(60);
        // The path cannot be read, so that is the failure, not the key format.
        assert!(matches!(
            open_snapshot_conn(&path, "not-hex", Some(&cancel)),
            Err(SnapshotError::CannotOpen)
        ));
    }

    #[test]
    fn an_oversized_source_is_budget_exceeded() {
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        // Smaller than even the main file, so the pre-read budget refuses it.
        assert!(matches!(
            open_snapshot_conn_inner(&fixture.path, TEST_KEY, Some(&cancel), 1024, &|| {}),
            Err(SnapshotError::BudgetExceeded)
        ));
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn an_expired_deadline_stops_before_reading() {
        let fixture = Fixture::new();
        let expired = CollectionCancel::within(Duration::ZERO);
        assert!(matches!(
            open_snapshot_conn(&fixture.path, TEST_KEY, Some(&expired)),
            Err(SnapshotError::DatabaseRead)
        ));
        fixture.writer.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn an_unchanged_source_reuses_the_verified_private_copy() {
        clear_snapshot_cache();
        let (_dir, path) = closed_db_with_data();
        let cancel = within_seconds(60);
        let reads = std::cell::Cell::new(0);
        let count_read = || reads.set(reads.get() + 1);

        let first = open_snapshot_conn_inner(
            &path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        assert_eq!(reads.get(), 1);
        drop(first);

        let second = open_snapshot_conn_inner(
            &path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        assert_eq!(reads.get(), 1);
        let count: i64 = second
            .query_row("SELECT count(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);

        let expired = CollectionCancel::within(Duration::ZERO);
        assert!(matches!(
            open_snapshot_conn(&path, TEST_KEY, Some(&expired)),
            Err(SnapshotError::DatabaseRead)
        ));
    }

    #[test]
    fn a_wal_appearance_or_disappearance_invalidates_the_cached_copy() {
        clear_snapshot_cache();
        let (_dir, path) = closed_db_with_data();
        let cancel = within_seconds(60);
        let reads = std::cell::Cell::new(0);
        let count_read = || reads.set(reads.get() + 1);

        open_snapshot_conn_inner(
            &path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        let wal = wal_path(&path);
        std::fs::write(&wal, []).unwrap();
        open_snapshot_conn_inner(
            &path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        std::fs::remove_file(&wal).unwrap();
        open_snapshot_conn_inner(
            &path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        assert_eq!(reads.get(), 3);
    }

    #[test]
    fn a_key_change_does_not_reuse_a_verified_copy() {
        clear_snapshot_cache();
        let (_dir, path) = closed_db_with_data();
        let cancel = within_seconds(60);
        let reads = std::cell::Cell::new(0);
        let count_read = || reads.set(reads.get() + 1);
        open_snapshot_conn_inner(
            &path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();

        let wrong_key = "22".repeat(32);
        assert!(matches!(
            open_snapshot_conn_inner(
                &path,
                &wrong_key,
                Some(&cancel),
                SNAPSHOT_MAX_INPUT_BYTES,
                &count_read,
            ),
            Err(SnapshotError::NotADatabase)
        ));
        assert_eq!(reads.get(), 2);
    }

    #[test]
    fn committed_wal_and_checkpointed_main_changes_rebuild_the_snapshot() {
        clear_snapshot_cache();
        let fixture = Fixture::new();
        let cancel = within_seconds(60);
        let reads = std::cell::Cell::new(0);
        let count_read = || reads.set(reads.get() + 1);

        let first = open_snapshot_conn_inner(
            &fixture.path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        let ids = |connection: &Connection| {
            let mut statement = connection.prepare("SELECT id FROM messages ORDER BY id").unwrap();
            statement
                .query_map([], |row| row.get::<_, i64>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(ids(&first), vec![1, 2]);

        fixture.writer.execute_batch("ROLLBACK;").unwrap();
        fixture
            .writer
            .execute("INSERT INTO messages VALUES (4, 'wal')", [])
            .unwrap();
        let second = open_snapshot_conn_inner(
            &fixture.path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        assert_eq!(ids(&second), vec![1, 2, 4]);

        fixture.writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get::<_, i64>(0))
            .unwrap();
        let third = open_snapshot_conn_inner(
            &fixture.path,
            TEST_KEY,
            Some(&cancel),
            SNAPSHOT_MAX_INPUT_BYTES,
            &count_read,
        )
        .unwrap();
        assert_eq!(ids(&third), vec![1, 2, 4]);
        assert_eq!(reads.get(), 3);
    }

    #[test]
    fn an_open_connection_survives_cache_eviction() {
        clear_snapshot_cache();
        let (_first_dir, first_path) = closed_db_with_data();
        let cancel = within_seconds(60);
        let first = open_snapshot_conn(&first_path, TEST_KEY, Some(&cancel)).unwrap();
        for _ in 0..SNAPSHOT_CACHE_LIMIT {
            let (_dir, path) = closed_db_with_data();
            let connection = open_snapshot_conn(&path, TEST_KEY, Some(&cancel)).unwrap();
            drop(connection);
        }
        let count: i64 = first
            .query_row("SELECT count(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn clearing_the_cache_removes_paths_without_invalidating_open_connections() {
        clear_snapshot_cache();
        let (_dir, path) = closed_db_with_data();
        let connection = open_snapshot_conn(&path, TEST_KEY, None).unwrap();
        let (file, dir) = with_snapshot_cache(|cache| {
            (cache[0].copy.file.clone(), cache[0].copy.dir.clone())
        });
        assert!(file.exists());
        clear_snapshot_cache();
        clear_snapshot_cache();
        assert!(!file.exists() && !dir.exists());
        let count: i64 = connection
            .query_row("SELECT count(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    /// A WAL-mode database that was closed cleanly: all data is in the main
    /// file and there is no `-wal`. A test can then plant its own `-wal`.
    fn closed_db_with_data() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("closed.db");
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(&format!(
                    "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                     PRAGMA journal_mode = WAL;
                     CREATE TABLE messages (id INTEGER PRIMARY KEY, content TEXT);
                     INSERT INTO messages VALUES (1, 'one'), (2, 'two');"
                ))
                .unwrap();
        }
        (dir, path)
    }

    #[test]
    fn a_short_non_empty_wal_header_is_source_changed() {
        let (_dir, path) = closed_db_with_data();
        std::fs::write(wal_path(&path), [0x37u8, 0x7f, 0x06, 0x82, 0, 0, 0, 0]).unwrap();
        let cancel = within_seconds(60);
        assert!(matches!(
            open_snapshot_conn(&path, TEST_KEY, Some(&cancel)),
            Err(SnapshotError::SourceChanged)
        ));
    }

    #[test]
    fn an_unknown_wal_format_is_an_error_not_a_stale_success() {
        let (_dir, path) = closed_db_with_data();
        let mut wal = vec![0u8; 32];
        wal[0..4].copy_from_slice(&0xdead_beefu32.to_be_bytes());
        std::fs::write(wal_path(&path), &wal).unwrap();
        let cancel = within_seconds(60);
        assert!(matches!(
            open_snapshot_conn(&path, TEST_KEY, Some(&cancel)),
            Err(SnapshotError::DatabaseRead)
        ));
    }

    #[test]
    fn a_wal_with_the_wrong_version_or_page_size_is_an_error() {
        for offset in [4usize, 8] {
            let (_dir, path) = closed_db_with_data();
            let mut wal = vec![0u8; 32];
            let big = cfg!(target_endian = "big");
            let magic = if big { WAL_MAGIC_BE } else { WAL_MAGIC_LE };
            wal[0..4].copy_from_slice(&magic.to_be_bytes());
            wal[4..8].copy_from_slice(&WAL_VERSION.to_be_bytes());
            wal[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_be_bytes());
            // Corrupt either the version or the page size, then fix the header
            // checksum so only the field of interest is wrong.
            wal[offset..offset + 4].copy_from_slice(&1u32.to_be_bytes());
            let (a, b) = wal_checksum(&wal[0..24], 0, 0, big);
            wal[24..28].copy_from_slice(&a.to_be_bytes());
            wal[28..32].copy_from_slice(&b.to_be_bytes());
            std::fs::write(wal_path(&path), &wal).unwrap();
            let cancel = within_seconds(60);
            assert!(
                matches!(
                    open_snapshot_conn(&path, TEST_KEY, Some(&cancel)),
                    Err(SnapshotError::DatabaseRead)
                ),
                "field at offset {offset} must be refused"
            );
        }
    }

    #[test]
    fn a_wal_with_a_bad_header_checksum_is_an_error() {
        let (_dir, path) = closed_db_with_data();
        let wal = build_wal(&[]);
        let mut wal = wal;
        wal[24] ^= 0x01;
        std::fs::write(wal_path(&path), &wal).unwrap();
        let cancel = within_seconds(60);
        assert!(matches!(
            open_snapshot_conn(&path, TEST_KEY, Some(&cancel)),
            Err(SnapshotError::DatabaseRead)
        ));
    }

    #[test]
    fn a_non_regular_source_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source_is_a_directory");
        std::fs::create_dir(&path).unwrap();
        let cancel = within_seconds(60);
        assert!(matches!(
            open_snapshot_conn(&path, TEST_KEY, Some(&cancel)),
            Err(SnapshotError::CannotOpen)
        ));
    }

    #[test]
    fn a_fifo_source_is_refused_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source_fifo");
        let made = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if !made {
            eprintln!("[skipped] mkfifo unavailable");
            return;
        }
        let cancel = within_seconds(60);
        assert!(matches!(
            open_snapshot_conn(&path, TEST_KEY, Some(&cancel)),
            Err(SnapshotError::CannotOpen)
        ));
    }

    #[test]
    fn a_short_read_is_a_source_change_not_a_short_ok() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short");
        std::fs::write(&path, b"abc").unwrap();
        let file = File::open(&path).unwrap();
        let cancel = within_seconds(60);
        let mut out = Vec::new();
        assert!(matches!(
            read_bounded(&file, 100, &cancel, &mut out),
            Err(SnapshotError::SourceChanged)
        ));
    }

    #[test]
    fn the_private_copy_is_0700_0600_and_removed_on_drop() {
        let cancel = within_seconds(60);
        let copy = PrivateCopy::new(b"payload", &cancel).unwrap();
        let dir = copy.dir.clone();
        let file = copy.file.clone();
        let dir_mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        let file_mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);

        drop(copy);
        assert!(!file.exists());
        assert!(!dir.exists());
    }

    #[test]
    fn uri_escaping_covers_reserved_characters() {
        assert_eq!(
            escape_uri_path(Path::new("/tmp/wechat snapshot/a?b#c%d")),
            "/tmp/wechat%20snapshot/a%3Fb%23c%25d"
        );
    }

    // ── Pure WAL merge ──────────────────────────────────────────────────────

    fn build_wal(frames: &[(u32, u32, u8)]) -> Vec<u8> {
        let big = cfg!(target_endian = "big");
        let magic = if big { WAL_MAGIC_BE } else { WAL_MAGIC_LE };
        let salt = [9u8, 8, 7, 6, 5, 4, 3, 2];

        let mut header = Vec::with_capacity(24);
        header.extend_from_slice(&magic.to_be_bytes());
        header.extend_from_slice(&WAL_VERSION.to_be_bytes());
        header.extend_from_slice(&(PAGE_SIZE as u32).to_be_bytes());
        header.extend_from_slice(&0u32.to_be_bytes());
        header.extend_from_slice(&salt);
        let (a, b) = wal_checksum(&header, 0, 0, big);

        let mut wal = header;
        wal.extend_from_slice(&a.to_be_bytes());
        wal.extend_from_slice(&b.to_be_bytes());

        let mut running = (a, b);
        for &(page_number, db_size, fill) in frames {
            // The frame checksum covers page number + db size (8 bytes), then
            // the page payload; the salt is not part of it.
            let prefix = {
                let mut prefix = Vec::with_capacity(8);
                prefix.extend_from_slice(&page_number.to_be_bytes());
                prefix.extend_from_slice(&db_size.to_be_bytes());
                prefix
            };
            let page = vec![fill; PAGE_SIZE];
            let mid = wal_checksum(&prefix, running.0, running.1, big);
            let next = wal_checksum(&page, mid.0, mid.1, big);

            let mut frame_header = Vec::with_capacity(24);
            frame_header.extend_from_slice(&page_number.to_be_bytes());
            frame_header.extend_from_slice(&db_size.to_be_bytes());
            frame_header.extend_from_slice(&salt);
            frame_header.extend_from_slice(&next.0.to_be_bytes());
            frame_header.extend_from_slice(&next.1.to_be_bytes());

            running = next;
            wal.extend_from_slice(&frame_header);
            wal.extend_from_slice(&page);
        }
        wal
    }

    fn page_fill(image: &[u8], one_based: usize) -> u8 {
        image[(one_based - 1) * PAGE_SIZE]
    }

    #[test]
    fn merge_without_wal_returns_the_file_verbatim() {
        let main = vec![0x11u8; PAGE_SIZE * 4];
        let cancel = within_seconds(60);
        // No WAL: the logical size is the whole file, so nothing is truncated.
        assert_eq!(
            merge_wal(&main, &[], SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap(),
            main
        );
    }

    #[test]
    fn merge_without_wal_returns_the_file_verbatim_is_red_on_stale_main_tail() {
        // Guard that the equality test above can actually fail: writing past the
        // logical end must change the image.
        let mut main = vec![0u8; PAGE_SIZE * 4];
        main[(4 - 1) * PAGE_SIZE] = 0x9A;
        let wal = build_wal(&[(1, 3, 0xA1), (2, 3, 0xA2)]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_ne!(merged, main, "the stale physical tail must not survive");
        assert_eq!(page_fill(&merged, 4), 0x00);
    }

    #[test]
    fn merge_keeps_a_preallocated_tail() {
        // Committed size of one page while the file physically holds four: the
        // physical length is preserved and only bytes past the logical end are
        // cleared, so the length test still distinguishes preallocation.
        let mut main = vec![0u8; PAGE_SIZE * 4];
        main[0] = 0x01;
        let wal = build_wal(&[(1, 1, 0xAA)]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(merged.len(), PAGE_SIZE * 4, "physical length must not shrink");
        assert_eq!(page_fill(&merged, 1), 0xAA);
    }

    #[test]
    fn merge_extends_and_reads_committed_wal_pages() {
        let main = vec![0u8; PAGE_SIZE];
        let wal = build_wal(&[(1, 0, 0xAA), (2, 2, 0xBB)]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(merged.len(), PAGE_SIZE * 2);
        assert_eq!(page_fill(&merged, 1), 0xAA);
        assert_eq!(page_fill(&merged, 2), 0xBB);
    }

    #[test]
    fn merge_drops_an_uncommitted_tail() {
        let main = vec![0u8; PAGE_SIZE];
        // Page 1 rewritten twice, committed at the second frame with size 1, then
        // a third frame that is never committed.
        let wal = build_wal(&[(1, 0, 0xAA), (1, 1, 0xBB), (1, 0, 0xCC)]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(merged.len(), PAGE_SIZE);
        assert_eq!(page_fill(&merged, 1), 0xBB);
    }

    #[test]
    fn merge_stops_at_a_torn_frame() {
        let main = vec![0u8; PAGE_SIZE];
        let mut wal = build_wal(&[(1, 1, 0xAA), (1, 0, 0xCC)]);
        // Corrupt the last byte of the second frame's checksum.
        let end = wal.len();
        wal[end - 1] ^= 0x01;
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(page_fill(&merged, 1), 0xAA);
    }

    #[test]
    fn merge_accepts_a_grow_then_shrink_sequence() {
        // Transaction 1 grows the database to three pages; transaction 2 shrinks
        // it back to two. The shrink must drop page 3 instead of being rejected.
        let main = vec![0u8; PAGE_SIZE];
        let wal = build_wal(&[(1, 0, 0xA1), (2, 0, 0xA2), (3, 3, 0xA3), (2, 2, 0xA4)]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(merged.len(), PAGE_SIZE * 2);
        assert_eq!(page_fill(&merged, 1), 0xA1);
        assert_eq!(page_fill(&merged, 2), 0xA4);
    }

    #[test]
    fn a_truncated_page_does_not_revive_on_a_later_grow() {
        // Grow to three, shrink to two, then grow back to three by rewriting
        // page 3: the old page 3 must not be reused.
        let main = vec![0u8; PAGE_SIZE];
        let wal = build_wal(&[
            (1, 0, 0xA1),
            (2, 0, 0xA2),
            (3, 3, 0xA3),
            (2, 2, 0xA4),
            (3, 3, 0xA6),
        ]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(merged.len(), PAGE_SIZE * 3);
        assert_eq!(page_fill(&merged, 2), 0xA4);
        assert_eq!(page_fill(&merged, 3), 0xA6);
    }

    #[test]
    fn a_page_truncated_by_an_early_commit_but_regrown_before_a_later_one_stays_dead() {
        // main is four pages; commits shrink to two, then grow to three (page 3
        // NOT rewritten), then grow to four writing page 4. The second commit's
        // limit (3) is still below the physical main size, but page 3 was
        // already truncated by the first commit and must not revive from main.
        let mut main = vec![0u8; PAGE_SIZE * 4];
        main[(3 - 1) * PAGE_SIZE] = 0x3C;
        main[(4 - 1) * PAGE_SIZE] = 0x4C;
        let wal = build_wal(&[(1, 2, 0xA1), (2, 3, 0xA2), (4, 4, 0xA4)]);
        let cancel = within_seconds(60);
        assert!(matches!(
            merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel),
            Err(SnapshotError::DatabaseRead)
        ));
    }

    #[test]
    fn a_main_page_truncated_away_does_not_revive_on_regrow() {
        // The four-page main file holds non-zero payload on pages 3 and 4. A
        // commit shrinks the logical image to two pages, then another grows it
        // back to four without writing pages 3 and 4: those pages are gone and
        // their old main bytes must not reappear.
        let mut main = vec![0u8; PAGE_SIZE * 4];
        main[(3 - 1) * PAGE_SIZE] = 0x3C;
        main[(4 - 1) * PAGE_SIZE] = 0x4C;
        let wal = build_wal(&[(1, 2, 0xA1), (2, 4, 0xA2)]);
        let cancel = within_seconds(60);
        assert!(matches!(
            merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel),
            Err(SnapshotError::DatabaseRead)
        ));
    }

    #[test]
    fn a_regrown_main_page_with_a_fresh_frame_is_accepted() {
        // Same shrink-then-regrow, but pages 3 and 4 are written again: the
        // fresh payload must win over the stale main bytes.
        let mut main = vec![0u8; PAGE_SIZE * 4];
        main[(3 - 1) * PAGE_SIZE] = 0x3C;
        main[(4 - 1) * PAGE_SIZE] = 0x4C;
        let wal = build_wal(&[
            (1, 2, 0xA1),
            (2, 4, 0xA2),
            (3, 4, 0xA3),
            (4, 4, 0xA4),
        ]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(merged.len(), PAGE_SIZE * 4);
        assert_eq!(page_fill(&merged, 3), 0xA3);
        assert_eq!(page_fill(&merged, 4), 0xA4);
    }

    #[test]
    fn a_main_page_beyond_the_logical_range_is_cleared() {
        // Main has three physical pages; a commit shrinks the logical size to
        // two. Page 3 is outside the logical image: it is not required, and its
        // physical tail is zeroed so no stale bytes sit past the logical end.
        let mut main = vec![0u8; PAGE_SIZE * 3];
        main[(3 - 1) * PAGE_SIZE] = 0x3C;
        let wal = build_wal(&[(1, 2, 0xA1), (2, 2, 0xA2)]);
        let cancel = within_seconds(60);
        let merged = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel).unwrap();
        assert_eq!(merged.len(), PAGE_SIZE * 3);
        assert_eq!(page_fill(&merged, 3), 0x00);
    }

    #[test]
    fn merge_reports_a_missing_extended_page() {
        // Commit says three pages; only pages 1 and 2 are provided.
        let main = vec![0u8; PAGE_SIZE];
        let wal = build_wal(&[(1, 0, 0xAA), (2, 3, 0xBB)]);
        let cancel = within_seconds(60);
        assert!(matches!(
            merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel),
            Err(SnapshotError::DatabaseRead)
        ));
    }

    #[test]
    fn the_merge_budget_counts_the_frame_index_overhead() {
        let main = vec![0u8; PAGE_SIZE];
        let wal = build_wal(&[(1, 1, 0xAA)]);
        // Exactly the page payloads, with no room for the merge index.
        let payload_only = main.len() + wal.len() + PAGE_SIZE;
        let cancel = within_seconds(60);
        assert!(matches!(
            merge_wal(&main, &wal, payload_only, &cancel),
            Err(SnapshotError::BudgetExceeded)
        ));
    }

    #[test]
    fn the_frame_index_upper_bound_rejects_a_big_wal_before_parsing() {
        // A WAL of N complete frames would allocate N index entries. The bound
        // is derived from the WAL length alone, so a small budget refuses it
        // before any parse or allocation happens.
        let wal_frame_count = 64u64;
        let wal_len = wal_frame_count * (24 + PAGE_SIZE as u64);
        let main_len = PAGE_SIZE as u64;
        let cancel = within_seconds(60);
        // Budget above the payloads but below payloads + the worst-case index.
        let budget = (main_len + wal_len) as usize + 1;
        assert!(matches!(
            check_index_budget(wal_len, main_len, budget),
            Err(SnapshotError::BudgetExceeded)
        ));
        // A budget that accounts for the index passes the precheck.
        let generous = (main_len
            + wal_len
            + wal_frame_count * INDEX_ENTRY_BYTES_PER_FRAME_UPPER
            + main_len
            + wal_len) as usize;
        assert!(check_index_budget(wal_len, main_len, generous).is_ok());
    }

    #[test]
    fn a_merge_cancel_during_the_image_copy_is_refused() {
        let main = vec![0x55u8; PAGE_SIZE * 4];
        let wal = build_wal(&[(1, 4, 0xA1)]);
        let cancel = within_seconds(60);
        set_trip(Stage::Copy, &cancel);
        let result = merge_wal(&main, &wal, SNAPSHOT_MAX_INPUT_BYTES, &cancel);
        clear_trip();
        assert!(matches!(result, Err(SnapshotError::DatabaseRead)));
    }

    #[test]
    fn valid_keys_are_hex_only() {
        assert!(valid_key(TEST_KEY));
        assert!(!valid_key(""));
        assert!(!valid_key("xyz"));
        assert!(!valid_key("abc"));
    }

    #[test]
    fn a_real_wal_grow_then_drop_reads_the_writer_ground_truth() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shrink.db");
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(&format!(
                "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                 PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
                 PRAGMA auto_vacuum = FULL;
                 CREATE TABLE keep (id INTEGER PRIMARY KEY, v TEXT);
                 CREATE TABLE dropme (id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO keep VALUES (1, 'kept');
                 WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 2000)
                 INSERT INTO dropme SELECT x, printf('%0500d', x) FROM n;"
            ))
            .unwrap();
        let grown: i64 = writer.query_row("PRAGMA page_count", [], |row| row.get(0)).unwrap();
        // The big insert lives in the WAL; dropping it and compacting must
        // shrink the image, so the WAL commits a smaller page count.
        writer.execute("DROP TABLE dropme", []).unwrap();
        writer.execute_batch("VACUUM;").unwrap();
        let shrunken: i64 = writer.query_row("PRAGMA page_count", [], |row| row.get(0)).unwrap();
        assert!(
            shrunken < grown,
            "fixture guard: the WAL must grow then shrink the image ({grown} -> {shrunken})"
        );

        let cancel = within_seconds(60);
        let connection = open_snapshot_conn(&path, TEST_KEY, Some(&cancel)).unwrap();
        let kept: i64 = connection
            .query_row("SELECT count(*) FROM keep", [], |row| row.get(0))
            .unwrap();
        let dropped = connection.query_row("SELECT count(*) FROM dropme", [], |row| {
            row.get::<_, i64>(0)
        });
        assert_eq!(kept, 1, "the surviving table is read");
        assert!(dropped.is_err(), "the dropped table is gone, as the writer sees it");

        let _ = writer.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            row.get::<_, i64>(0)
        });
    }
}
