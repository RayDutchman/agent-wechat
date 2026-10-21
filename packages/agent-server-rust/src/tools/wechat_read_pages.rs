//! Frozen, in-memory pagination for reliable read APIs.
//!
//! A caller first collects a fixed sample of rows (for one account, one
//! resource, under one fence/epoch), hands the whole sample to
//! [`FrozenPageStore::insert`] exactly once, and then serves pagination purely
//! from that frozen copy. Paging afterwards never re-reads the source, so rows
//! added or modified between pages cannot shift offsets under the caller.
//!
//! Scope of the guarantee, stated plainly:
//!
//! * This primitive collects nothing. It does not query the database, does not
//!   verify that the account is real or logged in, and is not a point-in-time
//!   snapshot across stores. It only freezes rows the caller already read.
//! * The caller must pass the source scope and fence on every request and the
//!   router must re-validate them; [`FrozenPageStore::next`] refuses a scope
//!   that differs from the one the snapshot was frozen under.
//! * Byte budgets count serialized JSON, not exact heap usage. Per-row metadata
//!   stays bounded by the row and page limits.
//! * `insert` receives an already materialized `Vec`, so it cannot bound the
//!   memory the caller used while collecting. The collection stage needs its own
//!   budget.

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use serde_json::Value;
use uuid::Uuid;

use crate::tools::wechat_messages::{CollectionCancel, COLLECTION_BUDGET};

/// Absolute lifetime of a frozen sample. Fixed; paging never renews it.
const DEFAULT_TTL: Duration = Duration::from_secs(300);
/// Page size used when the caller does not care; any `1..=1000` is accepted.
const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 1000;
const DEFAULT_MAX_SNAPSHOTS: usize = 4;
const DEFAULT_MAX_ROWS_PER_SNAPSHOT: usize = 50_000;
const DEFAULT_MAX_BYTES_PER_SNAPSHOT: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_TOTAL_BYTES: usize = 32 * 1024 * 1024;
// Leave at least 1 MiB for the envelope within the client's 4 MiB limit.
const MAX_PAGE_BYTES: usize = 3 * 1024 * 1024;

/// The source identity a frozen sample belongs to.
///
/// The caller supplies all three parts on every request: the account directory
/// it believes it is reading, the resource name, and the fence/epoch that was
/// valid when the rows were collected.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SnapshotScope {
    pub(crate) account_dir: String,
    pub(crate) resource: String,
    pub(crate) epoch: String,
}

/// Stable failure reasons for a freeze or a page read.
///
/// `Display` yields only these codes; it never echoes a cursor, scope, row, or
/// any other request content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageError {
    /// Bad arguments: page size out of range, non-object row, malformed cursor.
    InvalidRequest,
    /// The snapshot or the cursor is gone: TTL elapsed, an expired sample was
    /// purged, or the server restarted and lost the in-memory registry.
    Expired,
    /// The request's scope does not match the scope the snapshot was frozen
    /// under. No page is returned.
    ScopeChanged,
    /// A documented budget would be exceeded. Nothing is stored or evicted.
    BudgetExceeded,
    /// The caller's round ran out while the sample was being prepared. Nothing
    /// is stored, and this is not a budget refusal: the sample may well have
    /// fitted, the caller simply stopped waiting.
    DeadlineExceeded,
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            PageError::InvalidRequest => "invalid_request",
            PageError::Expired => "expired",
            PageError::ScopeChanged => "scope_changed",
            PageError::BudgetExceeded => "budget_exceeded",
            PageError::DeadlineExceeded => "deadline_exceeded",
        };
        f.write_str(code)
    }
}

impl std::error::Error for PageError {}

/// One page of a frozen sample.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FrozenPage {
    /// Rows exactly as frozen. The scope, epoch and snapshot id are not mixed
    /// into the rows.
    pub(crate) items: Vec<Value>,
    /// Cursor for the following page; `None` on the final page.
    pub(crate) next_cursor: Option<String>,
    /// Row count of the frozen sample, not a claim about WeChat history.
    pub(crate) total: usize,
    /// Random id of the frozen sample, stable across all of its pages.
    pub(crate) snapshot_id: String,
    pub(crate) release_token: Option<String>,
}

/// Budgets and page default applied by [`FrozenPageStore::new`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct PageStoreConfig {
    pub(crate) ttl: Duration,
    pub(crate) page_size: usize,
    pub(crate) max_snapshots: usize,
    pub(crate) max_rows_per_snapshot: usize,
    pub(crate) max_bytes_per_snapshot: usize,
    pub(crate) max_total_bytes: usize,
}

impl Default for PageStoreConfig {
    fn default() -> Self {
        PageStoreConfig {
            ttl: DEFAULT_TTL,
            page_size: DEFAULT_PAGE_SIZE,
            max_snapshots: DEFAULT_MAX_SNAPSHOTS,
            max_rows_per_snapshot: DEFAULT_MAX_ROWS_PER_SNAPSHOT,
            max_bytes_per_snapshot: DEFAULT_MAX_BYTES_PER_SNAPSHOT,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
        }
    }
}

/// A frozen sample plus its cursor registry.
struct FrozenSample {
    scope: SnapshotScope,
    rows: Vec<Value>,
    /// Serialized byte size of the rows, for budget accounting only.
    row_bytes: usize,
    ranges: Vec<std::ops::Range<usize>>,
    expires_at: Instant,
    /// `cursors[page_index]` is the random id that always resolves to that
    /// page of this sample. The mapping is fixed at insert time and is never
    /// consumed, so retrying a cursor yields an identical page.
    cursors: Vec<String>,
    release_token: String,
}

/// What a cursor stands for. Holding the frozen scope lets `next` detect a
/// caller that moved to another account, resource, or fence.
#[derive(Clone)]
struct CursorEntry {
    scope: SnapshotScope,
    snapshot_id: String,
    page_index: usize,
}

#[derive(Clone)]
struct ReleaseEntry {
    scope: SnapshotScope,
    snapshot_id: String,
}

/// In-memory registry of frozen samples, capped by count and serialized bytes.
///
/// Not persistent: a server restart drops every sample and cursor, and callers
/// then see [`PageError::Expired`] and must collect again.
pub(crate) struct FrozenPageStore {
    config: PageStoreConfig,
    samples: HashMap<String, FrozenSample>,
    cursors: HashMap<String, CursorEntry>,
    release_tokens: HashMap<String, ReleaseEntry>,
    total_bytes: usize,
}

impl FrozenPageStore {
    /// Store with the documented defaults: 300 s absolute TTL, page size 100,
    /// at most 4 live snapshots, 50 000 rows and 16 MiB per snapshot, 32 MiB in
    /// total.
    pub(crate) fn new() -> Self {
        FrozenPageStore::with_config(PageStoreConfig::default())
    }

    fn with_config(config: PageStoreConfig) -> Self {
        FrozenPageStore {
            config,
            samples: HashMap::new(),
            cursors: HashMap::new(),
            release_tokens: HashMap::new(),
            total_bytes: 0,
        }
    }

    /// Freeze one complete sample and return its first page.
    ///
    /// The two stages, composed: [`prepare`] does the work that has nothing to
    /// do with the store's state, and [`FrozenPageStore::publish`] registers what
    /// it produced. A caller answering a request under a deadline runs the two
    /// separately, so the first is not holding this store's lock.
    ///
    /// # Errors
    ///
    /// * [`PageError::InvalidRequest`] if `page_size` is not in `1..=1000` or any
    ///   row is not a JSON object. Nothing is stored.
    /// * [`PageError::BudgetExceeded`] if the sample exceeds the row, per snapshot
    ///   byte, snapshot count, or total byte budget. Nothing is stored and no
    ///   unexpired snapshot is evicted.
    /// * [`PageError::DeadlineExceeded`] if the caller's round ran out while the
    ///   sample was being measured. Nothing is stored.
    #[allow(dead_code)] // Composed by the read routes that own collection.
    pub(crate) fn insert(
        &mut self,
        scope: SnapshotScope,
        items: Vec<Value>,
        page_size: usize,
        now: Instant,
    ) -> Result<FrozenPage, PageError> {
        let prepared = prepare(
            &self.config,
            scope,
            items,
            page_size,
            &CollectionCancel::within(COLLECTION_BUDGET),
        )?;
        self.publish(prepared, now)
    }

    /// Register a prepared sample and return its first page.
    ///
    /// The only part of a freeze that touches the store, and the only part that
    /// needs its lock: purging, the two budgets, the cursors and the first page.
    /// A round that runs out before it gets this far has registered nothing, so
    /// it cannot leave a sample behind for a continuation that will never come.
    pub(crate) fn publish(
        &mut self,
        prepared: PreparedSnapshot,
        now: Instant,
    ) -> Result<FrozenPage, PageError> {
        let PreparedSnapshot {
            scope,
            items,
            ranges,
            row_bytes,
            single_page,
        } = prepared;

        self.purge_expired(now);

        let snapshot_id = Uuid::new_v4().to_string();
        if single_page {
            return Ok(FrozenPage {
                total: items.len(), items, next_cursor: None, snapshot_id,
                release_token: None,
            });
        }

        if self.samples.len() >= self.config.max_snapshots
            || self.total_bytes.saturating_add(row_bytes) > self.config.max_total_bytes
        {
            return Err(PageError::BudgetExceeded);
        }

        let page_count = ranges.len();
        let release_token = Uuid::new_v4().to_string();
        let cursors: Vec<String> = (0..page_count).map(|_| Uuid::new_v4().to_string()).collect();
        for (page_index, cursor) in cursors.iter().enumerate() {
            self.cursors.insert(
                cursor.clone(),
                CursorEntry {
                    scope: scope.clone(),
                    snapshot_id: snapshot_id.clone(),
                    page_index,
                },
            );
        }

        // An unrepresentable TTL yields an already expired sample rather than a
        // panic; the production TTL is minutes.
        let expires_at = now.checked_add(self.config.ttl).unwrap_or(now);
        let first_page = build_page(
            &items,
            &ranges,
            0,
            &cursors,
            &snapshot_id,
            Some(&release_token),
        );

        self.total_bytes = self.total_bytes.saturating_add(row_bytes);
        self.samples.insert(
            snapshot_id.clone(),
            FrozenSample {
                scope,
                rows: items,
                row_bytes,
                ranges,
                expires_at,
                cursors,
                release_token: release_token.clone(),
            },
        );
        self.release_tokens.insert(
            release_token,
            ReleaseEntry { scope: self.samples.get(&snapshot_id).expect("sample inserted").scope.clone(), snapshot_id },
        );

        Ok(first_page)
    }
    /// Read the page addressed by `cursor`.
    ///
    /// The caller re-supplies `scope` on every request; a mismatch fails and
    /// returns no page. The cursor is a random capability, so it cannot be
    /// steered by editing an offset, and it is not consumed by reading.
    ///
    /// # Errors
    ///
    /// * [`PageError::InvalidRequest`] if `cursor` is not a UUID. Random but
    ///   unissued UUIDs report [`PageError::Expired`] instead, because a server
    ///   restart empties this registry the same way and the two cases cannot be
    ///   told apart from memory alone.
    /// * [`PageError::ScopeChanged`] if `scope` differs from the frozen scope.
    /// * [`PageError::Expired`] if the sample or cursor is gone.
    #[allow(dead_code)] // Consumed by the read routes that own collection.
    pub(crate) fn next(
        &mut self,
        cursor: &str,
        scope: &SnapshotScope,
        now: Instant,
    ) -> Result<FrozenPage, PageError> {
        if Uuid::parse_str(cursor.trim()).is_err() {
            return Err(PageError::InvalidRequest);
        }

        self.purge_expired(now);

        let entry = match self.cursors.get(cursor) {
            Some(entry) => entry.clone(),
            None => return Err(PageError::Expired),
        };
        if &entry.scope != scope {
            return Err(PageError::ScopeChanged);
        }
        let sample = match self.samples.get(&entry.snapshot_id) {
            Some(sample) => sample,
            None => return Err(PageError::Expired),
        };
        if now >= sample.expires_at {
            return Err(PageError::Expired);
        }

        Ok(build_page(
            &sample.rows,
            &sample.ranges,
            entry.page_index,
            &sample.cursors,
            &entry.snapshot_id,
            Some(&sample.release_token),
        ))
    }

    /// Acknowledge a consumed sample. This only removes the in-memory copy;
    /// it never accesses or deletes a source message.
    pub(crate) fn release(&mut self, id: &str, scope: &SnapshotScope, now: Instant) -> Result<bool, PageError> {
        if Uuid::parse_str(id).is_err() {
            return Err(PageError::InvalidRequest);
        }
        self.purge_expired(now);
        let Some(sample) = self.samples.get(id) else { return Ok(false); };
        if &sample.scope != scope {
            return Err(PageError::ScopeChanged);
        }
        let sample = self.samples.remove(id).expect("sample was checked under exclusive borrow");
        self.total_bytes = self.total_bytes.saturating_sub(sample.row_bytes);
        for cursor in sample.cursors { self.cursors.remove(&cursor); }
        self.release_tokens.retain(|_, entry| entry.snapshot_id != id);
        Ok(true)
    }

    pub(crate) fn release_by_token(
        &mut self,
        token: &str,
        scope: &SnapshotScope,
        now: Instant,
    ) -> Result<bool, PageError> {
        if Uuid::parse_str(token).is_err() {
            return Err(PageError::InvalidRequest);
        }
        self.purge_expired(now);
        let Some(entry) = self.release_tokens.get(token).cloned() else {
            return Err(PageError::Expired);
        };
        if &entry.scope != scope {
            return Err(PageError::ScopeChanged);
        }
        self.release(&entry.snapshot_id, scope, now)
    }

    pub(crate) fn release_by_token_account(
        &mut self,
        token: &str,
        account_dir: &str,
        epoch: &str,
        now: Instant,
    ) -> Result<bool, PageError> {
        if Uuid::parse_str(token).is_err() {
            return Err(PageError::InvalidRequest);
        }
        self.purge_expired(now);
        let Some(entry) = self.release_tokens.get(token).cloned() else {
            return Err(PageError::Expired);
        };
        if entry.scope.account_dir != account_dir || entry.scope.epoch != epoch {
            return Err(PageError::ScopeChanged);
        }
        self.release(&entry.snapshot_id, &entry.scope, now)
    }

    pub(crate) fn scope_for_cursor(&mut self, cursor: &str, now: Instant) -> Result<SnapshotScope, PageError> {
        if Uuid::parse_str(cursor.trim()).is_err() {
            return Err(PageError::InvalidRequest);
        }
        self.purge_expired(now);
        let entry = self.cursors.get(cursor).ok_or(PageError::Expired)?;
        if !self.samples.contains_key(&entry.snapshot_id) {
            return Err(PageError::Expired);
        }
        Ok(entry.scope.clone())
    }

    /// Drop samples whose absolute TTL has elapsed, releasing their bytes and
    /// their cursors. Never touches a sample that is still inside its window.
    fn purge_expired(&mut self, now: Instant) {
        let expired: Vec<String> = self
            .samples
            .iter()
            .filter(|(_, sample)| now >= sample.expires_at)
            .map(|(id, _)| id.clone())
            .collect();

        for snapshot_id in expired {
            if let Some(sample) = self.samples.remove(&snapshot_id) {
                self.total_bytes = self.total_bytes.saturating_sub(sample.row_bytes);
                for cursor in sample.cursors {
                    self.cursors.remove(&cursor);
                }
                self.release_tokens.retain(|_, entry| entry.snapshot_id != snapshot_id);
            }
        }
    }

    #[cfg(test)]
    fn live_snapshot_count(&self, _now: Instant) -> usize {
        self.samples.len()
    }

    #[cfg(test)]
    fn total_tracked_bytes(&self) -> usize {
        self.total_bytes
    }
}

/// A sample that has been measured but not registered.
///
/// Carries the results of the work that does not need the store: the rows as
/// they will be served, where each page starts and ends, and what the sample
/// costs. Nothing here is visible to a continuation until [`FrozenPageStore`]
/// registers it, so a preparation that is abandoned — because the round ended,
/// or because the client went away — costs memory that is freed at once and
/// never a snapshot slot.
#[derive(Debug)]
pub(crate) struct PreparedSnapshot {
    scope: SnapshotScope,
    items: Vec<Value>,
    ranges: Vec<std::ops::Range<usize>>,
    row_bytes: usize,
    /// A sample that fits in one page is never registered: it has no cursor, so
    /// there is nothing a continuation could lose.
    single_page: bool,
}

/// Validate a sample and measure it, without touching the store.
///
/// This is the heavy part of a freeze — one serialization per row, and a page
/// boundary decision for each — and it needs nothing from the store but its
/// configuration. It runs off the store's lock, on a blocking thread, and checks
/// the caller's round as it goes: a sample of up to 50 000 rows is exactly the
/// kind of work that can outlast the answer somebody is still waiting for.
///
/// # Errors
///
/// The same refusals [`FrozenPageStore::insert`] documents, plus
/// [`PageError::DeadlineExceeded`] when the round ends first. Nothing is stored
/// in any of them, and no budget is charged: the store is not involved yet.
pub(crate) fn prepare(
    config: &PageStoreConfig,
    scope: SnapshotScope,
    items: Vec<Value>,
    page_size: usize,
    cancel: &CollectionCancel,
) -> Result<PreparedSnapshot, PageError> {
    if page_size == 0 || page_size > MAX_PAGE_SIZE {
        return Err(PageError::InvalidRequest);
    }
    if items.len() > config.max_rows_per_snapshot {
        return Err(PageError::BudgetExceeded);
    }

    if [&scope.account_dir, &scope.resource, &scope.epoch].iter()
        .any(|value| value.is_empty() || value.len() > 4096) {
        return Err(PageError::InvalidRequest);
    }
    let mut row_bytes = 0usize;
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut page_bytes = 2usize;
    for (index, item) in items.iter().enumerate() {
        // Once per page's worth of rows rather than per row: the clock is read
        // on the same schedule the pages are cut on, so a preparation of 50 000
        // rows is checked about 500 times instead of 50 000.
        if index % 64 == 0 && cancel.expired() {
            return Err(PageError::DeadlineExceeded);
        }
        if !item.is_object() {
            return Err(PageError::InvalidRequest);
        }
        let encoded =
            serde_json::to_vec(item).map_err(|_| PageError::InvalidRequest)?;
        if encoded.len() + 2 > MAX_PAGE_BYTES {
            return Err(PageError::BudgetExceeded);
        }
        let extra = encoded.len() + usize::from(index > start);
        if index > start && (index - start == page_size || page_bytes + extra > MAX_PAGE_BYTES) {
            ranges.push(start..index);
            start = index;
            page_bytes = 2;
        }
        page_bytes += encoded.len() + usize::from(index > start);
        row_bytes = row_bytes.saturating_add(encoded.len());
    }
    if row_bytes > config.max_bytes_per_snapshot {
        return Err(PageError::BudgetExceeded);
    }

    ranges.push(start..items.len());
    let single_page = ranges.len() == 1;
    Ok(PreparedSnapshot {
        scope,
        items,
        ranges,
        row_bytes,
        single_page,
    })
}

/// Slice one page out of a frozen sample. `cursors[page_index + 1]` is the
/// continuation, so the final page has `next_cursor: None`.
fn build_page(
    rows: &[Value],
    ranges: &[std::ops::Range<usize>],
    page_index: usize,
    cursors: &[String],
    snapshot_id: &str,
    release_token: Option<&str>,
) -> FrozenPage {
    FrozenPage {
        items: rows[ranges[page_index].clone()].to_vec(),
        next_cursor: cursors.get(page_index + 1).cloned(),
        total: rows.len(),
        snapshot_id: snapshot_id.to_string(),
        release_token: release_token.map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    fn row(id: u64) -> Value {
        json!({ "local_id": id, "nickname": format!("row-{id}") })
    }

    fn rows(count: u64) -> Vec<Value> {
        (0..count).map(row).collect()
    }

    fn ids(page: &FrozenPage) -> Vec<u64> {
        page.items
            .iter()
            .map(|item| {
                item.get("local_id")
                    .and_then(Value::as_u64)
                    .expect("row carries local_id")
            })
            .collect()
    }

    fn scope(resource: &str) -> SnapshotScope {
        SnapshotScope {
            account_dir: "/data/Accounts/wxid_test".to_string(),
            resource: resource.to_string(),
            epoch: "e-1".to_string(),
        }
    }

    fn small_config() -> PageStoreConfig {
        PageStoreConfig::default()
    }

    /// A round with the production budget, for the cases that are about the
    /// freeze itself rather than about stopping it.
    fn generous() -> CollectionCancel {
        CollectionCancel::within(COLLECTION_BUDGET)
    }

    /// Flip the first hex digit so the result is still a well formed UUID that
    /// this store never issued.
    fn tamper(cursor: &str) -> String {
        let first = cursor.as_bytes()[0] as char;
        let replacement = if first == '0' { '1' } else { '0' };
        format!("{replacement}{}", &cursor[1..])
    }

    #[test]
    fn pages_fit_the_client_response_budget_without_losing_large_rows() {
        let mut store = FrozenPageStore::new();
        let now = t0();
        let sc = scope("large");
        let input: Vec<_> = (0..3).map(|id| json!({"id": id, "body": "x".repeat(2 * 1024 * 1024)})).collect();
        let mut page = store.insert(sc.clone(), input.clone(), 100, now).unwrap();
        let mut output = Vec::new();
        loop {
            assert!(serde_json::to_vec(&page.items).unwrap().len() <= 3 * 1024 * 1024);
            output.extend(page.items.clone());
            match page.next_cursor {
                Some(cursor) => page = store.next(&cursor, &sc, now).unwrap(),
                None => break,
            }
        }
        assert_eq!(output, input);
    }

    #[test]
    fn explicit_release_is_scoped_idempotent_and_reclaims_budget() {
        let mut store = FrozenPageStore::new();
        let now = t0();
        for _ in 0..10 {
            let sc = scope("messages:chat");
            let page = store.insert(sc.clone(), rows(4), 2, now).unwrap();
            let cursor = page.next_cursor.unwrap();
            assert_eq!(store.release(&page.snapshot_id, &scope("other"), now), Err(PageError::ScopeChanged));
            assert!(store.next(&cursor, &sc, now).is_ok());
            assert_eq!(store.release(&page.snapshot_id, &sc, now), Ok(true));
            assert_eq!(store.release(&page.snapshot_id, &sc, now), Ok(false));
            assert_eq!(store.next(&cursor, &sc, now), Err(PageError::Expired));
            assert_eq!(store.total_tracked_bytes(), 0);
        }
    }

    #[test]
    fn single_page_polls_do_not_exhaust_cursor_storage() {
        let mut store = FrozenPageStore::new();
        for _ in 0..20 {
            let page = store.insert(scope("small"), rows(1), 100, t0()).unwrap();
            assert!(page.next_cursor.is_none());
        }
        assert_eq!(store.total_tracked_bytes(), 0);
    }

    #[test]
    fn default_config_matches_documented_budgets() {
        let config = FrozenPageStore::new().config;
        assert_eq!(config.ttl, Duration::from_secs(300));
        assert_eq!(config.page_size, 100);
        assert_eq!(config.max_snapshots, 4);
        assert_eq!(config.max_rows_per_snapshot, 50_000);
        assert_eq!(config.max_bytes_per_snapshot, 16 * 1024 * 1024);
        assert_eq!(config.max_total_bytes, 32 * 1024 * 1024);
    }

    #[test]
    fn frozen_page_is_stable_when_caller_collects_more_rows_later() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");

        let first = store
            .insert(sc.clone(), rows(3), 2, base)
            .expect("first sample freezes");
        let cursor = first.next_cursor.clone().expect("more pages follow");

        // The caller keeps collecting from the live source afterwards.
        let _second = store
            .insert(sc.clone(), rows(9), 2, base)
            .expect("second sample freezes");
        let mut later_rows = rows(5);
        later_rows[0] = json!({ "local_id": 999, "nickname": "mutated" });

        let second = store
            .next(&cursor, &sc, base)
            .expect("first snapshot still readable");

        assert_eq!(ids(&first), vec![0, 1]);
        assert_eq!(ids(&second), vec![2]);
        assert_eq!(first.snapshot_id, second.snapshot_id);
        assert_eq!(first.total, 3);
        assert_eq!(second.total, 3);
        assert!(
            second.items.iter().all(|item| item.get("local_id") != Some(&json!(999))),
            "rows collected after the freeze never enter the old snapshot"
        );
    }

    #[test]
    fn three_pages_cover_every_row_without_gap_or_repeat() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("messages");

        let mut collected: Vec<u64> = Vec::new();
        let mut page = store.insert(sc.clone(), rows(7), 3, base).expect("insert");
        assert_eq!(ids(&page), vec![0, 1, 2]);
        assert_eq!(page.total, 7);
        collected.extend(ids(&page));

        let cursor = page.next_cursor.clone().expect("page two exists");
        page = store.next(&cursor, &sc, base).expect("page two");
        assert_eq!(ids(&page), vec![3, 4, 5]);
        assert_eq!(page.total, 7);
        collected.extend(ids(&page));

        let cursor = page.next_cursor.clone().expect("page three exists");
        page = store.next(&cursor, &sc, base).expect("page three");
        assert_eq!(ids(&page), vec![6]);
        assert_eq!(page.total, 7);
        collected.extend(ids(&page));

        assert!(page.next_cursor.is_none(), "last page terminates");
        assert_eq!(collected, (0..7).collect::<Vec<u64>>());
    }

    #[test]
    fn same_cursor_retry_returns_identical_page() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("chats");

        let first = store.insert(sc.clone(), rows(5), 2, base).expect("insert");
        let cursor = first.next_cursor.clone().expect("more pages");

        let a = store.next(&cursor, &sc, base).expect("first read");
        let b = store.next(&cursor, &sc, base).expect("retry");
        assert_eq!(a, b, "identical items, next_cursor and snapshot_id");

        let third_cursor = a.next_cursor.clone().expect("third page exists");
        let c = store.next(&third_cursor, &sc, base).expect("third page");
        let d = store.next(&third_cursor, &sc, base).expect("third page retry");
        assert_eq!(c, d);
        assert_eq!(c.total, 5);
        assert!(c.next_cursor.is_none());
        assert!(!c.snapshot_id.is_empty());

        // The consumed-looking cursor is still live: reading never advances it.
        assert_eq!(store.next(&cursor, &sc, base).expect("still page two"), a);
    }

    #[test]
    fn cursor_is_an_opaque_capability_not_an_offset() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");

        let page = store.insert(sc.clone(), rows(4), 2, base).expect("insert");
        let cursor = page.next_cursor.clone().expect("more pages");
        assert!(
            Uuid::parse_str(&cursor).is_ok(),
            "cursor must be a random uuid, not a client-shifted offset"
        );

        for forged in ["offset=2", "0", "", "2:2", "..", "uuid:2"] {
            assert_eq!(
                store.next(forged, &sc, base).unwrap_err(),
                PageError::InvalidRequest,
                "fabricated cursor {forged:?} must not resolve"
            );
        }

        // An unissued uuid in another legal encoding is still unissued.
        let unissued = Uuid::new_v4().to_string().to_uppercase().replace('-', "");
        assert_eq!(
            store.next(&unissued, &sc, base).unwrap_err(),
            PageError::Expired
        );
    }

    #[test]
    fn scope_mismatch_is_rejected_on_every_dimension() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");
        store.insert(sc.clone(), rows(6), 2, base).expect("insert");
        let cursor = store
            .insert(sc.clone(), rows(6), 2, base)
            .expect("insert")
            .next_cursor
            .expect("cursor");

        let variants = [
            SnapshotScope {
                resource: "messages".to_string(),
                ..sc.clone()
            },
            SnapshotScope {
                account_dir: "/data/Accounts/wxid_other".to_string(),
                ..sc.clone()
            },
            SnapshotScope {
                epoch: "e-2".to_string(),
                ..sc.clone()
            },
        ];

        for wrong in variants {
            assert_eq!(
                store.next(&cursor, &wrong, base).unwrap_err(),
                PageError::ScopeChanged
            );
        }
        assert_eq!(
            store.next(&cursor, &sc, base).expect("owner scope still reads"),
            store.next(&cursor, &sc, base).expect("owner scope still reads")
        );
    }

    #[test]
    fn unknown_and_tampered_cursors_return_no_page() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");
        let cursor = store
            .insert(sc.clone(), rows(4), 2, base)
            .expect("insert")
            .next_cursor
            .expect("cursor");

        // Well formed but never issued: after a restart the registry is empty
        // too, so the caller is told Expired rather than a page is fabricated.
        assert_eq!(
            store.next(&tamper(&cursor), &sc, base).unwrap_err(),
            PageError::Expired
        );
        assert_eq!(
            store
                .next(&Uuid::new_v4().to_string(), &sc, base)
                .unwrap_err(),
            PageError::Expired
        );
        assert_eq!(
            store.next(&cursor, &sc, base).expect("real cursor unaffected"),
            store.next(&cursor, &sc, base).expect("real cursor unaffected")
        );
    }

    #[test]
    fn ttl_is_absolute_and_does_not_extend_while_paging() {
        let base = t0();
        let mut store = FrozenPageStore::with_config(PageStoreConfig {
            ttl: Duration::from_secs(60),
            ..small_config()
        });
        let sc = scope("contacts");

        let first = store.insert(sc.clone(), rows(9), 2, base).expect("insert");
        let cursor = first.next_cursor.clone().expect("cursor");

        // Paging at t+50s must not renew the snapshot.
        let late = store
            .next(&cursor, &sc, at(base, 50))
            .expect("still inside the window");
        assert_eq!(late.total, 9);
        assert_eq!(
            store.next(&cursor, &sc, at(base, 59)).expect("last live second").snapshot_id,
            first.snapshot_id
        );
        assert_eq!(
            store
                .next(&late.next_cursor.unwrap(), &sc, at(base, 60))
                .unwrap_err(),
            PageError::Expired,
            "expires_at itself is already expired"
        );
        assert_eq!(
            store.next(&cursor, &sc, at(base, 61)).unwrap_err(),
            PageError::Expired
        );
        assert_eq!(store.live_snapshot_count(at(base, 61)), 0);
    }

    #[test]
    fn expired_cursor_stays_expired_on_retry() {
        let base = t0();
        let mut store = FrozenPageStore::with_config(PageStoreConfig {
            ttl: Duration::from_secs(30),
            ..small_config()
        });
        let sc = scope("contacts");
        let cursor = store
            .insert(sc.clone(), rows(4), 2, base)
            .expect("insert")
            .next_cursor
            .expect("cursor");

        assert_eq!(
            store.next(&cursor, &sc, at(base, 31)).unwrap_err(),
            PageError::Expired
        );
        assert_eq!(
            store.next(&cursor, &sc, at(base, 120)).unwrap_err(),
            PageError::Expired
        );
    }

    #[test]
    fn snapshot_count_budget_refuses_instead_of_evicting_live_samples() {
        let base = t0();
        let mut store = FrozenPageStore::with_config(PageStoreConfig {
            max_snapshots: 2,
            ..small_config()
        });

        let first = store.insert(scope("a"), rows(4), 2, base).expect("a");
        let second = store.insert(scope("b"), rows(4), 2, base).expect("b");
        assert_eq!(
            store.insert(scope("c"), rows(4), 2, base).unwrap_err(),
            PageError::BudgetExceeded
        );

        // The refused insert must not have displaced the live samples.
        assert_eq!(
            store
                .next(&first.next_cursor.clone().unwrap(), &scope("a"), base)
                .expect("a intact")
                .snapshot_id,
            first.snapshot_id
        );
        assert_eq!(
            store
                .next(&second.next_cursor.clone().unwrap(), &scope("b"), base)
                .expect("b intact")
                .snapshot_id,
            second.snapshot_id
        );

        // Once the older snapshots expire, capacity is released again.
        let third = store
            .insert(scope("c"), rows(4), 2, at(base, 301))
            .expect("expired samples freed budget");
        assert_eq!(store.live_snapshot_count(at(base, 301)), 1);
        assert_eq!(third.total, 4);
    }

    #[test]
    fn per_snapshot_row_budget_blocks_oversized_sample() {
        let base = t0();
        let mut store = FrozenPageStore::with_config(PageStoreConfig {
            max_rows_per_snapshot: 4,
            ..small_config()
        });

        assert_eq!(
            store.insert(scope("contacts"), rows(5), 2, base).unwrap_err(),
            PageError::BudgetExceeded
        );
        assert_eq!(store.live_snapshot_count(base), 0);
        assert_eq!(store.total_tracked_bytes(), 0);
        assert_eq!(
            store
                .insert(scope("contacts"), rows(4), 2, base)
                .expect("at the limit is accepted")
                .items
                .len(),
            2
        );
    }

    #[test]
    fn per_snapshot_byte_budget_blocks_oversized_sample() {
        let base = t0();
        let mut store = FrozenPageStore::with_config(PageStoreConfig {
            max_bytes_per_snapshot: 120,
            ..small_config()
        });
        let sc = scope("contacts");

        assert_eq!(
            store.insert(sc.clone(), rows(10), 2, base).unwrap_err(),
            PageError::BudgetExceeded
        );
        assert_eq!(store.live_snapshot_count(base), 0);
        assert_eq!(store.total_tracked_bytes(), 0);

        let ok = store.insert(sc.clone(), rows(3), 2, base).expect("fits");
        assert_eq!(ok.total, 3);
        assert!(store.total_tracked_bytes() > 0);
    }

    #[test]
    fn total_byte_budget_is_shared_across_snapshots() {
        let base = t0();
        let mut store = FrozenPageStore::with_config(PageStoreConfig {
            max_bytes_per_snapshot: 200,
            max_total_bytes: 240,
            ..small_config()
        });

        store
            .insert(scope("a"), rows(4), 2, base)
            .expect("first fits");
        assert_eq!(
            store.insert(scope("b"), rows(4), 2, base).unwrap_err(),
            PageError::BudgetExceeded
        );
        assert_eq!(store.live_snapshot_count(base), 1);
        // Expiry releases the shared bytes.
        store
            .insert(scope("b"), rows(4), 2, at(base, 301))
            .expect("second fits after expiry");
        assert_eq!(store.live_snapshot_count(at(base, 301)), 1);
    }

    #[test]
    fn empty_sample_is_a_normal_terminal_page() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");

        let page = store.insert(sc.clone(), Vec::new(), 10, base).expect("insert");
        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
        assert_eq!(page.total, 0);
        assert!(!page.snapshot_id.is_empty());
        assert_eq!(
            store
                .next(&Uuid::new_v4().to_string(), &sc, base)
                .unwrap_err(),
            PageError::Expired
        );
    }

    #[test]
    fn invalid_page_size_is_rejected() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");

        for bad in [0usize, 1001, usize::MAX] {
            assert_eq!(
                store.insert(sc.clone(), rows(4), bad, base).unwrap_err(),
                PageError::InvalidRequest,
                "page_size {bad} is out of range"
            );
        }
        assert_eq!(store.live_snapshot_count(base), 0);
        assert_eq!(
            store
                .insert(sc.clone(), rows(4), 1, base)
                .expect("1 is allowed")
                .items
                .len(),
            1
        );
        assert_eq!(
            store
                .insert(sc.clone(), rows(4), 1000, base)
                .expect("1000 is allowed")
                .items
                .len(),
            4
        );
    }

    #[test]
    fn non_object_rows_are_rejected_without_partial_freeze() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");

        for bad in [json!([1, 2]), json!("text"), Value::Null, json!(7)] {
            assert_eq!(
                store.insert(sc.clone(), vec![row(0), bad], 2, base).unwrap_err(),
                PageError::InvalidRequest
            );
        }
        assert_eq!(store.live_snapshot_count(base), 0);
        assert_eq!(store.total_tracked_bytes(), 0);
    }

    #[test]
    fn scope_and_epoch_are_not_injected_into_items() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");
        let inserted = rows(2);

        let page = store
            .insert(sc.clone(), inserted.clone(), 2, base)
            .expect("insert");
        assert_eq!(page.items, inserted, "items are exactly the frozen rows");
        assert!(!page.items[0].as_object().unwrap().contains_key("epoch"));
        assert!(!page.items[0].as_object().unwrap().contains_key("account_dir"));

        let again = store.insert(sc, inserted, 2, base).expect("second insert");
        assert_ne!(
            page.snapshot_id, again.snapshot_id,
            "snapshot id is random, not derived from the scope"
        );
        assert_eq!(Uuid::parse_str(&page.snapshot_id).is_ok(), true);
    }

    #[test]
    fn page_error_codes_are_stable_and_carry_no_content() {
        let base = t0();
        let mut store = FrozenPageStore::new();
        let sc = scope("contacts");
        store.insert(sc.clone(), rows(4), 2, base).expect("insert");

        assert_eq!(
            [
                PageError::InvalidRequest.to_string(),
                PageError::Expired.to_string(),
                PageError::ScopeChanged.to_string(),
                PageError::BudgetExceeded.to_string(),
            ],
            [
                "invalid_request",
                "expired",
                "scope_changed",
                "budget_exceeded"
            ]
        );
        let debug = format!("{:?}", store.next("offset=1", &sc, base).unwrap_err());
        assert!(!debug.contains("offset=1"), "no echoed input: {debug}");
        assert_eq!(
            store.next("offset=1", &sc, base).unwrap_err().to_string(),
            "invalid_request"
        );
    }

    // ── Preparation off the lock ────────────────────────────────────────────

    #[test]
    fn a_sample_whose_deadline_passed_while_it_was_being_prepared_is_never_registered() {
        let mut store = FrozenPageStore::new();
        let now = t0();
        let config = small_config();
        let sc = scope("messages:c");

        let prepared = prepare(
            &config,
            sc.clone(),
            rows(400),
            100,
            &CollectionCancel::within(Duration::from_millis(0)),
        );
        assert_eq!(prepared.unwrap_err(), PageError::DeadlineExceeded);
        assert_eq!(store.live_snapshot_count(now), 0, "nothing was registered");
        assert_eq!(store.total_tracked_bytes(), 0, "no bytes were charged");

        // The same rows with time to spare freeze exactly what the single-call
        // form freezes: the split changed where the work happens, not what a
        // caller receives.
        let direct = store
            .insert(sc.clone(), rows(400), 100, now)
            .expect("the documented single call still works");
        let split = store
            .publish(
                prepare(&config, sc.clone(), rows(400), 100, &generous())
                    .expect("prepared"),
                now,
            )
            .expect("published");
        assert_eq!(ids(&direct), ids(&split));
        assert_eq!(direct.total, split.total);
        assert_eq!(store.live_snapshot_count(now), 2, "both samples are live");
    }

    #[test]
    fn the_deadline_of_a_preparation_is_its_own_code_and_leaves_the_budgets_alone() {
        assert_eq!(PageError::DeadlineExceeded.to_string(), "deadline_exceeded");
        let mut store = FrozenPageStore::new();
        let now = t0();
        store
            .insert(scope("messages:live"), rows(6), 2, now)
            .expect("insert");

        let refused = prepare(
            &small_config(),
            scope("messages:late"),
            rows(6),
            2,
            &CollectionCancel::within(Duration::from_millis(0)),
        );
        assert_eq!(refused.unwrap_err(), PageError::DeadlineExceeded);
        assert_eq!(
            store.live_snapshot_count(now),
            1,
            "a refused preparation must not consume a snapshot slot"
        );
        assert_eq!(
            store.total_tracked_bytes(),
            6 * serde_json::to_vec(&row(0)).unwrap().len(),
            "a refused preparation must not charge bytes"
        );
    }
}
