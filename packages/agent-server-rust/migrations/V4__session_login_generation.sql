-- Monotonic login generation for read scopes.
--
-- Advanced by the same UPDATE that writes `logged_in_user`, so the counter is a
-- durable property of the row: a re-login of the very same account also
-- produces a new generation, and an account directory alone can never stand in
-- for a login epoch.
--
-- DEFAULT 0 marks "no login has been recorded for this session yet". SQLite has
-- no `ADD COLUMN IF NOT EXISTS`; refinery applies each version exactly once, so
-- the plain statement is the correct one here.

ALTER TABLE sessions
    ADD COLUMN login_generation INTEGER NOT NULL DEFAULT 0;

-- Existing successful logins become the migration baseline; upgrading must not
-- require a new interactive login just to establish the counter.
UPDATE sessions SET login_generation = 1
WHERE login_state = 'logged_in' AND length(trim(logged_in_user)) > 0;

-- Quarantine for login writes that did not complete.
--
-- A row here means: a login or logout happened, and the session row may still be
-- naming the previous login. Without it a refused write is invisible — the row
-- keeps the old account and the old generation, and a reader would answer from
-- an epoch that is no longer on screen.
--
-- Written in the same transaction as the update that it qualifies, and deleted
-- by that same update, so the row is either current or quarantined. There is
-- deliberately no foreign key: the marker is a statement about a write, and it
-- must be possible to record one for a session whose row a failed write could
-- not find.
CREATE TABLE IF NOT EXISTS session_login_write (
    session_id      TEXT PRIMARY KEY,
    unverified_since TEXT NOT NULL
);
