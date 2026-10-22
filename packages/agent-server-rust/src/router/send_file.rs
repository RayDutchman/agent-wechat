//! Staging and trusted-session helpers shared by the unified V2 send handler.

use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::PathBuf;
use sha2::{Digest, Sha256};
use crate::ia::types::Session;

// ============================================
// Errors
// ============================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendFileError {
    InvalidFilename,
    IdentityUnavailable,
}

/// A filename is a single basename only. It may be non-ASCII (the original name
/// is preserved for the paste tool), but it may never name a path or a control
/// sequence, and it may not be `.` or `..`.
pub(crate) fn validate_filename(name: &str) -> Result<(), SendFileError> {
    if name.is_empty() || name == "." || name == ".." {
        return Err(SendFileError::InvalidFilename);
    }
    if name
        .chars()
        .any(|c| c == '/' || c == '\\' || c == '\0' || c.is_control())
    {
        return Err(SendFileError::InvalidFilename);
    }
    Ok(())
}

pub(crate) fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

// ============================================
// Staging
// ============================================

/// One staged upload in its own exclusive directory.
///
/// The directory name is a random UUID, never a timestamp, and both the
/// directory and the file are created exclusively. [`TempUpload::discard`]
/// removes exactly the one file and the one directory this value created; it
/// never walks or removes an unknown tree. [`Drop`] is the last line of defence
/// so every exit path, including an early `?`, cleans up.
pub(crate) struct TempUpload {
    dir: PathBuf,
    file: PathBuf,
}

impl TempUpload {
    /// Stage `bytes` under a fresh UUID directory owned by `owner`.
    ///
    /// `owner` is the uid/gid the trusted session's login user maps to, so the
    /// client process can read the file the paste points it at. The directory
    /// stays `0700` and the file stays `0600`; only the owner changes.
    pub(crate) fn stage(filename: &str, bytes: &[u8], owner: (u32, u32)) -> std::io::Result<Self> {
        Self::stage_with(filename, bytes, owner, set_owner)
    }

    /// The staging body with an injectable owner step, so its failure path can be
    /// exercised without a privileged or cross-user filesystem.
    fn stage_with<F>(
        filename: &str,
        bytes: &[u8],
        owner: (u32, u32),
        apply_owner: F,
    ) -> std::io::Result<Self>
    where
        F: Fn(&std::path::Path, (u32, u32)) -> std::io::Result<()>,
    {
        let dir = std::env::temp_dir().join(format!(
            "agent-wechat-send-file-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::DirBuilder::new().mode(0o700).create(&dir)?;

        // From here every `?` drops `staged`, which removes exactly the file and
        // the directory this call created.
        let staged = TempUpload {
            file: dir.join(filename),
            dir,
        };
        apply_owner(&staged.dir, owner)?;

        let mut handle = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staged.file)?;
        apply_owner(&staged.file, owner)?;
        handle.write_all(bytes)?;
        drop(handle);
        Ok(staged)
    }

    pub(crate) fn path(&self) -> &str {
        self.file.to_str().unwrap_or_default()
    }

    pub(crate) fn discard(&self) -> std::io::Result<()> {
        match std::fs::remove_file(&self.file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        match std::fs::remove_dir(&self.dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }
}

impl Drop for TempUpload {
    fn drop(&mut self) {
        if self.discard().is_err() {
            // Stable code only: the path and the filename must not reach a log.
            tracing::error!(
                code = "temp_cleanup_failed",
                "fenced file send could not remove its staged upload"
            );
        }
    }
}

/// Give one staged path to `owner` when it is not already there.
///
/// A same-user deployment creates the path as the target user already, so this
/// is a no-op and needs no privilege; only a cross-user deployment (a root
/// server staging for the client's user) calls `chown`, and a caller without the
/// privilege fails explicitly instead of leaving a file the client cannot read.
fn set_owner(path: &std::path::Path, owner: (u32, u32)) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(path)?;
    if metadata.uid() == owner.0 && metadata.gid() == owner.1 {
        return Ok(());
    }
    std::os::unix::fs::chown(path, Some(owner.0), Some(owner.1))
}

/// Parse the uid and gid of one account from the traditional `/etc/passwd`
/// layout. `name` is the trusted session's login user, never a request field.
fn passwd_ids(contents: &str, name: &str) -> Option<(u32, u32)> {
    if name.is_empty() || name.contains(':') || name.chars().any(char::is_control) {
        return None;
    }
    contents.lines().find_map(|line| {
        let mut fields = line.split(':');
        let user = fields.next()?;
        let _password = fields.next()?;
        let uid = fields.next()?.parse::<u32>().ok()?;
        let gid = fields.next()?.parse::<u32>().ok()?;
        (user == name).then_some((uid, gid))
    })
}

/// One trusted execution target: the session and the uid/gid its login user maps
/// to. Resolved once per request so the ownership granted to the staged file and
/// the session that drives the UI cannot drift apart.
#[derive(Debug)]
pub(crate) struct SendFileTarget {
    pub session: Session,
    pub owner: (u32, u32),
}

pub(crate) fn target_from_session(session: Session, passwd: &str) -> Result<SendFileTarget, SendFileError> {
    let owner = passwd_ids(passwd, &session.linux_user).ok_or(SendFileError::IdentityUnavailable)?;
    Ok(SendFileTarget { session, owner })
}
