---
"@agent-wechat/agent-server": patch
---

Reuse a verified private encrypted snapshot when a WeChat database's main file, WAL state, and key are unchanged. Each caller still opens its own read-only SQLCipher connection, while bounded cache eviction unlinks unused snapshot paths without invalidating already-open connections. Concurrent first misses are intentionally not single-flight; a four-entry cache can also churn when more than four databases are rotated through it.

Clear retained snapshot paths during graceful server shutdown after requests and media downloads have drained. Abrupt process termination can still leave private encrypted files; this change does not scan or remove other processes' directories.

Raise the per-snapshot processing budget from 256 MiB to 512 MiB, including the main database, WAL, merge index and merged image. This is not a process-wide memory limit and does not change the read deadline.
