---
"@agent-wechat/agent-server": patch
---

Validate extracted raw database credentials using SQLCipher 4 first-page authentication instead of opening live databases with the SQLCipher CLI. Schema validation remains on private snapshots; credential setup no longer reports a table count or credential prefix.
