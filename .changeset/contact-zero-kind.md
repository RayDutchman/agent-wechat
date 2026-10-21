---
"@agent-wechat/agent-server": patch
---

Classify contacts with `local_type=0` as `unknown` when no stronger account-kind evidence is present, rather than treating that value alone as `system`. Preserve explicit system accounts, group and enterprise identifiers, business-account metadata, source fields, and existing row filtering. Contact kind does not indicate friendship or deletion status.
