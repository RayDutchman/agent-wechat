---
"@agent-wechat/cli": patch
---

Use the configured Compose project's agent-wechat service for status, startup readiness, and logs. Other running services no longer make a stopped WeChat container appear running. Preserve standalone behavior when no local container configuration exists.
