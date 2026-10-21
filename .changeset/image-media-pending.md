---
"@agent-wechat/agent-server": patch
---

Return `pending` when the requested image variant is missing, unreadable, or cannot be decrypted, rather than returning an image result without data. Preserve image-quality selection and native transfer behavior.
