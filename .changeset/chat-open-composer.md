---
"@agent-wechat/agent-server": patch
---

Use the existing send-path composer selector when opening chats with unread clearing, supporting both `Send` and `Send(S)` and separately nested editor controls. This addresses "No action selected" for these layouts; the nested-layout fallback requires a unique editor paired with a nearby Send button and excludes Send Voice.
