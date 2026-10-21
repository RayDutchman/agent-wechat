---
"@agent-wechat/agent-server": patch
---

Recognize an open conversation by its visible message pane and Send control instead of requiring a selected row in the chat list. A conversation whose row is outside the visible list no longer stops before text entry with `chat_not_open`. Chat titles are not identity gates; existing native chat selection, composer checks, and epoch/target send fences remain in place.
