---
"@agent-wechat/agent-server": patch
---

Choose chat-selection click points inside the current visible list region rather than trusting stale row bounds or caller coordinates. Keep identity checks and HTTP error contracts unchanged, without scrolling or retrying selection.

Record privacy-filtered chat selection failure diagnostics, including the helper stage, stable reason, process outcome, elapsed time and output parse status, without logging target identities or raw helper output.
