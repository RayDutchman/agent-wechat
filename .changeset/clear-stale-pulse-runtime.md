---
"@agent-wechat/cli": patch
---

Start the container reliably on restart by clearing PulseAudio's stale runtime directory before launch.

`/tmp` belongs to the container writable layer, so it survives `docker start`. After a run that
started PulseAudio successfully, the container keeps that run's runtime directory with a dead pid
file and a socket that no longer has a listener. The next `pulseaudio --start` then cannot take it
over, `voice-audio-ready` exhausts its 15 `pactl info` retries, and the entrypoint exits with
`PulseAudio voice devices did not become ready; WeChat was not started` — leaving the container
stopped and the port closed. The entrypoint now removes the stale directory before starting
PulseAudio, matching how it already clears a stale `/tmp/.X99-lock` before Xvfb.
