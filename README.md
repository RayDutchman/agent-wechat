# agent-wechat

A programmable WeChat interface. Controls a WeChat Linux client running in a Docker container through an HTTP API and the `wx` CLI.

**[Documentation](https://raydutchman.github.io/agent-wechat/)** · [Upstream](https://github.com/thisnick/agent-wechat)

This RayDutchman-maintained fork retains the Rust server, shared TypeScript client, and CLI; the OpenClaw extension and Wechaty packages have been removed. Upstream npm packages, container images, and documentation do not describe this fork, and this fork's legacy routes have been retired. The upstream license and attribution are retained; changes are available in Git history and the source code.

## Packages

| Package | npm | Description |
|---------|-----|-------------|
| [`@agent-wechat/cli`](./packages/cli) | Built from this repository | CLI for managing the Docker container and interacting with WeChat |

## What It Does

- **Read** chats, messages, and media (images, voice, files) via REST API
- **Send** text messages, images, files, and voice notes
- **Login** via QR code displayed in your terminal
- **Monitor** data-change hints over WebSocket, then read messages through the API; consumers handle reconnects and periodic catch-up

## Requirements

- Docker (Colima on macOS, or Docker Desktop)
- Node.js >= 22 (for CLI)
- pnpm (for development)
- **Not compatible with serverless environments** — requires ptrace capabilities

## Quick Start

```bash
# Clone this fork's main branch and build its CLI locally
git clone --branch main https://github.com/RayDutchman/agent-wechat.git
cd agent-wechat
pnpm --filter @agent-wechat/cli... --filter @agent-wechat/shared... install --frozen-lockfile
pnpm --filter @agent-wechat/shared build
pnpm --filter @agent-wechat/cli build
node packages/cli/dist/cli.js --help
alias wx='node packages/cli/dist/cli.js'

# Pull the image for your platform, then complete Docker Setup below before login
docker pull ghcr.io/raydutchman/agent-wechat:2026.10.29

# Login (displays QR code in terminal)
wx auth login

# List your chats
wx chats list

# Send a message
wx messages send <chatId> --text "Hello"

# Record audio as a WeChat voice note
wx messages send <chatId> --voice ./reply.mp3

# Read messages
wx messages list <chatId>

# Stop the container
wx down
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `wx up [--proxy user:pass@host:port]` | Start a configured WeChat container (default image may be upstream) |
| `wx down` | Stop the configured Compose service; without local configuration, stop and remove the default container |
| `wx logs` | Stream container logs |
| `wx status` | Show server and login status |
| `wx auth login` | Login flow (shows QR code) |
| `wx auth status` | Show login status |
| `wx auth logout` | Log out of WeChat |
| `wx auth token [--regenerate]` | Show or regenerate the local API token |
| `wx chats list` | List chats |
| `wx chats get <id>` | Show one chat |
| `wx chats open <id>` | Select a chat in WeChat |
| `wx chats find <name>` | Find chat by name |
| `wx chats members <id>` | Read locally available group members |
| `wx contacts list` / `wx contacts find <name>` | List or find contacts |
| `wx messages list <id>` | List messages in a chat |
| `wx messages send <id> --text <msg>` | Send text message |
| `wx messages send <id> --image <file>` | Send image |
| `wx messages send <id> --file <file>` | Send a file attachment |
| `wx messages send <id> --voice <audio>` | Send audio as one or more voice notes and wait for verification |
| `wx messages send <id> --voice <audio> --detach` | Start a voice job and return its ID immediately |
| `wx messages voice status <jobId>` | Check voice-job progress and sent message IDs |
| `wx messages voice cancel <jobId>` | Request cancellation of a voice job |
| `wx messages media <id> <localId> [-o <path>] [--full\|--thumbnail]` | Save an attachment; images use the best available copy by default |
| `wx messages media <id> --server-id <serverId> [-o <path>]` | Retrieve media using the native message ID |
| `wx events watch --json` | Subscribe to data-change hints as JSON lines |

Run `wx --help` or `wx <command> --help` for all commands and options.

Local Compose lifecycle configuration is described in the [CLI container guide](./docs/src/content/docs/getting-started/container-setup/cli.mdx).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Docker Container                     │
│                                                     │
│   WeChat Linux  ←──  Xvfb + AT-SPI (accessibility)  │
│        ↕                                            │
│   agent-server (Rust/Axum, port 6174)               │
│     - FSM engine for UI automation                  │
│     - REST + WebSocket API                          │
└──────────────────────┬──────────────────────────────┘
                       │ HTTP / WebSocket
                       ↓
               CLI or AI agent
```

- **UI automation**: Login, open chats, send messages — all via deterministic FSM (no LLM needed)
- **API**: REST endpoints for supported operations and a WebSocket login flow

## Docker Setup

**Docker Compose**

The repository's [`docker-compose.yml`](./docker-compose.yml) uses host networking. The published `2026.10.29` image pins WeChat 4.1.13.23 for AMD64 and ARM64. Both architecture images passed isolated startup smoke checks; these do not verify login or account-data behavior. This release replaces the previous image's unpinned package download. See the [Compose deployment guide](./docs/src/content/docs/getting-started/container-setup/docker-compose.mdx) for build and deployment details.

## Documentation

See the [documentation site](https://raydutchman.github.io/agent-wechat/), including the [User Guide](./docs/src/content/docs/operations/user-guide.mdx), [API reference](./docs/src/content/docs/reference/api.mdx), and [database field dictionary](./docs/src/content/docs/operations/data.mdx).

Documentation sources are in `docs/src/content/docs/`. Run `pnpm --filter @agent-wechat/docs dev` to preview changes locally.

## Development

```bash
pnpm install
pnpm build                   # Build CLI + shared types
pnpm dev:deploy              # Cross-compile Rust server + deploy to running container
pnpm build:image:arm64       # Build Docker image (Apple Silicon)
pnpm build:image:amd64       # Build Docker image (Intel)
```

See [AGENTS.md](./AGENTS.md) for contributor guidance and implementation pointers.

## Ports

| Port | Service |
|------|---------|
| 6174 | Agent server REST API + VNC web viewer at `/vnc/` |
