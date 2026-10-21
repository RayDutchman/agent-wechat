# @agent-wechat/cli

Command-line tool for managing agent-wechat containers and interacting with WeChat.

This fork's CLI is built from this repository; do not install the identically named upstream npm package and assume it is this fork. See the [fork documentation](../../docs/src/content/docs/getting-started/container-setup/cli.mdx). Upstream docs and images may differ.

## Install

```bash
git clone --branch main https://github.com/RayDutchman/agent-wechat.git
cd agent-wechat
pnpm --filter cli... --filter shared... install --frozen-lockfile
pnpm --filter @agent-wechat/shared build
pnpm --filter @agent-wechat/cli build
node packages/cli/dist/cli.js --help
alias wx='node packages/cli/dist/cli.js'
```

This builds the fork's CLI locally; use `node packages/cli/dist/cli.js` without global installation or upstream npm packages.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed and running

Choose an image compatible with this fork, or build one locally using a WeChat `.deb` version you have selected and verified. `wx up` may use a default image that is not this fork's server; use the repository Compose file to select your image explicitly as described in the [deployment guide](../../docs/src/content/docs/getting-started/container-setup/docker-compose.mdx).

> **Note:** agent-wechat requires `SYS_PTRACE` and `seccomp=unconfined` to interact with the WeChat desktop process. It cannot run in serverless or restricted container environments (AWS Fargate, Cloud Run, etc.). Use a VM or bare-metal Docker host.

## Quick Start

```bash
# Start the container
wx up

# Check status
wx status

# Log in (displays QR code in terminal)
wx auth login

# List chats
wx chats list

# Send a message
wx messages send <chatId> --text "Hello"
```

## Commands

### Container

| Command | Description |
|---------|-------------|
| `wx up [--proxy user:pass@host:port]` | Start a configured container (check which image it uses) |
| `wx down` | Stop and remove the container (or stop Compose services when configured locally); volumes are retained |
| `wx logs` | Tail container logs |
| `wx status` | Show container up/down status and login status (when available) |

### Auth

| Command | Description |
|---------|-------------|
| `wx auth login` | Log in to WeChat (shows QR code) |
| `wx auth logout` | Log out of WeChat |
| `wx auth status` | Check login status |
| `wx auth token` | Show current auth token |
| `wx auth token --regenerate` | Generate a new auth token |

Login options:
- `--timeout <seconds>` — login timeout (default: 300)
- `--new` — switch to a new account instead of existing

### Chats

| Command | Description |
|---------|-------------|
| `wx chats list` | List chats |
| `wx chats get <chatId>` | Get details for a specific chat |
| `wx chats find <name>` | Search chats by username or name |
| `wx chats members <roomUsername>` | List group members |
| `wx chats open <chatId> [--clear-unreads]` | Open a chat in the WeChat UI |

List/find/members support `--limit <n>`, `--cursor <cursor>`, `--all`, and `--json`. Chats list also supports `--query <text>`; list/find/members support `--expected-account-username <username>`. These are cursor-paged reads; there is no `--offset` option.

### Messages

| Command | Description |
|---------|-------------|
| `wx messages list <chatId>` | List messages in a chat |
| `wx messages send <chatId>` | Send a message |
| `wx messages media <chatId> <localId>` | Download media for the uniquely matched message |
| `wx messages media <chatId> --server-id <id>` | Download media using the native message ID |

Send options:
- `--text "message"` — send text
- `--image photo.jpg` — send an image
- `--file document.pdf` — send a file
- `--voice audio.wav` — send audio as one or more voice notes; supports `--detach`

Media options:
- `--output <path>` — save to a specific file path
- `--full` — require an original-resolution image
- `--thumbnail` — save the cached preview
- `--best` — explicitly choose the default best-available image behavior

`wx messages list <chatId>` supports `--limit`, `--cursor`, `--order asc|desc`, `--config`, `--all`, and `--json`. `wx messages voice status <jobId>` and `wx messages voice cancel <jobId>` inspect or cancel voice jobs.

Media retrieval does not open a chat. A positional `localId` is resolved against the chat's V2 message pages and must identify exactly one message with a nonzero native `server_id`; `--server-id` uses that native ID directly. Neither option is an offset or page index. Media may be pending or unavailable; opening the chat separately may cause WeChat to cache additional image variants.

### Sessions

| Command | Description |
|---------|-------------|
| `wx session list` | List all sessions |
| `wx session create <name>` | Create a new session |
| `wx session start <id>` | Start a session |
| `wx session stop <id>` | Stop a session |
| `wx session delete <id>` | Delete a session |

### Other commands

| Command | Description |
|---------|-------------|
| `wx events watch [--json]` | Watch broad data-change hints (not a message history) |
| `wx contacts list` / `wx contacts find <name>` | List or search contacts |

### Debug

| Command | Description |
|---------|-------------|
| `wx screenshot [file]` | Save a screenshot (default: screenshot.png) |
| `wx a11y` | Dump the accessibility tree (`--format json|aria`) |

## Global Options

| Option | Description |
|--------|-------------|
| `-s, --session <name>` | Use a specific session (default: "default") |
| `-V, --version` | Show version |
| `-h, --help` | Show help |

## Configuration

The CLI reads configuration from environment variables and a local token file:

| Source | Description |
|--------|-------------|
| `AGENT_WECHAT_URL` | Server URL (default: `http://localhost:6174`) |
| `AGENT_WECHAT_TOKEN` | Auth token (overrides token file) |
| `~/.config/agent-wechat/token` | Auto-generated auth token |

The auth token is generated automatically on first run and shared with the container via a read-only volume mount.

## Running the Container

The CLI includes container helper commands; Docker Compose is useful when you need to select the image explicitly.

> **Note:** agent-wechat requires `SYS_PTRACE` and `seccomp=unconfined` because it uses ptrace to interact with the WeChat desktop process. It cannot run in serverless or restricted container environments (AWS Fargate, Cloud Run, Azure Container Instances, etc.). Use a VM or bare-metal Docker host.

### Docker Compose lifecycle

Use Docker Compose when you need to select an image explicitly; the CLI is also available for container management and API interaction:

```bash
docker compose --env-file deployment.env up -d
```

The Compose service uses:
- **Port 6174** — REST API + VNC web viewer at `/vnc/` (exposed to all interfaces)
- Persistent volumes for data and WeChat home directory
- An existing auth token mounted read-only; never replace it when reusing account data

To route all container traffic through a proxy:

```bash
wx up --proxy user:pass@host:port
```

This sets up a transparent proxy (redsocks + iptables) inside the container — invisible to WeChat. Prefix with `socks5://` for SOCKS5 proxies.

### Image and platform

The published Linux AMD64 image is `ghcr.io/raydutchman/agent-wechat:2026.10.21`, with WeChat 4.1.13.23 and interactive noVNC through the authenticated API proxy. Pull it before starting the host-network Compose deployment. Source builds require a suitable WeChat `.deb`; the mutable fallback does not guarantee that version.

Host networking exposes the API on the Docker host at port `6174`; clients outside the container must use the host's actual reachable address (not `agent-wechat` as a Docker service name).

### Building locally

To build the Docker image from source, provide the WeChat `.deb` as documented by the Docker build configuration; its mutable fallback is not a version pin.

```bash
# From the repo root — auto-detects host architecture
pnpm build:image

# Or specify architecture
pnpm build:image:arm64
pnpm build:image:amd64
```
