# agent-wechat

A programmable WeChat interface. Controls a WeChat Linux client running in a Docker container through an HTTP API and the `wx` CLI.

**[本分支文档](#documentation)** · [上游文档（不适用于本分支）](https://thisnick.github.io/agent-wechat/)

这是 RayDutchman 维护的 fork，保留 Rust server、shared TypeScript client 和 CLI；OpenClaw extension 与 Wechaty packages 已移除。上游 npm CLI、容器镜像和文档不代表本 fork 版本；本 fork 的旧路由也已退役。保留上游 LICENSE 与来源归属；变更通过 Git 历史和原始源码公开。

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

# Pull the Linux AMD64 image, then complete Docker Setup below before login
docker pull ghcr.io/raydutchman/agent-wechat:2026.10.21

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

The repository's [`docker-compose.yml`](./docker-compose.yml) uses host networking. Pull `ghcr.io/raydutchman/agent-wechat:2026.10.21` and set `AGENT_WECHAT_IMAGE` to that tag. This Linux AMD64 image contains WeChat 4.1.13.23 and the verified server; it includes interactive noVNC through the authenticated API proxy. The source entrypoint retains its read-only default. See the [Compose deployment guide](./docs/src/content/docs/getting-started/container-setup/docker-compose.mdx) for external volumes and the token file. Preserve the existing token when reusing account data. Clients connect to the Docker host's reachable address on port `6174`.

## Documentation

文档网站使用 **Astro + Starlight**。源文件位于 `docs/src/content/docs/`；`.mdx` 是可嵌入组件的 Markdown，标题、列表、表格和代码块仍使用 Markdown 写法。网站也支持普通 `.md`，不需要为了纯文本内容使用组件。

在想查看的仓库或工作树根目录执行：

```bash
pnpm --filter @agent-wechat/docs install --frozen-lockfile
pnpm --filter @agent-wechat/docs dev
```

打开终端输出的网址。当前配置的基础路径是 `/agent-wechat`，默认端口下首页为 `http://127.0.0.1:4321/agent-wechat/`，数据库字段字典为 `http://127.0.0.1:4321/agent-wechat/operations/data/`。若提示 `astro: not found`，先在该工作树安装上述依赖。

构建静态站点并预览：

```bash
pnpm --filter @agent-wechat/docs build
pnpm --filter @agent-wechat/docs preview
```

常用入口：[使用指南](./docs/src/content/docs/operations/user-guide.mdx)、[API 契约](./docs/src/content/docs/reference/api.mdx)、[数据库字段字典](./docs/src/content/docs/operations/data.mdx)。当前用法放在文档站，开发过程记录放在 `.superpowers/`；历史设计不代替现行 API 契约。

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
