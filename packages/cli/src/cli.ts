import { Command, Option } from "commander";
import { SendMessageV2Error, WeChatClient, messageTypeDictionary, type ContactKind, type WeChatClientOptions, type MessageV2, type ListMessagesV2Options, type SendMessageV2Result } from "@agent-wechat/shared";
import { createSubscriptionClient, type SubscriptionClientOptions } from "./lib/client.js";
import { readAllPages } from "./lib/read-pages.js";
import { spawn, execSync, execFileSync } from "child_process";
import { randomBytes, createHash } from "crypto";
import fs from "fs";
import qrTerminal from "qrcode-terminal";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

declare const PKG_VERSION: string;
const VERSION = PKG_VERSION;
const CONTAINER_NAME = "agent-wechat";
const GHCR_IMAGE = "ghcr.io/thisnick/agent-wechat";
const DEFAULT_PORT = 6174;

// Get monorepo root (cli is at packages/cli)
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MONOREPO_ROOT = path.resolve(__dirname, "../../..");

// Auth token paths
const TOKEN_DIR = path.join(os.homedir(), ".config", "agent-wechat");
const TOKEN_PATH = path.join(TOKEN_DIR, "token");
const CONTAINER_CONFIG_PATH = path.join(TOKEN_DIR, "container.json");

interface ContainerConfig { project: string; files: string[]; env_file: string }

function readContainerConfig(): ContainerConfig | undefined {
  if (!fs.existsSync(CONTAINER_CONFIG_PATH)) return undefined;
  let parsed: unknown;
  try { parsed = JSON.parse(fs.readFileSync(CONTAINER_CONFIG_PATH, "utf8")); }
  catch (error) { throw new Error(`Invalid container config ${CONTAINER_CONFIG_PATH}: ${error instanceof Error ? error.message : String(error)}`); }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("container.json must be a JSON object");
  const config = parsed as Record<string, unknown>;
  if (Object.keys(config).some((key) => !["project", "files", "env_file"].includes(key))) throw new Error("container.json accepts only project, files, and env_file");
  if (typeof config.project !== "string" || !config.project.trim()) throw new Error("container.json project must be a non-empty string");
  if (!Array.isArray(config.files) || config.files.length === 0 || config.files.some((file) => typeof file !== "string" || !path.isAbsolute(file) || !fs.existsSync(file) || !fs.statSync(file).isFile())) throw new Error("container.json files must be a non-empty array of existing absolute file paths");
  if (typeof config.env_file !== "string" || !path.isAbsolute(config.env_file) || !fs.existsSync(config.env_file) || !fs.statSync(config.env_file).isFile()) throw new Error("container.json env_file must be an existing absolute file path");
  return { project: config.project, files: config.files as string[], env_file: config.env_file };
}

function composeArgs(config: ContainerConfig): string[] {
  return ["compose", "--project-name", config.project, "--env-file", config.env_file, ...config.files.flatMap((file) => ["-f", file])];
}

function ensureToken(): string {
  try {
    const existing = fs.readFileSync(TOKEN_PATH, "utf-8").trim();
    if (existing) return existing;
  } catch {
    // File doesn't exist, generate one
  }
  fs.mkdirSync(TOKEN_DIR, { recursive: true });
  const token = randomBytes(32).toString("hex");
  fs.writeFileSync(TOKEN_PATH, token + "\n", { mode: 0o600 });
  console.log(`Auth token generated: ${TOKEN_PATH}`);
  return token;
}

function printNoVncUrl() {
  const token = readToken();
  if (token) {
    console.log(`noVNC: http://localhost:${DEFAULT_PORT}/vnc/?token=${token}&autoconnect=true`);
  } else {
    console.log(`noVNC: http://localhost:${DEFAULT_PORT}/vnc/`);
  }
}

function readToken(): string | undefined {
  try {
    const t = fs.readFileSync(TOKEN_PATH, "utf-8").trim();
    return t || undefined;
  } catch {
    return undefined;
  }
}

interface Config {
  serverUrl: string;
  token?: string;
}

function getConfig(): Config {
  return {
    serverUrl: process.env.AGENT_WECHAT_URL || `http://localhost:${DEFAULT_PORT}`,
    token: process.env.AGENT_WECHAT_TOKEN || readToken(),
  };
}

function getImageTag(): string {
  return `${GHCR_IMAGE}:${VERSION}`;
}

// Create program
const program = new Command();

program
  .name("wx")
  .description("WeChat automation CLI")
  .version(VERSION)
  .option("-s, --session <name>", "Use specified session", "default");

// Helper to create REST client
function getClient(): WeChatClient {
  const config = getConfig();
  const opts = program.opts();
  return new WeChatClient({
    baseUrl: config.serverUrl,
    token: config.token,
    sessionId: opts.session,
  });
}

// Helper to get subscription client options (WebSocket login)
function getSubscriptionOptions(): SubscriptionClientOptions {
  const config = getConfig();
  const opts = program.opts();
  return {
    url: config.serverUrl,
    token: config.token,
    sessionId: opts.session,
  };
}

// ============================================
// Container Commands
// ============================================

program
  .command("up")
  .description("Start the WeChat container")
  .option("--proxy <url>", "Transparent proxy (user:pass@host:port)")
  .action((opts) => cmdUp(opts));

program
  .command("down")
  .description("Stop the container")
  .action(cmdDown);

program
  .command("logs")
  .description("Show container logs")
  .action(cmdLogs);

// ============================================
// Session Commands
// ============================================

const sessionCmd = program
  .command("session")
  .description("Manage sessions");

sessionCmd
  .command("list")
  .description("List all sessions")
  .action(async () => {
    await cmdSessionList(getClient());
  });

sessionCmd
  .command("create <name>")
  .description("Create a new session")
  .action(async (name: string) => {
    await cmdSessionCreate(getClient(), name);
  });

sessionCmd
  .command("start <id>")
  .description("Start a session")
  .action(async (id: string) => {
    await cmdSessionStart(getClient(), id);
  });

sessionCmd
  .command("stop <id>")
  .description("Stop a session")
  .action(async (id: string) => {
    await cmdSessionStop(getClient(), id);
  });

sessionCmd
  .command("delete <id>")
  .description("Delete a session")
  .action(async (id: string) => {
    await cmdSessionDelete(getClient(), id);
  });

// ============================================
// API Commands
// ============================================

program
  .command("status")
  .description("Show container and login status")
  .option("-j, --json", "Output the V2 account observation as JSON")
  .action(async (opts: { json?: boolean }) => {
    await cmdStatus(getClient(), opts.json ?? false);
  });

const eventsCmd = program.command("events").description("Change notification commands");
eventsCmd
  .command("watch")
  .description("Watch for broad data-change hints")
  .option("-j, --json", "Write API event objects as JSON Lines")
  .action(async (opts: { json?: boolean }) => {
    await cmdEventsWatch(opts.json ?? false);
  });

// ============================================
// Auth Commands
// ============================================

const authCmd = program
  .command("auth")
  .description("Authentication commands");

authCmd
  .command("login")
  .description("Log in to WeChat (shows QR code)")
  .option("-t, --timeout <seconds>", "Timeout in seconds", "300")
  .option("-n, --new", "Switch to new account instead of existing")
  .action(async (opts) => {
    const timeoutMs = parseInt(opts.timeout, 10) * 1000;
    await cmdLogin(getSubscriptionOptions(), timeoutMs, opts.new ?? false);
  });

authCmd
  .command("logout")
  .description("Log out of WeChat")
  .action(async () => {
    const client = getClient();
    const result = await client.logout();
    if (result.success) {
      console.log("Logged out");
    } else {
      console.log(`Logout failed: ${result.error ?? "unknown error"}`);
    }
  });

authCmd
  .command("status")
  .description("Check login status")
  .option("-j, --json", "Output the V2 account observation as JSON")
  .action(async (opts: { json?: boolean }) => {
    const client = getClient();
    const auth = await client.getAccount();
    if (opts.json) { console.log(JSON.stringify(auth)); return; }
    if (auth.status === "logged_in") {
      console.log(`Logged in${auth.account_username ? ` as ${auth.account_username}` : ""}`);
    } else {
      console.log(`Status: ${auth.status.replace(/_/g, " ")}`);
    }
  });

authCmd
  .command("token")
  .description("Show or regenerate the auth token")
  .option("--regenerate", "Generate a new token")
  .action(async (opts: { regenerate?: boolean }) => {
    if (opts.regenerate) {
      fs.mkdirSync(TOKEN_DIR, { recursive: true });
      const token = randomBytes(32).toString("hex");
      fs.writeFileSync(TOKEN_PATH, token + "\n", { mode: 0o600 });
      console.log(`New token: ${token}`);
      console.log(`Restart the container for it to take effect: pnpm cli down && pnpm cli up`);
    } else {
      const token = ensureToken();
      console.log(token);
    }
  });

// ============================================
// Chats Commands
// ============================================

const chatsCmd = program
  .command("chats")
  .description("Chat management commands");

chatsCmd
  .command("list")
  .description("List chats from WeChat database")
  .option("-l, --limit <number>", "Maximum number of chats", "100")
  .option("--cursor <cursor>", "Continue a V2 read")
  .option("--query <text>", "Filter by chat username or name")
  .option("--expected-account-username <username>", "Require this account on the first page")
  .option("--all", "Read all pages and release the frozen sample")
  .option("-j, --json", "Output as JSON")
  .action(async (opts) => {
    await cmdChatsV2(getClient(), { limit: opts.cursor ? undefined : parseInt(opts.limit, 10), cursor: opts.cursor, q: opts.query, expected_account_username: opts.expectedAccountUsername, all: opts.all ?? false, json: opts.json ?? false });
  });

chatsCmd
  .command("get <chatId>")
  .description("Get details for a specific chat")
  .option("-j, --json", "Output as JSON")
  .action(async (chatId: string, opts) => {
    await cmdChatGetV2(getClient(), chatId, opts.json ?? false);
  });

chatsCmd
  .command("find <name>")
  .description("Find chat by name")
  .option("-l, --limit <number>", "Maximum number of results", "100")
  .option("--cursor <cursor>", "Continue a V2 read")
  .option("--expected-account-username <username>", "Require this account on the first page")
  .option("--all", "Read all pages and release the frozen sample")
  .option("-j, --json", "Output as JSON")
  .action(async (name: string, opts) => {
    await cmdChatsV2(getClient(), { limit: opts.cursor ? undefined : parseInt(opts.limit, 10), cursor: opts.cursor, q: name, expected_account_username: opts.expectedAccountUsername, all: opts.all ?? false, json: opts.json ?? false });
  });

chatsCmd
  .command("members <roomUsername>")
  .description("List members and their names in this room")
  .option("-l, --limit <number>", "Maximum number of members", "100")
  .option("--cursor <cursor>", "Continue a V2 read")
  .option("--expected-account-username <username>", "Require this account on the first page")
  .option("--all", "Read all pages and release the frozen sample")
  .option("-j, --json", "Output as JSON")
  .action(async (roomUsername: string, opts) => {
    await cmdGroupMembersV2(getClient(), roomUsername, { limit: opts.cursor ? undefined : parseInt(opts.limit, 10), cursor: opts.cursor, expected_account_username: opts.expectedAccountUsername, all: opts.all ?? false, json: opts.json ?? false });
  });

chatsCmd
  .command("open <chatId>")
  .description("Open a chat in WeChat UI (triggers media downloads + clears unread)")
  .option("--clear-unreads", "Clear unread count after opening")
  .action(async (chatId: string, opts: { clearUnreads?: boolean }) => {
    await cmdChatOpen(getClient(), chatId, opts.clearUnreads);
  });

// ============================================
// Contacts Commands
// ============================================

const contactsCmd = program
  .command("contacts")
  .description("Contact management commands");

contactsCmd
  .command("list")
  .description("List all contacts from WeChat address book")
  .option("-l, --limit <number>", "Maximum number of contacts", "100")
  .option("--cursor <cursor>", "Continue a V2 read")
  .option("--kind <kind>", "Filter by source classification")
  .option("--query <text>", "Filter by username, alias, remark or nickname")
  .option("--expected-account-username <username>", "Require this account on the first page")
  .option("--all", "Read all pages and release the frozen sample")
  .option("-j, --json", "Output as JSON")
  .action(async (opts) => {
    await cmdContactsV2(getClient(), { limit: opts.cursor ? undefined : parseInt(opts.limit, 10), cursor: opts.cursor, kind: opts.kind as ContactKind | undefined, q: opts.query, expected_account_username: opts.expectedAccountUsername, all: opts.all ?? false, json: opts.json ?? false });
  });

contactsCmd
  .command("find <name>")
  .description("Search contacts by name")
  .option("-l, --limit <number>", "Maximum number of results", "100")
  .option("--cursor <cursor>", "Continue a V2 read")
  .option("--expected-account-username <username>", "Require this account on the first page")
  .option("--kind <kind>", "Filter by source classification")
  .option("--all", "Read all pages and release the frozen sample")
  .option("-j, --json", "Output as JSON")
  .action(async (name: string, opts) => {
    await cmdContactsV2(getClient(), { limit: opts.cursor ? undefined : parseInt(opts.limit, 10), cursor: opts.cursor, kind: opts.kind as ContactKind | undefined, q: name, expected_account_username: opts.expectedAccountUsername, all: opts.all ?? false, json: opts.json ?? false });
  });

// ============================================
// Messages Commands
// ============================================

const messagesCmd = program
  .command("messages")
  .description("Message commands");

messagesCmd
  .command("list <chatId>")
  .description("List messages for a chat")
  .option("-l, --limit <number>", "Page size", "30")
  .option("--cursor <cursor>", "Continue a frozen message page")
  .option("--order <order>", "Fetch order: asc or desc; readable pages display oldest first", "desc")
  .option("--config <path>", "JSON file with include/exclude message type filters")
  .option("--expected-account-username <username>", "Assert the intended account on the first page")
  .option("--all", "Read all pages serially and release the frozen sample")
  .option("-j, --json", "Output as JSON")
  .action(async (chatId: string, opts: { limit: string; cursor?: string; order: string; config?: string; expectedAccountUsername?: string; all?: boolean; json?: boolean }) => {
    if (opts.order !== "asc" && opts.order !== "desc") throw new Error("--order must be asc or desc");
    const filters = opts.config ? readMessageTypeFilterConfig(opts.config) : {};
    await cmdMessagesV2(getClient(), chatId, {
      ...filters, limit: parseInt(opts.limit, 10), cursor: opts.cursor, order: opts.order as "asc" | "desc",
      expected_account_username: opts.expectedAccountUsername,
    }, opts.all ?? false, opts.json ?? false);
  });

messagesCmd
  .command("media <chatId> [localId]")
  .description("Save a media attachment (best available image, file, or voice)")
  .option("--server-id <id>", "Locate media by the native server message ID")
  .option("--full", "Require the original-resolution image")
  .option("--thumbnail", "Retrieve an image thumbnail instead of full resolution")
  .option("--best", "Retrieve the best available image (default)")
  .option("-o, --output <path>", "Output file path")
  .action(async (chatId: string, localIdStr: string | undefined, opts: { serverId?: string; full?: boolean; thumbnail?: boolean; best?: boolean; output?: string }) => {
    if (opts.serverId !== undefined && localIdStr !== undefined) {
      throw new Error("Use either <localId> or --server-id, not both.");
    }
    if (opts.serverId === undefined && localIdStr === undefined) {
      throw new Error("Provide <localId> or --server-id <id>.");
    }
    if (Number(!!opts.full) + Number(!!opts.thumbnail) + Number(!!opts.best) > 1) {
      console.error("Choose only one of --full, --thumbnail, or --best.");
      process.exit(1);
    }
    const quality = opts.full ? "full" : opts.thumbnail ? "thumbnail" : "best";
    await cmdMedia(getClient(), chatId, localIdStr, opts.output, quality, opts.serverId);
  });

messagesCmd
  .command("send <chatId>")
  .description("Send a message to a chat")
  .option("--text <text>", "Text message to send")
  .option("--image <path>", "Image file to send")
  .option("--file <path>", "File to send")
  .option("--voice <path>", "Record audio as one or more voice notes")
  .option("--detach", "Return after the voice job is queued")
  .option("--expected-account-username <username>", "Bind this send to the intended account")
  .option("-j, --json", "Output the V2 send result as JSON")
  .action(async (chatId: string, opts: { text?: string; image?: string; file?: string; voice?: string; detach?: boolean; expectedAccountUsername?: string; json?: boolean }) => {
    if (!opts.text && !opts.image && !opts.file && !opts.voice) {
      console.error("Must provide --text, --image, --file, or --voice");
      process.exit(1);
    }
    if (opts.voice) {
      if (opts.text || opts.image || opts.file) throw new Error("--voice must be sent on its own");
      const audio = fs.readFileSync(opts.voice);
      const job = await getClient().createVoiceJob(chatId, audio, randomBytes(16).toString("hex"));
      console.log(`Voice job: ${job.jobId}`);
      if (!opts.detach) await waitVoiceJob(getClient(), job.jobId);
      return;
    }
    if (opts.detach) throw new Error("--detach requires --voice");

    let image: { data: string; mimeType: string } | undefined;
    if (opts.image) {
      if (!fs.existsSync(opts.image)) {
        console.error(`File not found: ${opts.image}`);
        process.exit(1);
      }
      const data = fs.readFileSync(opts.image);
      const ext = path.extname(opts.image).toLowerCase();
      const mimeType = ext === ".jpg" || ext === ".jpeg" ? "image/jpeg" :
                       ext === ".gif" ? "image/gif" : "image/png";
      image = { data: data.toString("base64"), mimeType };
    }

    let file: { data: string; filename: string; sha256: string; size: number } | undefined;
    if (opts.file) {
      if (!fs.existsSync(opts.file)) {
        console.error(`File not found: ${opts.file}`);
        process.exit(1);
      }
      const data = fs.readFileSync(opts.file);
      file = { data: data.toString("base64"), filename: path.basename(opts.file), sha256: createHash("sha256").update(data).digest("hex"), size: data.byteLength };
    }

    await cmdSend(getClient(), chatId, opts.text, image, file, opts.expectedAccountUsername, opts.json ?? false);
  });

const voiceCmd = messagesCmd.command("voice").description("Inspect or cancel voice jobs");
voiceCmd.command("status <jobId>").action(async (jobId: string) => {
  console.log(JSON.stringify(await getClient().getVoiceJob(jobId), null, 2));
});
voiceCmd.command("cancel <jobId>").action(async (jobId: string) => {
  const job = await getClient().cancelVoiceJob(jobId);
  console.log(`Cancellation requested for ${job.jobId}`);
});

async function waitVoiceJob(client: WeChatClient, jobId: string): Promise<void> {
  let lastProgress = "";
  for (;;) {
    const job = await client.getVoiceJob(jobId);
    const verified = job.chunks.filter((chunk) => chunk.status === "verified").length;
    const progress = `${job.status}: ${verified}/${job.chunks.length} notes verified`;
    if (progress !== lastProgress) {
      console.log(progress);
      lastProgress = progress;
    }
    if (job.status === "completed") {
      console.log(`Message IDs: ${job.chunks.map((chunk) => chunk.messageId).join(", ")}`);
      return;
    }
    if (["failed", "cancelled", "needs_review"].includes(job.status)) {
      throw new Error(`${job.status}: ${job.error ?? "No further details"}. Job ${jobId}; do not resend the whole recording.`);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

// ============================================
// Update Command
// ============================================

program
  .command("update")
  .description("Update the agent-server binary in the running container to match CLI version")
  .action(cmdUpdate);

// ============================================
// Debug Commands
// ============================================

program
  .command("screenshot")
  .description("Save screenshot to file")
  .argument("[file]", "Output file path", "screenshot.png")
  .action(async (file: string) => {
    await cmdScreenshot(getClient(), file);
  });

program
  .command("a11y")
  .description("Dump accessibility tree")
  .addOption(
    new Option("-f, --format <format>", "Output format")
      .choices(["json", "aria"])
      .default("json")
  )
  .action(async (options: { format: "json" | "aria" }) => {
    await cmdA11y(getClient(), options.format);
  });

// ============================================
// Command Implementations
// ============================================

async function cmdStatus(client: WeChatClient, jsonOutput = false) {
  if (jsonOutput) {
    console.log(JSON.stringify(await client.getAccount()));
    return;
  }
  const container = getContainerRuntimeState();
  if (container === "up") {
    console.log("Container: up");
  } else if (container === "down") {
    console.log("Container: down");
    return;
  } else {
    console.log("Container: unknown (Docker unavailable)");
    return;
  }

  try {
    const account = await client.getAccount();
    console.log("Server: reachable");
  } catch (err) {
    console.log("Server: unreachable");
    console.log(`Error: ${err instanceof Error ? err.message : String(err)}`);
    return;
  }

  try {
    const auth = await client.getAccount();
    if (auth.status === "logged_in") {
      console.log(`Login: logged in${auth.account_username ? ` as ${auth.account_username}` : ""}`);
    } else {
      console.log(`Login: ${auth.status.replace(/_/g, " ")}`);
    }
  } catch {
    console.log("Login: unknown (auth status unavailable)");
  }
}

async function cmdEventsWatch(json: boolean): Promise<void> {
  const controller = new AbortController();
  const onInterrupt = () => controller.abort();
  process.once("SIGINT", onInterrupt);
  try {
    for await (const event of getClient().subscribeChanges({
      signal: controller.signal,
      onStatus: (status) => console.error(`Events: ${status}`),
    })) {
      if (json) console.log(JSON.stringify(event));
      else console.log("Data may have changed; read again if needed.");
    }
  } finally {
    process.removeListener("SIGINT", onInterrupt);
    controller.abort();
  }
}

function getContainerRuntimeState(): "up" | "down" | "unknown" {
  const containerConfig = readContainerConfig();
  if (containerConfig) {
    try {
      const running = execFileSync("docker", [...composeArgs(containerConfig), "ps", "--status", "running", "--quiet", "agent-wechat"], {
        encoding: "utf-8",
        stdio: ["ignore", "pipe", "ignore"],
      }).trim();
      return running ? "up" : "down";
    } catch {
      return "unknown";
    }
  }
  try {
    const existing = execSync(`docker ps -aq -f "name=^${CONTAINER_NAME}$"`, {
      encoding: "utf-8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    if (!existing) {
      return "down";
    }
    const running = execSync(`docker ps -q -f "name=^${CONTAINER_NAME}$"`, {
      encoding: "utf-8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    return running ? "up" : "down";
  } catch {
    return "unknown";
  }
}

async function cmdLogin(
  options: SubscriptionClientOptions,
  timeoutMs: number = 300_000,
  newAccount: boolean = false,
) {
  console.log(newAccount ? "Initiating login with new account...\n" : "Initiating login...\n");

  const { client, close } = createSubscriptionClient(options);
  let subscription: { unsubscribe: () => void } | null = null;

  // Handle Ctrl+C to abort subscription
  const abortHandler = () => {
    console.log("\n\nLogin cancelled.");
    if (subscription) {
      subscription.unsubscribe();
    }
    close();
    process.exit(0);
  };
  process.on("SIGINT", abortHandler);

  try {
    await new Promise<void>((resolve, reject) => {
      subscription = client.status.loginSubscription.subscribe(
        {
          timeoutMs,
          newAccount,
        },
        {
          onData: (event) => {
            switch (event.type) {
              case "status":
                console.log(`Status: ${event.message}`);
                break;
              case "qr":
                console.log("Scan this QR code with WeChat:\n");
                // Use binaryData if available (preserves exact bytes), fallback to string
                const qrInput = event.qrBinaryData
                  ? Buffer.from(event.qrBinaryData as number[]).toString("utf-8")
                  : event.qrData;
                qrTerminal.generate(qrInput as string, { small: true });
                console.log("\nWaiting for scan... (Ctrl+C to cancel)\n");
                break;
              case "phone_confirm":
                console.log(`\n📱 ${event.message || "Please confirm login on your phone"}\n`);
                break;
              case "login_success":
                console.log("\n\nLogin successful!");
                if (event.userId) {
                  console.log(`User ID: ${event.userId}`);
                }
                resolve();
                break;
              case "login_timeout":
                console.log("\n\nLogin timed out. Please try again.");
                resolve();
                break;
              case "error":
                console.error(`\nError: ${event.message}`);
                reject(new Error(event.message));
                break;
            }
          },
          onError: (err) => {
            console.error("\nConnection error:", err.message);
            reject(err);
          },
          onComplete: () => {
            // Subscription completed normally
          },
        }
      );
    });
  } finally {
    process.removeListener("SIGINT", abortHandler);
    close();
  }
}

type V2ReadPage<T> = { items: T[]; next_cursor: string | null; release_token: string | null };
type V2CliOptions = { limit?: number; cursor?: string; all: boolean; json: boolean; q?: string; kind?: ContactKind; expected_account_username?: string };

async function outputV2Pages<T>(
  client: WeChatClient,
  options: V2CliOptions,
  fetchPage: (pageOptions: { limit?: number; cursor?: string; q?: string; kind?: ContactKind; expected_account_username?: string }) => Promise<V2ReadPage<T>>,
  render: (items: T[]) => void,
) {
  const pageOptions = (cursor?: string, first = false) => ({
    ...(options.q !== undefined ? { q: options.q } : {}),
    ...(options.kind !== undefined ? { kind: options.kind } : {}),
    ...(first && cursor === undefined && options.expected_account_username !== undefined ? { expected_account_username: options.expected_account_username } : {}),
    ...(cursor !== undefined ? { cursor } : {}),
    ...(first && cursor === undefined && options.limit !== undefined ? { limit: options.limit } : {}),
  });
  if (options.all) {
    let first = true;
    let interrupted = false;
    const onInterrupt = () => { interrupted = true; };
    process.on("SIGINT", onInterrupt);
    try {
      await readAllPages(
        (cursor) => {
          const current = first ? options.cursor : cursor;
          first = false;
          return fetchPage(pageOptions(current, current === undefined));
        },
        (token) => client.releaseReadSnapshotV2(token),
        (page) => {
          if (options.json) console.log(JSON.stringify(page));
          else render(page.items);
        },
        () => interrupted,
      );
    } finally {
      process.removeListener("SIGINT", onInterrupt);
    }
    if (interrupted) throw new Error("V2 read interrupted");
    return;
  }
  const page = await fetchPage(pageOptions(options.cursor, options.cursor === undefined));
  if (options.json) console.log(JSON.stringify(page, null, 2));
  else render(page.items);
}

async function cmdChatsV2(client: WeChatClient, options: V2CliOptions) {
  await outputV2Pages(client, options, (pageOptions) => client.listChatsV2({ q: pageOptions.q, limit: pageOptions.limit, cursor: pageOptions.cursor, expected_account_username: pageOptions.expected_account_username }), (chats) => {
    if (chats.length === 0) { console.log("No chats found."); return; }
    console.log(`Found ${chats.length} chat(s):\n`);
    const width = Math.max(10, ...chats.map((chat) => chat.username.length));
    console.log(`${"Username".padEnd(width)}  Unread  Group  Name`);
    console.log("-".repeat(width + 30));
    for (const chat of chats) {
      console.log(`${chat.username.padEnd(width)}  ${chat.unread_count === null ? "—" : chat.unread_count > 0 ? String(chat.unread_count).padStart(2) : "  "}    ${chat.is_group ? "Y" : " "}      ${chat.display_name}`);
    }
  });
}

async function cmdChatGetV2(client: WeChatClient, username: string, json: boolean) {
  const chat = await client.getChatV2(username);
  if (json) { console.log(JSON.stringify(chat, null, 2)); return; }
  console.log(`Username:       ${chat.username}`);
  console.log(`Name:           ${chat.display_name}`);
  if (chat.remark) console.log(`Remark:         ${chat.remark}`);
  console.log(`Group:          ${chat.is_group ? "Yes" : "No"}`);
  console.log(`Unread:         ${chat.unread_count ?? "—"}`);
  if (chat.last_message_preview !== null) console.log(`Last message:   ${chat.last_sender_display_name ? `${chat.last_sender_display_name}: ` : ""}${chat.last_message_preview}`);
  if (chat.last_activity_at !== null) console.log(`Last activity:  ${chat.last_activity_at}`);
}

async function cmdContactsV2(client: WeChatClient, options: V2CliOptions) {
  await outputV2Pages(client, options, (pageOptions) => client.listContactsV2(pageOptions), (contacts) => {
    if (contacts.length === 0) { console.log("No contacts found."); return; }
    console.log(`Found ${contacts.length} contact(s):\n`);
    const width = Math.max(10, ...contacts.map((contact) => contact.username.length));
    console.log(`${"Username".padEnd(width)}  Kind        Name`);
    console.log("-".repeat(width + 30));
    for (const contact of contacts) console.log(`${contact.username.padEnd(width)}  ${contact.kind.padEnd(10)}  ${contact.display_name}`);
  });
}

async function cmdGroupMembersV2(client: WeChatClient, roomUsername: string, options: V2CliOptions) {
  await outputV2Pages(client, options, (pageOptions) => client.listGroupMembersV2(roomUsername, { limit: pageOptions.limit, cursor: pageOptions.cursor, expected_account_username: pageOptions.expected_account_username }), (members) => {
    if (members.length === 0) { console.log("No local member records found."); return; }
    console.log(`Found ${members.length} member(s) in ${roomUsername}:\n`);
    const width = Math.max(8, ...members.map((member) => member.username.length));
    console.log(`${"Username".padEnd(width)}  Group nickname  Name`);
    console.log("-".repeat(width + 30));
    for (const member of members) console.log(`${member.username.padEnd(width)}  ${member.group_nickname}  ${member.display_name}`);
  });
}

function readMessageTypeFilterConfig(filePath: string): Pick<ListMessagesV2Options, "include" | "exclude"> {
  const parsed: unknown = JSON.parse(fs.readFileSync(filePath, "utf8"));
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Message filter config must be a JSON object");
  const config = parsed as Record<string, unknown>;
  if (Object.keys(config).some((key) => key !== "include" && key !== "exclude")) throw new Error("Message filter config accepts only include and exclude");
  const parseList = (key: "include" | "exclude") => {
    if (!(key in config)) return undefined;
    const value = config[key];
    if (!Array.isArray(value)) throw new Error(`${key} must be an array`);
    return value.map((entry) => {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) throw new Error(`${key} entries must be objects`);
      const item = entry as Record<string, unknown>;
      if (Object.keys(item).some((field) => field !== "type" && field !== "sub_type")) throw new Error(`${key} entries accept only type and sub_type`);
      const validCode = (code: unknown) => Number.isInteger(code) && Number(code) >= 0 && Number(code) <= 0xFFFF_FFFF;
      if (!validCode(item.type) || (item.sub_type !== undefined && !validCode(item.sub_type))) throw new Error(`${key} type codes must be unsigned 32-bit integers`);
      return { type: item.type as number, ...(item.sub_type === undefined ? {} : { sub_type: item.sub_type as number }) };
    });
  };
  const include = parseList("include");
  const exclude = parseList("exclude");
  return { ...(include === undefined ? {} : { include }), ...(exclude === undefined ? {} : { exclude }) };
}

function messageTypeLabel(type: number, subType: number): string {
  const main = messageTypeDictionary.main.find((entry) => entry.code === type);
  if (!main) return `未知类型(${type})`;
  if (type === 49 && subType > 0) {
    const app = messageTypeDictionary.appmsg.find((entry) => entry.code === subType);
    return app ? `${app.label}(${subType})` : `应用消息(${subType})`;
  }
  return `${main.label}(${type})`;
}

async function cmdMessagesV2(client: WeChatClient, chatId: string, options: ListMessagesV2Options, all: boolean, jsonOutput: boolean) {
  if (!Number.isInteger(options.limit) || (options.limit ?? 0) < 1 || (options.limit ?? 0) > 1000) throw new Error("--limit must be an integer from 1 to 1000");
  if (!all) {
    const page = await client.listMessagesV2(chatId, options);
    if (jsonOutput) console.log(JSON.stringify(page, null, 2));
    else renderMessagesV2(page.items);
    return;
  }
  let first = true;
  let interrupted = false;
  const onInterrupt = () => { interrupted = true; };
  process.on("SIGINT", onInterrupt);
  try {
    await readAllPages(
      (nextCursor) => {
        if (first) {
          first = false;
          const { cursor: startCursor, ...initial } = options;
          return client.listMessagesV2(chatId, startCursor ? { cursor: startCursor } : initial);
        }
        return client.listMessagesV2(chatId, { cursor: nextCursor });
      },
      (token) => client.releaseReadSnapshotV2(token),
      (page) => {
        if (jsonOutput) console.log(JSON.stringify(page));
        else renderMessagesV2(page.items);
      },
      () => interrupted,
    );
  } finally {
    process.removeListener("SIGINT", onInterrupt);
  }
}

function renderMessagesV2(messages: MessageV2[]) {
  if (messages.length === 0) {
    console.log("No messages found.");
    return;
  }
  for (const message of [...messages].sort((a, b) => {
    if (a.timestamp === null) return b.timestamp === null ? 0 : 1;
    if (b.timestamp === null) return -1;
    return Date.parse(a.timestamp) - Date.parse(b.timestamp);
  })) {
    const time = message.timestamp === null ? "—" : new Date(message.timestamp).toLocaleString();
    const label = messageTypeLabel(message.type, message.sub_type);
    const sender = message.sender_display_name ?? message.sender_username ?? "";
    const preview = message.content.length > 120 ? `${message.content.slice(0, 120)}...` : message.content;
    console.log(`${message.server_id ?? "-"}  ${time}  ${label}  ${sender}  ${preview}`);
    if (message.reference) {
      const reference = message.reference;
      const display = reference.display_name ? `名称=${reference.display_name}` : "名称=未知";
      const identity = reference.sender_username ?? "身份缺失";
      console.log(`  引用: ${display} 身份=${identity} 会话=${reference.chat_username ?? "未知"} ${reference.content}`);
    }
    if (message.forwarded_record) {
      console.log(`  合并转发 [${message.forwarded_record.parse_status}]`);
      for (const item of message.forwarded_record.items) {
        const entryType = item.datatype === null
          ? "内部类型缺失"
          : messageTypeDictionary.forwarded_inner.find((entry) => entry.code === item.datatype)?.label ?? `未知内部类型(${item.datatype})`;
        const timeLabel = item.sourcetime ?? item.src_msg_create_time;
        const sourceHash = item.dataitem_source_hash_username ? ` source hash=${item.dataitem_source_hash_username}` : "";
        const details = [item.sourcename, timeLabel, item.from_new_msg_id].filter(Boolean).join("; ");
        console.log(`    ${entryType}(${item.datatype ?? "?"}) ${item.datadesc ?? ""} — ${details}${sourceHash}`);
      }
    }
  }
  console.log(`\n${messages.length} message(s) shown.`);
}

async function cmdMedia(
  client: WeChatClient, chatId: string, localId: string | undefined, outputPath?: string,
  quality: "full" | "thumbnail" | "best" = "best",
  serverId?: string,
) {
  let resolvedServerId = serverId;
  if (resolvedServerId === undefined && localId !== undefined) {
    let cursor: string | undefined;
    let releaseToken: string | null = null;
    const matches: MessageV2[] = [];
    try {
      do {
        const page = await client.listMessagesV2(chatId, { cursor });
        releaseToken = page.release_token ?? releaseToken;
        matches.push(...page.items.filter((message) => message.local_id === localId));
        cursor = page.next_cursor ?? undefined;
      } while (cursor !== undefined);
    } finally {
      if (releaseToken) await client.releaseReadSnapshotV2(releaseToken);
    }
    if (matches.length !== 1) throw new Error(matches.length ? "Local ID is ambiguous in this chat." : "Local ID was not found in this chat.");
    resolvedServerId = matches[0].server_id ?? undefined;
    if (!resolvedServerId || resolvedServerId === "0") throw new Error("The matching message has no non-zero server_id; media cannot be located safely.");
  }
  if (resolvedServerId === undefined) throw new Error("Provide a local ID or native server ID.");
  const result = await client.getMessageMediaV2(resolvedServerId, { chat_username: chatId, quality });

  if (result.type === "unsupported") {
    console.error("No media found for this message (unsupported type or not found).");
    process.exit(1);
  }
  if (result.type === "pending") {
    console.error("Media not yet available. WeChat may still be downloading it — try again shortly.");
    process.exit(1);
  }

  const outFile = outputPath ?? result.filename;

  if (result.data) {
    // Decode base64
    const buffer = Buffer.from(result.data, "base64");
    fs.writeFileSync(outFile, buffer);
    const variant = result.type === "image" && result.quality ? ` (${result.quality})` : "";
    console.log(`Saved ${result.type}${variant} to ${outFile} (${buffer.length} bytes)`);
  } else if (result.type === "image") {
    console.error(`Requested image ${quality} is not yet available.${quality === "full" ? " Regular phone sends may have no Original copy; try without --full." : ""}`);
    process.exit(1);
  } else if (result.type === "video") {
    console.error("Video not yet downloaded by WeChat. Try playing the video in the app first.");
    process.exit(1);
  } else {
    console.error(`Media type "${result.type}" has no downloadable data.`);
    process.exit(1);
  }
}

async function cmdChatOpen(client: WeChatClient, chatId: string, clearUnreads?: boolean) {
  console.log(`Opening chat ${chatId}...`);
  const result = await client.openChat(chatId, clearUnreads);

  if (result.ok) {
    console.log(`Chat opened: ${result.username} (index ${result.index})`);
  } else if (result.error === "NOT_LOGGED_IN") {
    console.error("Not logged in. Run: pnpm cli auth login");
    process.exit(1);
  } else {
    console.error(`Failed: ${result.error}`);
    process.exit(1);
  }
}

async function cmdSend(client: WeChatClient, chatId: string, text?: string, image?: { data: string; mimeType: string }, file?: { data: string; filename: string; sha256: string; size: number }, expectedAccountUsername?: string, jsonOutput = false) {
  if (text === undefined && image === undefined && file === undefined) {
    throw new Error("At least one of --text, --image, or --file is required.");
  }
  const what = file ? `file "${file.filename}"` : image ? "image" : "message";
  if (!jsonOutput) console.log(`Sending ${what} to ${chatId}...`);
  const payload = {
    chat_username: chatId,
    ...(expectedAccountUsername === undefined ? {} : { expected_account_username: expectedAccountUsername }),
    ...(text === undefined ? {} : { text }),
    ...(image ? { image: { data: image.data, mime_type: image.mimeType } } : {}),
    ...(file ? { file } : {}),
  };
  let result: SendMessageV2Result;
  try {
    result = await client.sendMessageV2(payload as Parameters<WeChatClient["sendMessageV2"]>[0]);
  } catch (error) {
    if (!(error instanceof SendMessageV2Error)) throw error;
    for (const ignored of error.result.ignored_fields) console.error(`Ignored send field: ${ignored}`);
    if (jsonOutput) console.log(JSON.stringify(error.result));
    console.error(`Failed to send message: ${error.result.error_code ?? "unconfirmed"}`);
    process.exit(1);
  }

  for (const ignored of result.ignored_fields) console.error(`Ignored send field: ${ignored}`);
  if (jsonOutput) console.log(JSON.stringify(result));
  if (result.success) {
    if (jsonOutput) return;
    console.log("Message UI confirmed (not a delivery receipt).");
  } else {
    console.error(`Failed to send message: ${result.error_code ?? "unconfirmed"}`);
    process.exit(1);
  }
}

async function cmdScreenshot(client: WeChatClient, outputPath: string) {
  console.log(`Capturing screenshot...`);
  const result = await client.screenshot();
  const buffer = Buffer.from(result.base64, "base64");
  fs.writeFileSync(outputPath, buffer);
  console.log(`Screenshot saved to ${outputPath}`);
}

async function cmdA11y(client: WeChatClient, format: "json" | "aria") {
  const result = await client.a11y(format);
  if (result.error) {
    console.error(`Error: ${result.error}`);
    return;
  }
  if (format === "aria" && result.aria) {
    console.log(result.aria);
  } else if (result.tree) {
    console.log(JSON.stringify(result.tree, null, 2));
  }
}

// ============================================
// Session Commands Implementation
// ============================================

async function cmdSessionList(client: WeChatClient) {
  const sessions = await client.listSessions();
  if (sessions.length === 0) {
    console.log("No sessions found.");
    return;
  }

  console.log(`Found ${sessions.length} session(s):\n`);
  for (const session of sessions) {
    const status = session.status === "running" ? "✓ running" : session.status;
    const login = session.loginState.status === "logged_in" ? "logged in" : session.loginState.status;
    console.log(`  ${session.id}: ${session.name}`);
    console.log(`    Status: ${status}, Login: ${login}`);
    console.log(`    Display: ${session.display}, VNC: ${session.vncPort}`);
    console.log(`    User: ${session.linuxUser}`);
    if (session.errorMessage) {
      console.log(`    Error: ${session.errorMessage}`);
    }
    console.log();
  }
}

async function cmdSessionCreate(client: WeChatClient, name: string) {
  console.log(`Creating session "${name}"...`);
  const session = await client.createSession(name);
  console.log(`Session created!`);
  console.log(`  ID: ${session.id}`);
  console.log(`  Name: ${session.name}`);
  console.log(`  User: ${session.linuxUser}`);
  console.log(`  Display: ${session.display}`);
  console.log(`  VNC Port: ${session.vncPort}`);
  console.log(`\nStart the session with: pnpm cli session start ${session.name}`);
}

async function cmdSessionStart(client: WeChatClient, idOrName: string) {
  console.log(`Starting session "${idOrName}"...`);
  const session = await client.startSession(idOrName);
  console.log(`Session started!`);
  console.log(`  Status: ${session.status}`);
  console.log(`  Display: ${session.display}`);
  console.log(`  VNC Port: ${session.vncPort}`);
  if (session.dbusAddress) {
    console.log(`  D-Bus: ${session.dbusAddress}`);
  }
  console.log(`\nLogin with: pnpm cli --session ${session.name} login`);
}

async function cmdSessionStop(client: WeChatClient, idOrName: string) {
  console.log(`Stopping session "${idOrName}"...`);
  const session = await client.stopSession(idOrName);
  console.log(`Session stopped.`);
  console.log(`  Status: ${session.status}`);
}

async function cmdSessionDelete(client: WeChatClient, idOrName: string) {
  console.log(`Deleting session "${idOrName}"...`);
  const result = await client.deleteSession(idOrName);
  if (result.success) {
    console.log(`Session deleted.`);
  } else {
    console.error(`Failed to delete session.`);
    process.exit(1);
  }
}

// ============================================
// Update Command Implementation
// ============================================

async function cmdUpdate() {
  const version = VERSION;
  console.log(`Updating agent-server to v${version}...`);

  // Find running container
  let container: string;
  try {
    container = execSync(
      `docker ps --filter "name=agent-wechat" --format "{{.Names}}" | head -1`,
      { encoding: "utf-8" }
    ).trim();
  } catch {
    container = "";
  }
  if (!container) {
    console.error("No running agent-wechat container found.");
    process.exit(1);
  }

  // Detect container architecture
  const uname = execSync(
    `docker exec "${container}" uname -m`,
    { encoding: "utf-8" }
  ).trim();
  const arch = uname === "x86_64" ? "amd64" : "arm64";

  const assetName = `agent-server-${version}-linux-${arch}`;
  const tmpFile = path.join(os.tmpdir(), assetName);

  // Download binary from GitHub Releases (no gh CLI dependency)
  const releaseUrl = `https://github.com/thisnick/agent-wechat/releases/download/v${version}/${assetName}`;
  console.log(`Downloading ${assetName}...`);
  try {
    const resp = await fetch(releaseUrl, { redirect: "follow" });
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    }
    const buffer = Buffer.from(await resp.arrayBuffer());
    fs.writeFileSync(tmpFile, buffer, { mode: 0o755 });
  } catch (err) {
    console.error(
      `Failed to download ${assetName} from GitHub Releases.\n` +
      `Make sure v${version} has been released with binary assets.\n` +
      `URL: ${releaseUrl}\n` +
      `Error: ${err instanceof Error ? err.message : String(err)}`
    );
    process.exit(1);
  }

  // Deploy into container
  console.log(`Deploying to ${container}...`);
  execSync(`docker cp "${tmpFile}" "${container}:/opt/agent-server/agent-server"`, {
    stdio: "inherit",
  });
  execSync(`docker exec "${container}" chmod +x /opt/agent-server/agent-server`, {
    stdio: "inherit",
  });

  // Restart server process (entrypoint loop brings it back)
  execSync(
    `docker exec "${container}" pkill -f "/opt/agent-server/agent-server" 2>/dev/null || true`,
    { stdio: "inherit" }
  );

  // Clean up
  try {
    fs.unlinkSync(tmpFile);
  } catch {
    // ignore
  }

  console.log("Server restarting with new binary.");

  // Wait for health check
  console.log("Waiting for server...");
  const config = getConfig();
  for (let i = 0; i < 15; i++) {
    await new Promise((r) => setTimeout(r, 1000));
    try {
      const resp = await fetch(`${config.serverUrl}/health`);
      if (resp.ok) {
        console.log("Server is ready!");
        return;
      }
    } catch {
      // not ready yet
    }
  }
  console.log("Server did not become ready in time. Check logs with: wx logs");
}

// ============================================
// Container Commands Implementation
// ============================================

async function cmdUp(opts: { proxy?: string } = {}) {
  const containerConfig = readContainerConfig();
  if (containerConfig) {
    if (opts.proxy) throw new Error("--proxy is not supported with container.json; configure the proxy in the Compose environment instead.");
    execFileSync("docker", [...composeArgs(containerConfig), "up", "-d"], { stdio: "inherit" });
    await waitForServer(() => composeRunning(containerConfig));
    return;
  }
  let image = getImageTag();

  // Check if container already exists
  try {
    const existingId = execSync(`docker ps -aq -f "name=^${CONTAINER_NAME}$"`, { encoding: "utf-8" }).trim();
    if (existingId) {
      const running = execSync(`docker ps -q -f "name=^${CONTAINER_NAME}$"`, { encoding: "utf-8" }).trim();
      if (running) {
        console.log(`Container ${CONTAINER_NAME} is already running.`);
        console.log(`API: http://localhost:${DEFAULT_PORT}`);
        printNoVncUrl();
        return;
      }
      console.log(`Starting existing container ${CONTAINER_NAME}...`);
      execSync(`docker start ${CONTAINER_NAME}`, { stdio: "inherit" });
      console.log(`API: http://localhost:${DEFAULT_PORT}`);
      printNoVncUrl();
      return;
    }
  } catch {
    // No container found, continue to create
  }

  // Check if image exists locally, pull if not
  try {
    execSync(`docker image inspect ${image}`, { stdio: "ignore" });
  } catch {
    console.log(`Image ${image} not found locally. Pulling...`);
    try {
      execSync(`docker pull ${image}`, { stdio: "inherit" });
    } catch {
      // Versioned tag may not exist yet — fall back to latest
      const fallback = `${GHCR_IMAGE}:latest`;
      if (image !== fallback) {
        console.log(`Tag ${VERSION} not found, trying latest...`);
        try {
          execSync(`docker pull ${fallback}`, { stdio: "inherit" });
          image = fallback;
        } catch {
          console.error(`Failed to pull ${fallback}. Check your internet connection and Docker setup.`);
          process.exit(1);
        }
      } else {
        console.error(`Failed to pull ${image}. Check your internet connection and Docker setup.`);
        process.exit(1);
      }
    }
  }

  // Ensure auth token exists
  const token = ensureToken();

  console.log(`Starting container ${CONTAINER_NAME} from ${image}...`);

  const dockerArgs = [
    "run", "-d",
    "--name", CONTAINER_NAME,
    "--security-opt", "seccomp=unconfined",
    "--cap-add=SYS_PTRACE",
    "--cap-add=NET_ADMIN",
    "-p", `${DEFAULT_PORT}:${DEFAULT_PORT}`,
    "-v", `${CONTAINER_NAME}-data:/data`,
    "-v", `${CONTAINER_NAME}-wechat-home:/home/wechat`,
    "-v", `${TOKEN_PATH}:/data/auth-token:ro`,
  ];

  if (opts.proxy) {
    dockerArgs.push("-e", `PROXY=${opts.proxy}`);
  }

  dockerArgs.push(image);

  try {
    execFileSync("docker", dockerArgs, { stdio: "inherit" });
    console.log(`\nContainer started successfully!`);
    console.log(`API: http://localhost:${DEFAULT_PORT}`);
    printNoVncUrl();
    console.log(`\nWaiting for server to be ready...`);

    await waitForServer(() => {
      try { return Boolean(execFileSync("docker", ["ps", "-q", "-f", `name=^${CONTAINER_NAME}$`], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim()); }
      catch { return false; }
    });
  } catch (error) {
    console.error("Failed to start container:", error);
    process.exit(1);
  }
}

async function cmdDown() {
  const containerConfig = readContainerConfig();
  if (containerConfig) {
    execFileSync("docker", [...composeArgs(containerConfig), "stop"], { stdio: "inherit" });
    console.log("Compose services stopped; containers and volumes were retained.");
    return;
  }
  console.log(`Stopping container ${CONTAINER_NAME}...`);
  try {
    execSync(`docker stop ${CONTAINER_NAME}`, { stdio: "inherit" });
    execSync(`docker rm ${CONTAINER_NAME}`, { stdio: "inherit" });
    console.log("Container stopped and removed.");
  } catch {
    console.log("Container not found or already stopped.");
  }
}

function composeRunning(config: ContainerConfig): boolean {
  return Boolean(execFileSync("docker", [...composeArgs(config), "ps", "--status", "running", "--quiet", "agent-wechat"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim());
}

async function waitForServer(isRunning: () => boolean): Promise<void> {
  console.log("\nWaiting for server to be ready...");
  const serverUrl = getConfig().serverUrl;
  for (let i = 0; i < 30; i++) {
    if (!isRunning()) throw new Error("Container process exited before the server became ready. Check logs with: wx logs");
    try {
      const response = await fetch(`${serverUrl}/health`, { signal: AbortSignal.timeout(2_000) });
      if (response.ok) { console.log("Server is ready!"); return; }
    } catch { /* retry until the bounded deadline */ }
    if (i < 29) await new Promise((resolve) => setTimeout(resolve, 1000));
    process.stdout.write(".");
  }
  throw new Error("Server did not become ready in time. Check logs with: wx logs");
}

async function cmdLogs() {
  const containerConfig = readContainerConfig();
  const args = containerConfig
    ? [...composeArgs(containerConfig), "logs", "-f", "agent-wechat"]
    : ["logs", "-f", CONTAINER_NAME];
  try {
    const logs = spawn("docker", args, {
      stdio: "inherit",
    });
    logs.on("error", () => {
      console.error(`Container ${CONTAINER_NAME} not found.`);
      process.exit(1);
    });
  } catch {
    console.error(`Container ${CONTAINER_NAME} not found.`);
    process.exit(1);
  }
}

// Parse and run
program.parseAsync(process.argv).catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
