import type {
  MediaResult,
  LoginResult,
  LoginSubscriptionEvent,
  DataChangedEvent,
  ChangeSubscriptionStatus,
  OpenChatResult,
  Session,
  ContactKind,
  ContactV2,
  GroupMemberV2,
  ChatV2,
  ReadPageV2,
  MessageV2,
} from "./types/index.js";
import { accountObservationSchema, sendMessageV2RequestSchema, sendMessageV2ResultSchema } from "./schemas/index.js";
import type { AccountObservation as AccountObservationSchema, SendMessageV2Request as SendMessageV2RequestSchema, SendMessageV2Result } from "./schemas/index.js";

export type AccountObservation = AccountObservationSchema;
export type SendMessageV2Request = SendMessageV2RequestSchema;
export type SendMessageV2Params = SendMessageV2Request;
export type { SendMessageV2Result };

export class SendMessageV2Error extends Error {
  constructor(readonly status: number, readonly result: SendMessageV2Result) {
    super(`${status} send preflight rejected: ${result.error_code ?? "unknown"}`);
    this.name = "SendMessageV2Error";
  }
}

type SendMessageV2ErrorCode = Exclude<SendMessageV2Result["error_code"], null>;
const sendPreflightCodesByStatus: Record<number, readonly SendMessageV2ErrorCode[]> = {
  400: ["invalid_parameter", "invalid_filename", "invalid_encoding", "payload_mismatch"],
  409: ["account_mismatch", "scope_changed"],
  413: ["payload_too_large"],
  500: ["temp_storage_failed"],
  503: ["identity_unavailable", "not_logged_in", "login_unverified", "target_unavailable", "deadline_exceeded"],
};

function isKnownPreflightHttpResult(status: number, result: SendMessageV2Result): boolean {
  return result.success === false
    && result.commit_attempted === false
    && result.ui_confirmed === false
    && result.failure_stage === "preflight"
    && result.error_code !== null
    && (sendPreflightCodesByStatus[status]?.includes(result.error_code) ?? false);
}

export type VoiceJob = {
  jobId: string;
  chatId: string;
  status: "queued" | "preparing" | "prepared" | "sending" | "completed" | "cancelled" | "failed" | "needs_review";
  sourceDurationSeconds: number | null;
  chunks: Array<{
    index: number;
    status: string;
    sourceDurationSeconds: number;
    playbackDurationSeconds: number;
    messageId: number | null;
  }>;
  error: string | null;
};

export interface WeChatClientOptions {
  baseUrl: string;
  token?: string;
  sessionId?: string;
  headers?: Record<string, string>;
}

export interface SubscribeChangesOptions {
  signal?: AbortSignal;
  onStatus?: (status: ChangeSubscriptionStatus) => void;
}

export interface ReadV2Options {
  q?: string;
  kind?: ContactKind;
  limit?: number;
  cursor?: string;
  expected_account_username?: string;
}

export type MessageTypeFilter = { type: number; sub_type?: number };
export interface ListMessagesV2Options {
  limit?: number;
  cursor?: string;
  order?: "asc" | "desc";
  include?: MessageTypeFilter[];
  exclude?: MessageTypeFilter[];
  expected_account_username?: string;
}
export interface MessageLocatorOptions {
  chat_username?: string;
  expected_account_username?: string;
}

type ChangeConnection = {
  next: () => Promise<DataChangedEvent | null>;
  close: () => void;
};

const MAX_CHANGE_FRAME_CHARS = 256;

function isDataChangedEvent(data: unknown): data is DataChangedEvent {
  if (typeof data !== "string" || data.length > MAX_CHANGE_FRAME_CHARS) return false;
  try {
    const parsed: unknown = JSON.parse(data);
    return (
      typeof parsed === "object" &&
      parsed !== null &&
      !Array.isArray(parsed) &&
      Object.keys(parsed).length === 1 &&
      (parsed as { type?: unknown }).type === "data_changed"
    );
  } catch {
    return false;
  }
}

function openChangeConnection(url: string, signal?: AbortSignal): Promise<ChangeConnection | null> {
  if (signal?.aborted) return Promise.resolve(null);

  let socket: WebSocket;
  try {
    socket = new WebSocket(url);
  } catch {
    return Promise.resolve(null);
  }

  return new Promise((resolve) => {
    let opened = false;
    let settled = false;
    let closed = false;
    let eventPending = false;
    let wakeReader: (() => void) | undefined;

    const wake = () => {
      const wakeReaderNow = wakeReader;
      wakeReader = undefined;
      wakeReaderNow?.();
    };
    const removeListeners = () => {
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("close", onClose);
      socket.removeEventListener("error", onError);
      signal?.removeEventListener("abort", onAbort);
    };
    const closeSocket = () => {
      try {
        socket.close();
      } catch {
        // A closing/failed native WebSocket may reject close(); treat it as closed.
      }
    };
    const finish = () => {
      if (closed) return;
      closed = true;
      if (!opened && !settled) {
        settled = true;
        removeListeners();
        resolve(null);
      }
      wake();
    };
    const onOpen = () => {
      if (closed || settled) return;
      opened = true;
      settled = true;
      resolve({
        next: async () => {
          while (!eventPending && !closed && !signal?.aborted) {
            await new Promise<void>((wakeNext) => { wakeReader = wakeNext; });
          }
          if (eventPending) {
            eventPending = false;
            return { type: "data_changed" };
          }
          return null;
        },
        close: () => {
          finish();
          removeListeners();
          closeSocket();
        },
      });
    };
    const onMessage = (event: MessageEvent) => {
      if (!eventPending && isDataChangedEvent(event.data)) {
        eventPending = true;
        wake();
      }
    };
    const onClose = () => {
      finish();
      if (opened) removeListeners();
    };
    const onError = () => {
      finish();
      closeSocket();
      if (opened) removeListeners();
    };
    const onAbort = () => {
      finish();
      closeSocket();
      removeListeners();
    };

    socket.addEventListener("open", onOpen);
    socket.addEventListener("message", onMessage);
    socket.addEventListener("close", onClose);
    socket.addEventListener("error", onError);
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) onAbort();
  });
}

function waitForReconnect(delayMs: number, signal?: AbortSignal): Promise<boolean> {
  if (signal?.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    const finish = (elapsed: boolean) => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      resolve(elapsed);
    };
    const onAbort = () => finish(false);
    const timer = setTimeout(() => finish(true), delayMs);
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) onAbort();
  });
}

function normalizeUrl(base: string): string {
  const url = base.startsWith("http") ? base : `http://${base}`;
  return url.replace(/\/$/, "");
}

function qs(params: Record<string, unknown>): string {
  const entries = Object.entries(params).filter(([, v]) => v != null);
  if (entries.length === 0) return "";
  return (
    "?" +
    entries
      .map(
        ([k, v]) =>
          `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`,
      )
      .join("&")
  );
}

export class WeChatClient {
  private base: string;
  private headers: Record<string, string>;

  constructor(options: WeChatClientOptions) {
    this.base = normalizeUrl(options.baseUrl);
    this.headers = { "Content-Type": "application/json" };
    if (options.token) this.headers.Authorization = `Bearer ${options.token}`;
    if (options.sessionId)
      this.headers["X-Session-Id"] = options.sessionId;
    if (options.headers) Object.assign(this.headers, options.headers);
  }

  /** Get the base URL (for WebSocket URL derivation, etc.) */
  get baseUrl(): string {
    return this.base;
  }

  // ---- internal helpers ----

  private async get<T>(path: string): Promise<T> {
    const res = await fetch(`${this.base}${path}`, {
      headers: this.headers,
    });
    if (!res.ok)
      throw new Error(
        `${res.status} ${res.statusText}: ${await res.text()}`,
      );
    return res.json() as Promise<T>;
  }

  private async getBytes(path: string): Promise<Uint8Array> {
    const res = await fetch(`${this.base}${path}`, { headers: this.headers });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${await res.text()}`);
    return new Uint8Array(await res.arrayBuffer());
  }

  private async post<T>(path: string, body?: unknown): Promise<T> {
    const res = await fetch(`${this.base}${path}`, {
      method: "POST",
      headers: this.headers,
      body: body != null ? JSON.stringify(body) : undefined,
    });
    if (!res.ok)
      throw new Error(
        `${res.status} ${res.statusText}: ${await res.text()}`,
      );
    return res.json() as Promise<T>;
  }

  private async del<T>(path: string): Promise<T> {
    const res = await fetch(`${this.base}${path}`, {
      method: "DELETE",
      headers: this.headers,
    });
    if (!res.ok)
      throw new Error(
        `${res.status} ${res.statusText}: ${await res.text()}`,
      );
    return res.json() as Promise<T>;
  }

  // ---- Status ----

  async getAccount(): Promise<AccountObservation> {
    return accountObservationSchema.parse(await this.get("/api/v2/account"));
  }

  async login(): Promise<LoginResult> {
    return this.post("/api/status/login");
  }

  async logout(): Promise<{ success: boolean; error?: string }> {
    return this.post("/api/status/logout");
  }

  // ---- Chats ----

  async openChat(
    chatId: string,
    clearUnreads?: boolean,
  ): Promise<OpenChatResult> {
    return this.post(
      `/api/chats/${encodeURIComponent(chatId)}/open${qs({ clearUnreads })}`,
    );
  }

  // ---- Contacts ----

  async listContactsV2(options: ReadV2Options = {}): Promise<ReadPageV2<ContactV2>> {
    return this.get(`/api/v2/contacts${qs({ ...options })}`);
  }

  async listChatsV2(options: Omit<ReadV2Options, "kind"> = {}): Promise<ReadPageV2<ChatV2>> {
    return this.get(`/api/v2/chats${qs(options)}`);
  }

  async getChatV2(username: string): Promise<ChatV2> {
    return this.get(`/api/v2/chats/${encodeURIComponent(username)}`);
  }

  async listGroupMembersV2(roomUsername: string, options: Omit<ReadV2Options, "q" | "kind"> = {}): Promise<ReadPageV2<GroupMemberV2>> {
    return this.get(`/api/v2/chats/${encodeURIComponent(roomUsername)}/members${qs(options)}`);
  }

  async listMessagesV2(chatUsername: string, options: ListMessagesV2Options = {}): Promise<ReadPageV2<MessageV2>> {
    const params: Record<string, unknown> = { ...options };
    if (options.include !== undefined) params.include = JSON.stringify(options.include);
    if (options.exclude !== undefined) params.exclude = JSON.stringify(options.exclude);
    return this.get(`/api/v2/chats/${encodeURIComponent(chatUsername)}/messages${qs(params)}`);
  }

  async getMessageV2(serverId: string, options: MessageLocatorOptions = {}): Promise<MessageV2> {
    return this.get(`/api/v2/messages/${encodeURIComponent(serverId)}${qs({ ...options })}`);
  }

  async getMessageMediaV2(serverId: string, options: MessageLocatorOptions & { quality?: "legacy" | "full" | "standard" | "thumbnail" | "best" } = {}): Promise<MediaResult> {
    return this.get(`/api/v2/messages/${encodeURIComponent(serverId)}/media${qs({ ...options })}`);
  }

  async getMessageVoiceV2(serverId: string, options: MessageLocatorOptions = {}): Promise<Uint8Array> {
    return this.getBytes(`/api/v2/messages/${encodeURIComponent(serverId)}/voice${qs({ ...options })}`);
  }

  async releaseReadSnapshotV2(token: string): Promise<{ released: boolean }> {
    return this.del(`/api/v2/read-snapshots/${encodeURIComponent(token)}`);
  }

  // ---- Messages ----

  async sendMessageV2(params: SendMessageV2Params): Promise<SendMessageV2Result> {
    const request = sendMessageV2RequestSchema.parse(params);
    const response = await fetch(`${this.base}/api/v2/messages/send`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      const body = await response.text();
      let decoded: unknown;
      try {
        decoded = JSON.parse(body);
      } catch {
        throw new Error(`${response.status} ${response.statusText}: ${body}`);
      }
      const parsed = sendMessageV2ResultSchema.safeParse(decoded);
      if (parsed.success && isKnownPreflightHttpResult(response.status, parsed.data)) {
        throw new SendMessageV2Error(response.status, parsed.data);
      }
      throw new Error(`${response.status} ${response.statusText}: ${body}`);
    }
    return sendMessageV2ResultSchema.parse(await response.json());
  }

  async createVoiceJob(chatId: string, audio: Uint8Array, idempotencyKey: string): Promise<VoiceJob> {
    const form = new FormData();
    form.set("chatId", chatId);
    form.set("audio", new Blob([new Uint8Array(audio)]), "voice-audio");
    const headers: Record<string, string> = { ...this.headers, "Idempotency-Key": idempotencyKey };
    delete headers["Content-Type"];
    const res = await fetch(`${this.base}/api/messages/voice`, { method: "POST", headers, body: form });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${await res.text()}`);
    return res.json() as Promise<VoiceJob>;
  }

  async getVoiceJob(jobId: string): Promise<VoiceJob> {
    return this.get(`/api/messages/voice/${encodeURIComponent(jobId)}`);
  }

  async cancelVoiceJob(jobId: string): Promise<VoiceJob> {
    return this.post(`/api/messages/voice/${encodeURIComponent(jobId)}/cancel`);
  }

  /**
   * Stream broad data-change hints. Reconnection never triggers a read; callers
   * decide whether and when to reconcile their own data.
   */
  async *subscribeChanges(
    options: SubscribeChangesOptions = {},
  ): AsyncGenerator<DataChangedEvent, void, void> {
    const { signal, onStatus } = options;
    const reportStatus = (status: ChangeSubscriptionStatus) => onStatus?.(status);
    if (signal?.aborted) {
      reportStatus("closed");
      return;
    }

    const url = new URL(`${this.base}/api/ws/events`);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    if (this.wsToken) url.searchParams.set("token", this.wsToken);

    let reconnectDelayMs = 250;
    let attempted = false;
    let statusReported = false;
    let activeConnection: ChangeConnection | null = null;
    try {
      while (!signal?.aborted) {
        if (!statusReported) reportStatus(attempted ? "reconnecting" : "connecting");
        statusReported = false;
        attempted = true;

        activeConnection = await openChangeConnection(url.toString(), signal);
        if (signal?.aborted) break;
        if (!activeConnection) {
          reportStatus("reconnecting");
          statusReported = true;
          if (!(await waitForReconnect(reconnectDelayMs, signal))) break;
          reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30_000);
          continue;
        }

        reportStatus("connected");
        reconnectDelayMs = 250;
        while (!signal?.aborted) {
          const event = await activeConnection.next();
          if (!event) break;
          yield event;
        }
        activeConnection.close();
        activeConnection = null;
        if (signal?.aborted) break;

        reportStatus("reconnecting");
        statusReported = true;
        if (!(await waitForReconnect(reconnectDelayMs, signal))) break;
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30_000);
      }
    } finally {
      activeConnection?.close();
      reportStatus("closed");
    }
  }

  // ---- Debug ----

  async screenshot(): Promise<{ base64: string }> {
    return this.get("/api/debug/screenshot");
  }

  async a11y(
    format: "json" | "aria",
  ): Promise<{ tree: unknown; aria: string | null; error?: string }> {
    return this.get(`/api/debug/a11y${qs({ format })}`);
  }

  // ---- Sessions ----

  async createSession(name: string): Promise<Session> {
    return this.post("/api/sessions", { name });
  }

  async listSessions(): Promise<Session[]> {
    return this.get("/api/sessions");
  }

  async getSession(id: string): Promise<Session | null> {
    return this.get(`/api/sessions/${encodeURIComponent(id)}`);
  }

  async startSession(id: string): Promise<Session> {
    return this.post(
      `/api/sessions/${encodeURIComponent(id)}/start`,
    );
  }

  async stopSession(id: string): Promise<Session> {
    return this.post(
      `/api/sessions/${encodeURIComponent(id)}/stop`,
    );
  }

  async deleteSession(
    id: string,
  ): Promise<{ success: boolean }> {
    return this.del(`/api/sessions/${encodeURIComponent(id)}`);
  }

  // ---- Login subscription (WebSocket) ----

  /**
   * Subscribe to login events via WebSocket.
   * Uses the native WebSocket API (Node 22+).
   * Returns a handle with close() to tear down the connection.
   */
  /** Extract the bearer token (if any) for use in WebSocket query params. */
  private get wsToken(): string | undefined {
    const auth = this.headers.Authorization;
    return auth?.replace(/^Bearer\s+/i, "");
  }

  loginSubscribe(opts: {
    timeoutMs?: number;
    newAccount?: boolean;
    onEvent: (event: LoginSubscriptionEvent) => void;
    onError?: (err: Error) => void;
    onClose?: () => void;
  }): { close: () => void } {
    const wsUrl = this.base.replace(/^http/, "ws");
    const params = qs({
      timeoutMs: opts.timeoutMs,
      newAccount: opts.newAccount,
      token: this.wsToken,
    });
    const ws = new WebSocket(`${wsUrl}/api/ws/login${params}`);

    ws.addEventListener("message", (event) => {
      try {
        const data =
          typeof event.data === "string"
            ? event.data
            : String(event.data);
        const parsed = JSON.parse(data) as LoginSubscriptionEvent;
        opts.onEvent(parsed);
      } catch (e) {
        opts.onError?.(e instanceof Error ? e : new Error(String(e)));
      }
    });

    ws.addEventListener("error", (event) => {
      const msg =
        "message" in event && typeof (event as any).message === "string"
          ? (event as any).message
          : "WebSocket error";
      opts.onError?.(new Error(msg));
    });

    ws.addEventListener("close", () => {
      opts.onClose?.();
    });

    return {
      close: () => {
        ws.close();
      },
    };
  }
}
