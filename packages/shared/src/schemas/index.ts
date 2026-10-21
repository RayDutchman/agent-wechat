import { z } from "zod";

// ============================================
// SESSIONS
// ============================================

export const sessionStatusSchema = z.enum([
  "stopped",
  "starting",
  "running",
  "stopping",
  "error",
]);

export const sessionSchema = z.object({
  id: z.string(),
  name: z.string(),
  linuxUser: z.string(),
  display: z.string(),
  dbusAddress: z.string().optional(),
  vncPort: z.number().int(),
  status: sessionStatusSchema,
  loginState: z.lazy(() => loginStateSchema),
  loggedInUser: z.string().optional(),
  wechatPid: z.number().int().optional(),
  xvfbPid: z.number().int().optional(),
  dbusPid: z.number().int().optional(),
  errorMessage: z.string().optional(),
  createdAt: z.string(),
  updatedAt: z.string(),
});

export const createSessionParamsSchema = z.object({
  name: z.string().min(1).max(50).regex(/^[a-zA-Z0-9_-]+$/, "Name must be alphanumeric with _ or -"),
});

export const sessionIdParamsSchema = z.object({
  id: z.string().min(1),
});

export const sessionNameParamsSchema = z.object({
  name: z.string().min(1),
});

export const dbSessionRowSchema = z.object({
  id: z.string(),
  name: z.string(),
  linux_user: z.string(),
  display: z.string(),
  dbus_address: z.string().nullable(),
  vnc_port: z.number().int(),
  status: z.string(),
  login_state: z.string(),
  wechat_pid: z.number().int().nullable(),
  xvfb_pid: z.number().int().nullable(),
  dbus_pid: z.number().int().nullable(),
  error_message: z.string().nullable(),
  created_at: z.string(),
  updated_at: z.string(),
});

// ============================================
// CONTAINER LIFECYCLE
// ============================================

export const upParamsSchema = z.object({
  image: z.string().optional(),
});

export const upResultSchema = z.object({
  url: z.string(),
});

export const statusSchema = z.object({
  container: z.enum(["running", "stopped", "unknown"]),
  loginState: z.discriminatedUnion("status", [
    z.object({ status: z.literal("logged_out") }),
    z.object({
      status: z.literal("qr_pending"),
      qrDataUrl: z.string().optional(),
    }),
    z.object({
      status: z.literal("logged_in"),
      userId: z.string().optional(),
    }),
  ]),
  version: z.string(),
});

// ============================================
// AUTHENTICATION
// ============================================

export const loginStateSchema = z.discriminatedUnion("status", [
  z.object({ status: z.literal("logged_out") }),
  z.object({
    status: z.literal("qr_pending"),
    qrDataUrl: z.string().optional(),
  }),
  z.object({
    status: z.literal("logged_in"),
    userId: z.string().optional(),
  }),
]);

export const loginResultSchema = z.object({
  success: z.boolean(),
  state: loginStateSchema,
});

export const accountObservationSchema = z.object({
  status: z.enum(["logged_in", "logged_out", "app_not_running", "unknown"]),
  account_username: z.string().nullable(),
  error_code: z.enum(["session_unavailable", "observation_failed", "observation_timeout", "identity_unavailable", "scope_changed"]).nullable(),
}).strict();

const base64PayloadSchema = z.string().regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/);
const safeUploadFilenameSchema = z.string().min(1).refine(
  (filename) => filename !== "." && filename !== ".." && !/[\/\\\p{Cc}]/u.test(filename),
);

export const sendMessageV2RequestSchema = z.object({
  chat_username: z.string().trim().min(1),
  expected_account_username: z.string().trim().min(1).optional(),
  text: z.string().optional(),
  image: z.object({ data: z.string(), mime_type: z.enum(["image/png", "image/jpeg", "image/gif", "image/webp"]) }).strict().optional(),
  file: z.object({
    filename: safeUploadFilenameSchema,
    data: z.string(),
    sha256: z.string().regex(/^[0-9a-f]{64}$/),
    size: z.number().int().nonnegative(),
  }).strict().optional(),
}).strict()
  .refine((request) => request.text !== undefined || request.image !== undefined || request.file !== undefined)
  .superRefine((request, context) => {
    const selected = request.file ? request.file.data : request.image ? request.image.data : undefined;
    if (selected !== undefined && !base64PayloadSchema.safeParse(selected).success) {
      context.addIssue({ code: z.ZodIssueCode.custom, path: [request.file ? "file" : "image", "data"] });
    }
    if (request.image && !request.file && request.image.data.length === 0) {
      context.addIssue({ code: z.ZodIssueCode.custom, path: ["image", "data"] });
    }
  });
export const sendMessageV2ErrorSchema = z.enum([
  "invalid_parameter", "invalid_filename", "invalid_encoding", "payload_mismatch", "payload_too_large",
  "account_mismatch", "scope_changed", "temp_storage_failed", "identity_unavailable", "not_logged_in",
  "login_unverified", "target_unavailable", "deadline_exceeded", "composer_not_empty", "composer_not_ready",
  "paste_failed", "chat_not_open", "chat_selection_failed", "target_changed", "aborted", "timeout",
  "execution_failed", "commit_unconfirmed",
]);
export const sendMessageV2ResultSchema = z.object({
  success: z.boolean(), account_username: z.string().nullable(), chat_username: z.string().nullable(),
  selected_payload: z.enum(["text", "image", "file"]).nullable(),
  ignored_fields: z.array(z.enum(["text", "image", "file"])), commit_attempted: z.boolean(), ui_confirmed: z.boolean(),
  error_code: sendMessageV2ErrorSchema.nullable(),
  failure_stage: z.enum(["preflight", "prepare", "pre_commit", "post_commit"]).nullable(),
  payload_sha256: z.string().regex(/^[0-9a-f]{64}$/).nullable().optional(),
  payload_size: z.number().int().nonnegative().nullable().optional(),
}).strict()
  .refine((result) => result.success === result.ui_confirmed && (!result.success || (result.commit_attempted && result.error_code === null && result.failure_stage === null)))
  .refine((result) => result.success || (result.error_code !== null && ((result.commit_attempted && !result.ui_confirmed) || (!result.commit_attempted && !result.ui_confirmed))))
  .refine((result) => result.failure_stage === null || (result.failure_stage === "post_commit" ? result.commit_attempted : !result.commit_attempted))
  .superRefine((result, context) => {
    const hasSha = Object.prototype.hasOwnProperty.call(result, "payload_sha256");
    const hasSize = Object.prototype.hasOwnProperty.call(result, "payload_size");
    if (result.selected_payload === "file") {
      if (!hasSha || !hasSize) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "file results require both payload metadata fields" });
      } else if ((result.payload_sha256 === null) !== (result.payload_size === null)) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "file payload metadata must be both null or both verified" });
      }
    } else if (hasSha || hasSize) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "non-file results must omit file payload metadata" });
    }
  });

// Login subscription events (for real-time QR monitoring)
export const loginSubscriptionEventSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("status"), message: z.string() }),
  z.object({ type: z.literal("qr"), qrData: z.string() }),
  z.object({ type: z.literal("login_success"), userId: z.string().optional() }),
  z.object({ type: z.literal("login_timeout") }),
  z.object({ type: z.literal("error"), message: z.string() }),
]);

// ============================================
// CHATS
// ============================================

export const chatSchema = z.object({
  id: z.string(),
  username: z.string(),
  name: z.string(),
  remark: z.string().optional(),
  lastMessagePreview: z.string().optional(),
  lastMessageSender: z.string().optional(),
  lastActivityAt: z.string().optional(),
  unreadCount: z.number().int().nonnegative(),
  isGroup: z.boolean(),
});

export const contactKindV2Schema = z.enum(["individual", "group", "enterprise", "official", "system", "unknown"]);
export const contactV2Schema = z.object({
  username: z.string(), alias: z.string(), nick_name: z.string(), remark: z.string(),
  display_name: z.string(), kind: contactKindV2Schema, local_type: z.number(),
  official_type: z.number().optional(), verify_flag: z.number().optional(),
});
export const groupMemberV2Schema = z.object({
  room_username: z.string(), username: z.string(), group_nickname: z.string(), display_name: z.string(),
});
export const chatV2Schema = z.object({
  username: z.string(), display_name: z.string(), remark: z.string(), nick_name: z.string(),
  unread_count: z.number().int().nullable(), is_group: z.boolean(),
  last_message_preview: z.string().nullable(),
  last_msg_sender: z.string().nullable(),
  last_sender_display_name: z.string().nullable(),
  last_activity_at: z.string().nullable(), last_msg_local_id: z.number().int().nullable(),
});
export const readPageV2Schema = <T extends z.ZodTypeAny>(item: T) => z.object({
  items: z.array(item), next_cursor: z.string().nullable(), release_token: z.string().nullable(),
});

export const listChatsParamsSchema = z.object({
  limit: z.number().int().positive().max(100).optional().default(50),
  offset: z.number().int().nonnegative().optional().default(0),
});

export const findChatParamsSchema = z.object({
  name: z.string().min(1),
});

export const getChatParamsSchema = z.object({
  id: z.string().min(1),
});

export const openChatParamsSchema = z.object({
  chatId: z.string().min(1),
});

export const openChatResultSchema = z.object({
  ok: z.boolean(),
  username: z.string().optional(),
  index: z.number().int().optional(),
  error: z.string().optional(),
});

// ============================================
// MESSAGES
// ============================================

export const messageSchema = z.object({
  localId: z.number().int(),
  serverId: z.number(),
  chatId: z.string(),
  sender: z.string().optional(),
  senderName: z.string().optional(),
  type: z.number().int(),
  content: z.string(),
  timestamp: z.string(),
});

export const referenceV2Schema = z.object({
  server_id: z.string().nullable(), chat_username: z.string().nullable(),
  sender_username: z.string().nullable(), display_name: z.string().nullable(), content: z.string(),
});

export const xmlParseStatusSchema = z.enum(["complete", "truncated", "invalid"]);

export const forwardedRecordItemSchema = z.object({
  datatype: z.number().int().nonnegative().nullable(), dataid: z.string().nullable(),
  datadesc: z.string().nullable(), sourcename: z.string().nullable(), sourceheadurl: z.string().nullable(),
  sourcetime: z.string().nullable(), src_msg_create_time: z.string().nullable(),
  from_new_msg_id: z.string().nullable(), dataitem_source_hash_username: z.string().nullable(),
});

export const forwardedRecordSchema = z.object({
  parse_status: xmlParseStatusSchema, items: z.array(forwardedRecordItemSchema),
});

export const messageV2Schema = z.object({
  server_id: z.string().nullable(), local_id: z.string().nullable(), raw_type: z.string(),
  type: z.number().int().nonnegative(), sub_type: z.number().int().nonnegative(),
  xml_sub_type: z.number().int().nonnegative().nullable(),
  chat_username: z.string(), sender_username: z.string().nullable(),
  sender_display_name: z.string().nullable(), content: z.string(), timestamp: z.string().nullable(),
  is_self: z.boolean().nullable(), is_mentioned: z.boolean().nullable(), raw_content: z.string().nullable(),
  raw_content_compression: z.boolean(),
  reference: referenceV2Schema.nullable(), forwarded_record: forwardedRecordSchema.nullable(),
});

const messageTypeDictionaryEntrySchema = z.object({
  code: z.number().int().nonnegative().max(0xFFFF_FFFF),
  key: z.string().min(1),
  label: z.string().min(1),
  basis: z.enum(["default_adopted", "local_observed"]),
});
export const messageTypeDictionarySchema = z.object({
  main: z.array(messageTypeDictionaryEntrySchema),
  appmsg: z.array(messageTypeDictionaryEntrySchema),
  forwarded_inner: z.array(messageTypeDictionaryEntrySchema),
});

export const mediaResultSchema = z.object({
  type: z.enum(["image", "emoji", "voice", "video", "file", "pending", "unsupported"]),
  data: z.string().optional(),
  url: z.string().optional(),
  format: z.string(),
  filename: z.string(),
});

// ============================================
// AGENT CONFIGURATION
// ============================================

export const agentConfigSchema = z.object({
  maxTurns: z.number().int().positive().default(30),
  turnTimeout: z.number().int().positive().default(60_000),
  totalTimeout: z.number().int().positive().default(600_000),
});


// Type exports from schemas
export type UpParams = z.infer<typeof upParamsSchema>;
export type UpResult = z.infer<typeof upResultSchema>;
export type Status = z.infer<typeof statusSchema>;
export type LoginState = z.infer<typeof loginStateSchema>;
export type LoginResult = z.infer<typeof loginResultSchema>;
export type AccountObservation = z.infer<typeof accountObservationSchema>;
export type SendMessageV2Request = z.infer<typeof sendMessageV2RequestSchema>;
export type SendMessageV2Result = z.infer<typeof sendMessageV2ResultSchema>;
export type Chat = z.infer<typeof chatSchema>;
export type ContactKindV2 = z.infer<typeof contactKindV2Schema>;
export type ContactV2 = z.infer<typeof contactV2Schema>;
export type GroupMemberV2 = z.infer<typeof groupMemberV2Schema>;
export type ChatV2 = z.infer<typeof chatV2Schema>;
export type ReadPageV2<T> = { items: T[]; next_cursor: string | null; release_token: string | null };
export type ListChatsParams = z.infer<typeof listChatsParamsSchema>;
export type FindChatParams = z.infer<typeof findChatParamsSchema>;
export type GetChatParams = z.infer<typeof getChatParamsSchema>;
export type OpenChatParams = z.infer<typeof openChatParamsSchema>;
export type OpenChatResult = z.infer<typeof openChatResultSchema>;
export type Message = z.infer<typeof messageSchema>;
export type MessageV2 = z.infer<typeof messageV2Schema>;
export type MessageTypeDictionary = z.infer<typeof messageTypeDictionarySchema>;
export type MediaResult = z.infer<typeof mediaResultSchema>;
export type AgentConfig = z.infer<typeof agentConfigSchema>;
export type SessionStatus = z.infer<typeof sessionStatusSchema>;
export type Session = z.infer<typeof sessionSchema>;
export type CreateSessionParams = z.infer<typeof createSessionParamsSchema>;
export type SessionIdParams = z.infer<typeof sessionIdParamsSchema>;
export type SessionNameParams = z.infer<typeof sessionNameParamsSchema>;
export type DbSessionRow = z.infer<typeof dbSessionRowSchema>;
export type LoginSubscriptionEvent = z.infer<typeof loginSubscriptionEventSchema>;
