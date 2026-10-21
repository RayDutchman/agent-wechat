// Export types from types module
export * from "./types/index.js";
export { messageTypeDictionary } from "./message-types.generated.js";

// Export HTTP client
export {
  WeChatClient,
  SendMessageV2Error,
  type WeChatClientOptions,
  type ReadV2Options,
  type ListMessagesV2Options,
  type MessageTypeFilter,
  type MessageLocatorOptions,
  type AccountObservation,
  type SendMessageV2Request,
  type SendMessageV2Params,
  type SendMessageV2Result,
  type VoiceJob,
} from "./client.js";

// Export schemas (but not the inferred types which duplicate types/)
export {
  // Session schemas
  sessionStatusSchema,
  sessionSchema,
  createSessionParamsSchema,
  sessionIdParamsSchema,
  sessionNameParamsSchema,
  dbSessionRowSchema,
  // Container lifecycle schemas
  upParamsSchema,
  upResultSchema,
  statusSchema,
  // Authentication schemas
  loginStateSchema,
  loginResultSchema,
  accountObservationSchema,
  sendMessageV2RequestSchema,
  sendMessageV2ErrorSchema,
  sendMessageV2ResultSchema,
  loginSubscriptionEventSchema,
  // Chat schemas
  chatSchema,
  contactKindV2Schema,
  contactV2Schema,
  groupMemberV2Schema,
  chatV2Schema,
  readPageV2Schema,
  listChatsParamsSchema,
  findChatParamsSchema,
  getChatParamsSchema,
  openChatParamsSchema,
  openChatResultSchema,
  // Message schemas
  messageSchema,
  messageV2Schema,
  referenceV2Schema,
  xmlParseStatusSchema,
  forwardedRecordItemSchema,
  forwardedRecordSchema,
  messageTypeDictionarySchema,
  mediaResultSchema,
  // Agent config schema
  agentConfigSchema,
} from "./schemas/index.js";
export type { MessageTypeDictionary } from "./schemas/index.js";
