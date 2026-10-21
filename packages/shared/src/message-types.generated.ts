// Generated from packages/agent-server-rust/src/message-type-dictionary.json; do not edit manually.
export const messageTypeDictionary = {
  "main": [
    {
      "code": 1,
      "key": "text",
      "label": "文本",
      "basis": "default_adopted"
    },
    {
      "code": 3,
      "key": "image",
      "label": "图片",
      "basis": "default_adopted"
    },
    {
      "code": 34,
      "key": "voice",
      "label": "语音",
      "basis": "default_adopted"
    },
    {
      "code": 37,
      "key": "friend_request",
      "label": "好友申请详情",
      "basis": "default_adopted"
    },
    {
      "code": 42,
      "key": "contact_card",
      "label": "名片",
      "basis": "default_adopted"
    },
    {
      "code": 43,
      "key": "video",
      "label": "视频",
      "basis": "default_adopted"
    },
    {
      "code": 47,
      "key": "emoji",
      "label": "表情包",
      "basis": "default_adopted"
    },
    {
      "code": 48,
      "key": "location",
      "label": "位置",
      "basis": "default_adopted"
    },
    {
      "code": 49,
      "key": "app_message",
      "label": "应用消息",
      "basis": "default_adopted"
    },
    {
      "code": 50,
      "key": "call",
      "label": "通话",
      "basis": "default_adopted"
    },
    {
      "code": 51,
      "key": "status_sync",
      "label": "状态同步",
      "basis": "default_adopted"
    },
    {
      "code": 64,
      "key": "group_call_invite",
      "label": "群通话邀请",
      "basis": "default_adopted"
    },
    {
      "code": 10000,
      "key": "system_notice",
      "label": "系统提示",
      "basis": "default_adopted"
    },
    {
      "code": 10001,
      "key": "interaction_security_notice",
      "label": "交互/安全提示",
      "basis": "default_adopted"
    },
    {
      "code": 10002,
      "key": "recall_notice",
      "label": "撤回通知",
      "basis": "default_adopted"
    }
  ],
  "appmsg": [
    {
      "code": 3,
      "key": "music",
      "label": "音乐",
      "basis": "default_adopted"
    },
    {
      "code": 4,
      "key": "video_link",
      "label": "视频链接",
      "basis": "default_adopted"
    },
    {
      "code": 5,
      "key": "link",
      "label": "链接",
      "basis": "default_adopted"
    },
    {
      "code": 6,
      "key": "file",
      "label": "文件",
      "basis": "default_adopted"
    },
    {
      "code": 19,
      "key": "forwarded_record",
      "label": "合并转发",
      "basis": "default_adopted"
    },
    {
      "code": 33,
      "key": "mini_program",
      "label": "小程序",
      "basis": "default_adopted"
    },
    {
      "code": 36,
      "key": "mini_program",
      "label": "小程序",
      "basis": "default_adopted"
    },
    {
      "code": 57,
      "key": "quoted_message",
      "label": "引用",
      "basis": "local_observed"
    },
    {
      "code": 62,
      "key": "nudge",
      "label": "拍一拍",
      "basis": "default_adopted"
    }
  ],
  "forwarded_inner": [
    {
      "code": 1,
      "key": "text",
      "label": "文本",
      "basis": "local_observed"
    }
  ]
} as const;
