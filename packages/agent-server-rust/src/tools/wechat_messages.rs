use super::wechat_db::{get_db_path, numbered_db_index, try_query_wechat_db, value_ref_to_json, WechatReadError};
use super::wechat_snapshot;
use crate::ia::types::{Message, MessageV2, ReferenceV2, ReplyInfo, XmlParseStatus};
use super::message_record::{
    extract_reference_v2 as extract_reference_fields, project_message_record,
    unparsed_message_record,
};
use md5::{Digest, Md5};
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// ZSTD magic number (little-endian): 0xFD2FB528
const ZSTD_MAGIC: &str = "28b52ffd";

#[cfg(test)]
mod read_tests {
    use super::*;
    use crate::tools::wechat_db::WechatReadError;

    fn forwarded_record_for(body: &str) -> crate::ia::types::ForwardedRecord {
        let raw_hex = body.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let row = serde_json::json!({
            "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "existing title",
            "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false,
        });
        message_v2_from_snapshot(&row).unwrap().forwarded_record.unwrap()
    }

    #[test]
    fn message_contract_splits_types_and_preserves_large_ids_as_strings() {
        assert_eq!(split_message_type((6_i64 << 32) | 49), (49, 6));
        assert_eq!(split_message_type((57_i64 << 32) | 49), (49, 57));
        assert_eq!(split_message_type(48), (48, 0));
        let row = serde_json::json!({
            "serverId": 9168265261376983777_i64,
            "localId": 7_i64,
            "rawType": 25769803825_i64,
            "chatId": "chat-a", "sender": "wxid-a", "senderName": "Name",
            "content": "body", "timestamp": "2026-01-01T00:00:00Z",
            "isSelf": null, "isMentioned": null, "rawContent": "3c6d73672f3e", "rawContentCompression": false
        });
        let message = message_v2_from_snapshot(&row).unwrap();
        assert_eq!(message.server_id.as_deref(), Some("9168265261376983777"));
        assert_eq!(message.raw_type, "25769803825");
        assert_eq!((message.r#type, message.sub_type), (49, 6));
        assert_eq!(message.timestamp.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert_eq!(message.raw_content.as_deref(), Some("3c6d73672f3e"));
        assert_eq!(serde_json::to_value(&message).unwrap()["raw_content_compression"], false);
        let mut invalid_time = row.clone();
        invalid_time["timestamp"] = serde_json::json!("");
        assert_eq!(message_v2_from_snapshot(&invalid_time).unwrap().timestamp, None);
    }

    #[test]
    fn message_v2_rejects_missing_null_and_non_boolean_compression_flags() {
        let base = serde_json::json!({
            "serverId": 77_i64, "localId": 8_i64, "rawType": 49_i64,
            "chatId": "chat-a", "content": "body", "timestamp": "now",
            "rawContent": "3c6d73672f3e",
        });
        assert!(message_v2_from_snapshot(&base).is_none(), "missing flag");

        for value in [serde_json::Value::Null, serde_json::json!("false"), serde_json::json!(0)] {
            let mut row = base.clone();
            row["rawContentCompression"] = value;
            assert!(message_v2_from_snapshot(&row).is_none(), "{}", row["rawContentCompression"]);
        }
    }

    #[test]
    fn packed_and_xml_subtypes_remain_separately_recoverable_in_b1_raw_values() {
        let xml = "<msg><appmsg><type>19</type></appmsg></msg>";
        let raw_hex = xml.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let row = serde_json::json!({
            "serverId": 77_i64, "localId": 8_i64, "rawType": ((6_i64 << 32) | 49),
            "chatId": "chat-a", "sender": "wxid-a", "senderName": "Name",
            "content": "existing display", "timestamp": "2026-01-01T00:00:00Z",
            "isSelf": null, "isMentioned": null, "rawContent": raw_hex, "rawContentCompression": false,
        });
        let projected = message_v2_from_snapshot(&row).unwrap();
        assert_eq!((projected.r#type, projected.sub_type), (49, 6));
        assert_eq!(projected.raw_type, ((6_i64 << 32) | 49).to_string());
        assert_eq!(projected.raw_content.as_deref(), Some(raw_hex.as_str()));
        assert!(String::from_utf8(hex_decode(projected.raw_content.as_deref().unwrap()).unwrap()).unwrap().contains("<type>19</type>"));
    }

    #[test]
    fn reference_contract_keeps_quoted_identity_separate_from_outer_sender() {
        let body = "<msg><refermsg><svrid>1234567890123456789</svrid><fromusr>room@chatroom</fromusr><chatusr>quoted-author</chatusr><displayname>777</displayname><content>sample</content></refermsg></msg>";
        let reference = extract_reference_v2(body, "room@chatroom").unwrap();
        assert_eq!(reference.server_id.as_deref(), Some("1234567890123456789"));
        assert_eq!(reference.chat_username.as_deref(), Some("room@chatroom"));
        assert_eq!(reference.sender_username.as_deref(), Some("quoted-author"));
        assert_eq!(reference.display_name.as_deref(), Some("777"));
        assert_eq!(reference.content, "sample");

        let missing_author = extract_reference_v2(
            "<msg><refermsg><fromusr>room@chatroom</fromusr><displayname>123</displayname><content>sample</content></refermsg></msg>",
            "room@chatroom",
        ).unwrap();
        assert_eq!(missing_author.sender_username, None);
        assert_eq!(missing_author.display_name.as_deref(), Some("123"));

        let direct = extract_reference_v2(
            "<msg><refermsg><fromusr>must-not-be-guessed-as-author</fromusr><content>sample</content></refermsg></msg>",
            "alice",
        ).unwrap();
        assert_eq!(direct.chat_username.as_deref(), Some("alice"));
        assert_eq!(direct.sender_username, None);
    }

    #[test]
    fn reference_contract_rejects_malformed_xml_and_preserves_zero_server_id() {
        assert_eq!(extract_reference_v2("<msg><refermsg>", "alice"), None);
        let reference = extract_reference_v2(
            "<msg><refermsg><svrid>0</svrid><content><![CDATA[a &amp; b]]></content></refermsg></msg>",
            "alice",
        ).unwrap();
        assert_eq!(reference.server_id.as_deref(), Some("0"));
        assert_eq!(reference.content, "a &amp; b");
        let whitespace = extract_reference_v2(
            "<msg><refermsg><content><![CDATA[  quoted text  ]]></content></refermsg></msg>",
            "alice",
        ).unwrap();
        assert_eq!(whitespace.content, "  quoted text  ");
    }

    #[test]
    fn forwarded_record_contract_preserves_order_lexical_ids_and_source_hash_identity() {
        let xml = r#"<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist>
          <dataitem><datatype>1</datatype><dataid>9007199254740993</dataid><datadesc>first text</datadesc><sourcename>First author</sourcename><sourcetime>1700000000123456789</sourcetime><srcMsgCreateTime>1700000000.125</srcMsgCreateTime><fromnewmsgid>1234567890123456789</fromnewmsgid><dataitemsource><hashusername>hash-only-first</hashusername></dataitemsource></dataitem>
          <dataitem><datatype>987</datatype><dataid>second-id</dataid><datadesc>second text</datadesc><sourcename>Second author</sourcename><sourcetime>1700000000999</sourcetime><dataitemsource><hashusername>hash-only-second</hashusername></dataitemsource></dataitem>
        </datalist></recordinfo>]]></recorditem></appmsg></msg>"#;
        let raw_hex = xml.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let row = serde_json::json!({
            "serverId": 77_i64, "localId": 8_i64, "rawType": ((6_i64 << 32) | 49),
            "chatId": "room@chatroom", "sender": "outer-forwarder", "senderName": "Outer",
            "content": "existing display title", "timestamp": "2026-01-01T00:00:00Z",
            "isSelf": null, "isMentioned": null, "rawContent": raw_hex,
            "rawContentCompression": false,
        });
        let projected = message_v2_from_snapshot(&row).unwrap();
        assert_eq!(projected.content, "existing display title");
        assert_eq!(projected.sender_username.as_deref(), Some("outer-forwarder"));
        assert_eq!(projected.raw_type, (((6_i64 << 32) | 49)).to_string());
        assert_eq!(projected.sub_type, 6);
        assert_eq!(projected.xml_sub_type, Some(19));
        let forwarded = projected.forwarded_record.unwrap();
        assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Complete);
        assert_eq!(forwarded.items.len(), 2);
        assert_eq!(forwarded.items[0].datatype, Some(1));
        assert_eq!(forwarded.items[0].dataid.as_deref(), Some("9007199254740993"));
        assert_eq!(forwarded.items[0].datadesc.as_deref(), Some("first text"));
        assert_eq!(forwarded.items[0].sourcetime.as_deref(), Some("1700000000123456789"));
        assert_eq!(forwarded.items[0].src_msg_create_time.as_deref(), Some("1700000000.125"));
        assert_eq!(forwarded.items[0].from_new_msg_id.as_deref(), Some("1234567890123456789"));
        assert_eq!(forwarded.items[0].dataitem_source_hash_username.as_deref(), Some("hash-only-first"));
        assert_eq!(forwarded.items[1].datatype, Some(987));
        assert_eq!(forwarded.items[1].datadesc.as_deref(), Some("second text"));
        assert_eq!(forwarded.items[1].dataitem_source_hash_username.as_deref(), Some("hash-only-second"));
    }

    #[test]
    fn forwarded_record_contract_treats_cdata_and_escaped_recorditem_as_equivalent() {
        let inner = "<recordinfo><datalist><dataitem><datatype>1</datatype><datadesc>text &amp; more</datadesc></dataitem></datalist></recordinfo>";
        let escaped = inner.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
        let bodies = [
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[{inner}]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem>{escaped}</recorditem></appmsg></msg>"),
        ];
        let records: Vec<_> = bodies.iter().map(|body| {
            let raw_hex = body.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
            let row = serde_json::json!({ "rawType": 49_i64, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
            message_v2_from_snapshot(&row).unwrap().forwarded_record.unwrap()
        }).collect();
        assert_eq!(records[0], records[1]);
        assert_eq!(records[0].items[0].datadesc.as_deref(), Some("text & more"));
    }

    #[test]
    fn forwarded_record_contract_exposes_invalid_and_truncated_states() {
        let invalid_body = "<msg><appmsg><type>19</type><recorditem>not xml</recorditem></appmsg></msg>";
        let raw_hex = invalid_body.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let invalid_row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
        let invalid = message_v2_from_snapshot(&invalid_row).unwrap().forwarded_record.unwrap();
        assert_eq!(invalid.parse_status, crate::ia::types::XmlParseStatus::Invalid);
        assert!(invalid.items.is_empty());

        let broken_xml = "<msg><appmsg><type>19</type><recorditem><recordinfo></appmsg></msg>";
        let raw_hex = broken_xml.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let broken_row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
        let broken = message_v2_from_snapshot(&broken_row).unwrap().forwarded_record.unwrap();
        assert_eq!(broken.parse_status, crate::ia::types::XmlParseStatus::Invalid);
        assert!(broken.items.is_empty());

        let items = (0..101).map(|index| format!("<dataitem><datatype>777</datatype><dataid>{index}</dataid></dataitem>")).collect::<String>();
        let over_count = format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist>{items}</datalist></recordinfo>]]></recorditem></appmsg></msg>");
        let raw_hex = over_count.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
        let truncated = message_v2_from_snapshot(&row).unwrap().forwarded_record.unwrap();
        assert_eq!(truncated.parse_status, crate::ia::types::XmlParseStatus::Truncated);
        assert_eq!(truncated.items.len(), 100);

        let oversized_xml = format!("<msg><appmsg><type>19</type><recorditem>{}</recorditem></appmsg></msg>", "x".repeat(4 * 1024 * 1024));
        let compressed = zstd::encode_all(oversized_xml.as_bytes(), 1).unwrap();
        let raw_hex = compressed.iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let oversized_row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": true });
        let oversized = message_v2_from_snapshot(&oversized_row).unwrap().forwarded_record.unwrap();
        assert_eq!(oversized.parse_status, crate::ia::types::XmlParseStatus::Truncated);
        assert!(oversized.items.is_empty());

        let mut invalid_utf8 = b"<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><datatype>1</datatype><datadesc>".to_vec();
        invalid_utf8.push(0xff);
        invalid_utf8.extend_from_slice(b"</datadesc></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>");
        let raw_hex = invalid_utf8.iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let invalid_utf8_row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
        let invalid_utf8 = message_v2_from_snapshot(&invalid_utf8_row).unwrap().forwarded_record.unwrap();
        assert_eq!(invalid_utf8.parse_status, crate::ia::types::XmlParseStatus::Invalid);
        assert!(invalid_utf8.items.is_empty());
        assert_eq!(extract_reference_v2("<!DOCTYPE msg [<!ENTITY x SYSTEM 'file:///etc/passwd'>]><msg><refermsg><content>&x;</content></refermsg></msg>", "alice"), None);
    }

    #[test]
    fn forwarded_record_contract_rejects_illegal_xml_characters_and_text_terminator() {
        let bodies = [
            "<msg><appmsg><type>19</type><recorditem><recordinfo><datalist><dataitem><datatype>1</datatype><datadesc>bad\u{1}text</datadesc></dataitem></datalist></recordinfo></recorditem></appmsg></msg>".to_string(),
            "<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><datatype>1</datatype><datadesc>bad\u{1}text</datadesc></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>".to_string(),
            "<msg><appmsg><type>19</type><recorditem><recordinfo><datalist><dataitem mode='bad\u{1}'><datatype>1</datatype></dataitem></datalist></recordinfo></recorditem></appmsg></msg>".to_string(),
            "<msg><appmsg><type>19</type><recorditem><recordinfo><datalist><dataitem><datatype>1</datatype><datadesc>bad&#x1;text</datadesc></dataitem></datalist></recordinfo></recorditem></appmsg></msg>".to_string(),
            "<msg><appmsg><type>19</type><recorditem><recordinfo><datalist><dataitem><datatype>1</datatype><datadesc>bad ]]> text</datadesc></dataitem></datalist></recordinfo></recorditem></appmsg></msg>".to_string(),
        ];
        for body in bodies {
            let raw_hex = body.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
            let row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
            let forwarded = message_v2_from_snapshot(&row).unwrap().forwarded_record.unwrap();
            assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Invalid, "{body:?}");
            assert!(forwarded.items.is_empty(), "{body:?}");
        }
    }

    #[test]
    fn forwarded_record_contract_rejects_unknown_entries_and_repeated_single_fields() {
        let item = "<dataitem><datatype>1</datatype><dataid>id</dataid><datadesc>text</datadesc><dataitemsource><hashusername>hash</hashusername></dataitemsource></dataitem>";
        let cases = [
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist>{item}<futureitem>opaque</futureitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem>{}</recorditem><recorditem>{}</recorditem></appmsg></msg>", "<recordinfo><datalist></datalist></recordinfo>", "<recordinfo><datalist></datalist></recordinfo>"),
            "<msg><appmsg><type>19</type><recorditem><recordinfo><datalist></datalist></recordinfo><recordinfo><datalist></datalist></recordinfo></recorditem></appmsg></msg>".to_string(),
            "<msg><appmsg><type>19</type><recorditem><recordinfo><datalist></datalist><datalist></datalist></recordinfo></recorditem></appmsg></msg>".to_string(),
            format!("<msg><appmsg><type>19</type><type>19</type><recorditem><![CDATA[<recordinfo><datalist>{item}</datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist>{item}</datalist></recordinfo>]]></recorditem></appmsg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist>{item}</datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><datatype>1</datatype><datatype>1</datatype></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><dataid>a</dataid><dataid>b</dataid></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><datadesc>a</datadesc><datadesc>b</datadesc></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><sourcename>a</sourcename><sourcename>b</sourcename></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><sourceheadurl>a</sourceheadurl><sourceheadurl>b</sourceheadurl></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><sourcetime>1</sourcetime><sourcetime>2</sourcetime></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><srcMsgCreateTime>1</srcMsgCreateTime><srcMsgCreateTime>2</srcMsgCreateTime></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><fromnewmsgid>1</fromnewmsgid><fromnewmsgid>2</fromnewmsgid></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><dataitemsource><hashusername>a</hashusername><hashusername>b</hashusername></dataitemsource></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><dataitemsource><hashusername>a</hashusername></dataitemsource><dataitemsource><hashusername>b</hashusername></dataitemsource></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>"),
        ];
        for body in cases {
            let raw_hex = body.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
            let row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
            let forwarded = message_v2_from_snapshot(&row).unwrap().forwarded_record.unwrap();
            assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Invalid, "{body}");
            assert!(forwarded.items.is_empty(), "{body}");
        }
    }

    #[test]
    fn forwarded_record_contract_does_not_choose_first_repeated_item_value() {
        let body = "<msg><appmsg><type>19</type><recorditem><![CDATA[<recordinfo><datalist><dataitem><datatype>1</datatype><dataid>first</dataid><dataid>second</dataid></dataitem></datalist></recordinfo>]]></recorditem></appmsg></msg>";
        let raw_hex = body.as_bytes().iter().map(|byte| format!("{byte:02X}")).collect::<String>();
        let row = serde_json::json!({ "rawType": 19_i64 << 32 | 49, "chatId": "alice", "content": "title", "timestamp": "now", "rawContent": raw_hex, "rawContentCompression": false });
        let forwarded = message_v2_from_snapshot(&row).unwrap().forwarded_record.unwrap();
        assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Invalid);
        assert!(forwarded.items.is_empty());
    }

    #[test]
    fn forwarded_record_rejects_non_xml_character_in_comment() {
        let inner = "<recordinfo><datalist><dataitem><datatype>1</datatype></dataitem></datalist></recordinfo>";
        let body = format!("<msg><appmsg><type>19</type><!-- bad \u{1} --><recorditem><![CDATA[{inner}]]></recorditem></appmsg></msg>");
        let forwarded = forwarded_record_for(&body);
        assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Invalid);
        assert!(forwarded.items.is_empty());
    }

    #[test]
    fn forwarded_record_rejects_non_xml_character_in_processing_instruction() {
        let inner = "<recordinfo><datalist><dataitem><datatype>1</datatype></dataitem></datalist></recordinfo>";
        let body = format!("<msg><appmsg><type>19</type><?agent bad \u{1}?><recorditem><![CDATA[{inner}]]></recorditem></appmsg></msg>");
        let forwarded = forwarded_record_for(&body);
        assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Invalid);
        assert!(forwarded.items.is_empty());
    }

    #[test]
    fn forwarded_record_rejects_non_whitespace_text_in_structure_containers() {
        let inner = "<recordinfo><datalist><dataitem><datatype>1</datatype></dataitem></datalist></recordinfo>";
        let bodies = [
            format!("<msg><appmsg><type>19</type><recorditem>unexpected{inner}</recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><recordinfo>unexpected<datalist><dataitem><datatype>1</datatype></dataitem></datalist></recordinfo></recorditem></appmsg></msg>"),
            format!("<msg><appmsg><type>19</type><recorditem><recordinfo><datalist>unexpected<dataitem><datatype>1</datatype></dataitem></datalist></recordinfo></recorditem></appmsg></msg>"),
        ];
        for body in bodies {
            let forwarded = forwarded_record_for(&body);
            assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Invalid, "{body}");
            assert!(forwarded.items.is_empty(), "{body}");
        }
    }

    #[test]
    fn forwarded_record_preserves_body_mixed_text_and_allows_container_whitespace() {
        let body = "<msg><appmsg><type>19</type><recorditem> \n <recordinfo> \n <datalist> \n <dataitem><datatype>1</datatype><datadesc>before<b>middle</b>after</datadesc></dataitem> \n </datalist> \n </recordinfo> \n </recorditem></appmsg></msg>";
        let forwarded = forwarded_record_for(body);
        assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Complete);
        assert_eq!(forwarded.items.len(), 1);
        assert_eq!(forwarded.items[0].datadesc.as_deref(), Some("beforemiddleafter"));
    }

    #[test]
    fn forwarded_record_rejects_doctype_and_external_entity_without_resolving_it() {
        let body = "<!DOCTYPE msg [<!ENTITY external SYSTEM 'file:///nonexistent/never-open'>]><msg><appmsg><type>19</type><recorditem>&external;</recorditem></appmsg></msg>";
        let forwarded = forwarded_record_for(body);
        assert_eq!(forwarded.parse_status, crate::ia::types::XmlParseStatus::Invalid);
        assert!(forwarded.items.is_empty());
    }

    #[test]
    fn message_reads_without_keys_are_not_empty_history() {
        let keys = HashMap::new();
        assert_eq!(try_find_message_db("test-account", &keys, "test-chat").unwrap_err(),
            WechatReadError::KeysUnavailable);
        assert_eq!(list_messages("test-account", &keys, "test-chat", 10, 0).unwrap_err(),
            WechatReadError::KeysUnavailable);
    }

    #[test]
    fn unreadable_candidate_is_not_a_missing_chat() {
        let dir = tempfile::tempdir().unwrap();
        let paths = vec![("message_0.db", "11", dir.path().join("absent.db").to_str().unwrap().to_owned())];
        assert_eq!(find_message_db_from_paths(&paths, "Msg_test").unwrap_err(),
            WechatReadError::DatabaseRead);
    }

    #[test]
    fn readable_candidates_distinguish_missing_and_present_chat() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_str().unwrap().to_owned();
        let key = "11".repeat(32);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{key}'\"; PRAGMA cipher_compatibility = 4;
             CREATE TABLE Msg_present (local_id INTEGER);"
        )).unwrap();
        drop(conn);
        let paths = vec![("message_0.db", key.as_str(), path)];
        assert!(find_message_db_from_paths(&paths, "Msg_absent").unwrap().is_none());
        assert_eq!(find_message_db_from_paths(&paths, "Msg_present").unwrap(),
            Some(("message_0.db".to_string(), key.as_str())));
    }
}

/// Get the Msg table name for a given chat username.
/// WeChat uses MD5(username) as the table suffix.
pub fn get_msg_table_name(chat_id: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(chat_id.as_bytes());
    let hash = hasher.finalize();
    format!("Msg_{:x}", hash)
}

/// Split WeChat's packed signed SQLite integer without first narrowing it.
pub fn split_message_type(raw: i64) -> (u32, u32) {
    let bits = raw as u64;
    (bits as u32, (bits >> 32) as u32)
}

/// Project an existing frozen row into the lossless V2 wire contract.
pub fn message_v2_from_snapshot(row: &Value) -> Option<MessageV2> {
    let raw_type = row.get("rawType")?.as_i64()?;
    let (msg_type, sub_type) = split_message_type(raw_type);
    let server_id = row.get("serverId").and_then(Value::as_i64).filter(|id| *id != 0).map(|id| id.to_string());
    let local_id = row.get("localId").and_then(Value::as_i64).filter(|id| *id != 0).map(|id| id.to_string());
    let chat_username = row.get("chatId")?.as_str()?.to_string();
    let raw_content = row.get("rawContent").and_then(Value::as_str).map(str::to_string);
    let raw_content_compression = row.get("rawContentCompression")?.as_bool()?;
    let compressed = raw_content_compression;
    let records = match raw_content.as_deref() {
        Some(hex) => match decode_stored_content_for_record(hex, compressed) {
            Ok(body) => {
                let body = if chat_username.contains("@chatroom") {
                    extract_group_sender(&body).1
                } else {
                    body
                };
                project_message_record(&body, &chat_username, msg_type, sub_type)
            }
            Err(RecordBodyError::Truncated) => {
                unparsed_message_record(msg_type, sub_type, XmlParseStatus::Truncated)
            }
            Err(RecordBodyError::Invalid) => {
                unparsed_message_record(msg_type, sub_type, XmlParseStatus::Invalid)
            }
        },
        None => unparsed_message_record(msg_type, sub_type, XmlParseStatus::Invalid),
    };
    Some(MessageV2 {
        server_id,
        local_id,
        raw_type: raw_type.to_string(),
        r#type: msg_type,
        sub_type,
        chat_username,
        sender_username: row.get("sender").and_then(Value::as_str).map(str::to_string),
        sender_display_name: row.get("senderName").and_then(Value::as_str).map(str::to_string),
        content: row.get("content")?.as_str()?.to_string(),
        timestamp: row.get("timestamp").and_then(Value::as_str).filter(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).is_ok()).map(str::to_string),
        is_self: row.get("isSelf").and_then(Value::as_bool),
        is_mentioned: row.get("isMentioned").and_then(Value::as_bool),
        raw_content,
        raw_content_compression,
        xml_sub_type: records.xml_sub_type,
        reference: records.reference,
        forwarded_record: records.forwarded_record,
    })
}

/// Extract a reference's identity using the context the record was read from.
/// The old display-only `ReplyInfo` extractor remains intentionally unchanged.
pub fn extract_reference_v2(body: &str, chat_username: &str) -> Option<ReferenceV2> {
    extract_reference_fields(body, chat_username)
}

#[derive(Debug, Clone)]
pub struct MessageSourceAnchor {
    pub message: MessageV2,
    pub source_shard: String,
    pub table_name: String,
    pub local_id: i64,
    pub server_id: i64,
    pub created: i64,
    pub sort_seq: Option<String>,
    pub local_type: i64,
    pub raw_content: String,
    pub raw_source_hex: String,
    pub raw_source_compressed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageLocateError {
    InvalidRequest,
    NotFound,
    Ambiguous,
    ChatNameUnavailable,
    Snapshot(MessageSnapshotError),
}

/// Find one exact native server ID in the current account's isolated shard set.
/// This is an indexed lookup per validated Msg table, not a full history scan.
pub fn locate_message_by_server_id(
    database_root: &Path,
    account_dir: &str,
    keys: &HashMap<String, String>,
    server_id: i64,
    chat_username: Option<&str>,
    known_chat_usernames: &[String],
    cancel: &CollectionCancel,
) -> Result<MessageSourceAnchor, MessageLocateError> {
    if server_id == 0 || chat_username.is_some_and(|chat| chat.trim().is_empty()) {
        return Err(MessageLocateError::InvalidRequest);
    }
    cancel.check().map_err(MessageLocateError::Snapshot)?;
    let shard_names = discover_message_shards(database_root).map_err(MessageLocateError::Snapshot)?;
    let shards = resolve_message_shards(database_root, &shard_names, keys).map_err(MessageLocateError::Snapshot)?;
    let target_table = chat_username.map(get_msg_table_name);
    let mut found: Option<(String, String, Value)> = None;

    for shard in &shards {
        cancel.check().map_err(MessageLocateError::Snapshot)?;
        let conn = open_shard_readonly(&shard.path, &shard.key, cancel).map_err(MessageLocateError::Snapshot)?;
        let tables = if let Some(table) = target_table.as_ref() {
            if shard_has_chat_table(&conn, table, cancel).map_err(MessageLocateError::Snapshot)? { vec![table.clone()] } else { Vec::new() }
        } else {
            let mut statement = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'Msg_[0-9a-f]*'")
                .map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            let mut rows = statement.query([]).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            let mut names = Vec::new();
            while let Some(row) = rows.next().map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))? {
                let name: String = row.get(0).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
                if valid_msg_table_name(&name) { names.push(name); }
            }
            names
        };

        for table in tables {
            cancel.check().map_err(MessageLocateError::Snapshot)?;
            // The table name comes from the MD5 helper or validated sqlite_master data.
            let sql = format!(
                "SELECT m.local_id, m.server_id, m.local_type, m.create_time, CAST(m.sort_seq AS TEXT) AS sort_seq,
                        hex(m.message_content) AS hex_content, m.WCDB_CT_message_content AS is_compressed,
                        hex(m.source) AS hex_source, m.WCDB_CT_source AS source_compressed,
                        n.user_name AS sender_name
                 FROM \"{table}\" m LEFT JOIN Name2Id n ON m.real_sender_id=n.rowid
                 WHERE m.server_id=?1 LIMIT 2"
            );
            let mut statement = conn.prepare(&sql).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            let columns: Vec<String> = statement.column_names().iter().map(|name| name.to_string()).collect();
            let mut rows = statement.query([server_id]).map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))?;
            while let Some(row) = rows.next().map_err(|_| MessageLocateError::Snapshot(read_outcome(cancel)))? {
                cancel.check().map_err(MessageLocateError::Snapshot)?;
                let value = row_to_value(row, &columns).map_err(MessageLocateError::Snapshot)?;
                if found.is_some() { return Err(MessageLocateError::Ambiguous); }
                found = Some((shard.name.clone(), table.clone(), value));
            }
        }
    }
    let (source_shard, table_name, row) = found.ok_or(MessageLocateError::NotFound)?;
    let chat_username = if let Some(chat) = chat_username {
        chat.to_string()
    } else {
        let candidates: Vec<_> = known_chat_usernames.iter().filter(|chat| get_msg_table_name(chat) == table_name).collect();
        if candidates.len() != 1 { return Err(MessageLocateError::ChatNameUnavailable); }
        candidates[0].clone()
    };
    let source_index = message_shard_index(&source_shard).ok_or(MessageLocateError::Snapshot(MessageSnapshotError::SourceUnavailable))?;
    let contact_names = HashMap::new();
    let context = RowContext {
        chat_id: &chat_username,
        account_dir,
        contact_names: &contact_names,
        require_create_time: true,
        bounded_decode: true,
    };
    let decoded = decode_message_row(&row, context).map_err(|error| MessageLocateError::Snapshot(error.into()))?;
    let local_id = decoded.message.local_id;
    if local_id == 0 { return Err(MessageLocateError::InvalidRequest); }
    let server_id_value = row.get("server_id").and_then(Value::as_i64).ok_or(MessageLocateError::InvalidRequest)?;
    let created = decoded.create_time.ok_or(MessageLocateError::InvalidRequest)?;
    let shard = MessageShard { name: source_shard.clone(), path: database_root.join("message").join(&source_shard), key: String::new(), index: source_index };
    let mut sample = sample_value(&decoded, &shard).map_err(MessageLocateError::Snapshot)?;
    if let Some(sender) = decoded.message.sender.as_ref() {
        let names = resolve_contact_names(database_root, keys, &HashSet::from([sender.clone()]), cancel)
            .map_err(MessageLocateError::Snapshot)?;
        apply_contact_names(std::slice::from_mut(&mut sample), &names, &SampleBudget::default(), cancel)
            .map_err(MessageLocateError::Snapshot)?;
    }
    let message = message_v2_from_snapshot(&sample).ok_or(MessageLocateError::InvalidRequest)?;
    let raw_source_hex = row.get("hex_content").and_then(Value::as_str).unwrap_or_default().to_string();
    let raw_source_compressed = row.get("is_compressed").and_then(Value::as_i64).unwrap_or_default() != 0;
    Ok(MessageSourceAnchor {
        message,
        source_shard,
        table_name,
        local_id,
        server_id: server_id_value,
        created,
        sort_seq: row.get("sort_seq").and_then(Value::as_str).map(str::to_string),
        local_type: decoded.raw_type,
        raw_content: decoded.raw_content,
        raw_source_hex,
        raw_source_compressed,
    })
}

fn valid_msg_table_name(name: &str) -> bool {
    name.strip_prefix("Msg_").is_some_and(|suffix| suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
}

/// Decompressed bytes one stored blob may expand into on the sample path (4 MiB).
///
/// A stored blob is capped at [`SNAPSHOT_ROW_CONTENT_LIMIT`] before it is read,
/// which bounds what the *database* holds, not what the decoder produces: a
/// megabyte of zstd can expand into orders of magnitude more. The sample answers
/// from a byte budget, so its decode is bounded too. The legacy list keeps
/// [`decode_message_content`]'s unbounded behaviour — it is a display path with
/// no budget behind it, and changing it would change what the list has always
/// returned.
const SNAPSHOT_DECODE_MAX_BYTES: usize = 4 * 1024 * 1024;
/// Window log the sample's decoder accepts (8 MiB windows). The default accepts
/// 128 MiB, which is itself more than the whole output budget.
const SNAPSHOT_DECODE_WINDOW_LOG: u32 = 23;

/// The placeholder a bounded decode reports instead of a body it will not build.
const DECODE_TOO_LARGE: &str = "[compressed content - exceeds the read budget]";

/// Decode stored content, bounding the decompression when the caller needs it.
fn decode_stored_content(hex: &str, is_compressed: bool, bounded: bool) -> String {
    decode_stored_content_with_limit(hex, is_compressed, bounded, SNAPSHOT_DECODE_MAX_BYTES)
}

fn decode_stored_content_with_limit(
    hex: &str,
    is_compressed: bool,
    bounded: bool,
    max_bytes: usize,
) -> String {
    if !bounded {
        return decode_message_content(hex, is_compressed);
    }
    if hex.is_empty() {
        return String::new();
    }
    let Some(bytes) = hex_decode(hex) else {
        return String::new();
    };
    if !(is_compressed && hex.len() >= 8 && hex[..8].eq_ignore_ascii_case(ZSTD_MAGIC)) {
        // A plain blob is already bounded by the stored length the sample
        // statement checked.
        return String::from_utf8_lossy(&bytes).to_string();
    }
    match decode_zstd_bounded(&bytes, max_bytes) {
        Ok(decoded) => String::from_utf8_lossy(&decoded).to_string(),
        Err(_) => DECODE_TOO_LARGE.to_string(),
    }
}

/// Decompress one zstd body under an output cap, or fail.
///
/// A frame whose declared content size is already above the cap is refused
/// before any window is allocated. For a frame that does not declare one, the
/// decoder reads at most `cap + 1` bytes, so a frame that lies about its size
/// still cannot make this allocate without limit — the cap is what stops it, not
/// the declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedDecodeError {
    Invalid,
    TooLarge,
}

fn decode_zstd_bounded(bytes: &[u8], max_bytes: usize) -> Result<Vec<u8>, BoundedDecodeError> {
    use std::io::Read;
    let mut decoder = zstd::stream::read::Decoder::new(bytes).map_err(|_| BoundedDecodeError::Invalid)?;
    // A frame that asks for a window above the cap is refused by the decoder
    // itself, before any window is allocated for it.
    decoder
        .window_log_max(SNAPSHOT_DECODE_WINDOW_LOG)
        .map_err(|_| BoundedDecodeError::TooLarge)?;
    let mut out = Vec::new();
    // One byte past the cap: reaching it proves the output is over the cap. The
    // cap is what bounds this, not the frame's own declared size.
    let mut limited = (&mut decoder).take(max_bytes as u64 + 1);
    limited.read_to_end(&mut out).map_err(|_| BoundedDecodeError::Invalid)?;
    if out.len() > max_bytes {
        return Err(BoundedDecodeError::TooLarge);
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordBodyError {
    Invalid,
    Truncated,
}

fn decode_stored_content_for_record(
    hex: &str,
    is_compressed: bool,
) -> Result<String, RecordBodyError> {
    if hex.is_empty() {
        return Ok(String::new());
    }
    let bytes = hex_decode(hex).ok_or(RecordBodyError::Invalid)?;
    if is_compressed && hex.len() >= 8 && hex[..8].eq_ignore_ascii_case(ZSTD_MAGIC) {
        let decoded = decode_zstd_bounded(&bytes, SNAPSHOT_DECODE_MAX_BYTES).map_err(|error| match error {
            BoundedDecodeError::Invalid => RecordBodyError::Invalid,
            BoundedDecodeError::TooLarge => RecordBodyError::Truncated,
        })?;
        String::from_utf8(decoded).map_err(|_| RecordBodyError::Invalid)
    } else {
        String::from_utf8(bytes).map_err(|_| RecordBodyError::Invalid)
    }
}

/// Decode hex-encoded message content, decompressing zstd if needed.
///
/// This is the display decode the message list has always used, and it is lossy
/// on purpose: invalid UTF-8 becomes replacement characters, an odd-length hex
/// string becomes an empty string, and a zstd body that cannot be decompressed
/// becomes a placeholder. The fixed sample therefore also carries the stored
/// bytes as `rawContent`, so a reader that needs the original text does not have
/// to trust this string.
pub fn decode_message_content(hex: &str, is_compressed: bool) -> String {
    if hex.is_empty() {
        return String::new();
    }
    let bytes = match hex_decode(hex) {
        Some(b) => b,
        None => return String::new(),
    };
    if is_compressed && hex.len() >= 8 && hex[..8].eq_ignore_ascii_case(ZSTD_MAGIC) {
        match zstd::decode_all(bytes.as_slice()) {
            Ok(decompressed) => String::from_utf8_lossy(&decompressed).to_string(),
            Err(_) => "[compressed content - decompression failed]".to_string(),
        }
    } else {
        String::from_utf8_lossy(&bytes).to_string()
    }
}

/// Decode a hex string to bytes.
pub fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    Some(bytes)
}

/// Extract sender from group message content.
/// Group messages have format "sender_wxid:\nmessage_content".
fn extract_group_sender(content: &str) -> (Option<String>, String) {
    if let Some(idx) = content.find(":\n") {
        if idx < 80 {
            let sender = content[..idx].to_string();
            let msg = content[idx + 2..].to_string();
            return (Some(sender), msg);
        }
    }
    (None, content.to_string())
}

/// Clean message content for display based on message type.
/// Replaces verbose XML with concise summaries.
fn clean_content(content: &str, msg_type: i32) -> String {
    let base = msg_type & 0x7FFFFFFF;
    match base {
        // Image (type 3): replace XML with empty string
        3 if content.contains("<img") => String::new(),
        // Emoji (type 47): show cdnurl or [emoji]
        47 if content.contains("<emoji") => {
            extract_xml_attr(content, "cdnurl")
                .filter(|u| u.starts_with("http"))
                .unwrap_or_else(|| "[emoji]".to_string())
        }
        // Appmsg (type 49): handle subtypes
        49 if content.contains("<msg>") => {
            let title = extract_xml_tag(content, "title").unwrap_or_default();
            let appmsg_type = extract_xml_tag(content, "type")
                .and_then(|t| t.parse::<i32>().ok())
                .unwrap_or(0);
            match appmsg_type {
                // Link share (5), video link (4), music share (3)
                3 | 4 | 5 => {
                    let mut parts = Vec::new();
                    parts.push(format!("[Link] {title}"));
                    if let Some(des) = extract_xml_tag(content, "des") {
                        parts.push(des);
                    }
                    if let Some(url) = extract_xml_tag(content, "url") {
                        let url = url.replace("&amp;", "&");
                        parts.push(url);
                    }
                    parts.join("\n")
                }
                _ => {
                    if title.is_empty() {
                        content.to_string()
                    } else {
                        title
                    }
                }
            }
        }
        _ => content.to_string(),
    }
}

/// Extract reply info from type 49 (appmsg) messages with <refermsg>.
fn extract_reply_info(content: &str, msg_type: i32) -> Option<ReplyInfo> {
    let base = msg_type & 0x7FFFFFFF;
    if base != 49 || !content.contains("<refermsg>") {
        return None;
    }
    // Extract the refermsg block
    let rm_start = content.find("<refermsg>")?;
    let rm_end = content.find("</refermsg>")? + "</refermsg>".len();
    let refermsg = &content[rm_start..rm_end];

    let sender = extract_xml_tag(refermsg, "displayname");
    let ref_content = extract_xml_tag(refermsg, "content").unwrap_or_default();

    // The referred content may be XML-escaped — unescape first
    let unescaped = ref_content
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"");

    // The referred content may itself be XML — clean it to a short text
    let clean = if unescaped.contains("<msg>") {
        extract_xml_tag(&unescaped, "title").unwrap_or(unescaped)
    } else {
        unescaped
    };
    Some(ReplyInfo {
        sender,
        content: clean,
    })
}

/// Extract an XML attribute value: attr="value"
fn extract_xml_attr(xml: &str, attr: &str) -> Option<String> {
    let pattern = format!("{attr}=\"");
    let start = xml.find(&pattern)? + pattern.len();
    let end = xml[start..].find('"')? + start;
    let val = xml[start..end].trim().to_string();
    if val.is_empty() { None } else { Some(val) }
}

/// Extract text between XML tags: <tag>text</tag>
/// Also handles CDATA: <tag><![CDATA[text]]></tag>
pub(crate) fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    let mut val = xml[start..end].trim().to_string();
    // Strip CDATA wrapper if present
    if val.starts_with("<![CDATA[") && val.ends_with("]]>") {
        val = val[9..val.len() - 3].to_string();
    }
    if val.is_empty() { None } else { Some(val) }
}

/// Check if the source XML indicates the current user is @-mentioned.
fn check_is_mentioned(source: &str, account_dir: &str) -> bool {
    if let Some(at_list) = extract_xml_tag(source, "atuserlist") {
        // atuserlist may contain comma-separated wxids
        for wxid in at_list.split(',') {
            let wxid = wxid.trim();
            if !wxid.is_empty() && account_dir.starts_with(wxid) {
                return true;
            }
        }
    }
    false
}

/// Find which message DB contains a chat and return (db_name, key).
pub fn find_message_db<'a>(
    account_dir: &str,
    keys: &'a HashMap<String, String>,
    chat_id: &str,
) -> Option<(String, &'a str)> {
    // Media callers retain their optional-result contract until migrated.
    try_find_message_db(account_dir, keys, chat_id).unwrap_or_else(|error| {
        tracing::warn!(code = error.code(), "WeChat media database lookup failed");
        None
    })
}

pub fn is_message_db(name: &str) -> bool {
    name.starts_with("message_") && name.ends_with(".db")
        && !name.contains("fts") && !name.contains("resource")
}

pub fn try_find_message_db<'a>(
    account_dir: &str,
    keys: &'a HashMap<String, String>,
    chat_id: &str,
) -> Result<Option<(String, &'a str)>, WechatReadError> {
    let table_name = get_msg_table_name(chat_id);
    let mut message_dbs: Vec<(&str, &str)> = keys
        .iter()
        .filter(|(k, _)| is_message_db(k))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    message_dbs.sort_by_key(|(k, _)| k.to_string());

    if message_dbs.is_empty() {
        return Err(WechatReadError::KeysUnavailable);
    }
    let paths: Vec<_> = message_dbs.into_iter()
        .map(|(name, key)| (name, key, get_db_path(account_dir, name)))
        .collect();
    find_message_db_from_paths(&paths, &table_name)
}

fn find_message_db_from_paths<'a>(
    databases: &[(&str, &'a str, String)],
    table_name: &str,
) -> Result<Option<(String, &'a str)>, WechatReadError> {
    for (db_name, key, db_path) in databases {
        let check = try_query_wechat_db(
            db_path,
            key,
            &format!(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
            ),
        )?;
        if !check.is_empty() {
            return Ok(Some((db_name.to_string(), *key)));
        }
    }
    Ok(None)
}

/// List messages for a specific chat.
///
/// Messages may be spread across message_0.db, message_1.db, etc.
/// Each chat's messages are in a `Msg_{MD5(username)}` table.
pub fn list_messages(
    account_dir: &str,
    keys: &HashMap<String, String>,
    chat_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<Message>, WechatReadError> {
    let table_name = get_msg_table_name(chat_id);

    let (db_name, key) = match try_find_message_db(account_dir, keys, chat_id)? {
        Some(dk) => dk,
        None => return Ok(Vec::new()),
    };
    let db_path = get_db_path(account_dir, &db_name);

    // Query messages using hex() for safe binary/compressed content extraction
    let rows = try_query_wechat_db(
        &db_path,
        key,
        &format!(
            "SELECT m.local_id, m.server_id, m.local_type, m.create_time,
                    hex(m.message_content) as hex_content,
                    m.WCDB_CT_message_content as is_compressed,
                    hex(m.source) as hex_source,
                    m.WCDB_CT_source as source_compressed,
                    n.user_name as sender_name
             FROM \"{table_name}\" m
             LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
             ORDER BY m.create_time DESC
             LIMIT {limit} OFFSET {offset};"
        ),
    )?;

    // Resolve sender display names from contact.db
    let contact_names: HashMap<String, String> = {
        let mut map = HashMap::new();
        if let Some(contact_key) = keys.get("contact.db") {
            // Collect unique sender wxids
            let senders: Vec<String> = rows.iter()
                .filter_map(|row| {
                    row.get("sender_name")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                })
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            if !senders.is_empty() {
                let contact_db = get_db_path(account_dir, "contact.db");
                let placeholders = senders.iter().map(|s| format!("'{}'", s.replace('\'', "''"))).collect::<Vec<_>>().join(",");
                let contacts = try_query_wechat_db(
                    &contact_db,
                    contact_key,
                    &format!("SELECT username, nick_name, remark FROM contact WHERE username IN ({placeholders});"),
                )?;
                for c in contacts {
                    if let Some(username) = c.get("username").and_then(|v| v.as_str()) {
                        let name = c.get("remark").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                            .or_else(|| c.get("nick_name").and_then(|v| v.as_str()).filter(|s| !s.is_empty()))
                            .unwrap_or(username);
                        map.insert(username.to_string(), name.to_string());
                    }
                }
            }
        }
        map
    };

    let context = RowContext {
        chat_id,
        account_dir,
        contact_names: &contact_names,
        // Unchanged behaviour: a row without a usable `create_time` still
        // produces a message with an empty timestamp, and a row that cannot be
        // decoded at all is skipped, exactly as before.
        require_create_time: false,
        // The list is a display path with no budget behind it, so its decode
        // stays what it has always been.
        bounded_decode: false,
    };

    Ok(rows
        .iter()
        .filter_map(|row| decode_message_row(row, context).ok().map(|decoded| decoded.message))
        .collect())
}

// ── Row decoding ───────────────────────────────────────────────────────────

/// The inputs both message readers decode a row with, so the legacy list and
/// the fixed sample cannot drift apart.
#[derive(Clone, Copy)]
struct RowContext<'a> {
    chat_id: &'a str,
    /// The WeChat account directory name. It is only used by the `isSelf`
    /// heuristic below, which compares name prefixes; it is not a verified
    /// account identity and says nothing about who the session belongs to.
    account_dir: &'a str,
    contact_names: &'a HashMap<String, String>,
    /// The sample requires a decodable `create_time`, because the sample's order
    /// is defined by it. The legacy list tolerates its absence.
    require_create_time: bool,
    /// Decode compressed content under a bound. The sample sets it: a stored
    /// blob is capped before it is read, but its decompressed size is not, so
    /// the sample — which answers from a fixed budget — must not let one row
    /// allocate whatever the stored bytes expand into.
    bounded_decode: bool,
}

impl RowContext<'_> {
    fn is_group(&self) -> bool {
        self.chat_id.contains("@chatroom")
    }
}

/// Why a row cannot become a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowError {
    /// A field the sample needs for identity or ordering is missing or is not an
    /// integer, or a stored blob is above the per-row limit.
    Invalid,
    /// A stored blob is above [`SNAPSHOT_ROW_CONTENT_LIMIT`].
    TooLarge,
}

impl From<RowError> for MessageSnapshotError {
    fn from(error: RowError) -> Self {
        match error {
            RowError::Invalid => MessageSnapshotError::RowInvalid,
            RowError::TooLarge => MessageSnapshotError::RowTooLarge,
        }
    }
}

/// A decoded row: the message the legacy list returns, plus the stored values
/// the display fields cannot carry.
#[derive(Debug)]
struct DecodedRow {
    message: Message,
    create_time: Option<i64>,
    /// `local_type` before the 64 → 32 bit truncation of `Message.msg_type`.
    raw_type: i64,
    /// The stored `message_content` bytes, hex encoded.
    raw_content: String,
    raw_content_compressed: bool,
}

/// Reject a row whose stored blob is above the per-row limit.
///
/// The sample statement only transfers blobs up to the limit, so an oversized
/// row arrives with its length but without content; the length column is what
/// separates "too large" from "empty". Nothing is truncated, the row is refused.
/// The legacy list has no length column and is unaffected.
fn check_row_blob_length(row: &Value, column: &str) -> Result<(), RowError> {
    match row.get(column).and_then(Value::as_i64) {
        Some(length) if length > SNAPSHOT_ROW_CONTENT_LIMIT as i64 => Err(RowError::TooLarge),
        _ => Ok(()),
    }
}

/// Decode one message row.
///
/// This is the single decode both readers use. The legacy list keeps its
/// tolerant behaviour by ignoring failures, while the sample turns them into a
/// fixed error: a row that cannot be decoded is never dropped silently.
fn decode_message_row(row: &Value, context: RowContext<'_>) -> Result<DecodedRow, RowError> {
    check_row_blob_length(row, "content_len")?;
    check_row_blob_length(row, "source_len")?;

    // The sample's order and its duplicate check both rest on these two fields,
    // so a row without a usable value is refused rather than skipped.
    let local_id = row.get("local_id").and_then(Value::as_i64).ok_or(RowError::Invalid)?;
    let create_time = row.get("create_time").and_then(Value::as_i64);
    if context.require_create_time && create_time.is_none() {
        return Err(RowError::Invalid);
    }

    let server_id = row.get("server_id").and_then(Value::as_i64).unwrap_or(0);
    let raw_type = row.get("local_type").and_then(Value::as_i64).unwrap_or(0);
    let msg_type = raw_type as i32;

    let hex_content = row.get("hex_content").and_then(Value::as_str).unwrap_or("");
    let is_compressed = row.get("is_compressed").and_then(Value::as_i64).unwrap_or(0) != 0;
    let raw_content = decode_stored_content(hex_content, is_compressed, context.bounded_decode);

    // Get sender from Name2Id join (works for both group and 1:1 chats)
    let sender = row
        .get("sender_name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    // Strip group sender prefix from content ("wxid:\ncontent" format)
    let body = if context.is_group() {
        extract_group_sender(&raw_content).1
    } else {
        raw_content
    };

    // Extract reply info before cleaning (needs raw XML)
    let reply = extract_reply_info(&body, msg_type);

    // Clean content for display (replace XML with summaries)
    let content = clean_content(&body, msg_type);

    let timestamp = create_time
        .map(|time| {
            chrono::DateTime::from_timestamp(time, 0)
                .map(|date| date.to_rfc3339())
                .unwrap_or_default()
        })
        .unwrap_or_default();

    // Check @-mention from source XML (only for group chats)
    let is_mentioned = if context.is_group() {
        let hex_source = row.get("hex_source").and_then(Value::as_str).unwrap_or("");
        let source_compressed = row.get("source_compressed").and_then(Value::as_i64).unwrap_or(0) != 0;
        let from_source = if !hex_source.is_empty() {
            let source_xml = decode_stored_content(hex_source, source_compressed, context.bounded_decode);
            check_is_mentioned(&source_xml, context.account_dir)
        } else {
            false
        };

        // For type-49 (appmsg) messages — especially reference/quote messages —
        // WeChat may place <atuserlist> inside the content XML instead of source.
        let from_content = if !from_source && (msg_type & 0x7FFFFFFF) == 49 {
            check_is_mentioned(&body, context.account_dir)
        } else {
            false
        };

        if from_source || from_content {
            Some(true)
        } else {
            None
        }
    } else {
        None
    };

    // Check if message was sent by the logged-in user. This compares the
    // account directory name with the sender name and is only a prefix
    // heuristic, not proof of which account is logged in.
    let is_self = sender.as_ref().map(|s| context.account_dir.starts_with(s.as_str()));

    let sender_name = sender
        .as_ref()
        .and_then(|wxid| context.contact_names.get(wxid))
        .cloned();

    Ok(DecodedRow {
        message: Message {
            local_id,
            server_id,
            chat_id: context.chat_id.to_string(),
            sender,
            sender_name,
            msg_type,
            content,
            timestamp,
            is_mentioned,
            is_self,
            reply,
        },
        create_time,
        raw_type,
        raw_content: hex_content.to_string(),
        raw_content_compressed: is_compressed,
    })
}


// ── Fixed cross-shard message sample ───────────────────────────────────────

/// Rows one sample may contain before it is refused.
const SNAPSHOT_MAX_ROWS: usize = 50_000;
/// Serialized JSON bytes one sample may contain before it is refused.
const SNAPSHOT_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Largest stored `message_content` or `source` blob one row may carry (1 MiB).
const SNAPSHOT_ROW_CONTENT_LIMIT: usize = 1024 * 1024;
/// Wall clock one whole collection may take, discovery and contact names
/// included.
///
/// A round that cannot answer in this time answers "later" instead: the caller
/// keeps its own answerable work — the guard, the identity capture, the rows it
/// has already frozen — instead of being pinned behind a single shard that is
/// slow or locked. The message list's own budget is longer, so the paginated
/// read is the tighter of the two, not the looser.
pub const COLLECTION_BUDGET: Duration = Duration::from_secs(8);
/// VDBE steps between two deadline checks inside one statement.
const COLLECTION_PROGRESS_STEPS: u32 = 1_000;

/// The deadline of one collection, shared with the work it is collecting.
///
/// Shared because the collection runs on a blocking thread that outlives the
/// caller's patience: when the caller stops waiting it sets the flag, and the
/// thread sees it at its next check instead of running to whatever its own idea
/// of the budget was. Cloning shares one deadline, so every clone stops at the
/// same moment.
#[derive(Clone)]
pub struct CollectionCancel {
    inner: Arc<CollectionState>,
}

struct CollectionState {
    at: Instant,
    cancelled: AtomicBool,
    authentication_failure: std::sync::Mutex<Option<String>>,
    /// Test-only seam: the clock the round reads. `None` is the real clock and
    /// is the only value production ever holds. A test pins it to reach the
    /// deadline at an exact point in the pass instead of racing the wall clock.
    #[cfg(test)]
    now: std::sync::Mutex<Option<Instant>>,
}

impl CollectionCancel {
    /// A deadline `budget` from now.
    pub fn within(budget: Duration) -> Self {
        Self {
            inner: Arc::new(CollectionState {
                at: Instant::now() + budget,
                cancelled: AtomicBool::new(false),
                authentication_failure: std::sync::Mutex::new(None),
                #[cfg(test)]
                now: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Whether the collection has to stop: its own deadline passed, or nobody is
    /// waiting for it any more.
    pub fn expired(&self) -> bool {
        self.inner.cancelled.load(Ordering::Relaxed) || self.now() >= self.inner.at
    }

    /// The round's clock. Production reads the real clock; a test can pin it, so
    /// the deadline is reached at a chosen moment rather than by wall time.
    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(at) = *self.inner.now.lock().unwrap() {
            return at;
        }
        Instant::now()
    }

    /// Time left, or zero once expired. Used to shorten the wait for a lock, so
    /// a collision with WeChat's writer cannot outlive the deadline: a caller that
    /// stopped waiting is zero time left, not the time the deadline still had.
    pub fn remaining(&self) -> Duration {
        if self.expired() {
            return Duration::ZERO;
        }
        self.inner.at.saturating_duration_since(self.now())
    }

    /// Stop waiting. The collection still in flight will see this and stop; it
    /// is what keeps a caller that gave up from leaving a thread behind.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn record_authentication_failure(&self, database_name: &str) {
        if !valid_database_basename(database_name) { return; }
        if let Ok(mut failure) = self.inner.authentication_failure.lock() {
            if failure.is_none() { *failure = Some(database_name.to_owned()); }
        }
    }

    pub(crate) fn authentication_failure(&self) -> Option<String> {
        self.inner.authentication_failure.lock().ok()?.clone()
    }

    /// Test-only: reach the deadline deterministically, so a test can put a
    /// round past its deadline at an exact point in the pass instead of waiting
    /// for the wall clock. It moves the round's clock, so the real `expired()`
    /// deadline comparison is what refuses.
    #[cfg(test)]
    pub fn reach_deadline_for_test(&self) {
        *self.inner.now.lock().unwrap() = Some(self.inner.at + Duration::from_millis(1));
    }

    /// The one check every stage, shard and row makes.
    pub fn check(&self) -> Result<(), MessageSnapshotError> {
        if self.expired() {
            return Err(MessageSnapshotError::DeadlineExceeded);
        }
        Ok(())
    }
}

/// A read error that happened at or after the deadline is reported as the
/// deadline being missed, not as a database failure: the same collision, one
/// millisecond earlier, would have been `DatabaseRead`, and the distinction the
/// caller acts on is "ask again later" against "this source is broken".
pub(crate) fn read_outcome(cancel: &CollectionCancel) -> MessageSnapshotError {
    if cancel.expired() {
        MessageSnapshotError::DeadlineExceeded
    } else {
        MessageSnapshotError::DatabaseRead
    }
}

/// Fixed outcomes of a message sample.
///
/// Every variant is fieldless, so the value carries no path, key, SQL or
/// message content and is safe to log and to map onto an HTTP response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageSnapshotError {
    /// A discovered shard has no decryption key.
    KeysUnavailable,
    /// The shard set cannot be used: no readable `message` directory, no
    /// numbered shard in it, or an entry that is not a shard file.
    SourceUnavailable,
    /// The shard set differed between the first and the last discovery.
    SourceChanged,
    /// A shard could not be opened, decrypted, queried or stepped.
    DatabaseRead,
    /// The sample would exceed the row or byte budget.
    BudgetExceeded,
    /// A row lacks a field the sample needs for identity or ordering.
    RowInvalid,
    /// One shard reports the same `local_id` twice.
    RowDuplicate,
    /// A stored blob is above the per-row limit.
    RowTooLarge,
    /// The collection ran out of its wall clock. The sample is discarded whole:
    /// a partial sample is not a sample.
    DeadlineExceeded,
}

impl MessageSnapshotError {
    pub fn code(self) -> &'static str {
        match self {
            Self::KeysUnavailable => "WECHAT_KEYS_UNAVAILABLE",
            Self::SourceUnavailable => "WECHAT_MESSAGE_SOURCE_UNAVAILABLE",
            Self::SourceChanged => "WECHAT_MESSAGE_SOURCE_CHANGED",
            Self::DatabaseRead => "WECHAT_DB_READ_FAILED",
            Self::BudgetExceeded => "WECHAT_MESSAGE_BUDGET_EXCEEDED",
            Self::RowInvalid => "WECHAT_MESSAGE_ROW_INVALID",
            Self::RowDuplicate => "WECHAT_MESSAGE_ROW_DUPLICATE",
            Self::RowTooLarge => "WECHAT_MESSAGE_ROW_TOO_LARGE",
            Self::DeadlineExceeded => "WECHAT_MESSAGE_DEADLINE_EXCEEDED",
        }
    }
}

impl std::fmt::Display for MessageSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for MessageSnapshotError {}

/// Collect one fixed sample of a chat's messages from every message shard.
///
/// `database_root` is the `db_storage` directory the identity module resolved
/// from the running client. The sample never picks a root of its own and never
/// falls back to another candidate: it reads the shards that are actually there.
///
/// The result is the frozen sample a paginated read serves from, not a single
/// instant across databases. Each shard is one read-only connection with one
/// complete bounded statement, so every shard is internally consistent, and
/// nothing here implies that two shards were read at the same moment. The shard
/// set is discovered again afterwards, so a shard that appeared or disappeared
/// during the reads is reported instead of being missed.
///
/// Every row keeps the fields the message list returns, plus `sourceShard` and
/// the stored `rawType` / `rawContent` / `rawContentCompression` values, so a
/// later reader can parse recalls and media from the original bytes. Rows are
/// never merged across shards by `local_id` or `server_id`: the same value in
/// two shards is two rows. The order is `create_time`, `local_id`, shard.
///
/// A shard that cannot be found, keyed, opened, decrypted, queried or decoded
/// fails the whole sample rather than being left out of it.
pub fn collect_message_snapshot(
    database_root: &Path,
    account_dir: &str,
    chat_id: &str,
    keys: &HashMap<String, String>,
) -> Result<Vec<Value>, MessageSnapshotError> {
    collect_message_snapshot_until(
        database_root,
        account_dir,
        chat_id,
        keys,
        &CollectionCancel::within(COLLECTION_BUDGET),
    )
}

/// Collect one sample under a deadline the caller owns.
///
/// Same sample, same refusals, same order — with a wall clock over the *whole*
/// collection rather than per statement: discovery, every shard, the second
/// discovery and the contact names all draw on the same budget. A caller that
/// stops waiting calls [`CollectionCancel::cancel`]; the sample is then reported
/// as [`MessageSnapshotError::DeadlineExceeded`] and nothing of it is returned.
pub fn collect_message_snapshot_until(
    database_root: &Path,
    account_dir: &str,
    chat_id: &str,
    keys: &HashMap<String, String>,
    cancel: &CollectionCancel,
) -> Result<Vec<Value>, MessageSnapshotError> {
    collect_fixed_sample(database_root, account_dir, chat_id, keys, &discover_message_shards, cancel)
}

/// `discover` is a parameter so a test can make the shard set change between
/// the first and the last discovery; production passes the directory scan.
fn collect_fixed_sample(
    database_root: &Path,
    account_dir: &str,
    chat_id: &str,
    keys: &HashMap<String, String>,
    discover: &dyn Fn(&Path) -> Result<Vec<String>, MessageSnapshotError>,
    cancel: &CollectionCancel,
) -> Result<Vec<Value>, MessageSnapshotError> {
    cancel.check()?;
    let before = discover(database_root)?;
    let shards = resolve_message_shards(database_root, &before, keys)?;
    cancel.check()?;

    let sample = collect_shard_rows(&shards, account_dir, chat_id, SampleBudget::default(), cancel)?;

    // Re-discover before returning: a sample that cannot prove its shard set was
    // stable is reported as a changed source, not as a complete sample.
    cancel.check()?;
    if discover(database_root)? != before {
        return Err(MessageSnapshotError::SourceChanged);
    }

    let names = resolve_contact_names(database_root, keys, &sample.senders, cancel)?;
    let mut values: Vec<Value> = sample.rows.into_iter().map(|row| row.value).collect();
    apply_contact_names(&mut values, &names, &SampleBudget::default(), cancel)?;
    cancel.check()?;
    Ok(values)
}

/// The row and byte budget of one sample.
struct SampleBudget {
    max_rows: usize,
    max_bytes: usize,
}

impl Default for SampleBudget {
    fn default() -> Self {
        Self {
            max_rows: SNAPSHOT_MAX_ROWS,
            max_bytes: SNAPSHOT_MAX_BYTES,
        }
    }
}

/// One `message_N.db` shard: its file name, its path inside the resolved root
/// and its key. Shards are separate databases with separate read views.
struct MessageShard {
    name: String,
    path: PathBuf,
    key: String,
    /// Shard number, the sample's last ordering key.
    index: u32,
}

/// Shard number of a numbered `message_N.db`, or `None` for every other file
/// (`message_fts.db`, `message_resource.db`, `message_abc.db`, ...).
fn message_shard_index(name: &str) -> Option<u32> {
    numbered_db_index(name, "message_")
}

/// Discover the shards inside `<database_root>/message`, in shard order.
///
/// The resolved root is the only directory consulted. A missing or unreadable
/// directory, an entry that cannot be read, a name that is not valid UTF-8 and
/// a numbered shard name that is not a regular file are all
/// `SourceUnavailable`: none of them may be reported as "no messages". Finding
/// no numbered shard at all is also `SourceUnavailable`, because a source that
/// cannot be read and a chat that was never stored look the same from here, and
/// an empty sample would claim the history is complete.
fn discover_message_shards(database_root: &Path) -> Result<Vec<String>, MessageSnapshotError> {
    let directory = database_root.join("message");
    let entries = std::fs::read_dir(&directory).map_err(|_| MessageSnapshotError::SourceUnavailable)?;

    let mut shards: Vec<(u32, String)> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| MessageSnapshotError::SourceUnavailable)?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or(MessageSnapshotError::SourceUnavailable)?;
        let Some(index) = message_shard_index(name) else {
            continue;
        };
        let file_type = entry
            .file_type()
            .map_err(|_| MessageSnapshotError::SourceUnavailable)?;
        if !file_type.is_file() {
            return Err(MessageSnapshotError::SourceUnavailable);
        }
        shards.push((index, name.to_string()));
    }

    if shards.is_empty() {
        return Err(MessageSnapshotError::SourceUnavailable);
    }
    shards.sort();
    Ok(shards.into_iter().map(|(_, name)| name).collect())
}

/// Attach the path and key of every discovered shard. A shard without a key
/// fails the whole sample: returning the other shards' rows would be a sample
/// that silently misses a chat.
fn resolve_message_shards(
    database_root: &Path,
    names: &[String],
    keys: &HashMap<String, String>,
) -> Result<Vec<MessageShard>, MessageSnapshotError> {
    names
        .iter()
        .map(|name| {
            let key = keys.get(name).ok_or(MessageSnapshotError::KeysUnavailable)?;
            Ok(MessageShard {
                index: message_shard_index(name).ok_or(MessageSnapshotError::SourceUnavailable)?,
                path: database_root.join("message").join(name),
                key: key.clone(),
                name: name.clone(),
            })
        })
        .collect()
}

/// A collected row together with the keys that fix the sample's order.
#[derive(Debug)]
struct SampledRow {
    create_time: i64,
    local_id: i64,
    shard_index: u32,
    shard_name: String,
    value: Value,
}

/// Everything one collection produced.
#[derive(Debug)]
struct CollectedSample {
    /// Already in sample order.
    rows: Vec<SampledRow>,
    /// Distinct senders, for the optional contact display names.
    senders: HashSet<String>,
}

/// Running state of one collection, so every shard spends the same row and byte
/// budget and the accounting cannot drift from one shard to the next.
struct SampleCollector {
    budget: SampleBudget,
    rows: Vec<SampledRow>,
    senders: HashSet<String>,
    bytes: usize,
    /// Test-only seam: run once after the rows are ordered and before the round
    /// is checked again, so a test can end the round *inside* the pass
    /// deterministically instead of racing the wall clock. Production never
    /// sets it.
    #[cfg(test)]
    after_sort: Option<Box<dyn FnOnce() + Send>>,
}

impl SampleCollector {
    fn new(budget: SampleBudget) -> Self {
        Self {
            budget,
            rows: Vec::new(),
            senders: HashSet::new(),
            bytes: 0,
            #[cfg(test)]
            after_sort: None,
        }
    }

    /// Rows still allowed, which also bounds each shard's statement.
    fn remaining_rows(&self) -> Result<usize, MessageSnapshotError> {
        self.budget
            .max_rows
            .checked_sub(self.rows.len())
            .ok_or(MessageSnapshotError::BudgetExceeded)
    }

    /// Add one row, or refuse the whole sample. The budget is charged while the
    /// rows are read, so an oversized shard is never read in full first.
    fn push(&mut self, row: SampledRow) -> Result<(), MessageSnapshotError> {
        if self.rows.len() >= self.budget.max_rows {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        let size = serialized_len(&row.value);
        if self.bytes + size > self.budget.max_bytes {
            return Err(MessageSnapshotError::BudgetExceeded);
        }
        self.bytes += size;
        self.rows.push(row);
        Ok(())
    }

    /// Order the sample and hand it over, or report that the round is over.
    ///
    /// The ordering is the last thing that touches every row, and `sort_by` has
    /// no place to stop inside a comparison, so the round is checked on both
    /// sides of it. That is not the same as interrupting the sort: what is
    /// refused is *returning* the result of one that ran out of time.
    fn into_sample(mut self, cancel: &CollectionCancel) -> Result<CollectedSample, MessageSnapshotError> {
        cancel.check()?;
        // `create_time`, `local_id`, shard number and shard name are unique
        // together, so the order is total and repeatable.
        self.rows.sort_by(|left, right| {
            left.create_time
                .cmp(&right.create_time)
                .then(left.local_id.cmp(&right.local_id))
                .then(left.shard_index.cmp(&right.shard_index))
                .then(left.shard_name.cmp(&right.shard_name))
        });
        // Test-only: the round can be ended here, between the sort and the check
        // that follows it, to prove that a pass ending *inside* the ordering is
        // refused. Production never installs a hook.
        #[cfg(test)]
        if let Some(hook) = self.after_sort.take() {
            hook();
        }
        cancel.check()?;
        Ok(CollectedSample {
            rows: self.rows,
            senders: self.senders,
        })
    }
}

/// Serialized size of a row, which is what the byte budget measures.
fn serialized_len(value: &Value) -> usize {
    // A `Value` cannot fail to serialize: it never holds a NaN, the only input
    // `to_vec` rejects.
    serde_json::to_vec(value).map(|bytes| bytes.len()).unwrap_or(0)
}

/// Read every shard into one sample.
fn collect_shard_rows(
    shards: &[MessageShard],
    account_dir: &str,
    chat_id: &str,
    budget: SampleBudget,
    cancel: &CollectionCancel,
) -> Result<CollectedSample, MessageSnapshotError> {
    let table = get_msg_table_name(chat_id);
    let mut collector = SampleCollector::new(budget);

    for shard in shards {
        // Between shards as well as inside one: with a hundred shards, the check
        // that matters is the one after the ninety-ninth.
        cancel.check()?;
        // One more row than the budget allows, so a shard that is too large is
        // detected instead of being silently cut short.
        let limit = i64::try_from(collector.remaining_rows()? + 1)
            .map_err(|_| MessageSnapshotError::BudgetExceeded)?;
        // The existence check and the sample read share one connection, so both
        // see the same view of this shard.
        let conn = open_shard_readonly(&shard.path, &shard.key, cancel)?;
        if !shard_has_chat_table(&conn, &table, cancel)? {
            // This shard simply does not hold the chat: zero rows, no failure.
            continue;
        }
        read_shard_rows(&conn, shard, chat_id, account_dir, limit, &mut collector, cancel)?;
    }

    collector.into_sample(cancel)
}

/// Whether a shard holds the chat at all.
fn shard_has_chat_table(
    conn: &Connection,
    table: &str,
    cancel: &CollectionCancel,
) -> Result<bool, MessageSnapshotError> {
    cancel.check()?;
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1")
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement
        .query([table])
        .map_err(|_| read_outcome(cancel))?;
    match rows.next() {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(_) => Err(read_outcome(cancel)),
    }
}

/// The single statement one shard is sampled with.
///
/// `length()` measures the stored bytes rather than the hex text the client
/// would receive, and the `CASE` keeps an oversized blob from being transferred
/// at all: such a row arrives with its length but without content, which
/// [`decode_message_row`] refuses instead of truncating.
fn sample_sql(chat_id: &str, limit: i64) -> String {
    let table = get_msg_table_name(chat_id);
    let cap = SNAPSHOT_ROW_CONTENT_LIMIT;
    format!(
        "SELECT m.local_id, m.server_id, m.local_type, m.create_time,
                CASE WHEN length(CAST(m.message_content AS BLOB)) <= {cap}
                     THEN hex(m.message_content) ELSE NULL END AS hex_content,
                length(CAST(m.message_content AS BLOB)) AS content_len,
                m.WCDB_CT_message_content AS is_compressed,
                CASE WHEN length(CAST(m.source AS BLOB)) <= {cap}
                     THEN hex(m.source) ELSE NULL END AS hex_source,
                length(CAST(m.source AS BLOB)) AS source_len,
                m.WCDB_CT_source AS source_compressed,
                n.user_name AS sender_name
         FROM \"{table}\" m
         LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
         ORDER BY m.create_time ASC, m.local_id ASC
         LIMIT {limit};"
    )
}

/// Stream one shard's rows into the sample.
///
/// The statement is complete and bounded: `ORDER BY create_time, local_id`
/// gives the shard its own deterministic order, and the `LIMIT` is the row
/// budget left plus one. Rows are stepped one at a time, so the byte budget can
/// stop the read before the rest of the shard is materialised, and a repeated
/// `local_id` inside one shard is reported instead of being deduplicated.
fn read_shard_rows(
    conn: &Connection,
    shard: &MessageShard,
    chat_id: &str,
    account_dir: &str,
    limit: i64,
    collector: &mut SampleCollector,
    cancel: &CollectionCancel,
) -> Result<(), MessageSnapshotError> {
    // Contact display names are resolved after the sample is collected, so the
    // rows are decoded without them first and the byte budget is charged again
    // once they are filled in.
    let contact_names = HashMap::new();
    let context = RowContext {
        chat_id,
        account_dir,
        contact_names: &contact_names,
        require_create_time: true,
        bounded_decode: true,
    };
    let mut statement = conn
        .prepare(&sample_sql(chat_id, limit))
        .map_err(|_| read_outcome(cancel))?;
    let columns: Vec<String> = statement
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let mut rows = statement.query([]).map_err(|_| read_outcome(cancel))?;
    // `local_id` identifies a row only inside its own shard and chat.
    let mut local_ids: HashSet<i64> = HashSet::new();

    while let Some(row) = rows.next().map_err(|_| read_outcome(cancel))? {
        // Per row, not per statement: the statement may be a hundred thousand
        // rows long, and the deadline has to be able to land in the middle of
        // it. The interrupt installed on the connection covers the case where
        // one row's own work is what is slow.
        cancel.check()?;
        let value = row_to_value(row, &columns)?;
        let decoded = decode_message_row(&value, context)?;
        if !local_ids.insert(decoded.message.local_id) {
            return Err(MessageSnapshotError::RowDuplicate);
        }
        let create_time = decoded
            .create_time
            .ok_or(MessageSnapshotError::RowInvalid)?;
        if let Some(sender) = decoded.message.sender.as_ref() {
            collector.senders.insert(sender.clone());
        }
        collector.push(SampledRow {
            create_time,
            local_id: decoded.message.local_id,
            shard_index: shard.index,
            shard_name: shard.name.clone(),
            value: sample_value(&decoded, shard)?,
        })?;
    }
    Ok(())
}

/// One sample row: the legacy message fields, the shard it came from and the
/// stored values the display fields cannot carry.
fn sample_value(decoded: &DecodedRow, shard: &MessageShard) -> Result<Value, MessageSnapshotError> {
    let mut value = serde_json::to_value(&decoded.message)
        .map_err(|_| MessageSnapshotError::RowInvalid)?;
    let object = value
        .as_object_mut()
        .ok_or(MessageSnapshotError::RowInvalid)?;
    object.insert("sourceShard".to_string(), Value::String(shard.name.clone()));
    object.insert("rawType".to_string(), Value::Number(decoded.raw_type.into()));
    object.insert("rawContent".to_string(), Value::String(decoded.raw_content.clone()));
    object.insert(
        "rawContentCompression".to_string(),
        Value::Bool(decoded.raw_content_compressed),
    );
    Ok(value)
}

/// Convert one row into JSON. A column that cannot be read fails the sample
/// instead of leaving a hole in it. The value representation is the shared
/// database helper's, so the snapshot path and the plain read agree.
fn row_to_value(row: &rusqlite::Row<'_>, columns: &[String]) -> Result<Value, MessageSnapshotError> {
    let mut object = serde_json::Map::new();
    for (index, name) in columns.iter().enumerate() {
        let value = row
            .get_ref(index)
            .map_err(|_| MessageSnapshotError::DatabaseRead)?;
        object.insert(name.clone(), value_ref_to_json(value));
    }
    Ok(Value::Object(object))
}

/// Open one shard from an isolated byte snapshot of it.
///
/// The shard file and its `-wal` are copied as source bytes, the committed WAL
/// prefix is merged, and the returned connection reads a private encrypted copy
/// (`immutable=1`). The live source is never opened by SQLite, never locked and
/// never needs its `-shm`; a source that changes mid-capture is refused as
/// [`MessageSnapshotError::SourceChanged`]. Every rowid of the copy is the
/// source's, so `WHERE rowid = ?1` lookups (voice notes, sender joins) keep
/// working.
///
/// The snapshot path returns its own connection because it streams rows and
/// stops early, which a helper that materialises every row cannot do; the key
/// and read-only policy are the same. The connection carries an interrupt so a
/// statement that is already running when the deadline passes is stopped from
/// inside SQLite rather than at the next row.
pub(crate) fn open_shard_readonly(
    path: &Path,
    hex_key: &str,
    cancel: &CollectionCancel,
) -> Result<Connection, MessageSnapshotError> {
    cancel.check()?;
    let conn = wechat_snapshot::open_snapshot_conn(path, hex_key, Some(cancel)).map_err(|error| {
        match error {
            wechat_snapshot::SnapshotError::SourceChanged => MessageSnapshotError::SourceChanged,
            wechat_snapshot::SnapshotError::BudgetExceeded => MessageSnapshotError::BudgetExceeded,
            wechat_snapshot::SnapshotError::NotADatabase => {
                if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                    cancel.record_authentication_failure(name);
                }
                MessageSnapshotError::DatabaseRead
            }
            wechat_snapshot::SnapshotError::CannotOpen
            | wechat_snapshot::SnapshotError::DatabaseRead => read_outcome(cancel),
        }
    })?;
    let guard = cancel.clone();
    // `true` from the progress callback is what interrupts: it is called every
    // `COLLECTION_PROGRESS_STEPS` VDBE instructions, so a statement that is
    // already running when the deadline passes stops from inside SQLite.
    conn.progress_handler(
        COLLECTION_PROGRESS_STEPS as i32,
        Some(move || guard.expired()),
    );
    Ok(conn)
}

fn valid_database_basename(name: &str) -> bool {
    name.ends_with(".db") && name.len() > 3
        && name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

/// Optional display names for the sample's senders, resolved like the message
/// list resolves them.
///
/// Without a `contact.db` key the sample carries no display names. A contact
/// database that cannot be read is a read failure, not an empty name.
///
/// Opened and queried here rather than through the shared helper, because the
/// helper has no deadline: this query is part of the collection the caller is
/// waiting on, so it draws on the same budget as the shards. The sender set is
/// bounded by the sample's row budget, so the statement is bounded with it.
fn resolve_contact_names(
    database_root: &Path,
    keys: &HashMap<String, String>,
    senders: &HashSet<String>,
    cancel: &CollectionCancel,
) -> Result<HashMap<String, String>, MessageSnapshotError> {
    let contact_key = match keys.get("contact.db") {
        Some(key) if !senders.is_empty() => key,
        _ => return Ok(HashMap::new()),
    };
    cancel.check()?;

    let mut senders: Vec<&str> = senders.iter().map(String::as_str).collect();
    senders.sort_unstable();
    let placeholders = senders
        .iter()
        .map(|sender| format!("'{}'", sender.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");

    let conn = open_shard_readonly(
        &database_root.join("contact").join("contact.db"),
        contact_key,
        cancel,
    )?;
    let mut statement = conn
        .prepare(&format!(
            "SELECT username, nick_name, remark FROM contact WHERE username IN ({placeholders});"
        ))
        .map_err(|_| read_outcome(cancel))?;
    let mut rows = statement
        .query([])
        .map_err(|_| read_outcome(cancel))?;

    let mut names = HashMap::new();
    while let Some(row) = rows.next().map_err(|_| read_outcome(cancel))? {
        cancel.check()?;
        let username: Option<String> = row.get(0).map_err(|_| read_outcome(cancel))?;
        let Some(username) = username.filter(|name| !name.is_empty()) else {
            continue;
        };
        let name: Option<String> = row.get(2).ok().flatten();
        let nick_name: Option<String> = row.get(1).ok().flatten();
        let display = name
            .filter(|name| !name.is_empty())
            .or_else(|| nick_name.filter(|name| !name.is_empty()))
            .unwrap_or(username.clone());
        names.insert(username, display);
    }
    Ok(names)
}

/// Fill in the optional `senderName` field and charge it against the byte
/// budget, because the names are part of the sample the caller receives.
///
/// The round is checked per row. This is the last pass over a sample that can
/// hold 50 000 rows, and every row here costs a serialization; without a check
/// inside the loop a round that ended during the shards would still walk the
/// whole tail before anything reported the deadline.
fn apply_contact_names(
    values: &mut [Value],
    names: &HashMap<String, String>,
    budget: &SampleBudget,
    cancel: &CollectionCancel,
) -> Result<(), MessageSnapshotError> {
    let mut bytes = 0usize;
    for value in values.iter_mut() {
        cancel.check()?;
        let display_name = value
            .get("sender")
            .and_then(Value::as_str)
            .and_then(|sender| names.get(sender))
            .map(String::as_str);
        if let (Some(display_name), Some(object)) = (display_name, value.as_object_mut()) {
            object.insert("senderName".to_string(), Value::String(display_name.to_string()));
        }
        bytes += serialized_len(value);
    }
    cancel.check()?;
    if bytes > budget.max_bytes {
        return Err(MessageSnapshotError::BudgetExceeded);
    }
    Ok(())
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use rusqlite::types::Value as SqlValue;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    const TEST_KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    const CHAT_ID: &str = "test-chat";
    const ACCOUNT: &str = "wxid_snapshot_fixture";

    #[test]
    fn only_valid_database_authentication_failures_raise_a_recovery_signal() {
        let cancel = CollectionCancel::within(Duration::from_secs(1));
        cancel.record_authentication_failure("message_12.db");
        assert_eq!(cancel.authentication_failure().as_deref(), Some("message_12.db"));

        let invalid = CollectionCancel::within(Duration::from_secs(1));
        invalid.record_authentication_failure("../message_12.db");
        assert_eq!(invalid.authentication_failure(), None);

        cancel.record_authentication_failure("media_4.db");
        assert_eq!(cancel.authentication_failure().as_deref(), Some("message_12.db"));
    }

    // ── Row fixture ────────────────────────────────────────────────────────

    /// One `Msg_<md5>` row. `local_id` and `create_time` are raw SQL values so a
    /// test can store NULL or text where the real schema stores integers.
    #[derive(Clone)]
    struct Row {
        local_id: SqlValue,
        server_id: i64,
        local_type: i64,
        create_time: SqlValue,
        content: Option<Vec<u8>>,
        compressed: i64,
        source: Option<Vec<u8>>,
        sender: Option<String>,
    }

    impl Row {
        fn new(local_id: i64, create_time: i64, content: &[u8]) -> Self {
            Self {
                local_id: SqlValue::Integer(local_id),
                server_id: 0,
                local_type: 1,
                create_time: SqlValue::Integer(create_time),
                content: Some(content.to_vec()),
                compressed: 0,
                source: None,
                sender: None,
            }
        }

        fn sender(mut self, wxid: &str) -> Self {
            self.sender = Some(wxid.to_string());
            self
        }

        fn local_type(mut self, value: i64) -> Self {
            self.local_type = value;
            self
        }

        fn server_id(mut self, value: i64) -> Self {
            self.server_id = value;
            self
        }

        fn compressed(mut self, flag: i64) -> Self {
            self.compressed = flag;
            self
        }

        fn source(mut self, xml: &[u8]) -> Self {
            self.source = Some(xml.to_vec());
            self
        }

        fn null_local_id(mut self) -> Self {
            self.local_id = SqlValue::Null;
            self
        }

        fn text_local_id(mut self) -> Self {
            self.local_id = SqlValue::Text("not-an-id".to_string());
            self
        }

        fn null_create_time(mut self) -> Self {
            self.create_time = SqlValue::Null;
            self
        }

        fn no_content(mut self) -> Self {
            self.content = None;
            self
        }
    }

    // ── Encrypted database fixtures ────────────────────────────────────────

    fn chat_table_ddl() -> String {
        format!(
            "CREATE TABLE \"{table}\" (
                 local_id INTEGER, server_id INTEGER, local_type INTEGER, create_time INTEGER,
                 sort_seq INTEGER,
                 message_content BLOB, WCDB_CT_message_content INTEGER,
                 source BLOB, WCDB_CT_source INTEGER, real_sender_id INTEGER
             );",
            table = get_msg_table_name(CHAT_ID)
        )
    }

    const NAME2ID_DDL: &str = "CREATE TABLE Name2Id (user_name TEXT);";

    /// Resolve a sender to its `Name2Id` row, the way the reader's join does.
    fn sender_id(conn: &Connection, wxid: &str) -> i64 {
        if let Ok(id) = conn.query_row("SELECT rowid FROM Name2Id WHERE user_name = ?1", [wxid], |row| {
            row.get::<_, i64>(0)
        }) {
            return id;
        }
        conn.execute("INSERT INTO Name2Id (user_name) VALUES (?1)", [wxid])
            .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_chat_row(conn: &Connection, row: &Row) {
        let real_sender_id = row.sender.as_deref().map(|wxid| sender_id(conn, wxid));
        conn.execute(
            &format!(
                "INSERT INTO \"{table}\"
                 (local_id, server_id, local_type, create_time, message_content,
                  WCDB_CT_message_content, source, WCDB_CT_source, real_sender_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                table = get_msg_table_name(CHAT_ID)
            ),
            rusqlite::params![
                row.local_id,
                row.server_id,
                row.local_type,
                row.create_time,
                row.content,
                row.compressed,
                row.source,
                0,
                real_sender_id,
            ],
        )
        .unwrap();
    }

    #[test]
    fn native_server_id_locates_the_matching_shard_not_a_duplicate_local_id() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(7, 10, b"wrong").server_id(11)]);
        root.add_chat_shard("message_1.db", &[Row::new(7, 20, b"right").server_id(9_168_265_261_376_983_777)]);
        let keys = root.keys(&["message_0.db", "message_1.db"]);
        let anchor = locate_message_by_server_id(
            &root.path, ACCOUNT, &keys, 9_168_265_261_376_983_777,
            Some(CHAT_ID), &[CHAT_ID.to_string()], &CollectionCancel::within(Duration::from_secs(2)),
        ).unwrap();
        assert_eq!(anchor.source_shard, "message_1.db");
        assert_eq!(anchor.local_id, 7);
        assert_eq!(anchor.message.server_id.as_deref(), Some("9168265261376983777"));
        assert_eq!(anchor.raw_content, "7269676874");
        let unscoped = locate_message_by_server_id(
            &root.path, ACCOUNT, &keys, 9_168_265_261_376_983_777,
            None, &[CHAT_ID.to_string()], &CollectionCancel::within(Duration::from_secs(2)),
        ).unwrap();
        assert_eq!(unscoped.message.chat_username, CHAT_ID);
        assert_eq!(unscoped.source_shard, "message_1.db");
    }

    #[test]
    fn native_server_id_ambiguity_and_shard_read_failures_are_not_not_found() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(7, 10, b"same").server_id(77)]);
        root.add_chat_shard("message_1.db", &[Row::new(8, 11, b"same").server_id(77)]);
        let keys = root.keys(&["message_0.db", "message_1.db"]);
        let cancel = CollectionCancel::within(Duration::from_secs(2));
        assert_eq!(locate_message_by_server_id(&root.path, ACCOUNT, &keys, 77, Some(CHAT_ID), &[CHAT_ID.to_string()], &cancel).unwrap_err(), MessageLocateError::Ambiguous);

        let unreadable = Root::empty();
        unreadable.add_chat_shard("message_0.db", &[Row::new(7, 10, b"present").server_id(77)]);
        unreadable.add_unencrypted_shard("message_1.db");
        let keys = unreadable.keys(&["message_0.db", "message_1.db"]);
        assert!(matches!(locate_message_by_server_id(&unreadable.path, ACCOUNT, &keys, 404, Some(CHAT_ID), &[CHAT_ID.to_string()], &CollectionCancel::within(Duration::from_secs(2))), Err(MessageLocateError::Snapshot(_))));
    }

    /// A `db_storage` root. The read path takes the directory the identity
    /// module resolved, so a temp tree is enough for every fixture.
    struct Root {
        _dir: tempfile::TempDir,
        path: PathBuf,
    }

    impl Root {
        fn empty() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_path_buf();
            Self { _dir: dir, path }
        }

        fn shard_path(&self, name: &str) -> PathBuf {
            self.path.join("message").join(name)
        }

        fn write_shard(&self, name: &str, build: impl FnOnce(&Connection)) {
            let path = self.shard_path(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;"
            ))
            .unwrap();
            build(&conn);
        }

        /// A shard that holds this chat and the `Name2Id` mapping.
        fn add_chat_shard(&self, name: &str, rows: &[Row]) {
            self.write_shard(name, |conn| {
                conn.execute_batch(&chat_table_ddl()).unwrap();
                conn.execute_batch(NAME2ID_DDL).unwrap();
                for row in rows {
                    insert_chat_row(conn, row);
                }
            });
        }

        /// A readable shard that simply does not hold this chat.
        fn add_shard_without_chat(&self, name: &str) {
            self.write_shard(name, |conn| {
                conn.execute_batch(NAME2ID_DDL).unwrap();
            });
        }

        /// A readable shard that cannot be decrypted with the sample key.
        fn add_unencrypted_shard(&self, name: &str) {
            let path = self.shard_path(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&chat_table_ddl()).unwrap();
            conn.execute_batch(NAME2ID_DDL).unwrap();
            insert_chat_row(&conn, &Row::new(1, 1_000, b"plaintext"));
        }

        fn add_garbage_shard(&self, name: &str) {
            let path = self.shard_path(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"this is not a sqlite database").unwrap();
        }

        /// A `message_N.db` name that is a directory rather than a shard file.
        fn add_directory_named_shard(&self, name: &str) {
            std::fs::create_dir_all(self.shard_path(name)).unwrap();
        }

        fn keys(&self, names: &[&str]) -> HashMap<String, String> {
            names
                .iter()
                .map(|name| (name.to_string(), TEST_KEY.to_string()))
                .collect()
        }

        fn shards(&self) -> Vec<MessageShard> {
            resolve_message_shards(&self.path, &self.shard_names(), &self.keys_all())
                .expect("shards resolve")
        }

        fn shard_names(&self) -> Vec<String> {
            discover_message_shards(&self.path).expect("shards are discoverable")
        }

        fn keys_all(&self) -> HashMap<String, String> {
            self.shard_names()
                .iter()
                .map(|name| (name.clone(), TEST_KEY.to_string()))
                .collect()
        }
    }

    /// A deadline no test collection can reach, so an existing test fails for its
    /// own reason rather than because the wall clock ran out.
    fn generous() -> CollectionCancel {
        CollectionCancel::within(Duration::from_secs(300))
    }

    fn collect(root: &Root, keys: &HashMap<String, String>) -> Result<Vec<Value>, MessageSnapshotError> {
        collect_message_snapshot(&root.path, ACCOUNT, CHAT_ID, keys)
    }

    // ── Ordering helpers ───────────────────────────────────────────────────

    /// `(timestamp, localId, sourceShard, content)`: the sample's fixed order
    /// plus the values a reader compares across shards.
    fn order(rows: &[Value]) -> Vec<(String, i64, String, String)> {
        rows.iter()
            .map(|row| {
                (
                    row["timestamp"].as_str().unwrap().to_string(),
                    row["localId"].as_i64().unwrap(),
                    row["sourceShard"].as_str().unwrap().to_string(),
                    row["content"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    fn text(rows: &[Value]) -> Vec<String> {
        order(rows)
            .into_iter()
            .map(|(_, _, shard, content)| format!("{shard}:{content}"))
            .collect()
    }

    fn hex_upper(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02X}")).collect()
    }

    // ── Cross-shard completeness ───────────────────────────────────────────

    #[test]
    fn one_local_id_present_in_two_shards_is_kept_once_per_shard() {
        let root = Root::empty();
        root.add_chat_shard(
            "message_0.db",
            &[
                Row::new(1, 1_000, b"a-1").sender("alice"),
                Row::new(2, 2_000, b"a-2").sender("alice"),
            ],
        );
        root.add_chat_shard(
            "message_1.db",
            &[
                Row::new(1, 1_000, b"b-1").sender("alice"),
                Row::new(3, 3_000, b"b-2").sender("bob"),
            ],
        );

        let sample = collect(&root, &root.keys(&["message_0.db", "message_1.db"])).unwrap();

        assert_eq!(
            text(&sample),
            vec![
                "message_0.db:a-1",
                "message_1.db:b-1",
                "message_0.db:a-2",
                "message_1.db:b-2"
            ],
            "a repeated local_id must not merge rows across shards"
        );
    }

    #[test]
    fn a_server_id_repeated_in_two_shards_does_not_merge_either() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(10, 1_000, b"first").server_id(7_777)]);
        root.add_chat_shard("message_1.db", &[Row::new(20, 1_000, b"second").server_id(7_777)]);

        let sample = collect(&root, &root.keys(&["message_0.db", "message_1.db"])).unwrap();

        assert_eq!(text(&sample), vec!["message_0.db:first", "message_1.db:second"]);
    }

    #[test]
    fn rows_sharing_a_second_are_ordered_by_local_id_not_by_insertion() {
        let root = Root::empty();
        root.add_chat_shard(
            "message_0.db",
            &[
                Row::new(9, 1_000, b"nine"),
                Row::new(2, 1_000, b"two"),
                Row::new(5, 1_000, b"five"),
            ],
        );

        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();

        assert_eq!(
            text(&sample),
            vec!["message_0.db:two", "message_0.db:five", "message_0.db:nine"]
        );
    }

    #[test]
    fn shards_are_tie_broken_in_shard_number_order() {
        let root = Root::empty();
        root.add_chat_shard("message_10.db", &[Row::new(1, 1_000, b"ten")]);
        root.add_chat_shard("message_2.db", &[Row::new(1, 1_000, b"two")]);

        let sample = collect(&root, &root.keys(&["message_10.db", "message_2.db"])).unwrap();

        assert_eq!(text(&sample), vec!["message_2.db:two", "message_10.db:ten"]);
    }

    #[test]
    fn a_shard_without_this_chat_contributes_no_rows_and_no_error() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"only")]);
        root.add_shard_without_chat("message_1.db");

        let sample = collect(&root, &root.keys(&["message_0.db", "message_1.db"])).unwrap();

        assert_eq!(text(&sample), vec!["message_0.db:only"]);
    }

    #[test]
    fn auxiliary_databases_in_the_message_directory_are_not_shards() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"only")]);
        for name in ["message_fts.db", "message_resource.db", "message_abc.db", "message_.db"] {
            root.write_shard(name, |conn| {
                conn.execute_batch(NAME2ID_DDL).unwrap();
            });
        }

        assert_eq!(root.shard_names(), vec!["message_0.db".to_string()]);
        assert_eq!(text(&collect(&root, &root.keys(&["message_0.db"])).unwrap()), vec!["message_0.db:only"]);
    }

    // ── Source discovery ───────────────────────────────────────────────────

    #[test]
    fn a_root_without_a_message_directory_is_not_an_empty_history() {
        let root = Root::empty();
        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::SourceUnavailable
        );
    }

    #[test]
    fn a_root_without_any_message_shard_is_not_an_empty_history() {
        let root = Root::empty();
        root.write_shard("message_fts.db", |conn| {
            conn.execute_batch(NAME2ID_DDL).unwrap();
        });

        assert_eq!(
            collect(&root, &root.keys(&[])).unwrap_err(),
            MessageSnapshotError::SourceUnavailable
        );
    }

    #[test]
    fn a_message_shard_name_that_is_not_a_file_is_reported() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"only")]);
        root.add_directory_named_shard("message_1.db");

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db", "message_1.db"])).unwrap_err(),
            MessageSnapshotError::SourceUnavailable
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_undecodable_directory_entry_is_reported() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"only")]);
        let invalid = root
            .path
            .join("message")
            .join(OsString::from_vec(b"message_\xff.db".to_vec()));
        std::fs::write(&invalid, b"").unwrap();

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::SourceUnavailable
        );
    }

    #[test]
    fn a_discovered_shard_without_a_key_fails_the_whole_sample() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"first")]);
        root.add_chat_shard("message_1.db", &[Row::new(2, 2_000, b"second")]);

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::KeysUnavailable
        );
    }

    #[test]
    fn a_shard_set_that_changes_while_collecting_is_reported() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"only")]);

        let seen = std::cell::Cell::new(0usize);
        let growing = |_: &Path| -> Result<Vec<String>, MessageSnapshotError> {
            let call = seen.get() + 1;
            seen.set(call);
            Ok(if call == 1 {
                vec!["message_0.db".to_string()]
            } else {
                vec!["message_0.db".to_string(), "message_1.db".to_string()]
            })
        };

        assert_eq!(
            collect_fixed_sample(
                &root.path,
                ACCOUNT,
                CHAT_ID,
                &root.keys(&["message_0.db"]),
                &growing,
                &generous(),
            )
            .unwrap_err(),
            MessageSnapshotError::SourceChanged
        );
        assert_eq!(seen.get(), 2, "the shard set is discovered before and after the reads");
    }

    #[test]
    fn a_stable_shard_set_is_collected_twice_identically() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"only")]);
        root.add_chat_shard("message_1.db", &[Row::new(2, 2_000, b"other")]);
        let keys = root.keys(&["message_0.db", "message_1.db"]);

        let first = collect_message_snapshot(&root.path, ACCOUNT, CHAT_ID, &keys).unwrap();
        let second = collect_message_snapshot(&root.path, ACCOUNT, CHAT_ID, &keys).unwrap();

        assert_eq!(first, second);
    }

    // ── Per-shard read failures ────────────────────────────────────────────

    #[test]
    fn a_corrupt_shard_fails_the_whole_sample() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"first")]);
        root.add_garbage_shard("message_1.db");

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db", "message_1.db"])).unwrap_err(),
            MessageSnapshotError::DatabaseRead
        );
    }

    #[test]
    fn a_shard_that_cannot_be_decrypted_fails_the_whole_sample() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"first")]);
        root.add_unencrypted_shard("message_1.db");

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db", "message_1.db"])).unwrap_err(),
            MessageSnapshotError::DatabaseRead
        );
    }

    #[test]
    fn a_shard_file_that_disappears_after_discovery_is_a_source_change() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"first")]);
        root.add_chat_shard("message_1.db", &[Row::new(2, 2_000, b"second")]);
        let seen = std::cell::Cell::new(0usize);
        let vanishing = |_: &Path| -> Result<Vec<String>, MessageSnapshotError> {
            let call = seen.get() + 1;
            seen.set(call);
            if call == 2 {
                // The shard disappears while the sample is being read.
                std::fs::remove_file(root.shard_path("message_1.db")).ok();
                return Ok(vec!["message_0.db".to_string()]);
            }
            Ok(vec!["message_0.db".to_string(), "message_1.db".to_string()])
        };

        assert_eq!(
            collect_fixed_sample(
                &root.path,
                ACCOUNT,
                CHAT_ID,
                &root.keys(&["message_0.db", "message_1.db"]),
                &vanishing,
                &generous(),
            )
            .unwrap_err(),
            MessageSnapshotError::SourceChanged
        );
    }

    #[test]
    fn a_chat_table_without_the_sender_mapping_fails_instead_of_returning_partial_rows() {
        let root = Root::empty();
        root.write_shard("message_0.db", |conn| {
            conn.execute_batch(&chat_table_ddl()).unwrap();
            insert_chat_row(conn, &Row::new(1, 1_000, b"only"));
        });

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::DatabaseRead
        );
    }

    // ── Row level strictness ───────────────────────────────────────────────

    #[test]
    fn a_row_without_a_decodable_local_id_is_reported_not_skipped() {
        for (label, row) in [
            ("null", Row::new(1, 1_000, b"first").null_local_id()),
            ("text", Row::new(1, 1_000, b"first").text_local_id()),
        ] {
            let root = Root::empty();
            root.add_chat_shard("message_0.db", &[Row::new(2, 2_000, b"second"), row]);

            assert_eq!(
                collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
                MessageSnapshotError::RowInvalid,
                "{label} local_id must fail the sample"
            );
        }
    }

    #[test]
    fn a_row_without_a_decodable_create_time_is_reported_not_skipped() {
        let root = Root::empty();
        root.add_chat_shard(
            "message_0.db",
            &[Row::new(1, 1_000, b"first"), Row::new(2, 2_000, b"second").null_create_time()],
        );

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::RowInvalid
        );
    }

    #[test]
    fn a_repeated_local_id_inside_one_shard_is_reported() {
        let root = Root::empty();
        root.add_chat_shard(
            "message_0.db",
            &[Row::new(7, 1_000, b"first"), Row::new(7, 2_000, b"duplicate")],
        );

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::RowDuplicate
        );
    }

    #[test]
    fn a_row_whose_content_exceeds_the_row_limit_is_reported_and_never_truncated() {
        let root = Root::empty();
        let huge = vec![b'x'; SNAPSHOT_ROW_CONTENT_LIMIT + 1];
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"small"), Row::new(2, 2_000, &huge)]);

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::RowTooLarge
        );
    }

    #[test]
    fn a_row_exactly_at_the_row_limit_is_collected() {
        let root = Root::empty();
        let at_limit = vec![b'x'; SNAPSHOT_ROW_CONTENT_LIMIT];
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, &at_limit)]);

        assert_eq!(collect(&root, &root.keys(&["message_0.db"])).unwrap().len(), 1);
    }

    #[test]
    fn an_oversized_source_blob_is_reported_instead_of_an_unverified_mention_flag() {
        let root = Root::empty();
        let huge = vec![b'x'; SNAPSHOT_ROW_CONTENT_LIMIT + 1];
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"body").source(&huge)]);

        assert_eq!(
            collect(&root, &root.keys(&["message_0.db"])).unwrap_err(),
            MessageSnapshotError::RowTooLarge
        );
    }

    // ── Budgets ────────────────────────────────────────────────────────────

    #[test]
    fn the_default_budget_is_the_documented_one() {
        let budget = SampleBudget::default();
        assert_eq!(budget.max_rows, 50_000);
        assert_eq!(budget.max_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn a_sample_at_the_row_budget_is_collected_and_one_more_row_is_rejected() {
        let root = Root::empty();
        let rows: Vec<Row> = (1..=4).map(|id| Row::new(id, id * 1_000, b"row")).collect();
        root.add_chat_shard("message_0.db", &rows);

        let at_limit = collect_shard_rows(
            &root.shards(),
            ACCOUNT,
            CHAT_ID,
            SampleBudget { max_rows: 4, max_bytes: 16 * 1024 * 1024 },
            &generous(),
        )
        .unwrap();
        assert_eq!(at_limit.rows.len(), 4);

        assert_eq!(
            collect_shard_rows(
                &root.shards(),
                ACCOUNT,
                CHAT_ID,
                SampleBudget { max_rows: 3, max_bytes: 16 * 1024 * 1024 },
                &generous(),
            )
            .unwrap_err(),
            MessageSnapshotError::BudgetExceeded
        );
    }

    #[test]
    fn the_row_budget_counts_every_shard_together() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"a"), Row::new(2, 2_000, b"b")]);
        root.add_chat_shard("message_1.db", &[Row::new(3, 3_000, b"c"), Row::new(4, 4_000, b"d")]);

        let budget = SampleBudget { max_rows: 4, max_bytes: 16 * 1024 * 1024 };
        assert_eq!(collect_shard_rows(&root.shards(), ACCOUNT, CHAT_ID, budget, &generous()).unwrap().rows.len(), 4);

        let tighter = SampleBudget { max_rows: 3, max_bytes: 16 * 1024 * 1024 };
        assert_eq!(
            collect_shard_rows(&root.shards(), ACCOUNT, CHAT_ID, tighter, &generous()).unwrap_err(),
            MessageSnapshotError::BudgetExceeded
        );
    }

    #[test]
    fn the_byte_budget_is_measured_on_the_serialized_sample() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"a"), Row::new(2, 2_000, b"b")]);

        let full = collect_shard_rows(
            &root.shards(),
            ACCOUNT,
            CHAT_ID,
            SampleBudget { max_rows: 50, max_bytes: 16 * 1024 * 1024 },
            &generous(),
        )
        .unwrap();
        let measured: usize = full.rows.iter().map(|row| serialized_len(&row.value)).sum();
        assert!(measured > 0);

        let exact = SampleBudget { max_rows: 50, max_bytes: measured };
        assert_eq!(
            collect_shard_rows(&root.shards(), ACCOUNT, CHAT_ID, exact, &generous()).unwrap().rows.len(),
            2,
            "a sample of exactly the budget is collected"
        );

        let one_byte_short = SampleBudget { max_rows: 50, max_bytes: measured - 1 };
        assert_eq!(
            collect_shard_rows(&root.shards(), ACCOUNT, CHAT_ID, one_byte_short, &generous()).unwrap_err(),
            MessageSnapshotError::BudgetExceeded
        );
    }

    #[test]
    fn the_collection_stops_before_reading_a_row_it_cannot_budget() {
        let root = Root::empty();
        let huge = vec![b'x'; SNAPSHOT_ROW_CONTENT_LIMIT + 1];
        root.add_chat_shard(
            "message_0.db",
            &[
                Row::new(1, 1_000, b"first"),
                Row::new(2, 2_000, b"second"),
                Row::new(3, 3_000, &huge),
            ],
        );

        assert_eq!(
            collect_shard_rows(
                &root.shards(),
                ACCOUNT,
                CHAT_ID,
                SampleBudget { max_rows: 50, max_bytes: 1 },
                &generous(),
            )
            .unwrap_err(),
            MessageSnapshotError::BudgetExceeded,
            "the budget is spent on the first row, so the oversized row is never read"
        );
    }

    // ── Field fidelity ─────────────────────────────────────────────────────

    #[test]
    fn every_row_keeps_the_legacy_message_fields() {
        let root = Root::empty();
        root.add_chat_shard(
            "message_0.db",
            &[Row::new(11, 1_700_000_000, b"hello")
                .server_id(99)
                .local_type(1)
                .sender("alice")],
        );

        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();
        let row = &sample[0];

        assert_eq!(row["localId"], 11);
        assert_eq!(row["serverId"], 99);
        assert_eq!(row["chatId"], CHAT_ID);
        assert_eq!(row["sender"], "alice");
        assert_eq!(row["type"], 1);
        assert_eq!(row["content"], "hello");
        assert_eq!(row["timestamp"], "2023-11-14T22:13:20+00:00");
        assert!(row.get("isMentioned").is_none(), "a 1:1 chat has no mention flag");
        // `isSelf` is the legacy account-directory prefix heuristic, not a
        // verified account identity.
        assert_eq!(row["isSelf"], Value::Bool(false));
    }

    #[test]
    fn is_self_stays_the_account_directory_prefix_heuristic() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"mine").sender(ACCOUNT)]);

        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();

        assert_eq!(sample[0]["isSelf"], Value::Bool(true));
    }

    #[test]
    fn raw_type_keeps_the_full_64_bit_value_the_legacy_type_truncates() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"body").local_type(2_147_483_649)]);

        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();
        let row = &sample[0];

        assert_eq!(row["type"], -2_147_483_647, "the legacy field keeps its 32 bit value");
        assert_eq!(
            row["rawType"],
            2_147_483_649i64,
            "the sample also carries the 64 bit value"
        );
    }

    #[test]
    fn raw_content_keeps_bytes_the_display_field_cannot_represent() {
        let root = Root::empty();
        let binary = vec![0xFFu8, 0x00, b'<', b'm', b's', b'>', 0x80];
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, &binary)]);

        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();
        let row = &sample[0];

        assert_eq!(row["rawContent"], "FF003C6D733E80");
        assert_eq!(row["rawContentCompression"], false);
        assert!(
            row["content"].as_str().unwrap().contains('\u{FFFD}'),
            "the display field stays the legacy lossy string"
        );
    }

    #[test]
    fn a_compressed_row_keeps_its_stored_bytes_and_the_compression_flag() {
        let root = Root::empty();
        let stored = zstd::encode_all(b"<msg><title>kept</title></msg>".as_slice(), 3).unwrap();
        root.add_chat_shard(
            "message_0.db",
            &[Row::new(1, 1_000, &stored).local_type(49).compressed(1)],
        );

        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();
        let row = &sample[0];

        assert_eq!(row["rawContentCompression"], true);
        assert_eq!(row["rawContent"], hex_upper(&stored));
        // The display field keeps the legacy type-49 cleaning, so the stored
        // document is only recoverable from `rawContent`.
        assert_eq!(row["content"], "kept");
    }

    #[test]
    fn message_v2_projection_preserves_v1_snapshot_bytes_and_compression_flag() {
        let plain = "<msg><appmsg><type>19</type><title>plain</title></appmsg></msg>";
        let zipped = "<msg><appmsg><type>19</type><title>zstd</title></appmsg></msg>";
        let compressed = zstd::encode_all(zipped.as_bytes(), 3).unwrap();
        let root = Root::empty();
        root.add_chat_shard(
            "message_0.db",
            &[
                Row::new(1, 1_000, plain.as_bytes()).local_type(49),
                Row::new(2, 2_000, &compressed).local_type(49).compressed(1),
            ],
        );
        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();
        assert_eq!(sample.len(), 2);

        for (index, expected_body, expected_compressed) in [
            (0, plain, false),
            (1, zipped, true),
        ] {
            let v1_row = &sample[index];
            let projected = message_v2_from_snapshot(v1_row).unwrap();
            let v2_wire = serde_json::to_value(&projected).unwrap();
            assert_eq!(v1_row["rawContentCompression"], expected_compressed);
            assert_eq!(v2_wire["raw_content_compression"], expected_compressed);
            assert_eq!(projected.raw_content.as_deref(), v1_row["rawContent"].as_str());
            assert_eq!(v2_wire["content"], v1_row["content"]);

            let raw_hex = projected.raw_content.as_deref().unwrap();
            assert_eq!(
                decode_stored_content_for_record(raw_hex, v2_wire["raw_content_compression"].as_bool().unwrap()).unwrap(),
                expected_body,
            );
        }
    }

    #[test]
    fn a_null_content_is_reported_as_no_raw_bytes() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"").no_content()]);

        let sample = collect(&root, &root.keys(&["message_0.db"])).unwrap();

        assert_eq!(sample[0]["rawContent"], "");
        assert_eq!(sample[0]["content"], "");
    }

    #[test]
    fn a_group_row_keeps_the_mention_flag_from_the_source_blob() {
        let root = Root::empty();
        let chat_id = "group@chatroom";
        let source = format!("<msgsource><atuserlist>{ACCOUNT}</atuserlist></msgsource>");
        root.write_shard("message_0.db", |conn| {
            conn.execute_batch(&format!(
                "CREATE TABLE \"{table}\" (
                     local_id INTEGER, server_id INTEGER, local_type INTEGER, create_time INTEGER,
                     message_content BLOB, WCDB_CT_message_content INTEGER,
                     source BLOB, WCDB_CT_source INTEGER, real_sender_id INTEGER
                 );
                 CREATE TABLE Name2Id (user_name TEXT);",
                table = get_msg_table_name(chat_id)
            ))
            .unwrap();
            conn.execute(
                &format!(
                    "INSERT INTO \"{table}\" (local_id, create_time, message_content, source)
                     VALUES (1, 1000, x'6869', ?1)",
                    table = get_msg_table_name(chat_id)
                ),
                [source.as_bytes()],
            )
            .unwrap();
        });

        let keys = root.keys(&["message_0.db"]);
        let sample = collect_message_snapshot(&root.path, ACCOUNT, chat_id, &keys).unwrap();

        assert_eq!(sample[0]["isMentioned"], Value::Bool(true));
    }

    #[test]
    fn the_sample_is_strict_where_the_legacy_list_is_tolerant() {
        let names = HashMap::from([("alice".to_string(), "Alice A".to_string())]);
        let strict = RowContext {
            chat_id: CHAT_ID,
            account_dir: ACCOUNT,
            contact_names: &names,
            require_create_time: true,
            bounded_decode: true,
        };
        let row = json!({
            "local_id": 7, "server_id": 3, "local_type": 1, "create_time": 1_000,
            "hex_content": "6869", "is_compressed": 0, "hex_source": "",
            "source_compressed": 0, "sender_name": "alice",
        });
        assert!(decode_message_row(&row, strict).is_ok());

        let without_time = json!({
            "local_id": 7, "server_id": 3, "local_type": 1,
            "hex_content": "6869", "is_compressed": 0, "hex_source": "",
            "source_compressed": 0, "sender_name": "alice",
        });
        assert_eq!(decode_message_row(&without_time, strict).unwrap_err(), RowError::Invalid);

        let legacy = RowContext { require_create_time: false, ..strict };
        let decoded = decode_message_row(&without_time, legacy).expect("legacy tolerates it");
        assert_eq!(decoded.message.timestamp, "");
        assert_eq!(decoded.message.content, "hi");
    }

    // ── Contact display names ──────────────────────────────────────────────

    #[test]
    fn contact_display_names_are_optional() {
        let senders: HashSet<String> = HashSet::from(["alice".to_string()]);
        let root = Root::empty();

        assert!(resolve_contact_names(&root.path, &HashMap::new(), &senders, &generous())
            .unwrap()
            .is_empty());
        assert!(resolve_contact_names(&root.path, &HashMap::new(), &HashSet::new(), &generous())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn contact_display_names_are_added_without_growing_past_the_budget() {
        let mut values = vec![
            json!({ "localId": 1, "sender": "alice" }),
            json!({ "localId": 2, "sender": "carol" }),
        ];
        let names = HashMap::from([("alice".to_string(), "Alice A".to_string())]);
        let budget = SampleBudget { max_rows: 10, max_bytes: 16 * 1024 * 1024 };

        apply_contact_names(&mut values, &names, &budget, &generous()).unwrap();

        assert_eq!(values[0]["senderName"], "Alice A");
        assert!(values[1].get("senderName").is_none(), "an unknown sender keeps the field absent");

        let tight = SampleBudget { max_rows: 10, max_bytes: 1 };
        let mut values = vec![json!({ "localId": 1, "sender": "alice" })];
        assert_eq!(
            apply_contact_names(&mut values, &names, &tight, &generous()).unwrap_err(),
            MessageSnapshotError::BudgetExceeded
        );
    }

    // ── Stopping at the deadline ────────────────────────────────────────────

    fn ordered_collector(rows: u64, budget: SampleBudget) -> SampleCollector {
        let mut collector = SampleCollector::new(budget);
        for local_id in (0..rows).rev() {
            collector
                .push(SampledRow {
                    create_time: local_id as i64,
                    local_id: local_id as i64,
                    shard_index: 0,
                    shard_name: "message_0.db".to_string(),
                    value: json!({ "localId": local_id }),
                })
                .expect("a row of this sample fits the budget");
        }
        collector
    }

    #[test]
    fn a_sample_that_ran_out_of_time_while_it_was_ordered_is_not_returned() {
        // A full-size tail — 60 000 rows is the production maximum, so this is
        // the pass a real first page does. The round is still live when the
        // ordering starts and reaches its deadline in the middle of the pass,
        // which is exactly the case the check *after* the sort exists for. The
        // moment is driven by the round's clock, not the wall clock, so the test
        // does not depend on how long ordering rows happens to take in this
        // build.
        let big = SampleBudget { max_rows: 60_000, max_bytes: SNAPSHOT_MAX_BYTES };
        let cancel = CollectionCancel::within(Duration::from_secs(300));
        let mut collector = ordered_collector(60_000, big);
        let mid_pass = cancel.clone();
        collector.after_sort = Some(Box::new(move || mid_pass.reach_deadline_for_test()));
        // Live at entry: the refusal below can only come from the check that
        // follows the ordering, not from the one before it.
        assert!(!cancel.expired(), "the round has to start with time left");
        assert_eq!(
            collector.into_sample(&cancel).unwrap_err(),
            MessageSnapshotError::DeadlineExceeded,
            "an expired round must not hand back an ordered sample"
        );

        // The other end of the same pair: a round that was already over when the
        // ordering was asked for refuses before it sorts anything, so the check
        // that catches a round ending *inside* the pass is not the only one.
        assert_eq!(
            ordered_collector(3, SampleBudget::default())
                .into_sample(&CollectionCancel::within(Duration::from_millis(0)))
                .unwrap_err(),
            MessageSnapshotError::DeadlineExceeded
        );

        // With time to spare the same rows come back in sample order.
        let sample = ordered_collector(5, SampleBudget::default())
            .into_sample(&generous())
            .expect("ordered");
        assert_eq!(
            sample.rows.iter().map(|row| row.local_id).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn contact_names_are_not_attached_to_a_sample_that_ran_out_of_time() {
        let names = HashMap::from([
            ("alice".to_string(), "Alice A".to_string()),
            ("carol".to_string(), "Carol C".to_string()),
        ]);
        let budget = SampleBudget::default();

        let mut values = vec![json!({ "localId": 1, "sender": "alice" })];
        assert_eq!(
            apply_contact_names(
                &mut values,
                &names,
                &budget,
                &CollectionCancel::within(Duration::from_millis(0))
            )
            .unwrap_err(),
            MessageSnapshotError::DeadlineExceeded
        );
        assert!(
            values[0].get("senderName").is_none(),
            "a refused tail must leave the sample as the shards returned it"
        );

        // And the check is per row rather than once at the end: a long tail is
        // cut short instead of being finished.
        let mut values: Vec<Value> = (0..60_000i64)
            .map(|local_id| json!({ "localId": local_id, "sender": "alice" }))
            .collect();
        assert_eq!(
            apply_contact_names(
                &mut values,
                &names,
                &budget,
                &CollectionCancel::within(Duration::from_millis(15))
            )
            .unwrap_err(),
            MessageSnapshotError::DeadlineExceeded
        );
    }

    // ── Diagnostics ────────────────────────────────────────────────────────

    #[test]
    fn snapshot_error_codes_are_stable_and_carry_no_content() {
        assert_eq!(
            [
                MessageSnapshotError::KeysUnavailable,
                MessageSnapshotError::SourceUnavailable,
                MessageSnapshotError::SourceChanged,
                MessageSnapshotError::DatabaseRead,
                MessageSnapshotError::BudgetExceeded,
                MessageSnapshotError::RowInvalid,
                MessageSnapshotError::RowDuplicate,
                MessageSnapshotError::RowTooLarge,
            ]
            .map(MessageSnapshotError::code),
            [
                "WECHAT_KEYS_UNAVAILABLE",
                "WECHAT_MESSAGE_SOURCE_UNAVAILABLE",
                "WECHAT_MESSAGE_SOURCE_CHANGED",
                "WECHAT_DB_READ_FAILED",
                "WECHAT_MESSAGE_BUDGET_EXCEEDED",
                "WECHAT_MESSAGE_ROW_INVALID",
                "WECHAT_MESSAGE_ROW_DUPLICATE",
                "WECHAT_MESSAGE_ROW_TOO_LARGE",
            ]
        );

        let root = Root::empty();
        let error = collect(&root, &root.keys(&[])).unwrap_err();
        let rendered = format!("{error} {error:?}");
        for leak in ["/", ".db", "SELECT", "xwechat", "key", "fixture"] {
            assert!(!rendered.contains(leak), "{leak} leaked in {rendered}");
        }
    }

    #[test]
    fn shard_names_are_numbered_shards_in_shard_order() {
        assert_eq!(message_shard_index("message_0.db"), Some(0));
        assert_eq!(message_shard_index("message_10.db"), Some(10));
        assert_eq!(message_shard_index("message_007.db"), Some(7));
        for name in [
            "message_fts.db",
            "message_resource.db",
            "message_.db",
            "message_abc.db",
            "message_+1.db",
            "message_ 1.db",
            "message_1.db-wal",
            "message_99999999999999999999.db",
        ] {
            assert_eq!(message_shard_index(name), None, "{name} is not a numbered shard");
        }
    }

    // ── Write-ahead log ────────────────────────────────────────────────────

    /// A real SQLCipher shard whose newest commit is provably still in the
    /// `-wal` file: `wal_autocheckpoint = 0` stops commit-time checkpoints and
    /// the writer stays open, so SQLite never sees the "last connection closed"
    /// trigger. The `-wal` size is the guard.
    struct JournalFixture {
        _dir: tempfile::TempDir,
        root: PathBuf,
        writer: Connection,
    }

    impl JournalFixture {
        fn new(journal_mode: &str) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().to_path_buf();
            let path = root.join("message").join("message_0.db");
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let writer = Connection::open(&path).unwrap();
            writer
                .execute_batch(&format!(
                    "PRAGMA key = \"x'{TEST_KEY}'\"; PRAGMA cipher_compatibility = 4;
                     PRAGMA journal_mode = {journal_mode}; PRAGMA wal_autocheckpoint = 0;
                     {chat}{names}",
                    journal_mode = journal_mode,
                    chat = chat_table_ddl(),
                    names = NAME2ID_DDL
                ))
                .unwrap();
            insert_chat_row(&writer, &Row::new(1, 1_000, b"checkpointed"));
            if journal_mode == "WAL" {
                // Fold the first commit into the main file so the fixture mixes
                // one checkpointed row with one `-wal` only row.
                let busy: i64 = writer
                    .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(busy, 0, "fixture checkpoint must not be blocked");
            }
            insert_chat_row(&writer, &Row::new(2, 2_000, b"wal_only"));

            Self { _dir: dir, root, writer }
        }

        fn path(&self) -> PathBuf {
            self.root.join("message").join("message_0.db")
        }

        fn wal_len(&self) -> u64 {
            std::fs::metadata(self.path().with_extension("db-wal"))
                .map(|meta| meta.len())
                .unwrap_or(0)
        }

        fn main_len(&self) -> u64 {
            std::fs::metadata(self.path()).map(|meta| meta.len()).unwrap_or(0)
        }

        fn journal_mode(&self) -> String {
            self.writer
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap()
        }
    }

    fn shard_keys() -> HashMap<String, String> {
        HashMap::from([("message_0.db".to_string(), TEST_KEY.to_string())])
    }

    #[test]
    fn rows_committed_only_to_the_write_ahead_log_are_collected() {
        let fixture = JournalFixture::new("WAL");
        assert!(fixture.wal_len() > 32, "fixture guard: the newest commit must stay in the -wal");
        let main_before = fixture.main_len();

        let sample = collect_message_snapshot(&fixture.root, ACCOUNT, CHAT_ID, &shard_keys()).unwrap();

        assert_eq!(
            text(&sample),
            vec!["message_0.db:checkpointed", "message_0.db:wal_only"]
        );
        assert_eq!(fixture.journal_mode(), "wal", "the read must not change journal_mode");
        assert_eq!(fixture.main_len(), main_before, "the read must not checkpoint the -wal");
    }

    #[test]
    fn an_exclusive_writer_lock_does_not_block_the_snapshot_read() {
        // Historical contract: a DELETE-journal EXCLUSIVE lock made the read
        // wait for a shared lock and then fail with `DatabaseRead`. The snapshot
        // read takes no lock on the source, so the committed rows come back
        // without waiting — and the uncommitted insert never leaks in.
        let fixture = JournalFixture::new("DELETE");
        fixture
            .writer
            .execute_batch("BEGIN EXCLUSIVE; INSERT INTO Name2Id (user_name) VALUES ('blocked');")
            .unwrap();

        let started = std::time::Instant::now();
        let result = collect_message_snapshot(&fixture.root, ACCOUNT, CHAT_ID, &shard_keys());
        let elapsed = started.elapsed();

        fixture.writer.execute_batch("ROLLBACK;").unwrap();
        let sample = result.unwrap_or_else(|error| {
            panic!("a writer lock must not fail the snapshot read: {error}")
        });
        assert_eq!(
            text(&sample),
            vec!["message_0.db:checkpointed", "message_0.db:wal_only"],
            "only the committed rows are read"
        );
        assert!(
            elapsed < Duration::from_millis(1_500),
            "the read must not wait on the writer's lock, took {elapsed:?}"
        );
    }

    // ── The wall clock of one collection ───────────────────────────────────

    #[test]
    fn one_deadline_is_shared_by_every_clone_and_can_be_stopped_by_its_caller() {
        let cancel = CollectionCancel::within(Duration::from_secs(300));
        let clone = cancel.clone();
        assert!(!cancel.expired());
        assert!(cancel.check().is_ok());

        // Nobody is waiting for it any more.
        clone.cancel();
        assert!(cancel.expired(), "a stopped collection stops on every handle");
        assert_eq!(cancel.check().unwrap_err(), MessageSnapshotError::DeadlineExceeded);
        assert_eq!(cancel.remaining(), Duration::ZERO);
    }

    #[test]
    fn a_past_deadline_stops_the_collection_before_it_reads_a_row() {
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"a")]);
        let cancel = CollectionCancel::within(Duration::from_millis(0));
        cancel.cancel();

        assert_eq!(
            collect_message_snapshot_until(&root.path, ACCOUNT, CHAT_ID, &root.keys_all(), &cancel)
                .unwrap_err(),
            MessageSnapshotError::DeadlineExceeded,
            "an abandoned round reads nothing, not even one row"
        );
    }

    #[test]
    fn a_collection_that_outlives_its_budget_returns_no_partial_sample() {
        // Calibrated rather than guessed: the same fixture is collected once
        // with time to spare, and the deadline below is a fraction of what that
        // took. On a slow machine the interrupt is what fires; on a fast one the
        // first check is. Both are the deadline, and neither returns rows.
        let root = Root::empty();
        let rows: Vec<Row> = (1..=4_000)
            .map(|id| Row::new(id, id * 1_000, &vec![b'x'; 64]))
            .collect();
        root.add_chat_shard("message_0.db", &rows);

        let started = std::time::Instant::now();
        let full = collect_message_snapshot_until(
            &root.path,
            ACCOUNT,
            CHAT_ID,
            &root.keys_all(),
            &CollectionCancel::within(Duration::from_secs(120)),
        )
        .expect("the whole collection fits in the generous budget");
        let measured = started.elapsed();
        assert_eq!(full.len(), 4_000);

        let budget = (measured / 8).max(Duration::from_millis(1));
        let started = std::time::Instant::now();
        let cut = collect_message_snapshot_until(
            &root.path,
            ACCOUNT,
            CHAT_ID,
            &root.keys_all(),
            &CollectionCancel::within(budget),
        );
        let elapsed = started.elapsed();

        assert_eq!(
            cut.unwrap_err(),
            MessageSnapshotError::DeadlineExceeded,
            "budget {budget:?} against a collection that takes {measured:?}"
        );
        assert!(
            elapsed < measured,
            "the budget has to end the collection early, waited {elapsed:?} of {measured:?}"
        );
    }

    #[test]
    fn a_running_statement_is_interrupted_from_inside_sqlite() {
        // The per-row check can only fire between rows. A statement that is
        // already running when the deadline passes is stopped by the progress
        // handler, which is what this exercises: no row boundary is crossed here.
        let root = Root::empty();
        root.add_chat_shard("message_0.db", &[Row::new(1, 1_000, b"a")]);
        let cancel = CollectionCancel::within(Duration::from_millis(30));
        let conn = open_shard_readonly(&root.shard_path("message_0.db"), TEST_KEY, &cancel)
            .expect("the shard opens");

        let started = std::time::Instant::now();
        let outcome = conn.query_row(
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 200000000)
             SELECT count(*) FROM seq",
            [],
            |row| row.get::<_, i64>(0),
        );
        let elapsed = started.elapsed();

        let error = outcome.expect_err("a statement past the deadline must not run to its end");
        let interrupted = matches!(
            error,
            rusqlite::Error::SqliteFailure(ref failure, _)
                if failure.code == rusqlite::ffi::ErrorCode::OperationInterrupted
        );
        assert!(interrupted, "expected an interrupt, got {error:?}");
        assert!(
            elapsed < Duration::from_secs(20),
            "the interrupt must land on the deadline, waited {elapsed:?}"
        );
    }

    #[test]
    fn an_expiry_during_the_snapshot_is_a_deadline_not_a_database_error() {
        // The snapshot layer reports a cancelled capture as a read failure; the
        // message collection must translate that into its own deadline code, not
        // a 500-class database error.
        let fixture = JournalFixture::new("WAL");
        let cancel = CollectionCancel::within(Duration::from_secs(60));
        wechat_snapshot::test_trip_on_merge(&cancel);
        let result =
            collect_message_snapshot_until(&fixture.root, ACCOUNT, CHAT_ID, &shard_keys(), &cancel);
        wechat_snapshot::test_clear_trip();
        assert_eq!(result.unwrap_err(), MessageSnapshotError::DeadlineExceeded);
    }

    #[test]
    fn a_short_deadline_is_not_spent_waiting_for_a_writer_lock() {
        // The old read clamped a lock wait to the remaining deadline, so a short
        // deadline was spent waiting and the sample was refused. The snapshot
        // read never waits for the lock, so the same short deadline answers with
        // the committed rows. What must never happen is an empty sample.
        let fixture = JournalFixture::new("DELETE");
        fixture
            .writer
            .execute_batch("BEGIN EXCLUSIVE; INSERT INTO Name2Id (user_name) VALUES ('blocked');")
            .unwrap();

        let started = std::time::Instant::now();
        let result = collect_message_snapshot_until(
            &fixture.root,
            ACCOUNT,
            CHAT_ID,
            &shard_keys(),
            &CollectionCancel::within(Duration::from_millis(200)),
        );
        let elapsed = started.elapsed();
        fixture.writer.execute_batch("ROLLBACK;").unwrap();

        let sample = result.unwrap_or_else(|error| {
            panic!("a writer lock must not turn a quick read into a refusal: {error}")
        });
        assert!(
            !sample.is_empty(),
            "a writer lock must never become an empty sample"
        );
        assert!(
            elapsed < Duration::from_millis(600),
            "the deadline must not be spent waiting on the lock, took {elapsed:?}"
        );
    }

    #[test]
    fn a_collision_at_or_after_the_deadline_is_reported_as_the_deadline() {
        // The mapping itself, which the collision above can only reach by racing:
        // the same failure is a deadline once the deadline has passed, and a
        // read failure before it, because those are the two things the caller
        // does differently.
        let stopped = CollectionCancel::within(Duration::from_millis(0));
        stopped.cancel();
        assert_eq!(read_outcome(&stopped), MessageSnapshotError::DeadlineExceeded);

        let live = CollectionCancel::within(Duration::from_secs(60));
        assert_eq!(read_outcome(&live), MessageSnapshotError::DatabaseRead);
    }

    // ── Bounded decode on the sample path ──────────────────────────────────

    #[test]
    fn a_stored_blob_that_expands_past_the_read_budget_is_reported_not_expanded() {
        // 8 MiB of one byte compresses to a few KiB, so this passes the stored
        // length the sample checks and is only caught by the decode's own cap.
        let bomb = vec![b'x'; 8 * 1024 * 1024];
        let compressed = zstd::encode_all(&bomb[..], 3).expect("compresses");
        let hex = hex_upper(&compressed);
        assert!(
            hex.len() / 2 < SNAPSHOT_ROW_CONTENT_LIMIT,
            "the stored form must stay under the stored limit, {} bytes",
            hex.len() / 2
        );

        assert_eq!(
            decode_stored_content(&hex, true, true),
            DECODE_TOO_LARGE,
            "the sample must not build a body it cannot budget"
        );
        // The list has no budget behind it and keeps what it has always returned.
        assert_eq!(decode_stored_content(&hex, true, false).len(), bomb.len());
    }

    #[test]
    fn injected_small_decode_budget_proves_media_xml_is_lost_only_on_sample_path() {
        let xml = "<msg><img md5=\"0123456789abcdef0123456789abcdef\" cdnbigimgurl=\"cached\" /></msg>";
        let compressed = zstd::encode_all(xml.as_bytes(), 3).expect("compresses");
        let hex = hex_upper(&compressed);
        let bounded = decode_stored_content_with_limit(&hex, true, true, xml.len() - 1);
        assert_eq!(bounded, DECODE_TOO_LARGE);
        assert_eq!(decode_message_content(&hex, true), xml);
        assert_eq!(bounded.find("md5=\""), None);
        assert!(decode_message_content(&hex, true).contains("md5=\"0123456789abcdef0123456789abcdef\""));
    }

    #[test]
    fn a_compressed_row_under_the_budget_is_still_decoded_by_the_sample() {
        let body = "a compressed body the sample is expected to read";
        let compressed = zstd::encode_all(body.as_bytes(), 3).expect("compresses");
        let root = Root::empty();
        root.add_chat_shard(
            "message_0.db",
            &[Row::new(1, 1_000, &compressed).compressed(1)],
        );

        let sample = collect_message_snapshot(&root.path, ACCOUNT, CHAT_ID, &root.keys_all())
            .expect("a small compressed row is collected");
        assert_eq!(sample[0]["content"], body);
        // The stored bytes are still reported, so a client can decode them itself.
        assert_eq!(sample[0]["rawContentCompression"], true);
    }

    #[test]
    fn the_sample_treats_a_bomb_and_the_list_treats_it_the_same_way_it_always_did() {
        // Both readers share one decode, so this pins the difference to the
        // sample's bound rather than to two copies of the code.
        let bomb = vec![b'y'; 8 * 1024 * 1024];
        let hex = hex_upper(&zstd::encode_all(&bomb[..], 3).expect("compresses"));
        let names = HashMap::new();

        let sample_row = json!({
            "local_id": 1, "create_time": 1_000, "hex_content": hex, "is_compressed": 1,
            "hex_source": "", "source_compressed": 0, "content_len": 0, "source_len": 0,
        });
        let sample = decode_message_row(
            &sample_row,
            RowContext {
                chat_id: CHAT_ID,
                account_dir: ACCOUNT,
                contact_names: &names,
                require_create_time: true,
                bounded_decode: true,
            },
        )
        .expect("the row is well formed; only its body is refused");
        assert_eq!(sample.message.content, DECODE_TOO_LARGE);

        let listed = decode_message_row(
            &sample_row,
            RowContext {
                chat_id: CHAT_ID,
                account_dir: ACCOUNT,
                contact_names: &names,
                require_create_time: false,
                bounded_decode: false,
            },
        )
        .expect("the list tolerates what it always tolerated");
        assert_eq!(listed.message.content.len(), bomb.len());
    }

    #[test]
    fn the_deadline_has_its_own_code_and_the_budget_is_stated_once() {
        assert_eq!(
            MessageSnapshotError::DeadlineExceeded.code(),
            "WECHAT_MESSAGE_DEADLINE_EXCEEDED"
        );
        assert_eq!(COLLECTION_BUDGET, Duration::from_secs(8));
        // The snapshot layer's own fallback deadline is a bound of its own,
        // never longer than the round that is waiting on it.
        assert!(wechat_snapshot::SNAPSHOT_DEFAULT_BUDGET <= COLLECTION_BUDGET);
    }
}
