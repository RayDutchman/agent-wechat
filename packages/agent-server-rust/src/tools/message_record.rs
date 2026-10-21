//! Bounded projections for the message-record XML shapes used by references and
//! forwarded records. `quick-xml` tokenizes XML; this module only builds the small
//! bounded projection tree, rejects DTD events, and never resolves external data.

use crate::ia::types::{
    ForwardedRecord, ForwardedRecordItem, ReferenceV2, XmlParseStatus,
};
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::{BytesStart, Event};
use quick_xml::reader::Reader;

/// Match the existing fixed-sample decompression ceiling rather than creating a
/// second independently configurable message-body budget.
const MESSAGE_XML_MAX_BYTES: usize = 4 * 1024 * 1024;
const XML_MAX_DEPTH: usize = 16;
const XML_MAX_NODES: usize = 20_000;
const FORWARDED_MAX_ITEMS: usize = 100;
const FORWARDED_ITEM_TEXT_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageRecordFields {
    pub xml_sub_type: Option<u32>,
    pub reference: Option<ReferenceV2>,
    pub forwarded_record: Option<ForwardedRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct XmlNode {
    name: String,
    parts: Vec<XmlPart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum XmlPart {
    Text(String),
    Element(XmlNode),
}

impl XmlNode {
    fn child(&self, name: &str) -> Option<&XmlNode> {
        self.elements().find(|child| child.name == name)
    }

    fn descendants(&self, name: &str) -> Option<&XmlNode> {
        if self.name == name {
            return Some(self);
        }
        self.elements().find_map(|child| child.descendants(name))
    }

    fn text_content(&self) -> String {
        let mut text = String::new();
        for part in &self.parts {
            match part {
                XmlPart::Text(value) => text.push_str(value),
                XmlPart::Element(child) => text.push_str(&child.text_content()),
            }
        }
        text
    }

    fn elements(&self) -> impl Iterator<Item = &XmlNode> {
        self.parts.iter().filter_map(|part| match part {
            XmlPart::Text(_) => None,
            XmlPart::Element(node) => Some(node),
        })
    }

    fn child_text(&self, name: &str) -> Option<String> {
        let value = self.child(name)?.text_content();
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_string())
    }

    fn child_raw_text(&self, name: &str) -> Option<String> {
        self.child(name).map(XmlNode::text_content)
    }

    fn has_non_whitespace_text(&self) -> bool {
        self.parts.iter().any(|part| match part {
            XmlPart::Text(value) => !is_xml_whitespace(value),
            XmlPart::Element(_) => false,
        })
    }

    fn unique_child(&self, name: &str) -> Result<Option<&XmlNode>, XmlFailure> {
        let mut matches = self.elements().filter(|child| child.name == name);
        let first = matches.next();
        if matches.next().is_some() {
            Err(XmlFailure::Invalid)
        } else {
            Ok(first)
        }
    }

    fn descendant_count(&self, name: &str) -> usize {
        usize::from(self.name == name)
            + self.elements().map(|child| child.descendant_count(name)).sum::<usize>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XmlFailure {
    Invalid,
    Truncated,
}

struct XmlParser;

impl XmlParser {
    fn parse(input: &str) -> Result<XmlNode, XmlFailure> {
        if input.len() > MESSAGE_XML_MAX_BYTES {
            return Err(XmlFailure::Truncated);
        }
        let input = input.strip_prefix('\u{feff}').unwrap_or(input);
        let mut reader = Reader::from_str(input);
        reader.config_mut().enable_all_checks(true);
        reader.config_mut().allow_dangling_amp = false;
        reader.config_mut().allow_unmatched_ends = false;

        let mut root = None;
        let mut stack: Vec<XmlNode> = Vec::with_capacity(XML_MAX_DEPTH);
        let mut node_count = 0;
        let mut saw_document_content = false;

        loop {
            let event = reader.read_event().map_err(|_| XmlFailure::Invalid)?;
            match event {
                Event::Start(tag) => {
                    if stack.is_empty() && root.is_some() {
                        return Err(XmlFailure::Invalid);
                    }
                    let depth = stack.len() + 1;
                    if depth > XML_MAX_DEPTH {
                        return Err(XmlFailure::Truncated);
                    }
                    charge_node(&mut node_count)?;
                    validate_attributes(&tag)?;
                    let name = tag.name().as_ref().to_string();
                    stack.push(XmlNode { name, parts: Vec::new() });
                    saw_document_content = true;
                }
                Event::Empty(tag) => {
                    if stack.is_empty() && root.is_some() {
                        return Err(XmlFailure::Invalid);
                    }
                    if stack.len() + 1 > XML_MAX_DEPTH {
                        return Err(XmlFailure::Truncated);
                    }
                    charge_node(&mut node_count)?;
                    validate_attributes(&tag)?;
                    let name = tag.name().as_ref().to_string();
                    attach_node(&mut root, &mut stack, XmlNode { name, parts: Vec::new() })?;
                    saw_document_content = true;
                }
                Event::End(_) => {
                    let node = stack.pop().ok_or(XmlFailure::Invalid)?;
                    attach_node(&mut root, &mut stack, node)?;
                    saw_document_content = true;
                }
                Event::Text(text) => {
                    let in_element = !stack.is_empty();
                    if in_element {
                        charge_node(&mut node_count)?;
                    }
                    let value = text.xml10_content().into_owned();
                    validate_xml_chars(&value)?;
                    if value.contains("]]>") {
                        return Err(XmlFailure::Invalid);
                    }
                    if let Some(parent) = stack.last_mut() {
                        parent.push_text(value);
                    } else if !is_xml_whitespace(&value) {
                        return Err(XmlFailure::Invalid);
                    }
                    saw_document_content = true;
                }
                Event::GeneralRef(reference) => {
                    let parent = stack.last_mut().ok_or(XmlFailure::Invalid)?;
                    charge_node(&mut node_count)?;
                    let value = if reference.is_char_ref() {
                        let ch = reference
                            .resolve_char_ref()
                            .map_err(|_| XmlFailure::Invalid)?
                            .ok_or(XmlFailure::Invalid)?;
                        validate_xml_chars(ch.encode_utf8(&mut [0; 4]))?;
                        ch.to_string()
                    } else {
                        resolve_predefined_entity(reference.as_ref())
                            .ok_or(XmlFailure::Invalid)?
                            .to_string()
                    };
                    parent.push_text(value);
                    saw_document_content = true;
                }
                Event::CData(cdata) => {
                    if stack.is_empty() {
                        return Err(XmlFailure::Invalid);
                    }
                    charge_node(&mut node_count)?;
                    let parent = stack.last_mut().ok_or(XmlFailure::Invalid)?;
                    let value = cdata.xml10_content().into_owned();
                    validate_xml_chars(&value)?;
                    parent.push_text(value);
                    saw_document_content = true;
                }
                Event::Comment(comment) => {
                    charge_node(&mut node_count)?;
                    validate_xml_chars(comment.as_ref())?;
                    saw_document_content = true;
                }
                Event::PI(instruction) => {
                    charge_node(&mut node_count)?;
                    validate_xml_chars(instruction.as_ref())?;
                    saw_document_content = true;
                }
                Event::Decl(declaration) => {
                    if saw_document_content || !stack.is_empty() || root.is_some() {
                        return Err(XmlFailure::Invalid);
                    }
                    validate_xml_chars(declaration.as_ref())?;
                    saw_document_content = true;
                }
                Event::DocType(_) => return Err(XmlFailure::Invalid),
                Event::Eof => break,
            }
        }

        if !stack.is_empty() {
            return Err(XmlFailure::Invalid);
        }
        root.ok_or(XmlFailure::Invalid)
    }
}

fn attach_node(
    root: &mut Option<XmlNode>,
    stack: &mut [XmlNode],
    node: XmlNode,
) -> Result<(), XmlFailure> {
    if let Some(parent) = stack.last_mut() {
        parent.parts.push(XmlPart::Element(node));
    } else if root.is_some() {
        return Err(XmlFailure::Invalid);
    } else {
        *root = Some(node);
    }
    Ok(())
}

fn charge_node(count: &mut usize) -> Result<(), XmlFailure> {
    if *count >= XML_MAX_NODES {
        return Err(XmlFailure::Truncated);
    }
    *count += 1;
    Ok(())
}

fn validate_attributes(tag: &BytesStart<'_>) -> Result<(), XmlFailure> {
    for attribute in tag.attributes().with_checks(true) {
        let attribute = attribute.map_err(|_| XmlFailure::Invalid)?;
        let value = attribute
            .normalized_value(quick_xml::XmlVersion::Implicit1_0)
            .map_err(|_| XmlFailure::Invalid)?;
        validate_xml_chars(&value)?;
    }
    Ok(())
}

fn is_xml_whitespace(text: &str) -> bool {
    text.chars().all(|ch| matches!(ch, '\t' | '\n' | '\r' | ' '))
}

impl XmlNode {
    fn push_text(&mut self, text: String) {
        if let Some(XmlPart::Text(existing)) = self.parts.last_mut() {
            existing.push_str(&text);
        } else {
            self.parts.push(XmlPart::Text(text));
        }
    }
}

fn validate_xml_chars(text: &str) -> Result<(), XmlFailure> {
    if text.chars().all(valid_xml_char) {
        Ok(())
    } else {
        Err(XmlFailure::Invalid)
    }
}

fn valid_xml_char(ch: char) -> bool {
    matches!(ch as u32, 0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF)
}

pub(crate) fn extract_reference_v2(body: &str, chat_username: &str) -> Option<ReferenceV2> {
    let root = XmlParser::parse(body).ok()?;
    let refermsg = root.descendants("refermsg")?;
    let from_user = refermsg.child_text("fromusr");
    let is_group = chat_username.contains("@chatroom");
    let chat_username = if is_group {
        from_user.or_else(|| nonempty(chat_username))
    } else {
        nonempty(chat_username)
    };
    Some(ReferenceV2 {
        server_id: refermsg.child_text("svrid"),
        chat_username,
        // WeChat's refermsg.chatusr is the only supported quoted-author ID.
        // In particular, fromusr is never treated as the author.
        sender_username: refermsg.child_text("chatusr"),
        display_name: refermsg.child_text("displayname"),
        content: refermsg.child_raw_text("content").unwrap_or_default(),
    })
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

pub(crate) fn project_message_record(
    body: &str,
    chat_username: &str,
    message_type: u32,
    packed_sub_type: u32,
) -> MessageRecordFields {
    let root = match XmlParser::parse(body) {
        Ok(root) => root,
        Err(error) => {
            let status = match error {
                XmlFailure::Invalid => XmlParseStatus::Invalid,
                XmlFailure::Truncated => XmlParseStatus::Truncated,
            };
            return MessageRecordFields {
                xml_sub_type: None,
                reference: None,
                forwarded_record: (message_type == 49 && packed_sub_type == 19)
                    .then(|| empty_record(status)),
            };
        }
    };
    let appmsg = (root.descendant_count("appmsg") == 1)
        .then(|| root.descendants("appmsg"))
        .flatten();
    let xml_sub_type = appmsg
        .and_then(|node| node.unique_child("type").ok().flatten())
        .and_then(|node| {
            let value = node.text_content();
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        })
        .and_then(|value| value.parse::<u32>().ok());
    let reference = root.descendants("refermsg").map(|refermsg| {
        let from_user = refermsg.child_text("fromusr");
        let chat_username = if chat_username.contains("@chatroom") {
            from_user.or_else(|| nonempty(chat_username))
        } else {
            nonempty(chat_username)
        };
        ReferenceV2 {
            server_id: refermsg.child_text("svrid"),
            chat_username,
            sender_username: refermsg.child_text("chatusr"),
            display_name: refermsg.child_text("displayname"),
            content: refermsg.child_raw_text("content").unwrap_or_default(),
        }
    });
    let is_forwarded = message_type == 49 && (packed_sub_type == 19 || xml_sub_type == Some(19));
    let forwarded_record = is_forwarded.then(|| parse_forwarded_record(&root));
    MessageRecordFields { xml_sub_type, reference, forwarded_record }
}

fn empty_record(parse_status: XmlParseStatus) -> ForwardedRecord {
    ForwardedRecord { parse_status, items: Vec::new() }
}

pub(crate) fn unparsed_message_record(
    message_type: u32,
    packed_sub_type: u32,
    parse_status: XmlParseStatus,
) -> MessageRecordFields {
    MessageRecordFields {
        xml_sub_type: None,
        reference: None,
        forwarded_record: (message_type == 49 && packed_sub_type == 19)
            .then(|| empty_record(parse_status)),
    }
}

fn parse_forwarded_record(root: &XmlNode) -> ForwardedRecord {
    if root.descendant_count("appmsg") != 1 {
        return empty_record(XmlParseStatus::Invalid);
    }
    let Some(appmsg) = root.descendants("appmsg") else {
        return empty_record(XmlParseStatus::Invalid);
    };
    if appmsg.unique_child("type").is_err() {
        return empty_record(XmlParseStatus::Invalid);
    }
    let recorditem = match appmsg.unique_child("recorditem") {
        Ok(Some(recorditem)) => recorditem,
        Ok(None) | Err(_) => return empty_record(XmlParseStatus::Invalid),
    };
    if recorditem.elements().any(|child| child.name != "recordinfo") {
        return empty_record(XmlParseStatus::Invalid);
    }
    let direct_recordinfo = match recorditem.unique_child("recordinfo") {
        Ok(recordinfo) => recordinfo,
        Err(_) => return empty_record(XmlParseStatus::Invalid),
    };
    let recordinfo = if let Some(recordinfo) = direct_recordinfo {
        if recorditem.has_non_whitespace_text() {
            return empty_record(XmlParseStatus::Invalid);
        }
        recordinfo.clone()
    } else {
        let serialized = recorditem.text_content();
        match XmlParser::parse(&serialized) {
            Ok(node) if node.name == "recordinfo" => node,
            Ok(_) | Err(XmlFailure::Invalid) => return empty_record(XmlParseStatus::Invalid),
            Err(XmlFailure::Truncated) => return empty_record(XmlParseStatus::Truncated),
        }
    };
    if recordinfo.has_non_whitespace_text() {
        return empty_record(XmlParseStatus::Invalid);
    }
    if recordinfo.elements().any(|child| !matches!(child.name.as_str(), "title" | "desc" | "datalist")) {
        return empty_record(XmlParseStatus::Invalid);
    }
    let data_list = match recordinfo.unique_child("datalist") {
        Ok(Some(data_list)) => data_list,
        Ok(None) | Err(_) => return empty_record(XmlParseStatus::Invalid),
    };
    if data_list.elements().any(|child| child.name != "dataitem") {
        return empty_record(XmlParseStatus::Invalid);
    }
    if data_list.has_non_whitespace_text() {
        return empty_record(XmlParseStatus::Invalid);
    }
    let mut items = Vec::new();
    let mut parse_status = XmlParseStatus::Complete;
    for data_item in data_list.elements() {
        if items.len() >= FORWARDED_MAX_ITEMS {
            parse_status = XmlParseStatus::Truncated;
            break;
        }
        const SINGLE_ITEM_FIELDS: &[&str] = &[
            "datatype", "dataid", "datadesc", "sourcename", "sourceheadurl",
            "sourcetime", "srcMsgCreateTime", "fromnewmsgid", "dataitemsource",
        ];
        if data_item.elements().any(|child| !SINGLE_ITEM_FIELDS.contains(&child.name.as_str()))
            || SINGLE_ITEM_FIELDS.iter().any(|field| data_item.unique_child(field).is_err())
            || data_item.has_non_whitespace_text()
        {
            return empty_record(XmlParseStatus::Invalid);
        }
        let datatype = match data_item.child_text("datatype") {
            Some(value) => match value.parse::<u32>() {
                Ok(value) => Some(value),
                Err(_) => return empty_record(XmlParseStatus::Invalid),
            },
            None => None,
        };
        let datadesc = data_item.child_raw_text("datadesc");
        if datadesc.as_ref().is_some_and(|text| text.len() > FORWARDED_ITEM_TEXT_MAX_BYTES) {
            parse_status = XmlParseStatus::Truncated;
            break;
        }
        let source = data_item.child("dataitemsource");
        if source.is_some_and(|node| {
            node.elements().any(|child| child.name != "hashusername")
                || node.unique_child("hashusername").is_err()
                || node.has_non_whitespace_text()
        }) {
            return empty_record(XmlParseStatus::Invalid);
        }
        items.push(ForwardedRecordItem {
            datatype,
            dataid: data_item.child_text("dataid"),
            datadesc,
            sourcename: data_item.child_text("sourcename"),
            sourceheadurl: data_item.child_text("sourceheadurl"),
            sourcetime: data_item.child_text("sourcetime"),
            src_msg_create_time: data_item.child_text("srcMsgCreateTime"),
            from_new_msg_id: data_item.child_text("fromnewmsgid"),
            dataitem_source_hash_username: source.and_then(|node| node.child_text("hashusername")),
        });
    }
    ForwardedRecord { parse_status, items }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_xml_subset_reports_deep_and_large_documents_as_truncated() {
        let deep = format!("{}x{}", "<a>".repeat(XML_MAX_DEPTH + 1), "</a>".repeat(XML_MAX_DEPTH + 1));
        assert_eq!(XmlParser::parse(&deep), Err(XmlFailure::Truncated));
        let large = format!("<msg>{}</msg>", "x".repeat(MESSAGE_XML_MAX_BYTES));
        assert_eq!(XmlParser::parse(&large), Err(XmlFailure::Truncated));
        let many_nodes = format!("<msg>{}</msg>", "<node/>".repeat(XML_MAX_NODES));
        assert_eq!(XmlParser::parse(&many_nodes), Err(XmlFailure::Truncated));
    }

    #[test]
    fn bounded_xml_subset_preserves_text_order_around_nested_elements() {
        let node = XmlParser::parse("<content>before<b>middle</b>after</content>").unwrap();
        assert_eq!(node.text_content(), "beforemiddleafter");
    }

    #[test]
    fn bounded_xml_subset_rejects_duplicate_attributes() {
        assert_eq!(XmlParser::parse("<msg mode='one' mode='two'/>").unwrap_err(), XmlFailure::Invalid);
    }
}
