/// Canonical message-type dictionary also used to generate the shared TypeScript
/// module consumed by CLI formatting.
pub const MESSAGE_TYPE_DICTIONARY_JSON: &str =
    include_str!("../message-type-dictionary.json");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_has_three_independent_namespaces_and_open_numeric_codes() {
        let dictionary: serde_json::Value = serde_json::from_str(MESSAGE_TYPE_DICTIONARY_JSON).unwrap();
        assert_eq!(dictionary.as_object().unwrap().len(), 3);
        assert_eq!(dictionary["main"][0]["code"], 1);
        assert_eq!(dictionary["appmsg"][0]["code"], 3);
        assert_eq!(dictionary["forwarded_inner"][0]["code"], 1);
        for entries in dictionary.as_object().unwrap().values() {
            for entry in entries.as_array().unwrap() {
                assert!(entry["key"].is_string());
                assert!(entry["label"].is_string());
                assert!(matches!(entry["basis"].as_str(), Some("default_adopted" | "local_observed")));
            }
        }
    }
}
