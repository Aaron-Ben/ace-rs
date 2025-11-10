use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeltaError {
    #[error("JSON解析错误：{0}")]
    JsonParseError(#[from] serde_json::Error),
    #[error("无效的操作类型：{0}（仅支持ADD/UPDATE/TAG/REMOVE）")]
    InvalidOperationType(String),
    #[error("字段缺失：{0}（必填字段）")]
    MissingRequiredField(String),
    #[error("整数溢出：{0} 超出i32范围")]
    IntegerOverflow(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OperationType {
    Add,
    Update,
    Tag,
    Remove,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Add => write!(f, "ADD"),
            OperationType::Update => write!(f, "UPDATE"),
            OperationType::Tag => write!(f, "TAG"),
            OperationType::Remove => write!(f, "REMOVE"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaOperation {
    #[serde(rename = "type")]
    pub type_: OperationType,
    pub section: String,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bullet_id: Option<String>,
    
    #[serde(default = "HashMap::new")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, i32>,
}

impl DeltaOperation {
    pub fn from_json(payload: &serde_json::Value) -> Result<Self, DeltaError> {
        let mut op: Self = serde_json::from_value(payload.clone())?;

        // 验证TAG操作的metadata
        if op.type_ == OperationType::Tag {
            let valid_tags = ["helpful", "harmful", "neutral"];
            op.metadata.retain(|k, _| valid_tags.contains(&k.as_str()));
        }

        Ok(op)
    }

    pub fn to_json(&self) -> Result<serde_json::Value, DeltaError> {
        Ok(serde_json::to_value(self)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaBatch {
    #[serde(default)]
    pub reasoning: String,
    
    #[serde(default)]
    pub operations: Vec<DeltaOperation>,
}

impl DeltaBatch {
    pub fn from_json(payload: &serde_json::Value) -> Result<Self, DeltaError> {
        let batch: Self = serde_json::from_value(payload.clone())?;
        Ok(batch)
    }

    pub fn to_json(&self) -> Result<serde_json::Value, DeltaError> {
        Ok(serde_json::to_value(self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_delta_batch_deserialization() {
        let json = json!({
            "reasoning": "This is a test",
            "operations": [
                {
                    "type": "ADD",
                    "section": "test section",
                    "content": "This is a test"
                }
            ]
        });

        let delta = DeltaBatch::from_json(&json).unwrap();
        assert_eq!(delta.operations.len(), 1);
        assert_eq!(delta.operations[0].type_, OperationType::Add);
        assert_eq!(delta.operations[0].section, "test section");
        assert_eq!(delta.operations[0].content, Some("This is a test".to_string()));
    }

    #[test]
    fn test_delta_batch_serialization() {
        let delta = DeltaBatch {
            reasoning: "This is a test".to_string(),
            operations: vec![],
        };
        let json = delta.to_json().unwrap();
        let obj = json.as_object().unwrap();
        assert!(obj.contains_key("operations"));
        assert!(obj["operations"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_delta_operation_with_metadata() {
        let json = json!({
            "type": "ADD",
            "section": "test section",
            "content": "This is a test",
            "bullet_id": "123",
            "metadata": {
                "helpful": 1,
                "harmful": 0,
                "neutral": 0
            }
        });
        
        let delta = DeltaOperation::from_json(&json).unwrap();
        assert_eq!(delta.type_, OperationType::Add);
        assert_eq!(delta.section, "test section");
        assert_eq!(delta.bullet_id, Some("123".to_string()));
        assert_eq!(delta.metadata["helpful"], 1);
    }

    #[test]
    fn test_tag_operation_metadata_filtering() {
        let json = json!({
            "type": "TAG",
            "section": "test",
            "bullet_id": "123",
            "metadata": {
                "helpful": 1,
                "invalid_tag": 5,  // 被过滤掉
                "neutral": 2
            }
        });
        
        let delta = DeltaOperation::from_json(&json).unwrap();
        assert_eq!(delta.metadata.len(), 2); // 只保留helpful和neutral
        assert!(delta.metadata.contains_key("helpful"));
        assert!(delta.metadata.contains_key("neutral"));
        assert!(!delta.metadata.contains_key("invalid_tag"));
    }

    #[test]
    fn test_unknown_fields_rejection() {
        let json = json!({
            "type": "ADD",
            "section": "test",
            "unknown_field": "should fail"  // 这个字段不存在于结构体中
        });
        
        let result = DeltaOperation::from_json(&json);
        assert!(result.is_err());
    }
}