use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OperationType {
    ADD,
    UPDATE,
    TAG,
    REMOVE,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::ADD => write!(f, "ADD"),
            OperationType::UPDATE => write!(f, "UPDATE"),
            OperationType::TAG => write!(f, "TAG"),
            OperationType::REMOVE => write!(f, "REMOVE"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaOperation {
    pub type_: OperationType,
    pub section: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bullet_id: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, i32>,
}

impl DeltaOperation {
    pub fn from_json(payload: &serde_json::Value) -> Result<Self, Box<dyn std::error::Error>> {
        let op_type_str = payload["type"].as_str().ok_or("Invalid type")?.to_uppercase();
        let op_type = match op_type_str.as_str() {
            "ADD" => OperationType::ADD,
            "UPDATE" => OperationType::UPDATE,
            "TAG" => OperationType::TAG,
            "REMOVE" => OperationType::REMOVE,
            _ => return Err(format!("Invalid operation type: {}", op_type_str).into()),
        };

        let mut metadata: HashMap<String, i32> = HashMap::new();
        if let Some(meta_raw) = payload["metadata"].as_object() {
            if op_type == OperationType::TAG {
                let valid_tags = vec!["helpful", "harmful", "neutral"];
                for (k, v) in meta_raw {
                    if valid_tags.contains(&k.as_str()) {
                        if let Some(val) = v.as_i64() {
                            metadata.insert(k.clone(), val as i32);
                        }
                    }
                }
            } else {
                for (k, v) in meta_raw {
                    if let Some(val) = v.as_i64() {
                        metadata.insert(k.clone(), val as i32);
                    }
                }
            }
        }

        Ok(Self {
            type_: op_type,
            section: payload["section"].as_str().unwrap_or("").to_string(),
            content: payload["content"].as_str().map(|s| s.to_string()),
            bullet_id: payload["bullet_id"].as_str().map(|s| s.to_string()),
            metadata,
        })
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        let type_str = format!("{}", self.type_).to_lowercase();
        map.insert("type".to_string(), serde_json::Value::String(type_str));
        map.insert("section".to_string(), serde_json::Value::String(self.section.clone()));

        if let Some(content) = &self.content {
            map.insert("content".to_string(), serde_json::Value::String(content.clone()));
        }

        if let Some(bullet_id) = &self.bullet_id {
            map.insert("bullet_id".to_string(), serde_json::Value::String(bullet_id.clone()));
        }

        if !self.metadata.is_empty() {
            let mut meta_map = serde_json::Map::new();
            for (k, v) in &self.metadata {
                meta_map.insert(k.clone(), serde_json::Value::Number(serde_json::Number::from(*v)));
            }
            map.insert("metadata".to_string(), serde_json::Value::Object(meta_map));
        }

        serde_json::Value::Object(map)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaBatch {
    pub reasoning: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<DeltaOperation>,
}

impl DeltaBatch {
    pub fn from_json(payload: &serde_json::Value) -> Result<Self, Box<dyn std::error::Error>> {
        let mut operations = Vec::new();
        if let Some(ops_array) = payload["operations"].as_array() {
            for item in ops_array {
                if let serde_json::Value::Object(_) = item {
                    operations.push(DeltaOperation::from_json(item)?);
                }
            }
        }

        Ok(Self {
            reasoning: payload["reasoning"].as_str().unwrap_or("").to_string(),
            operations,
        })
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("reasoning".to_string(), serde_json::Value::String(self.reasoning.clone()));

        let mut ops_array = Vec::new();
        for op in &self.operations {
            ops_array.push(op.to_json());
        }
        map.insert("operations".to_string(), serde_json::Value::Array(ops_array));

        serde_json::Value::Object(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test1() {
        let json = json!({
            "reasoning": "This is a test",
            "operations": [
                {
                    "type": "add",
                    "value": "This is a test"
                }
            ]
        });

        let delta = DeltaBatch::from_json(&json).unwrap();
        println!("{:?}", delta)
    }

    #[test]
    fn test2() {
        let delta = DeltaBatch {
            reasoning: "This is a test".to_string(),
            operations: vec![],
        };
        let json = delta.to_json();
        println!("{}", json);
    }

    #[test]
    fn test3() {
        let json = json!({
            "type": "add",
            "section": "This is a test",
            "content": "This is a test",
            "bullet_id": "123",
            "metadata": {
                "helpful": 1,
                "harmful": 0,
                "neutral": 0
            }
        });
        let delta =  DeltaOperation::from_json(&json).unwrap();
        println!("{:?}", delta);
    }

    #[test]
    fn test4() {
        let delta = DeltaOperation {
            type_: OperationType::ADD,
            section: "This is a test".to_string(),
            content: Some("This is a test".to_string()),
            bullet_id: None,
            metadata: HashMap::new(),
        };
        let json = delta.to_json();
        println!("{}", json);
    }
}