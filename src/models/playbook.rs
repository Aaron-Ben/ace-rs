//! ACE的知识存储系统，让代理能持久化学习到策略，并在生成任务时作为上下文注入 LLM 提示

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    fs::{self, File},
    io::Read,
    path::Path,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::models::delta::{DeltaBatch, DeltaOperation, OperationType};

#[derive(Debug, Error)]
pub enum PlaybookError {
    #[error("Bullet not found: {0}")]
    BulletNotFound(String),

    #[error("Invalid tag: {0}. Supported tags: helpful, harmful, neutral")]
    InvalidTag(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid playbook data: {0}")]
    InvalidData(String),

    #[error("Delta operation missing required field: {0}")]
    DeltaMissingField(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Bullet {
    pub id: String,
    pub section: String,
    pub content: String,
    pub helpful: u32,
    pub harmful: u32,
    pub neutral: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Bullet {

    pub fn new(section: String, content: String) -> Self {
        let now = Utc::now();
        Self {
            id: String::new(),
            section,
            content,
            helpful: 0,
            harmful: 0,
            neutral: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn apply_metadata(&mut self, metadata: BTreeMap<String, u32>) {
        for (key, value) in metadata {
            match key.as_str() {
                "helpful" => self.helpful = value,
                "harmful" => self.harmful = value,
                "neutral" => self.neutral = value,
                _ => continue,
            }
        }
        self.updated_at = Utc::now();
    }

    /// 给子弹打标签（增量修改，支持正负值）
    pub fn tag(&mut self, tag: &str, increment: i32) -> Result<(), PlaybookError> {
        // 用saturating_add_signed避免溢出（u32不能为负，最小到0）
        match tag {
            "helpful" => self.helpful = self.helpful.saturating_add_signed(increment),
            "harmful" => self.harmful = self.harmful.saturating_add_signed(increment),
            "neutral" => self.neutral = self.neutral.saturating_add_signed(increment),
            _ => return Err(PlaybookError::InvalidTag(tag.to_string())),
        }
        self.updated_at = Utc::now();
        Ok(())
    }
}

// --------------------------
// 核心存储结构（Playbook）
// --------------------------
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Playbook {
    pub bullets: HashMap<String, Bullet>,
    pub sections: HashMap<String, Vec<String>>,
    pub next_id: u64,
}

impl fmt::Display for Playbook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.bullets.is_empty() {
            return write!(f, "Playbook(empty)");
        }
        write!(f, "{}", self.as_prompt())
    }
}

impl Default for Playbook {
    fn default() -> Self {
        Self {
            bullets: HashMap::new(),
            sections: HashMap::new(),
            next_id: 0,
        }
    }
}

impl Playbook {
    /// 创建空的Playbook实例
    pub fn new() -> Self {
        Self::default()
    }

    // --------------------------
    // 核心CRUD方法
    // --------------------------

    pub fn add_bullet(
        &mut self,
        section: String,
        content: String,
        bullet_id: Option<String>,
        metadata: Option<BTreeMap<String, u32>>,
    ) -> &Bullet {
        let bullet_id = bullet_id.unwrap_or_else(|| self.generate_id(&section));
        let mut bullet = Bullet::new(section.clone(), content);

        if let Some(meta) = metadata {
            bullet.apply_metadata(meta);
        }

        self.bullets.insert(bullet_id.clone(), bullet);
        self.sections.entry(section).or_default().push(bullet_id.clone());

        self.bullets.get(&bullet_id).unwrap()
    }

    pub fn update_bullet(
        &mut self,
        bullet_id: &str,
        content: Option<String>,
        metadata: Option<BTreeMap<String, u32>>,
    ) -> Result<&Bullet, PlaybookError> {
        let bullet = self
            .bullets
            .get_mut(bullet_id)
            .ok_or_else(|| PlaybookError::BulletNotFound(bullet_id.to_string()))?;

        if let Some(c) = content {
            bullet.content = c;
        }

        if let Some(meta) = metadata {
            bullet.apply_metadata(meta);
        }

        bullet.updated_at = Utc::now();

        Ok(bullet)
    }

    pub fn tag_bullet(
        &mut self,
        bullet_id: &str,
        tag: &str,
        increment: i32,
    ) -> Result<&Bullet, PlaybookError> {
        let bullet = self
            .bullets
            .get_mut(bullet_id)
            .ok_or_else(|| PlaybookError::BulletNotFound(bullet_id.to_string()))?;

        bullet.tag(tag, increment)?;
        Ok(bullet)
    }

    pub fn remove_bullet(&mut self, bullet_id: &str) -> Option<Bullet> {
        let bullet = self.bullets.remove(bullet_id)?;

        if let Some(section_ids) = self.sections.get_mut(&bullet.section) {
            section_ids.retain(|id| id != bullet_id);
            if section_ids.is_empty() {
                self.sections.remove(&bullet.section);
            }
        }
        Some(bullet)
    }

    pub fn get_bullet(&self, bullet_id: &str) -> Option<&Bullet> {
        self.bullets.get(bullet_id)
    }

    pub fn bullets(&self) -> Vec<&Bullet> {
        self.bullets.values().collect()
    }

    // --------------------------
    // Delta批量操作（对齐Python功能）
    // --------------------------

    /// 应用Delta批量操作（添加/更新/标签/删除）
    pub fn apply_delta(&mut self, delta: DeltaBatch) -> Result<(), PlaybookError> {
        for operation in delta.operations {
            self._apply_operation(operation)?;
        }
        Ok(())
    }

    /// 执行单个Delta操作
    fn _apply_operation(&mut self, op: DeltaOperation) -> Result<(), PlaybookError> {

        match op.type_ {
            OperationType::Add => {
                let metadata = if op.metadata.is_empty() {
                    None
                } else {
                    Some(
                        op.metadata
                            .into_iter()
                            .map(|(k, v)| (k, v.max(0) as u32))
                            .collect::<BTreeMap<_,_>>(),
                    )
                };

                self.add_bullet(
                    op.section,
                    op.content.unwrap_or_default(),
                    op.bullet_id,
                    metadata,
                );
                Ok(())
            }

            OperationType::Update => {
                let bullet_id = op.bullet_id.ok_or_else(|| {
                    PlaybookError::DeltaMissingField("bullet_id required for UPDATE".to_string())
                })?;

                let metadata = if op.metadata.is_empty() {
                    None
                } else {
                    Some(
                        op.metadata
                            .into_iter()
                            .map(|(k, v)| (k, v.max(0) as u32))
                            .collect::<BTreeMap<_, _>>(),
                    )
                };

                self.update_bullet(&bullet_id, op.content, metadata)?;
                Ok(())
            }

            OperationType::Tag => {
                let bullet_id = op.bullet_id.ok_or_else(|| {
                    PlaybookError::DeltaMissingField("bullet_id required for TAG".to_string())
                })?;

                // 批量应用标签增量
                for (tag, increment) in op.metadata {
                    self.tag_bullet(&bullet_id, &tag, increment)?;
                }
                Ok(())
            }

            OperationType::Remove => {
                let bullet_id = op.bullet_id.ok_or_else(|| {
                    PlaybookError::DeltaMissingField("bullet_id required for REMOVE".to_string())
                })?;

                self.remove_bullet(&bullet_id);
                Ok(())
            }

        }
    }

    // --------------------------
    // 序列化/反序列化（对齐Python）
    // --------------------------

    /// 转换为JSON字符串（带格式化，易读）
    pub fn to_json(&self) -> Result<String, PlaybookError> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// 从JSON字符串解析Playbook
    pub fn from_json(data: &str) -> Result<Self, PlaybookError> {
        serde_json::from_str(data)
            .map_err(|e| PlaybookError::InvalidData(format!("Failed to parse JSON: {}", e)))
    }

    /// 保存到文件（自动创建父目录）
    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), PlaybookError> {
        let path = path.as_ref();
        // 自动创建父目录（避免文件路径不存在报错）
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let json = self.to_json()?;
        fs::write(path, json)?;
        Ok(())
    }

    /// 从文件加载（处理文件不存在的情况）
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, PlaybookError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(PlaybookError::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Playbook file not found: {}", path.display()),
            )));
        }

        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        self::Playbook::from_json(&contents)
    }

    // --------------------------
    // 辅助方法（对齐Python）
    // --------------------------

    /// 转换为LLM提示词格式（有序输出章节和子弹）
    pub fn as_prompt(&self) -> String {
        let mut parts = Vec::new();

        // 章节按字母排序（保证输出一致性，对齐Python的sorted）
        let mut sorted_sections: Vec<_> = self.sections.keys().collect();
        sorted_sections.sort();

        for section in sorted_sections {
            parts.push(format!("## {}", section));

            // 子弹按插入顺序输出（HashMap的values顺序不保证，但章节内的ID列表是插入顺序）
            if let Some(bullet_ids) = self.sections.get(section) {
                for bullet_id in bullet_ids {
                    if let Some(bullet) = self.bullets.get(bullet_id) {
                        let counters = format!(
                            "(helpful={}, harmful={}, neutral={})",
                            bullet.helpful, bullet.harmful, bullet.neutral
                        );
                        parts.push(format!("- [{}] {} {}", bullet.id, bullet.content, counters));
                    }
                }
            }
        }

        parts.join("\n")
    }

    /// 获取统计信息（有序输出，用BTreeMap保证JSON字段顺序）
    pub fn stats(&self) -> BTreeMap<String, serde_json::Value> {
        let mut tags = BTreeMap::new();
        tags.insert(
            "helpful".to_string(),
            serde_json::Value::Number(
                self.bullets
                    .values()
                    .map(|b| b.helpful as u64)
                    .sum::<u64>()
                    .into(),
            ),
        );
        tags.insert(
            "harmful".to_string(),
            serde_json::Value::Number(
                self.bullets
                    .values()
                    .map(|b| b.harmful as u64)
                    .sum::<u64>()
                    .into(),
            ),
        );
        tags.insert(
            "neutral".to_string(),
            serde_json::Value::Number(
                self.bullets
                    .values()
                    .map(|b| b.neutral as u64)
                    .sum::<u64>()
                    .into(),
            ),
        );

        let mut stats = BTreeMap::new();
        stats.insert(
            "sections".to_string(),
            serde_json::Value::Number(self.sections.len().into()),
        );
        stats.insert(
            "bullets".to_string(),
            serde_json::Value::Number(self.bullets.len().into()),
        );

        let tags_map: serde_json::Map<String, serde_json::Value> = tags.into_iter().collect();

        stats.insert("tags".to_string(), serde_json::Value::Object(tags_map));

        stats
    }

    fn generate_id(&mut self, section: &str) -> String {
        self.next_id += 1;
        let section_prefix = section
            .split_whitespace()
            .next()
            .unwrap_or("default")
            .to_lowercase();
        format!("{}-{:05}", section_prefix, self.next_id)
    }

}

#[cfg(test)]
mod tests { 
    use super::*;
    #[test]
    fn test_stats() {
        let mut pb = Playbook::new();

        pb.add_bullet(
            "测试章节".to_string(),
            "测试内容1".to_string(),
            None,
            Some(BTreeMap::from([("helpful".to_string(), 2), ("harmful".to_string(), 1)])),
        );
        pb.add_bullet(
            "测试章节2".to_string(),
            "测试内容2".to_string(),
            None,
            Some(BTreeMap::from([("neutral".to_string(), 3)])),
        );

        let stats = pb.stats();
        assert_eq!(stats.get("sections").unwrap(), &serde_json::Value::Number(2.into()));
        assert_eq!(stats.get("bullets").unwrap(), &serde_json::Value::Number(2.into()));

        // 验证 tags 顺序和值
        let tags = stats.get("tags").unwrap().as_object().unwrap();
        assert_eq!(tags.get("helpful").unwrap(), &serde_json::Value::Number(2.into()));
        assert_eq!(tags.get("harmful").unwrap(), &serde_json::Value::Number(1.into()));
        assert_eq!(tags.get("neutral").unwrap(), &serde_json::Value::Number(3.into()));
    }
}