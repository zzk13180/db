use thiserror::Error;
use std::io;

/// 数据库自定义错误类型。
#[derive(Error, Debug)]
pub enum DbError {
    /// IO 错误包装。
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    /// 序列化/反序列化错误包装。
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    /// 数据损坏错误（例如：校验和不匹配，无效的 UTF-8）。
    #[error("Data corruption: {0}")]
    Corruption(String),
    
    /// 维度不匹配错误（例如：插入错误维度的向量）。
    #[error("Invalid dimension: expected {expected}, got {got}")]
    DimensionMismatch { expected: u32, got: u32 },
    
    /// 键未找到错误。
    #[error("Key not found: {0}")]
    NotFound(String),

    /// 数据库锁中毒（之前的操作 panic 了）
    #[error("Database lock is poisoned, data may be inconsistent")]
    LockPoisoned,

    /// 无效的向量（包含 NaN 或 Inf）
    #[error("Invalid vector: {0}")]
    InvalidVector(String),

    /// 文件未打开（内部错误）
    #[error("File not open")]
    FileNotOpen,

    /// 配置错误
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// DbError 的 Result 类型别名。
pub type Result<T> = std::result::Result<T, DbError>;
