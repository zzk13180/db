use std::io::{self, Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// 16进制的 "VECT" 魔数，用于标识文件类型。
pub const MAGIC: u32 = 0x56454354;

/// 当前文件格式版本。
pub const VERSION: u8 = 1;

/// 文件头大小（字节）。
pub const HEADER_SIZE: usize = 32;

/// 文件头结构，存在于 data.log 和 vectors.bin 的开头。
#[derive(Debug, Clone, Copy)]
pub struct FileHeader {
    /// 用于验证文件类型的魔数。
    pub magic: u32,
    /// 文件格式版本。
    pub version: u8,
    /// 标志位，预留给未来使用。
    pub flags: u8,
    /// 文件中存储的向量维度。
    pub dimension: u32,
}

impl FileHeader {
    /// 创建一个指定维度的 FileHeader。
    pub fn new(dimension: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            flags: 0,
            dimension,
        }
    }

    /// 将头部写入 writer。
    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32::<BigEndian>(self.magic)?;
        writer.write_u8(self.version)?;
        writer.write_u8(self.flags)?;
        writer.write_u32::<BigEndian>(self.dimension)?;
        // 填充至 32 字节
        writer.write_all(&[0u8; 22])?;
        Ok(())
    }

    /// 从 reader 读取头部。
    pub fn read<R: Read>(reader: &mut R) -> io::Result<Self> {
        let magic = reader.read_u32::<BigEndian>()?;
        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid magic number"));
        }
        let version = reader.read_u8()?;
        if version != VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported version"));
        }
        let flags = reader.read_u8()?;
        let dimension = reader.read_u32::<BigEndian>()?;
        let mut reserved = [0u8; 22];
        reader.read_exact(&mut reserved)?;
        
        Ok(Self {
            magic,
            version,
            flags,
            dimension,
        })
    }
}

/// 内存中的向量记录表示（不直接用于磁盘存储格式）。
#[derive(Debug, Clone)]
pub struct VectorRecord {
    pub id: u32,
    pub vector: Vec<f32>,
}

/// 内存索引条目，将键映射到其位置和状态。
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// vectors.bin 中的向量 ID（扁平数组中的索引）。
    pub id: u32,
    /// data.log 中数据记录的偏移量。
    pub data_offset: u64,
    /// 记录是否已删除。
    pub deleted: bool,
}

/// 自动压缩的默认阈值：删除比例。
pub const DEFAULT_COMPACT_RATIO_THRESHOLD: f64 = 0.5;

/// 自动压缩的默认阈值：删除数量。
pub const DEFAULT_COMPACT_COUNT_THRESHOLD: usize = 1000;

/// 数据库配置。
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// 向量维度。
    pub dimension: u32,
    /// 自动压缩的删除比例阈值（0.0 到 1.0）。
    pub compact_threshold_ratio: f64,
    /// 自动压缩的删除数量阈值。
    pub compact_threshold_count: usize,
    /// 是否启用自动压缩。
    pub enable_auto_compact: bool,
}

impl DbConfig {
    /// 创建具有默认设置的配置。
    pub fn new(dimension: u32) -> Self {
        Self {
            dimension,
            compact_threshold_ratio: DEFAULT_COMPACT_RATIO_THRESHOLD,
            compact_threshold_count: DEFAULT_COMPACT_COUNT_THRESHOLD,
            enable_auto_compact: true,
        }
    }

    /// 设置压缩阈值比例。
    pub fn with_compact_ratio(mut self, ratio: f64) -> Self {
        self.compact_threshold_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    /// 设置压缩阈值数量。
    pub fn with_compact_count(mut self, count: usize) -> Self {
        self.compact_threshold_count = count;
        self
    }

    /// 设置是否启用自动压缩。
    pub fn with_auto_compact(mut self, enabled: bool) -> Self {
        self.enable_auto_compact = enabled;
        self
    }
}
