use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use crate::models::{FileHeader, HEADER_SIZE, IndexEntry};
use crate::error::{Result, DbError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

#[cfg(unix)]
use std::os::unix::fs::FileExt as UnixFileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as WindowsFileExt;

/// Cross-platform trait for reading at an offset
pub trait StorageFileExt {
    fn read_exact_at_offset(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()>;
}

#[cfg(unix)]
impl StorageFileExt for File {
    fn read_exact_at_offset(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        UnixFileExt::read_exact_at(self, buf, offset)
    }
}

#[cfg(windows)]
impl StorageFileExt for File {
    fn read_exact_at_offset(&self, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
        while !buf.is_empty() {
            match WindowsFileExt::seek_read(self, buf, offset) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"))
        } else {
            Ok(())
        }
    }
}

/// 处理 data.log 和 vectors.bin 的底层文件 I/O。
///
/// 使用 `StorageFileExt` trait (Unix pread / Windows seek_read) 支持并发读取，不再需要 Mutex 保护文件句柄。
/// 这允许在持有读锁时进行真正的并行读取。
pub struct Storage {
    data_file: Option<File>,
    vector_file: Option<File>,
    pub dimension: u32,
    pub path: PathBuf,
}

impl Storage {
    /// 打开或创建存储文件。
    pub fn new<P: AsRef<Path>>(path: P, dimension: u32) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let data_path = path.join("data.log");
        let vector_path = path.join("vectors.bin");

        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)?;

        let mut vector_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&vector_path)?;

        // 检查或写入头部
        if data_file.metadata()?.len() == 0 {
            let header = FileHeader::new(dimension);
            header.write(&mut data_file)?;
            data_file.sync_all()?;
        } else {
            data_file.seek(SeekFrom::Start(0))?;
            let _header = FileHeader::read(&mut data_file)?;
        }

        if vector_file.metadata()?.len() == 0 {
            let header = FileHeader::new(dimension);
            header.write(&mut vector_file)?;
            vector_file.sync_all()?;
        } else {
            vector_file.seek(SeekFrom::Start(0))?;
            let header = FileHeader::read(&mut vector_file)?;
            if header.dimension != dimension {
                return Err(DbError::DimensionMismatch {
                    expected: dimension,
                    got: header.dimension,
                });
            }
        }

        // 定位到末尾以进行追加
        data_file.seek(SeekFrom::End(0))?;
        vector_file.seek(SeekFrom::End(0))?;

        Ok(Self {
            data_file: Some(data_file),
            vector_file: Some(vector_file),
            dimension,
            path,
        })
    }

    /// 关闭文件句柄。
    pub fn close(&mut self) -> Result<()> {
        if let Some(f) = self.data_file.take() {
            f.sync_all()?;
        }
        if let Some(f) = self.vector_file.take() {
            f.sync_all()?;
        }
        Ok(())
    }

    /// 向 data.log 追加日志记录（Put 或 Delete）。
    /// 返回记录的偏移量。
    pub fn append_log(&mut self, id: u32, key: &str, value: &serde_json::Value, tombstone: bool) -> Result<u64> {
        let file = self.data_file.as_mut().ok_or(DbError::FileNotOpen)?;
        
        let offset = file.seek(SeekFrom::End(0))?;
        
        let key_bytes = key.as_bytes();
        let val_str = serde_json::to_string(value)?;
        let val_bytes = val_str.as_bytes();
        
        let key_len = key_bytes.len() as u32;
        let val_len = val_bytes.len() as u32;
        let tomb_byte = if tombstone { 1u8 } else { 0u8 };

        // 计算校验和
        let mut hasher = Hasher::new();
        hasher.update(&id.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&val_len.to_be_bytes());
        hasher.update(&[tomb_byte]);
        hasher.update(key_bytes);
        hasher.update(val_bytes);
        let checksum = hasher.finalize();

        // 写入文件
        file.write_u32::<BigEndian>(checksum)?;
        file.write_u32::<BigEndian>(id)?;
        file.write_u32::<BigEndian>(key_len)?;
        file.write_u32::<BigEndian>(val_len)?;
        file.write_u8(tomb_byte)?;
        file.write_all(key_bytes)?;
        file.write_all(val_bytes)?;
        
        file.sync_all()?;

        Ok(offset)
    }

    /// 向 vectors.bin 追加向量。
    /// 返回向量的 ID。
    pub fn append_vector(&mut self, vector: &[f32]) -> Result<u32> {
        if vector.len() as u32 != self.dimension {
            return Err(DbError::DimensionMismatch {
                expected: self.dimension,
                got: vector.len() as u32,
            });
        }

        let file = self.vector_file.as_mut().ok_or(DbError::FileNotOpen)?;

        let current_len = file.metadata()?.len();
        // 根据文件位置计算 ID
        let id = ((current_len - HEADER_SIZE as u64) / (self.dimension as u64 * 4)) as u32;

        for &val in vector {
            file.write_f32::<BigEndian>(val)?;
        }
        file.sync_all()?;

        Ok(id)
    }

    /// 更新 vectors.bin 中的现有向量。
    ///
    /// 用于复用已删除向量的空间。
    /// 注意：此操作会修改文件中间的内容，需要确保 ID 是有效的。
    pub fn update_vector(&mut self, id: u32, vector: &[f32]) -> Result<()> {
        if vector.len() as u32 != self.dimension {
            return Err(DbError::DimensionMismatch {
                expected: self.dimension,
                got: vector.len() as u32,
            });
        }

        let file = self.vector_file.as_mut().ok_or(DbError::FileNotOpen)?;
        let offset = HEADER_SIZE as u64 + (id as u64 * self.dimension as u64 * 4);
        
        file.seek(SeekFrom::Start(offset))?;
        for &val in vector {
            file.write_f32::<BigEndian>(val)?;
        }
        file.sync_all()?;
        
        Ok(())
    }

    /// 从指定偏移量读取日志记录。
    pub fn read_log_record(&self, offset: u64) -> Result<(u32, String, serde_json::Value, bool)> {
        let file = self.data_file.as_ref().ok_or(DbError::FileNotOpen)?;
        
        // 1. 读取头部 (4+4+4+4+1 = 17 字节)
        let mut header = [0u8; 17];
        file.read_exact_at_offset(&mut header, offset)?;
        
        let checksum = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let id = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        let key_len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
        let val_len = u32::from_be_bytes([header[12], header[13], header[14], header[15]]);
        let tombstone = header[16];
        
        // 2. 读取数据
        let mut data = vec![0u8; (key_len + val_len) as usize];
        file.read_exact_at_offset(&mut data, offset + 17)?;
        
        let (key_bytes, val_bytes) = data.split_at(key_len as usize);
        let key = String::from_utf8(key_bytes.to_vec()).map_err(|_| DbError::Corruption("Invalid UTF-8 key".into()))?;
        let value: serde_json::Value = serde_json::from_slice(val_bytes)?;

        // 验证校验和
        let mut hasher = Hasher::new();
        hasher.update(&id.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&val_len.to_be_bytes());
        hasher.update(&[tombstone]);
        hasher.update(key.as_bytes());
        hasher.update(val_bytes);
        
        if hasher.finalize() != checksum {
            return Err(DbError::Corruption("Checksum mismatch".into()));
        }

        Ok((id, key, value, tombstone == 1))
    }

    /// 将 vectors.bin 中的所有向量加载到内存中。
    pub fn load_vectors(&mut self) -> Result<Vec<f32>> {
        let file = self.vector_file.as_mut().ok_or(DbError::FileNotOpen)?;
        
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let mut vectors = Vec::with_capacity(buffer.len() / 4);
        let mut cursor = std::io::Cursor::new(buffer);
        
        while let Ok(val) = cursor.read_f32::<BigEndian>() {
            vectors.push(val);
        }
        
        Ok(vectors)
    }

    /// 辅助函数：从当前流位置读取下一条记录。
    fn read_record_from_stream(file: &mut File) -> Result<(u32, String, serde_json::Value, bool)> {
        let checksum = file.read_u32::<BigEndian>()?;
        let id = file.read_u32::<BigEndian>()?;
        let key_len = file.read_u32::<BigEndian>()?;
        let val_len = file.read_u32::<BigEndian>()?;
        let tombstone = file.read_u8()?;
        
        let mut key_buf = vec![0u8; key_len as usize];
        file.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf).map_err(|_| DbError::Corruption("Invalid UTF-8 key".into()))?;
        
        let mut val_buf = vec![0u8; val_len as usize];
        file.read_exact(&mut val_buf)?;
        let value: serde_json::Value = serde_json::from_slice(&val_buf)?;

        // 验证校验和
        let mut hasher = Hasher::new();
        hasher.update(&id.to_be_bytes());
        hasher.update(&key_len.to_be_bytes());
        hasher.update(&val_len.to_be_bytes());
        hasher.update(&[tombstone]);
        hasher.update(key.as_bytes());
        hasher.update(&val_buf);
        
        if hasher.finalize() != checksum {
            return Err(DbError::Corruption("Checksum mismatch".into()));
        }

        Ok((id, key, value, tombstone == 1))
    }

    /// 扫描 data.log 和 vectors.bin 以恢复索引并验证一致性。
    /// 
    /// 恢复过程：
    /// 1. 扫描 data.log，读取每条记录的 ID、Key 和 Tombstone。
    /// 2. 使用日志中的 ID 重建内存索引。
    /// 3. 检查日志中引用的最大 ID 是否超出 vectors.bin 的范围（对齐检查）。
    pub fn scan_and_recover(&mut self) -> Result<(HashMap<String, IndexEntry>, Vec<f32>)> {
        // 1. 对齐 vectors.bin
        let vec_file = self.vector_file.as_mut().ok_or(DbError::FileNotOpen)?;
        
        let vec_file_len = vec_file.metadata()?.len();
        let vec_data_len = vec_file_len.saturating_sub(HEADER_SIZE as u64);
        let vec_bytes = self.dimension as u64 * 4;
        let remainder = vec_data_len % vec_bytes;
        if remainder != 0 {
            // 截断部分写入的向量
            let new_len = vec_file_len - remainder;
            vec_file.set_len(new_len)?;
            vec_file.sync_all()?;
        }
        let disk_vec_count = (vec_data_len / vec_bytes) as usize;

        // 2. 扫描 data.log
        let data_file = self.data_file.as_mut().ok_or(DbError::FileNotOpen)?;
        
        data_file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        let mut valid_offset = HEADER_SIZE as u64;
        let mut max_id = -1i64;
        
        let mut temp_index: HashMap<String, IndexEntry> = HashMap::new();

        loop {
            let start_offset = data_file.stream_position()?;
            match Self::read_record_from_stream(data_file) {
                Ok((id, key, _val, tombstone)) => {
                    let end_offset = data_file.stream_position()?;
                    
                    max_id = max_id.max(id as i64);

                    if tombstone {
                        if let Some(entry) = temp_index.get_mut(&key) {
                            entry.deleted = true;
                        }
                    } else {
                        temp_index.insert(key.clone(), IndexEntry {
                            id,
                            data_offset: start_offset,
                            deleted: false,
                        });
                    }
                    valid_offset = end_offset;
                }
                Err(DbError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // 文件末尾，正常退出
                    break;
                }
                Err(e) => {
                    // 其他错误（如校验和不匹配、UTF-8 错误等），视为文件损坏，停止并截断
                    log::warn!("Recovering from corruption at offset {}: {}", start_offset, e);
                    break;
                }
            }
        }
        
        // 如果需要，截断 data.log（损坏/部分写入）
        if valid_offset < data_file.metadata()?.len() {
             data_file.set_len(valid_offset)?;
             data_file.sync_all()?;
        }
        
        // 3. 对齐检查
        // 采用 "Vector First, Log Last" 写入策略。
        // 检查日志中引用的最大 ID 是否超出 vectors.bin 的范围。
        if max_id >= disk_vec_count as i64 {
             return Err(DbError::Corruption(
                format!(
                    "Data corruption detected: log references vector ID {} but vector file only has {} vectors.",
                    max_id, disk_vec_count
                )
            ));
        }
        
        // 如果 vectors.bin 比 max_id 大很多，可能是崩溃导致日志没写进去。
        // 我们可以选择截断 vectors.bin 到 max_id + 1，或者保留（作为未引用的垃圾数据）。
        // 为了保持一致性，截断是比较安全的做法，但考虑到 ID 复用，中间可能有空洞。
        // 实际上，只要 disk_vec_count > max_id，说明 vectors.bin 足够大，是安全的。
        // 只有当 disk_vec_count <= max_id 时才是严重错误。
        
        // 加载向量
        let vectors = self.load_vectors()?;
        
        Ok((temp_index, vectors))
    }
}
