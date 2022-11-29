use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::{HashMap, BinaryHeap};
use std::path::Path;
use std::cmp::Ordering as CmpOrdering;
use log::{info, warn, debug};
use crate::storage::Storage;
use crate::models::{IndexEntry, DbConfig};
use crate::error::{Result, DbError};
use serde_json::Value;

/// 数据库统计信息。
#[derive(Debug, Clone)]
pub struct DbStats {
    /// 总向量数（包括已删除）。
    pub total_vectors: usize,
    /// 已删除的向量数。
    pub deleted_vectors: usize,
    /// 活跃向量数。
    pub active_vectors: usize,
    /// 内存索引大小。
    pub index_size: usize,
    /// 数据文件大小（字节）。
    pub data_file_size: u64,
    /// 向量文件大小（字节）。
    pub vector_file_size: u64,
    /// 删除比例。
    pub deletion_ratio: f64,
    /// Free list 大小。
    pub free_list_size: usize,
}

/// 数据库的内部状态，由 RwLock 保护。
struct Inner {
    storage: Storage,
    /// 内存索引，将键映射到其位置。
    index: HashMap<String, IndexEntry>,
    /// 所有加载到内存中的向量，用于快速搜索。
    vectors: Vec<f32>,
    /// 从向量 ID 到键的反向映射。
    id_to_key: Vec<String>,
    /// 类似位图的向量，用于标记已删除的 ID。
    deleted: Vec<bool>,
    /// 可复用的已删除向量 ID 列表。
    free_list: Vec<u32>,
    /// 数据库配置。
    config: DbConfig,
}

/// 线程安全的数据库句柄。
#[derive(Clone)]
pub struct Database {
    inner: Arc<RwLock<Inner>>,
    compacting: Arc<AtomicBool>,
}

#[derive(PartialEq)]
struct OrderedFloat(f32);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.0.partial_cmp(&other.0).unwrap_or_else(|| {
            if self.0.is_nan() { CmpOrdering::Greater } else { CmpOrdering::Less }
        })
    }
}

#[derive(PartialEq, Eq)]
struct SearchItem {
    id: usize,
    dist_sq: OrderedFloat,
}

impl Ord for SearchItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.dist_sq.cmp(&other.dist_sq)
    }
}

impl PartialOrd for SearchItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Database {
    /// 在指定路径打开或创建数据库（使用默认配置）。
    pub fn open<P: AsRef<Path>>(path: P, dimension: u32) -> Result<Self> {
        Self::open_with_config(path, DbConfig::new(dimension))
    }

    /// 在指定路径打开或创建数据库，使用自定义配置。
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let dimension = config.dimension;

        if dimension == 0 || dimension > 100_000 {
             return Err(DbError::ConfigError(format!("Invalid dimension: {}", dimension)));
        }

        if config.compact_threshold_ratio < 0.0 || config.compact_threshold_ratio > 1.0 {
             return Err(DbError::ConfigError(format!("Invalid compact_threshold_ratio: {}", config.compact_threshold_ratio)));
        }

        info!("Opening database: path={:?}, dimension={}, auto_compact={}", 
              path, dimension, config.enable_auto_compact);

        // 检查是否存在未完成的压缩操作
        let temp_path = path.join("compact_temp");
        if temp_path.join(".compact_ready").exists() {
            warn!("Found incomplete compaction, completing it...");
            // 压缩已准备好但未完成，继续完成重命名
            if temp_path.join("data.log").exists() {
                 std::fs::rename(temp_path.join("data.log"), path.join("data.log"))?;
            }
            if temp_path.join("vectors.bin").exists() {
                 std::fs::rename(temp_path.join("vectors.bin"), path.join("vectors.bin"))?;
            }
            std::fs::remove_dir_all(&temp_path)?;
            info!("Completed incomplete compaction");
        } else if temp_path.exists() {
            warn!("Found interrupted compaction, cleaning up temporary files...");
            // 压缩在生成阶段中断，清理临时目录
            std::fs::remove_dir_all(&temp_path)?;
            info!("Cleaned up interrupted compaction");
        }

        let mut storage = Storage::new(path, dimension)?;
        let (index, vectors) = storage.scan_and_recover()?;
        
        let count = vectors.len() / dimension as usize;
        let mut id_to_key = vec![String::new(); count];
        // 默认认为所有 ID 都是已删除/空闲的，除非找到一个活跃的 Key 指向它
        let mut deleted = vec![true; count];
        
        // 从索引重建 id_to_key 和删除状态
        for (k, v) in &index {
            if (v.id as usize) < count {
                if !v.deleted {
                    // 找到活跃拥有者，标记为未删除
                    id_to_key[v.id as usize] = k.clone();
                    deleted[v.id as usize] = false;
                } else {
                    // 已删除记录。
                    // 只有当该 ID 目前被认为是已删除时（即尚未发现活跃拥有者），才更新 Key 映射。
                    // 这确保了如果 ID 被复用（有一个活跃 Key），我们不会用旧的已删除 Key 覆盖它。
                    if deleted[v.id as usize] {
                        id_to_key[v.id as usize] = k.clone();
                    }
                }
            }
        }
        
        // 构建 free_list
        let mut free_list = Vec::new();
        for (i, &is_deleted) in deleted.iter().enumerate() {
            if is_deleted {
                free_list.push(i as u32);
            }
        }
        
        info!("Database opened successfully: {} vectors ({} active, {} deleted)", 
              count, count - deleted.iter().filter(|&&d| d).count(), 
              deleted.iter().filter(|&&d| d).count());
        
        Ok(Self {
            inner: Arc::new(RwLock::new(Inner {
                storage,
                index,
                vectors,
                id_to_key,
                deleted,
                free_list,
                config,
            })),
            compacting: Arc::new(AtomicBool::new(false)),
        })
    }

    /// 插入向量及其关联的元数据。
    ///
    /// # 参数
    ///
    /// * `key` - 唯一标识符。如果键已存在，将覆盖旧数据。
    /// * `vector` - 浮点数向量，长度必须与数据库维度一致。
    /// * `value` - 关联的 JSON 元数据。
    ///
    /// # 错误
    ///
    /// * `DbError::DimensionMismatch` - 向量维度不匹配。
    /// * `DbError::InvalidVector` - 向量包含 NaN 或 Inf。
    pub fn put(&self, key: String, vector: Vec<f32>, value: Value) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
        
        // 1. 校验
        if vector.len() as u32 != inner.storage.dimension {
             return Err(DbError::DimensionMismatch {
                expected: inner.storage.dimension,
                got: vector.len() as u32,
            });
        }

        if vector.iter().any(|&v| !v.is_finite()) {
            return Err(DbError::InvalidVector("Vector contains NaN or Inf values".into()));
        }

        // 2. 写入向量
        // 优先复用已删除的空间 (free_list)，否则追加到文件末尾。
        // 这可以防止频繁更新导致的 vectors.bin 无限膨胀。
        let id = if let Some(free_id) = inner.free_list.pop() {
            inner.storage.update_vector(free_id, &vector)?;
            free_id
        } else {
            inner.storage.append_vector(&vector)?
        };

        // 3. 写入日志 (WAL)
        // 即使是更新操作，日志也是追加写入的。
        let offset = inner.storage.append_log(id, &key, &value, false)?;
        
        // 4. 更新内存索引
        // 如果键已存在且旧记录处于活跃状态：
        // - 标记旧 ID 为已删除
        // - 将旧 ID 加入 free_list 以便复用
        if let Some(old_entry) = inner.index.get(&key) {
            // 只有当旧记录未删除时才需要回收，避免重复 push 到 free_list
            if !old_entry.deleted {
                let old_id = old_entry.id as usize;
                if old_id < inner.deleted.len() {
                    inner.deleted[old_id] = true;
                    inner.free_list.push(old_id as u32);
                }
            }
        }

        // 更新内存中的向量缓存
        let dim = inner.storage.dimension as usize;
        if (id as usize) * dim < inner.vectors.len() {
            let start = (id as usize) * dim;
            inner.vectors[start..start + dim].copy_from_slice(&vector);
        } else {
            inner.vectors.extend(&vector);
        }

        // 确保辅助数组大小足够
        if inner.id_to_key.len() <= id as usize {
            inner.id_to_key.resize(id as usize + 1, String::new());
            inner.deleted.resize(id as usize + 1, false);
        }
        inner.id_to_key[id as usize] = key.clone();
        inner.deleted[id as usize] = false;

        inner.index.insert(key.clone(), IndexEntry {
            id,
            data_offset: offset,
            deleted: false,
        });


        // 检查是否需要自动压缩
        let deleted_count = inner.deleted.iter().filter(|&&d| d).count();
        let total_count = inner.deleted.len();
        let ratio = deleted_count as f64 / total_count.max(1) as f64;
        
        let should_compact = inner.config.enable_auto_compact 
            && ratio > inner.config.compact_threshold_ratio 
            && deleted_count > inner.config.compact_threshold_count;
        drop(inner); // 释放锁，避免阻塞后续操作
        
        if should_compact {
            warn!("Auto-compaction triggered: deleted={}/{} ({:.1}%)", 
                  deleted_count, total_count, ratio * 100.0);
            // 尝试启动后台压缩任务
            // 使用 AtomicBool 确保同一时间只有一个压缩任务在运行
            if !self.compacting.swap(true, Ordering::SeqCst) {
                let db = self.clone();
                std::thread::spawn(move || {
                    info!("Starting background compaction...");
                    if let Err(e) = db.compact() {
                        warn!("Compaction failed: {}", e);
                    } else {
                        info!("Background compaction completed successfully");
                    }
                    db.compacting.store(false, Ordering::SeqCst);
                });
            }
        }
        
        Ok(())
    }

    /// 根据键删除记录。
    ///
    /// 删除操作是逻辑删除：
    /// 1. 在 data.log 中追加一条墓碑记录 (Tombstone)。
    /// 2. 在内存中标记该 ID 为已删除。
    /// 3. 将 ID 加入 free_list 以便后续插入操作复用空间。
    pub fn delete(&self, key: &str) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
        
        let id = match inner.index.get(key) {
            Some(entry) if entry.deleted => {
                // 幂等：重复 delete 不再写 tombstone，也不重复回收 free_list
                return Ok(());
            }
            Some(entry) => entry.id,
            None => return Err(DbError::NotFound(key.to_string())),
        };

        // 1. 写入墓碑标记
        inner.storage.append_log(id, key, &Value::Null, true)?;
        
        // 2. 标记为已删除
        if let Some(entry) = inner.index.get_mut(key) {
            entry.deleted = true;
        }

        let id_usize = id as usize;
        if id_usize < inner.deleted.len() {
            inner.deleted[id_usize] = true;
            inner.free_list.push(id);
        }
        
        Ok(())
    }
    
    /// 获取与键关联的元数据。
    pub fn get(&self, key: &str) -> Result<Value> {
        let inner = self.inner.read().map_err(|_| DbError::LockPoisoned)?;
        let Inner { storage, index, .. } = &*inner;
        
        let (offset, is_deleted) = if let Some(entry) = index.get(key) {
            (entry.data_offset, entry.deleted)
        } else {
            return Err(DbError::NotFound(key.to_string()));
        };

        if is_deleted {
            return Err(DbError::NotFound(key.to_string()));
        }
        
        let (_, _, val, _) = storage.read_log_record(offset)?;
        Ok(val)
    }

    /// 执行数据库压缩。
    ///
    /// 压缩过程：
    /// 1. 创建新的临时存储文件。
    /// 2. 遍历内存索引，只将未删除的有效数据写入新文件。
    /// 3. 原子替换旧文件。
    /// 4. 重建内存结构（此时 free_list 会被清空，因为所有空洞都已移除）。
    ///
    /// 注意：此操作会获取全局写锁，阻塞所有读写操作。
    pub fn compact(&self) -> Result<()> {
        let mut inner = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
        let path = inner.storage.path.clone();
        let dimension = inner.storage.dimension;
        
        debug!("Starting compaction: path={:?}", path);
        
        // 创建新的存储文件
        let temp_path = path.join("compact_temp");
        if temp_path.exists() {
            std::fs::remove_dir_all(&temp_path)?;
        }
        let mut new_storage = Storage::new(&temp_path, dimension)?;
        
        let mut new_index = HashMap::new();
        let mut new_vectors = Vec::new();
        let mut new_id_to_key = Vec::new();
        let mut new_deleted = Vec::new();
        let new_free_list = Vec::new();
        
        // 遍历当前索引
        let mut entries: Vec<(String, IndexEntry)> = inner.index.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entries.sort_by_key(|(_, entry)| entry.id);
        
        let mut skipped_count = 0;
        
        for (key, entry) in &entries {
            if entry.deleted {
                skipped_count += 1;
                continue;
            }
            
            // 读取值
            let (_, _, value, _) = inner.storage.read_log_record(entry.data_offset)?;
            
            // 获取向量
            let vector = &inner.vectors[entry.id as usize * dimension as usize .. (entry.id as usize + 1) * dimension as usize];
            
            // 写入新存储
            let new_id = new_storage.append_vector(vector)?;
            let new_offset = new_storage.append_log(new_id, key, &value, false)?;
            
            // 更新新内存结构
            new_index.insert(key.clone(), IndexEntry {
                id: new_id,
                data_offset: new_offset,
                deleted: false,
            });
            new_vectors.extend_from_slice(vector);
            new_id_to_key.push(key.clone());
            new_deleted.push(false);
        }
        
        // 关闭文件并确保落盘
        new_storage.close()?;
        inner.storage.close()?;
        
        // 创建标记文件以指示新文件已准备就绪
        let ready_marker = temp_path.join(".compact_ready");
        let f = std::fs::File::create(&ready_marker)?;
        f.sync_all()?;
        drop(f);

        // 同步临时目录元数据
        #[cfg(unix)]
        {
            if let Ok(dir) = std::fs::File::open(&temp_path) {
                let _ = dir.sync_all();
            }
        }

        // 重命名文件
        if temp_path.join("data.log").exists() {
            std::fs::rename(temp_path.join("data.log"), path.join("data.log"))?;
        }
        if temp_path.join("vectors.bin").exists() {
            std::fs::rename(temp_path.join("vectors.bin"), path.join("vectors.bin"))?;
        }
        
        // 同步父目录
        #[cfg(unix)]
        {
            if let Ok(dir) = std::fs::File::open(&path) {
                let _ = dir.sync_all();
            }
        }

        std::fs::remove_dir_all(&temp_path)?;
        
        // 重新打开存储
        inner.storage = Storage::new(&path, dimension)?;
        
        let new_index_len = new_index.len();
        
        // 更新内存
        inner.index = new_index;
        inner.vectors = new_vectors;
        inner.id_to_key = new_id_to_key;
        inner.deleted = new_deleted;
        inner.free_list = new_free_list;
        
        info!("Compaction completed: {} active vectors (reclaimed {} deleted)", 
              new_index_len, skipped_count);
        
        Ok(())
    }

    /// 获取数据库统计信息。
    pub fn stats(&self) -> Result<DbStats> {
        let inner = self.inner.read().map_err(|_| DbError::LockPoisoned)?;
        
        let total_vectors = inner.deleted.len();
        let deleted_vectors = inner.deleted.iter().filter(|&&d| d).count();
        let active_vectors = total_vectors - deleted_vectors;
        let deletion_ratio = deleted_vectors as f64 / total_vectors.max(1) as f64;
        
        let data_file_size = std::fs::metadata(inner.storage.path.join("data.log"))
            .map(|m| m.len())
            .unwrap_or(0);
        let vector_file_size = std::fs::metadata(inner.storage.path.join("vectors.bin"))
            .map(|m| m.len())
            .unwrap_or(0);
        
        Ok(DbStats {
            total_vectors,
            deleted_vectors,
            active_vectors,
            index_size: inner.index.len(),
            data_file_size,
            vector_file_size,
            deletion_ratio,
            free_list_size: inner.free_list.len(),
        })
    }

    /// 搜索查询向量的 k 个最近邻。
    ///
    /// 使用暴力搜索计算所有未删除向量的欧氏距离。
    /// 优化：使用大小为 k 的最小堆 (Min-Heap) 维护 Top-K 结果，避免对所有结果进行全量排序。
    /// 时间复杂度：O(N * D + N * log k)，其中 N 是向量数量，D 是维度。
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(String, f32)>> {
        let inner = self.inner.read().map_err(|_| DbError::LockPoisoned)?;
        let dim = inner.storage.dimension as usize;
        
        if query.len() != dim {
             return Err(DbError::DimensionMismatch {
                expected: dim as u32,
                got: query.len() as u32,
            });
        }

        if query.iter().any(|&v| !v.is_finite()) {
            return Err(DbError::InvalidVector("Query vector contains NaN or Inf values".into()));
        }

        // 边界条件：空数据库
        if inner.deleted.is_empty() {
            return Ok(Vec::new());
        }

        // 使用最大堆（MaxHeap）来维护 k 个最小距离
        // 堆顶是这 k 个中最大的距离。如果遇到更小的距离，就弹出堆顶并插入新距离。
        let mut heap: BinaryHeap<SearchItem> = BinaryHeap::with_capacity(k + 1);
        
        // 遍历所有向量
        for i in 0..inner.deleted.len() {
            if inner.deleted[i] {
                continue;
            }
            
            let vector = &inner.vectors[i * dim .. (i + 1) * dim];
            let dist_sq = euclidean_distance_squared(query, vector);
            
            if heap.len() < k {
                heap.push(SearchItem { id: i, dist_sq: OrderedFloat(dist_sq) });
            } else if let Some(max_item) = heap.peek() {
                if dist_sq < max_item.dist_sq.0 {
                    heap.pop();
                    heap.push(SearchItem { id: i, dist_sq: OrderedFloat(dist_sq) });
                }
            }
        }
        
        // 提取结果并排序
        // BinaryHeap::into_sorted_vec 返回升序排列的元素 (min -> max)
        // 因为 BinaryHeap 是 MaxHeap，pop() 出来的是最大值，
        // into_sorted_vec 内部不断 pop 并反转，最终得到升序序列。
        let result = heap.into_sorted_vec(); 
        
        let result_vec: Vec<(String, f32)> = result.into_iter()
            .map(|item| (inner.id_to_key[item.id].clone(), item.dist_sq.0.sqrt()))
            .collect();
            
        Ok(result_vec)
    }
}

/// 计算两个向量之间的欧几里得距离平方。
///
/// 使用手动循环展开 (4路) 以提高计算性能。
#[inline]
fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0;
    let mut iter_a = a.chunks_exact(4);
    let mut iter_b = b.chunks_exact(4);

    for (ca, cb) in iter_a.by_ref().zip(iter_b.by_ref()) {
        let d0 = ca[0] - cb[0];
        let d1 = ca[1] - cb[1];
        let d2 = ca[2] - cb[2];
        let d3 = ca[3] - cb[3];
        sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3;
    }

    for (x, y) in iter_a.remainder().iter().zip(iter_b.remainder().iter()) {
        let diff = x - y;
        sum += diff * diff;
    }

    sum
}
