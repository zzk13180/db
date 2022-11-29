# DB

本项目实现了一个简单、崩溃安全、嵌入式的 Rust 向量数据库。它专为中小型应用（最多 10万向量）设计，优先考虑简单性、可靠性和易集成性，而不是大规模扩展性。

## 特性

*   **极简设计**: 核心代码 < 1000 行，零外部服务依赖。
*   **高性能**: 暴力搜索优化，Top-K 堆排序，SIMD 距离计算，< 10ms 延迟 (100k 向量)。
*   **数据安全**: Append-only 日志结构，崩溃恢复，CRC32 校验，"Vector First" 写入保证一致性。
*   **并发友好**: 细粒度读写锁 (RwLock) + `pread` (FileExt)，支持高并发读取。
*   **空间高效**: 自动空间复用 (Free List) 和后台压缩机制，防止磁盘膨胀。
*   **SSD 友好**: 日志文件纯顺序写入，向量文件支持原地更新，减少磁盘磨损。
*   **易于集成**: Rust 库，直接嵌入应用。

## 架构设计

### 1. 设计理念

#### KISS 原则 (Keep It Simple, Stupid)
通过减少逻辑分支和状态管理提高稳定性。核心代码保持在可审计范围内，无 `unsafe` 代码块。

#### 操作系统优先 (OS over App)
依赖操作系统提供的文件系统原子性、Page Cache 和 fsync，而不是在应用层重新实现缓存和复杂的 WAL。

#### 通过对齐实现崩溃安全 (Crash Safety via Alignment)
利用双文件追加写入的特性，通过启动时的"对齐检查"实现崩溃恢复，无需额外的 WAL 文件。数据文件本身即日志。

#### Rust 安全性 (Rust Safety)
利用 Rust 的所有权机制和类型系统确保内存安全，利用 `std::fs` 和 `std::io` 进行稳健的 I/O 操作。

#### 反囤积设计 (Anti-Hoarding)
技术限制服务于产品哲学。容量上限隐式地鼓励用户追求"高信噪比"的思考与记录，拒绝成为互联网的"搬运工"。系统旨在**连接灵感**，而非堆积资料。

### 2. 系统架构

#### 2.1 设计决策与权衡

*   **放弃 HNSW 索引，选择暴力搜索**: 
    *   HNSW 极其复杂，内存占用巨大。
    *   暴力搜索在 < 10万规模下延迟 < 10ms，且零索引构建时间，零额外内存。
*   **放弃 LSM-Tree/B+ 树，选择 Append-only Log**: 
    *   顺序磁盘写入是 SSD 的最佳模式，无需处理页面分裂和碎片整理。
*   **放弃 Buffer Pool，信任 OS**: 
    *   直接使用堆分配 (`Vec<f32>`)，利用 OS Page Cache。
*   **数据即日志**: 
    *   数据文件本身即追加日志，无需独立的 WAL。
*   **同步 I/O + RwLock + pread**: 
    *   `RwLock` 保护内存结构，底层使用 `pread` 进行无锁文件读取，支持高并发读。

#### 2.2 存储架构

系统物理上由两个文件组成，通过**索引位置**隐式关联。

**文件头 (File Header)**:
所有数据文件均以 32 字节 Header 开头 (Magic, Version, Dimension)。

**向量文件 (`vectors.bin`)**:
*   紧凑存储向量数据，SIMD 计算友好。
*   格式: `[File Header] [Vector 0] [Vector 1] ...`
*   启动时全量读取到 `Vec<f32>`。

**数据文件 (`data.log`)**:
*   存储 Key 和 JSON Value，作为 Source of Truth。
*   格式: `[File Header] [Record 0] [Record 1] ...`
*   单条 Record 结构（Big Endian）：
    ```text
    +----------+-------+---------+---------+-----------+----------+----------+
    | Checksum | ID    | Key Len | Val Len | Tombstone | Key      | Value    |
    | (4 B)    | (4 B) | (4 B)   | (4 B)   | (1 B)     | (var)    | (var)    |
    +----------+-------+---------+---------+-----------+----------+----------+
    ```

#### 2.3 空间管理与压缩

*   **Free List (空闲列表)**: 维护已删除向量的 ID，插入新数据时优先复用，防止向量文件膨胀。
*   **自动压缩 (Auto Compaction)**: 当删除比例超过阈值（默认 50%）时触发后台压缩，采用 Stop-the-world 策略原子替换文件。

#### 2.4 搜索优化

*   **Top-K 堆排序**: 使用最小堆 (Min-Heap) 维护 K 个最近邻，避免全量排序。
*   **SIMD 距离计算**: 手动循环展开，利用 CPU 自动向量化。

#### 2.5 内存架构

*   **索引 (Index)**: `HashMap<String, IndexEntry>`，映射 Key 到 ID 和文件偏移。
*   **向量存储**: `Vec<f32>`，扁平化数组，常驻内存。

### 3. 存储流程

#### 3.1 写入 (Put)
遵循 **"Vector First, Log Last"** 原则保证一致性。
1.  获取写锁。
2.  追加向量到 `vectors.bin` (或复用 Free List)。
3.  追加元数据到 `data.log`。
4.  更新内存索引。

#### 3.2 崩溃恢复 (Recovery)
启动时的对齐检查逻辑：
1.  **Check Header**: 验证 Magic 和 Version。
2.  **Align Vectors**: 计算 `vectors.bin` 中的有效记录数，截断部分写入。
3.  **Scan Log**: 解析 `data.log`，验证 Checksum，统计 max_id。
4.  **Consistency Check**: 
    *   若 `max_id >= vec_count`，报告数据损坏 (违反 Vector First 原则)。
    *   若 `max_id < vec_count`，正常 (忽略未提交的向量数据)。
5.  **Rebuild Index**: 重建 HashMap。

## 适用场景

*   ✅ 个人知识库 / 笔记应用
*   ✅ 桌面端 RAG (Retrieval-Augmented Generation)
*   ✅ 嵌入式设备上的轻量级搜索

## 限制

*   ❌ 不适合海量数据 (> 1M 向量)
*   ❌ 不支持高并发写入
*   ❌ 全量内存加载 (内存占用 ≈ 向量大小)
