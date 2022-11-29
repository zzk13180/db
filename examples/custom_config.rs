/// 演示如何使用自定义配置创建数据库
use db::db::Database;
use db::models::DbConfig;
use serde_json::json;
use std::path::Path;
use std::fs;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let data_dir = "./data_custom";

    // 1. 清理旧数据（确保示例从干净状态开始）
    if Path::new(data_dir).exists() {
        println!("Cleaning up old data directory...");
        fs::remove_dir_all(data_dir)?;
    }
    fs::create_dir_all(data_dir)?;

    // 2. 创建自定义配置
    let config = DbConfig::new(768)
        .with_compact_ratio(0.3)           // 删除比例达到 30% 时触发压缩
        .with_compact_count(500)            // 至少有 500 个删除记录时触发压缩
        .with_auto_compact(true);           // 启用自动压缩

    // 3. 使用配置打开数据库
    println!("Opening database with custom config...");
    let db = Database::open_with_config(data_dir, config.clone())?;

    // 4. 插入数据
    println!("Inserting 1000 documents...");
    for i in 0..1000 {
        let key = format!("doc_{}", i);
        let vector = vec![i as f32 / 1000.0; 768];
        let metadata = json!({
            "id": i,
            "title": format!("Document {}", i),
            "category": if i % 2 == 0 { "even" } else { "odd" }
        });
        
        db.put(key, vector, metadata)?;
    }

    // 5. 获取统计信息
    let stats = db.stats()?;
    println!("\n数据库统计 (插入后):");
    print_stats(&stats);

    // 6. 删除一些文档
    println!("\nDeleting 400 documents...");
    for i in 0..400 {
        db.delete(&format!("doc_{}", i))?;
    }
    
    // 7. 再次查看统计
    let stats = db.stats()?;
    println!("\n数据库统计 (删除后):");
    print_stats(&stats);

    // 8. 搜索相似向量
    let query = vec![0.5; 768];
    let results = db.search(&query, 5)?;
    
    println!("\n搜索结果 (Top 5):");
    for (key, distance) in results {
        let doc = db.get(&key)?;
        println!("  {} - 距离: {:.4}, 标题: {}", 
                 key, distance, doc["title"]);
    }

    // 9. 演示持久化：关闭并重新打开数据库
    println!("\nReopening database to verify persistence...");
    drop(db); // 显式释放 db 句柄

    let db = Database::open_with_config(data_dir, config)?;
    let stats = db.stats()?;
    println!("数据库统计 (重启后):");
    print_stats(&stats);

    // 验证数据仍然存在
    let doc_500 = db.get("doc_500")?;
    println!("Verified doc_500 exists: {}", doc_500["title"]);

    // 10. 手动触发压缩
    println!("\nTriggering manual compaction...");
    db.compact()?;

    let stats = db.stats()?;
    println!("数据库统计 (压缩后):");
    print_stats(&stats);

    // 清理
    fs::remove_dir_all(data_dir)?;
    println!("\nExample completed successfully.");

    Ok(())
}

fn print_stats(stats: &db::db::DbStats) {
    println!("  总向量数: {}", stats.total_vectors);
    println!("  活跃向量数: {}", stats.active_vectors);
    println!("  已删除向量数: {}", stats.deleted_vectors);
    println!("  删除比例: {:.2}%", stats.deletion_ratio * 100.0);
    println!("  索引大小: {}", stats.index_size);
    println!("  数据文件大小: {} bytes", stats.data_file_size);
    println!("  向量文件大小: {} bytes", stats.vector_file_size);
    println!("  Free list 大小: {}", stats.free_list_size);
}
