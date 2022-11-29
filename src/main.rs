use db::db::Database;
use serde_json::json;
use std::path::Path;

/// 向量数据库的使用示例。
fn main() -> anyhow::Result<()> {
    let path = Path::new("./data");
    // 清理之前的运行数据
    if path.exists() {
        std::fs::remove_dir_all(path)?;
    }
    
    let dim = 4;
    let db = Database::open(path, dim)?;

    println!("Database opened at {:?}", path);

    // 插入数据
    db.put("vec1".to_string(), vec![1.0, 0.0, 0.0, 0.0], json!({"name": "vector 1"}))?;
    db.put("vec2".to_string(), vec![0.0, 1.0, 0.0, 0.0], json!({"name": "vector 2"}))?;
    db.put("vec3".to_string(), vec![0.5, 0.5, 0.0, 0.0], json!({"name": "vector 3"}))?;

    println!("Inserted 3 vectors.");

    // 搜索
    let query = vec![1.0, 0.0, 0.0, 0.0];
    let results = db.search(&query, 3)?;
    println!("Search results for [1, 0, 0, 0]:");
    for (key, dist) in results {
        println!("Key: {}, Dist: {}", key, dist);
    }

    // 删除
    db.delete("vec1")?;
    println!("Deleted vec1.");

    // 再次搜索
    let results = db.search(&query, 3)?;
    println!("Search results after delete:");
    for (key, dist) in results {
        println!("Key: {}, Dist: {}", key, dist);
    }
    
    // 获取
    let val = db.get("vec2")?;
    println!("Get vec2: {}", val);

    // 恢复测试
    println!("Reopening database...");
    drop(db); // Close db
    
    let db2 = Database::open(path, dim)?;
    let results = db2.search(&query, 3)?;
    println!("Search results after recovery:");
    for (key, dist) in results {
        println!("Key: {}, Dist: {}", key, dist);
    }

    Ok(())
}
