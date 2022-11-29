use db::db::Database;
use serde_json::json;

fn main() -> anyhow::Result<()> {
    // 确保数据目录存在且为空（为了演示目的，先清理）
    let data_dir = "./data_simple";
    if std::path::Path::new(data_dir).exists() {
        std::fs::remove_dir_all(data_dir)?;
    }
    std::fs::create_dir_all(data_dir)?;

    // 打开数据库 (指定数据目录和向量维度)
    let db = Database::open(data_dir, 768)?;

    // 插入数据
    db.put(
        "doc1".to_string(), 
        vec![0.1; 768], 
        json!({"title": "Hello World"})
    )?;

    // 搜索相似向量
    let results = db.search(&vec![0.1; 768], 5)?;
    for (key, distance) in results {
        println!("Found: {} (dist: {})", key, distance);
    }

    // 获取原始数据
    let doc = db.get("doc1")?;
    println!("Document: {}", doc);

    // 查看数据库统计信息
    let stats = db.stats()?;
    println!("Active vectors: {}/{}", stats.active_vectors, stats.total_vectors);

    // 压缩数据库 (清理已删除数据)
    db.compact()?;

    // 清理演示数据
    std::fs::remove_dir_all(data_dir)?;

    Ok(())
}
