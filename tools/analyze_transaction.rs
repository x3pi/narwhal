// Tool để phân tích transaction hash
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

fn main() {
    let tx_hash = "bb6406ec7fcd291a08b8e22ccdd043a185b867d6ef8edde9434835e7e907765b";
    
    println!("=== Phân tích Transaction Hash ===");
    println!("Hash: {}", tx_hash);
    println!("Độ dài: {} ký tự ({} bytes)", tx_hash.len(), tx_hash.len() / 2);
    println!();
    
    // Kiểm tra format
    if tx_hash.len() != 64 {
        println!("⚠️  Cảnh báo: Hash không đúng độ dài (phải là 64 ký tự hex)");
    }
    
    // Tìm kiếm trong log files
    let log_dir = Path::new("benchmark/logs");
    if !log_dir.exists() {
        println!("❌ Không tìm thấy thư mục log: {:?}", log_dir);
        return;
    }
    
    println!("=== Tìm kiếm trong log files ===");
    let mut found = false;
    
    if let Ok(entries) = fs::read_dir(log_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("log") {
                    if let Ok(file) = fs::File::open(&path) {
                        let reader = BufReader::new(file);
                        let mut line_num = 0;
                        for line in reader.lines() {
                            line_num += 1;
                            if let Ok(line) = line {
                                if line.contains(tx_hash) {
                                    found = true;
                                    println!("✅ Tìm thấy tại: {:?}, dòng {}", path, line_num);
                                    println!("   {}", line);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    if !found {
        println!("❌ Không tìm thấy transaction hash trong log files");
        println!();
        println!("=== Các khả năng ===");
        println!("1. Transaction chưa được gửi đến worker");
        println!("2. Hash được tính khác ở client-side");
        println!("3. Log đã bị rotate hoặc xóa");
        println!("4. Transaction được gửi đến worker khác");
        println!();
        println!("=== Khuyến nghị ===");
        println!("- Kiểm tra client-side logs");
        println!("- Kiểm tra network traffic");
        println!("- So sánh cách tính hash ở client vs worker");
        println!("- Kiểm tra tất cả worker logs");
    }
    
    // Phân tích hash
    println!();
    println!("=== Phân tích Hash ===");
    let prefix = &tx_hash[0..8];
    let suffix = &tx_hash[56..64];
    println!("Prefix (8 ký tự đầu): {}", prefix);
    println!("Suffix (8 ký tự cuối): {}", suffix);
    
    // Tìm các transaction có prefix/suffix tương tự
    println!();
    println!("=== Tìm transaction có prefix/suffix tương tự ===");
    if let Ok(entries) = fs::read_dir(log_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("log") {
                    if let Ok(file) = fs::File::open(&path) {
                        let reader = BufReader::new(file);
                        for line in reader.lines() {
                            if let Ok(line) = line {
                                if line.contains(&format!("Hash: {}", prefix)) || 
                                   line.contains(&format!("Hash={}", prefix)) ||
                                   line.contains(&format!("Hash: {}", suffix)) ||
                                   line.contains(&format!("Hash={}", suffix)) {
                                    println!("Tìm thấy hash tương tự trong {:?}:", path);
                                    println!("  {}", line);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

