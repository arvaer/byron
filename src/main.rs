use std::sync::Arc;

use lsm::lsm_database::LsmDatabase;
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, BufReader};

#[derive(Default, Debug)]
pub struct WorkloadStats {
    pub total_lines: usize,
    pub put_success: usize,
    pub put_fail: usize,
    pub get_success: usize,
    pub get_fail: usize,
    pub delete_success: usize,
    pub delete_fail: usize,
    pub range_success: usize,
    pub range_fail: usize,
    pub parse_errors: usize,
    pub unknown_commands: usize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parent_directory = "./data".to_string();
    let mut byron = Arc::new(LsmDatabase::new(parent_directory, None));

    let file = tokio::fs::File::open("workload.txt".to_string()).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut stats = WorkloadStats::default();

    while let Some(line) = lines.next_line().await? {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "p" if parts.len() == 3 => {
                let key = parts[1].parse::<i64>()?;
                let value = parts[2].parse::<i64>()?;
                match byron.put(key.to_string(), value.to_string()).await {
                    Ok(_) => stats.put_success += 1,
                    Err(e) => {
                        stats.put_fail += 1;
                        eprintln!("Failed to handle put({}, {}): {}", key, value, e);
                    }
                }
            }
            "g" if parts.len() == 2 => match parts[1].parse::<i64>() {
                Ok(key) => match byron.get(key.to_string()) {
                    Ok(target) => {
                        println!("GET {} -> {}", key, target.value);
                    }
                    Err(e) => {
                        stats.get_fail += 1;
                        eprintln!("Failed to handle get({}): {}", key, e);
                    }
                },
                Err(_) => stats.parse_errors += 1,
            },
            "d" if parts.len() == 2 => match parts[1].parse::<i64>() {
                Ok(key) => match  byron.delete(key.to_string()).await {
                    Ok(_) => stats.delete_success += 1,
                    Err(e) => {
                        stats.delete_fail += 1;
                        eprintln!("Failed to handle delete({}): {}", key, e);
                    }
                },
                Err(_) => stats.parse_errors += 1,
            },
            "r" if parts.len() == 3 => {
                let from = parts[1].parse::<i64>()?;
                let to = parts[2].parse::<i64>()?;

                match byron.range(from.to_string(), to.to_string()) {
                    Ok(target) => {
                        for value in target {
                            println!("{} -> {}", value.key, value.value);
                        }
                        stats.range_success += 1;
                    }
                    Err(e) => {
                        stats.range_fail += 1;
                        eprintln!("Failed to handle range({}, {}): {}", from, to, e);
                    }
                }
            }
            _ => {
                stats.unknown_commands += 1;
                eprintln!("Unknown or malformed command: {:?}", parts);
            }
        }
    }

    println!("\n=== Workload Summary ===");
    println!("Total lines:            {}", stats.total_lines);
    println!(
        "PUT:    {} success / {} fail",
        stats.put_success, stats.put_fail
    );
    println!(
        "GET:    {} success / {} fail",
        stats.get_success, stats.get_fail
    );
    println!(
        "DELETE: {} success / {} fail",
        stats.delete_success, stats.delete_fail
    );
    println!(
        "RANGE:  {} success / {} fail",
        stats.range_success, stats.range_fail
    );
    println!("Parse errors:           {}", stats.parse_errors);
    println!("Unknown commands:       {}", stats.unknown_commands);

    Ok(())

    // Time 10,000,000 writes.
}
