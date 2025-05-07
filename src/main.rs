use lsm::{lsm_compaction::Monkey, lsm_database::LsmDatabase};
use rand::Rng;
use std::time::Instant;

fn main() {
    env_logger::init();
    let parent_directory = "./data".to_string();
    let mut db = LsmDatabase::new(parent_directory, None);

    // Time 10,000,000 writes.
    let start_writes = Instant::now();
    for i in 0..5_000_000 {
        // Using 8-digit formatting so keys and values have consistent length.
        let key = format!("key-{:08}", i);
        let value = format!("value-{:08}", i);
        db.put(key, value).unwrap();
    }
    let duration_writes = start_writes.elapsed();
    println!("Inserted 1,000,000 entries in {:?}", duration_writes);

    // Time 10,000 random reads.
    let mut rng = rand::thread_rng();
    let mut found_count = 0;
    let mut error_count = 0;

    let start_reads = Instant::now();
    for _ in 0..500_000 {
        let random_index = rng.gen_range(0..2_500_000);
        let key = format!("key-{:08}", random_index);
        match db.get(key) {
            Ok(_) => found_count += 1,
            Err(_) => error_count += 1,
        }
    }
    let duration_reads = start_reads.elapsed();
    println!("Performed 100,000 random reads in {:?}", duration_reads);
    println!(
        "Found {} keys, encountered {} errors",
        found_count, error_count
    );

    let start_key = "key-00005000".to_string();
    let end_key = "key-00005010".to_string();

    let start_range = Instant::now();
    match db.range(start_key.clone(), end_key.clone()) {
        Ok(results) => {
            let elapsed = start_range.elapsed();
            println!(
                "Range query [{} … {}] returned {} entries in {:?}",
                start_key,
                end_key,
                results.len(),
                elapsed
            );
            for res in results {
                println!("Key: {:?} -> Value: {:?}", res.key, res.value);
            }
        }
        Err(e) => {
            println!("Range query [{} … {}] failed: {:?}", start_key, end_key, e);
        }
    }
}
