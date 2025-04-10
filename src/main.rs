use lsm::lsm_database::LsmDatabase;
use rand::Rng;
use std::time::Instant;

#[tokio::main]
async fn main() {
    env_logger::init();
    let parent_directory = "/mnt/C/data".to_string();
    let mut db = LsmDatabase::new(parent_directory, None);

    // Time 100,000 writes.
    let start_writes = Instant::now();
    for i in 0..100_000 {
        // Using 8-digit formatting so keys and values have consistent length.
        let key = format!("key-{:08}", i);
        let value = format!("value-{:08}", i);
        db.put(key, value).await.unwrap();
    }
    let duration_writes = start_writes.elapsed();
    println!("Inserted 100,000 entries in {:?}", duration_writes);

    // Time 10,000 random reads.
    let mut rng = rand::thread_rng();
    let mut found_count = 0;
    let mut error_count = 0;
    let start_reads = Instant::now();
    for _ in 0..10_000 {
        let random_index = rng.gen_range(0..75_000);
        let key = format!("key-{:08}", random_index);
        match db.get(key).await{
            Ok(_) => found_count += 1,
            Err(_) => error_count += 1,
        }
    }
    let duration_reads = start_reads.elapsed();
    println!("Performed 10,000 random reads in {:?}", duration_reads);
    println!("Found {} keys, encountered {} errors", found_count, error_count);
}
