use lsm::lsm_database::LsmDatabase;
use lsm::lsm_operators::LsmSearchOperators;
use rand::Rng;

fn main() {
    let parent_directory = "./data".to_string();
    let mut db = LsmDatabase::new(parent_directory);

    for i in 0..5000 {
        let key = format!("key-{:05}", i);
        let value = format!("value-{:05}", i);
        db.put(key, value);
    }

    println!("Inserted 5000 entries into the LSM database.");

    let mut rng = rand::rng();
    for _ in 0..5 {
        let random_index = rng.random_range(0..5000);
        let key = format!("key-{:05}", random_index);
        match db.get(key.clone()) {
            Ok(kv) => println!("Found {}: {:?}", key, kv),
            Err(e) => println!("Error retrieving {}: {:?}", key, e),
        }
    }
}
