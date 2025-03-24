use memtable::MemTableOperations;
use memtable::mem_table_builder::MemTableBuilder;
fn main() {
    let mut memtable = MemTableBuilder::default().max_entries(1000).build();
    memtable.put("Key1".to_string(), "value_1".to_string());

    if let Some(value) = memtable.get("Key1") {
        println!("found: {}", value);
    } else {
        println!("key not found");
    }
    if let Some(value) = memtable.get("key2") {
        println!("found: {}", value);
    } else {
        println!("key not found");
    }
}
