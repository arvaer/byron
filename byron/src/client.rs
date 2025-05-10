#![allow(non_camel_case_types)]

mod stats;
use byron::byron_client::ByronClient;
use byron::*;
use clap::{Parser, Subcommand};
use stats::WorkloadStats;
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, BufReader};
use tonic::transport::Channel;

pub mod byron {
    tonic::include_proto!("byron");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("byron_descriptor");
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    p { key: i64, value: i64 },
    g { key: i64 },
    r { from: i64, to: i64 },
    d { key: i64 },
    l { file: String },
}

async fn handle_put(
    mut client: ByronClient<Channel>,
    key: i64,
    value: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = tonic::Request::new(PutRequest { key, value });
    let response = client.put(request).await?;
    let _ = response.into_inner();

    Ok(())
}

async fn handle_get(
    mut client: ByronClient<Channel>,
    key: i64,
) -> Result<GetResponse, Box<dyn std::error::Error>> {
    let request = tonic::Request::new(GetRequest { key });

    let response = client.get(request).await?;
    let get_response = response.into_inner();

    Ok(get_response)
}

async fn handle_range(
    mut client: ByronClient<Channel>,
    from: i64,
    to: i64,
) -> Result<RangeResponse, Box<dyn std::error::Error>> {
    let request = tonic::Request::new(RangeRequest {
        start: from,
        end: to,
    });

    let response = client.range(request).await?;
    let range_response = response.into_inner();

    Ok(range_response)
}

async fn handle_delete(
    mut client: ByronClient<Channel>,
    key: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = tonic::Request::new(DeleteRequest { key });

    let response = client.delete(request).await?;
    let _ = response.into_inner();

    Ok(())
}

async fn handle_load(
    mut client: ByronClient<Channel>,
    file_path: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = tokio::fs::File::open(file_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut stats = WorkloadStats::default();

    while let Some(line) = lines.next_line().await? {
        stats.total_lines += 1;

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "p" if parts.len() == 3 => {
                let key = parts[1].parse::<i64>()?;
                let value = parts[2].parse::<i64>()?;
                match handle_put(client.clone(), key, value).await {
                    Ok(_) => stats.put_success += 1,
                    Err(e) => {
                        stats.put_fail += 1;
                        eprintln!("Failed to handle put({}, {}): {}", key, value, e);
                    }
                }
            }
            "g" if parts.len() == 2 => match parts[1].parse::<i64>() {
                Ok(key) => match handle_get(client.clone(), key).await {
                    Ok(target) => {
                        println!("GET {} -> {}", key, target.value);
                        stats.get_success += 1;
                    }
                    Err(e) => {
                        stats.get_fail += 1;
                        eprintln!("Failed to handle get({}): {}", key, e);
                    }
                },
                Err(_) => stats.parse_errors += 1,
            },
            "d" if parts.len() == 2 => match parts[1].parse::<i64>() {
                Ok(key) => match handle_delete(client.clone(), key).await {
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

                match handle_range(client.clone(), from, to).await {
                    Ok(target) => {
                        for value in target.pairs {
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let addr = "http://[::1]:50051";
    let client = ByronClient::connect(addr).await?;

    match cli.command {
        Commands::p { key, value } => handle_put(client, key, value).await?,
        Commands::g { key } => {
            let target = handle_get(client, key).await?;
            println!("{:}", target.value);
        }
        Commands::r { from, to } => {
            let target = handle_range(client, from, to).await?;
            for value in target.pairs {
                println!("{:?} -> {:?}", value.key, value.value);
            }
        }
        Commands::d { key } => handle_delete(client, key).await?,
        Commands::l { file } => handle_load(client, file).await?,
    }

    Ok(())
}
