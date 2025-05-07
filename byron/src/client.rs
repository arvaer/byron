use byron::byron_client::ByronClient;
use byron::*;
use clap::{Parser, Subcommand};
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
    p { key: i32, value: i32 },
    g { key: i32 },
    r { from: i32, to: i32 },
    d { key: i32 },
}

async fn handle_put(
    mut client: ByronClient<Channel>,
    key: i32,
    value: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = tonic::Request::new(PutRequest { key, value });
    let response = client.put(request).await?;
    let _ = response.into_inner();

    Ok(())
}

async fn handle_get(
    mut client: ByronClient<Channel>,
    key: i32,
) -> Result<GetResponse, Box<dyn std::error::Error>> {
    let request = tonic::Request::new(GetRequest { key });

    let response = client.get(request).await?;
    let get_response = response.into_inner();

    Ok(get_response)
}

async fn handle_range(
    mut client: ByronClient<Channel>,
    from: i32,
    to: i32,
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
    key: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = tonic::Request::new(DeleteRequest { key });

    let response = client.delete(request).await?;
    let _ = response.into_inner();

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
        },
        Commands::d { key } => handle_delete(client, key).await?,
    }

    Ok(())
}
