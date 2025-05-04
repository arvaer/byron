use std::sync::Arc;
use tokio::sync::RwLock;

use lsm::lsm_database::LsmDatabase;
use tonic::{transport::Server, Request, Response, Status};

use byron::byron_server::{Byron, ByronServer};
use byron::*;

pub mod byron {
    tonic::include_proto!("byron");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("byron_descriptor");
}

#[derive(Debug, Default)]
pub struct ByronServerContext {
    pub database: Arc<RwLock<LsmDatabase>>,
}

#[tonic::async_trait]
impl Byron for ByronServerContext {
    #[tracing::instrument]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        tracing::debug!("Received get request: {:?}", request);
        let input = request.get_ref();
        let key = input.key.to_string();

        let db = self.database.read().await;
        let kv = db
            .get(key)
            .map_err(|e| Status::internal(format!("Database error: {:?}", e)))?;

        let value: i32 = kv
            .value
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Value parsing error: {:?}", e)))?;

        let response = GetResponse {
            key: input.key,
            value,
        };
        tracing::info!("Returning get response: {:?}", response);
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        tracing::debug!("Received put request: {:?}", request);
        let input = request.get_ref();
        let key = input.key.to_string();
        let value = input.value.to_string();
        let mut db = self.database.write().await;
        db.put(key, value);

        let response = PutResponse {};
        tracing::info!("Processed put request successfully.");
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        tracing::debug!("Received range request: {:?}", request);
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr = "[::1]:50051".parse()?;
    let byron = ByronServerContext::default();
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(byron::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    tracing::info!("Starting Byron gRPC server on socket address: {}", addr);
    Server::builder()
        .add_service(ByronServer::new(byron))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
