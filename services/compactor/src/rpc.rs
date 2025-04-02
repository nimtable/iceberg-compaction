use std::sync::Arc;

use ic_core::executor::DataFusionExecutor;
use ic_core::{CompactionConfig, CompactionExecutor};
use ic_prost::compactor::compactor_service_server::CompactorService;
use ic_prost::compactor::{RewriteFilesRequest, RewriteFilesResponse};

use crate::util::{build_file_io_from_pb, build_file_scan_tasks_schema_from_pb, data_file_into_pb};

#[derive(Default)]
pub struct CompactorServiceImpl;

#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    async fn rewrite_files(
        &self,
        request: tonic::Request<RewriteFilesRequest>,
    ) -> std::result::Result<tonic::Response<RewriteFilesResponse>, tonic::Status> {
        let request = request.into_inner();
        let (all_file_scan_tasks, schema) = build_file_scan_tasks_schema_from_pb(
            request.file_scan_task_descriptor,
            request
                .schema
                .ok_or_else(|| tonic::Status::invalid_argument("schema is required"))?,
        )
        .map_err(|e| {
            tonic::Status::internal(format!("Failed to build file scan tasks schema: {}", e))
        })?;
        let file_io = build_file_io_from_pb(
            request
                .file_io_builder
                .ok_or_else(|| tonic::Status::invalid_argument("file_io is required"))?,
        )
        .map_err(|e| tonic::Status::internal(format!("Failed to build file io: {}", e)))?;
        let config = serde_json::from_value::<CompactionConfig>(
            serde_json::to_value(request.rewrite_file_config).unwrap(),
        )
        .map_err(|e| tonic::Status::internal(format!("Failed to build file io: {}", e)))?;
        let data_files = DataFusionExecutor::rewrite_files(
            file_io,
            schema,
            all_file_scan_tasks,
            Arc::new(config),
            request.dir_path,
        )
        .await
        .map_err(|e| tonic::Status::internal(format!("Failed to compact files: {}", e)))?;

        let data_files = data_files.into_iter().map(data_file_into_pb).collect();
        Ok(tonic::Response::new(RewriteFilesResponse { data_files }))
    }
}
