/*
 * Copyright 2025 IC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::Arc;

use ic_codegen::compactor::compactor_service_server::CompactorService;
use ic_codegen::compactor::{EchoRequest, EchoResponse, RewriteFilesRequest, RewriteFilesResponse};
use ic_core::executor::{CompactionResult, DataFusionExecutor};
use ic_core::{CompactionConfig, CompactionExecutor};

use crate::util::{
    build_file_io_from_pb, build_file_scan_tasks_schema_from_pb, build_partition_spec_from_pb,
    data_file_into_pb,
};

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
            serde_json::to_value(request.rewrite_file_config).map_err(|e| {
                tonic::Status::internal(format!(
                    "Failed to convert rewrite_file_config to JSON value: {}",
                    e
                ))
            })?,
        )
        .map_err(|e| tonic::Status::internal(format!("Failed to build file io: {}", e)))?;
        let partition_spec = build_partition_spec_from_pb(
            request.partition_spec.ok_or_else(|| {
                tonic::Status::internal("cannot find partition_spec in request".to_owned())
            })?,
            schema.clone(),
        )
        .map_err(|e| tonic::Status::internal(format!("Failed to build partition spec: {}", e)))?;
        let CompactionResult { data_files, stat } = DataFusionExecutor::default()
            .rewrite_files(
                file_io,
                schema,
                all_file_scan_tasks,
                Arc::new(config),
                request.dir_path,
                Arc::new(partition_spec),
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("Failed to compact files: {}", e)))?;

        let data_files = data_files.into_iter().map(data_file_into_pb).collect();
        Ok(tonic::Response::new(RewriteFilesResponse {
            data_files,
            stat: Some(stat),
        }))
    }

    async fn echo(
        &self,
        request: tonic::Request<EchoRequest>,
    ) -> std::result::Result<tonic::Response<EchoResponse>, tonic::Status> {
        tracing::info!("Echo request: {:?}", request);
        Ok(tonic::Response::new(EchoResponse {
            message: format!("Echo: {}", request.into_inner().message),
        }))
    }
}
