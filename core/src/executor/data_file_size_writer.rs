/*
 * Copyright 2025 BergLoom
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

use datafusion::arrow::array::RecordBatch;
use iceberg::Result;
use iceberg::{
    spec::DataFile,
    writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder},
};

#[derive(Clone)]
pub struct DataFileSizeWriter<B, D> {
    inner_writer_builder: B,
    inner_writer: D,
    max_file_size: usize,
    data_files: Vec<DataFile>,
}

#[async_trait::async_trait]
impl<B, D> IcebergWriter for DataFileSizeWriter<B, D>
where
    B: IcebergWriterBuilder<R = D>,
    D: IcebergWriter + CurrentFileStatus,
{
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let current_written_size = self.inner_writer.current_written_size();
        if current_written_size + input.get_array_memory_size() > self.max_file_size
            && current_written_size > 0
        {
            let data_files = self.inner_writer.close().await?;
            self.data_files.extend(data_files);
            self.inner_writer = self.inner_writer_builder.clone().build().await?;
        }
        self.inner_writer.write(input).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let mut data_files = std::mem::take(&mut self.data_files);
        data_files.extend(self.inner_writer.close().await?);
        Ok(data_files)
    }
}

#[derive(Clone)]
pub struct DataFileSizeWriterBuilder<B> {
    inner_builder: B,
    max_file_size: usize,
}

impl<B> DataFileSizeWriterBuilder<B> {
    pub fn new(inner_builder: B, max_file_size: usize) -> Self {
        Self {
            inner_builder,
            max_file_size,
        }
    }
}

#[async_trait::async_trait]
impl<B> IcebergWriterBuilder for DataFileSizeWriterBuilder<B>
where
    B: IcebergWriterBuilder,
    B::R: IcebergWriter + CurrentFileStatus,
{
    type R = DataFileSizeWriter<B, B::R>;

    async fn build(self) -> Result<Self::R> {
        Ok(DataFileSizeWriter {
            inner_writer_builder: self.inner_builder.clone(),
            inner_writer: self.inner_builder.build().await?,
            max_file_size: self.max_file_size,
            data_files: Vec::new(),
        })
    }
}
