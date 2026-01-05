# Compaction Runtime for Apache Iceberg™

**Disclaimer:** This project is not affiliated with or endorsed by the Apache Software Foundation. “Apache”, “Apache Iceberg”, and related marks are trademarks of the ASF.

`iceberg-compaction` is a high-performance Rust-based engine that compacts Apache Iceberg™ tables efficiently and safely at scale.


[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🌟 Core Highlights

- **Rust-Native Performance**: Low-latency, high-throughput compaction with memory safety guarantees
- **DataFusion Engine**: Leverages Apache DataFusion for query planning and vectorized execution
- **Iceberg Native Support**: Full compliance with Iceberg table formats via iceberg-rs
- **Multi-Cloud Ready**: Currently supports AWS S3, with plans for Azure Blob Storage and GCP Cloud Storage

## 🛠️ Basic Functionality

- **Full Compaction**: Merges all data files in an Iceberg table and removes old files
- **Deletion Support**:
  - Positional deletions (POS_DELETE)
  - Equality deletions (EQ_DELETE)

## 📝 Examples

### REST Catalog Example

We provide a complete working example using a REST catalog. This example demonstrates how to use iceberg-compaction for Iceberg table compaction with a REST catalog backend:

```bash
# Navigate to the example directory
cd examples/rest-catalog

# Run the example
cargo run
```

The example includes:
- Setting up a REST Iceberg catalog with S3 storage
- Configuring authentication and connection settings
- Performing table compaction using iceberg-compaction

For more details, see the [rest-catalog example](./examples/rest-catalog/).

### Benchmark Tool

See the [bench binary](./integration-tests/src/bin/README.md) for benchmarking compaction performance with mock data.

## Development Roadmap

### Performance 🚀

- [ ] Partial compaction: Support incremental compaction strategies
- [ ] Compaction Policy: Multiple built-in policies (size-based, time-based, cost-optimized)
- [ ] Built-in cache: Metadata and query result caching for improved performance

### Stability 🔒

- [ ] Spill to disk: Handle large datasets that exceed memory limits
- [ ] Network rebuild: Robust handling of network failures and retries
- [ ] Task breakpoint resume: Resume operations from failure points
- [ ] E2E test framework: Comprehensive testing infrastructure

### Observability 📊

- [ ] Job progress display: Progress tracking
- [ ] Comprehensive compaction metrics: Detailed performance and operation metrics

### Customizability 🎛️

- [ ] Tune parquet: Configurable Parquet writer parameters
- [ ] Fine-grained configurable compaction parameters: Extensive customization options

### Functionality ⚙️

#### iceberg-rust

- [ ] Expire snapshot
- [ ] Rewrite manifest

#### iceberg-compaction

- [ ] Binpack/Sort/ZOrder Compaction
- [ ] Clustering / Order by: Support for data reorganization and sorting
- [ ] File clean: Delete orphan files
