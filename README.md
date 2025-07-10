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

## 🗺️ Roadmap

### Runtime Enhancements
- [ ] Incremental compaction support
- [ ] Merge-on-read performance optimization
- [ ] Standalone scheduler component

### Iceberg Features
- [ ] Partition evolution support
- [ ] Schema evolution support

### Cloud Support
- [ ] Azure Blob Storage integration
- [ ] GCP Cloud Storage integration
