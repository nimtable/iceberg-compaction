#!/usr/bin/env bash
#
# Copyright 2025 iceberg-compaction
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Development environment setup script for iceberg-compaction

set -euo pipefail

echo "=== iceberg-compaction Development Setup ==="

# Check Rust installation
if ! command -v rustup &> /dev/null; then
    echo "❌ rustup not found. Please install Rust: https://rustup.rs/"
    exit 1
fi

echo "✅ Rust installed: $(rustc --version)"

# Install required Rust toolchain (rustup reads rust-toolchain file automatically)
echo "📦 Syncing Rust toolchain..."
rustup show active-toolchain || rustup update

# Install cargo tools
echo "📦 Installing cargo-sort..."
cargo install cargo-sort --quiet || echo "  (already installed)"

# Check Docker
if command -v docker &> /dev/null; then
    echo "✅ Docker installed: $(docker --version)"
else
    echo "⚠️  Docker not found. Required for integration tests."
fi

# Check Docker Compose
if command -v docker-compose &> /dev/null || docker compose version &> /dev/null; then
    echo "✅ Docker Compose available"
else
    echo "⚠️  Docker Compose not found. Required for integration tests."
fi

echo ""
echo "=== Setup Complete ==="
echo "Run './scripts/check.sh' to verify your setup."
