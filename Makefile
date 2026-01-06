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

.PHONY: build check check-fmt check-clippy check-toml test unit-test integration-test fmt fix-toml setup clean

# Build the project
build:
	cargo build --workspace --all-targets

# Run all checks (format, clippy, toml)
check: check-fmt check-clippy check-toml

# Check code formatting
check-fmt:
	cargo fmt --all -- --check

# Run clippy
check-clippy:
	cargo clippy --workspace --all-targets -- -D warnings

# Install taplo-cli for TOML formatting
install-taplo-cli:
	@which taplo > /dev/null || cargo install taplo-cli

# Check TOML formatting
check-toml: install-taplo-cli
	taplo check

# Fix TOML formatting
fix-toml: install-taplo-cli
	taplo fmt

# Run all tests
test: unit-test integration-test

# Run unit tests only
unit-test:
	cargo test --workspace --lib --exclude iceberg-compaction-integration-tests

# Run integration tests only (requires Docker)
integration-test:
	cargo test -p iceberg-compaction-integration-tests --lib

# Format code
fmt:
	cargo fmt --all

# Run clippy (alias for check-clippy)
clippy: check-clippy

# Setup development environment
setup:
	./scripts/setup.sh

# Clean build artifacts
clean:
	cargo clean
