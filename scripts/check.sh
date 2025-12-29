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

# Pre-commit check script for iceberg-compaction
# Run this before submitting a PR to catch common issues

set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== iceberg-compaction Pre-commit Checks ==="
echo ""

# Track failures
FAILED=0

# 1. Format check
echo "🔍 Checking code formatting..."
if cargo fmt --all -- --check; then
    echo "✅ Format check passed"
else
    echo "❌ Format check failed. Run 'cargo fmt --all' to fix."
    FAILED=1
fi
echo ""

# 2. TOML sort check
echo "🔍 Checking Cargo.toml sorting..."
if cargo sort --check --workspace 2>/dev/null; then
    echo "✅ TOML sort check passed"
else
    echo "❌ TOML sort check failed. Run 'cargo sort --workspace' to fix."
    FAILED=1
fi
echo ""

# 3. Clippy
echo "🔍 Running clippy..."
if cargo clippy --workspace --all-targets -- -D warnings; then
    echo "✅ Clippy check passed"
else
    echo "❌ Clippy check failed. Fix the warnings above."
    FAILED=1
fi
echo ""

# 4. Unit tests (excluding integration tests)
echo "🔍 Running unit tests..."
if cargo test --workspace --lib --exclude iceberg-compaction-integration-tests; then
    echo "✅ Unit tests passed"
else
    echo "❌ Unit tests failed."
    FAILED=1
fi
echo ""

# 5. Integration tests (requires Docker)
echo "🔍 Running integration tests..."
if cargo test -p iceberg-compaction-integration-tests --lib; then
    echo "✅ Integration tests passed"
else
    echo "❌ Integration tests failed."
    FAILED=1
fi
echo ""

# Summary
echo "=== Check Summary ==="
if [ $FAILED -eq 0 ]; then
    echo "✅ All checks passed! Ready to submit PR."
    exit 0
else
    echo "❌ Some checks failed. Please fix the issues above."
    exit 1
fi
