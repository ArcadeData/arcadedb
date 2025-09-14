#!/bin/bash
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
#


# ArcadeDB Studio Security Audit Script
# This script performs comprehensive security checks on the frontend dependencies

set -e

echo "🔍 ArcadeDB Studio Security Audit"
echo "================================"

# Change to studio directory
cd "$(dirname "$0")/.."

# Check if npm is available
if ! command -v npm &> /dev/null; then
    echo "❌ npm is not installed or not in PATH"
    exit 1
fi

# Check if package.json exists
if [ ! -f "package.json" ]; then
    echo "❌ package.json not found"
    exit 1
fi

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

echo ""
echo "🔐 Running security audit..."
echo "============================"

# Run npm audit with moderate level
echo "Checking for vulnerabilities (moderate and above):"
npm audit --audit-level=moderate

echo ""
echo "📊 Dependency Status"
echo "==================="

# Check for outdated packages
echo "Checking for outdated packages:"
npm outdated || true

echo ""
echo "🏷️  License Check"
echo "================"

# Check licenses (requires license-checker package)
if npm list license-checker &> /dev/null; then
    npx license-checker --summary
else
    echo "ℹ️  Install license-checker for license analysis: npm install -g license-checker"
fi

echo ""
echo "📈 Bundle Analysis"
echo "=================="

# Check bundle size (basic analysis)
if [ -d "src/main/resources/static/dist" ]; then
    echo "Built bundle sizes:"
    du -sh src/main/resources/static/dist/* 2>/dev/null || echo "No built bundles found"
else
    echo "No built bundles found (run 'npm run build' first)"
fi

echo ""
echo "🎯 Recommendations"
echo "=================="

# Check for critical vulnerabilities
CRITICAL_VULNS=$(npm audit --json 2>/dev/null | jq -r '.metadata.vulnerabilities.critical // 0' 2>/dev/null || echo "0")
HIGH_VULNS=$(npm audit --json 2>/dev/null | jq -r '.metadata.vulnerabilities.high // 0' 2>/dev/null || echo "0")

if [ "$CRITICAL_VULNS" -gt 0 ] || [ "$HIGH_VULNS" -gt 0 ]; then
    echo "⚠️  URGENT: Critical or high severity vulnerabilities found!"
    echo "   Run 'npm audit fix' to attempt automatic fixes"
    echo "   Review 'npm audit' output for manual intervention"
    exit 1
else
    echo "✅ No critical or high severity vulnerabilities found"
fi

# Check for very outdated packages (rough heuristic)
OUTDATED_COUNT=$(npm outdated --json 2>/dev/null | jq '. | length' 2>/dev/null || echo "0")
if [ "$OUTDATED_COUNT" -gt 10 ]; then
    echo "📅 Many packages are outdated ($OUTDATED_COUNT). Consider updating."
else
    echo "✅ Dependencies are reasonably up to date"
fi

echo ""
echo "🔧 Available Actions"
echo "==================="
echo "npm audit fix          - Fix vulnerabilities automatically"
echo "npm update             - Update to latest compatible versions"
echo "npm audit fix --force  - Force fix (may cause breaking changes)"
echo "npm outdated           - Show detailed outdated package information"

echo ""
echo "✅ Security audit completed"
