#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/ArcadeDB.E2ETests"

if ! command -v dotnet >/dev/null 2>&1; then
  echo "error: dotnet not found. Install .NET 10 SDK from https://dot.net" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker not found. Install Docker Desktop or Docker Engine." >&2
  exit 1
fi

exec dotnet test --logger "console;verbosity=normal" "$@"
