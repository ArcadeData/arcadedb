# ArcadeDB Modular Distribution Builder

Build custom ArcadeDB distributions with only the modules you need.

## Overview

The modular distribution builder (`arcadedb-builder.sh`) creates custom ArcadeDB packages by:
1. Downloading a base distribution (engine, server, network + dependencies)
2. Adding user-selected optional modules from Maven Central
3. Generating zip, tar.gz, and optionally Docker images

## Prerequisites

- `curl` or `wget` - for downloading files
- `tar` - for extracting and creating archives
- `unzip` and `zip` - for creating zip archives
- `sha256sum` or `shasum` - for checksum verification
- `docker` (optional) - for Docker image generation

## Quick Start

### Interactive Mode

```bash
./arcadedb-builder.sh
```

The script will prompt for:
- ArcadeDB version
- Optional modules to include

### CLI Mode

```bash
./arcadedb-builder.sh --version=26.1.0 --modules=gremlin,postgresw,studio
```

## Available Modules

**Core (always included):**
- `engine` - Database engine
- `server` - HTTP/REST API, clustering
- `network` - Network communication

**Optional:**
- `console` - Interactive database console
- `gremlin` - Apache Tinkerpop Gremlin support
- `studio` - Web-based administration interface
- `redisw` - Redis wire protocol compatibility
- `mongodbw` - MongoDB wire protocol compatibility
- `postgresw` - PostgreSQL wire protocol compatibility
- `grpcw` - gRPC wire protocol support
- `graphql` - GraphQL API support
- `metrics` - Prometheus metrics integration

## Usage Examples

### Minimal Build (PostgreSQL only)

```bash
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw
```

### Development Build

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=console,gremlin,studio \
  --output-name=arcadedb-dev
```

### Production Build (no Studio)

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=postgresw,metrics \
  --output-name=arcadedb-prod
```

### CI/CD Build

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=gremlin,studio \
  --quiet \
  --skip-docker \
  --output-dir=/tmp/builds
```

### Dockerfile Only (no build)

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=gremlin,studio \
  --dockerfile-only
```

## Command-Line Options

### Required (non-interactive mode)

- `--version=X.Y.Z` - ArcadeDB version to build

### Optional

- `--modules=mod1,mod2,...` - Comma-separated list of optional modules
- `--output-name=NAME` - Custom output name (default: arcadedb-{version}-custom-{timestamp})
- `--output-dir=PATH` - Output directory (default: current directory)
- `--docker-tag=TAG` - Docker image tag (default: arcadedb-custom:{version})
- `--skip-docker` - Skip Docker image generation
- `--dockerfile-only` - Generate Dockerfile without building image
- `--keep-temp` - Don't delete temporary working directory
- `--dry-run` - Show what would be downloaded without doing it
- `-v, --verbose` - Verbose output
- `-q, --quiet` - Quiet mode (errors only)
- `-h, --help` - Show help message

## Output Files

The builder creates:
- `{output-name}.zip` - Zip archive
- `{output-name}.tar.gz` - Compressed tarball
- Docker image with tag `{docker-tag}` (if not skipped)

## Directory Structure

```
arcadedb-{version}-custom-{timestamp}/
├── bin/              # Server and console scripts
├── config/           # Configuration files
├── lib/              # JARs (core + selected modules)
├── databases/        # Database storage (empty)
├── backups/          # Backup storage (empty)
├── log/              # Log files (empty)
├── replication/      # Replication data (empty)
├── README.md         # ArcadeDB README
└── LICENSE           # Apache 2.0 License
```

## How It Works

1. **Download Base**: Fetches base distribution from GitHub releases
2. **Verify Checksums**: Validates SHA-256 checksum for base
3. **Add Modules**: Downloads selected modules from Maven Central
4. **Verify Modules**: Validates SHA-1 checksums for each module
5. **Create Archives**: Generates zip and tar.gz files
6. **Build Docker**: Optionally creates Docker image

## Troubleshooting

### Error: Base distribution not found

The base distribution for the specified version doesn't exist on GitHub releases. Check that:
- Version number is correct
- Version has been released
- Base distribution was included in the release

### Error: Module not found on Maven Central

The specified module doesn't exist for that version. This can happen with:
- Older versions before a module was introduced
- Typos in module names
- Unreleased or snapshot versions

### Error: Docker daemon not running

Docker is installed but not running. Start Docker Desktop or the Docker daemon.

### Error: Checksum verification failed

Downloaded file is corrupted or doesn't match expected checksum. Try:
- Running the script again (download may have been interrupted)
- Checking network connection
- Verifying the version exists

## Contributing

Report issues or suggest improvements at:
https://github.com/arcadedata/arcadedb/issues

## License

Copyright © 2021-present Arcade Data Ltd

Licensed under the Apache License, Version 2.0
