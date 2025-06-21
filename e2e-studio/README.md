# ArcadeDB Studio E2E Tests

This directory contains end-to-end tests for ArcadeDB Studio using Playwright.

## Setup

1. Install dependencies:
   ```bash
   npm install
   ```

2. Install Playwright browsers:
   ```bash
   npx playwright install
   ```

## Prerequisites

- ArcadeDB server running on `http://localhost:2480`
- Server configured with username: `root` and password: `playwithdata`
- Beer database with sample data available

## Running Tests

### Local Development (with Docker)
```bash
# Run tests with automatic server management
npm run test:local

# Run tests in headed mode (visible browser)
npm run test:headed

# Debug tests
npm run test:debug
```

### CI Environment
```bash
# Install browsers for CI
npm run install-browsers

# Run tests (assumes ArcadeDB server is already running)
npm test
```

**Note**: In CI, the tests include a global setup that validates server connectivity before running tests. This helps identify server startup issues early.

## Test Files

- `tests/create-database.spec.ts` - Tests database creation functionality
- `tests/query-beer-database.spec.ts` - Tests Beer database querying and graph visualization

## Configuration

- `playwright.config.ts` - Playwright configuration with CI/local mode support
- `package.json` - Project dependencies and scripts

## CI Integration

The tests are configured to work in GitHub Actions CI:
- Uses JUnit reporter for test result reporting
- Assumes external server management (no webServer configuration)
- Headless mode with failure screenshots and videos
- Generates test artifacts for debugging failures
