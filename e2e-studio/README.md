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

## Running Tests

```bash
# Run all tests
npm test

# Run tests in headed mode (visible browser)
npm run test:headed

# Debug tests
npm run test:debug
```

## Test Files

- `tests/create-database.spec.ts` - Tests database creation functionality

## Configuration

- `playwright.config.ts` - Playwright configuration
- `package.json` - Project dependencies and scripts
