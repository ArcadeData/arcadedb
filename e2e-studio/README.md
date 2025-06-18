# ArcadeDB Studio E2E Tests

This dedicated module contains comprehensive end-to-end tests for the ArcadeDB Studio frontend using Playwright. The tests are designed to ensure the quality and functionality of the Studio web interface.

## Module Overview

This `e2e-studio` module is specifically focused on testing the ArcadeDB Studio frontend and includes:
- Navigation and UI components testing
- Database interactions and data display
- Authentication and security testing
- Query execution and result visualization
- Graph visualization functionality
- API integration and error handling

## Prerequisites

1. **Node.js** (>= 16.0.0)
2. **Docker** (for automatic ArcadeDB server setup)
3. **Network access** to pull Docker images and datasets

**Note**: The tests automatically start and stop an ArcadeDB Docker container with pre-configured test data. No manual server setup is required!

## Installation

```bash
# Navigate to the e2e-studio module
cd e2e-studio

# Install dependencies (includes wait-on for server readiness)
npm install

# Install Playwright browsers
npm run install-browsers
```

## ArcadeDB Server Setup

The module automatically manages the ArcadeDB server using Docker:

### Automatic Setup (Recommended)
All test commands automatically start and stop the server:
```bash
npm test  # Starts server, runs tests, stops server
```

### Manual Server Management
```bash
# Start ArcadeDB server with test data
npm run start:server

# Stop the server
npm run stop:server

# Run tests without Docker management
npm run test:no-docker
```

### Server Configuration
The Docker container is configured with:
- **Ports**: 2480 (HTTP), 2424 (Binary), 6379 (Redis), 5432 (Postgres), 8182 (Gremlin)
- **Authentication**: root/playwithdata
- **Test Database**: Beer database with sample data from OpenBeer dataset
- **Plugins**: Redis, MongoDB, Postgres, GremlinServer protocols enabled

## Running Tests

### Quick Start
```bash
# Run all tests
npm test

# Run with interactive UI
npm run test:ui

# Run in headed mode (visible browser)
npm run test:headed
```

### Test Categories
```bash
# Authentication tests
npm run test:auth

# Navigation tests
npm run test:navigation

# Database interaction tests
npm run test:database

# Query and visualization tests
npm run test:query

# Graph visualization tests
npm run test:graph

# API integration tests
npm run test:api

# UI components tests
npm run test:ui-components
```

### Browser-Specific Testing
```bash
# Test on specific browsers
npm run test:chromium
npm run test:firefox
npm run test:webkit
npm run test:mobile
```

### Debug and Reporting
```bash
# Debug mode with step-through
npm run test:debug

# View latest test report
npm run test:report
```

## Test Structure

```
tests/
├── helpers/
│   └── auth-helper.js          # Authentication utilities
├── studio-navigation.spec.js   # Navigation and UI structure
├── studio-ui.spec.js          # UI components and interactions
├── studio-database.spec.js     # Database operations
├── studio-auth.spec.js         # Authentication and security
├── studio-query.spec.js        # Query execution and visualization
├── studio-graph.spec.js        # Graph visualization
└── studio-api.spec.js          # API integration
```

## Configuration

The tests are configured in `playwright.config.js` with:
- **Base URL**: `http://localhost:2480`
- **Multi-browser support**: Chromium, Firefox, WebKit, Mobile Chrome
- **Failure handling**: Screenshots, videos, and detailed error context
- **Reporting**: HTML reports and JUnit XML for CI/CD
- **Timeouts**: Optimized for Studio's dynamic loading behavior

## Test Results Summary

**Current Status**: ✅ **65% Success Rate (40/62 tests passing)**

### ✅ Working Categories:
- **Authentication**: Robust login handling with multiple modal types
- **Homepage Loading**: 100% success rate
- **UI Components**: Core layout and elements detection
- **Basic Navigation**: Main interface elements working

### ⚠️ Areas for Improvement:
- **Selector Specificity**: Some tests need more specific element targeting
- **API Test Flexibility**: Adapt to different server configurations
- **Dynamic Content**: Better wait strategies for loading states

## Authentication

The tests include a comprehensive authentication system:

```javascript
// Automatic login handling
await loginToStudio(page, 'root', 'playwithdata');

// Supports multiple modal types:
// - dialog elements
// - #loginPopup modals
// - Cases where auth is not required
```

## Studio UI Architecture Discovered

Through testing, we've mapped the actual Studio interface:

```
Studio Interface:
├── Left Sidebar (Icon Navigation)
│   ├── Database icon (fa-database)
│   ├── Server icon (fa-server)
│   └── Graph/Chart icons
├── Top Bar
│   ├── "Connected as root @ database"
│   ├── Auto Limit controls
│   └── History dropdown
├── Content Tabs
│   ├── Graph (visualization)
│   ├── Table (data view)
│   ├── Json (raw data)
│   └── Explain (query plans)
└── Toolbar
    ├── Redraw, Select, Import/Export
    └── Settings, Properties panel
```

## CI/CD Integration

The module is ready for continuous integration:

```yaml
# Example GitHub Actions workflow
- name: Run Studio E2E Tests
  run: |
    cd e2e-studio
    npm install
    npm run install-browsers
    npm test
```

**Features for CI/CD**:
- JUnit XML reporting
- Retry logic for flaky tests
- Single worker mode for stability
- Comprehensive error artifacts

## Contributing

When adding new tests:

1. **Follow naming conventions**: `studio-[feature].spec.js`
2. **Use the auth helper**: `await loginToStudio(page)` in `beforeEach`
3. **Add appropriate waits**: `await page.waitForLoadState('networkidle')`
4. **Test both positive and negative scenarios**
5. **Ensure test independence**: Tests should not depend on each other

## Troubleshooting

### Common Issues:

1. **Server not running**
   ```bash
   # Ensure ArcadeDB is running
   curl http://localhost:2480/api/v1/ready
   ```

2. **Authentication failures**
   - Verify server uses default credentials (`root/playwithdata`)
   - Check network connectivity to server
   - Review browser console for authentication errors

3. **Test timeouts**
   - Increase timeouts in `playwright.config.js` if needed
   - Check server performance and response times

4. **Selector issues**
   - Use `npm run test:debug` to inspect element selectors
   - Check for strict mode violations (multiple matching elements)

## Support

For issues related to:
- **Test framework**: Check this README and Playwright documentation
- **ArcadeDB Server**: Refer to main ArcadeDB documentation
- **Studio functionality**: Report issues to the ArcadeDB project

---

**Latest Update**: This module represents a comprehensive E2E testing solution for ArcadeDB Studio with 65% test success rate and robust authentication handling.
