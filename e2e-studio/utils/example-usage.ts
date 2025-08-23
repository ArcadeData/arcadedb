/**
 * Example Usage of Shared Test Utilities
 *
 * This file demonstrates how to use the ArcadeStudioTestHelper
 * and utility functions in test files.
 */

import { test, expect } from '@playwright/test';
import { ArcadeStudioTestHelper, assertGraphState, getElementWithFallback } from './test-utils';
import { TEST_CONFIG, getTestCredentials } from './test-config';

// Example of using the test helper in a test
const exampleTest = async () => {
  test('example usage of test utilities', async ({ page }) => {
    // Initialize the helper
    const helper = new ArcadeStudioTestHelper(page);

    // Use environment-aware login
    await helper.login(TEST_CONFIG.database);

    // Execute a query and wait for graph
    await helper.executeQuery('SELECT FROM Beer LIMIT 10');

    // Assert graph state
    const graphInfo = await assertGraphState(page, 10);
    expect(graphInfo.nodeCount).toBe(10);

    // Get graph canvas for further interactions
    const canvas = await helper.getGraphCanvas();

    // Perform right-click and get context menu info
    const contextMenuInfo = await helper.rightClickOnCanvas(canvas);
    expect(contextMenuInfo.isVisible).toBe(true);

    // Select multiple nodes
    await helper.selectMultipleNodes(canvas, 3);

    // Perform zoom operations
    await helper.performZoomOperation(canvas, 2); // Zoom in 2 steps

    // Get performance metrics
    const metrics = await helper.getPerformanceMetrics();
    expect(metrics.loadTime).toBeLessThan(TEST_CONFIG.performance.renderTime);

    // Use fallback element selector
    const executeButton = await getElementWithFallback(
      page,
      'execute-query-btn',
      'button[title*="execute"], .execute-btn'
    );
    await expect(executeButton).toBeVisible();
  });
};

// Example of using credentials
const exampleCredentials = () => {
  const { username, password } = getTestCredentials();
  console.log(`Using credentials: ${username}/${password.replace(/./g, '*')}`);
};

// Example of environment-aware configuration
const exampleEnvironmentConfig = () => {
  console.log('Test Configuration:');
  console.log(`Database: ${TEST_CONFIG.database}`);
  console.log(`Login Timeout: ${TEST_CONFIG.timeouts.login}ms`);
  console.log(`Query Timeout: ${TEST_CONFIG.timeouts.query}ms`);
  console.log(`Is CI Environment: ${TEST_CONFIG.environment.isCI}`);
  console.log(`Performance Thresholds:`, TEST_CONFIG.performance);
};

export { exampleTest, exampleCredentials, exampleEnvironmentConfig };
