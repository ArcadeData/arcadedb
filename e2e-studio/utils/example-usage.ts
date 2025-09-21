/**
* Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

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
