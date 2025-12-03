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
 * Playwright Test Fixtures for ArcadeDB Studio E2E Tests
 *
 * Provides reusable fixtures to eliminate code duplication across test files.
 * These fixtures handle common setup patterns like authentication, graph initialization,
 * and data loading.
 */

import { test as base, Page, Locator } from '@playwright/test';
import { ArcadeStudioTestHelper, TEST_CONFIG } from '../utils';

/**
 * Extended test fixtures with shared setup patterns
 */
type TestFixtures = {
  /** Basic helper instance for manual operations */
  helper: ArcadeStudioTestHelper;

  /** Pre-authenticated helper ready for database operations */
  authenticatedHelper: ArcadeStudioTestHelper;

  /** Fully setup graph with data loaded and ready for interaction */
  graphReady: {
    helper: ArcadeStudioTestHelper;
    canvas: Locator;
  };

  /** Large graph setup for performance testing */
  largeGraphReady: {
    helper: ArcadeStudioTestHelper;
    canvas: Locator;
    nodeCount: number;
  };

  /** Graph setup with export-friendly data */
  exportGraphReady: {
    helper: ArcadeStudioTestHelper;
    canvas: Locator;
  };

  /** Graph setup with styling data for testing visual features */
  styledGraphReady: {
    helper: ArcadeStudioTestHelper;
    canvas: Locator;
  };
};

/**
 * Extended Playwright test with custom fixtures
 */
export const test = base.extend<TestFixtures>({
  /**
   * Basic helper fixture - provides ArcadeStudioTestHelper instance
   */
  helper: async ({ page }, use) => {
    const helper = new ArcadeStudioTestHelper(page);
    await use(helper);
  },

  /**
   * Authenticated helper fixture - login performed, database selected
   */
  authenticatedHelper: async ({ page }, use) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(TEST_CONFIG.database);
    await use(helper);
  },

  /**
   * Graph ready fixture - complete setup with graph data loaded and rendered
   */
  graphReady: async ({ page }, use) => {
    const helper = new ArcadeStudioTestHelper(page);
    const canvas = await helper.setupGraphWithData(5);
    await use({ helper, canvas });
  },

  /**
   * Large graph ready fixture - for performance testing with more nodes
   */
  largeGraphReady: async ({ page }, use) => {
    const helper = new ArcadeStudioTestHelper(page);

    // Login and setup
    await helper.login(TEST_CONFIG.database);

    // Execute query for larger dataset
    await helper.executeQuery('SELECT FROM Beer LIMIT 50');
    await helper.waitForGraphReady();

    // Get canvas and node count
    const canvas = await helper.getGraphCanvas();
    const nodeCount = await page.evaluate(() => {
      return globalThis.globalCy ? globalThis.globalCy.nodes().length : 0;
    });

    await use({ helper, canvas, nodeCount });
  },

  /**
   * Export graph ready fixture - optimized for export testing
   */
  exportGraphReady: async ({ page }, use) => {
    const helper = new ArcadeStudioTestHelper(page);

    // Setup with good dataset for export testing
    await helper.login(TEST_CONFIG.database);
    await helper.executeQuery('SELECT FROM Beer LIMIT 10');
    await helper.waitForGraphReady();

    const canvas = await helper.getGraphCanvas();
    await use({ helper, canvas });
  },

  /**
   * Styled graph ready fixture - for testing visual styling features
   */
  styledGraphReady: async ({ page }, use) => {
    const helper = new ArcadeStudioTestHelper(page);

    // Setup with diverse data for styling tests
    await helper.login(TEST_CONFIG.database);
    await helper.executeQuery('SELECT FROM Beer LIMIT 10');
    await helper.waitForGraphReady();

    const canvas = await helper.getGraphCanvas();
    await use({ helper, canvas });
  }
});

/**
 * Export expect for test assertions
 */
export { expect } from '@playwright/test';
