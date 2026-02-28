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
 * Shared Test Utilities for ArcadeDB Studio E2E Tests
 *
 * Provides reusable helper classes and functions to reduce code duplication
 * and improve test reliability across the cytoscape 3.33.1 test suite.
 */

import { Page, Locator, expect } from '@playwright/test';
import { TEST_CONFIG, getTestCredentials, getAdaptiveTimeout } from './test-config';

// Type reference for globals
/// <reference path="../types/global.d.ts" />

/**
 * Graph information returned from Cytoscape evaluation
 */
export interface GraphInfo {
  nodeCount: number;
  edgeCount: number;
  zoom: number;
  isInitialized: boolean;
  hasSelectedElements?: boolean;
  selectedCount?: number;
}

/**
 * Context menu information for validation
 */
export interface ContextMenuInfo {
  isVisible: boolean;
  menuItems: string[];
  position: { x: number; y: number };
}

/**
 * Main test helper class for ArcadeDB Studio operations
 */
export class ArcadeStudioTestHelper {
  constructor(public page: Page) {}

  /**
   * Login to ArcadeDB Studio with environment-aware credentials
   * @param database - Database to connect to (defaults to Beer)
   */
  async login(database = 'Beer'): Promise<void> {
    const { username, password } = getTestCredentials();

    await this.page.goto('/');

    // Wait for login page to appear
    await expect(this.page.locator('#loginPage'))
      .toBeVisible({ timeout: TEST_CONFIG.timeouts.login });

    // Fill in login credentials using actual HTML IDs
    await this.page.fill('#inputUserName', username);
    await this.page.fill('#inputUserPassword', password);

    // Click sign in button
    await this.page.click('.login-submit-btn');

    // Wait for login spinner to appear (indicates login started) - with short timeout for fast logins
    await expect(this.page.locator('#loginSpinner')).toBeVisible({ timeout: 2000 }).catch(() => {
      // If spinner doesn't appear, login might be very fast - continue
    });

    // Wait for login to complete - check multiple conditions
    await Promise.all([
      expect(this.page.locator('#loginSpinner')).toBeHidden({ timeout: 30000 }),
      expect(this.page.locator('#studioPanel')).toBeVisible({ timeout: 30000 }),
      expect(this.page.locator('#loginPage')).toBeHidden({ timeout: 30000 })
    ]);

    // Verify username is populated in the query tab
    await expect(this.page.locator('#queryUser')).not.toBeEmpty();

    // Select database using the searchable database selector widget
    const dbSelectContainer = this.page.locator('#queryDbSelectContainer');
    await expect(dbSelectContainer).toBeVisible();

    // Open the dropdown
    await dbSelectContainer.locator('.db-select-toggle').click();
    await expect(dbSelectContainer.locator('.db-select-menu')).toBeVisible();

    // Click the database item
    const dbItem = dbSelectContainer.locator(`.db-select-list li[data-db="${database}"]`);
    const hasDatabase = await dbItem.count() > 0;
    if (hasDatabase) {
      await dbItem.click();
      // Verify database is selected (toggle text shows the db name)
      await expect(dbSelectContainer.locator('.db-name')).toHaveText(database);
    }
  }

  /**
   * Execute a query and wait for results
   * @param query - SQL/Cypher query to execute
   * @param waitForGraph - Whether to wait for graph rendering (default: true)
   */
  async executeQuery(query: string, waitForGraph = true): Promise<void> {
    // Set query via CodeMirror API (fill() doesn't reliably update CodeMirror's internal model)
    await this.page.evaluate((q) => {
      (window as any).editor.setValue(q);
    }, query);

    // Dismiss any existing error toasts that could block the execute button
    await this.page.evaluate(() => {
      const toasts = document.querySelectorAll('#toastContainer .studio-toast');
      toasts.forEach(t => t.remove());
    });

    // Execute query using the data-testid selector
    await this.page.locator('[data-testid="execute-query-button"]').click();

    // Wait for results with network stability
    await expect(this.page.getByText('Returned')).toBeVisible({
      timeout: TEST_CONFIG.timeouts.query
    });
    await this.page.waitForLoadState('networkidle');

    if (waitForGraph) {
      await this.waitForGraphReady();
    }
  }

  /**
   * Wait for Cytoscape graph to be ready and initialized
   */
  async waitForGraphReady(): Promise<void> {
    // Switch to Graph tab first - results default to Table view
    const graphTab = this.page.locator('a[href="#tab-graph"]');
    if (await graphTab.isVisible({ timeout: 2000 }).catch(() => false)) {
      await graphTab.click();
      // Wait for tab panel to become active
      await expect(this.page.locator('#tab-graph')).toBeVisible({ timeout: 3000 });
      await this.page.waitForTimeout(500);
    }

    // Wait for Cytoscape global object to be available with generous timeout
    await this.page.waitForFunction(() => {
      return typeof (globalThis as any).globalCy !== 'undefined' && (globalThis as any).globalCy !== null && (globalThis as any).globalCy.nodes().length > 0;
    }, { timeout: 15000 });

    // Ensure the Cytoscape graph container is visible
    const graphContainer = this.page.locator('#graph');
    await expect(graphContainer).toBeVisible();

    // Additional stability wait for rendering
    await this.page.waitForTimeout(500);
  }

  /**
   * Setup graph with test data - complete login and query execution
   * @param nodeLimit - Number of nodes to load (default: 5)
   * @returns Locator for the graph container
   */
  async setupGraphWithData(nodeLimit = 5): Promise<Locator> {
    await this.login(TEST_CONFIG.database);
    await this.executeQuery(`SELECT FROM Beer LIMIT ${nodeLimit}`);
    await this.waitForGraphReady();
    return this.page.locator('#graph');
  }

  /**
   * Get graph canvas element with verification
   * @returns Locator for the graph container
   */
  async getGraphCanvas(): Promise<Locator> {
    const canvas = this.page.locator('#graph');
    await expect(canvas).toBeVisible();
    return canvas;
  }

  /**
   * Perform a right-click on graph canvas and wait for context menu
   * @param canvas - Graph canvas locator
   * @param position - Optional position for click (defaults to center)
   * @returns Context menu information
   */
  async rightClickOnCanvas(
    canvas: Locator,
    position?: { x: number; y: number }
  ): Promise<ContextMenuInfo> {
    const canvasBounds = await canvas.boundingBox();
    if (!canvasBounds) {
      throw new Error('Canvas bounds not available');
    }

    const clickX = position?.x ?? canvasBounds.x + canvasBounds.width / 2;
    const clickY = position?.y ?? canvasBounds.y + canvasBounds.height / 2;

    // Perform right-click
    await this.page.mouse.click(clickX, clickY, { button: 'right' });

    // Wait for context menu to appear using a more robust selector
    await this.page.waitForFunction(() => {
      return document.querySelector('.fa, [id*="cxt"]') !== null;
    }, { timeout: getAdaptiveTimeout(2000) });

    // Extract context menu information
    return await this.page.evaluate(() => {
      const contextMenu = document.querySelector('.fa, [id*="cxt"]');
      if (!contextMenu) {
        return {
          isVisible: false,
          menuItems: [],
          position: { x: 0, y: 0 }
        };
      }

      const rect = contextMenu.getBoundingClientRect();
      const menuItems = Array.from(contextMenu.querySelectorAll('*'))
        .map((el: Element) => el.textContent?.trim())
        .filter(text => text && text.length > 0);

      return {
        isVisible: true,
        menuItems: menuItems as string[],
        position: { x: rect.x, y: rect.y }
      };
    });
  }

  /**
   * Select multiple nodes using Ctrl+click pattern
   * @param canvas - Graph canvas locator
   * @param nodeCount - Number of nodes to select
   */
  async selectMultipleNodes(canvas: Locator, nodeCount = 2): Promise<void> {
    await this.waitForGraphReady();

    // Get node positions from Cytoscape
    const nodePositions = await this.page.evaluate((count) => {
      const cy = (globalThis as any).globalCy;
      if (!cy || cy.nodes().length === 0) return [];

      return cy.nodes()
        .slice(0, count)
        .map((node: any) => {
          const position = node.renderedPosition();
          return { x: position.x, y: position.y };
        });
    }, nodeCount);

    if (nodePositions.length === 0) {
      throw new Error('No nodes available for selection');
    }

    // Select first node normally
    await canvas.click({
      position: { x: nodePositions[0].x, y: nodePositions[0].y }
    });

    // Select additional nodes with Ctrl key
    for (let i = 1; i < Math.min(nodePositions.length, nodeCount); i++) {
      await canvas.click({
        position: { x: nodePositions[i].x, y: nodePositions[i].y },
        modifiers: ['Control']
      });
      await this.page.waitForTimeout(100); // Brief pause between selections
    }
  }

  /**
   * Perform zoom operations on the graph
   * @param canvas - Graph canvas locator
   * @param zoomSteps - Number of zoom steps (positive for zoom in, negative for zoom out)
   */
  async performZoomOperation(canvas: Locator, zoomSteps = 3): Promise<void> {
    await this.waitForGraphReady();

    const canvasBounds = await canvas.boundingBox();
    if (!canvasBounds) {
      throw new Error('Canvas bounds not available');
    }

    const centerX = canvasBounds.x + canvasBounds.width / 2;
    const centerY = canvasBounds.y + canvasBounds.height / 2;

    for (let i = 0; i < Math.abs(zoomSteps); i++) {
      const wheelDelta = zoomSteps > 0 ? -100 : 100; // Negative for zoom in, positive for zoom out

      await this.page.mouse.move(centerX, centerY);
      await this.page.mouse.wheel(0, wheelDelta);

      // Wait for zoom to stabilize
      await this.page.waitForFunction(() => {
        return typeof (globalThis as any).globalCy !== 'undefined' && (globalThis as any).globalCy.zoom() > 0;
      }, { timeout: 1000 });

      await this.page.waitForTimeout(100); // Brief pause between zoom steps
    }
  }

  /**
   * Wait for specific element with adaptive timeout
   * @param selector - Element selector
   * @param timeout - Custom timeout (optional)
   */
  async waitForElement(selector: string, timeout?: number): Promise<Locator> {
    const element = this.page.locator(selector);
    await expect(element).toBeVisible({
      timeout: timeout || getAdaptiveTimeout(5000)
    });
    return element;
  }

  /**
   * Get current page performance metrics
   */
  async getPerformanceMetrics() {
    return await this.page.evaluate(() => {
      const navigation = performance.getEntriesByType('navigation')[0] as any;
      const memory = (performance as any).memory;

      return {
        loadTime: navigation ? navigation.loadEventEnd - navigation.loadEventStart : 0,
        domContentLoaded: navigation ? navigation.domContentLoadedEventEnd - navigation.domContentLoadedEventStart : 0,
        totalTime: navigation ? navigation.loadEventEnd - navigation.fetchStart : 0,
        memoryUsed: memory ? memory.usedJSHeapSize : undefined,
        memoryTotal: memory ? memory.totalJSHeapSize : undefined
      };
    });
  }
}

/**
 * Assert graph state with comprehensive validation
 * @param page - Playwright page object
 * @param expectedNodes - Expected number of nodes (optional)
 * @param expectedEdges - Expected number of edges (optional)
 * @returns GraphInfo object with current state
 */
export const assertGraphState = async (
  page: Page,
  expectedNodes?: number,
  expectedEdges?: number
): Promise<GraphInfo> => {
  const graphInfo = await page.evaluate(() => {
    const cy = (globalThis as any).globalCy;
    if (!cy) {
      return null;
    }

    const nodes = cy.nodes();
    const edges = cy.edges();
    const selected = cy.elements(':selected');

    return {
      nodeCount: nodes.length,
      edgeCount: edges.length,
      zoom: cy.zoom(),
      isInitialized: true,
      hasSelectedElements: selected.length > 0,
      selectedCount: selected.length
    };
  });

  expect(graphInfo).toBeTruthy();
  expect(graphInfo!.isInitialized).toBe(true);
  expect(graphInfo!.nodeCount).toBeGreaterThan(0);

  if (expectedNodes !== undefined) {
    expect(graphInfo!.nodeCount).toBe(expectedNodes);
  }

  if (expectedEdges !== undefined) {
    expect(graphInfo!.edgeCount).toBe(expectedEdges);
  }

  return graphInfo!;
};

/**
 * Assert context menu visibility and contents
 * @param page - Playwright page object
 * @param expectedItems - Expected menu items (optional)
 */
export const assertContextMenu = async (page: Page, expectedItems?: string[]): Promise<void> => {
  // Check that context menu is visible
  await page.waitForFunction(() => {
    return document.querySelector('.fa, [id*="cxt"]') !== null;
  }, { timeout: getAdaptiveTimeout(2000) });

  const contextMenuElement = page.locator('.fa, [id*="cxt"]');
  await expect(contextMenuElement).toBeVisible();

  if (expectedItems && expectedItems.length > 0) {
    // Verify specific menu items are present
    for (const item of expectedItems) {
      await expect(page.locator(`text=${item}`)).toBeVisible();
    }
  }
};

/**
 * Assert performance metrics meet expected thresholds
 * @param metrics - Performance metrics from getPerformanceMetrics
 * @param thresholds - Performance thresholds to validate against
 */
export const assertPerformanceMetrics = (
  metrics: any,
  thresholds: { loadTime?: number; totalTime?: number; memoryGrowth?: number }
): void => {
  if (thresholds.loadTime !== undefined) {
    expect(metrics.loadTime).toBeLessThan(thresholds.loadTime);
  }

  if (thresholds.totalTime !== undefined) {
    expect(metrics.totalTime).toBeLessThan(thresholds.totalTime);
  }

  if (thresholds.memoryGrowth !== undefined && metrics.memoryUsed) {
    expect(metrics.memoryUsed).toBeLessThan(thresholds.memoryGrowth);
  }
};

/**
 * Create a robust element getter with fallback strategies
 * @param page - Playwright page object
 * @param testId - Primary data-testid to look for
 * @param fallbackSelector - Fallback CSS selector
 * @returns Locator for the element
 */
export const getElementWithFallback = async (
  page: Page,
  testId: string,
  fallbackSelector: string
): Promise<Locator> => {
  const testIdElement = page.getByTestId(testId);

  try {
    await expect(testIdElement).toBeVisible({ timeout: 1000 });
    return testIdElement;
  } catch {
    // Fallback to CSS selector
    const fallbackElement = page.locator(fallbackSelector);
    await expect(fallbackElement).toBeVisible({ timeout: getAdaptiveTimeout(5000) });
    return fallbackElement;
  }
};

/**
 * Utility to check if running in CI environment
 */
export const isCI = (): boolean => {
  return !!process.env.CI;
};

/**
 * Get environment-specific configuration
 */
export const getEnvironmentConfig = () => {
  return {
    isCI: isCI(),
    timeoutMultiplier: isCI() ? 2 : 1,
    testDataSize: isCI() ? 'small' : 'normal',
    concurrency: isCI() ? 2 : 4
  };
};
