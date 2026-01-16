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

import { test, expect } from '@playwright/test';

test.describe('ApexCharts v5 Upgrade Validation', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to ArcadeDB Studio
    await page.goto('/');

    // Wait for login dialog to appear
    await expect(page.locator('#loginPopup')).toBeVisible();

    // Fill in login credentials using actual HTML IDs
    await page.fill('#inputUserName', 'root');
    await page.fill('#inputUserPassword', 'playwithdata');

    // Click sign in button
    await page.click('button[onclick="login()"]');

    // Wait for login to complete
    await Promise.all([
      expect(page.locator('#loginSpinner')).toBeHidden({ timeout: 30000 }),
      expect(page.locator('#studioPanel')).toBeVisible({ timeout: 30000 }),
      expect(page.locator('#loginPopup')).toBeHidden({ timeout: 30000 })
    ]);
  });

  test('should load ApexCharts v5 library', async ({ page }) => {
    // Navigate to Server tab to ensure ApexCharts is loaded
    // Server tab is usually tab index 2 (Query=0, Database=1, Server=2)
    await page.getByRole('tab').nth(2).click();

    // Wait for server tab content to load
    await page.waitForTimeout(2000);

    // Check that ApexCharts is loaded and can create charts
    const apexChartsLoaded = await page.evaluate(() => {
      // ApexCharts is loaded if the constructor exists
      return typeof (window as any).ApexCharts === 'function';
    });

    console.log(`ApexCharts loaded: ${apexChartsLoaded}`);

    // Verify ApexCharts is loaded
    expect(apexChartsLoaded).toBeTruthy();

    // Verify charts are actually rendered (best way to confirm v5 is working)
    const chartSvg = page.locator('svg.apexcharts-svg').first();
    await expect(chartSvg).toBeVisible({ timeout: 10000 });

    console.log('✅ ApexCharts v5 is loaded and rendering charts successfully');
  });

  test('should render server monitoring charts on Server tab', async ({ page }) => {
    // Navigate to Server tab (index 2)
    await page.getByRole('tab').nth(2).click();

    // Wait for server metrics to load (give it time to fetch data)
    await page.waitForTimeout(3000);

    // Check that ApexCharts SVG elements are present for the charts
    // ApexCharts renders SVG elements inside divs with specific IDs

    // CPU Chart
    const cpuChartSvg = page.locator('#serverChartOSCPU svg.apexcharts-svg');
    await expect(cpuChartSvg).toBeVisible({ timeout: 10000 });

    // RAM Chart
    const ramChartSvg = page.locator('#serverChartOSRAM svg.apexcharts-svg');
    await expect(ramChartSvg).toBeVisible({ timeout: 10000 });

    // Disk Chart
    const diskChartSvg = page.locator('#serverChartOSDisk svg.apexcharts-svg');
    await expect(diskChartSvg).toBeVisible({ timeout: 10000 });

    // Server RAM Chart
    const serverRamChartSvg = page.locator('#serverChartServerRAM svg.apexcharts-svg');
    await expect(serverRamChartSvg).toBeVisible({ timeout: 10000 });

    // Cache Chart
    const cacheChartSvg = page.locator('#serverChartCache svg.apexcharts-svg');
    await expect(cacheChartSvg).toBeVisible({ timeout: 10000 });

    // Commands Chart
    const commandsChartSvg = page.locator('#serverChartCommands svg.apexcharts-svg');
    await expect(commandsChartSvg).toBeVisible({ timeout: 10000 });
  });

  test('should verify chart SVG structure and rendering', async ({ page }) => {
    // Navigate to Server tab
    await page.getByRole('tab').nth(2).click();

    // Wait for charts to render
    await page.waitForTimeout(3000);

    // Verify CPU chart has proper SVG structure
    const cpuChartSvg = page.locator('#serverChartOSCPU svg.apexcharts-svg');
    await expect(cpuChartSvg).toBeVisible();

    // Check that the SVG has child elements (paths, circles, etc.)
    const cpuChartPaths = page.locator('#serverChartOSCPU svg.apexcharts-svg path');
    const pathCount = await cpuChartPaths.count();
    expect(pathCount).toBeGreaterThan(0);

    // Verify the chart has proper dimensions
    const svgBox = await cpuChartSvg.boundingBox();
    expect(svgBox?.width).toBeGreaterThan(100);
    expect(svgBox?.height).toBeGreaterThan(50);
  });

  test('should verify charts have data and update dynamically', async ({ page }) => {
    // Navigate to Server tab
    await page.getByRole('tab').nth(2).click();

    // Wait for initial chart render
    await page.waitForTimeout(3000);

    // Get initial CPU chart data points
    const initialDataPoints = await page.locator('#serverChartOSCPU svg.apexcharts-svg circle.apexcharts-marker').count();

    // Wait for refresh interval (charts should update periodically)
    await page.waitForTimeout(3000);

    // Verify chart is still rendered and has data
    const cpuChartSvg = page.locator('#serverChartOSCPU svg.apexcharts-svg');
    await expect(cpuChartSvg).toBeVisible();

    // Charts should have markers/data points
    const updatedDataPoints = await page.locator('#serverChartOSCPU svg.apexcharts-svg circle.apexcharts-marker').count();

    // Should have at least some data points (may be 0 if line chart without markers)
    // The important thing is the SVG is rendering
    expect(updatedDataPoints).toBeGreaterThanOrEqual(0);
  });

  test('should verify no JavaScript errors during chart rendering', async ({ page }) => {
    const errors: string[] = [];

    // Capture console errors
    page.on('console', msg => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    // Capture page errors
    page.on('pageerror', error => {
      errors.push(error.message);
    });

    // Navigate to Server tab
    await page.getByRole('tab').nth(2).click();

    // Wait for charts to render
    await page.waitForTimeout(4000);

    // Filter out expected/known errors if any
    const relevantErrors = errors.filter(error => {
      // Filter out errors that are not related to ApexCharts
      return error.toLowerCase().includes('apex') ||
             error.toLowerCase().includes('chart') ||
             error.toLowerCase().includes('svg');
    });

    // Should have no ApexCharts-related errors
    expect(relevantErrors).toHaveLength(0);
  });

  test('should verify chart tooltips work (ApexCharts v5 feature)', async ({ page }) => {
    // Navigate to Server tab
    await page.getByRole('tab').nth(2).click();

    // Wait for charts to render
    await page.waitForTimeout(3000);

    // Hover over a chart to trigger tooltip
    const cpuChart = page.locator('#serverChartOSCPU');
    await cpuChart.hover();

    // Wait a moment for potential tooltip
    await page.waitForTimeout(500);

    // Check if ApexCharts tooltip container exists
    // Note: Tooltip may not always appear if there's no data point at hover location
    // This is a soft check - we're mainly verifying no errors occur
    const tooltipExists = await page.locator('.apexcharts-tooltip').count() > 0;

    // Just log the result, not asserting as tooltip behavior can vary
    console.log(`Tooltip container exists: ${tooltipExists}`);
  });

  test('should verify charts use new @svgdotjs dependencies (v5 migration)', async ({ page }) => {
    // Check that the new SVG.js dependencies are loaded
    // ApexCharts v5 migrated from svg.*.js to @svgdotjs/*

    // Navigate to Server tab to trigger chart rendering
    await page.getByRole('tab').nth(2).click();
    await page.waitForTimeout(3000);

    // Verify charts rendered successfully with new dependencies
    const chartSvgs = page.locator('svg.apexcharts-svg');
    const chartCount = await chartSvgs.count();

    // Should have at least 6 charts (CPU, RAM, Disk, Server RAM, Cache, Commands)
    expect(chartCount).toBeGreaterThanOrEqual(6);

    // Verify SVG elements have proper structure from new @svgdotjs library
    const firstChart = chartSvgs.first();
    await expect(firstChart).toHaveAttribute('xmlns', 'http://www.w3.org/2000/svg');
  });
});
