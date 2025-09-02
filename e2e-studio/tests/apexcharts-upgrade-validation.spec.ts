import { test, expect, Page } from '@playwright/test';
import { ApexChartsTestHelper } from '../utils/apexcharts-test-helper';

/**
 * ApexCharts Upgrade Validation Test Suite
 * 
 * This test suite validates the upgrade from ApexCharts v3.54.1 to v5.3.4
 * for all charts used in the ArcadeDB Studio server monitoring interface.
 * 
 * Charts tested:
 * - serverChartCommands (line chart for request/sec)
 * - serverChartOSCPU (donut chart for CPU usage)
 * - serverChartOSRAM (donut chart for OS RAM)
 * - serverChartOSDisk (donut chart for disk usage)
 * - serverChartServerRAM (donut chart for server RAM)
 * - serverChartCache (donut chart for cache usage)
 */

let helper: ApexChartsTestHelper;

test.beforeEach(async ({ page }) => {
  helper = new ApexChartsTestHelper(page);
  await helper.navigateToServerAndWaitForCharts();
});

test.describe('ApexCharts v5.3.4 Core Functionality', () => {
  
  test('should render all 6 server monitoring charts successfully', async ({ page }) => {
    console.log('Testing basic chart rendering...');
    
    const chartIds = [
      'serverChartCommands',
      'serverChartOSCPU', 
      'serverChartOSRAM',
      'serverChartOSDisk',
      'serverChartServerRAM',
      'serverChartCache'
    ];

    // Verify all charts are rendered
    for (const chartId of chartIds) {
      const chart = page.locator(`#${chartId}`);
      await expect(chart).toBeVisible({ timeout: 10000 });
      
      // Verify ApexCharts structure exists
      const apexChart = chart.locator('.apexcharts-canvas');
      await expect(apexChart).toBeVisible();
      
      console.log(`✓ Chart ${chartId} rendered successfully`);
    }

    // Get comprehensive metrics for all charts
    const allMetrics = await helper.validateAllCharts();
    expect(allMetrics.length).toBe(6);
    
    // Verify each chart has proper structure
    for (const metrics of allMetrics) {
      expect(metrics.isVisible).toBe(true);
      expect(metrics.hasApexchartsCanvas).toBe(true);
      expect(metrics.hasSvgElement).toBe(true);
    }
  });

  test('should handle chart interactions correctly', async ({ page }) => {
    console.log('Testing chart interactions...');
    
    // Test line chart interactions (serverChartCommands)
    const lineChart = page.locator('#serverChartCommands');
    await expect(lineChart).toBeVisible();
    
    const lineResult = await helper.testChartInteraction(lineChart);
    expect(lineResult.canHover).toBe(true);
    
    // Test donut chart interactions (serverChartOSCPU)
    const donutChart = page.locator('#serverChartOSCPU');
    await expect(donutChart).toBeVisible();
    
    const donutResult = await helper.testChartInteraction(donutChart);
    expect(donutResult.canHover).toBe(true);
    
    console.log('✓ Chart interactions working correctly');
  });

  test('should display proper chart structure for line charts', async ({ page }) => {
    console.log('Testing line chart structure...');
    
    const lineChart = page.locator('#serverChartCommands');
    await expect(lineChart).toBeVisible();
    
    const isValid = await helper.verifyLineChart(lineChart);
    expect(isValid).toBe(true);
    
    // Verify specific ApexCharts v5.x elements
    const canvas = lineChart.locator('.apexcharts-canvas');
    await expect(canvas).toBeVisible();
    
    const svg = lineChart.locator('.apexcharts-svg');
    await expect(svg).toBeVisible();
    
    // Check for grid lines and axes (important for v5.x)
    const gridLines = lineChart.locator('.apexcharts-gridlines-horizontal');
    await expect(gridLines).toBeVisible();
    
    console.log('✓ Line chart structure validated');
  });

  test('should display proper chart structure for donut charts', async ({ page }) => {
    console.log('Testing donut chart structure...');
    
    const donutCharts = [
      '#serverChartOSCPU',
      '#serverChartOSRAM', 
      '#serverChartOSDisk',
      '#serverChartServerRAM',
      '#serverChartCache'
    ];

    for (const chartId of donutCharts) {
      const chart = page.locator(chartId);
      await expect(chart).toBeVisible();
      
      const isValid = await helper.verifyDonutChart(chart);
      expect(isValid).toBe(true);
      
      // Verify ApexCharts v5.x specific elements
      const canvas = chart.locator('.apexcharts-canvas');
      await expect(canvas).toBeVisible();
      
      // Check for pie/donut specific elements
      const pieSlices = chart.locator('.apexcharts-pie-slice');
      const sliceCount = await pieSlices.count();
      expect(sliceCount).toBeGreaterThan(0);
      
      console.log(`✓ Donut chart ${chartId} structure validated`);
    }
  });

  test('should handle window resize gracefully', async ({ page }) => {
    console.log('Testing responsive chart behavior...');
    
    // Test with different viewport sizes
    const sizes = [
      { width: 1920, height: 1080 },
      { width: 1366, height: 768 },
      { width: 768, height: 1024 }
    ];

    for (const size of sizes) {
      await page.setViewportSize(size);
      await page.waitForTimeout(1000); // Allow charts to resize
      
      // Verify all charts are still visible and properly sized
      const allMetrics = await helper.validateAllCharts();
      expect(allMetrics.length).toBe(6);
      
      for (const metrics of allMetrics) {
        expect(metrics.isVisible).toBe(true);
        expect(metrics.hasApexchartsCanvas).toBe(true);
      }
      
      console.log(`✓ Charts responsive at ${size.width}x${size.height}`);
    }
  });

  test('should maintain chart performance standards', async ({ page }) => {
    console.log('Testing chart performance...');
    
    const chartIds = [
      'serverChartCommands',
      'serverChartOSCPU',
      'serverChartOSRAM'
    ];

    for (const chartId of chartIds) {
      const chart = page.locator(`#${chartId}`);
      const renderTime = await helper.measureChartRenderTime(chart);
      
      // ApexCharts v5.x should render within 2 seconds
      expect(renderTime).toBeLessThan(2000);
      
      console.log(`✓ Chart ${chartId} rendered in ${renderTime}ms`);
    }
  });

  test('should handle data updates correctly', async ({ page }) => {
    console.log('Testing chart data updates...');
    
    // Wait for initial chart load
    await helper.waitForAllChartsReady();
    
    // Trigger data refresh (simulate server polling)
    await page.evaluate(() => {
      // Simulate the data refresh that happens every few seconds
      if (window.refreshServerStats) {
        window.refreshServerStats();
      }
    });
    
    await page.waitForTimeout(2000); // Allow time for update
    
    // Verify charts are still functional after data update
    const allMetrics = await helper.validateAllCharts();
    expect(allMetrics.length).toBe(6);
    
    for (const metrics of allMetrics) {
      expect(metrics.isVisible).toBe(true);
      expect(metrics.hasApexchartsCanvas).toBe(true);
    }
    
    console.log('✓ Charts handle data updates correctly');
  });

  test('should display proper tooltips on hover', async ({ page }) => {
    console.log('Testing chart tooltips...');
    
    // Test line chart tooltip
    const lineChart = page.locator('#serverChartCommands');
    await lineChart.hover();
    
    // Wait for tooltip to appear
    await page.waitForTimeout(500);
    
    // Check for ApexCharts v5.x tooltip class
    const tooltip = page.locator('.apexcharts-tooltip');
    await expect(tooltip).toBeVisible({ timeout: 5000 });
    
    // Test donut chart tooltip
    const donutChart = page.locator('#serverChartOSCPU');
    await donutChart.hover();
    await page.waitForTimeout(500);
    
    // Tooltip should still be visible or update
    await expect(tooltip).toBeVisible();
    
    console.log('✓ Chart tooltips working correctly');
  });

  test('should maintain accessibility standards', async ({ page }) => {
    console.log('Testing chart accessibility...');
    
    const chartIds = [
      'serverChartCommands',
      'serverChartOSCPU',
      'serverChartOSRAM'
    ];

    for (const chartId of chartIds) {
      const chart = page.locator(`#${chartId}`);
      
      // Check for proper ARIA attributes
      const svg = chart.locator('.apexcharts-svg');
      
      // ApexCharts v5.x should maintain accessibility
      const role = await svg.getAttribute('role');
      expect(role).not.toBeNull();
      
      console.log(`✓ Chart ${chartId} maintains accessibility`);
    }
  });

  test('should handle errors gracefully', async ({ page }) => {
    console.log('Testing error handling...');
    
    // Monitor for JavaScript errors
    const errors: string[] = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });
    
    // Monitor console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });
    
    // Wait for all charts to load
    await helper.waitForAllChartsReady();
    
    // Check that no ApexCharts-related errors occurred
    const apexchartsErrors = errors.filter(error => 
      error.toLowerCase().includes('apexcharts') ||
      error.toLowerCase().includes('chart')
    );
    
    expect(apexchartsErrors.length).toBe(0);
    
    if (apexchartsErrors.length > 0) {
      console.log('ApexCharts errors found:', apexchartsErrors);
    }
    
    console.log('✓ No ApexCharts errors detected');
  });
});