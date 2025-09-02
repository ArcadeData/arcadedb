import { Page, Locator, expect } from '@playwright/test';

/**
 * Specialized helper class for testing ApexCharts functionality
 * in the ArcadeDB Studio server monitoring interface.
 * 
 * Provides utilities for testing chart rendering, interactions,
 * performance, and compatibility during the v3.54.1 -> v5.3.4 upgrade.
 */

export interface ChartMetrics {
  chartId: string;
  isVisible: boolean;
  hasApexchartsCanvas: boolean;
  hasSvgElement: boolean;
  width: number;
  height: number;
  renderTime?: number;
  elementCount?: number;
}

export interface ChartInteractionResult {
  canHover: boolean;
  hasTooltip: boolean;
  canClick: boolean;
  tooltipContent?: string;
}

export class ApexChartsTestHelper {
  private page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  /**
   * Navigate to the server monitoring page and wait for all charts to load
   */
  async navigateToServerAndWaitForCharts(): Promise<void> {
    console.log('Navigating to ArcadeDB Studio server monitoring...');
    
    // Navigate to the Studio using dynamic baseURL
    await this.page.goto('/');
    
    // Wait for login dialog to appear
    await expect(this.page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();

    // Fill in login credentials
    await this.page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await this.page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');

    // Click sign in button
    await this.page.getByRole('button', { name: 'Sign in' }).click();

    // Wait for login dialog to disappear
    await expect(this.page.getByRole('dialog', { name: 'Login to the server' })).toBeHidden({ timeout: 10000 });
    
    // Wait for the main interface to load
    await expect(this.page.getByText('Connected as').first()).toBeVisible({ timeout: 10000 });
    
    // Navigate to Server monitoring tab using the specific selector
    await this.page.click('#tab-server-sel');
    await this.page.waitForTimeout(3000);
    
    // Wait for all server monitoring charts to be present
    await this.waitForAllChartsReady();
    
    console.log('✓ Successfully navigated to server monitoring with charts loaded');
  }

  /**
   * Wait for all 6 server monitoring charts to be ready
   */
  async waitForAllChartsReady(timeoutMs: number = 15000): Promise<void> {
    console.log('Waiting for all ApexCharts to be ready...');
    
    const chartIds = [
      'serverChartCommands',
      'serverChartOSCPU',
      'serverChartOSRAM',
      'serverChartOSDisk',
      'serverChartServerRAM',
      'serverChartCache'
    ];

    // Wait for each chart container to be visible
    for (const chartId of chartIds) {
      await this.page.waitForSelector(`#${chartId}`, { timeout: timeoutMs });
    }

    // Wait for ApexCharts canvas elements to be present
    for (const chartId of chartIds) {
      await this.page.waitForSelector(`#${chartId} .apexcharts-canvas`, { 
        timeout: timeoutMs,
        state: 'attached'
      });
    }

    // Additional wait for chart rendering to complete
    await this.page.waitForTimeout(2000);
    
    console.log('✓ All 6 ApexCharts are ready');
  }

  /**
   * Get comprehensive metrics for a specific chart
   */
  async getChartMetrics(chartLocator: Locator): Promise<ChartMetrics> {
    const chartId = await chartLocator.getAttribute('id') || 'unknown';
    
    const metrics: ChartMetrics = {
      chartId,
      isVisible: await chartLocator.isVisible(),
      hasApexchartsCanvas: false,
      hasSvgElement: false,
      width: 0,
      height: 0
    };

    if (metrics.isVisible) {
      // Check for ApexCharts canvas
      const canvas = chartLocator.locator('.apexcharts-canvas');
      metrics.hasApexchartsCanvas = await canvas.count() > 0;

      // Check for SVG element
      const svg = chartLocator.locator('.apexcharts-svg');
      metrics.hasSvgElement = await svg.count() > 0;

      // Get dimensions
      const boundingBox = await chartLocator.boundingBox();
      if (boundingBox) {
        metrics.width = boundingBox.width;
        metrics.height = boundingBox.height;
      }

      // Count chart elements (for complexity assessment)
      const elements = chartLocator.locator('.apexcharts-canvas *');
      metrics.elementCount = await elements.count();
    }

    return metrics;
  }

  /**
   * Test chart interaction capabilities
   */
  async testChartInteraction(chartLocator: Locator): Promise<ChartInteractionResult> {
    const result: ChartInteractionResult = {
      canHover: false,
      hasTooltip: false,
      canClick: false
    };

    try {
      // Test hover functionality
      await chartLocator.hover();
      await this.page.waitForTimeout(500);
      result.canHover = true;

      // Check for tooltip appearance
      const tooltip = this.page.locator('.apexcharts-tooltip');
      result.hasTooltip = await tooltip.isVisible();

      if (result.hasTooltip) {
        result.tooltipContent = await tooltip.textContent();
      }

      // Test click functionality
      await chartLocator.click();
      result.canClick = true;

    } catch (error) {
      console.log(`Chart interaction test failed: ${error}`);
    }

    return result;
  }

  /**
   * Verify chart structure for line charts
   */
  async verifyLineChart(chartLocator: Locator): Promise<boolean> {
    try {
      // Check for essential line chart elements
      const essentialSelectors = [
        '.apexcharts-canvas',
        '.apexcharts-svg',
        '.apexcharts-inner',
        '.apexcharts-line-series'
      ];

      for (const selector of essentialSelectors) {
        const element = chartLocator.locator(selector);
        const exists = await element.count() > 0;
        if (!exists) {
          console.log(`Missing essential line chart element: ${selector}`);
          return false;
        }
      }

      return true;
    } catch (error) {
      console.log(`Line chart verification failed: ${error}`);
      return false;
    }
  }

  /**
   * Verify chart structure for donut charts
   */
  async verifyDonutChart(chartLocator: Locator): Promise<boolean> {
    try {
      // Check for essential donut chart elements
      const essentialSelectors = [
        '.apexcharts-canvas',
        '.apexcharts-svg',
        '.apexcharts-inner'
      ];

      for (const selector of essentialSelectors) {
        const element = chartLocator.locator(selector);
        const exists = await element.count() > 0;
        if (!exists) {
          console.log(`Missing essential donut chart element: ${selector}`);
          return false;
        }
      }

      // Check for pie slices (specific to donut/pie charts)
      const pieSlices = chartLocator.locator('.apexcharts-pie-slice');
      const sliceCount = await pieSlices.count();
      
      if (sliceCount === 0) {
        console.log('No pie slices found in donut chart');
        return false;
      }

      return true;
    } catch (error) {
      console.log(`Donut chart verification failed: ${error}`);
      return false;
    }
  }

  /**
   * Test chart resize behavior
   */
  async testChartResize(chartLocator: Locator): Promise<boolean> {
    try {
      // Get initial dimensions
      const initialMetrics = await this.getChartMetrics(chartLocator);

      // Change viewport size
      await this.page.setViewportSize({ width: 800, height: 600 });
      await this.page.waitForTimeout(1000);

      // Get new dimensions
      const resizedMetrics = await this.getChartMetrics(chartLocator);

      // Verify chart is still visible and dimensions changed appropriately
      const resizedCorrectly = resizedMetrics.isVisible && 
                              resizedMetrics.hasApexchartsCanvas &&
                              (resizedMetrics.width !== initialMetrics.width ||
                               resizedMetrics.height !== initialMetrics.height);

      // Restore original viewport
      await this.page.setViewportSize({ width: 1366, height: 768 });
      await this.page.waitForTimeout(1000);

      return resizedCorrectly;
    } catch (error) {
      console.log(`Chart resize test failed: ${error}`);
      return false;
    }
  }

  /**
   * Measure chart render time
   */
  async measureChartRenderTime(chartLocator: Locator): Promise<number> {
    const startTime = Date.now();
    
    try {
      // Wait for chart to be visible
      await expect(chartLocator).toBeVisible({ timeout: 10000 });
      
      // Wait for ApexCharts canvas to be present
      await expect(chartLocator.locator('.apexcharts-canvas')).toBeVisible({ timeout: 10000 });
      
      // Additional wait for rendering completion
      await this.page.waitForTimeout(500);
      
    } catch (error) {
      console.log(`Chart render measurement failed: ${error}`);
      return -1;
    }

    return Date.now() - startTime;
  }

  /**
   * Assert chart performance meets thresholds
   */
  async assertChartPerformance(chartLocator: Locator, maxRenderTimeMs: number = 2000): Promise<void> {
    const renderTime = await this.measureChartRenderTime(chartLocator);
    
    if (renderTime === -1) {
      throw new Error('Chart failed to render');
    }
    
    if (renderTime > maxRenderTimeMs) {
      throw new Error(`Chart render time ${renderTime}ms exceeds maximum ${maxRenderTimeMs}ms`);
    }

    console.log(`✓ Chart rendered in ${renderTime}ms (within ${maxRenderTimeMs}ms threshold)`);
  }

  /**
   * Validate all server monitoring charts
   */
  async validateAllCharts(): Promise<ChartMetrics[]> {
    const chartIds = [
      'serverChartCommands',
      'serverChartOSCPU',
      'serverChartOSRAM',
      'serverChartOSDisk',
      'serverChartServerRAM',
      'serverChartCache'
    ];

    const allMetrics: ChartMetrics[] = [];

    for (const chartId of chartIds) {
      const chart = this.page.locator(`#${chartId}`);
      const metrics = await this.getChartMetrics(chart);
      allMetrics.push(metrics);
    }

    return allMetrics;
  }

  /**
   * Get ApexCharts version from the page
   */
  async getApexChartsVersion(): Promise<string | null> {
    return await this.page.evaluate(() => {
      const ApexCharts = (window as any).ApexCharts;
      if (ApexCharts && ApexCharts.version) {
        return ApexCharts.version;
      }
      return null;
    });
  }

  /**
   * Check for chart-related JavaScript errors
   */
  async getChartErrors(): Promise<string[]> {
    const errors: string[] = [];

    // Listen for page errors
    this.page.on('pageerror', (error) => {
      if (error.message.toLowerCase().includes('apexcharts') ||
          error.message.toLowerCase().includes('chart')) {
        errors.push(error.message);
      }
    });

    // Listen for console errors
    this.page.on('console', (msg) => {
      if (msg.type() === 'error') {
        const text = msg.text();
        if (text.toLowerCase().includes('apexcharts') ||
            text.toLowerCase().includes('chart')) {
          errors.push(text);
        }
      }
    });

    return errors;
  }

  /**
   * Simulate server data refresh
   */
  async simulateDataRefresh(): Promise<void> {
    await this.page.evaluate(() => {
      // Trigger the server stats refresh function if available
      if ((window as any).refreshServerStats) {
        (window as any).refreshServerStats();
      }
    });

    // Wait for charts to update
    await this.page.waitForTimeout(2000);
  }

  /**
   * Take screenshot of all charts for visual comparison
   */
  async screenshotAllCharts(filename: string): Promise<void> {
    const serverTab = this.page.locator('#server-tab');
    await serverTab.screenshot({ path: `screenshots/${filename}` });
  }

  /**
   * Verify chart accessibility attributes
   */
  async verifyChartAccessibility(chartLocator: Locator): Promise<boolean> {
    try {
      const svg = chartLocator.locator('.apexcharts-svg');
      
      // Check for ARIA attributes
      const role = await svg.getAttribute('role');
      const ariaLabel = await svg.getAttribute('aria-label');
      
      // ApexCharts should have proper accessibility attributes
      return role !== null || ariaLabel !== null;
    } catch (error) {
      console.log(`Accessibility check failed: ${error}`);
      return false;
    }
  }

  /**
   * Wait for chart animations to complete
   */
  async waitForAnimationsComplete(timeoutMs: number = 3000): Promise<void> {
    // Wait for CSS animations to complete
    await this.page.waitForTimeout(1000);
    
    // Check if animations are still running
    const animationsRunning = await this.page.evaluate(() => {
      const elements = document.querySelectorAll('.apexcharts-canvas *');
      for (const element of elements) {
        const style = window.getComputedStyle(element);
        if (style.animationDuration !== '0s' && style.animationName !== 'none') {
          return true;
        }
      }
      return false;
    });

    if (animationsRunning) {
      await this.page.waitForTimeout(timeoutMs);
    }
  }

  /**
   * Monitor chart memory usage
   */
  async getChartMemoryUsage(): Promise<number> {
    return await this.page.evaluate(() => {
      // Get performance memory if available
      if ((performance as any).memory) {
        return (performance as any).memory.usedJSHeapSize;
      }
      return 0;
    });
  }
}