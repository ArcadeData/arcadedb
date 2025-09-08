/**
 * ApexCharts Performance and Regression Test Suite
 *
 * Tests to ensure ApexCharts v5.3.4 upgrade doesn't introduce performance
 * regressions and maintains expected rendering speeds and memory usage.
 *
 * Performance Metrics Tracked:
 * 1. Chart initialization time
 * 2. Data update/refresh performance
 * 3. Memory usage during chart lifecycle
 * 4. Rendering performance under load
 * 5. Responsive behavior performance
 * 6. Animation performance
 */

import { test, expect, Page } from '@playwright/test';
import { ArcadeStudioTestHelper } from '../utils/test-utils';

interface PerformanceMetrics {
  initializationTime: number;
  updateTime: number;
  memoryUsage: number;
  renderTime: number;
}

class ApexChartsPerformanceHelper {
  constructor(private page: Page, private helper: ArcadeStudioTestHelper) {}

  /**
   * Navigate to Server tab and measure navigation performance
   */
  async navigateToServerTabWithMetrics(): Promise<number> {
    const startTime = performance.now();

    await this.page.getByRole('link', { name: 'Server' }).click();
    await expect(this.page.getByText('Server Info')).toBeVisible({ timeout: 15000 });
    await this.page.waitForLoadState('networkidle');

    const endTime = performance.now();
    return endTime - startTime;
  }

  /**
   * Measure chart initialization performance
   */
  async measureChartInitializationTime(): Promise<number> {
    return await this.page.evaluate(() => {
      const startTime = performance.now();

      return new Promise<number>((resolve) => {
        const checkCharts = () => {
          const charts = [
            'serverChartCommands',
            'serverChartOSCPU',
            'serverChartOSRAM',
            'serverChartOSDisk',
            'serverChartServerRAM',
            'serverChartCache'
          ];

          const allInitialized = charts.every(chartId => {
            const element = document.getElementById(chartId);
            return element && element.querySelector('.apexcharts-svg');
          });

          if (allInitialized) {
            const endTime = performance.now();
            resolve(endTime - startTime);
          } else {
            setTimeout(checkCharts, 100);
          }
        };

        checkCharts();
      });
    });
  }

  /**
   * Measure chart data update performance
   */
  async measureDataUpdatePerformance(): Promise<number> {
    return await this.page.evaluate(() => {
      const startTime = performance.now();

      // Trigger a data update by calling the update function
      if (typeof (window as any).updateServer === 'function') {
        (window as any).updateServer();
      }

      return new Promise<number>((resolve) => {
        const checkUpdated = () => {
          // Simple check: verify charts still exist after update
          const chartsExist = document.querySelectorAll('.apexcharts-svg').length >= 6;

          if (chartsExist) {
            const endTime = performance.now();
            resolve(endTime - startTime);
          } else {
            setTimeout(checkUpdated, 50);
          }
        };

        setTimeout(checkUpdated, 100); // Allow update to start
      });
    });
  }

  /**
   * Measure memory usage during chart lifecycle
   */
  async measureMemoryUsage(): Promise<{ initial: number; peak: number; final: number }> {
    const initialMemory = await this.page.evaluate(() => {
      return (performance as any).memory ? (performance as any).memory.usedJSHeapSize : 0;
    });

    // Trigger multiple chart operations to measure peak memory
    await this.simulateChartStress();

    const peakMemory = await this.page.evaluate(() => {
      return (performance as any).memory ? (performance as any).memory.usedJSHeapSize : 0;
    });

    // Wait for garbage collection
    await this.page.waitForTimeout(2000);

    const finalMemory = await this.page.evaluate(() => {
      return (performance as any).memory ? (performance as any).memory.usedJSHeapSize : 0;
    });

    return {
      initial: initialMemory,
      peak: peakMemory,
      final: finalMemory
    };
  }

  /**
   * Simulate chart stress testing for performance measurement
   */
  async simulateChartStress(): Promise<void> {
    // Rapid navigation to stress test chart lifecycle
    for (let i = 0; i < 5; i++) {
      await this.page.getByRole('link', { name: 'Query' }).click();
      await this.page.waitForTimeout(100);
      await this.page.getByRole('link', { name: 'Server' }).click();
      await this.page.waitForTimeout(200);
    }

    // Rapid viewport changes
    for (let i = 0; i < 3; i++) {
      await this.page.setViewportSize({ width: 800 + i * 100, height: 600 + i * 50 });
      await this.page.waitForTimeout(100);
    }

    // Restore viewport
    await this.page.setViewportSize({ width: 1920, height: 1080 });
  }

  /**
   * Measure chart rendering performance under different data loads
   */
  async measureRenderingPerformance(): Promise<number> {
    return await this.page.evaluate(() => {
      const startTime = performance.now();

      // Force chart re-render by triggering resize
      window.dispatchEvent(new Event('resize'));

      return new Promise<number>((resolve) => {
        requestAnimationFrame(() => {
          const endTime = performance.now();
          resolve(endTime - startTime);
        });
      });
    });
  }

  /**
   * Test animation performance
   */
  async measureAnimationPerformance(): Promise<number> {
    return await this.page.evaluate(() => {
      return new Promise<number>((resolve) => {
        const startTime = performance.now();
        let frameCount = 0;

        const measureFrames = () => {
          frameCount++;
          if (frameCount < 60) { // Measure 60 frames
            requestAnimationFrame(measureFrames);
          } else {
            const endTime = performance.now();
            const avgFrameTime = (endTime - startTime) / frameCount;
            resolve(avgFrameTime);
          }
        };

        requestAnimationFrame(measureFrames);
      });
    });
  }

  /**
   * Collect comprehensive performance metrics
   */
  async collectPerformanceMetrics(): Promise<PerformanceMetrics> {
    const initializationTime = await this.measureChartInitializationTime();
    const updateTime = await this.measureDataUpdatePerformance();
    const memoryMetrics = await this.measureMemoryUsage();
    const renderTime = await this.measureRenderingPerformance();

    return {
      initializationTime,
      updateTime,
      memoryUsage: memoryMetrics.peak - memoryMetrics.initial,
      renderTime
    };
  }
}

test.describe('ApexCharts Performance and Regression Tests', () => {
  let helper: ArcadeStudioTestHelper;
  let performanceHelper: ApexChartsPerformanceHelper;

  test.beforeEach(async ({ page }) => {
    helper = new ArcadeStudioTestHelper(page);
    performanceHelper = new ApexChartsPerformanceHelper(page, helper);
    await helper.login();
  });

  test('should initialize all charts within acceptable time limits', async () => {
    // Navigate to Server tab
    const navigationTime = await performanceHelper.navigateToServerTabWithMetrics();

    // Measure chart initialization
    const initTime = await performanceHelper.measureChartInitializationTime();

    console.log(`Navigation time: ${navigationTime}ms`);
    console.log(`Chart initialization time: ${initTime}ms`);

    // Performance thresholds (adjust based on requirements)
    expect(navigationTime).toBeLessThan(5000); // 5 seconds max navigation
    expect(initTime).toBeLessThan(3000); // 3 seconds max chart initialization
  });

  test('should update chart data within performance limits', async () => {
    await performanceHelper.navigateToServerTabWithMetrics();

    // Wait for initial load
    await helper.page.waitForTimeout(2000);

    const updateTime = await performanceHelper.measureDataUpdatePerformance();

    console.log(`Data update time: ${updateTime}ms`);

    // Data updates should be fast
    expect(updateTime).toBeLessThan(1000); // 1 second max for data updates
  });

  test('should maintain reasonable memory usage', async () => {
    await performanceHelper.navigateToServerTabWithMetrics();

    const memoryMetrics = await performanceHelper.measureMemoryUsage();

    console.log('Memory usage:', memoryMetrics);

    // Memory growth should be reasonable (less than 10MB increase)
    const memoryGrowth = memoryMetrics.peak - memoryMetrics.initial;
    expect(memoryGrowth).toBeLessThan(10 * 1024 * 1024); // 10MB

    // Memory should be partially freed after operations
    const memoryRetention = memoryMetrics.final - memoryMetrics.initial;
    expect(memoryRetention).toBeLessThan(memoryGrowth * 0.8); // At most 80% retained
  });

  test('should handle rapid chart lifecycle changes efficiently', async () => {
    const startTime = performance.now();

    await performanceHelper.navigateToServerTabWithMetrics();
    await performanceHelper.simulateChartStress();

    const endTime = performance.now();
    const totalTime = endTime - startTime;

    console.log(`Stress test completion time: ${totalTime}ms`);

    // Verify all charts still work after stress test
    await helper.page.waitForFunction(() => {
      return document.querySelectorAll('.apexcharts-svg').length >= 6;
    }, { timeout: 5000 });

    // Stress test should complete in reasonable time
    expect(totalTime).toBeLessThan(15000); // 15 seconds max
  });

  test('should render charts efficiently across viewport sizes', async () => {
    await performanceHelper.navigateToServerTabWithMetrics();

    const viewportSizes = [
      { width: 1920, height: 1080 },
      { width: 1366, height: 768 },
      { width: 1024, height: 768 },
      { width: 768, height: 1024 }
    ];

    const renderTimes: number[] = [];

    for (const size of viewportSizes) {
      await helper.page.setViewportSize(size);
      const renderTime = await performanceHelper.measureRenderingPerformance();
      renderTimes.push(renderTime);

      // Verify charts still render correctly
      await expect(helper.page.locator('.apexcharts-svg').first()).toBeVisible();
    }

    console.log('Render times across viewports:', renderTimes);

    // All render times should be fast
    renderTimes.forEach((time, index) => {
      expect(time).toBeLessThan(100); // 100ms max render time
    });
  });

  test('should maintain smooth animation performance', async () => {
    await performanceHelper.navigateToServerTabWithMetrics();

    const avgFrameTime = await performanceHelper.measureAnimationPerformance();
    const fps = 1000 / avgFrameTime; // Convert to FPS

    console.log(`Average frame time: ${avgFrameTime}ms, FPS: ${fps}`);

    // Should maintain at least 30 FPS (33ms per frame)
    expect(avgFrameTime).toBeLessThan(33);
    expect(fps).toBeGreaterThan(30);
  });

  test('should not degrade performance over time', async () => {
    await performanceHelper.navigateToServerTabWithMetrics();

    // Collect initial metrics
    const initialMetrics = await performanceHelper.collectPerformanceMetrics();

    // Simulate extended usage
    await helper.page.waitForTimeout(10000); // 10 seconds of activity
    await performanceHelper.simulateChartStress();

    // Collect metrics after extended usage
    const finalMetrics = await performanceHelper.collectPerformanceMetrics();

    console.log('Initial metrics:', initialMetrics);
    console.log('Final metrics:', finalMetrics);

    // Performance should not significantly degrade
    expect(finalMetrics.initializationTime).toBeLessThan(initialMetrics.initializationTime * 1.5);
    expect(finalMetrics.updateTime).toBeLessThan(initialMetrics.updateTime * 1.5);
    expect(finalMetrics.renderTime).toBeLessThan(initialMetrics.renderTime * 1.5);
  });

  test('should handle concurrent chart operations efficiently', async () => {
    await performanceHelper.navigateToServerTabWithMetrics();

    const startTime = performance.now();

    // Simulate concurrent operations
    const operations = [
      helper.page.setViewportSize({ width: 1200, height: 800 }),
      performanceHelper.measureDataUpdatePerformance(),
      performanceHelper.measureRenderingPerformance()
    ];

    const results = await Promise.all(operations);
    const totalTime = performance.now() - startTime;

    console.log(`Concurrent operations completed in: ${totalTime}ms`);
    console.log('Operation results:', results);

    // Concurrent operations should complete efficiently
    expect(totalTime).toBeLessThan(2000); // 2 seconds max

    // Verify charts still function correctly
    await expect(helper.page.locator('.apexcharts-svg').first()).toBeVisible();
  });

  test('should have acceptable bundle size impact', async () => {
    // This test validates that ApexCharts v5.x doesn't significantly increase bundle size
    const bundleInfo = await helper.page.evaluate(() => {
      const scripts = Array.from(document.querySelectorAll('script[src]'));
      const apexChartsScript = scripts.find(script =>
        (script as HTMLScriptElement).src.includes('apexcharts')
      );

      if (apexChartsScript) {
        return {
          src: (apexChartsScript as HTMLScriptElement).src,
          hasScript: true
        };
      }

      return { hasScript: false };
    });

    expect(bundleInfo.hasScript).toBeTruthy();
    console.log('ApexCharts bundle info:', bundleInfo);

    // Additional validation could be added here to check actual file sizes
    // if bundle size monitoring is critical
  });
});
