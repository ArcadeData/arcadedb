/**
 * ApexCharts Integration Test Suite
 * 
 * Tests to verify ApexCharts v5.3.4 integrates properly with other 
 * ArcadeDB Studio components and doesn't interfere with existing functionality.
 * 
 * Integration Points Tested:
 * 1. Interaction with Query execution and results
 * 2. Server tab navigation and data updates
 * 3. Theme switching compatibility
 * 4. Responsive layout behavior
 * 5. Browser compatibility
 * 6. Export functionality interaction
 * 7. Settings panel interaction
 */

import { test, expect, Page } from '@playwright/test';
import { ArcadeStudioTestHelper } from '../utils/test-utils';

class ApexChartsIntegrationHelper {
  constructor(private page: Page, private helper: ArcadeStudioTestHelper) {}

  /**
   * Navigate to Server tab and verify charts load with other components
   */
  async navigateToServerTabAndVerifyIntegration(): Promise<void> {
    await this.page.getByRole('link', { name: 'Server' }).click();
    await expect(this.page.getByText('Server Info')).toBeVisible({ timeout: 10000 });
    await this.page.waitForLoadState('networkidle');
    
    // Wait for all components to initialize
    await this.page.waitForTimeout(3000);
    
    // Verify charts coexist with other components
    await expect(this.page.locator('.apexcharts-svg')).toHaveCount(6);
    await expect(this.page.getByText('Connected to')).toBeVisible();
    await expect(this.page.locator('[data-bs-toggle="tab"]')).toHaveCountGreaterThan(0);
  }

  /**
   * Test charts update when switching between tabs
   */
  async testTabSwitchingWithCharts(): Promise<void> {
    // Start on Server tab with charts
    await this.navigateToServerTabAndVerifyIntegration();
    
    // Switch to Query tab
    await this.page.getByRole('link', { name: 'Query' }).click();
    await this.page.waitForTimeout(1000);
    
    // Switch back to Server tab
    await this.page.getByRole('link', { name: 'Server' }).click();
    await this.page.waitForTimeout(2000);
    
    // Verify charts are properly recreated
    await expect(this.page.locator('.apexcharts-svg')).toHaveCount(6);
    
    // Test multiple rapid switches
    for (let i = 0; i < 3; i++) {
      await this.page.getByRole('link', { name: 'Query' }).click();
      await this.page.waitForTimeout(300);
      await this.page.getByRole('link', { name: 'Server' }).click();
      await this.page.waitForTimeout(500);
    }
    
    // Final verification
    await expect(this.page.locator('.apexcharts-svg')).toHaveCount(6);
  }

  /**
   * Test charts work correctly with database switching
   */
  async testDatabaseSwitchingWithCharts(): Promise<void> {
    await this.navigateToServerTabAndVerifyIntegration();
    
    // Get current database
    const currentDb = await this.page.getByLabel('root').inputValue();
    
    // Switch database and verify charts still work
    await this.page.getByLabel('root').selectOption(''); // Select empty/default
    await this.page.waitForTimeout(1000);
    
    // Switch back to original database
    await this.page.getByLabel('root').selectOption(currentDb);
    await this.page.waitForTimeout(2000);
    
    // Verify charts still render
    await expect(this.page.locator('.apexcharts-svg')).toHaveCount(6);
  }

  /**
   * Test charts coexist with DataTables components
   */
  async testDataTablesIntegration(): Promise<void> {
    await this.navigateToServerTabAndVerifyIntegration();
    
    // Verify DataTables and ApexCharts coexist on Server tab
    await expect(this.page.locator('.apexcharts-svg')).toHaveCount(6);
    
    // Check if DataTables are present (server metrics table)
    const dataTables = this.page.locator('.dt-container, .dataTables_wrapper');
    if (await dataTables.count() > 0) {
      await expect(dataTables.first()).toBeVisible();
      
      // Interact with DataTable if present
      const tableRows = this.page.locator('tbody tr');
      if (await tableRows.count() > 0) {
        await tableRows.first().hover();
      }
    }
    
    // Verify charts still work after DataTable interaction
    await expect(this.page.locator('.apexcharts-svg')).toHaveCount(6);
  }

  /**
   * Test charts work with Bootstrap modal dialogs
   */
  async testModalIntegration(): Promise<void> {
    await this.navigateToServerTabAndVerifyIntegration();
    
    // Try to trigger server settings modal if available
    const settingsButtons = this.page.locator('button:has-text("Update"), button[title*="setting"], button[title*="config"]');
    const settingsCount = await settingsButtons.count();
    
    if (settingsCount > 0) {
      await settingsButtons.first().click();
      await this.page.waitForTimeout(1000);
      
      // If modal appeared, close it
      const modal = this.page.locator('.modal, .swal2-popup');
      if (await modal.count() > 0) {
        // Try to close modal
        const closeButtons = this.page.locator('.modal-header .btn-close, .swal2-close, button:has-text("Cancel")');
        if (await closeButtons.count() > 0) {
          await closeButtons.first().click();
        } else {
          await this.page.keyboard.press('Escape');
        }
        await this.page.waitForTimeout(500);
      }
    }
    
    // Verify charts still work after modal interaction
    await expect(this.page.locator('.apexcharts-svg')).toHaveCount(6);
  }

  /**
   * Test charts work with responsive layout changes
   */
  async testResponsiveLayoutIntegration(): Promise<void> {
    await this.navigateToServerTabAndVerifyIntegration();
    
    const viewportSizes = [
      { width: 1920, height: 1080 }, // Desktop
      { width: 1366, height: 768 },  // Laptop
      { width: 1024, height: 768 },  // Tablet landscape
      { width: 768, height: 1024 },  // Tablet portrait
      { width: 480, height: 800 }    // Mobile
    ];
    
    for (const size of viewportSizes) {
      await this.page.setViewportSize(size);
      await this.page.waitForTimeout(1000);
      
      // Verify charts adapt to viewport
      const charts = this.page.locator('.apexcharts-svg');
      const chartCount = await charts.count();
      
      // Charts should still be present (might be fewer on mobile)
      expect(chartCount).toBeGreaterThan(0);
      
      // Verify at least some charts are visible
      await expect(charts.first()).toBeVisible();
    }
    
    // Restore desktop size
    await this.page.setViewportSize({ width: 1920, height: 1080 });
  }

  /**
   * Test charts work with browser zoom levels
   */
  async testBrowserZoomIntegration(): Promise<void> {
    await this.navigateToServerTabAndVerifyIntegration();
    
    const zoomLevels = [0.5, 0.75, 1.0, 1.25, 1.5];
    
    for (const zoom of zoomLevels) {
      await this.page.setViewportSize({ width: Math.round(1920 / zoom), height: Math.round(1080 / zoom) });
      await this.page.evaluate((zoomLevel) => {
        document.body.style.zoom = zoomLevel.toString();
      }, zoom);
      
      await this.page.waitForTimeout(1000);
      
      // Verify charts still render at different zoom levels
      await expect(this.page.locator('.apexcharts-svg').first()).toBeVisible();
    }
    
    // Reset zoom
    await this.page.evaluate(() => {
      document.body.style.zoom = '1';
    });
    await this.page.setViewportSize({ width: 1920, height: 1080 });
  }

  /**
   * Test charts don't interfere with console errors or logging
   */
  async testConsoleIntegration(): Promise<void> {
    const consoleMessages: string[] = [];
    const errorMessages: string[] = [];
    
    this.page.on('console', msg => {
      consoleMessages.push(msg.text());
      if (msg.type() === 'error') {
        errorMessages.push(msg.text());
      }
    });
    
    await this.navigateToServerTabAndVerifyIntegration();
    
    // Wait for all initialization
    await this.page.waitForTimeout(5000);
    
    // Filter out expected/harmless messages
    const chartRelatedErrors = errorMessages.filter(msg => 
      msg.includes('ApexCharts') || 
      msg.includes('chart') ||
      msg.includes('svg') ||
      msg.includes('canvas')
    );
    
    console.log('Chart-related console messages:', chartRelatedErrors);
    
    // Should not have any critical ApexCharts errors
    expect(chartRelatedErrors.length).toBe(0);
  }

  /**
   * Test memory management with other components
   */
  async testMemoryIntegration(): Promise<void> {
    const initialMemory = await this.page.evaluate(() => {
      return (performance as any).memory ? (performance as any).memory.usedJSHeapSize : 0;
    });
    
    // Navigate to server tab and load charts
    await this.navigateToServerTabAndVerifyIntegration();
    
    // Interact with various components
    await this.testTabSwitchingWithCharts();
    await this.testDataTablesIntegration();
    
    const finalMemory = await this.page.evaluate(() => {
      return (performance as any).memory ? (performance as any).memory.usedJSHeapSize : 0;
    });
    
    const memoryIncrease = finalMemory - initialMemory;
    console.log(`Memory increase: ${memoryIncrease / 1024 / 1024} MB`);
    
    // Memory increase should be reasonable (less than 20MB for all interactions)
    expect(memoryIncrease).toBeLessThan(20 * 1024 * 1024);
  }
}

test.describe('ApexCharts Integration Tests', () => {
  let helper: ArcadeStudioTestHelper;
  let integrationHelper: ApexChartsIntegrationHelper;

  test.beforeEach(async ({ page }) => {
    helper = new ArcadeStudioTestHelper(page);
    integrationHelper = new ApexChartsIntegrationHelper(page, helper);
    await helper.login();
  });

  test('should integrate properly with Server tab navigation', async () => {
    await integrationHelper.navigateToServerTabAndVerifyIntegration();
    
    // Verify charts coexist with other server components
    await expect(helper.page.getByText('Server Info')).toBeVisible();
    await expect(helper.page.locator('.apexcharts-svg')).toHaveCount(6);
    await expect(helper.page.getByText('Connected to')).toBeVisible();
  });

  test('should maintain chart functionality during tab switching', async () => {
    await integrationHelper.testTabSwitchingWithCharts();
    
    // Final verification that all charts work
    const chartElements = helper.page.locator('.apexcharts-svg');
    await expect(chartElements).toHaveCount(6);
    
    // Verify charts are interactive
    await chartElements.first().hover();
  });

  test('should work correctly with database switching', async () => {
    await integrationHelper.testDatabaseSwitchingWithCharts();
  });

  test('should coexist with DataTables components', async () => {
    await integrationHelper.testDataTablesIntegration();
  });

  test('should work with Bootstrap modal dialogs', async () => {
    await integrationHelper.testModalIntegration();
  });

  test('should adapt to responsive layout changes', async () => {
    await integrationHelper.testResponsiveLayoutIntegration();
  });

  test('should handle browser zoom levels correctly', async () => {
    await integrationHelper.testBrowserZoomIntegration();
  });

  test('should not produce console errors in integration', async () => {
    await integrationHelper.testConsoleIntegration();
  });

  test('should manage memory efficiently with other components', async () => {
    await integrationHelper.testMemoryIntegration();
  });

  test('should work correctly after full page reload', async () => {
    await integrationHelper.navigateToServerTabAndVerifyIntegration();
    
    // Reload page
    await helper.page.reload();
    
    // Re-login after reload
    await helper.login();
    
    // Navigate back to server tab
    await integrationHelper.navigateToServerTabAndVerifyIntegration();
    
    // Verify everything still works
    await expect(helper.page.locator('.apexcharts-svg')).toHaveCount(6);
  });

  test('should maintain performance with concurrent component operations', async () => {
    await integrationHelper.navigateToServerTabAndVerifyIntegration();
    
    const startTime = performance.now();
    
    // Perform multiple operations concurrently
    const operations = [
      helper.page.setViewportSize({ width: 1200, height: 800 }),
      helper.page.locator('.apexcharts-svg').first().hover(),
      helper.page.getByLabel('root').selectOption('Beer'),
      helper.page.waitForTimeout(1000)
    ];
    
    await Promise.all(operations);
    
    const endTime = performance.now();
    const totalTime = endTime - startTime;
    
    console.log(`Concurrent operations completed in: ${totalTime}ms`);
    
    // Should complete quickly even with charts present
    expect(totalTime).toBeLessThan(3000);
    
    // Verify charts still work
    await expect(helper.page.locator('.apexcharts-svg')).toHaveCount(6);
  });

  test('should handle error scenarios gracefully', async () => {
    await integrationHelper.navigateToServerTabAndVerifyIntegration();
    
    // Simulate potential error conditions
    await helper.page.evaluate(() => {
      // Temporarily break server data updates
      (window as any).originalUpdateServer = (window as any).updateServer;
      (window as any).updateServer = () => {
        throw new Error('Simulated server update error');
      };
    });
    
    // Wait a bit for potential error handling
    await helper.page.waitForTimeout(2000);
    
    // Restore original function
    await helper.page.evaluate(() => {
      if ((window as any).originalUpdateServer) {
        (window as any).updateServer = (window as any).originalUpdateServer;
      }
    });
    
    // Verify charts are still present and functional
    await expect(helper.page.locator('.apexcharts-svg')).toHaveCount(6);
  });
});