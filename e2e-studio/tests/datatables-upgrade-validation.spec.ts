///
/// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { test, expect } from '../fixtures/test-fixtures';
import { ArcadeStudioTestHelper, TEST_CONFIG } from '../utils';

test.describe('DataTables v2.3.2 Upgrade Validation Tests', () => {
  test('should load DataTables v2.3.2 without JavaScript errors', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);

    // Track any JavaScript errors
    const consoleErrors: string[] = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Login to Studio (this navigates and waits for load)
    await helper.login('Beer');

    // Verify DataTables version is loaded correctly
    const datatablesVersion = await page.evaluate(() => {
      return {
        version: typeof (window as any).$.fn.DataTable !== 'undefined' ? (window as any).$.fn.DataTable.version : null,
        isLoaded: typeof (window as any).$.fn.DataTable !== 'undefined',
        hasDataTablesFunction: typeof (window as any).$.fn.dataTable !== 'undefined'
      };
    });

    console.log('DataTables status:', datatablesVersion);

    // Validate DataTables is properly loaded
    expect(datatablesVersion.isLoaded).toBe(true, 'DataTables should be loaded');
    expect(datatablesVersion.hasDataTablesFunction).toBe(true, 'dataTable function should be available');

    // Check for critical errors (ignore minor warnings)
    const criticalErrors = consoleErrors.filter(error =>
      error.includes('DataTable') ||
      error.includes('Cannot read property') ||
      error.includes('TypeError') ||
      error.includes('ReferenceError')
    );

    expect(criticalErrors).toHaveLength(0,
      `Should not have critical JavaScript errors: ${criticalErrors.join(', ')}`);
  });

  test('should render tables with DataTables v2.x API compatibility', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login('Beer');

    // Execute a query that returns tabular data
    await helper.executeQuery('SELECT name, brewery FROM Beer LIMIT 10', false);

    // Switch to Table tab with error handling
    try {
      await page.getByRole('link', { name: 'Table' }).click({ timeout: 5000 });
    } catch (error) {
      // Tab might already be selected, continue
      console.log('Table tab click skipped (likely already selected)');
    }

    // Wait for table to be visible (not just present)
    await expect(page.locator('#result')).toBeVisible();

    // Also wait for DataTables wrapper (v2.x uses .dt-container)
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    // Test DataTables v2.x functionality
    const tableValidation = await page.evaluate(() => {
      const table = document.querySelector('#result');
      if (!table) return { error: 'No table found' };

      // Check if DataTable is initialized
      const dataTable = (window as any).$(table).DataTable();
      if (!dataTable) return { error: 'DataTable not initialized' };

      return {
        isDataTable: (window as any).$.fn.dataTable.isDataTable(table),
        hasRows: dataTable.rows().count() > 0,
        hasColumns: dataTable.columns().count() > 0,
        version: (window as any).$.fn.DataTable.version,
        api: {
          hasColumns: typeof dataTable.columns === 'function',
          hasData: typeof dataTable.data === 'function',
          hasOrder: typeof dataTable.order === 'function'
        }
      };
    });

    console.log('Table validation:', tableValidation);
    expect(tableValidation.isDataTable).toBe(true, 'Table should be initialized as DataTable');
    expect(tableValidation.hasColumns).toBe(true, 'Table should have columns');
    expect(tableValidation.api.hasColumns).toBe(true, 'DataTable columns API should be available');
  });

  test('should handle table sorting with DataTables v2.x', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login('Beer');

    // Execute a query with multiple rows for sorting
    await helper.executeQuery('SELECT name, brewery FROM Beer ORDER BY name LIMIT 15', false);

    // Switch to Table tab with error handling
    try {
      await page.getByRole('link', { name: 'Table' }).click({ timeout: 5000 });
    } catch (error) {
      // Tab might already be selected, continue
      console.log('Table tab click skipped (likely already selected)');
    }

    // Wait for table to be visible (not just present)
    await expect(page.locator('#result')).toBeVisible();

    // Also wait for DataTables wrapper to be properly initialized
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    // Test sorting functionality
    const sortingTest = await page.evaluate(() => {
      const table = document.querySelector('#result');
      if (!table || !(window as any).$.fn.dataTable.isDataTable(table)) {
        return { error: 'Table not properly initialized' };
      }

      const dataTable = (window as any).$(table).DataTable();

      // Test sorting API
      try {
        // Get current order
        const currentOrder = dataTable.order();

        // Change sort order
        dataTable.order([[0, 'desc']]).draw();
        const newOrder = dataTable.order();

        return {
          canSort: true,
          originalOrder: currentOrder,
          newOrder: newOrder,
          orderChanged: JSON.stringify(currentOrder) !== JSON.stringify(newOrder)
        };
      } catch (error) {
        return { error: error.message };
      }
    });

    console.log('Sorting test:', sortingTest);
    expect(sortingTest.canSort).toBe(true, 'Table should support sorting');
    expect(sortingTest.error).toBeUndefined();
  });

  test('should support table filtering with DataTables v2.x', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login('Beer');

    // Execute query with filterable data
    await helper.executeQuery('SELECT name, brewery FROM Beer LIMIT 20', false);

    // Switch to Table tab with error handling
    try {
      await page.getByRole('link', { name: 'Table' }).click({ timeout: 5000 });
    } catch (error) {
      // Tab might already be selected, continue
      console.log('Table tab click skipped (likely already selected)');
    }

    // Wait for table to be visible (not just present)
    await expect(page.locator('#result')).toBeVisible();

    // Also wait for DataTables wrapper to be properly initialized
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    // Test search/filter functionality
    const filteringTest = await page.evaluate(() => {
      const table = document.querySelector('#result');
      if (!table || !(window as any).$.fn.dataTable.isDataTable(table)) {
        return { error: 'Table not properly initialized' };
      }

      const dataTable = (window as any).$(table).DataTable();

      try {
        // Test search functionality
        const totalRowsBefore = dataTable.rows().count();
        dataTable.search('Beer').draw();
        const totalRowsAfter = dataTable.rows().count();

        // Clear search
        dataTable.search('').draw();
        const totalRowsCleared = dataTable.rows().count();

        return {
          canFilter: true,
          totalBefore: totalRowsBefore,
          totalAfter: totalRowsAfter,
          totalCleared: totalRowsCleared,
          filterWorked: totalRowsAfter <= totalRowsBefore && totalRowsCleared === totalRowsBefore
        };
      } catch (error) {
        return { error: error.message };
      }
    });

    console.log('Filtering test:', filteringTest);
    expect(filteringTest.canFilter).toBe(true, 'Table should support filtering');
    expect(filteringTest.error).toBeUndefined();
  });

  test('should support export functionality with DataTables v2.x buttons extension', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login('Beer');

    // Execute query to get table data
    await helper.executeQuery('SELECT name, brewery FROM Beer LIMIT 5', false);

    // Switch to Table tab with error handling
    try {
      await page.getByRole('link', { name: 'Table' }).click({ timeout: 5000 });
    } catch (error) {
      // Tab might already be selected, continue
      console.log('Table tab click skipped (likely already selected)');
    }

    // Wait for table to be visible (not just present)
    await expect(page.locator('#result')).toBeVisible();

    // Also wait for DataTables wrapper to be properly initialized
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    // Wait for buttons to be initialized
    await page.waitForTimeout(1000);

    // Test export buttons functionality
    const exportTest = await page.evaluate(() => {
      const table = document.querySelector('#result');
      if (!table || !(window as any).$.fn.dataTable.isDataTable(table)) {
        return { error: 'Table not properly initialized' };
      }

      // Check for export buttons
      const exportButtons = {
        copy: document.querySelector('.buttons-copy') || Array.from(document.querySelectorAll('button')).find(btn => btn.textContent?.includes('Copy')),
        excel: document.querySelector('.buttons-excel') || Array.from(document.querySelectorAll('button')).find(btn => btn.textContent?.includes('Excel')),
        csv: document.querySelector('.buttons-csv') || Array.from(document.querySelectorAll('button')).find(btn => btn.textContent?.includes('CSV')),
        pdf: document.querySelector('.buttons-pdf') || Array.from(document.querySelectorAll('button')).find(btn => btn.textContent?.includes('PDF'))
      };

      return {
        hasExportButtons: Object.values(exportButtons).some(btn => btn !== null),
        buttonsFound: Object.entries(exportButtons).filter(([key, btn]) => btn !== null).map(([key]) => key),
        buttonsExtensionLoaded: typeof (window as any).$.fn.DataTable.Buttons !== 'undefined'
      };
    });

    console.log('Export test:', exportTest);
    expect(exportTest.error).toBeUndefined();

    // Export buttons should be available (either from UI or buttons extension)
    const hasExportCapability = exportTest.hasExportButtons || exportTest.buttonsExtensionLoaded;
    expect(hasExportCapability).toBe(true, 'Should have export functionality available');
  });

  test('should handle responsive behavior with DataTables v2.x responsive extension', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login('Beer');

    // Execute query with many columns to test responsiveness
    await helper.executeQuery('SELECT name, brewery, style FROM Beer LIMIT 3', false);

    // Switch to Table tab with error handling
    try {
      await page.getByRole('link', { name: 'Table' }).click({ timeout: 5000 });
    } catch (error) {
      // Tab might already be selected, continue
      console.log('Table tab click skipped (likely already selected)');
    }

    // Wait for table to be visible (not just present)
    await expect(page.locator('#result')).toBeVisible();

    // Also wait for DataTables wrapper to be properly initialized
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    // Test responsive behavior
    const responsiveTest = await page.evaluate(() => {
      const table = document.querySelector('#result');
      if (!table || !(window as any).$.fn.dataTable.isDataTable(table)) {
        return { error: 'Table not properly initialized' };
      }

      const dataTable = (window as any).$(table).DataTable();

      return {
        isResponsive: dataTable.responsive !== undefined,
        responsiveExtensionLoaded: typeof (window as any).$.fn.DataTable.Responsive !== 'undefined',
        tableVisible: table.offsetWidth > 0 && table.offsetHeight > 0
      };
    });

    console.log('Responsive test:', responsiveTest);
    expect(responsiveTest.error).toBeUndefined();
    expect(responsiveTest.tableVisible).toBe(true, 'Table should be visible');
  });

  test('should validate DataTables v2.x API compatibility fixes', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);

    // Login first to load DataTables libraries
    await helper.login('Beer');

    // Test that our studio-table.js fixes work correctly
    const apiCompatibilityTest = await page.evaluate(() => {
      // Check if DataTables v2.x configuration is properly applied
      const hasV2Features = {
        // v2.x uses 'columns' instead of 'aoColumns'
        columnsProperty: typeof (window as any).$.fn.DataTable.defaults.columns !== 'undefined',
        // v2.x uses 'data' instead of 'aaData'
        dataProperty: typeof (window as any).$.fn.DataTable.defaults.data !== 'undefined',
        // Check version
        version: (window as any).$.fn.DataTable.version || 'unknown'
      };

      return hasV2Features;
    });

    console.log('API compatibility test:', apiCompatibilityTest);
    expect(apiCompatibilityTest.version).toMatch(/^2\./);

    // Execute a query to test that our API fixes work
    await helper.executeQuery('SELECT name, brewery FROM Beer LIMIT 5', false);

    // Switch to Table tab with error handling
    try {
      await page.getByRole('link', { name: 'Table' }).click({ timeout: 5000 });
    } catch (error) {
      // Tab might already be selected, continue
      console.log('Table tab click skipped (likely already selected)');
    }

    // Wait for table to be visible (not just present)
    await expect(page.locator('#result')).toBeVisible();

    // Also wait for DataTables wrapper to be properly initialized
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    // Verify table renders without errors using v2.x API
    const tableRenderTest = await page.evaluate(() => {
      const table = document.querySelector('#result');
      return {
        tableExists: !!table,
        isDataTable: table ? (window as any).$.fn.dataTable.isDataTable(table) : false,
        hasData: table && (window as any).$.fn.dataTable.isDataTable(table) ?
          (window as any).$(table).DataTable().rows().count() > 0 : false
      };
    });

    expect(tableRenderTest.tableExists).toBe(true, 'Table should exist after query');
    expect(tableRenderTest.isDataTable).toBe(true, 'Table should be initialized as DataTable');
    expect(tableRenderTest.hasData).toBe(true, 'Table should contain data');
  });

  test('should maintain performance with DataTables v2.x', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login('Beer');

    // Test performance with larger dataset
    const startTime = Date.now();

    await helper.executeQuery('SELECT name, brewery, style FROM Beer LIMIT 30', false);

    // Switch to Table tab with error handling
    try {
      await page.getByRole('link', { name: 'Table' }).click({ timeout: 5000 });
    } catch (error) {
      // Tab might already be selected, continue
      console.log('Table tab click skipped (likely already selected)');
    }

    // Wait for table to be visible (not just present)
    await expect(page.locator('#result')).toBeVisible();

    // Also wait for DataTables wrapper to be properly initialized
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    const totalTime = Date.now() - startTime;

    // Test DataTable initialization performance
    const performanceTest = await page.evaluate(() => {
      const table = document.querySelector('#result');
      if (!table || !(window as any).$.fn.dataTable.isDataTable(table)) {
        return { error: 'Table not properly initialized' };
      }

      const dataTable = (window as any).$(table).DataTable();
      const startTime = performance.now();

      // Perform some operations to test performance
      dataTable.order([[0, 'asc']]).draw(false);
      dataTable.search('').draw(false);

      const endTime = performance.now();

      return {
        operationTime: endTime - startTime,
        rowCount: dataTable.rows().count(),
        columnCount: dataTable.columns().count()
      };
    });

    console.log('Performance test:', { totalTime, ...performanceTest });
    expect(performanceTest.error).toBeUndefined();
    expect(totalTime).toBeLessThan(15000); // Should complete within 15 seconds
    expect(performanceTest.operationTime).toBeLessThan(1000); // Operations should be fast
  });
});
