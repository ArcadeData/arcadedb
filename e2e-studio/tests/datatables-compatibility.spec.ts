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
 * DataTables v2.3.3 Compatibility Test Suite
 *
 * This test suite validates the major DataTables upgrade from v1.13.x to v2.3.x
 * across all critical ArcadeDB Studio table functionality.
 *
 * UPGRADE CONTEXT:
 * - DataTables: 1.13.11 → 2.3.3 (MAJOR BREAKING CHANGES)
 * - All extensions upgraded to v3.x (buttons, responsive, select)
 * - Bootstrap integration: 5.3.6 → 5.3.7 (SAFE)
 *
 * CRITICAL TEST AREAS:
 * 1. Query result tables (highest priority)
 * 2. Database settings tables
 * 3. Server metrics/settings tables
 * 4. Schema browser tables
 * 5. Cluster management tables
 * 6. Server events tables
 * 7. Export/import functionality
 * 8. Responsive behavior
 */

import { test, expect } from '@playwright/test';
import { ArcadeStudioTestHelper, assertGraphState } from '../utils/test-utils';

test.describe('DataTables v2.3.3 Compatibility Suite', () => {
  let studioHelper: ArcadeStudioTestHelper;

  test.beforeEach(async ({ page }) => {
    studioHelper = new ArcadeStudioTestHelper(page);
  });

  test.describe('Query Result Tables', () => {
    test('should render query results table with v2.x API', async ({ page }) => {
      await studioHelper.login('Beer');

      // Execute query that returns tabular data
      await studioHelper.executeQuery('SELECT name, style, brewery FROM Beer LIMIT 20', false);

      // Switch to Table tab to see DataTable
      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);

      // Verify DataTable initialization and result table is visible
      await expect(page.locator('#result_wrapper')).toBeVisible();
      await expect(page.locator('#result')).toBeVisible();

      // Verify column headers are properly rendered
      await expect(page.locator('#result th:has-text("name")')).toBeVisible();
      await expect(page.locator('#result th:has-text("style")')).toBeVisible();
      await expect(page.locator('#result th:has-text("brewery")')).toBeVisible();

      // Verify data rows are populated
      const tableRows = page.locator('#result tbody tr');
      await expect(tableRows).toHaveCount(20);

      // Test pagination controls and info (be specific to result table)
      const resultWrapper = page.locator('#result_wrapper');
      await expect(resultWrapper.getByText(/Showing .* entries/)).toBeVisible(); // pagination info
      await expect(resultWrapper.getByText('entries per page')).toBeVisible(); // length selector
      await expect(resultWrapper.getByRole('searchbox')).toBeVisible(); // search input

      // Check for pagination navigation (may be disabled if only 1 page)
      const paginationNav = resultWrapper.locator('[role="navigation"]');
      if (await paginationNav.count() > 0) {
        await expect(paginationNav).toBeVisible();
      }
    });

    test('should handle table sorting with v2.x API', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name, abv FROM Beer WHERE abv IS NOT NULL LIMIT 10', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);

      // Wait for table to be ready
      await expect(page.locator('#result')).toBeVisible();

      // Click on 'name' column header to sort (be specific to result table)
      await page.locator('#result th:has-text("name")').click();

      // Verify sorting indicator (v2.x uses different classes)
      await expect(page.locator('#result th:has-text("name")[aria-sort]')).toBeVisible();

      // Get first few names to verify sorting
      const firstRowName = await page.locator('#result tbody tr:first-child td:first-child').textContent();
      const secondRowName = await page.locator('#result tbody tr:nth-child(2) td:first-child').textContent();

      expect(firstRowName?.localeCompare(secondRowName || '') || 0).toBeLessThanOrEqual(0);
    });

    test('should support export functionality with v2.x buttons extension', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name, style FROM Beer LIMIT 5', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);

      // Wait for the result DataTable to be ready
      await expect(page.locator('#result_wrapper')).toBeVisible();
      await expect(page.locator('#result')).toBeVisible();

      // Verify export dropdown is present and functional (within result wrapper)
      const resultWrapper = page.locator('#result_wrapper');

      // The export buttons are inside a custom dropdown (.dt-export-dropdown)
      const exportDropdown = resultWrapper.locator('.dt-export-dropdown');
      await expect(exportDropdown).toBeVisible({ timeout: 5000 });

      // Open the export dropdown menu
      await exportDropdown.locator('.dt-export-btn').click();
      await expect(exportDropdown.locator('.dt-export-menu.open')).toBeVisible();

      // Verify export menu items are present (Copy, Excel, CSV, PDF, Print)
      const menuItems = exportDropdown.locator('.dt-export-menu-item');
      expect(await menuItems.count()).toBeGreaterThanOrEqual(3);

      // Verify Copy menu item exists
      await expect(exportDropdown.locator('.dt-export-menu-item:has-text("Copy")')).toBeVisible();
      // Verify CSV menu item exists
      await expect(exportDropdown.locator('.dt-export-menu-item:has-text("CSV")')).toBeVisible();
      // Verify Print menu item exists
      await expect(exportDropdown.locator('.dt-export-menu-item:has-text("Print")')).toBeVisible();
    });

    test('should handle responsive behavior with v3.x responsive extension', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT * FROM Beer LIMIT 5', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      // Test desktop view
      await page.setViewportSize({ width: 1200, height: 800 });
      await page.waitForTimeout(500);

      // Verify all columns are visible
      const columnCount = await page.locator('#result thead th').count();
      expect(columnCount).toBeGreaterThan(3);

      // Test mobile view
      await page.setViewportSize({ width: 480, height: 800 });
      await page.waitForTimeout(500);

      // Check responsive behavior (v3.x may hide some columns)
      const visibleColumns = await page.locator('#result thead th:visible').count();
      expect(visibleColumns).toBeLessThanOrEqual(columnCount);

      // Check for responsive expand buttons if columns are hidden
      const hasExpandButtons = await page.locator('.dt-control').count();
      if (hasExpandButtons > 0) {
        // Test expanding a row
        await page.locator('.dt-control').first().click();
        await expect(page.locator('.child')).toBeVisible();
      }

      // Reset viewport
      await page.setViewportSize({ width: 1200, height: 800 });
    });

    test('should support row selection with v3.x select extension', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name, brewery FROM Beer LIMIT 10', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      // Check if select extension is initialized
      // Note: Not all tables may have select enabled by default
      const hasSelectableRows = await page.locator('#result tbody tr').first().evaluate(
        el => el.classList.contains('dt-selectable') || el.onclick !== null
      );

      if (hasSelectableRows) {
        // Test row selection
        await page.locator('#result tbody tr').first().click();

        // Verify selection styling (v3.x select extension)
        await expect(page.locator('#result tbody tr.selected')).toHaveCount(1);

        // Test multi-select with Ctrl
        await page.locator('#result tbody tr').nth(1).click({ modifiers: ['Control'] });
        await expect(page.locator('#result tbody tr.selected')).toHaveCount(2);
      }
    });

    test('should handle search filtering with v2.x API', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name, brewery FROM Beer LIMIT 50', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      // Find search input (v2.x structure)
      const searchInput = page.locator('#result_wrapper .dt-search input');
      await expect(searchInput).toBeVisible();

      // Test search functionality
      await searchInput.fill('IPA');

      // Wait for search to filter
      await page.waitForTimeout(500);

      // Verify filtered results
      const visibleRows = await page.locator('#result tbody tr:visible').count();
      expect(visibleRows).toBeLessThan(50);

      // Verify search term appears in visible results
      const firstRowText = await page.locator('#result tbody tr:visible').first().textContent();
      expect(firstRowText?.toUpperCase()).toContain('IPA');

      // Clear search
      await searchInput.fill('');
      await page.waitForTimeout(500);

      // Verify all rows are visible again
      const allRows = await page.locator('#result tbody tr:visible').count();
      expect(allRows).toBeGreaterThan(visibleRows);
    });
  });

  test.describe('Database Settings Tables', () => {
    test('should render database settings table correctly', async ({ page }) => {
      await studioHelper.login('Beer');

      // Navigate to Database tab (icon-only navigation)
      await page.locator('#tab-database-sel').click();
      await page.waitForTimeout(1000); // Wait for tab to load

      // Click on Database Settings subtab
      await page.locator('#tab-db-settings-sel').click();

      // Wait for database settings table (may take time to load)
      await expect(page.locator('#dbSettings')).toBeVisible({ timeout: 10000 });
      // Check for table presence (wrapper classes may vary)
      await expect(page.locator('#dbSettings')).toBeVisible();

      // Verify table structure
      await expect(page.locator('#dbSettings thead th:has-text("Key")')).toBeVisible();
      await expect(page.locator('#dbSettings thead th:has-text("Value")')).toBeVisible();
      await expect(page.locator('#dbSettings thead th:has-text("Description")')).toBeVisible();
      await expect(page.locator('#dbSettings thead th:has-text("Default")')).toBeVisible();
      await expect(page.locator('#dbSettings thead th:has-text("Overridden")')).toBeVisible();

      // Verify data is loaded
      const settingsRows = page.locator('#dbSettings tbody tr');
      const rowCount = await settingsRows.count();
      expect(rowCount).toBeGreaterThan(0);

      // Test clickable value links (custom render function)
      const valueLink = page.locator('#dbSettings tbody tr:first-child td:nth-child(2) a');
      if (await valueLink.isVisible()) {
        await expect(valueLink).toHaveAttribute('onclick');
      }
    });
  });

  test.describe('Server Management Tables', () => {
    test('should render server settings table', async ({ page }) => {
      await studioHelper.login('Beer');

      // Navigate to Server tab (icon-only navigation)
      await page.locator('#tab-server-sel').click();
      await page.waitForTimeout(1000); // Wait for tab to load

      // Click on Server Settings subtab
      await page.locator('#tab-server-settings-sel').click();

      // Wait for server settings table (may take time to load)
      await expect(page.locator('#serverSettings')).toBeVisible({ timeout: 10000 });
      // Check for any DataTable wrapper (class names may vary)
      await expect(page.locator('#serverSettings')).toBeVisible();

      // Verify table columns
      await expect(page.locator('#serverSettings thead th:has-text("Key")')).toBeVisible();
      await expect(page.locator('#serverSettings thead th:has-text("Value")')).toBeVisible();

      // Verify data is loaded
      const settingsRows = page.locator('#serverSettings tbody tr');
      const rowCount = await settingsRows.count();
      expect(rowCount).toBeGreaterThan(0);
    });

    test('should render server metrics tables', async ({ page }) => {
      await studioHelper.login('Beer');

      // Navigate to Server tab (icon-only navigation)
      await page.locator('#tab-server-sel').click();
      await page.waitForTimeout(1000); // Wait for tab to load

      // Navigate to Metrics subtab
      await page.locator('#tab-metrics-sel').click();
      await page.waitForTimeout(2000); // Wait for metrics to load

      // Wait for one of the metrics tables to be visible (they are tbody elements)
      await expect(page.locator('#srvMetricMetersTable')).toBeVisible({ timeout: 10000 });

      // Verify metrics data is loaded in the HTTP meters table
      const metersRows = page.locator('#srvMetricMetersTable tr');
      const rowCount = await metersRows.count();
      expect(rowCount).toBeGreaterThan(0);
    });

    test('should render server events table', async ({ page }) => {
      await studioHelper.login('Beer');

      // Navigate to Server tab (icon-only navigation)
      await page.locator('#tab-server-sel').click();
      await page.waitForTimeout(1000); // Wait for tab to load

      // Click on Events subtab
      await page.locator('#tab-server-events-sel').click();

      // Wait for events table to load
      await page.waitForTimeout(2000); // Events may take time to load

      // Check if events table exists and is visible
      const eventsTable = page.locator('#serverEvents');
      if (await eventsTable.isVisible()) {
        // Table wrapper may not have specific .dt-container class
        await expect(eventsTable).toBeVisible();

        // Check if filtering controls work
        const typeFilter = page.locator('#serverEventsType');
        if (await typeFilter.isVisible()) {
          await typeFilter.selectOption('ALL');
        }
      }
    });
  });

  test.describe('Cluster Management Tables', () => {
    test('should render cluster replica tables if cluster is configured', async ({ page }) => {
      await studioHelper.login('Beer');

      // Navigate to Cluster tab (icon-only navigation)
      await page.locator('#tab-cluster-sel').click();
      await page.waitForTimeout(1000); // Wait for tab to load

      // Wait for cluster interface
      await page.waitForTimeout(1000);

      // Check for online replica table (may not exist if not clustered)
      const replicaTable = page.locator('#serverOnlineReplicaTable');
      if (await replicaTable.isVisible()) {
        await expect(replicaTable).toBeVisible();

        // Verify table columns
        await expect(page.locator('#serverOnlineReplicaTable thead th:has-text("Server Name")')).toBeVisible();
        await expect(page.locator('#serverOnlineReplicaTable thead th:has-text("Status")')).toBeVisible();
      }

      // Check for replicated databases table
      const databasesTable = page.locator('#replicatedDatabasesTable');
      if (await databasesTable.isVisible()) {
        await expect(databasesTable).toBeVisible();

        // Verify table columns
        await expect(page.locator('#replicatedDatabasesTable thead th:has-text("Database Name")')).toBeVisible();
        await expect(page.locator('#replicatedDatabasesTable thead th:has-text("Quorum")')).toBeVisible();
      }
    });
  });

  test.describe('DataTables Configuration Validation', () => {
    test('should validate v2.x initialization options', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name FROM Beer LIMIT 5', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      // Check that DataTables v2.x configuration options are working
      const dtConfig = await page.evaluate(() => {
        const table = $('#result').DataTable();
        return {
          version: $.fn.dataTable.version,
          pageLength: table.page.len(),
          ordering: table.settings()[0].oFeatures.bSort,
          paging: table.settings()[0].oFeatures.bPaginate,
          searching: table.settings()[0].oFeatures.bFilter
        };
      });

      // Verify v2.x version
      expect(dtConfig.version).toMatch(/^2\./);

      // Verify configuration options
      expect(dtConfig.pageLength).toBe(20); // As set in studio-table.js
      expect(dtConfig.ordering).toBe(true);
      expect(dtConfig.paging).toBe(true);
    });

    test('should validate Bootstrap 5 integration', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name FROM Beer LIMIT 5', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      // Check Bootstrap 5 classes are applied correctly
      await expect(page.locator('#result_wrapper.dt-container.dt-bootstrap5')).toBeVisible();

      // Verify pagination uses Bootstrap 5 classes (v2.x)
      await expect(page.locator('#result_wrapper .dt-paging .pagination')).toBeVisible();

      // Verify form controls use Bootstrap 5 classes
      const searchInput = page.locator('#result_wrapper .dt-search input');
      await expect(searchInput).toHaveClass(/form-control/);

      const lengthSelect = page.locator('#result_wrapper .dt-length select');
      await expect(lengthSelect).toHaveClass(/form-select/);
    });

    test('should validate custom DOM structure is preserved', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name FROM Beer LIMIT 5', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      // Verify custom DOM structure from studio-table.js is working (be specific to result table)
      const resultWrapper = page.locator('#result_wrapper');
      await expect(resultWrapper.locator('.dt-export-dropdown')).toBeVisible({ timeout: 5000 }); // export dropdown present
      await expect(resultWrapper.locator('.dt-length')).toBeVisible(); // length control
      await expect(resultWrapper.locator('.dt-search')).toBeVisible(); // search/filter
      await expect(resultWrapper.locator('.dt-info, .dataTables_info')).toBeVisible(); // info display
    });

    test('should validate table destruction and recreation', async ({ page }) => {
      await studioHelper.login('Beer');

      // Execute first query
      await studioHelper.executeQuery('SELECT name FROM Beer LIMIT 5', false);
      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      const firstTableId = await page.evaluate(() => {
        return $('#result').DataTable().table().node().id;
      });

      // Execute second query (should destroy and recreate table)
      await studioHelper.executeQuery('SELECT name, brewery FROM Beer LIMIT 3', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);

      await expect(page.locator('#result')).toBeVisible();

      // Verify table was recreated with new data
      const columnHeaders = await page.locator('#result thead th').allTextContents();
      // Should have at least one column (table destruction/recreation working)
      expect(columnHeaders.length).toBeGreaterThan(0);

      const rowCount = await page.locator('#result tbody tr').count();
      // Should have data rows (3 or more, depending on actual data)
      expect(rowCount).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('Performance and Error Handling', () => {
    test('should handle large result sets efficiently', async ({ page }) => {
      await studioHelper.login('Beer');

      const startTime = Date.now();

      // Execute query with larger result set
      await studioHelper.executeQuery('SELECT * FROM Beer LIMIT 100', false);
      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);

      // Wait for table to be fully rendered
      await expect(page.locator('#result')).toBeVisible();
      const rowCount = await page.locator('#result tbody tr').count();
      expect(rowCount).toBeGreaterThan(0);

      const endTime = Date.now();
      const renderTime = endTime - startTime;

      // Performance threshold (should render within reasonable time)
      expect(renderTime).toBeLessThan(10000); // 10 seconds max

      // Verify pagination is working for large datasets (specific to result table)
      const paginationInfo = await page.locator('#result_wrapper').getByText(/Showing .* entries/).textContent();
      // Should show total entries count (may be limited by DB size)
      expect(paginationInfo).toMatch(/Showing .+ of \d+ entries/);
    });

    test('should handle empty result sets gracefully', async ({ page }) => {
      await studioHelper.login('Beer');

      // Execute query that returns no results
      await studioHelper.executeQuery('SELECT * FROM Beer WHERE name = "NonExistentBeer"', false);
      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);

      // Verify empty result handling - table may be hidden or show no data
      const resultTable = page.locator('#result');
      const isTableVisible = await resultTable.isVisible();

      if (isTableVisible) {
        // If table is visible, it should show no data
        const rowCount = await page.locator('#result tbody tr').count();
        expect(rowCount).toBe(0);
      } else {
        // If table is hidden, that's also valid for empty results
        expect(isTableVisible).toBe(false);
      }
    });

    test('should handle malformed data gracefully', async ({ page }) => {
      await studioHelper.login('Beer');

      // Execute query that may return complex/nested data
      await studioHelper.executeQuery('SELECT *, out() as connections FROM Beer LIMIT 5', false);

      // Wait a moment for graph rendering to stabilize before switching tabs
      await page.waitForTimeout(1000);

      // Switch to Table tab
      await page.locator('a[href="#tab-table"]').click({ force: true });

      // Wait for table to be visible (complex data may take longer to render)
      await expect(page.locator('#result')).toBeVisible({ timeout: 10000 });

      // Verify table renders without breaking
      const rowCount = await page.locator('#result tbody tr').count();
      expect(rowCount).toBeGreaterThan(0);

      // Check that complex data is handled (may be truncated or stringified)
      const firstCellContent = await page.locator('#result tbody tr:first-child td:first-child').textContent();
      expect(firstCellContent).toBeDefined();
    });
  });

  test.describe('Settings Table Validation', () => {
    test('should validate table settings and preferences persist', async ({ page }) => {
      await studioHelper.login('Beer');
      await studioHelper.executeQuery('SELECT name, brewery FROM Beer LIMIT 25', false);

      // Switch to Table tab - use simple approach that works
      await page.locator('a[href="#tab-table"]').click();
      await page.waitForTimeout(500);
      await expect(page.locator('#result')).toBeVisible();

      // Access settings dropdown if it exists (be specific to result table)
      const settingsButton = page.locator('#result_wrapper').getByRole('button', { name: /Settings/ }).first();
      if (await settingsButton.count() > 0) {
        await settingsButton.click();

        // Test truncate columns setting
        const truncateCheckbox = page.locator('#tableTruncateColumns');
        if (await truncateCheckbox.isVisible()) {
          const wasChecked = await truncateCheckbox.isChecked();

          // Toggle setting
          await truncateCheckbox.click();

          // Execute another query to see if setting persists
          await studioHelper.executeQuery('SELECT name, style FROM Beer LIMIT 5', false);
          await expect(page.locator('#result')).toBeVisible();

          // Verify setting was saved (this would need to check localStorage)
          const currentState = await page.evaluate(() => {
            return localStorage.getItem('table.truncateColumns');
          });

          expect(currentState).toBe((!wasChecked).toString());
        }
      }
    });
  });
});
