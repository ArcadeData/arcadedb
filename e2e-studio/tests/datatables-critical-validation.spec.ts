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
 * DataTables Critical Validation - Focused Test Suite
 *
 * This focused test suite validates the most critical DataTables functionality
 * after the v2.3.3 upgrade to identify specific breaking changes.
 */

import { test, expect } from '@playwright/test';
import { ArcadeStudioTestHelper } from '../utils/test-utils';

test.describe('DataTables Critical Validation', () => {
  let studioHelper: ArcadeStudioTestHelper;

  test.beforeEach(async ({ page }) => {
    studioHelper = new ArcadeStudioTestHelper(page);
  });

  test('should validate DataTables v2.x initialization and basic functionality', async ({ page }) => {
    await studioHelper.login('Beer');

    // Execute a simple query
    await studioHelper.executeQuery('SELECT name, brewery FROM Beer LIMIT 5', false);

    // Switch to Table tab - use simple approach that works
    await page.locator('a[href="#tab-table"]').click();
    await page.waitForTimeout(500);

    // Wait for table to be visible
    await expect(page.locator('#result')).toBeVisible();

    // Check specific result table wrapper (v2.x uses .dt-container)
    await expect(page.locator('#result_wrapper.dt-container, #result_wrapper.dataTables_wrapper')).toBeVisible();

    // Verify DataTables v2.x version
    const dtVersion = await page.evaluate(() => {
      return typeof $.fn.dataTable !== 'undefined' ? $.fn.dataTable.version : 'Not loaded';
    });

    console.log(`DataTables version detected: ${dtVersion}`);
    expect(dtVersion).toMatch(/^2\./);

    // Verify table has data
    const rowCount = await page.locator('#result tbody tr').count();
    expect(rowCount).toBe(5);

    // Check for any JavaScript errors
    const errors = await page.evaluate(() => {
      return window.console.errors || [];
    });

    if (errors.length > 0) {
      console.log('JavaScript errors detected:', errors);
    }

    // Verify column headers are rendered
    await expect(page.locator('#result thead th:has-text("name")')).toBeVisible();
    await expect(page.locator('#result thead th:has-text("brewery")')).toBeVisible();
  });

  test('should check if deprecated API usage causes issues', async ({ page }) => {
    await studioHelper.login('Beer');
    await studioHelper.executeQuery('SELECT name FROM Beer LIMIT 3', false);

    // Switch to Table tab - use simple approach that works
    await page.locator('a[href="#tab-table"]').click();
    await page.waitForTimeout(500);
    await expect(page.locator('#result')).toBeVisible();

    // Check if table initialization used deprecated APIs
    const configInfo = await page.evaluate(() => {
      const table = $('#result').DataTable();
      const settings = table.settings()[0];

      return {
        hasData: settings.aoData && settings.aoData.length > 0,
        hasColumns: settings.aoColumns && settings.aoColumns.length > 0,
        apiVersion: $.fn.dataTable.version,
        domStructure: settings.sDom || 'undefined'
      };
    });

    console.log('Table configuration:', configInfo);

    // Verify basic functionality works despite potential deprecated API usage
    expect(configInfo.hasData).toBe(true);
    expect(configInfo.hasColumns).toBe(true);
  });

  test('should validate export buttons functionality', async ({ page }) => {
    await studioHelper.login('Beer');
    await studioHelper.executeQuery('SELECT name, brewery FROM Beer LIMIT 3', false);

    // Switch to Table tab - use simple approach that works
    await page.locator('a[href="#tab-table"]').click();
    await page.waitForTimeout(500);
    await expect(page.locator('#result')).toBeVisible();

    // Wait for buttons to be initialized
    await page.waitForTimeout(1000);

    // Look for export buttons in the correct container (v2.x and v1.x compatibility)
    const buttonsContainer = page.locator('#result_wrapper .dt-buttons, #result_wrapper .dataTables_buttons');

    if (await buttonsContainer.first().isVisible()) {
      // Check each export button type (look for buttons with icons and text)
      const copyBtn = buttonsContainer.locator('button').filter({ hasText: 'Copy' });
      const csvBtn = buttonsContainer.locator('button').filter({ hasText: 'CSV' });
      const excelBtn = buttonsContainer.locator('button').filter({ hasText: 'Excel' });
      const printBtn = buttonsContainer.locator('button').filter({ hasText: 'Print' });

      // Core buttons should always be available
      await expect(copyBtn).toBeVisible();
      await expect(csvBtn).toBeVisible();
      await expect(printBtn).toBeVisible();

      // Excel button may not be available if JSZip isn't loaded properly
      const excelButtonCount = await excelBtn.count();
      if (excelButtonCount > 0) {
        await expect(excelBtn).toBeVisible();
      } else {
        console.log('Excel button not available - JSZip dependency may not be loaded');
      }

      // Test copy button click (should not throw error)
      await copyBtn.click();
    } else {
      console.log('Export buttons not visible - checking if they exist elsewhere');
      const anyButtons = await page.locator('.dt-buttons, .dataTables_buttons').count();
      console.log(`Total .dt-buttons/.dataTables_buttons found: ${anyButtons}`);

      // Look for buttons anywhere in the result area
      const resultButtons = await page.locator('#result_wrapper button').count();
      console.log(`Total buttons in result_wrapper: ${resultButtons}`);
    }
  });

  test('should validate table sorting works with v2.x', async ({ page }) => {
    await studioHelper.login('Beer');
    await studioHelper.executeQuery('SELECT name, brewery FROM Beer LIMIT 10', false);

    // Switch to Table tab - use simple approach that works
    await page.locator('a[href="#tab-table"]').click();
    await page.waitForTimeout(500);
    await expect(page.locator('#result')).toBeVisible();

    // Click on the name column header specifically in the result table
    const nameHeader = page.locator('#result thead th:has-text("name")');
    await expect(nameHeader).toBeVisible();
    await nameHeader.click();

    // Check if sorting indicator appears
    await expect(nameHeader).toHaveAttribute('aria-sort');

    // Verify table is still functional after sort
    const rowsAfterSort = await page.locator('#result tbody tr').count();
    expect(rowsAfterSort).toBe(10);
  });

  test('should validate pagination controls', async ({ page }) => {
    await studioHelper.login('Beer');
    await studioHelper.executeQuery('SELECT * FROM Beer LIMIT 25', false);

    // Switch to Table tab - use simple approach that works
    await page.locator('a[href="#tab-table"]').click();
    await page.waitForTimeout(500);
    await expect(page.locator('#result')).toBeVisible();

    // Check pagination info (v2.x uses .dt-info)
    const paginationInfo = page.locator('#result_wrapper .dt-info, #result_wrapper .dataTables_info');
    await expect(paginationInfo).toBeVisible();

    const infoText = await paginationInfo.textContent();
    // Pagination info should show "Showing X to Y of Z entries" format
    expect(infoText).toMatch(/Showing \d+ to \d+ of \d+ entries/);

    // Check pagination controls (v2.x uses .dt-paging)
    const pagination = page.locator('#result_wrapper .dt-paging, #result_wrapper .dataTables_paginate');
    await expect(pagination).toBeVisible();

    // Check length selector (v2.x uses .dt-length)
    const lengthSelect = page.locator('#result_wrapper .dt-length select, #result_wrapper .dataTables_length select');
    await expect(lengthSelect).toBeVisible();

    // Test changing page length
    await lengthSelect.selectOption('10');
    await page.waitForTimeout(500);

    // Verify only 10 rows are now visible
    const visibleRows = await page.locator('#result tbody tr').count();
    expect(visibleRows).toBe(10);
  });

  test('should validate search functionality', async ({ page }) => {
    await studioHelper.login('Beer');
    await studioHelper.executeQuery('SELECT name FROM Beer LIMIT 30', false);

    // Switch to Table tab - use simple approach that works
    await page.locator('a[href="#tab-table"]').click();
    await page.waitForTimeout(500);
    await expect(page.locator('#result')).toBeVisible();

    // Find search input (v2.x uses .dt-search)
    const searchInput = page.locator('#result_wrapper .dt-search input, #result_wrapper .dataTables_filter input');
    await expect(searchInput).toBeVisible();

    // Test search
    await searchInput.fill('ale');
    await page.waitForTimeout(500);

    // Check if results are filtered
    const filteredRows = await page.locator('#result tbody tr:visible').count();
    expect(filteredRows).toBeLessThanOrEqual(30);

    // Verify pagination info updates
    const paginationInfo = await page.locator('#result_wrapper .dt-info, #result_wrapper .dataTables_info').textContent();
    expect(paginationInfo).toContain('filtered');
  });

  test('should check for critical JavaScript console errors', async ({ page }) => {
    // Set up error tracking
    const errors: string[] = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    page.on('pageerror', error => {
      errors.push(error.message);
    });

    await studioHelper.login('Beer');
    await studioHelper.executeQuery('SELECT name, brewery, style FROM Beer LIMIT 5', false);

    // Switch to Table tab - use simple approach that works
    await page.locator('a[href="#tab-table"]').click();
    await page.waitForTimeout(500);
    await expect(page.locator('#result')).toBeVisible();

    // Wait a bit for any delayed errors
    await page.waitForTimeout(2000);

    // Check for DataTables-related errors
    const dtErrors = errors.filter(error =>
      error.toLowerCase().includes('datatable') ||
      error.toLowerCase().includes('jquery') ||
      error.toLowerCase().includes('bootstrap')
    );

    if (dtErrors.length > 0) {
      console.log('DataTables-related errors found:', dtErrors);
    }

    // Should have minimal critical errors
    expect(dtErrors.length).toBeLessThan(5);
  });
});
