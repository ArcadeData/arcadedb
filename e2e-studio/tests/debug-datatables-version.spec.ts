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
import { ArcadeStudioTestHelper } from '../utils';

test.describe('Debug DataTables Version', () => {
  test('should check actual DataTables version being loaded', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login();

    // Execute a query to initialize DataTables
    await helper.executeQuery('SELECT * FROM Beer LIMIT 5', false);
    await page.getByRole('link', { name: 'Table' }).click();

    // Wait for DataTable to be initialized
    await page.waitForSelector('#result');

    // The version of the global DataTable
    const dtVersion = await page.evaluate(() => {
      // @ts-ignore
      return window.DataTable ? window.DataTable.version : 'Not found';
    });

    // Where the library was loaded from
    const scriptSrc = await page.evaluate(() => {
      const scripts = Array.from(document.scripts);
      const dtScript = scripts.find(s => s.src && s.src.includes('dataTables'));
      return dtScript ? dtScript.src : 'Not found';
    });

    // The version the jQuery plugin entry point reports
    const apiVersion = await page.evaluate(() => {
      try {
        // @ts-ignore
        if (window.$ && window.$.fn && window.$.fn.dataTable) {
          // @ts-ignore
          return window.$.fn.dataTable.version || 'version property not found';
        }
        return 'DataTable not found on $.fn';
      } catch (e) {
        return `Error: ${e.message}`;
      }
    });

    // Whether the result table is an instance of it
    const tableInfo = await page.evaluate(() => {
      try {
        // @ts-ignore
        const table = window.$('#result').DataTable();
        return { hasTable: !!table, rows: table.rows().count() };
      } catch (e) {
        return { error: e.message };
      }
    });

    // A modern DataTables (v2 or newer) is loaded, from the vendored bundle, behind both of its entry points
    expect(dtVersion).toMatch(/^\d+\.\d+/);
    expect(parseInt(dtVersion.split('.')[0], 10)).toBeGreaterThanOrEqual(2);
    expect(scriptSrc).toContain('dataTables');
    expect(apiVersion).toBe(dtVersion);
    expect(tableInfo).toEqual({ hasTable: true, rows: 5 });
  });
});
