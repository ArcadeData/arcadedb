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

    // Check what DataTables version is actually loaded
    const dtVersion = await page.evaluate(() => {
      // @ts-ignore
      return window.DataTable ? window.DataTable.version : 'Not found';
    });

    console.log(`DataTables version in window: ${dtVersion}`);

    // Check if the file itself was loaded correctly
    const fileContent = await page.evaluate(() => {
      const scripts = Array.from(document.scripts);
      const dtScript = scripts.find(s => s.src && s.src.includes('dataTables'));
      return dtScript ? dtScript.src : 'Not found';
    });

    console.log(`DataTables script src: ${fileContent}`);

    // Get the actual version from the loaded DataTable API
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

    console.log(`DataTables API version: ${apiVersion}`);

    // Check if the DataTable was actually initialized on the result table
    const tableInfo = await page.evaluate(() => {
      try {
        // @ts-ignore
        const table = window.$('#result').DataTable();
        // @ts-ignore
        return {
          hasTable: !!table,
          version: table.version ? table.version : 'no version on instance'
        };
      } catch (e) {
        return { error: e.message };
      }
    });

    console.log(`Table instance info:`, tableInfo);
  });
});
