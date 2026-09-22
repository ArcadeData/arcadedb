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
 * Regression test for issue #6985: choosing "All" entries per page in the query result table showed every
 * record on one page, but the paging control underneath still drew one button per record - 500 rows
 * produced "<<, <, 1, 2, 3, 4, 5, ..., 500, >, >>" instead of "<<, <, 1, >, >>".
 *
 * The defect was in the bundled DataTables 3.0.0 ("When the page length was set to -1, the paging control
 * wasn't correctly updated", fixed upstream in 3.0.1), not in studio-table.js, which passes -1 through
 * unchanged. Studio already ships a fixed release, so this spec pins the behavior: a DataTables bump that
 * regresses the paging control for page length -1 fails here instead of in a user's browser.
 *
 * Both entry points into page length -1 are covered: picking "All" from the length menu on a rendered
 * table, and re-rendering the table (a new query) with the persisted "table.pageLength" = -1, which
 * initialises DataTables with pageLength -1 directly.
 */

import { test, expect, Page } from '@playwright/test';
import { ArcadeStudioTestHelper } from '../utils/test-utils';

const ROWS = 500;

async function showTableTab(page: Page): Promise<void> {
  await page.locator('a[href="#tab-table"]').click();
  await expect(page.locator('#result')).toBeVisible();
  await expect(page.locator('#result_wrapper .dt-paging')).toBeVisible();
}

// Reads the paging control as the user sees it: the numbered buttons, whether an ellipsis is drawn, and
// the page count DataTables itself reports.
async function readPaging(page: Page): Promise<{ numbers: string[]; ellipsis: number; pages: number; length: number }> {
  return page.evaluate(() => {
    const wrapper = document.querySelector('#result_wrapper .dt-paging') as HTMLElement;
    const labels = Array.from(wrapper.querySelectorAll('.page-link, button')).map((b) => (b.textContent || '').trim());
    const info = ($('#result') as any).DataTable().page.info();
    return {
      numbers: labels.filter((t) => /^\d+$/.test(t)),
      ellipsis: labels.filter((t) => t === '…' || t === '...').length,
      pages: info.pages,
      length: info.length,
    };
  });
}

async function expectSinglePage(page: Page): Promise<void> {
  await expect(page.locator('#result tbody tr')).toHaveCount(ROWS);

  const paging = await readPaging(page);
  expect(paging.length, 'the table must be showing every row').toBe(-1);
  expect(paging.pages, 'all rows on one page is one page').toBe(1);
  // Do not reduce this to the page.info() check above: page.info() and the paging control compute the page
  // count separately. When a fault made the pager treat -1 as one row per page, page.info().pages still said
  // 1 while the control drew "1, 2, 3, 4, 5, ..., 500". Only the drawn buttons show the bug the user saw.
  expect(paging.numbers, `paging buttons drawn for "All": ${JSON.stringify(paging)}`).toEqual(['1']);
  expect(paging.ellipsis).toBe(0);

  // With a single page, every navigation button is inert.
  for (const cls of ['first', 'previous', 'next', 'last']) {
    const button = page.locator(`#result_wrapper .dt-paging .${cls}`);
    if ((await button.count()) > 0)
      await expect(button.first(), `the "${cls}" button must be disabled on the only page`).toHaveAttribute('aria-disabled', 'true');
  }
}

test.describe('Result table paging with "All" entries per page (#6985)', () => {
  let studioHelper: ArcadeStudioTestHelper;

  test.beforeEach(async ({ page }) => {
    studioHelper = new ArcadeStudioTestHelper(page);
    await studioHelper.login('Beer');
    // Studio caps every result at the "Auto Limit" setting (default 20), even over an explicit LIMIT in the
    // query; raise it the way the Settings sidebar does, or the table never spans more than one page.
    await page.evaluate((limit) => (window as any).applyDefaultLimit(String(limit)), ROWS);
  });

  test('choosing "All" in the length menu draws a single page button', async ({ page }) => {
    await studioHelper.executeQuery(`SELECT name FROM Beer LIMIT ${ROWS}`, false);
    await showTableTab(page);

    // Sanity check that the fixture has enough rows to span many pages at the default length, so a
    // one-button control after "All" is the fix and not an artefact of a short result.
    const before = await readPaging(page);
    expect(before.pages).toBeGreaterThan(1);

    await page.locator('#result_wrapper .dt-length select').selectOption('-1');

    await expectSinglePage(page);
  });

  test('a table re-rendered with the persisted "All" length draws a single page button', async ({ page }) => {
    await studioHelper.executeQuery(`SELECT name FROM Beer LIMIT ${ROWS}`, false);
    await showTableTab(page);
    await page.locator('#result_wrapper .dt-length select').selectOption('-1');
    expect(await page.evaluate(() => localStorage.getItem('table.pageLength'))).toBe('-1');

    // A new query rebuilds the DataTable from scratch, initialised with pageLength -1 from storage.
    await studioHelper.executeQuery(`SELECT name, brewery FROM Beer LIMIT ${ROWS}`, false);
    await showTableTab(page);
    await expect(page.locator('#result thead th:has-text("brewery")')).toBeVisible();

    await expectSinglePage(page);
  });
});
