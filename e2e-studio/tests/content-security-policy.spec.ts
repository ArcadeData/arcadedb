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

// Issue #9681: Studio is served with a Content-Security-Policy that refuses inline script. These tests drive Studio in a
// real browser and fail on any CSP violation the page itself causes - an inline on*="..." handler, an inline <script>
// or a javascript: URL left somewhere is a control that silently stops working - and check that injected markup
// cannot run script.

import { test, expect, Page } from '@playwright/test';

async function recordViolations(page: Page) {
  await page.addInitScript(() => {
    (window as any).__cspViolations = [];
    document.addEventListener('securitypolicyviolation', (e) => {
      (window as any).__cspViolations.push(e.violatedDirective + ' ' + e.blockedURI + ' ' + (e.sourceFile || '') + ':' + e.lineNumber);
    });
  });
}

async function takeViolations(page: Page): Promise<string[]> {
  return page.evaluate(() => (window as any).__cspViolations.splice(0));
}

async function login(page: Page) {
  await page.goto('/');
  await expect(page.locator('#loginPage')).toBeVisible();
  await page.fill('#inputUserName', 'root');
  await page.fill('#inputUserPassword', 'playwithdata');
  await page.click('.login-submit-btn');
  await expect(page.locator('#studioPanel')).toBeVisible({ timeout: 30000 });
}

test.describe('ArcadeDB Studio Content-Security-Policy', () => {
  test('Studio pages are served with a policy that refuses inline script', async ({ page }) => {
    const response = await page.goto('/');
    const policy = response!.headers()['content-security-policy'];
    expect(policy).toBeTruthy();
    expect(policy).toMatch(/script-src 'self'(;|$)/);
    expect(policy).not.toContain("'unsafe-eval'");
  });

  test('no Studio tab or control causes a policy violation', async ({ page }) => {
    await recordViolations(page);
    await login(page);
    expect(await takeViolations(page)).toEqual([]);

    // Query: run a query from the Run button and open a record from its RID link
    await page.click('#tab-query-sel');
    const dbSelect = page.locator('#queryDbSelectContainer');
    await dbSelect.locator('.db-select-toggle').click();
    await dbSelect.locator('.db-select-list li[data-db="Beer"]').click();
    await expect(dbSelect.locator('.db-name')).toHaveText('Beer');
    await page.evaluate(() => (window as any).editor.setValue('SELECT FROM Beer LIMIT 5'));
    await page.click('[data-testid="execute-query-button"]');
    await page.locator('a[href="#tab-table"]').click();
    await page.locator('#result a.link[data-on-click="openRecordEditorFromTable"]').first().click();
    await expect(page.locator('.record-editor-buttons')).toBeVisible();
    await page.locator('.record-editor-buttons button[data-on-click="cancelRecordEditor"]').click();

    // A menu item: every .dropdown-menu stops the clicks inside it on their way up, so this only works because the
    // actions are dispatched in the capture phase
    await page.locator('a[href="#tab-graph"]').click();
    await page.locator('#tab-graph a.dropdown-toggle', { hasText: 'Export' }).click();
    await page.locator('#tab-graph a[data-on-click="exportSettings"]').click();
    await expect(page.locator('#popupLabel')).toHaveText('Export Settings');
    await page.keyboard.press('Escape');
    await expect(page.locator('#popup')).toBeHidden();

    // Query sidebar panels, built in script with delegated handlers
    for (const panel of ['history', 'saved', 'reference', 'settings', 'overview'])
      await page.click(`.icon-sidebar-btn[data-panel="${panel}"]`);
    await page.click('.icon-sidebar-btn[data-panel="reference"]');
    const header = page.locator('button.reference-section-header').first();
    await header.click();
    await expect(header).toHaveAttribute('aria-expanded', 'true');

    for (const tab of ['database', 'server', 'profiler', 'security', 'cluster', 'api', 'resources', 'support', 'settings']) {
      await page.click(`#tab-${tab}-sel`);
      await page.waitForTimeout(500);
    }

    // API tab: an endpoint passes its own button, and Try It is built in script
    await page.click('#tab-api-sel');
    await page.click('button.api-endpoint[data-args^=\'["serverGet"\']');
    await expect(page.locator('button.api-endpoint.active')).toHaveCount(1);
    await page.click('.api-detail-tag-tryit');
    await expect(page.locator('.api-playground-send')).toBeVisible();

    // Settings: the theme switch
    await page.click('#tab-settings-sel');
    await page.click('.theme-option[data-theme-value="dark"]');
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
    await page.click('.theme-option[data-theme-value="light"]');

    expect(await takeViolations(page)).toEqual([]);
  });

  test('injected markup cannot run script', async ({ page }) => {
    await recordViolations(page);
    await login(page);

    await page.evaluate(() => {
      document.body.insertAdjacentHTML('beforeend', '<img src="data:," onerror="window.__injected = 1">');
      const script = document.createElement('script');
      script.textContent = 'window.__injected2 = 1';
      document.body.appendChild(script);
    });
    await page.waitForTimeout(500);

    expect(await page.evaluate(() => [(window as any).__injected, (window as any).__injected2])).toEqual([undefined, undefined]);
    expect((await takeViolations(page)).length).toBeGreaterThanOrEqual(2);
  });

  test('the Swagger UI page renders under its policy', async ({ browser, baseURL }) => {
    const context = await browser.newContext({ httpCredentials: { username: 'root', password: 'playwithdata' } });
    const page = await context.newPage();
    await recordViolations(page);
    const response = await page.goto(baseURL + '/api/v1/docs');
    expect(response!.headers()['content-security-policy']).toMatch(/script-src 'self' 'sha256-/);
    await expect(page.locator('.swagger-ui .info')).toBeVisible({ timeout: 15000 });
    expect(await takeViolations(page)).toEqual([]);
    await context.close();
  });
});
