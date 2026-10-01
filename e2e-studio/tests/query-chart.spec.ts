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
 * The Query panel's Chart tab plots the rows of any result, and the Time Series management lives in the Database
 * panel (there is no separate TimeSeries tab any more).
 */

import { test, expect, APIRequestContext } from '@playwright/test';
import { ArcadeStudioTestHelper, getTestCredentials } from '../utils';

const DATABASE = 'chart-e2e';
const RUN = '[data-testid="execute-query-button"]';

function authHeader(): Record<string, string> {
  const { username, password } = getTestCredentials();
  return { Authorization: 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64') };
}

async function serverCommand(request: APIRequestContext, command: string, failOnError = true): Promise<void> {
  const response = await request.post('/api/v1/server', { headers: authHeader(), data: { command } });
  if (failOnError)
    expect(response.ok(), `${command}: ${await response.text()}`).toBeTruthy();
}

async function sql(request: APIRequestContext, command: string): Promise<void> {
  const response = await request.post(`/api/v1/command/${DATABASE}`, { headers: authHeader(), data: { language: 'sql', command } });
  expect(response.ok(), `${command}: ${await response.text()}`).toBeTruthy();
}

test.describe('Studio Chart tab', () => {
  test.beforeAll(async ({ request }) => {
    await serverCommand(request, `drop database ${DATABASE}`, false);
    await serverCommand(request, `create database ${DATABASE}`);
    await sql(request, 'CREATE TIMESERIES TYPE Cpu TIMESTAMP ts TAGS (host STRING) FIELDS (usage DOUBLE)');
    const now = Date.now();
    for (let i = 0; i < 10; i++)
      for (const host of ['a', 'b'])
        await sql(request, `INSERT INTO Cpu SET ts = ${now - (10 - i) * 60000}, host = '${host}', usage = ${50 + i}`);
    await sql(request, 'CREATE VERTEX TYPE Beer');
    await sql(request, "INSERT INTO Beer SET style = 'IPA', abv = 6.5");
    await sql(request, "INSERT INTO Beer SET style = 'IPA', abv = 7.1");
    await sql(request, "INSERT INTO Beer SET style = 'Stout', abv = 8.2");
  });

  test.afterAll(async ({ request }) => {
    await serverCommand(request, `drop database ${DATABASE}`, false);
  });

  test('a time series result is charted with one line per host', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    await expect(page.locator('#tab-timeseries-sel')).toHaveCount(0);

    await page.locator('#inputLanguage').selectOption('sql');
    await page.evaluate(() => (window as any).editor.setValue('select ts, host, usage from Cpu'));
    await page.locator(RUN).click();
    await expect(page.locator('#result tbody tr').first()).toBeVisible({ timeout: 15000 });

    await page.locator('#tab-chart-sel').click();
    await expect(page.locator('#chartX')).toHaveValue('ts');
    await expect(page.locator('#chartSplit')).toHaveValue('host');
    await expect(page.locator('#queryChart .apexcharts-line-series path').first()).toBeVisible();
    expect(await page.locator('#queryChart .apexcharts-line-series path').count()).toBe(2);
  });

  test('a grouped result is charted as categories, and PromQL opens the chart on its own', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);

    await page.evaluate(() => (window as any).editor.setValue('select style, count(*) as total from Beer group by style'));
    await page.locator(RUN).click();
    await expect(page.locator('#result tbody tr').first()).toBeVisible({ timeout: 15000 });
    await page.locator('#tab-chart-sel').click();
    await expect(page.locator('#chartX')).toHaveValue('style');
    await expect(page.locator('#chartYs input:checked')).toHaveValue('total');

    await page.locator('#inputLanguage').selectOption('promql');
    await expect(page.locator('#promqlControls')).toBeVisible();
    await page.evaluate(() => (window as any).editor.setValue('Cpu'));
    await page.locator(RUN).click();
    await expect(page.locator('#tab-chart-sel')).toHaveClass(/active/);
    await expect(page.locator('#chartSplit')).toHaveValue('metric');
  });

  test('the time series management is a Database sub-tab', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    await page.locator('#tab-database-sel').click();
    await page.locator('#tab-db-timeseries-sel').click();
    await expect(page.locator('#tsType option[value="Cpu"]')).toHaveCount(1, { timeout: 15000 });
  });
});
