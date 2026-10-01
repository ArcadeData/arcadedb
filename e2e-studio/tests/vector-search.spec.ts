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
 * Issue #7312: Studio's Vector, Hybrid and Full-text query languages drive POST /api/v1/vector/{database}/search against a database with a dense
 * LSM_VECTOR index. The form must offer the index with the dimension count the server reports, refuse a query vector
 * of the wrong length without a round trip, and render the returned rows with their distance and the truncation flag.
 */

import { test, expect, APIRequestContext } from '@playwright/test';
import { ArcadeStudioTestHelper, getTestCredentials } from '../utils';

const DATABASE = 'vector-e2e';
const INDEX = 'Doc7312[embedding]';

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
  const response = await request.post(`/api/v1/command/${DATABASE}`, {
    headers: authHeader(),
    data: { language: 'sql', command }
  });
  expect(response.ok(), `${command}: ${await response.text()}`).toBeTruthy();
}

const RUN = '[data-testid="execute-query-button"]';

test.describe('Studio index search languages', () => {
  test.beforeAll(async ({ request }) => {
    await serverCommand(request, `drop database ${DATABASE}`, false);
    await serverCommand(request, `create database ${DATABASE}`);
    await sql(request, 'CREATE VERTEX TYPE Doc7312');
    await sql(request, 'CREATE PROPERTY Doc7312.title STRING');
    await sql(request, 'CREATE PROPERTY Doc7312.embedding ARRAY_OF_FLOATS');
    await sql(request, "CREATE INDEX ON Doc7312 (embedding) LSM_VECTOR METADATA { dimensions: 3, similarity: 'COSINE' }");
    await sql(request, 'CREATE INDEX ON Doc7312 (title) FULL_TEXT');
    const vectors = [[1, 0, 0], [0.9, 0.1, 0], [0, 1, 0], [0, 0, 1], [0.5, 0.5, 0.5]];
    for (let i = 0; i < vectors.length; i++)
      await sql(request, `INSERT INTO Doc7312 SET title = 'doc${i}', embedding = [${vectors[i].join(', ')}]`);
  });

  test.afterAll(async ({ request }) => {
    await serverCommand(request, `drop database ${DATABASE}`, false);
  });

  test('there is no Vector tab: the searches are languages of the Query panel', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    await expect(page.locator('#tab-vector-sel')).toHaveCount(0);
    await expect(page.locator('#inputLanguage option[value="vector"]')).toHaveCount(1);
    await expect(page.locator('#inputLanguage option[value="hybrid"]')).toHaveCount(1);
    await expect(page.locator('#inputLanguage option[value="fulltext"]')).toHaveCount(1);
    await page.locator('#inputLanguage').selectOption('vector');
    await expect(page.locator('#searchFormArea')).toBeVisible();
    await page.locator('#inputLanguage').selectOption('sql');
    await expect(page.locator('#searchFormArea')).toBeHidden();
  });

  test('catches a wrong-length vector, and renders distance and truncation in the Query result', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);

    await page.locator('#inputLanguage').selectOption('vector');
    await expect(page.locator(`#vecIndexName option[value="${INDEX}"]`)).toHaveCount(1, { timeout: 15000 });
    await page.locator('#vecIndexName').selectOption(INDEX);
    await expect(page.locator('#vecIndexHint')).toContainText('3 dimensions');

    // k takes its bounds from the server's OpenAPI document
    await expect(page.locator('#vecK')).toHaveAttribute('max', /^\d+$/);

    // A 2-element vector against a 3-dimension index is refused in the browser: no request reaches the endpoint
    let searchRequests = 0;
    page.on('request', (req) => {
      if (req.url().includes('/api/v1/vector/')) searchRequests++;
    });
    await page.locator('#vecQueryVector').fill('[1, 0]');
    await page.locator(RUN).click();
    await expect(page.locator('#vecError')).toContainText("has 2 dimensions, but index 'Doc7312[embedding]' has 3");
    expect(searchRequests).toBe(0);

    // A well-formed search: k=2 over 5 records fills the result window, so the truncation flag must be raised
    await page.locator('#vecQueryVector').fill('[1, 0, 0]');
    await page.locator('#vecK').fill('2');
    const responsePromise = page.waitForResponse((r) => r.url().includes(`/api/v1/vector/${DATABASE}/search`));
    await page.locator(RUN).click();
    const response = await responsePromise;
    expect(response.status()).toBe(200);
    expect((await response.json()).truncated).toBe(true);

    await expect(page.locator('#vecError')).toBeHidden();
    const rows = page.locator('#result tbody tr');
    await expect(rows).toHaveCount(2);
    await expect(page.locator('#result thead')).toContainText('distance');
    // the nearest neighbor of [1, 0, 0] is doc0 itself
    await expect(rows.nth(0)).toContainText('doc0');
    await expect(page.locator('#result-num')).toContainText('truncated');

    // Raising k past the number of records leaves the window short, so the flag goes away
    await page.locator('#vecK').fill('10');
    const secondResponse = page.waitForResponse((r) => r.url().includes(`/api/v1/vector/${DATABASE}/search`));
    await page.locator(RUN).click();
    expect((await (await secondResponse).json()).truncated).toBe(false);
    await expect(rows).toHaveCount(5);
    await expect(page.locator('#result-num')).not.toContainText('truncated');
  });

  test('the hybrid and full-text languages reach their endpoints and render what those return', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);

    // Hybrid: vector leg plus a full-text leg; the fusion strategies are offered from the OpenAPI document
    await page.locator('#inputLanguage').selectOption('hybrid');
    await expect(page.locator(`#vecIndexName option[value="${INDEX}"]`)).toHaveCount(1, { timeout: 15000 });
    await expect(page.locator('#vecFusionStrategy option[value="RRF"]')).toHaveCount(1);
    await page.locator('#vecIndexName').selectOption(INDEX);
    await page.locator('#vecQueryVector').fill('1, 0, 0');
    await page.locator('#vecK').fill('3');
    await page.locator('#vecFulltextIndexName').selectOption('Doc7312[title]');
    await page.locator('#vecFulltextQuery').fill('doc3');
    const hybrid = page.waitForResponse((r) => r.url().includes(`/api/v1/vector/${DATABASE}/hybrid`));
    await page.locator(RUN).click();
    expect((await hybrid).status()).toBe(200);
    await expect(page.locator('#vecError')).toBeHidden();
    await expect(page.locator('#result thead')).toContainText('sources');
    await expect(page.locator('#result tbody tr')).toHaveCount(3);

    // Full-text: scored hits
    await page.locator('#inputLanguage').selectOption('fulltext');
    await page.locator('#vecFtIndexName').selectOption('Doc7312[title]');
    await page.locator('#vecQueryText').fill('doc1');
    const fulltext = page.waitForResponse((r) => r.url().includes(`/api/v1/vector/${DATABASE}/fulltext`));
    await page.locator(RUN).click();
    expect((await fulltext).status()).toBe(200);
    const rows = page.locator('#result tbody tr');
    await expect(rows).toHaveCount(1);
    await expect(rows.first()).toContainText('doc1');
    await expect(page.locator('#result thead')).toContainText('score');
  });

  test('a search lands in the history and replays from it', async ({ page }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    await page.locator('#inputLanguage').selectOption('fulltext');
    await expect(page.locator('#vecFtIndexName option[value="Doc7312[title]"]')).toHaveCount(1, { timeout: 15000 });
    await page.locator('#vecFtIndexName').selectOption('Doc7312[title]');
    await page.locator('#vecQueryText').fill('doc2');
    await page.locator(RUN).click();
    await expect(page.locator('#result tbody tr')).toHaveCount(1);

    // Switching to SQL and back must not leak the form into the SQL editor, and the form must come back from history
    await page.locator('#inputLanguage').selectOption('sql');
    await expect(page.locator('#inputCommand')).toHaveValue('');
    await page.locator('#inputLanguage').selectOption('fulltext');
    await page.locator('#vecQueryText').fill('');
    await page.keyboard.press('Escape');
    await page.evaluate(() => (window as any).historyPrevious());
    await expect(page.locator('#vecQueryText')).toHaveValue('doc2');
  });
});
