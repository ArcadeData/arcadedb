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
 * Studio's record editor, driven in a real browser (issues #10082 and #10083): a Save that follows no edit writes nothing,
 * and the statement a Save produces does not depend on how many times the form was re-rendered by adding or removing a
 * property. The logic is pinned without a browser by studio/test/record-editor-save.test.js; this proves it holds with the
 * real jQuery and the real DOM, and that what reaches the database is what the user typed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

import { test, expect, APIRequestContext, Page } from '@playwright/test';
import { ArcadeStudioTestHelper, getTestCredentials } from '../utils';

const DATABASE = 'record-editor-e2e';
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

async function sql(request: APIRequestContext, command: string, params?: Record<string, unknown>): Promise<any[]> {
  const response = await request.post(`/api/v1/command/${DATABASE}`, { headers: authHeader(), data: { language: 'sql', command, params } });
  expect(response.ok(), `${command}: ${await response.text()}`).toBeTruthy();
  return (await response.json()).result;
}

// The stored record, as the database holds it: a STRING stays a string, a number a number
async function storedBeer(request: APIRequestContext): Promise<any> {
  return (await sql(request, "SELECT FROM Beer WHERE code = 'probe'"))[0];
}

// Runs the query, opens the only record in the editor through its @rid link in the table
async function openProbeInEditor(page: Page): Promise<void> {
  await page.locator('#inputLanguage').selectOption('sql');
  await page.evaluate(() => (window as any).editor.setValue("SELECT FROM Beer WHERE code = 'probe'"));
  await page.locator(RUN).click();
  await page.locator('a[href="#tab-table"]').click();
  await page.locator('#result tbody a.link').first().click();
  await expect(page.locator('#tableRecordEditorContent .record-editor-field[data-prop="name"]')).toBeVisible();
}

const field = (page: Page, prop: string) => page.locator(`#tableRecordEditorContent .record-editor-field[data-prop="${prop}"]`);

async function removeProperty(page: Page, prop: string): Promise<void> {
  await field(page, prop).locator('xpath=ancestor::div[contains(@class,"record-editor-prop-row")]')
    .locator('.record-editor-delete-prop').click();
}

async function addProperty(page: Page, name: string): Promise<void> {
  await page.locator('#tableRecordEditorContent [data-on-click="addPropertyToRecord"]').click();
  await page.locator('#newPropertyName').fill(name);
  await page.locator('#globalModalConfirmBtn').click();
  await expect(field(page, name)).toBeVisible();
}

test.describe('Studio record editor', () => {
  test.beforeEach(async ({ request }) => {
    await serverCommand(request, `drop database ${DATABASE}`, false);
    await serverCommand(request, `create database ${DATABASE}`);
    await sql(request, 'CREATE VERTEX TYPE Beer');
    // Schemaless on purpose: these text values look like a number, a boolean and JSON
    await sql(request, "INSERT INTO Beer SET code = 'probe', name = 'Lager', style = 'Pale', zip = '12345', active = 'true', "
      + "lead = '01234', pi = 3.14, nested = {'a': 1}");
    // Values a <textarea> rewrites on the way in: CRLF becomes LF and a line feed right after the start tag is dropped
    await sql(request, "UPDATE Beer SET crlf = :crlf, leading = :leading WHERE code = 'probe'",
      { crlf: 'line one\r\nline two', leading: '\nstarts with a line break' });
  });

  test.afterEach(async ({ request }) => {
    await serverCommand(request, `drop database ${DATABASE}`, false);
  });

  test('Save without any edit writes nothing and keeps every value as stored', async ({ page, request }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    const before = await storedBeer(request);
    await openProbeInEditor(page);

    await page.locator('#tableRecordEditorContent [data-on-click="saveRecordEditor"]').click();

    await expect(page.locator('#toastContainer')).toContainText('No changes to save');
    expect(await storedBeer(request)).toEqual(before);
    expect(before.crlf).toBe('line one\r\nline two');
    expect(typeof before.zip).toBe('string');
    expect(typeof before.active).toBe('string');
  });

  test('removing a property keeps the edit typed before it', async ({ page, request }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    await openProbeInEditor(page);

    await field(page, 'name').fill('Stout');
    await removeProperty(page, 'style');
    await expect(field(page, 'name')).toHaveValue('Stout');
    await page.locator('#tableRecordEditorContent [data-on-click="saveRecordEditor"]').click();

    await expect(page.locator('#toastContainer')).toContainText('Record updated');
    const beer = await storedBeer(request);
    expect(beer.name).toBe('Stout');
    expect(beer.style).toBeUndefined();
    // what was not touched is not retyped
    expect(beer.zip).toBe('12345');
    expect(beer.active).toBe('true');
    expect(beer.lead).toBe('01234');
  });

  test('adding a property keeps the edit typed before it', async ({ page, request }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    await openProbeInEditor(page);

    await field(page, 'name').fill('Stout');
    await addProperty(page, 'abv');
    await expect(field(page, 'name')).toHaveValue('Stout');
    await field(page, 'abv').fill('5.5');
    await page.locator('#tableRecordEditorContent [data-on-click="saveRecordEditor"]').click();

    await expect(page.locator('#toastContainer')).toContainText('Record updated');
    const beer = await storedBeer(request);
    expect(beer.name).toBe('Stout');
    expect(beer.abv).toBe(5.5);
    expect(beer.style).toBe('Pale');
  });

  test('an edited text property stays text', async ({ page, request }) => {
    const helper = new ArcadeStudioTestHelper(page);
    await helper.login(DATABASE);
    await openProbeInEditor(page);

    await field(page, 'zip').fill('12346');
    await field(page, 'lead').fill('01235');
    await page.locator('#tableRecordEditorContent [data-on-click="saveRecordEditor"]').click();

    await expect(page.locator('#toastContainer')).toContainText('Record updated');
    const beer = await storedBeer(request);
    expect(beer.zip).toBe('12346');
    expect(beer.lead).toBe('01235');
  });
});
