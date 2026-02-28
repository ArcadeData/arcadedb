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

test.describe('ArcadeDB Studio Database Creation', () => {
  test('should create a new database called e2e-studio', async ({ page }) => {
    // Navigate to ArcadeDB Studio using dynamic baseURL
    await page.goto('/');

    // Wait for login page to appear
    await expect(page.locator('#loginPage')).toBeVisible();

    // Fill in login credentials using actual HTML IDs
    await page.fill('#inputUserName', 'root');
    await page.fill('#inputUserPassword', 'playwithdata');

    // Click sign in button
    await page.click('.login-submit-btn');

    // Wait for login to complete - check multiple conditions
    await Promise.all([
      expect(page.locator('#loginSpinner')).toBeHidden({ timeout: 30000 }),
      expect(page.locator('#studioPanel')).toBeVisible({ timeout: 30000 }),
      expect(page.locator('#loginPage')).toBeHidden({ timeout: 30000 })
    ]);

    // Verify username is populated in the query tab
    await expect(page.locator('#queryUser')).not.toBeEmpty();

    // Navigate to Database tab
    await page.locator('#tab-database-sel').click();

    // Wait for the database tab to load
    await page.waitForTimeout(1000);

    // Click the Create button to open database creation dialog
    await page.locator('button[onclick="createDatabase()"]').click();

    // Wait for the global modal to appear with the creation prompt
    await expect(page.locator('#globalModal')).toBeVisible();
    await expect(page.locator('#globalModalLabel')).toHaveText('Create a new database');

    // Enter the database name
    await page.fill('#inputCreateDatabaseName', 'e2e-studio');

    // Click Create to create the database
    await page.locator('#globalModalConfirmBtn').click();

    // Wait for the modal to close
    await expect(page.locator('#globalModal')).not.toBeVisible({ timeout: 10000 });

    // Verify the new database is selected in the searchable dropdown
    await expect(page.locator('#schemaDbSelectContainer .db-name')).toHaveText('e2e-studio');

    // Verify we can see the schema tab (default subtab in Database)
    await expect(page.locator('#tab-schema-sel')).toBeVisible();
  });
});
