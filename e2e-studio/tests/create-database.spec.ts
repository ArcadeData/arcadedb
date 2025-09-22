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

    // Wait for login dialog to appear
    await expect(page.locator('#loginPopup')).toBeVisible();

    // Fill in login credentials using actual HTML IDs
    await page.fill('#inputUserName', 'root');
    await page.fill('#inputUserPassword', 'playwithdata');

    // Click sign in button using actual onclick handler
    await page.click('button[onclick="login()"]');

    // Wait for login spinner to appear (indicates login started)
    await expect(page.locator('#loginSpinner')).toBeVisible();

    // Wait for login to complete - check multiple conditions
    await Promise.all([
      expect(page.locator('#loginSpinner')).toBeHidden({ timeout: 30000 }),
      expect(page.locator('#studioPanel')).toBeVisible({ timeout: 30000 }),
      expect(page.locator('#loginPopup')).toBeHidden({ timeout: 30000 })
    ]);

    // Verify username is populated in the query tab
    await expect(page.locator('#queryUser')).not.toBeEmpty();

    // Navigate to Database tab (second tab with database icon)
    await page.getByRole('tab').nth(1).click();

    // Verify we're on the Database tab
    await expect(page.getByRole('heading', { name: 'Database' })).toBeVisible();

    // Click the Create button to open database creation dialog
    await page.getByRole('button', { name: 'Create' }).click();

    // Wait for the creation dialog to appear
    await expect(page.getByRole('dialog', { name: 'Create a new database' })).toBeVisible();

    // Enter the database name
    await page.getByRole('textbox', { name: 'Enter the database name:' }).fill('e2e-studio');

    // Click Send to create the database
    await page.getByRole('button', { name: 'Send' }).click();

    // Wait for the dialog to close
    await expect(page.getByRole('dialog', { name: 'Create a new database' })).not.toBeVisible();

    // Verify the new database is selected in the dropdown
    await expect(page.locator('select').first()).toContainText('e2e-studio');

    // Verify we can see the database schema section
    await expect(page.getByText('Database Schema')).toBeVisible();
  });
});
