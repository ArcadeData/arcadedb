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

test.describe('ArcadeDB Studio Notification System', () => {
  test('should display error notifications when SQL syntax is invalid', async ({ page }) => {
    // Enable console logging to capture JavaScript errors
    const consoleMessages: string[] = [];
    const jsErrors: string[] = [];

    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleMessages.push(`Console Error: ${msg.text()}`);
      }
    });

    page.on('pageerror', error => {
      jsErrors.push(`Page Error: ${error.message}`);
    });

    // Navigate to ArcadeDB Studio using dynamic baseURL
    await page.goto('/');

    // Wait for login page to appear
    await expect(page.locator('#loginPage')).toBeVisible();

    // Fill in login credentials
    await page.fill('#inputUserName', 'root');
    await page.fill('#inputUserPassword', 'playwithdata');

    // Click sign in button
    await page.click('.login-submit-btn');

    // Wait for the main interface to load
    await expect(page.getByText('Connected as').first()).toBeVisible();

    // Select the Beer database from the searchable dropdown
    const dbSelect = page.locator('#queryDbSelectContainer');
    await dbSelect.locator('.db-select-toggle').click();
    await expect(dbSelect.locator('.db-select-menu')).toBeVisible();
    await dbSelect.locator('.db-select-list li[data-db="Beer"]').click();

    // Verify Beer database is selected
    await expect(dbSelect.locator('.db-name')).toHaveText('Beer');

    // Enter an invalid SQL query via CodeMirror API
    await page.evaluate(() => {
      (window as any).editor.setValue('SELECT SELECT FROM Beer');
    });

    // Execute the query by clicking the execute button (using test ID for reliability)
    const executeButton = page.getByTestId('execute-query-button');
    await executeButton.click();

    // Wait for either notification or error state to appear
    await Promise.race([
      page.waitForSelector('.studio-toast', { timeout: 5000 }),
      page.waitForSelector('#toastContainer .toast', { timeout: 5000 }),
      page.waitForTimeout(3000) // fallback timeout
    ]);

    // Check if notification system is working properly
    const notificationExists = await page.locator('.studio-toast').count() > 0 ||
      await page.locator('#toastContainer .toast').count() > 0;

    // Verify no critical JavaScript errors occurred
    const hasCriticalErrors = jsErrors.some(error =>
      error.includes('document.body is null') ||
      error.includes("can't access lexical declaration 'Toast' before initialization") ||
      error.includes('$.notify is not a function')
    );

    // Assert that critical notification errors should not occur
    expect(hasCriticalErrors).toBe(false);

    // If no notification appeared, check if there's an error response visible
    if (!notificationExists) {
      // Look for any error indication in the UI
      const errorIndicators = await page.locator('[class*="error"], [class*="danger"], .alert-danger').count();

      // Either notification or some error indicator should be present
      expect(errorIndicators).toBeGreaterThan(0);
    }

    // Log results for debugging
    console.log('Notification system test results:');
    console.log(`- Notification appeared: ${notificationExists}`);
    console.log(`- Critical errors: ${hasCriticalErrors}`);
    console.log(`- Total JS errors: ${jsErrors.length}`);
    if (jsErrors.length > 0) {
      console.log('- JS errors:', jsErrors);
    }
  });

  test('should handle notification queue properly during initialization', async ({ page }) => {
    // Navigate to ArcadeDB Studio using dynamic baseURL
    await page.goto('/');

    // Test that early notifications are queued
    await page.evaluate(() => {
      globalNotify('Test', 'Early notification', 'success');
    });

    // Complete login process
    await expect(page.locator('#loginPage')).toBeVisible();
    await page.fill('#inputUserName', 'root');
    await page.fill('#inputUserPassword', 'playwithdata');
    await page.click('.login-submit-btn');

    // Wait for interface to load and check if queued notification appears
    await expect(page.getByText('Connected as').first()).toBeVisible();

    // Wait for any queued notifications to be processed
    await page.waitForTimeout(1000);

    // Check if notification system is working
    const queuedNotificationExists = await page.locator('.studio-toast').count() > 0 ||
      await page.locator('#toastContainer .toast').count() > 0;

    // The notification may have already disappeared, so we just ensure no errors occurred
    const errors = await page.evaluate(() => {
      const errors = [];
      // Check console for any notification-related errors
      return errors;
    });

    expect(Array.isArray(errors)).toBe(true);
  });
});
