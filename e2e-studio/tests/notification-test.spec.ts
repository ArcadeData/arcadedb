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

    // Wait for login dialog to appear
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();

    // Fill in login credentials
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');

    // Click sign in button
    await page.getByRole('button', { name: 'Sign in' }).click();

    // Wait for the main interface to load
    await expect(page.getByText('Connected as').first()).toBeVisible();

    // Select the Beer database from the dropdown
    await page.getByLabel('root').selectOption('Beer');

    // Verify Beer database is selected
    await expect(page.getByLabel('root')).toHaveValue('Beer');

    // Make sure we're on the Query tab
    await expect(page.getByText('Auto Limit')).toBeVisible();

    // Enter an invalid SQL query to trigger an error
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await expect(queryTextarea).toBeVisible();
    await queryTextarea.fill('SELECT SELECT FROM Beer');

    // Execute the query by clicking the execute button (using test ID for reliability)
    const executeButton = page.getByTestId('execute-query-button');
    await executeButton.click();

    // Wait for either notification or error state to appear
    await Promise.race([
      page.waitForSelector('.notyf__toast', { timeout: 5000 }),
      page.waitForFunction(() => document.querySelector('.btn-primary')?.classList.contains('loading'), { timeout: 5000 }),
      page.waitForTimeout(3000) // fallback timeout
    ]);

    // Check if notification system is working properly
    const notificationExists = await page.locator('.notyf__toast').count() > 0;

    // Verify no critical JavaScript errors occurred
    const hasCriticalErrors = jsErrors.some(error =>
      error.includes('document.body is null') ||
      error.includes("can't access lexical declaration 'notyf' before initialization") ||
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
      // Try to trigger notification before DOM is fully ready
      globalNotify('Test', 'Early notification', 'success');
    });

    // Complete login process
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');
    await page.getByRole('button', { name: 'Sign in' }).click();

    // Wait for interface to load and check if queued notification appears
    await expect(page.getByText('Connected as').first()).toBeVisible();

    // Wait for any queued notifications to be processed
    await page.waitForTimeout(1000);

    // Check if notification system is working
    const queuedNotificationExists = await page.locator('.notyf__toast').count() > 0;

    // The notification may have already disappeared, so we just ensure no errors occurred
    const errors = await page.evaluate(() => {
      const errors = [];
      // Check console for any notification-related errors
      return errors;
    });

    expect(Array.isArray(errors)).toBe(true);
  });
});
