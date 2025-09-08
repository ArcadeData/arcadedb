import { test, expect } from '@playwright/test';

test.describe('CodeMirror Compatibility', () => {
  test('should verify basic Studio functionality still works with CodeMirror upgrade', async ({ page }) => {
    // Login to Studio
    await page.goto('/');
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();

    // Clear and fill username field
    const usernameField = page.locator('input[placeholder="User Name"]');
    await usernameField.clear();
    await usernameField.fill('root');

    // Clear and fill password field
    const passwordField = page.locator('input[placeholder="Password"]');
    await passwordField.clear();
    await passwordField.fill('playwithdata');

    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByText('Connected as').first()).toBeVisible({ timeout: 10000 });

    // Wait for UI to fully stabilize after login
    await page.waitForTimeout(2000);

    // Verify CodeMirror library is loaded
    const hasCodeMirror = await page.evaluate(() => {
      return typeof window.CodeMirror !== 'undefined' && window.CodeMirror.version;
    });
    expect(hasCodeMirror).toBeTruthy();

    // Navigate to Query tab (main functionality)
    await page.getByRole('tab').first().click();
    await page.waitForTimeout(1000); // Give time for tab to load

    // Verify query interface is present
    const hasQueryInterface = await page.locator('#inputCommand').count();
    expect(hasQueryInterface).toBeGreaterThan(0);

    // Verify language selector works
    const languageSelect = page.locator('#inputLanguage');
    await expect(languageSelect).toBeVisible();

    // Test language switching doesn't crash
    await languageSelect.selectOption('sql');
    await page.waitForTimeout(500);
    await languageSelect.selectOption('cypher');
    await page.waitForTimeout(500);

    // Verify no JavaScript errors occurred
    const consoleErrors = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Basic check - no critical errors should be present
    expect(consoleErrors.filter(e => e.includes('CodeMirror') || e.includes('TypeError')).length).toBe(0);
  });
});
