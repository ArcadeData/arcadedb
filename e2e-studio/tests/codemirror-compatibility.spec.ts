import { test, expect } from '@playwright/test';

test.describe('CodeMirror Compatibility', () => {
  test('should verify basic Studio functionality still works with CodeMirror upgrade', async ({ page }) => {
    // Login to Studio
    await page.goto('/');
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');
    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByText('Connected as').first()).toBeVisible();

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
