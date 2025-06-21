const { test, expect } = require('@playwright/test');
const { loginToStudio } = require('./helpers/auth-helper');

test.describe('ArcadeDB Studio - UI Components Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await loginToStudio(page);
  });

  test('should display main layout components', async ({ page }) => {
    // Check for header/navigation
    await expect(page.locator('header, nav, .header, .navbar')).toBeVisible();

    // Check for main content area
    await expect(page.locator('main, .main-content, .content, #content')).toBeVisible();

    // Check for sidebar if exists
    const sidebarExists = await page.locator('.sidebar, .side-nav, aside').count() > 0;
    if (sidebarExists) {
      await expect(page.locator('.sidebar, .side-nav, aside')).toBeVisible();
    }
  });

  test('should load CSS styles properly', async ({ page }) => {
    // Check if Bootstrap is loaded (common in ArcadeDB Studio)
    const bodyElement = page.locator('body');
    await expect(bodyElement).toHaveCSS('margin', '0px');

    // Check for custom studio styles
    const studioStylesLoaded = await page.evaluate(() => {
      const stylesheets = Array.from(document.styleSheets);
      return stylesheets.some(sheet =>
        sheet.href && (sheet.href.includes('studio') || sheet.href.includes('bootstrap'))
      );
    });
    expect(studioStylesLoaded).toBe(true);
  });

  test('should handle dark/light theme toggle if available', async ({ page }) => {
    // Look for theme toggle button
    const themeToggle = page.locator('.theme-toggle, .dark-mode-toggle, [data-theme-toggle]');
    const toggleExists = await themeToggle.count() > 0;

    if (toggleExists) {
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Wait for theme change
      await page.waitForTimeout(500);

      // Check if body class or data attribute changed
      const bodyHasThemeClass = await page.evaluate(() => {
        return document.body.classList.contains('dark-theme') ||
               document.body.classList.contains('light-theme') ||
               document.body.hasAttribute('data-theme');
      });
      expect(bodyHasThemeClass).toBe(true);
    }
  });

  test('should display loading indicators properly', async ({ page }) => {
    // Navigate to a page that might show loading
    await page.click('text=Database');

    // Check for loading indicators
    const loadingIndicators = page.locator('.loading, .spinner, .loader, .progress');
    const hasLoadingIndicator = await loadingIndicators.count() > 0;

    if (hasLoadingIndicator) {
      // If loading indicator exists, it should eventually disappear
      await expect(loadingIndicators.first()).toBeHidden({ timeout: 10000 });
    }
  });

  test('should handle error messages appropriately', async ({ page }) => {
    // Try to trigger an error by accessing invalid endpoint
    await page.goto('/invalid-page', { waitUntil: 'domcontentloaded' });

    // Check for error messages or 404 page
    const errorElements = page.locator('.error, .alert-danger, .error-message, .not-found');
    const hasError = await errorElements.count() > 0;

    if (hasError) {
      await expect(errorElements.first()).toBeVisible();
    }
  });

  test('should display tooltips and help text', async ({ page }) => {
    await page.goto('/query');

    // Look for help icons or tooltips
    const helpElements = page.locator('[title], [data-tooltip], [data-toggle="tooltip"], .help-icon');
    const hasHelpElements = await helpElements.count() > 0;

    if (hasHelpElements) {
      await expect(helpElements.first()).toBeVisible();

      // Try to hover and see if tooltip appears
      await helpElements.first().hover();
      await page.waitForTimeout(500);
    }
  });

  test('should handle modal dialogs', async ({ page }) => {
    // Look for buttons that might open modals
    const modalTriggers = page.locator('[data-toggle="modal"], [data-bs-toggle="modal"], .modal-trigger');
    const hasModalTriggers = await modalTriggers.count() > 0;

    if (hasModalTriggers) {
      await modalTriggers.first().click();

      // Check if modal appears
      const modal = page.locator('.modal, .dialog, .popup');
      await expect(modal).toBeVisible();

      // Check for modal close button
      const closeButton = modal.locator('.close, .modal-close, [data-dismiss="modal"]');
      if (await closeButton.count() > 0) {
        await closeButton.click();
        await expect(modal).toBeHidden();
      }
    }
  });

  test('should display data tables properly', async ({ page }) => {
    await page.goto('/database');

    // Look for data tables
    const tables = page.locator('table, .table, .datatable');
    const hasTable = await tables.count() > 0;

    if (hasTable) {
      await expect(tables.first()).toBeVisible();

      // Check for table headers
      const headers = tables.first().locator('th, .table-header');
      if (await headers.count() > 0) {
        await expect(headers.first()).toBeVisible();
      }

      // Check for table rows
      const rows = tables.first().locator('tr, .table-row');
      if (await rows.count() > 1) {
        await expect(rows.nth(1)).toBeVisible();
      }
    }
  });
});
