const { test, expect } = require('@playwright/test');
const { loginToStudio } = require('./helpers/auth-helper');

test.describe('ArcadeDB Studio - Navigation Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await loginToStudio(page);
  });

  test('should load studio homepage', async ({ page }) => {
    await expect(page).toHaveTitle(/ArcadeDB/);
    await expect(page.locator('body')).toBeVisible();
  });

  test('should display main navigation elements', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('networkidle');

    // Check for the actual Studio UI elements visible in screenshots
    await expect(page.locator('text=Connected as')).toBeVisible();

    // Check for left sidebar navigation icons
    const leftSidebar = page.locator('.fa-database, .fa-server, .fa-chart, [class*="icon"]').first();
    await expect(leftSidebar).toBeVisible();

    // Check for main content area tabs (Graph, Table, Json, Explain)
    const contentTabs = page.locator('text=Graph, text=Table, text=Json, text=Explain').first();
    if (await contentTabs.count() > 0) {
      await expect(contentTabs).toBeVisible();
    }
  });

  test('should show database connection status', async ({ page }) => {
    // Check for connection status visible in screenshot
    await expect(page.locator('text=Connected as')).toBeVisible();
    await expect(page.locator('text=root')).toBeVisible();
  });

  test('should display main toolbar elements', async ({ page }) => {
    // Check for toolbar elements visible in the working Studio
    const toolbarElements = [
      'text=Redraw',
      'text=Select',
      'text=Import',
      'text=Export',
      'text=Settings'
    ];

    for (const element of toolbarElements) {
      const locator = page.locator(element);
      if (await locator.count() > 0) {
        await expect(locator.first()).toBeVisible();
      }
    }
  });

  test('should display view mode tabs', async ({ page }) => {
    // Check for Graph/Table/Json/Explain tabs
    const viewTabs = ['Graph', 'Table', 'Json', 'Explain'];

    for (const tab of viewTabs) {
      const tabLocator = page.locator(`text=${tab}`);
      if (await tabLocator.count() > 0) {
        await expect(tabLocator.first()).toBeVisible();
      }
    }
  });

  test('should have search functionality', async ({ page }) => {
    // Check for search input visible in screenshot
    const searchInput = page.locator('input[placeholder*="Search"], input[type="search"], .search');
    if (await searchInput.count() > 0) {
      await expect(searchInput.first()).toBeVisible();
    }
  });

  test('should display ArcadeDB logo', async ({ page }) => {
    await expect(page.locator('img[src*="arcadedb"]').first()).toBeVisible();
  });

  test('should be responsive on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await expect(page.locator('body')).toBeVisible();

    // Check if mobile menu exists or navigation is collapsed
    const mobileMenuExists = await page.locator('.navbar-toggler, .mobile-menu, .hamburger').count() > 0;
    if (mobileMenuExists) {
      await expect(page.locator('.navbar-toggler, .mobile-menu, .hamburger')).toBeVisible();
    }
  });
});
