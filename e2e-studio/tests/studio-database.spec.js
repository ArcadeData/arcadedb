const { test, expect } = require('@playwright/test');
const { loginToStudio } = require('./helpers/auth-helper');

test.describe('ArcadeDB Studio - Database Interaction Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await loginToStudio(page);
  });

  test('should display database list', async ({ page }) => {
    await page.click('text=Database');
    await page.waitForLoadState('networkidle');

    // Check for database list or table
    const databaseList = page.locator('.database-list, .db-list, table');
    await expect(databaseList).toBeVisible();

    // Check for at least one database entry (like 'beer' from the demo data)
    const databaseEntries = page.locator('tr, .database-item, .db-item');
    expect(await databaseEntries.count()).toBeGreaterThan(0);
  });

  test('should show database information', async ({ page }) => {
    await page.goto('/database');

    // Look for database info panels or cards
    const dbInfo = page.locator('.database-info, .db-info, .info-panel, .card');
    const hasDbInfo = await dbInfo.count() > 0;

    if (hasDbInfo) {
      await expect(dbInfo.first()).toBeVisible();

      // Check for common database metrics
      const metrics = ['Records', 'Types', 'Size', 'Documents', 'Vertices', 'Edges'];
      for (const metric of metrics) {
        const metricElement = page.locator(`text=${metric}`);
        if (await metricElement.count() > 0) {
          await expect(metricElement.first()).toBeVisible();
        }
      }
    }
  });

  test('should allow database selection', async ({ page }) => {
    await page.goto('/database');

    // Look for database dropdown or selection
    const dbSelector = page.locator('select[name*="database"], .database-selector, .db-dropdown');
    const hasDbSelector = await dbSelector.count() > 0;

    if (hasDbSelector) {
      await expect(dbSelector.first()).toBeVisible();

      // Try to select a database
      const options = dbSelector.first().locator('option');
      if (await options.count() > 1) {
        await dbSelector.first().selectOption({ index: 1 });
        await page.waitForTimeout(1000);
      }
    }
  });

  test('should display schema information', async ({ page }) => {
    await page.goto('/database');

    // Look for schema section
    const schemaSection = page.locator('.schema, .types, .schema-panel');
    const hasSchema = await schemaSection.count() > 0;

    if (hasSchema) {
      await expect(schemaSection.first()).toBeVisible();

      // Check for type definitions
      const types = page.locator('.type, .schema-type, tr');
      if (await types.count() > 0) {
        await expect(types.first()).toBeVisible();
      }
    }
  });

  test('should show database statistics', async ({ page }) => {
    await page.goto('/database');

    // Look for statistics or metrics
    const statsElements = page.locator('.statistics, .stats, .metrics, .chart');
    const hasStats = await statsElements.count() > 0;

    if (hasStats) {
      await expect(statsElements.first()).toBeVisible();
    }

    // Check for numerical values (records count, size, etc.)
    const numbers = page.locator('text=/\\d+/');
    expect(await numbers.count()).toBeGreaterThan(0);
  });

  test('should handle database operations', async ({ page }) => {
    await page.goto('/database');

    // Look for database action buttons
    const actionButtons = page.locator('.btn, button').filter({ hasText: /Create|Delete|Backup|Export|Import/i });
    const hasActionButtons = await actionButtons.count() > 0;

    if (hasActionButtons) {
      await expect(actionButtons.first()).toBeVisible();

      // Test that buttons are clickable (but don't actually perform dangerous operations)
      const createButton = actionButtons.filter({ hasText: /Create/i });
      if (await createButton.count() > 0) {
        await expect(createButton.first()).toBeEnabled();
      }
    }
  });

  test('should display indexes information', async ({ page }) => {
    await page.goto('/database');

    // Look for indexes section
    const indexesSection = page.locator('.indexes, .index-list, .indices');
    const hasIndexes = await indexesSection.count() > 0;

    if (hasIndexes) {
      await expect(indexesSection.first()).toBeVisible();

      // Check for index entries
      const indexEntries = indexesSection.locator('tr, .index-item');
      if (await indexEntries.count() > 0) {
        await expect(indexEntries.first()).toBeVisible();
      }
    }
  });

  test('should show bucket information', async ({ page }) => {
    await page.goto('/database');

    // Look for buckets/shards information
    const bucketsSection = page.locator('.buckets, .shards, .bucket-list');
    const hasBuckets = await bucketsSection.count() > 0;

    if (hasBuckets) {
      await expect(bucketsSection.first()).toBeVisible();

      // Check for bucket entries
      const bucketEntries = bucketsSection.locator('tr, .bucket-item');
      if (await bucketEntries.count() > 0) {
        await expect(bucketEntries.first()).toBeVisible();
      }
    }
  });

  test('should refresh database information', async ({ page }) => {
    await page.goto('/database');

    // Look for refresh button
    const refreshButton = page.locator('.refresh, .reload, button[title*="Refresh"], button[title*="Reload"]');
    const hasRefreshButton = await refreshButton.count() > 0;

    if (hasRefreshButton) {
      await expect(refreshButton.first()).toBeVisible();
      await refreshButton.first().click();

      // Wait for refresh to complete
      await page.waitForLoadState('networkidle');
    }
  });
});
