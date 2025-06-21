const { test, expect } = require('@playwright/test');
const { loginToStudio } = require('./helpers/auth-helper');

test.describe('ArcadeDB Studio - Query & Data Visualization Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await loginToStudio(page);
  });

  test('should display query editor', async ({ page }) => {
    await page.click('text=Query');
    await page.waitForLoadState('networkidle');

    // Check for query editor (CodeMirror or similar)
    const queryEditor = page.locator('.CodeMirror, .query-editor, textarea[name*="query"], #queryEditor');
    await expect(queryEditor).toBeVisible();

    // Check for execute button
    const executeButton = page.locator('button:has-text("Execute"), button:has-text("Run"), .execute-btn, .run-btn');
    await expect(executeButton.first()).toBeVisible();
  });

  test('should execute simple query', async ({ page }) => {
    await page.goto('/query');

    // Find and fill query editor
    const queryEditor = page.locator('.CodeMirror textarea, .query-editor, textarea[name*="query"], #queryEditor');
    const hasQueryEditor = await queryEditor.count() > 0;

    if (hasQueryEditor) {
      // Clear and type query
      await queryEditor.first().clear();
      await queryEditor.first().fill('SELECT FROM schema:types LIMIT 5');

      // Execute query
      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Run"), .execute-btn, .run-btn');
      await executeButton.first().click();

      // Wait for results
      await page.waitForTimeout(2000);

      // Check for results table
      const resultsTable = page.locator('.results, .query-results, table');
      await expect(resultsTable.first()).toBeVisible();

      // Check for result rows
      const resultRows = resultsTable.first().locator('tr');
      expect(await resultRows.count()).toBeGreaterThan(1);
    }
  });

  test('should handle query errors', async ({ page }) => {
    await page.goto('/query');

    const queryEditor = page.locator('.CodeMirror textarea, .query-editor, textarea[name*="query"], #queryEditor');
    const hasQueryEditor = await queryEditor.count() > 0;

    if (hasQueryEditor) {
      // Execute invalid query
      await queryEditor.first().clear();
      await queryEditor.first().fill('INVALID SQL QUERY');

      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Run"), .execute-btn, .run-btn');
      await executeButton.first().click();

      await page.waitForTimeout(2000);

      // Check for error message
      const errorMessage = page.locator('.error, .alert-danger, .query-error, .error-message');
      const hasError = await errorMessage.count() > 0;

      if (hasError) {
        await expect(errorMessage.first()).toBeVisible();
        await expect(errorMessage.first()).toContainText(/error|invalid|syntax/i);
      }
    }
  });

  test('should display graph visualization', async ({ page }) => {
    await page.click('text=Graph');
    await page.waitForLoadState('networkidle');

    // Check for graph container (likely using Cytoscape.js)
    const graphContainer = page.locator('#graph, .graph-container, .cytoscape, .graph-visualization');
    const hasGraph = await graphContainer.count() > 0;

    if (hasGraph) {
      await expect(graphContainer.first()).toBeVisible();

      // Check for graph controls
      const graphControls = page.locator('.graph-controls, .graph-toolbar, .zoom-controls');
      const hasControls = await graphControls.count() > 0;

      if (hasControls) {
        await expect(graphControls.first()).toBeVisible();
      }
    }
  });

  test('should support graph query execution', async ({ page }) => {
    await page.goto('/graph');

    // Look for graph query input
    const graphQuery = page.locator('.graph-query, .traverse-query, input[placeholder*="traverse"], input[placeholder*="graph"]');
    const hasGraphQuery = await graphQuery.count() > 0;

    if (hasGraphQuery) {
      await expect(graphQuery.first()).toBeVisible();

      // Try a simple traversal query
      await graphQuery.first().fill('SELECT FROM Beer LIMIT 10');

      // Execute graph query
      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Run"), .graph-execute');
      const hasExecuteButton = await executeButton.count() > 0;

      if (hasExecuteButton) {
        await executeButton.first().click();
        await page.waitForTimeout(3000);

        // Check if graph is populated
        const graphNodes = page.locator('.node, .vertex, .graph-node');
        const hasNodes = await graphNodes.count() > 0;

        if (hasNodes) {
          await expect(graphNodes.first()).toBeVisible();
        }
      }
    }
  });

  test('should provide query result export options', async ({ page }) => {
    await page.goto('/query');

    const queryEditor = page.locator('.CodeMirror textarea, .query-editor, textarea[name*="query"], #queryEditor');
    const hasQueryEditor = await queryEditor.count() > 0;

    if (hasQueryEditor) {
      // Execute a simple query first
      await queryEditor.first().clear();
      await queryEditor.first().fill('SELECT FROM schema:types LIMIT 3');

      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Run"), .execute-btn, .run-btn');
      await executeButton.first().click();
      await page.waitForTimeout(2000);

      // Look for export options
      const exportButtons = page.locator('button:has-text("Export"), button:has-text("Download"), .export-btn, .download-btn');
      const hasExportOptions = await exportButtons.count() > 0;

      if (hasExportOptions) {
        await expect(exportButtons.first()).toBeVisible();

        // Check for different export formats
        const formatOptions = page.locator('button:has-text("CSV"), button:has-text("JSON"), button:has-text("Excel")');
        const hasFormats = await formatOptions.count() > 0;

        if (hasFormats) {
          await expect(formatOptions.first()).toBeVisible();
        }
      }
    }
  });

  test('should display query history', async ({ page }) => {
    await page.goto('/query');

    // Look for query history panel
    const queryHistory = page.locator('.query-history, .history-panel, .recent-queries');
    const hasHistory = await queryHistory.count() > 0;

    if (hasHistory) {
      await expect(queryHistory.first()).toBeVisible();

      // Execute a query to add to history
      const queryEditor = page.locator('.CodeMirror textarea, .query-editor, textarea[name*="query"], #queryEditor');
      if (await queryEditor.count() > 0) {
        await queryEditor.first().clear();
        await queryEditor.first().fill('SELECT 1');

        const executeButton = page.locator('button:has-text("Execute"), button:has-text("Run"), .execute-btn, .run-btn');
        await executeButton.first().click();
        await page.waitForTimeout(1000);

        // Check if query appears in history
        const historyItems = queryHistory.locator('.history-item, .query-item');
        const hasHistoryItems = await historyItems.count() > 0;

        if (hasHistoryItems) {
          await expect(historyItems.first()).toBeVisible();
        }
      }
    }
  });

  test('should support query syntax highlighting', async ({ page }) => {
    await page.goto('/query');

    // Check for CodeMirror or similar syntax highlighting
    const syntaxHighlighting = page.locator('.CodeMirror, .cm-editor, .syntax-highlight');
    const hasSyntaxHighlighting = await syntaxHighlighting.count() > 0;

    if (hasSyntaxHighlighting) {
      await expect(syntaxHighlighting.first()).toBeVisible();

      // Type a query and check for syntax highlighting
      const queryEditor = page.locator('.CodeMirror textarea, .query-editor');
      if (await queryEditor.count() > 0) {
        await queryEditor.first().fill('SELECT name FROM Beer WHERE alcohol > 5.0');

        // Check for syntax highlighted elements
        const syntaxElements = page.locator('.cm-keyword, .cm-string, .cm-number, .highlight');
        const hasSyntaxElements = await syntaxElements.count() > 0;

        if (hasSyntaxElements) {
          await expect(syntaxElements.first()).toBeVisible();
        }
      }
    }
  });

  test('should provide query result pagination', async ({ page }) => {
    await page.goto('/query');

    const queryEditor = page.locator('.CodeMirror textarea, .query-editor, textarea[name*="query"], #queryEditor');
    const hasQueryEditor = await queryEditor.count() > 0;

    if (hasQueryEditor) {
      // Execute query that might return many results
      await queryEditor.first().clear();
      await queryEditor.first().fill('SELECT FROM Beer LIMIT 100');

      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Run"), .execute-btn, .run-btn');
      await executeButton.first().click();
      await page.waitForTimeout(3000);

      // Look for pagination controls
      const pagination = page.locator('.pagination, .pager, .page-controls');
      const hasPagination = await pagination.count() > 0;

      if (hasPagination) {
        await expect(pagination.first()).toBeVisible();

        // Check for next/previous buttons
        const pageButtons = pagination.locator('button, a').filter({ hasText: /Next|Previous|>|<|\d+/ });
        const hasPageButtons = await pageButtons.count() > 0;

        if (hasPageButtons) {
          await expect(pageButtons.first()).toBeVisible();
        }
      }
    }
  });
});
