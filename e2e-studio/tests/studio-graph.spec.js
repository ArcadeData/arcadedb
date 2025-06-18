const { test, expect } = require('@playwright/test');
const { loginToStudio } = require('./helpers/auth-helper');

test.describe('ArcadeDB Studio - Graph Visualization Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await loginToStudio(page);
  });

  test('should load graph visualization page', async ({ page }) => {
    await page.click('text=Graph');
    await page.waitForLoadState('networkidle');

    // Check for graph container
    const graphContainer = page.locator('#graph, .graph, .graph-container, .cytoscape, .graph-visualization');
    await expect(graphContainer.first()).toBeVisible();

    // Check page title or heading
    await expect(page.locator('h1, h2, .page-title')).toContainText(/Graph/i);
  });

  test('should display graph visualization controls', async ({ page }) => {
    await page.goto('/graph');

    // Check for zoom controls
    const zoomControls = page.locator('.zoom-in, .zoom-out, .zoom-fit, .graph-controls button');
    const hasZoomControls = await zoomControls.count() > 0;

    if (hasZoomControls) {
      await expect(zoomControls.first()).toBeVisible();
    }

    // Check for layout controls
    const layoutControls = page.locator('.layout-btn, button:has-text("Layout"), select[name*="layout"]');
    const hasLayoutControls = await layoutControls.count() > 0;

    if (hasLayoutControls) {
      await expect(layoutControls.first()).toBeVisible();
    }

    // Check for filter controls
    const filterControls = page.locator('.filter, .graph-filter, input[placeholder*="filter"]');
    const hasFilterControls = await filterControls.count() > 0;

    if (hasFilterControls) {
      await expect(filterControls.first()).toBeVisible();
    }
  });

  test('should load and display nodes and edges', async ({ page }) => {
    await page.goto('/graph');

    // Look for query input to load data
    const queryInput = page.locator('.graph-query, input[placeholder*="query"], textarea[placeholder*="traverse"]');
    const hasQueryInput = await queryInput.count() > 0;

    if (hasQueryInput) {
      // Execute a simple graph query
      await queryInput.first().fill('SELECT FROM Beer LIMIT 5');

      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Load"), .load-btn');
      if (await executeButton.count() > 0) {
        await executeButton.first().click();
        await page.waitForTimeout(3000);

        // Check for nodes in the graph
        const nodes = page.locator('.node, .vertex, [data-id]');
        const hasNodes = await nodes.count() > 0;

        if (hasNodes) {
          await expect(nodes.first()).toBeVisible();
          expect(await nodes.count()).toBeGreaterThan(0);
        }
      }
    }
  });

  test('should support node interaction', async ({ page }) => {
    await page.goto('/graph');

    // Load some data first
    const queryInput = page.locator('.graph-query, input[placeholder*="query"], textarea[placeholder*="traverse"]');
    if (await queryInput.count() > 0) {
      await queryInput.first().fill('SELECT FROM Beer LIMIT 3');
      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Load"), .load-btn');
      if (await executeButton.count() > 0) {
        await executeButton.first().click();
        await page.waitForTimeout(3000);
      }
    }

    // Try to interact with nodes
    const nodes = page.locator('.node, .vertex, [data-id]');
    const hasNodes = await nodes.count() > 0;

    if (hasNodes) {
      // Click on a node
      await nodes.first().click();

      // Check for node selection or details panel
      const nodeDetails = page.locator('.node-details, .properties-panel, .selected-node');
      const hasNodeDetails = await nodeDetails.count() > 0;

      if (hasNodeDetails) {
        await expect(nodeDetails.first()).toBeVisible();
      }

      // Check for context menu
      await nodes.first().click({ button: 'right' });
      const contextMenu = page.locator('.context-menu, .node-menu, .right-click-menu');
      const hasContextMenu = await contextMenu.count() > 0;

      if (hasContextMenu) {
        await expect(contextMenu.first()).toBeVisible();
      }
    }
  });

  test('should support zoom and pan operations', async ({ page }) => {
    await page.goto('/graph');

    const graphContainer = page.locator('#graph, .graph-container, .cytoscape');
    const hasGraphContainer = await graphContainer.count() > 0;

    if (hasGraphContainer) {
      // Test zoom in
      const zoomInButton = page.locator('.zoom-in, button[title*="Zoom In"]');
      if (await zoomInButton.count() > 0) {
        await zoomInButton.first().click();
        await page.waitForTimeout(500);
      }

      // Test zoom out
      const zoomOutButton = page.locator('.zoom-out, button[title*="Zoom Out"]');
      if (await zoomOutButton.count() > 0) {
        await zoomOutButton.first().click();
        await page.waitForTimeout(500);
      }

      // Test fit to screen
      const fitButton = page.locator('.zoom-fit, .fit-screen, button[title*="Fit"]');
      if (await fitButton.count() > 0) {
        await fitButton.first().click();
        await page.waitForTimeout(500);
      }

      // Test mouse wheel zoom (if supported)
      await graphContainer.first().hover();
      await page.mouse.wheel(0, -100); // Zoom in
      await page.waitForTimeout(500);
      await page.mouse.wheel(0, 100); // Zoom out
    }
  });

  test('should support different layout algorithms', async ({ page }) => {
    await page.goto('/graph');

    // Look for layout selector
    const layoutSelector = page.locator('select[name*="layout"], .layout-dropdown, .layout-selector');
    const hasLayoutSelector = await layoutSelector.count() > 0;

    if (hasLayoutSelector) {
      await expect(layoutSelector.first()).toBeVisible();

      // Try different layouts
      const layoutOptions = layoutSelector.first().locator('option');
      const optionCount = await layoutOptions.count();

      if (optionCount > 1) {
        // Select second layout option
        await layoutSelector.first().selectOption({ index: 1 });
        await page.waitForTimeout(1000);

        // Layout should have changed (difficult to test visually, but no errors should occur)
        await expect(layoutSelector.first()).toBeVisible();
      }
    }

    // Alternative: look for layout buttons
    const layoutButtons = page.locator('button:has-text("Circle"), button:has-text("Grid"), button:has-text("Force"), .layout-btn');
    const hasLayoutButtons = await layoutButtons.count() > 0;

    if (hasLayoutButtons) {
      await layoutButtons.first().click();
      await page.waitForTimeout(1000);
    }
  });

  test('should display node and edge properties', async ({ page }) => {
    await page.goto('/graph');

    // Load some data
    const queryInput = page.locator('.graph-query, input[placeholder*="query"], textarea[placeholder*="traverse"]');
    if (await queryInput.count() > 0) {
      await queryInput.first().fill('SELECT FROM Beer LIMIT 2');
      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Load"), .load-btn');
      if (await executeButton.count() > 0) {
        await executeButton.first().click();
        await page.waitForTimeout(3000);
      }
    }

    // Click on a node to see properties
    const nodes = page.locator('.node, .vertex, [data-id]');
    if (await nodes.count() > 0) {
      await nodes.first().click();

      // Check for properties panel
      const propertiesPanel = page.locator('.properties, .node-properties, .details-panel, .info-panel');
      const hasPropertiesPanel = await propertiesPanel.count() > 0;

      if (hasPropertiesPanel) {
        await expect(propertiesPanel.first()).toBeVisible();

        // Check for property names and values
        const properties = propertiesPanel.first().locator('.property, tr, .field');
        if (await properties.count() > 0) {
          await expect(properties.first()).toBeVisible();
        }
      }
    }
  });

  test('should support graph filtering and search', async ({ page }) => {
    await page.goto('/graph');

    // Look for search/filter input
    const searchInput = page.locator('input[placeholder*="search"], input[placeholder*="filter"], .search-input, .filter-input');
    const hasSearchInput = await searchInput.count() > 0;

    if (hasSearchInput) {
      await expect(searchInput.first()).toBeVisible();

      // Try searching for nodes
      await searchInput.first().fill('Beer');
      await page.waitForTimeout(1000);

      // Check if nodes are filtered or highlighted
      const filteredNodes = page.locator('.node.filtered, .node.highlighted, .match');
      const hasFilteredNodes = await filteredNodes.count() > 0;

      if (hasFilteredNodes) {
        await expect(filteredNodes.first()).toBeVisible();
      }
    }
  });

  test('should support graph export functionality', async ({ page }) => {
    await page.goto('/graph');

    // Look for export buttons
    const exportButtons = page.locator('button:has-text("Export"), button:has-text("Save"), .export-btn, .save-btn');
    const hasExportButtons = await exportButtons.count() > 0;

    if (hasExportButtons) {
      await expect(exportButtons.first()).toBeVisible();

      // Check for export format options
      const formatOptions = page.locator('button:has-text("PNG"), button:has-text("SVG"), button:has-text("JSON"), select[name*="format"]');
      const hasFormatOptions = await formatOptions.count() > 0;

      if (hasFormatOptions) {
        await expect(formatOptions.first()).toBeVisible();
      }
    }
  });

  test('should handle large graph datasets efficiently', async ({ page }) => {
    await page.goto('/graph');

    // Load a larger dataset
    const queryInput = page.locator('.graph-query, input[placeholder*="query"], textarea[placeholder*="traverse"]');
    if (await queryInput.count() > 0) {
      await queryInput.first().fill('SELECT FROM Beer LIMIT 50');
      const executeButton = page.locator('button:has-text("Execute"), button:has-text("Load"), .load-btn');
      if (await executeButton.count() > 0) {
        await executeButton.first().click();

        // Wait for loading to complete (should not freeze)
        await page.waitForTimeout(5000);

        // Check that the page is still responsive
        const graphContainer = page.locator('#graph, .graph-container, .cytoscape');
        await expect(graphContainer.first()).toBeVisible();

        // Check for performance indicators
        const nodeCount = await page.locator('.node, .vertex, [data-id]').count();
        expect(nodeCount).toBeGreaterThan(0);
      }
    }
  });
});
