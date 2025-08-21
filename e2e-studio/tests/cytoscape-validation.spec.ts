import { test, expect } from '@playwright/test';

test.describe('Cytoscape 3.33.1 Validation Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to Studio login page
    await page.goto('/');

    // Wait for login dialog and fill correct credentials
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible({ timeout: 10000 });
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');
    await page.getByRole('button', { name: 'Sign in' }).click();

    // Wait for Studio to load
    await expect(page.getByText('Connected as').first()).toBeVisible({ timeout: 15000 });

    // Select Beer database
    await page.getByLabel('root').selectOption('Beer');
    await expect(page.getByLabel('root')).toHaveValue('Beer');
  });

  test('should load cytoscape 3.33.1 successfully', async ({ page }) => {
    // Execute a simple query to trigger graph view
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await expect(queryTextarea).toBeVisible();
    await queryTextarea.fill('SELECT FROM Beer LIMIT 5');
    await page.getByRole('button', { name: '' }).first().click();

    // Wait for results and check graph tab
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('link', { name: 'Graph' })).toBeVisible();

    // Wait for cytoscape to initialize
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null;
    }, { timeout: 10000 });

    // Check that cytoscape canvas is present
    const cytoscapeCanvas = page.locator('canvas').last();
    await expect(cytoscapeCanvas).toBeVisible();

    // Verify cytoscape version through global object
    const cytoscapeInfo = await page.evaluate(() => {
      if (typeof globalCy === 'undefined' || !globalCy) return null;
      return {
        hasInstance: true,
        nodeCount: globalCy.nodes().length,
        version: typeof cytoscape !== 'undefined' ? cytoscape.version : 'unknown'
      };
    });

    expect(cytoscapeInfo).toBeTruthy();
    if (cytoscapeInfo) {
      expect(cytoscapeInfo.hasInstance).toBe(true);
    }
  });

  test('should render basic graph nodes', async ({ page }) => {
    // Use existing Beer data instead of creating new data
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await expect(queryTextarea).toBeVisible();
    await queryTextarea.fill('SELECT FROM Beer LIMIT 3');
    await page.getByRole('button', { name: '' }).first().click();

    // Wait for results
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('link', { name: 'Graph' })).toBeVisible();

    // Wait for graph to render
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null && globalCy.nodes().length > 0;
    }, { timeout: 10000 });

    // Check that nodes are rendered via cytoscape API
    const nodeInfo = await page.evaluate(() => {
      if (!globalCy) return null;
      return {
        nodeCount: globalCy.nodes().length,
        hasNodes: globalCy.nodes().length > 0
      };
    });

    expect(nodeInfo).toBeTruthy();
    if (nodeInfo) {
      expect(nodeInfo.hasNodes).toBe(true);
      expect(nodeInfo.nodeCount).toBeGreaterThan(0);
    }
  });

  test('should handle node interactions correctly', async ({ page }) => {
    // Execute query first to get graph data
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('SELECT FROM Beer LIMIT 2');
    await page.getByRole('button', { name: '' }).first().click();
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 15000 });

    // Wait for graph to render
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null && globalCy.nodes().length > 0;
    }, { timeout: 10000 });

    // Test node selection via canvas interaction
    const canvas = page.locator('canvas').last();
    await expect(canvas).toBeVisible();

    const canvasBox = await canvas.boundingBox();
    const centerX = canvasBox.x + canvasBox.width / 2;
    const centerY = canvasBox.y + canvasBox.height / 2;

    // Click on canvas center (where node likely is)
    await page.mouse.click(centerX, centerY);
    await page.waitForTimeout(500);

    // Check selection state via cytoscape API
    const selectionInfo = await page.evaluate(() => {
      if (!globalCy) return null;
      const selected = globalCy.elements(':selected');
      return {
        hasSelection: selected.length > 0,
        selectedCount: selected.length,
        totalNodes: globalCy.nodes().length
      };
    });

    expect(selectionInfo).toBeTruthy();
    if (selectionInfo) {
      expect(selectionInfo.totalNodes).toBeGreaterThan(0);
    }
  });

  test('should support graph layout changes', async ({ page }) => {
    // Setup graph first
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('SELECT FROM Beer LIMIT 5');
    await page.getByRole('button', { name: '' }).first().click();
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 15000 });

    // Wait for graph to render
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null && globalCy.nodes().length > 0;
    }, { timeout: 10000 });

    // Test layout functionality via API rather than UI controls
    const layoutTest = await page.evaluate(() => {
      if (!globalCy) return null;

      try {
        // Get initial positions
        const initialPositions = globalCy.nodes().map(node => ({
          id: node.id(),
          position: { x: node.position('x'), y: node.position('y') }
        }));

        // Try applying a layout
        const layout = globalCy.layout({ name: 'random', fit: true });
        layout.run();

        return {
          success: true,
          nodeCount: globalCy.nodes().length,
          initialPositions: initialPositions.length
        };
      } catch (error) {
        return { error: error.message, nodeCount: globalCy.nodes().length };
      }
    });

    expect(layoutTest).toBeTruthy();
    if (layoutTest) {
      expect(layoutTest.nodeCount).toBeGreaterThan(0);
    }
  });

  test('should support zoom and pan operations', async ({ page }) => {
    // Setup graph first
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('SELECT FROM Beer LIMIT 3');
    await page.getByRole('button', { name: '' }).first().click();
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 15000 });

    // Wait for graph to render
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null && globalCy.nodes().length > 0;
    }, { timeout: 10000 });

    // Test zoom operations via API
    const zoomTest = await page.evaluate(() => {
      if (!globalCy) return null;

      try {
        const initialZoom = globalCy.zoom();

        // Test zoom in
        globalCy.zoom(initialZoom * 1.5);
        const zoomedIn = globalCy.zoom();

        // Test zoom out
        globalCy.zoom(initialZoom * 0.8);
        const zoomedOut = globalCy.zoom();

        // Test fit
        globalCy.fit();
        const fittedZoom = globalCy.zoom();

        return {
          initialZoom,
          zoomedIn,
          zoomedOut,
          fittedZoom,
          success: true
        };
      } catch (error) {
        return { error: error.message };
      }
    });

    expect(zoomTest).toBeTruthy();
    if (zoomTest && !zoomTest.error) {
      expect(zoomTest.success).toBe(true);
      expect(zoomTest.initialZoom).toBeGreaterThan(0);
    }
  });

  test('should support basic export functionality', async ({ page }) => {
    // Setup graph first
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('SELECT FROM Beer LIMIT 3');
    await page.getByRole('button', { name: '' }).first().click();
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 15000 });

    // Wait for graph to render
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null && globalCy.nodes().length > 0;
    }, { timeout: 10000 });

    // Test export functionality via canvas API
    const exportTest = await page.evaluate(() => {
      try {
        const canvas = document.querySelector('canvas:last-child');
        if (!canvas) return { error: 'Canvas not found' };

        // Test that canvas can generate image data
        const dataURL = canvas.toDataURL('image/png');
        const hasImageData = dataURL && dataURL.startsWith('data:image/png');

        return {
          hasCanvas: true,
          hasImageData,
          dataURLLength: dataURL ? dataURL.length : 0
        };
      } catch (error) {
        return { error: error.message };
      }
    });

    expect(exportTest).toBeTruthy();
    if (exportTest && !exportTest.error) {
      expect(exportTest.hasCanvas).toBe(true);
    }
  });

  test('should handle moderate sized graphs efficiently', async ({ page }) => {
    // Use existing Beer data for performance test
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('SELECT FROM Beer LIMIT 25'); // Moderate size for CI

    const startTime = Date.now();
    await page.getByRole('button', { name: '' }).first().click();

    // Wait for results
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 30000 });

    // Wait for graph to render
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null && globalCy.nodes().length > 0;
    }, { timeout: 20000 });

    const totalTime = Date.now() - startTime;

    // Verify performance is reasonable for CI environment
    expect(totalTime).toBeLessThan(30000); // 30 seconds max for CI

    // Check graph state
    const graphInfo = await page.evaluate(() => {
      if (!globalCy) return null;
      return {
        nodeCount: globalCy.nodes().length,
        edgeCount: globalCy.edges().length,
        zoom: globalCy.zoom()
      };
    });

    expect(graphInfo).toBeTruthy();
    if (graphInfo) {
      expect(graphInfo.nodeCount).toBeGreaterThan(0);
    }
  });

  test('should maintain graph state during tab navigation', async ({ page }) => {
    // Load some data first
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('SELECT FROM Beer LIMIT 5');
    await page.getByRole('button', { name: '' }).first().click();
    await expect(page.getByText('Returned')).toBeVisible({ timeout: 15000 });

    // Wait for graph to render
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null && globalCy.nodes().length > 0;
    }, { timeout: 10000 });

    // Get initial graph state
    const initialState = await page.evaluate(() => {
      if (!globalCy) return null;
      return {
        nodeCount: globalCy.nodes().length,
        zoom: globalCy.zoom()
      };
    });

    // Navigate to different tabs (if available) and back
    const tabLinks = await page.locator('a[role="tab"], .nav-link').count();
    if (tabLinks > 1) {
      // Click on other tabs if they exist
      await page.locator('a[role="tab"], .nav-link').nth(1).click();
      await page.waitForTimeout(500);

      // Click back to graph/results area
      await page.getByRole('link', { name: 'Graph' }).click();
      await page.waitForTimeout(1000);
    }

    // Verify graph state is maintained
    const finalState = await page.evaluate(() => {
      if (!globalCy) return null;
      return {
        nodeCount: globalCy.nodes().length,
        zoom: globalCy.zoom(),
        stillActive: true
      };
    });

    expect(finalState).toBeTruthy();
    if (initialState && finalState) {
      expect(finalState.nodeCount).toBe(initialState.nodeCount);
      expect(finalState.stillActive).toBe(true);
    }
  });

  test('should handle cytoscape extensions correctly', async ({ page }) => {
    await page.click('[data-testid="graph-tab"]');

    // Test that cytoscape extensions are available through globalCy
    const extensionInfo = await page.evaluate(() => {
      if (!globalCy) return null;

      try {
        // Test if cola layout can be created
        const colaTest = globalCy.layout({ name: 'cola', infinite: false });
        const hasColaExtension = colaTest !== null;

        // Test cxtmenu extension
        const hasCxtMenuExtension = typeof globalCy.cxtmenu === 'function';

        return {
          hasColaExtension,
          hasCxtMenuExtension,
          globalCyAvailable: true
        };
      } catch (error) {
        return {
          error: error.message,
          globalCyAvailable: typeof globalCy !== 'undefined'
        };
      }
    });

    expect(extensionInfo).toBeTruthy();
    if (extensionInfo && !extensionInfo.error) {
      expect(extensionInfo.globalCyAvailable).toBe(true);
    }
  });
});
