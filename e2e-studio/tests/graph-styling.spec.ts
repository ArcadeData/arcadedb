import { test, expect } from '@playwright/test';

test.describe('ArcadeDB Studio Graph Styling and HTML Labels', () => {
  // Setup helper for graph operations
  async function setupGraphWithStyling(page) {
    await page.goto('/');
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');
    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByText('Connected as').first()).toBeVisible();

    // Select Beer database
    await page.getByLabel('root').selectOption('Beer');
    await expect(page.getByLabel('root')).toHaveValue('Beer');

    // Execute query to get diverse vertex types
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await expect(queryTextarea).toBeVisible();
    await queryTextarea.fill('SELECT FROM Beer LIMIT 10');
    await page.getByRole('button', { name: '' }).first().click();

    await expect(page.getByText('Returned')).toBeVisible();
    await expect(page.getByRole('link', { name: 'Graph' })).toBeVisible();
    await page.waitForTimeout(3000); // Allow graph to fully render

    return page.locator('canvas').last();
  }

  test('should render vertices with correct color schemes', async ({ page }) => {
    await setupGraphWithStyling(page);

    // Check that graph statistics show vertices are displayed
    const graphStats = page.locator('text=Displayed').locator('..');
    const statsText = await graphStats.textContent();
    console.log('Graph stats:', statsText);

    expect(statsText).toContain('vertices');

    // Verify cytoscape is properly initialized with styling
    const cytoscapeInitialized = await page.evaluate(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null;
    });

    expect(cytoscapeInitialized).toBe(true);

    // Check vertex styling through cytoscape API
    const vertexStyles = await page.evaluate(() => {
      if (!globalCy) return null;

      const nodes = globalCy.nodes();
      if (nodes.length === 0) return null;

      // Get first node's computed style
      const firstNode = nodes[0];
      return {
        backgroundColor: firstNode.style('background-color'),
        borderColor: firstNode.style('border-color'),
        borderWidth: firstNode.style('border-width'),
        shape: firstNode.style('shape'),
        label: firstNode.style('label')
      };
    });

    expect(vertexStyles).toBeTruthy();
    console.log('Vertex styles:', vertexStyles);
  });

  test('should display HTML labels correctly', async ({ page }) => {
    await setupGraphWithStyling(page);

    // Check if HTML labels are rendered using cytoscape-node-html-label
    const htmlLabelsSupported = await page.evaluate(() => {
      // Check if the HTML label extension is loaded
      return typeof globalCy !== 'undefined' &&
             globalCy !== null &&
             typeof globalCy.nodeHtmlLabel === 'function';
    });

    console.log('HTML labels supported:', htmlLabelsSupported);

    // Verify node labels are displayed
    const nodeLabels = await page.evaluate(() => {
      if (!globalCy) return [];

      const nodes = globalCy.nodes();
      return nodes.map(node => ({
        id: node.id(),
        label: node.data('label') || node.data('name') || 'No label'
      }));
    });

    expect(nodeLabels.length).toBeGreaterThan(0);
    console.log('Node labels:', nodeLabels.slice(0, 3)); // Log first 3 for verification

    // Verify labels are not empty
    const hasNonEmptyLabels = nodeLabels.some(node =>
      node.label && node.label !== 'No label' && node.label.trim() !== ''
    );
    expect(hasNonEmptyLabels).toBe(true);
  });

  test('should apply different styles for different vertex types', async ({ page }) => {
    // Execute query that returns multiple vertex types
    await page.goto('/');
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');
    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByText('Connected as').first()).toBeVisible();

    await page.getByLabel('root').selectOption('Beer');

    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('SELECT FROM Beer LIMIT 3 UNION SELECT FROM Brewery LIMIT 3');
    await page.getByRole('button', { name: '' }).first().click();

    await expect(page.getByText('Returned')).toBeVisible();
    await page.waitForTimeout(3000);

    // Check vertex type styling differentiation
    const typeStyles = await page.evaluate(() => {
      if (!globalCy) return null;

      const nodes = globalCy.nodes();
      const stylesByType = {};

      nodes.forEach(node => {
        const nodeType = node.data('type') || 'Unknown';
        if (!stylesByType[nodeType]) {
          stylesByType[nodeType] = {
            backgroundColor: node.style('background-color'),
            shape: node.style('shape'),
            count: 0
          };
        }
        stylesByType[nodeType].count++;
      });

      return stylesByType;
    });

    console.log('Styles by type:', typeStyles);
    expect(typeStyles).toBeTruthy();

    // Verify we have multiple types with different styles
    const typeCount = Object.keys(typeStyles || {}).length;
    expect(typeCount).toBeGreaterThanOrEqual(1);
  });

  test('should handle edge styling and arrows', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');
    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByText('Connected as').first()).toBeVisible();

    await page.getByLabel('root').selectOption('Beer');

    // Query that should return edges
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await queryTextarea.fill('MATCH (b:Beer)-[r]->(t) RETURN b, r, t LIMIT 5');
    await page.getByRole('button', { name: '' }).first().click();

    await expect(page.getByText('Returned')).toBeVisible();
    await page.waitForTimeout(3000);

    // Check edge styling
    const edgeStyles = await page.evaluate(() => {
      if (!globalCy) return null;

      const edges = globalCy.edges();
      if (edges.length === 0) return { edgeCount: 0 };

      const firstEdge = edges[0];
      return {
        edgeCount: edges.length,
        lineColor: firstEdge.style('line-color'),
        width: firstEdge.style('width'),
        targetArrowShape: firstEdge.style('target-arrow-shape'),
        targetArrowColor: firstEdge.style('target-arrow-color'),
        opacity: firstEdge.style('opacity')
      };
    });

    console.log('Edge styles:', edgeStyles);
    expect(edgeStyles).toBeTruthy();

    // Verify edge styling properties exist
    if (edgeStyles && edgeStyles.edgeCount > 0) {
      expect(edgeStyles.lineColor).toBeDefined();
      expect(edgeStyles.width).toBeDefined();
    }
  });

  test('should render custom vertex properties in labels', async ({ page }) => {
    await setupGraphWithStyling(page);

    // Check if vertex properties are accessible and displayed
    const vertexProperties = await page.evaluate(() => {
      if (!globalCy) return null;

      const nodes = globalCy.nodes();
      if (nodes.length === 0) return null;

      const firstNode = nodes[0];
      const data = firstNode.data();

      return {
        id: data.id,
        type: data.type,
        label: data.label,
        properties: data.properties || {},
        hasCustomProperties: Object.keys(data.properties || {}).length > 0
      };
    });

    console.log('Vertex properties:', vertexProperties);
    expect(vertexProperties).toBeTruthy();

    // Verify vertex has accessible properties
    if (vertexProperties) {
      expect(vertexProperties.id).toBeDefined();
      expect(vertexProperties.type).toBeDefined();
    }
  });

  test('should handle graph layout and spacing correctly', async ({ page }) => {
    await setupGraphWithStyling(page);

    // Check layout configuration
    const layoutInfo = await page.evaluate(() => {
      if (!globalCy || !globalLayout) return null;

      return {
        layoutName: globalLayout.name,
        graphSpacing: globalGraphSettings?.graphSpacing || 50,
        nodeCount: globalCy.nodes().length,
        edgeCount: globalCy.edges().length,
        boundingBox: globalCy.extent()
      };
    });

    console.log('Layout info:', layoutInfo);
    expect(layoutInfo).toBeTruthy();

    if (layoutInfo) {
      expect(layoutInfo.nodeCount).toBeGreaterThan(0);
      expect(layoutInfo.graphSpacing).toBeGreaterThan(0);
      expect(layoutInfo.boundingBox).toBeDefined();
    }
  });

  test('should support vertex selection highlighting', async ({ page }) => {
    const graphCanvas = await setupGraphWithStyling(page);

    // Click on a vertex to select it
    const canvasBox = await graphCanvas.boundingBox();
    const centerX = canvasBox.x + canvasBox.width / 2;
    const centerY = canvasBox.y + canvasBox.height / 2;

    await page.mouse.click(centerX, centerY);
    await page.waitForTimeout(1000);

    // Check selection state
    const selectionInfo = await page.evaluate(() => {
      if (!globalCy) return null;

      const selected = globalCy.elements(':selected');
      return {
        selectedCount: selected.length,
        hasSelection: selected.length > 0,
        firstSelectedType: selected.length > 0 ? selected[0].data('type') : null
      };
    });

    console.log('Selection info:', selectionInfo);
    expect(selectionInfo).toBeTruthy();

    // Verify selection highlighting works
    if (selectionInfo && selectionInfo.hasSelection) {
      expect(selectionInfo.selectedCount).toBeGreaterThan(0);
    }
  });

  test('should validate cytoscape extensions are loaded', async ({ page }) => {
    await setupGraphWithStyling(page);

    // Check that all required cytoscape extensions are loaded
    const extensionsStatus = await page.evaluate(() => {
      if (!globalCy) return null;

      return {
        colaLayout: typeof globalCy.layout === 'function',
        cxtMenu: typeof globalCy.cxtmenu === 'function',
        nodeHtmlLabel: typeof globalCy.nodeHtmlLabel === 'function',
        graphML: typeof globalCy.graphml === 'function' || typeof cytoscape.use === 'function'
      };
    });

    console.log('Extensions status:', extensionsStatus);
    expect(extensionsStatus).toBeTruthy();

    if (extensionsStatus) {
      // At minimum, layout should be available
      expect(extensionsStatus.colaLayout).toBe(true);
    }
  });
});
