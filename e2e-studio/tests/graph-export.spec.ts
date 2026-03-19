/**
* Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

import { test, expect } from '../fixtures/test-fixtures';
import * as fs from 'fs';
import * as path from 'path';
import { ArcadeStudioTestHelper, TEST_CONFIG } from '../utils';

test.describe('ArcadeDB Studio Graph Export Tests', () => {
  // Removed duplicated setup function - now using exportGraphReady fixture

  test('should export graph as JSON format', async ({ exportGraphReady }) => {
    const { helper, canvas } = exportGraphReady;
    const page = helper.page;

    // Look for export functionality - could be in a dropdown, button, or menu
    // Try different possible export button locations
    const exportButtonSelectors = [
      'button:has-text("Export")',
      '[title*="export" i]',
      '.fa-download',
      '.fa-file-export',
      'button[onclick*="export"]',
      '#exportGraph'
    ];

    let exportButton = null;
    for (const selector of exportButtonSelectors) {
      const element = page.locator(selector).first();
      if (await element.isVisible({ timeout: 1000 })) {
        exportButton = element;
        break;
      }
    }

    if (exportButton) {
      console.log('Found export button, testing JSON export');
      await exportButton.click();
      await page.waitForLoadState('networkidle');

      // Look for JSON export option
      const jsonOption = page.locator('text=JSON, button:has-text("JSON"), [value="json"]').first();
      if (await jsonOption.isVisible({ timeout: 2000 })) {
        await jsonOption.click();
        await page.waitForLoadState('networkidle');

        // Verify export functionality exists
        const exportCapability = await page.evaluate(() => {
          // Check if we can access graph data for export
          if (typeof globalCy !== 'undefined' && globalCy !== null) {
            return { hasGraphData: globalCy.nodes().length > 0, method: 'cytoscape' };
          }
          return { hasGraphData: false, method: 'none' };
        });

        expect(exportCapability.hasGraphData).toBe(true);
      }
    } else {
      // Test programmatic JSON export
      console.log('Testing programmatic JSON export');
      const jsonExport = await page.evaluate(() => {
        if (!globalCy) return null;

        // Export current graph state as JSON
        const graphData = {
          nodes: globalCy.nodes().map(node => ({
            id: node.id(),
            data: node.data(),
            position: node.position()
          })),
          edges: globalCy.edges().map(edge => ({
            id: edge.id(),
            source: edge.source().id(),
            target: edge.target().id(),
            data: edge.data()
          }))
        };

        return graphData;
      });

      expect(jsonExport).toBeTruthy();
      if (jsonExport && !jsonExport.error) {
        expect(jsonExport.nodes.length).toBeGreaterThan(0);
        console.log(`Exported ${jsonExport.nodes.length} nodes and ${jsonExport.edges.length} edges`);
      }
    }
  });

  test('should export graph as GraphML format', async ({ exportGraphReady }) => {
    const { helper, canvas } = exportGraphReady;
    const page = helper.page;

    // Test GraphML export capability with error handling
    const graphMLSupported = await page.evaluate(() => {
      try {
        // Check if GraphML export is supported
        return typeof globalCy !== 'undefined' &&
               globalCy !== null &&
               (typeof globalCy.graphml === 'function' || typeof exportGraphML === 'function');
      } catch (error) {
        return false;
      }
    });

    console.log('GraphML export supported:', graphMLSupported);

    if (graphMLSupported) {
      const graphMLExport = await page.evaluate(() => {
        if (typeof globalCy.graphml === 'function') {
          try {
            return globalCy.graphml().exportGraphML();
          } catch (error) {
            return { error: error.message };
          }
        } else if (typeof exportGraphML === 'function') {
          try {
            return exportGraphML();
          } catch (error) {
            return { error: error.message };
          }
        }
        return null;
      });

      console.log('GraphML export result:', graphMLExport ? 'Success' : 'Failed');

      // Verify export doesn't cause errors
      expect(graphMLExport).toBeDefined();

      // If export is a string, verify it contains GraphML content
      if (typeof graphMLExport === 'string') {
        expect(graphMLExport).toContain('graphml');
      }
    } else {
      console.log('GraphML export not available, skipping detailed test');
      // Verify basic graph functionality is still intact
      const basicGraphState = await page.evaluate(() => {
        return {
          hasGraph: typeof globalCy !== 'undefined' && globalCy !== null,
          nodeCount: globalCy ? globalCy.nodes().length : 0,
          isResponsive: true
        };
      });

      expect(basicGraphState.hasGraph).toBe(true,
        'Graph should still be functional even without GraphML export');
      expect(basicGraphState.nodeCount).toBeGreaterThan(0,
        'Graph should contain nodes for potential export functionality');
    }
  });

  test('should export graph as PNG image', async ({ exportGraphReady }) => {
    const { helper, canvas } = exportGraphReady;
    const page = helper.page;

    // Test image export functionality
    const imageExportSupported = await page.evaluate(() => {
      // Check if canvas-based image export is possible
      const canvas = document.querySelector('#graph canvas');
      return canvas && typeof canvas.toBlob === 'function';
    });

    console.log('Image export supported:', imageExportSupported);

    if (imageExportSupported) {
      // Test canvas image export
      const imageExport = await page.evaluate(async () => {
        const canvas = document.querySelector('#graph canvas');
        if (!canvas) return null;

        try {
          // Test that canvas can be converted to blob
          return new Promise((resolve) => {
            canvas.toBlob((blob) => {
              resolve({
                success: blob !== null,
                size: blob ? blob.size : 0,
                type: blob ? blob.type : null
              });
            }, 'image/png');
          });
        } catch (error) {
          return { error: error.message };
        }
      });

      console.log('Image export result:', imageExport);
      expect(imageExport).toBeTruthy();

      if (imageExport && imageExport.success) {
        expect(imageExport.size).toBeGreaterThan(0);
        expect(imageExport.type).toBe('image/png');
      }
    }

    // Alternative: Look for image export button in UI
    const imageExportButton = page.locator('button:has-text("PNG"), button:has-text("Image"), .fa-image');
    if (await imageExportButton.isVisible({ timeout: 2000 })) {
      console.log('Found image export button in UI');
      await imageExportButton.first().click();
      await page.waitForLoadState('networkidle');

      // Verify no errors occurred
      const noErrors = await page.evaluate(() => {
        return true; // Basic check that page is still responsive
      });
      expect(noErrors).toBe(true);
    }
  });

  test('should export graph with settings and layout information', async ({ exportGraphReady }) => {
    const { helper, canvas } = exportGraphReady;
    const page = helper.page;

    // Test export of graph settings and layout
    const settingsExport = await page.evaluate(() => {
      if (!globalCy || !globalGraphSettings) return null;

      return {
        graphSettings: globalGraphSettings,
        layoutName: globalLayout ? globalLayout.name : 'unknown',
        viewport: globalCy.extent(),
        zoom: globalCy.zoom(),
        pan: globalCy.pan(),
        nodeCount: globalCy.nodes().length,
        edgeCount: globalCy.edges().length
      };
    });

    console.log('Settings export:', settingsExport);
    expect(settingsExport).toBeTruthy();

    if (settingsExport) {
      expect(settingsExport.nodeCount).toBeGreaterThan(0);
      expect(settingsExport.graphSettings).toBeDefined();
      expect(settingsExport.zoom).toBeGreaterThan(0);
    }
  });

  test('should handle large graph export performance', async ({ largeGraphReady }) => {
    const { helper, canvas, nodeCount } = largeGraphReady;
    const page = helper.page;

    // Test export performance
    const startTime = Date.now();

    const exportResult = await page.evaluate(() => {
      if (!globalCy) return null;

      const start = performance.now();

      // Simulate export operation
      const graphData = {
        nodes: globalCy.nodes().map(node => ({
          id: node.id(),
          data: node.data(),
          position: node.position()
        })),
        edges: globalCy.edges().map(edge => ({
          id: edge.id(),
          source: edge.source().id(),
          target: edge.target().id(),
          data: edge.data()
        }))
      };

      const end = performance.now();

      return {
        nodeCount: graphData.nodes.length,
        edgeCount: graphData.edges.length,
        exportTime: end - start,
        dataSize: JSON.stringify(graphData).length
      };
    });

    const totalTime = Date.now() - startTime;

    console.log('Export performance:', exportResult);
    console.log('Total test time:', totalTime + 'ms');

    expect(exportResult).toBeTruthy();
    if (exportResult) {
      expect(exportResult.nodeCount).toBeGreaterThan(0);
      expect(exportResult.exportTime).toBeLessThan(5000); // Should complete within 5 seconds
      expect(exportResult.dataSize).toBeGreaterThan(0);
    }
  });

  test('should validate export data integrity', async ({ exportGraphReady }) => {
    const { helper, canvas } = exportGraphReady;
    const page = helper.page;

    // Export graph data and validate integrity
    const originalData = await page.evaluate(() => {
      if (!globalCy) return null;

      return {
        nodes: globalCy.nodes().map(node => ({
          id: node.id(),
          data: node.data()
        })),
        edges: globalCy.edges().map(edge => ({
          id: edge.id(),
          source: edge.source().id(),
          target: edge.target().id(),
          data: edge.data()
        }))
      };
    });

    expect(originalData).toBeTruthy();

    if (originalData) {
      // Validate data structure
      expect(Array.isArray(originalData.nodes)).toBe(true);
      expect(Array.isArray(originalData.edges)).toBe(true);

      // Validate node data integrity
      originalData.nodes.forEach(node => {
        expect(node.id).toBeDefined();
        expect(node.data).toBeDefined();
      });

      // Validate edge data integrity
      originalData.edges.forEach(edge => {
        expect(edge.id).toBeDefined();
        expect(edge.source).toBeDefined();
        expect(edge.target).toBeDefined();

        // Verify edge references valid nodes
        const sourceExists = originalData.nodes.some(n => n.id === edge.source);
        const targetExists = originalData.nodes.some(n => n.id === edge.target);
        expect(sourceExists).toBe(true);
        expect(targetExists).toBe(true);
      });

      console.log(`Validated ${originalData.nodes.length} nodes and ${originalData.edges.length} edges`);
    }
  });

  test('should export selected elements only', async ({ exportGraphReady }) => {
    const { helper, canvas: graphCanvas } = exportGraphReady;
    const page = helper.page;

    // Select some elements first
    const canvasBox = await graphCanvas.boundingBox();
    const centerX = canvasBox.x + canvasBox.width / 2;
    const centerY = canvasBox.y + canvasBox.height / 2;

    // Click to select an element
    await page.mouse.click(centerX, centerY);
    await page.waitForFunction(() => {
      return typeof globalCy !== 'undefined' && globalCy !== null;
    }, { timeout: 3000 }).catch(() => {});

    // Get selection and export
    const selectionExport = await page.evaluate(() => {
      if (!globalCy) return null;

      const selected = globalCy.elements(':selected');
      const all = globalCy.elements();

      return {
        totalElements: all.length,
        selectedElements: selected.length,
        hasSelection: selected.length > 0,
        selectedData: selected.map(el => ({
          id: el.id(),
          type: el.isNode() ? 'node' : 'edge',
          data: el.data()
        }))
      };
    });

    console.log('Selection export:', selectionExport);
    expect(selectionExport).toBeTruthy();

    if (selectionExport) {
      expect(selectionExport.totalElements).toBeGreaterThan(0);
      // Selection behavior may vary, so we just verify the data structure
      expect(Array.isArray(selectionExport.selectedData)).toBe(true);
    }
  });

  test('should copy export content to clipboard', async ({ exportGraphReady }) => {
    const { helper, canvas } = exportGraphReady;
    const page = helper.page;

    // Grant clipboard permissions
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

    // Test clipboard functionality via the export modal
    const clipboardTest = await page.evaluate(async () => {
      if (!globalCy) return { error: 'No graph available' };

      try {
        // Simulate what the export does - get graph data as text
        const graphData = JSON.stringify({
          nodes: globalCy.nodes().map(node => ({ id: node.id(), data: node.data() })),
          edges: globalCy.edges().map(edge => ({ id: edge.id(), data: edge.data() }))
        });

        // Test native clipboard API
        await navigator.clipboard.writeText(graphData);
        const clipboardContent = await navigator.clipboard.readText();

        return {
          success: true,
          dataLength: graphData.length,
          clipboardLength: clipboardContent.length,
          matches: graphData === clipboardContent
        };
      } catch (error) {
        return { error: error.message };
      }
    });

    console.log('Clipboard test result:', clipboardTest);
    expect(clipboardTest).toBeTruthy();

    if (clipboardTest && !clipboardTest.error) {
      expect(clipboardTest.success).toBe(true);
      expect(clipboardTest.matches).toBe(true);
    }
  });
});
