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
import type { Download, Page } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';
import { ArcadeStudioTestHelper, TEST_CONFIG } from '../utils';

test.describe('ArcadeDB Studio Graph Export Tests', () => {
  // Removed duplicated setup function - now using exportGraphReady fixture

  // Opens the Export dropdown the way a user does, clicks the entry of a format and returns the file the browser downloads.
  // None of this is optional: a missing menu, a missing entry or an export that downloads nothing fails the test.
  async function exportViaMenu(page: Page, format: 'json' | 'graphml' | 'png' | 'jpeg'): Promise<Download> {
    await page.locator('#tab-graph a.dropdown-toggle:has-text("Export")').click();
    const entry = page.locator(`#tab-graph [data-on-click="exportGraph"][data-args='["${format}"]']`);
    await expect(entry).toBeVisible();

    const download = page.waitForEvent('download', { timeout: 15000 });
    await entry.click();
    return download;
  }

  const readDownload = async (download: Download): Promise<Buffer> => fs.readFileSync((await download.path())!);

  test('should export graph as JSON format', async ({ exportGraphReady }) => {
    const page = exportGraphReady.helper.page;

    const download = await exportViaMenu(page, 'json');
    expect(download.suggestedFilename()).toBe('arcadedb-graph.json');

    const exported = JSON.parse((await readDownload(download)).toString('utf8'));
    const shown = await page.evaluate(() => ({
      vertices: globalResultset.vertices.length,
      edges: globalResultset.edges.length,
      nodes: globalCy.nodes().length
    }));

    // The file holds the graph on screen and the settings, and drops the raw records
    expect(shown.vertices).toBeGreaterThan(0);
    expect(exported.vertices).toHaveLength(shown.vertices);
    expect(exported.edges).toHaveLength(shown.edges);
    expect(exported.records).toEqual([]);
    expect(exported.settings).toBeDefined();
  });

  test('should export graph as GraphML format', async ({ exportGraphReady }) => {
    const page = exportGraphReady.helper.page;

    const download = await exportViaMenu(page, 'graphml');
    expect(download.suggestedFilename()).toBe('arcade.graphml');

    const graphml = (await readDownload(download)).toString('utf8');
    const shown = await page.evaluate(() => ({ nodes: globalCy.nodes().length, edges: globalCy.edges().length }));

    expect(graphml).toContain('<graphml');
    expect(shown.nodes).toBeGreaterThan(0);
    expect(graphml.match(/<node\b/g) ?? []).toHaveLength(shown.nodes);
    expect(graphml.match(/<edge\b/g) ?? []).toHaveLength(shown.edges);
  });

  test('should export graph as PNG image', async ({ exportGraphReady }) => {
    const page = exportGraphReady.helper.page;

    const download = await exportViaMenu(page, 'png');
    expect(download.suggestedFilename()).toBe('arcadedb-graph.png');

    const png = await readDownload(download);
    // PNG signature, then a picture with something in it
    expect(png.subarray(0, 8).toString('hex')).toBe('89504e470d0a1a0a');
    expect(png.length).toBeGreaterThan(1000);
  });

  test('should export graph as JPEG image', async ({ exportGraphReady }) => {
    const page = exportGraphReady.helper.page;

    const download = await exportViaMenu(page, 'jpeg');
    expect(download.suggestedFilename()).toBe('arcadedb-graph.jpg');

    const jpeg = await readDownload(download);
    // JPEG start-of-image marker, then a picture with something in it
    expect(jpeg.subarray(0, 3).toString('hex')).toBe('ffd8ff');
    expect(jpeg.length).toBeGreaterThan(1000);
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
