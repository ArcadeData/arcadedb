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

import { test, expect, Page } from '@playwright/test';

// Studio's charts are drawn with Apache ECharts (Apache-2.0) since issue #10579: ApexCharts, the library before it, left MIT
// at 5.1.0 for a licence that refuses embedding in a product other people use. These tests drive the Server tab's
// Database Operations chart, which every server shows without any data loaded.

async function openServerTab(page: Page) {
  const responsePromise = page.waitForResponse(response => response.url().includes('api/v1/server'));
  await page.locator('#tab-server-sel').click();
  await responsePromise;
}

test.describe('Studio charts (Apache ECharts)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('#loginPage')).toBeVisible();
    await page.fill('#inputUserName', 'root');
    await page.fill('#inputUserPassword', 'playwithdata');
    await page.click('.login-submit-btn');
    await Promise.all([
      expect(page.locator('#loginSpinner')).toBeHidden({ timeout: 30000 }),
      expect(page.locator('#studioPanel')).toBeVisible({ timeout: 30000 }),
      expect(page.locator('#loginPage')).toBeHidden({ timeout: 30000 })
    ]);
  });

  test('ECharts is the chart library, and ApexCharts is not loaded at all', async ({ page }) => {
    const libraries = await page.evaluate(() => ({
      echarts: typeof (window as any).echarts?.init === 'function',
      apexcharts: typeof (window as any).ApexCharts
    }));
    expect(libraries).toEqual({ echarts: true, apexcharts: 'undefined' });
    expect(await page.locator('script[src*="apexcharts"], link[href*="apexcharts"]').count()).toBe(0);
  });

  test('the Server tab shows its summary cards and draws the Database Operations chart', async ({ page }) => {
    await openServerTab(page);

    await expect(page.locator('#summCpu')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summHeapUsed')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summHeapMax')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summRamUsed')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summRamTotal')).toBeVisible({ timeout: 10000 });
    // Databases Disk card - the directory (#7223) names the filesystem the two figures beside it describe, so it
    // has to reach the page and not only displayServerSummary()
    await expect(page.locator('#summDiskUsed')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summDiskTotal')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summDiskDir')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summCacheUsed')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summCacheMax')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#summOpsPerSec')).toBeVisible({ timeout: 10000 });

    await expect(page.locator('#serverChartCommands svg').first()).toBeVisible({ timeout: 10000 });
  });

  test('the chart is a real SVG drawing with one line per operation', async ({ page }) => {
    await openServerTab(page);

    const chartSvg = page.locator('#serverChartCommands svg').first();
    await expect(chartSvg).toBeVisible({ timeout: 10000 });
    expect(await page.locator('#serverChartCommands svg path').count()).toBeGreaterThan(0);
    const svgBox = await chartSvg.boundingBox();
    expect(svgBox?.width).toBeGreaterThan(100);
    expect(svgBox?.height).toBeGreaterThan(50);

    const series = await page.evaluate(() => {
      const chart = (window as any).echarts.getInstanceByDom(document.querySelector('#serverChartCommands'));
      return chart.getOption().series.map((s: any) => s.name);
    });
    expect(series).toEqual(['Queries', 'Write Tx', 'Read Tx', 'Tx Rollbacks', 'MVCC Contention']);
  });

  test('a refresh updates the chart in place instead of stacking another one', async ({ page }) => {
    await openServerTab(page);
    await expect(page.locator('#serverChartCommands svg').first()).toBeVisible({ timeout: 10000 });

    // each server refresh redraws the summary from the last reading; two more redraws stand for two more refreshes
    await page.evaluate(() => {
      (window as any).displayServerSummary();
      (window as any).displayServerSummary();
    });

    await expect(page.locator('#serverChartCommands svg')).toHaveCount(1);
    const charts = await page.evaluate(() => (window as any).STUDIO_CHARTS.filter((c: any) => c.getDom().id === 'serverChartCommands').length);
    expect(charts).toBe(1);
  });

  test('drawing the chart raises no JavaScript error', async ({ page }) => {
    const errors: string[] = [];
    page.on('console', msg => {
      if (msg.type() === 'error') errors.push(msg.text());
    });
    page.on('pageerror', error => errors.push(error.message));

    await openServerTab(page);
    await expect(page.locator('#serverChartCommands svg').first()).toBeVisible({ timeout: 10000 });

    const relevantErrors = errors.filter(error => /echarts|chart|svg/i.test(error));
    expect(relevantErrors).toHaveLength(0);
  });

  test('hovering the chart shows its tooltip', async ({ page }) => {
    await openServerTab(page);
    const chart = page.locator('#serverChartCommands');
    await expect(chart.locator('svg').first()).toBeVisible({ timeout: 10000 });

    await chart.hover();

    await expect(page.locator('.studio-chart-tooltip')).toBeVisible({ timeout: 5000 });
  });

  test('switching the theme repaints the chart without drawing a new one', async ({ page }) => {
    await openServerTab(page);
    await expect(page.locator('#serverChartCommands svg').first()).toBeVisible({ timeout: 10000 });
    const labelFill = () => page.evaluate(() => document.querySelector('#serverChartCommands svg text')?.getAttribute('fill') ?? '');
    const chartId = () => page.evaluate(() => (window as any).echarts.getInstanceByDom(document.querySelector('#serverChartCommands')).id);
    const before = { id: await chartId(), fill: await labelFill() };
    const other = await page.evaluate(() => document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark');

    await page.evaluate((theme) => document.documentElement.setAttribute('data-theme', theme), other);

    // the axis labels take the other theme's colour, on the same chart
    await expect.poll(labelFill).not.toBe(before.fill);
    expect(await chartId()).toBe(before.id);
  });
});
