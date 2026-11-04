/*
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
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

// How every Studio chart is put on the page. The charts are drawn with Apache ECharts (Apache-2.0, the global `echarts`).
// Studio used ApexCharts until issue #10579: from 5.1.0 ApexCharts is no longer MIT, and its licence refuses embedding in a
// product other people use, which is what the Studio jar inside every ArcadeDB distribution is. Each panel builds its
// ECharts option in a pure function of its own (tested in studio/test/charts.test.js); this file only draws, resizes,
// re-themes and disposes.

var STUDIO_CHARTS = [];

/** True while Studio shows its dark theme. */
function studioChartIsDark() {
  return typeof document !== "undefined" && !!document.documentElement && document.documentElement.getAttribute("data-theme") === "dark";
}

/**
 * A number as an axis or a tooltip shows it: at most `decimals` decimals with the trailing zeros dropped, the text itself
 * for something that is not a number, and "" for no value.
 */
function studioChartNumber(value, decimals) {
  if (value === null || value === undefined || value === "") return "";
  var n = Number(value);
  return isNaN(n) ? String(value) : String(Number(n.toFixed(decimals)));
}

/**
 * Draws `option` into `element`, `height` pixels high, and returns the chart. `previous`, when it is the chart already
 * drawn into this element, is updated in place (a periodic refresh does not rebuild the chart); any other chart left on the
 * element is disposed first, so a panel that empties and refills its container never stacks two charts on one element.
 * Returns null when the element or the library is missing.
 */
function studioChartDraw(element, option, height, previous) {
  if (!element || typeof echarts === "undefined") return null;
  if (option.backgroundColor === undefined) option.backgroundColor = "transparent";
  if (option.tooltip && option.tooltip.className === undefined) option.tooltip.className = "studio-chart-tooltip";
  element.style.height = height + "px";
  if (previous && !previous.isDisposed() && previous.getDom() === element) {
    previous.setOption(option, true);
    previous.resize();
    return previous;
  }
  studioChartDispose(echarts.getInstanceByDom(element));
  var chart = echarts.init(element, studioChartIsDark() ? "dark" : null, { renderer: "svg" });
  chart.setOption(option);
  // ECharts does not follow its container: a chart drawn in a hidden tab has no size until the tab shows
  if (typeof ResizeObserver !== "undefined") {
    chart.studioResizeObserver = new ResizeObserver(function () {
      if (!chart.isDisposed()) chart.resize();
    });
    chart.studioResizeObserver.observe(element);
  }
  STUDIO_CHARTS.push(chart);
  return chart;
}

/** Disposes a chart drawn by studioChartDraw; null and an already disposed chart are ignored. */
function studioChartDispose(chart) {
  if (!chart) return;
  if (chart.studioResizeObserver) {
    chart.studioResizeObserver.disconnect();
    chart.studioResizeObserver = null;
  }
  var index = STUDIO_CHARTS.indexOf(chart);
  if (index >= 0) STUDIO_CHARTS.splice(index, 1);
  if (!chart.isDisposed()) chart.dispose();
}

/** Repaints every chart on the page in the current theme; a chart whose element has left the page is disposed instead. */
function studioChartsRetheme() {
  var dark = studioChartIsDark();
  var charts = STUDIO_CHARTS.slice();
  for (var i = 0; i < charts.length; i++) {
    var chart = charts[i];
    if (chart.isDisposed() || !document.body.contains(chart.getDom())) studioChartDispose(chart);
    else chart.setTheme(dark ? "dark" : {});
  }
}

if (typeof MutationObserver !== "undefined" && typeof document !== "undefined" && document.documentElement)
  new MutationObserver(studioChartsRetheme).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
