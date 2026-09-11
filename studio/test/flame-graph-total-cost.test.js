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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

// Regression test for issue #7391. Since #7329 a step's "cost" is its SELF time and the subtree roll-up travels
// as "totalCost"; a container step such as FetchFromTypeExecutionStep times nothing itself and reports cost -1.
// The flame graph kept reading "cost" as the roll-up, so a plain type scan - whose only top-level step is the
// container - summed to zero and rendered "No measurable cost recorded." on a plan full of cost data, and a
// container's bar was drawn at the 1.5% minimum with its timed children squeezed inside it. Run with:
//
//     node --test studio/test/flame-graph-total-cost.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-database.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-database.js: " + name);
  let i = src.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  if (depth !== 0) throw new Error("unbalanced braces while extracting " + name + ": reached end of file");
  const source = src.substring(start, i);
  try {
    new Function("return (" + source + ")");
  } catch (e) {
    throw new Error("the extracted source of " + name + " does not parse: " + e.message);
  }
  return source;
}

// Stubs for the globals the flame graph reaches for.
var flameTips = [];
function escapeHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function formatCostNanos(n) {
  return n < 0 ? "n/a" : n + "ns";
}
function simplifyStepName(name) {
  return name;
}

eval(extractFn("flameTipAttr"));
eval(extractFn("stepsHaveCost"));
eval(extractFn("stepTotalCost"));
eval(extractFn("computeTotalCost"));
eval(extractFn("renderFlameRow"));

// The plan the server emits for `SELECT FROM Item` once profiled: one container with self cost -1 whose bucket
// children carry the time, and totalCost the roll-up on every node.
function typeScanPlan() {
  return [
    {
      name: "FetchFromTypeExecutionStep",
      cost: -1,
      totalCost: 3000,
      subSteps: [
        { name: "FetchFromBucketExecutionStep", cost: 1000, totalCost: 1000 },
        { name: "FetchFromBucketExecutionStep", cost: 2000, totalCost: 2000 },
      ],
    },
  ];
}

// Widths of the bars rendered at a given depth, in document order.
function widthsAtDepth(html, depth) {
  const re = new RegExp("<div class='flame-cell' style='width:([0-9.]+)%'><div class='flame-bar flame-depth-" + depth + "'", "g");
  const widths = [];
  let m;
  while ((m = re.exec(html)) !== null) widths.push(parseFloat(m[1]));
  return widths;
}

test("a plain type scan has cost data and a non-zero total", () => {
  const steps = typeScanPlan();
  assert.equal(stepsHaveCost(steps), true);
  assert.equal(computeTotalCost(steps), 3000);
});

test("the total is the roll-up of the top-level steps, never a double count of nested ones", () => {
  const steps = typeScanPlan();
  steps.push({ name: "ProjectionCalculationStep", cost: 500, totalCost: 500 });
  assert.equal(computeTotalCost(steps), 3500);
});

test("a plan without totalCost still sums the self costs of the whole tree", () => {
  // An older server, or a step serialized before the roll-up was added: fall back to walking the tree.
  const steps = [
    { name: "Container", cost: -1, subSteps: [{ name: "Leaf", cost: 700 }, { name: "Leaf", cost: 300 }] },
    { name: "Projection", cost: 200 },
  ];
  assert.equal(computeTotalCost(steps), 1200);
});

test("a container bar spans its children instead of collapsing to the minimum width", () => {
  const steps = typeScanPlan();
  const html = renderFlameRow(steps, computeTotalCost(steps), 1);

  assert.deepEqual(widthsAtDepth(html, 1), [100], "the container is the whole subtree");
  assert.deepEqual(widthsAtDepth(html, 2).map((w) => w.toFixed(3)), ["33.333", "66.667"], "each child is its share of the root");
});

test("the tooltip of a container names both its self cost and its subtree total", () => {
  const html = renderFlameRow(typeScanPlan(), 3000, 1);
  const containerTip = flameTips.find((tip) => tip.indexOf("FetchFromTypeExecutionStep") >= 0);
  assert.ok(containerTip, "the container must have a tooltip");
  assert.ok(containerTip.indexOf("3000ns") >= 0, "the subtree total must be shown: " + containerTip);
  assert.ok(containerTip.indexOf("self") >= 0, "the self cost must be labelled as such: " + containerTip);
});
