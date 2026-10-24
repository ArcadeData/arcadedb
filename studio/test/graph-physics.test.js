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

// The live physics of the graph view (js/studio-graph-physics.js): the force constants and what they do to a graph.
//
// The constants come from the Graph Settings, so they are a pure function of the settings and are pinned here. The
// behaviour is then checked against the real d3-force, with the same simulation shape the engine builds: a graph
// where two nodes start on top of each other (the repulsion has to push them apart), a linked pair (the spring has
// to hold them at the link distance) and a node far away from everything (the gravity has to bring it back).
//
//     node --test studio/test/graph-physics.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STUDIO = path.join(__dirname, "..");
const SRC_PATH = path.join(STUDIO, "src", "main", "resources", "static", "js", "studio-graph-physics.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-graph-physics.js: " + name);
  let i = src.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  return src.substring(start, i);
}

eval(extractFn("physicsForceParameters"));

/** The same simulation the engine builds in physicsStart(), with the given parameters and node positions. */
function buildSimulation(d3, parameters, nodes, links) {
  const simulation = d3
    .forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id))
    .force("charge", d3.forceManyBody())
    .force("collide", d3.forceCollide())
    .force("x", d3.forceX(0))
    .force("y", d3.forceY(0))
    .alpha(0.3);

  simulation.force("link").distance(parameters.linkDistance);
  simulation.force("charge").strength(parameters.chargeStrength).distanceMax(parameters.chargeDistanceMax);
  simulation.force("collide").radius(d => d.r + parameters.collidePadding);
  simulation.force("x").strength(parameters.gravityStrength);
  simulation.force("y").strength(parameters.gravityStrength);

  simulation.stop();
  return simulation;
}

function distance(a, b) {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

test("the default forces are the ones the graph is tuned for", () => {
  const p = physicsForceParameters({ graphSpacing: 50 });

  assert.equal(p.linkDistance, 150);
  assert.equal(p.chargeStrength, -400);
  assert.equal(p.gravityStrength, 0.05);
  assert.equal(p.collidePadding, 12.5);
});

test("the forces follow the settings", () => {
  const p = physicsForceParameters({ graphSpacing: 100, physicsRepulsion: 12, physicsGravity: 9 });

  assert.equal(p.linkDistance, 300);
  assert.equal(p.chargeStrength, -1200);
  assert.equal(p.gravityStrength, 0.09);
});

test("a gravity of zero is honoured, not turned back into the default", () => {
  const p = physicsForceParameters({ graphSpacing: 50, physicsGravity: 0 });

  assert.equal(p.gravityStrength, 0);
});

test("a repulsion of zero is honoured, not turned back into the default", () => {
  const p = physicsForceParameters({ graphSpacing: 50, physicsRepulsion: 0 });

  assert.ok(p.chargeStrength === 0 || Object.is(p.chargeStrength, -0), "chargeStrength is " + p.chargeStrength);
});

test("the repulsion is clamped to what the slider can produce", () => {
  const p = physicsForceParameters({ graphSpacing: 50, physicsRepulsion: 99 });

  assert.equal(p.chargeStrength, -20 * 50);
});

test("a corrupt setting cannot produce a NaN force", () => {
  const p = physicsForceParameters({ graphSpacing: "garbage" });

  assert.equal(p.linkDistance, 150);
  for (const value of Object.values(p)) assert.ok(!Number.isNaN(value), "no NaN in " + JSON.stringify(p));
});

test("the repulsion pushes two nodes apart", () => {
  const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));
  const p = physicsForceParameters({ graphSpacing: 50 });

  const nodes = [
    { id: "a", x: 0, y: 0, r: 12.5 },
    { id: "b", x: 10, y: 0, r: 12.5 },
  ];

  const simulation = buildSimulation(d3, p, nodes, []);
  simulation.tick(300);

  assert.ok(distance(nodes[0], nodes[1]) > 2 * p.collidePadding + 25, "the nodes ended " + distance(nodes[0], nodes[1]) + " apart");
});

test("the link spring holds an edge at the link distance", () => {
  const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));
  const p = physicsForceParameters({ graphSpacing: 50 });

  const nodes = [
    { id: "a", x: 0, y: 100, r: 12.5 },
    { id: "b", x: 0, y: 110, r: 12.5 },
  ];

  const simulation = buildSimulation(d3, p, nodes, [{ source: "a", target: "b" }]);
  simulation.tick(300);

  const d = distance(nodes[0], nodes[1]);
  assert.ok(d >= 0.5 * p.linkDistance && d <= 1.5 * p.linkDistance, "the edge ended " + d + " long");
});

test("the gravity brings a node that drifted away back to the centre", () => {
  const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));
  const p = physicsForceParameters({ graphSpacing: 50 });

  const nodes = [{ id: "a", x: 2000, y: 0, r: 12.5 }];

  const simulation = buildSimulation(d3, p, nodes, []);
  simulation.tick(300);

  assert.ok(Math.abs(nodes[0].x) < 2000, "the node ended at " + nodes[0].x);
});

test("without gravity a node that drifted away stays where it is", () => {
  const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));
  const p = physicsForceParameters({ graphSpacing: 50, physicsGravity: 0 });

  const nodes = [{ id: "a", x: 2000, y: 0, r: 12.5 }];

  const simulation = buildSimulation(d3, p, nodes, []);
  simulation.tick(300);

  assert.equal(nodes[0].x, 2000);
});
