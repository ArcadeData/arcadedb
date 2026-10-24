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
const JS_DIR = path.join(STUDIO, "src", "main", "resources", "static", "js");
const src = fs.readFileSync(path.join(JS_DIR, "studio-graph-physics.js"), "utf8");
const graphSrc = fs.readFileSync(path.join(JS_DIR, "studio-graph.js"), "utf8");

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

// The defaults live in the settings model (js/studio-graph.js), so the test reads them from there: the numbers it
// pins are the numbers the graph really starts with.
const constants = graphSrc.match(/var PHYSICS_[A-Z_]+ = [^;]+;/g);
assert.ok(constants && constants.length > 0, "studio-graph.js declares the physics constants");
eval(constants.join("\n"));

eval(extractFn("physicsForceParameters"));

/** The same simulation the engine builds in physicsStart(), with the given parameters and node positions. */
function buildSimulation(d3, parameters, nodes, links) {
  const xs = nodes.map((n) => n.x);
  const ys = nodes.map((n) => n.y);
  const centre = { x: (Math.min(...xs) + Math.max(...xs)) / 2, y: (Math.min(...ys) + Math.max(...ys)) / 2 };

  const simulation = d3
    .forceSimulation(nodes)
    .force("link", d3.forceLink(links).id((d) => d.id))
    .force("charge", d3.forceManyBody())
    .force("collide", d3.forceCollide())
    .force("x", d3.forceX(centre.x))
    .force("y", d3.forceY(centre.y))
    .alpha(0.3);

  simulation.force("link").distance(parameters.linkDistance);
  simulation.force("charge").strength(parameters.chargeStrength).distanceMax(parameters.chargeDistanceMax);
  simulation.force("collide").radius((d) => d.r + parameters.collidePadding);
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

  assert.equal(p.chargeStrength, -PHYSICS_MAX_LEVEL * 50);
});

test("a corrupt setting cannot produce a NaN force", () => {
  const p = physicsForceParameters({ graphSpacing: "garbage" });

  assert.equal(p.linkDistance, 150);
  for (const value of Object.values(p)) assert.ok(!Number.isNaN(value), "no NaN in " + JSON.stringify(p));
});

test("the simulation pushes the graph apart, holds the edges and brings the strays back", () => {
  const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));
  const p = physicsForceParameters({ graphSpacing: 50 });

  // Two nodes on top of each other, a linked pair and a node far away from everything
  const nodes = [
    { id: "a", x: 0, y: 0, r: 12.5 },
    { id: "b", x: 10, y: 0, r: 12.5 },
    { id: "c", x: 2000, y: 0, r: 12.5 },
    { id: "d", x: 0, y: 100, r: 12.5 },
    { id: "e", x: 0, y: 110, r: 12.5 },
  ];
  const links = [{ source: "d", target: "e" }];

  const simulation = buildSimulation(d3, p, nodes, links);
  simulation.tick(300);

  const apart = distance(nodes[0], nodes[1]);
  assert.ok(apart > 2 * p.collidePadding, "the nodes that started on top of each other ended " + apart + " apart");

  const edge = distance(nodes[3], nodes[4]);
  assert.ok(edge >= 0.5 * p.linkDistance && edge <= 1.5 * p.linkDistance, "the edge ended " + edge + " long");

  assert.ok(Math.abs(nodes[2].x) < 2000, "the node that drifted away ended at x=" + nodes[2].x);
});

test("without gravity a node that drifted away stays where it is", () => {
  const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));
  const p = physicsForceParameters({ graphSpacing: 50, physicsGravity: 0 });

  const nodes = [{ id: "c", x: 2000, y: 0, r: 12.5 }];

  const simulation = buildSimulation(d3, p, nodes, []);
  simulation.tick(300);

  assert.equal(nodes[0].x, 2000, "the node stayed where it was");
  assert.equal(nodes[0].y, 0);
});
