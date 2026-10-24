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

// The live physics of the graph view, driven end to end: the real engine (js/studio-graph-physics.js) against a
// headless Cytoscape and the real d3-force, with the settings the Graph Settings panel writes.
//
// What is checked here is the wiring, not the constants (those are in graph-physics.test.js): that the simulation
// starts on the nodes the graph has, that it moves them, that a node added later joins it, that a node removed
// later leaves it, that a dragged node stays where it was dropped and a double-clicked one floats again, and that
// the toolbar switch stops and starts the whole thing.
//
//     node --test studio/test/graph-physics-engine.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STUDIO = path.join(__dirname, "..");
const SRC_DIR = path.join(STUDIO, "src", "main", "resources", "static", "js");
const PHYSICS_SRC = fs.readFileSync(path.join(SRC_DIR, "studio-graph-physics.js"), "utf8");

const cytoscape = require(path.join(STUDIO, "node_modules", "cytoscape"));
const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));

/** A jQuery stub: the engine only reads and writes a handful of controls through it. */
function jqueryStub(state) {
  const chain = {
    toggleClass: (name, on) => {
      if (name === "active") state.buttonActive = on;
      return chain;
    },
    attr: (name, value) => {
      if (name === "aria-pressed") state.buttonPressed = value;
      return chain;
    },
    prop: (name, value) => {
      if (name === "checked") state.checkboxChecked = value;
      return chain;
    },
    val: () => chain,
    text: () => chain,
  };
  return () => chain;
}

/**
 * A Studio page with the physics engine loaded: the same globals the widget and the settings panel use.
 *
 * The engine runs in this realm (not a vm context) on purpose: Cytoscape recognises a plain object by its
 * constructor, and an object made in another realm is not one, so the positions would silently be dropped.
 */
function buildPage(elements, settings) {
  const state = { buttonActive: null, buttonPressed: null, checkboxChecked: null, saved: null };

  const globals = {
    globalGraphSettings: Object.assign({ graphSpacing: 50, physics: true, physicsRepulsion: 8, physicsGravity: 5 }, settings),
    globalCy: null,
    saveGraphGlobalSettings: () => {
      state.saved = true;
    },
    $: jqueryStub(state),
    d3,
  };

  // The engine is a plain script that reads the page globals; `with` gives it exactly those, and the objects it
  // makes are the ones this realm made, which is what Cytoscape checks for.
  const factory = new Function(
    "__globals",
    "with (__globals) {" +
      PHYSICS_SRC +
      "\nreturn { physicsEnabled, physicsForceParameters, physicsStart, physicsStop, physicsTick, physicsSync," +
      " physicsScheduleSync, physicsAttach, physicsSetEnabled, physicsUpdateToggleButton, togglePhysics," +
      " get globalPhysics() { return globalPhysics; }," +
      " get globalPhysicsGrab() { return globalPhysicsGrab; } }; }"
  );

  const engine = factory(globals);
  globals.globalCy = cytoscape({
    headless: true,
    elements: elements,
    style: [{ selector: "node", style: { width: 10, height: 10 } }],
  });

  // The page the test drives: the engine's functions, and the globals it reads.
  const sandbox = Object.assign({}, engine, globals);
  Object.defineProperty(sandbox, "globalPhysics", {
    get: () => engine.globalPhysics,
    set: (v) => {
      engine.globalPhysics = v;
    },
  });

  return { sandbox, state };
}

/** Lets the simulation run: d3 ticks on a timer, so the test drives the ticks by hand. */
function settle(sandbox, ticks) {
  for (let i = 0; i < ticks; i++) sandbox.globalPhysics.sim.tick();
}

function positions(sandbox) {
  const out = {};
  sandbox.globalPhysics.nodes.forEach((n) => (out[n.id] = { x: n.x, y: n.y }));
  return out;
}

test("the simulation starts on the nodes of the graph and moves them", () => {
  const { sandbox } = buildPage([
    { data: { id: "a", size: 25 } },
    { data: { id: "b", size: 25 } },
    { data: { id: "c", size: 25 } },
  ]);

  sandbox.physicsStart();
  assert.ok(sandbox.globalPhysics != null, "the simulation started");
  assert.equal(sandbox.globalPhysics.nodes.length, 3);

  // Two nodes on top of each other: the repulsion has to separate them
  sandbox.globalCy.nodes()[1].position({ x: 0, y: 0 });
  sandbox.physicsSync();
  settle(sandbox, 300);

  const after = positions(sandbox);
  assert.notDeepEqual(after.a, after.b, "the nodes were pushed apart");
});

test("a node added to the graph joins the simulation, and one removed leaves it", () => {
  const { sandbox } = buildPage([
    { data: { id: "a", size: 25 } },
    { data: { id: "b", size: 25 } },
  ]);

  sandbox.physicsStart();
  assert.equal(sandbox.globalPhysics.nodes.length, 2);

  sandbox.globalCy.add([{ data: { id: "c", size: 25 } }]);
  sandbox.physicsSync();
  assert.equal(sandbox.globalPhysics.nodes.length, 3, "the new node joined");
  assert.ok(sandbox.globalPhysics.byId["c"] != null, "the new node is known");

  sandbox.globalCy.getElementById("b").remove();
  sandbox.physicsSync();
  assert.equal(sandbox.globalPhysics.nodes.length, 2, "the removed node left");
  assert.equal(sandbox.globalPhysics.byId["b"], undefined, "the removed node is gone");
});

test("a node dropped after a drag stays where it was dropped", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  const node = sandbox.globalCy.getElementById("a");

  node.emit("grab");
  node.position({ x: 300, y: 200 });
  node.emit("drag");
  node.emit("free");

  const simNode = sandbox.globalPhysics.byId["a"];
  assert.equal(simNode.fx, 300, "the node is pinned where it was dropped");
  assert.equal(simNode.fy, 200);
  assert.ok(node.hasClass("physics-pinned"), "the node shows it is pinned");

  // A double click unpins it
  node.emit("dbltap");
  assert.equal(simNode.fx, undefined, "the node floats again");
  assert.equal(simNode.fy, undefined);
  assert.ok(!node.hasClass("physics-pinned"), "the node no longer shows it is pinned");
});

test("a click that does not move the node leaves it floating", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  const node = sandbox.globalCy.getElementById("a");

  node.emit("grab");
  node.emit("free");

  const simNode = sandbox.globalPhysics.byId["a"];
  assert.equal(simNode.fx, undefined, "a click is not a drag");
  assert.ok(!node.hasClass("physics-pinned"));
});

test("the simulation writes its positions back to the graph", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.globalCy.nodes()[1].position({ x: 0, y: 0 });
  sandbox.physicsStart();
  settle(sandbox, 300);
  sandbox.physicsTick();

  const a = sandbox.globalCy.getElementById("a").position();
  const b = sandbox.globalCy.getElementById("b").position();
  assert.notDeepEqual(a, b, "the graph shows the nodes apart");
});

test("the switch turns the simulation off and on again", () => {
  const { sandbox, state } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  assert.ok(sandbox.globalPhysics != null, "the simulation runs");

  sandbox.physicsSetEnabled(false);
  assert.equal(sandbox.globalPhysics, null, "the simulation stopped");
  assert.equal(sandbox.globalGraphSettings.physics, false, "the setting was remembered");
  assert.ok(state.saved, "the setting was saved");
  assert.equal(state.buttonActive, false, "the toolbar button shows it is off");
  assert.equal(state.buttonPressed, "false");
  assert.equal(state.checkboxChecked, false, "the settings checkbox shows it is off");

  sandbox.physicsSetEnabled(true);
  assert.ok(sandbox.globalPhysics != null, "the simulation runs again");
  assert.equal(sandbox.globalGraphSettings.physics, true);
  assert.equal(state.buttonActive, true, "the toolbar button shows it is on");
  assert.equal(state.buttonPressed, "true");
  assert.equal(state.checkboxChecked, true, "the settings checkbox shows it is on");
});

test("the switch keeps the nodes where they were", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  settle(sandbox, 300);
  const before = sandbox.globalCy.getElementById("a").position();

  sandbox.physicsSetEnabled(false);
  const after = sandbox.globalCy.getElementById("a").position();
  assert.deepEqual(after, before, "the graph did not move when the simulation stopped");
});
