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
// later leaves it, that a dragged node stays where it was dropped, that a double-clicked one floats again and
// that the toolbar switch stops and starts the whole thing.
//
//     node --test studio/test/graph-physics-engine.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STUDIO = path.join(__dirname, "..");
const SRC_DIR = path.join(STUDIO, "src", "main", "resources", "static", "js");
const PHYSICS_SRC = fs.readFileSync(path.join(SRC_DIR, "studio-graph-physics.js"), "utf8");
const GRAPH_SRC = fs.readFileSync(path.join(SRC_DIR, "studio-graph.js"), "utf8");

const cytoscape = require(path.join(STUDIO, "node_modules", "cytoscape"));
const d3 = require(path.join(STUDIO, "node_modules", "d3-force"));

// The defaults and the pinned class live in the settings model (js/studio-graph.js): the engine reads them from
// there, so the page below has to have them too.
const constants = {};
for (const declaration of GRAPH_SRC.match(/var PHYSICS_[A-Z_]+ = [^;]+;/g)) {
  const name = declaration.match(/var ([A-Z_]+)/)[1];
  constants[name] = eval(declaration.replace(/^var /, ""));
}

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
  // A ready callback runs, like jQuery does when the document is parsed
  return (ready) => {
    if (typeof ready === "function") ready();
    return chain;
  };
}

/**
 * A Studio page with the physics engine loaded: the same globals the widget and the settings panel use.
 *
 * The engine runs in this realm (not a vm context) on purpose: Cytoscape recognises a plain object by its
 * constructor, and an object made in another realm is not one, so the positions would silently be dropped.
 */
function buildPage(elements, settings, stored) {
  const state = { buttonActive: null, buttonPressed: null, checkboxChecked: null, saved: null, stored: stored };

  const globals = Object.assign(
    {
      globalGraphSettings: Object.assign({ graphSpacing: 50, physics: true }, settings),
      globalCy: null,
      saveGraphGlobalSettings: () => {
        state.saved = true;
      },
      globalStorageLoad: (key, fallback) => (key === "graphGlobalSettings" && state.stored != null ? state.stored : fallback),
      $: jqueryStub(state),
      d3,
    },
    constants
  );

  // The engine is a plain script that reads the page globals; `with` gives it exactly those, and the objects it
  // makes are the ones this realm made, which is what Cytoscape checks for.
  const factory = new Function(
    "__globals",
    "with (__globals) {" +
      PHYSICS_SRC +
      "\nreturn { physicsEnabled, physicsForceParameters, physicsStart, physicsStop, physicsTick, physicsSync," +
      " physicsScheduleSync, physicsAttach, physicsSetEnabled, physicsUpdateControls, physicsReheat," +
      " physicsSettings, physicsLoadSettings, physicsRepulsionSetting, physicsGravitySetting, togglePhysics," +
      " physicsEdgeLengthSetting, physicsPaddingSetting, physicsUnpinAll," +
      " physicsLinks, get globalPhysics() { return globalPhysics; } }; }"
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
  // The graph the engine works on: a test can put another graph in its place, the way a new query does.
  Object.defineProperty(sandbox, "globalCy", {
    get: () => globals.globalCy,
    set: (v) => {
      globals.globalCy = v;
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

/** d3 keeps the charge strength as a function (it can be a different one per node), so the number is read through it. */
function chargeStrength(sandbox) {
  const strength = sandbox.globalPhysics.sim.force("charge").strength();
  return typeof strength === "function" ? strength() : strength;
}

/** d3 keeps the link distance as a function too (it can be a different one per link), so the number is read through it. */
function linkDistance(sandbox) {
  const distance = sandbox.globalPhysics.sim.force("link").distance();
  return typeof distance === "function" ? distance() : distance;
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

test("an edge added or removed on its own reaches the link force", async () => {
  const { sandbox } = buildPage([
    { data: { id: "a", size: 25 } },
    { data: { id: "b", size: 25 } },
  ]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  const linkCount = () => sandbox.globalPhysics.sim.force("link").links().length;
  assert.equal(linkCount(), 0);

  // BOTH ENDS ARE ALREADY ON THE GRAPH: ONLY AN EDGE EVENT TELLS THE SIMULATION
  sandbox.globalCy.add([{ data: { id: "ab", source: "a", target: "b" } }]);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(linkCount(), 1, "the new edge became a spring");

  sandbox.globalCy.getElementById("ab").remove();
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(linkCount(), 0, "the removed edge is no longer a spring");
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
  assert.ok(node.hasClass(constants.PHYSICS_PINNED_CLASS), "the node shows it is pinned");

  // A double click unpins it
  node.emit("dbltap");
  assert.equal(simNode.fx, undefined, "the node floats again");
  assert.equal(simNode.fy, undefined);
  assert.ok(!node.hasClass(constants.PHYSICS_PINNED_CLASS), "the node no longer shows it is pinned");
});

test("a pinned node stays where it was dropped when a layout moves the graph", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  const node = sandbox.globalCy.getElementById("a");
  node.emit("grab");
  node.position({ x: 300, y: 200 });
  node.emit("drag");
  node.emit("free");

  // A LAYOUT OF THE WHOLE GRAPH (A NODE EXPANSION) MOVES EVERY NODE, THE PINNED ONE TOO
  sandbox.globalCy.emit("layoutstart");
  node.position({ x: 900, y: 900 });
  sandbox.globalCy.emit("layoutstop");
  settle(sandbox, 5);
  sandbox.physicsTick();

  assert.deepEqual(node.position(), { x: 300, y: 200 }, "the pinned node is back where the user dropped it");
  assert.ok(node.hasClass(constants.PHYSICS_PINNED_CLASS), "and it is still pinned");
});

test("a slider moved while a layout runs does not start the simulation over the layout", async () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  sandbox.globalCy.emit("layoutstart");
  const before = positions(sandbox);
  const cyBefore = JSON.stringify(sandbox.globalCy.nodes().map((n) => n.position()));

  sandbox.globalGraphSettings.physicsRepulsion = 20;
  sandbox.physicsReheat();
  await new Promise((resolve) => setTimeout(resolve, 150));

  assert.equal(JSON.stringify(sandbox.globalCy.nodes().map((n) => n.position())), cyBefore, "the layout still owns the nodes");
  assert.deepEqual(positions(sandbox), before, "the simulation did not move");

  // THE FORCES WERE STILL UPDATED: THEY TAKE EFFECT WHEN THE LAYOUT HANDS THE NODES OVER
  assert.equal(chargeStrength(sandbox), -20 * 50);
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
  assert.ok(!node.hasClass(constants.PHYSICS_PINNED_CLASS));
});

test("a click on a pinned node keeps it pinned", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  const node = sandbox.globalCy.getElementById("a");

  // Drag it and let it go: it stays where it was dropped
  node.emit("grab");
  node.position({ x: 300, y: 200 });
  node.emit("drag");
  node.emit("free");
  const simNode = sandbox.globalPhysics.byId["a"];
  assert.equal(simNode.fx, 300);

  // A click that does not move it must not unpin it: only a double click does that
  node.emit("grab");
  node.emit("free");
  assert.equal(simNode.fx, 300, "the node is still pinned");
  assert.equal(simNode.fy, 200);
  assert.ok(node.hasClass(constants.PHYSICS_PINNED_CLASS), "the node still shows it is pinned");
});

test("unpin all releases every pinned node", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  for (const id of ["a", "b"]) {
    const node = sandbox.globalCy.getElementById(id);
    node.emit("grab");
    node.position({ x: 300, y: 200 });
    node.emit("drag");
    node.emit("free");
    assert.equal(sandbox.globalPhysics.byId[id].fx, 300, "node " + id + " is pinned");
    assert.ok(node.hasClass(constants.PHYSICS_PINNED_CLASS));
  }
  sandbox.globalPhysics.sim.alpha(0);

  sandbox.physicsUnpinAll();

  for (const id of ["a", "b"]) {
    assert.equal(sandbox.globalPhysics.byId[id].fx, undefined, "node " + id + " floats again");
    assert.equal(sandbox.globalPhysics.byId[id].fy, undefined);
  }
  assert.equal(sandbox.globalCy.nodes("." + constants.PHYSICS_PINNED_CLASS).length, 0, "no node shows it is pinned");
  assert.ok(sandbox.globalPhysics.sim.alpha() > 0.01, "the graph started moving again");
});

test("unpin all with the physics off only clears the pins on the graph", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  sandbox.physicsSetEnabled(false);
  const node = sandbox.globalCy.getElementById("a");
  node.addClass(constants.PHYSICS_PINNED_CLASS);

  sandbox.physicsUnpinAll();

  assert.ok(!node.hasClass(constants.PHYSICS_PINNED_CLASS), "the pin is gone");
  assert.equal(sandbox.globalPhysics, null, "no simulation was started");
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
  const { sandbox } = buildPage([
    { data: { id: "a", size: 25 }, position: { x: -100, y: 0 } },
    { data: { id: "b", size: 25 }, position: { x: 100, y: 0 } },
  ]);

  sandbox.physicsStart();
  settle(sandbox, 300);
  // THE SIMULATED POSITIONS ARE WRITTEN BACK TO THE GRAPH, SO THE SWITCH COMPARES WHAT IS ON SCREEN
  sandbox.physicsTick();
  const before = sandbox.globalCy.getElementById("a").position();
  assert.notDeepEqual(before, { x: -100, y: 0 }, "the simulation moved the node before the switch");

  sandbox.physicsSetEnabled(false);
  const after = sandbox.globalCy.getElementById("a").position();
  assert.deepEqual(after, before, "the graph did not move when the simulation stopped");
});

test("a new setting reaches the running simulation", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  const before = chargeStrength(sandbox);

  sandbox.globalGraphSettings.physicsRepulsion = 15;
  sandbox.physicsReheat();

  const after = chargeStrength(sandbox);
  assert.notEqual(after, before, "the repulsion changed");
  assert.equal(after, -15 * 50, "the new repulsion is the one the slider asks for");
});

test("the edge length slider reaches the running simulation", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  assert.equal(linkDistance(sandbox), 150);

  sandbox.globalGraphSettings.physicsEdgeLength = 20;
  sandbox.physicsReheat();

  assert.equal(linkDistance(sandbox), 300);
});

test("the settings of the graph keep the physics ones", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }], {
    physicsRepulsion: 11,
    physicsGravity: 3,
    physicsEdgeLength: 7,
    physicsPadding: 2,
  });

  assert.equal(sandbox.physicsSettings().physicsRepulsion, 11);
  assert.equal(sandbox.physicsSettings().physicsGravity, 3);
  assert.equal(sandbox.physicsSettings().physicsEdgeLength, 7);
  assert.equal(sandbox.physicsSettings().physicsPadding, 2);
  assert.equal(sandbox.physicsRepulsionSetting(), 11);
  assert.equal(sandbox.physicsGravitySetting(), 3);
  assert.equal(sandbox.physicsEdgeLengthSetting(), 7);
  assert.equal(sandbox.physicsPaddingSetting(), 2);

  // A settings file without the physics keys leaves the defaults in place
  sandbox.physicsLoadSettings({ graphSpacing: 50 });
  assert.equal(sandbox.physicsRepulsionSetting(), 11, "what was there stays there");
});

test("the toolbar shows the stored setting before any graph is rendered", () => {
  const { state } = buildPage([{ data: { id: "a", size: 25 } }], { physics: true }, JSON.stringify({ physics: false }));

  assert.equal(state.buttonActive, false, "the toolbar button shows the stored setting");
  assert.equal(state.buttonPressed, "false");
  assert.equal(state.checkboxChecked, false, "the settings checkbox shows it is off");
});

test("a corrupt setting cannot put anything but a number in the settings panel", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }], {
    physicsRepulsion: '8" onmouseover="alert(1)',
    physicsGravity: "not a number",
    physicsEdgeLength: "not a number",
    physicsPadding: '2" onmouseover="alert(1)',
  });

  assert.equal(sandbox.physicsRepulsionSetting(), 8, "the repulsion is the number the setting starts with");
  assert.equal(sandbox.physicsGravitySetting(), 5, "the gravity falls back to the default");
  assert.equal(typeof sandbox.physicsRepulsionSetting(), "number");
  assert.equal(typeof sandbox.physicsGravitySetting(), "number");
  assert.equal(sandbox.physicsEdgeLengthSetting(), 10, "the edge length falls back to the default");
  assert.equal(sandbox.physicsPaddingSetting(), 2, "the padding is the number the setting starts with");
  assert.equal(typeof sandbox.physicsEdgeLengthSetting(), "number");
  assert.equal(typeof sandbox.physicsPaddingSetting(), "number");
});

test("physics turned on during a layout waits for the layout to finish", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsAttach();

  // A LAYOUT TAKES THE NODES OVER
  sandbox.globalCy.emit("layoutstart");
  assert.equal(sandbox.globalPhysics, null, "the simulation is not running during a layout");

  // TURNING THE PHYSICS ON IN THE MIDDLE OF THE LAYOUT MUST NOT START IT: THE LAYOUT OWNS THE NODES
  sandbox.physicsSetEnabled(true);
  assert.equal(sandbox.globalPhysics, null, "the simulation waits for the layout");

  // WHEN THE LAYOUT ENDS, THE SIMULATION TAKES OVER
  sandbox.globalCy.emit("layoutstop");
  assert.ok(sandbox.globalPhysics != null, "the simulation started with the layout");
});

test("physics turned off during a layout leaves the layout in charge", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  sandbox.physicsAttach();
  assert.ok(sandbox.globalPhysics != null, "the simulation runs");

  sandbox.globalCy.emit("layoutstart");
  sandbox.physicsSetEnabled(false);
  assert.equal(sandbox.globalPhysics, null, "the simulation stopped");
  assert.equal(sandbox.globalGraphSettings.physics, false, "the setting was remembered");

  // THE LAYOUT IS STILL RUNNING: TURNING THE PHYSICS ON MUST WAIT FOR IT
  sandbox.physicsSetEnabled(true);
  assert.equal(sandbox.globalPhysics, null, "the simulation waits for the layout");

  sandbox.globalCy.emit("layoutstop");
  assert.ok(sandbox.globalPhysics != null, "the simulation started with the layout");
});

test("a link to a node the simulation does not know is left out", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }]);
  const edge = (source, target) => ({ data: (key) => (key === "source" ? source : target) });

  // d3 refuses a link to a node it does not know, and it would take the whole simulation down with it
  const links = sandbox.physicsLinks([edge("a", "a"), edge("a", "not-on-the-graph")], { a: { id: "a" } });

  assert.equal(links.length, 1, "the link to the unknown node was left out");
  assert.equal(links[0].source, "a");
  assert.equal(links[0].target, "a");
});

test("a node with an edge to itself does not break the simulation", () => {
  const { sandbox } = buildPage([
    { data: { id: "a", size: 25 } },
    { data: { id: "e1", source: "a", target: "a" } },
  ]);

  sandbox.physicsStart();
  assert.ok(sandbox.globalPhysics != null, "the simulation started");

  settle(sandbox, 50);
  assert.equal(sandbox.globalPhysics.nodes.length, 1, "the node is still in the simulation");
});

test("the simulation does not take a node away from the user who is dragging it", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }, { data: { id: "b", size: 25 } }]);

  sandbox.physicsStart();
  settle(sandbox, 50);

  const node = sandbox.globalCy.getElementById("a");
  // The renderer sets this flag when the mouse takes hold of the node
  node[0]._private.grabbed = true;

  // THE SIMULATION WANTS THE NODE SOMEWHERE ELSE: THE USER HAS IT
  sandbox.globalPhysics.byId["a"].x = 9999;
  sandbox.physicsTick();

  assert.notEqual(node.position("x"), 9999, "the node stayed where the user is holding it");
});

test("the simulation of a graph that is no longer on screen stops", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }]);

  sandbox.physicsStart();
  assert.ok(sandbox.globalPhysics != null, "the simulation runs");

  // A new query put another graph in its place
  sandbox.globalCy = null;
  sandbox.physicsTick();

  assert.equal(sandbox.globalPhysics, null, "the orphaned simulation stopped");
});

test("no simulation runs while the physics is off", () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }], { physics: false });

  sandbox.physicsStart();
  assert.equal(sandbox.globalPhysics, null, "the simulation did not start");

  sandbox.physicsSync();
  assert.equal(sandbox.globalPhysics, null, "the simulation did not start on a sync either");
});

test("a layout that never announces its end still hands the nodes to the simulation", async () => {
  const { sandbox } = buildPage([{ data: { id: "a", size: 25 } }]);

  sandbox.physicsAttach();
  sandbox.globalCy.emit("layoutstart");
  assert.equal(sandbox.globalPhysics, null, "the layout has the nodes");

  // A TAB IN THE BACKGROUND NEVER FINISHES AN ANIMATION: THE FALLBACK TIMER HANDS THE NODES OVER
  await new Promise((resolve) => setTimeout(resolve, 1800));
  assert.ok(sandbox.globalPhysics != null, "the simulation took the nodes over");
});
