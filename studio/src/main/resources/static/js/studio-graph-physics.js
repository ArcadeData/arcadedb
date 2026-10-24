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

// Live physics for the graph view. The fcose layout places the nodes once; from then on this file keeps the graph
// moving with a d3-force simulation (ISC): nodes repel each other, the edges act as springs and a weak centre
// gravity keeps the whole thing centred. The simulation is on by default and can be switched off from the toolbar.
//
// The force constants live in one pure function (physicsForceParameters) so they can be tested without a browser
// (studio/test/graph-physics.test.js); everything else here is the wiring between Cytoscape and d3-force.

/** The running simulation, or null when no simulation is attached: { cy, sim, nodes, eles, byId }. */
var globalPhysics = null;

/** Handle of the pending debounced rebuild of the simulation (the burst of 'add'/'remove' events of one change). */
var globalPhysicsSyncTimer = null;

/** Where the node being dragged was grabbed: { id, x, y }. A click that never moves the node is not a drag. */
var globalPhysicsGrab = null;

/** True while a Cytoscape layout is moving the nodes: the layout owns the positions until it stops. */
var globalPhysicsLayoutRunning = false;

/** Timer that takes the simulation over when a layout never announces its end (see physicsAttach). */
var globalPhysicsLayoutTimer = null;

/** True when live physics is enabled: absent, or anything but false, means on. */
function physicsEnabled() {
  return globalGraphSettings == null || globalGraphSettings.physics !== false;
}

/**
 * The force constants, derived from the graph settings. Pure, so the numbers can be pinned by a unit test.
 *
 * The spacing drives everything: the link distance is the same ideal edge length fcose uses, the repulsion is
 * scaled with it (the product keeps the same look whatever the spacing), and the charge cut-off keeps a big graph
 * cheap. The values are clamped so a corrupt setting can never produce a NaN force.
 */
function physicsForceParameters(settings) {
  if (settings == null) settings = { graphSpacing: 50 };

  let spacing = Math.min(150, Math.max(10, parseInt(settings.graphSpacing) || 50));
  let repulsion = settings.physicsRepulsion == null ? 8 : Math.min(20, Math.max(0, parseInt(settings.physicsRepulsion) || 0));
  let gravity = settings.physicsGravity == null ? 5 : Math.min(20, Math.max(0, parseInt(settings.physicsGravity) || 0));
  return {
    linkDistance: spacing * 3, // same as fcose idealEdgeLength
    chargeStrength: -repulsion * spacing, // default -400 (Neo4j Browser's order of magnitude)
    chargeDistanceMax: spacing * 20, // Barnes-Hut cut-off, keeps 1000-node graphs cheap
    gravityStrength: gravity / 100, // d3 forceX/forceY strength, default 0.05
    collidePadding: spacing / 4,
  };
}

/** Pushes the current settings into the running simulation. */
function physicsApplyForces() {
  if (globalPhysics == null) return;

  let p = physicsForceParameters(globalGraphSettings);
  let sim = globalPhysics.sim;

  sim.force("link").distance(p.linkDistance);
  sim.force("charge").strength(p.chargeStrength).distanceMax(p.chargeDistanceMax);
  sim.force("collide").radius(d => d.r + p.collidePadding);
  sim.force("x").strength(p.gravityStrength);
  sim.force("y").strength(p.gravityStrength);
}

/**
 * The links of the simulation, built from the graph edges. An edge whose endpoint is not on the graph is left out:
 * d3 refuses a link to a node it does not know, and it would take the whole simulation down with it.
 */
function physicsLinks(edges, byId) {
  let links = [];
  for (let i = 0; i < edges.length; i++) {
    let edge = edges[i];
    let source = edge.data("source");
    let target = edge.data("target");
    if (byId[source] == null || byId[target] == null) continue;
    links.push({ source: source, target: target });
  }
  return links;
}

/** Starts a simulation on the current graph, warm-started from the positions fcose left behind. */
function physicsStart() {
  if (globalCy == null || !physicsEnabled() || typeof d3 === "undefined") return;

  physicsStop();

  let eles = globalCy.nodes();
  if (eles.length == 0) return;

  let nodes = [];
  let byId = {};
  for (let i = 0; i < eles.length; i++) {
    let ele = eles[i];
    let pos = ele.position();
    let node = { id: ele.id(), x: pos.x, y: pos.y, r: (ele.data("size") || 25) / 2 };
    nodes.push(node);
    byId[node.id] = node;
  }

  let links = physicsLinks(globalCy.edges(), byId);

  // THE CENTRE OF THE GRAPH: WHAT THE GRAVITY PULLS TOWARDS
  let bb = globalCy.nodes().boundingBox();
  let cx = (bb.x1 + bb.x2) / 2;
  let cy = (bb.y1 + bb.y2) / 2;

  let sim = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id))
    .force("charge", d3.forceManyBody())
    .force("collide", d3.forceCollide())
    .force("x", d3.forceX(cx))
    .force("y", d3.forceY(cy))
    .alpha(0.3)
    .on("tick", physicsTick);

  globalPhysics = { cy: globalCy, sim: sim, nodes: nodes, eles: eles, byId: byId };

  // A NODE PINNED BEFORE THE SIMULATION RESTARTED STAYS WHERE IT WAS LEFT
  for (let i = 0; i < eles.length; i++) {
    let ele = eles[i];
    if (!ele.hasClass("physics-pinned")) continue;
    let node = byId[ele.id()];
    if (node == null) continue;
    let pos = ele.position();
    node.fx = pos.x;
    node.fy = pos.y;
  }

  physicsApplyForces();
}

/** Stops the simulation and forgets it. The nodes keep the positions they had. */
function physicsStop() {
  if (globalPhysics != null) {
    globalPhysics.sim.stop();
    globalPhysics = null;
  }

  if (globalPhysicsSyncTimer != null) {
    clearTimeout(globalPhysicsSyncTimer);
    globalPhysicsSyncTimer = null;
  }

  if (globalPhysicsLayoutTimer != null) {
    clearTimeout(globalPhysicsLayoutTimer);
    globalPhysicsLayoutTimer = null;
  }

  globalPhysicsLayoutRunning = false;
}

/** One simulation step: the new node positions are written back to Cytoscape. */
function physicsTick() {
  if (globalPhysics == null) return;

  if (globalPhysics.cy !== globalCy) {
    // The graph was replaced (a new query, another tab): this simulation is an orphan
    physicsStop();
    return;
  }

  globalPhysics.eles.positions(function (ele, i) {
    if (ele.removed()) return null; // gone from the graph: the rebuild will drop it
    if (ele.grabbed()) return null; // the user is dragging it: Cytoscape owns that position
    let n = globalPhysics.nodes[i];
    return { x: n.x, y: n.y };
  });
}

/**
 * Rebuilds the simulation from the current graph: the nodes that are still there keep their velocity and their
 * pin, the new ones join where they were placed, and the removed ones go away.
 */
function physicsSync() {
  if (globalCy == null || !physicsEnabled()) return;

  if (globalPhysics == null || globalPhysics.cy !== globalCy) {
    physicsStart();
    return;
  }

  let eles = globalCy.nodes();
  if (eles.length == 0) {
    physicsStop();
    return;
  }

  let oldById = globalPhysics.byId;
  let nodes = [];
  let byId = {};
  for (let i = 0; i < eles.length; i++) {
    let ele = eles[i];
    let pos = ele.position();
    let node = oldById[ele.id()];
    if (node == null) {
      node = { id: ele.id(), x: pos.x, y: pos.y, r: (ele.data("size") || 25) / 2 };
    } else {
      // THE POSITION COMES FROM THE GRAPH (A LAYOUT MAY HAVE MOVED IT), THE MOTION FROM THE SIMULATION
      node.x = pos.x;
      node.y = pos.y;
      node.r = (ele.data("size") || 25) / 2;
    }
    nodes.push(node);
    byId[node.id] = node;
  }

  let links = physicsLinks(globalCy.edges(), byId);

  let sim = globalPhysics.sim;
  // THE OLD LINK FORCE IS DROPPED FIRST: IT STILL POINTS AT THE NODES THAT ARE GONE
  sim.force("link", null);
  sim.nodes(nodes);
  sim.force("link", d3.forceLink(links).id(d => d.id));

  globalPhysics.nodes = nodes;
  globalPhysics.eles = eles;
  globalPhysics.byId = byId;

  physicsApplyForces();
  sim.alpha(0.3).restart();
}

/** Rebuilds the simulation once, after the burst of 'add'/'remove' events of a single change has settled. */
function physicsScheduleSync() {
  clearTimeout(globalPhysicsSyncTimer);
  globalPhysicsSyncTimer = setTimeout(function () {
    globalPhysicsSyncTimer = null;
    // A LAYOUT IS STILL MOVING THE NODES: IT WILL CALL physicsSync() WHEN IT STOPS
    if (globalPhysicsLayoutRunning) return;
    physicsSync();
  }, 0);
}

/** Wires the simulation to the Cytoscape events of the current graph. */
function physicsAttach() {
  if (globalCy == null) return;

  let cy = globalCy;

  cy.on("layoutstart", function (event) {
    if (cy !== globalCy) return;
    globalPhysicsLayoutRunning = true;
    // THE LAYOUT IS MOVING THE NODES: THE SIMULATION MUST NOT FIGHT IT
    if (globalPhysics != null && globalPhysics.cy === cy) globalPhysics.sim.stop();

    // A LAYOUT ONLY ANNOUNCES ITS END WHEN ITS ANIMATION FINISHES, AND A TAB IN THE BACKGROUND NEVER
    // FINISHES AN ANIMATION: THIS TIMER HANDS THE NODES OVER TO THE SIMULATION REGARDLESS
    let duration = event.layout && event.layout.options ? event.layout.options.animationDuration : 0;
    clearTimeout(globalPhysicsLayoutTimer);
    globalPhysicsLayoutTimer = setTimeout(function () {
      globalPhysicsLayoutTimer = null;
      if (!globalPhysicsLayoutRunning) return;
      globalPhysicsLayoutRunning = false;
      physicsSync();
    }, (duration || 500) + 1000);
  });

  cy.on("layoutstop", function () {
    if (cy !== globalCy) return;
    clearTimeout(globalPhysicsLayoutTimer);
    globalPhysicsLayoutTimer = null;
    globalPhysicsLayoutRunning = false;
    physicsSync();
  });

  cy.on("remove", "node", function (event) {
    if (cy !== globalCy) return;
    physicsScheduleSync();
  });

  cy.on("add", "node", function (event) {
    if (cy !== globalCy) return;
    physicsScheduleSync();
  });

  cy.on("grab", "node", function (event) {
    if (globalPhysics == null || globalPhysics.cy !== globalCy) return;
    let node = globalPhysics.byId[event.target.id()];
    if (node == null) return;

    let pos = event.target.position();
    node.fx = pos.x;
    node.fy = pos.y;
    globalPhysicsGrab = { id: event.target.id(), x: pos.x, y: pos.y };
    globalPhysics.sim.alphaTarget(0.3).restart();
  });

  cy.on("drag", "node", function (event) {
    if (globalPhysics == null || globalPhysics.cy !== globalCy) return;
    let node = globalPhysics.byId[event.target.id()];
    if (node == null) return;

    let pos = event.target.position();
    node.fx = pos.x;
    node.fy = pos.y;
  });

  cy.on("free", "node", function (event) {
    if (globalPhysics == null || globalPhysics.cy !== globalCy) return;
    let node = globalPhysics.byId[event.target.id()];
    if (node == null) return;

    let pos = event.target.position();
    globalPhysics.sim.alphaTarget(0);

    // A CLICK THAT NEVER MOVED THE NODE IS NOT A DRAG: THE NODE GOES BACK TO FLOATING
    let dragged = globalPhysicsGrab != null && globalPhysicsGrab.id === event.target.id() && (globalPhysicsGrab.x !== pos.x || globalPhysicsGrab.y !== pos.y);
    globalPhysicsGrab = null;

    if (dragged) {
      // THE NODE STAYS WHERE IT WAS RELEASED, UNTIL IT IS DOUBLE-CLICKED
      node.fx = pos.x;
      node.fy = pos.y;
      event.target.addClass("physics-pinned");
    } else {
      delete node.fx;
      delete node.fy;
      event.target.removeClass("physics-pinned");
    }
  });

  cy.on("dbltap", "node", function (event) {
    if (globalPhysics == null || globalPhysics.cy !== globalCy) return;
    let node = globalPhysics.byId[event.target.id()];
    if (node == null) return;
    if (!event.target.hasClass("physics-pinned")) return;

    delete node.fx;
    delete node.fy;
    event.target.removeClass("physics-pinned");
    globalPhysics.sim.alpha(0.3).restart();
  });
}

/** Turns the live physics on or off and remembers the choice. */
function physicsSetEnabled(enabled) {
  globalGraphSettings.physics = enabled;
  saveGraphGlobalSettings();
  physicsUpdateToggleButton();

  if (enabled) {
    physicsSync();
  } else {
    physicsStop();
    if (globalCy != null) globalCy.nodes(".physics-pinned").removeClass("physics-pinned");
  }
}

/** Keeps the toolbar button and the settings checkbox in step with the setting. */
function physicsUpdateToggleButton() {
  $("#togglePhysicsBtn").toggleClass("active", physicsEnabled());
  $("#togglePhysicsBtn").attr("aria-pressed", physicsEnabled() ? "true" : "false");
  $("#settingPhysics").prop("checked", physicsEnabled());
}

/** The toolbar button: live physics on or off. */
function togglePhysics() {
  physicsSetEnabled(!physicsEnabled());
}
