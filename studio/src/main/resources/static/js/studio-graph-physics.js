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

/** True while a Cytoscape layout is moving the nodes: the layout owns the positions until it stops. */
var globalPhysicsLayoutRunning = false;

/** Timer that takes the simulation over when a layout never announces its end (see physicsAttach). */
var globalPhysicsLayoutTimer = null;

/** True when live physics is enabled: absent, or anything but false, means on. */
function physicsEnabled() {
  return globalGraphSettings.physics !== false;
}

/**
 * The repulsion the graph runs with: the setting, or the default when it was never saved.
 *
 * The value is a number, always: the settings can be replaced wholesale by an imported file, and the number goes
 * straight into the settings panel markup.
 */
function physicsRepulsionSetting() {
  let value = parseInt(globalGraphSettings.physicsRepulsion);
  return isNaN(value) ? PHYSICS_DEFAULT_REPULSION : value;
}

/**
 * The gravity the graph runs with: the setting, or the default when it was never saved.
 *
 * The value is a number, always: the settings can be replaced wholesale by an imported file, and the number goes
 * straight into the settings panel markup.
 */
function physicsGravitySetting() {
  let value = parseInt(globalGraphSettings.physicsGravity);
  return isNaN(value) ? PHYSICS_DEFAULT_GRAVITY : value;
}

/** The edge length the graph runs with: the setting, or the default when it was never saved. */
function physicsEdgeLengthSetting() {
  let value = parseInt(globalGraphSettings.physicsEdgeLength);
  return isNaN(value) ? PHYSICS_DEFAULT_EDGE_LENGTH : value;
}

/** The node padding the graph runs with: the setting, or the default when it was never saved. */
function physicsPaddingSetting() {
  let value = parseInt(globalGraphSettings.physicsPadding);
  return isNaN(value) ? PHYSICS_DEFAULT_PADDING : value;
}

/**
 * The force constants, derived from the graph settings. Pure, so the numbers can be pinned by a unit test.
 *
 * The spacing drives everything: the link distance is the ideal edge length fcose uses at the default edge length
 * level and scales with it, the padding is spacing/4 at the default padding level, the repulsion is
 * scaled with it (the product keeps the same look whatever the spacing), and the charge cut-off keeps a big graph
 * cheap. The values are clamped so a corrupt setting can never produce a NaN force.
 */
function physicsForceParameters(settings) {
  let spacing = Math.min(150, Math.max(10, parseInt(settings.graphSpacing) || 50));
  let repulsion =
    settings.physicsRepulsion == null
      ? PHYSICS_DEFAULT_REPULSION
      : Math.min(PHYSICS_MAX_LEVEL, Math.max(0, parseInt(settings.physicsRepulsion) || 0));
  let gravity =
    settings.physicsGravity == null
      ? PHYSICS_DEFAULT_GRAVITY
      : Math.min(PHYSICS_MAX_LEVEL, Math.max(0, parseInt(settings.physicsGravity) || 0));
  let edgeLength =
    settings.physicsEdgeLength == null
      ? PHYSICS_DEFAULT_EDGE_LENGTH
      : Math.min(PHYSICS_MAX_LEVEL, Math.max(1, parseInt(settings.physicsEdgeLength) || 1));
  let padding =
    settings.physicsPadding == null
      ? PHYSICS_DEFAULT_PADDING
      : Math.min(PHYSICS_MAX_LEVEL, Math.max(0, parseInt(settings.physicsPadding) || 0));
  return {
    linkDistance: (spacing * 3 * edgeLength) / PHYSICS_DEFAULT_EDGE_LENGTH, // level 10 = fcose idealEdgeLength (spacing*3)
    chargeStrength: -repulsion * spacing, // default -400 (Neo4j Browser's order of magnitude)
    chargeDistanceMax: spacing * 20, // Barnes-Hut cut-off, keeps 1000-node graphs cheap
    gravityStrength: gravity / 100, // d3 forceX/forceY strength, default 0.05
    collidePadding: (spacing * padding) / (PHYSICS_DEFAULT_PADDING * 4), // level 5 = spacing/4
  };
}

/** Pushes the current settings into the running simulation. */
function physicsApplyForces() {
  if (globalPhysics == null) return;

  let parameters = physicsForceParameters(globalGraphSettings);
  let sim = globalPhysics.sim;

  sim.force("link").distance(parameters.linkDistance);
  sim.force("charge").strength(parameters.chargeStrength).distanceMax(parameters.chargeDistanceMax);
  sim.force("collide").radius(d => d.r + parameters.collidePadding);
  sim.force("x").strength(parameters.gravityStrength);
  sim.force("y").strength(parameters.gravityStrength);
}

/** Gives the simulation a nudge: the current settings are applied and the graph starts moving again. */
function physicsReheat() {
  if (globalPhysics == null) return;
  physicsApplyForces();
  globalPhysics.sim.alpha(0.3).restart();
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

/**
 * The simulation nodes, built from the graph nodes. A node that is still on the graph keeps its simulation object,
 * so it keeps its velocity and its pin; a new one joins where the graph put it.
 */
function physicsBuildNodes(eles, previousById) {
  let nodes = [];
  let byId = {};
  for (let i = 0; i < eles.length; i++) {
    let ele = eles[i];
    let pos = ele.position();
    let node = previousById[ele.id()];
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
  return { nodes: nodes, byId: byId };
}

/** Starts a simulation on the current graph, warm-started from the positions fcose left behind. */
function physicsStart() {
  if (globalCy == null || !physicsEnabled() || typeof d3 === "undefined") return;

  physicsStop();

  let eles = globalCy.nodes();
  if (eles.length == 0) return;

  let nodeSet = physicsBuildNodes(eles, {});
  let nodes = nodeSet.nodes;
  let byId = nodeSet.byId;
  let links = physicsLinks(globalCy.edges(), byId);

  // THE CENTRE OF THE GRAPH: WHAT THE GRAVITY PULLS TOWARDS
  let bb = eles.boundingBox();
  let cx = (bb.x1 + bb.x2) / 2;
  let cy = (bb.y1 + bb.y2) / 2;

  let sim = d3
    .forceSimulation(nodes)
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
    if (!ele.hasClass(PHYSICS_PINNED_CLASS)) continue;
    let node = byId[ele.id()];
    if (node == null) continue;
    let pos = ele.position();
    node.fx = pos.x;
    node.fy = pos.y;
  }

  physicsApplyForces();
}

/**
 * Stops the simulation and forgets it. The nodes keep the positions they had.
 *
 * `preserveLayout` keeps the bookkeeping of a layout that is still running (its flag and its fallback timer): the
 * simulation is stopped, but the layout keeps the right to hand the nodes over when it ends.
 */
function physicsStop(preserveLayout) {
  if (globalPhysics != null) {
    globalPhysics.sim.stop();
    globalPhysics = null;
  }

  if (globalPhysicsSyncTimer != null) {
    clearTimeout(globalPhysicsSyncTimer);
    globalPhysicsSyncTimer = null;
  }

  if (!preserveLayout) {
    if (globalPhysicsLayoutTimer != null) {
      clearTimeout(globalPhysicsLayoutTimer);
      globalPhysicsLayoutTimer = null;
    }

    globalPhysicsLayoutRunning = false;
  }
}

/** One simulation step: the new node positions are written back to Cytoscape. */
function physicsTick() {
  if (globalPhysics == null) return;

  if (globalPhysics.cy !== globalCy) {
    // The graph was replaced (a new query, another tab): this simulation is an orphan
    physicsStop();
    return;
  }

  // ONE OBJECT FOR THE WHOLE FRAME: Cytoscape copies the values out of it before the next node is written, so
  // there is no need to make a new one for every node of every frame
  //
  // THE POSITIONS ARE WRITTEN WITH THE EVENTS ON: THE RENDERER NEEDS THE NOTIFICATION TO REDRAW, AND THE HTML
  // LABELS OF THE GRAPH FOLLOW THE 'position' AND 'bounds' EVENTS OF THE NODES
  let position = { x: 0, y: 0 };

  globalPhysics.eles.positions(function (ele, i) {
    if (ele.removed()) return null; // gone from the graph: the rebuild will drop it
    if (ele.grabbed()) return null; // the user is dragging it: Cytoscape owns that position
    let n = globalPhysics.nodes[i];
    position.x = n.x;
    position.y = n.y;
    return position;
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

  let nodeSet = physicsBuildNodes(eles, globalPhysics.byId);
  let nodes = nodeSet.nodes;
  let byId = nodeSet.byId;
  let links = physicsLinks(globalCy.edges(), byId);

  let sim = globalPhysics.sim;
  // THE OLD LINK FORCE IS DROPPED FIRST: IT STILL POINTS AT THE NODES THAT ARE GONE
  sim.force("link", null);
  sim.nodes(nodes);
  sim.force("link", d3.forceLink(links).id(d => d.id));

  globalPhysics.nodes = nodes;
  globalPhysics.eles = eles;
  globalPhysics.byId = byId;

  physicsReheat();
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
    // THE NODE IS HELD WHILE IT IS DRAGGED, AND THE STATE IT HAD BEFORE IS REMEMBERED: A CLICK ON A PINNED NODE
    // MUST NOT UNPIN IT
    node.grab = { x: pos.x, y: pos.y, pinned: event.target.hasClass(PHYSICS_PINNED_CLASS) };
    node.fx = pos.x;
    node.fy = pos.y;
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
    let grab = node.grab;
    delete node.grab;
    globalPhysics.sim.alphaTarget(0);

    // A CLICK THAT NEVER MOVED THE NODE IS NOT A DRAG: THE NODE KEEPS THE STATE IT HAD BEFORE THE CLICK
    let dragged = grab != null && (grab.x !== pos.x || grab.y !== pos.y);

    if (dragged) {
      // THE NODE STAYS WHERE IT WAS RELEASED, UNTIL IT IS DOUBLE-CLICKED
      node.fx = pos.x;
      node.fy = pos.y;
      event.target.addClass(PHYSICS_PINNED_CLASS);
    } else if (grab == null || !grab.pinned) {
      delete node.fx;
      delete node.fy;
      event.target.removeClass(PHYSICS_PINNED_CLASS);
    }
  });

  cy.on("dbltap", "node", function (event) {
    if (globalPhysics == null || globalPhysics.cy !== globalCy) return;
    let node = globalPhysics.byId[event.target.id()];
    if (node == null) return;
    if (!event.target.hasClass(PHYSICS_PINNED_CLASS)) return;

    delete node.fx;
    delete node.fy;
    event.target.removeClass(PHYSICS_PINNED_CLASS);
    globalPhysics.sim.alpha(0.3).restart();
  });
}

/** Turns the live physics on or off and remembers the choice. */
function physicsSetEnabled(enabled) {
  globalGraphSettings.physics = enabled;
  saveGraphGlobalSettings();
  physicsUpdateControls();

  if (enabled) {
    // A LAYOUT IS STILL MOVING THE NODES: IT OWNS THEM UNTIL IT STOPS, AND IT STARTS THE SIMULATION THEN
    if (!globalPhysicsLayoutRunning) physicsSync();
  } else {
    // THE LAYOUT THAT IS RUNNING KEEPS ITS PLACE: ONLY THE SIMULATION GOES AWAY
    physicsStop(globalPhysicsLayoutRunning);
    if (globalCy != null) globalCy.nodes("." + PHYSICS_PINNED_CLASS).removeClass(PHYSICS_PINNED_CLASS);
  }
}

/** Keeps the toolbar button, the settings checkbox and the sliders in step with the settings. */
function physicsUpdateControls() {
  $("#togglePhysicsBtn").toggleClass("active", physicsEnabled());
  $("#togglePhysicsBtn").attr("aria-pressed", physicsEnabled() ? "true" : "false");
  $("#settingPhysics").prop("checked", physicsEnabled());

  let repulsion = physicsRepulsionSetting();
  $("#settingPhysicsRepulsion").val(repulsion);
  $("#settingPhysicsRepulsionVal").text(repulsion);
  let gravity = physicsGravitySetting();
  $("#settingPhysicsGravity").val(gravity);
  $("#settingPhysicsGravityVal").text(gravity);
  let edgeLength = physicsEdgeLengthSetting();
  $("#settingPhysicsEdgeLength").val(edgeLength);
  $("#settingPhysicsEdgeLengthVal").text(edgeLength);
  let padding = physicsPaddingSetting();
  $("#settingPhysicsPadding").val(padding);
  $("#settingPhysicsPaddingVal").text(padding);
}

/** The toolbar button: live physics on or off. */
function togglePhysics() {
  physicsSetEnabled(!physicsEnabled());
}

/** The toolbar button: every pinned node floats again. Works with the physics on or off. */
function physicsUnpinAll() {
  if (globalCy == null) return;

  if (globalPhysics != null && globalPhysics.cy === globalCy) {
    let nodes = globalPhysics.nodes;
    for (let i = 0; i < nodes.length; i++) {
      delete nodes[i].fx;
      delete nodes[i].fy;
    }
  }

  globalCy.nodes("." + PHYSICS_PINNED_CLASS).removeClass(PHYSICS_PINNED_CLASS);
  physicsReheat();
}

/** The physics part of the settings, in the shape the settings store keeps them. */
function physicsSettings() {
  return {
    physics: physicsEnabled(),
    physicsRepulsion: physicsRepulsionSetting(),
    physicsGravity: physicsGravitySetting(),
    physicsEdgeLength: physicsEdgeLengthSetting(),
    physicsPadding: physicsPaddingSetting(),
  };
}

/** Applies the physics part of a settings object that was loaded from the store. */
function physicsLoadSettings(parsed) {
  if (parsed.physics != null) globalGraphSettings.physics = parsed.physics;
  if (parsed.physicsRepulsion != null) globalGraphSettings.physicsRepulsion = parsed.physicsRepulsion;
  if (parsed.physicsGravity != null) globalGraphSettings.physicsGravity = parsed.physicsGravity;
  if (parsed.physicsEdgeLength != null) globalGraphSettings.physicsEdgeLength = parsed.physicsEdgeLength;
  if (parsed.physicsPadding != null) globalGraphSettings.physicsPadding = parsed.physicsPadding;
}

// THE TOOLBAR BUTTON IS PART OF THE PAGE FROM THE START, BUT THIS SCRIPT RUNS BEFORE THE MARKUP IS PARSED: THE
// CONTROLS ARE SYNCED AS SOON AS THE PAGE IS READY, SO A GRAPH IS NOT NEEDED FOR THE BUTTON TO SHOW THE SETTING.
// THE STORED SETTINGS ARE OTHERWISE ONLY READ WHEN A GRAPH IS RENDERED, SO THE PHYSICS PART IS READ HERE TOO.
$(function () {
  let stored = globalStorageLoad("graphGlobalSettings", null);
  if (stored != null) {
    try {
      physicsLoadSettings(JSON.parse(stored));
    } catch (e) {
      /* ignore corrupt data */
    }
  }

  physicsUpdateControls();
});
