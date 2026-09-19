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

// Regression test for issues #7538 and #7548: Studio has to show that the cluster is not ready for a group or
// API-token change BEFORE the operator attempts one.
//
// #7511 made the leader refuse such a change - HTTP 409 - while any peer has not advertised that it can decode
// the entry it would be replicated as, and GET /api/v1/cluster already carries the per-peer 'capabilities' array
// and 'capabilitiesUnknownReason' the decision is made from. Studio read neither, so a half-finished rolling
// upgrade looked finished on the cluster page and the operator found out by pressing the button.
//
// Run with:
//
//     node --test studio/test/cluster-capability-readiness.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC = path.join(__dirname, "..", "src", "main", "resources", "static");
const clusterSrc = fs.readFileSync(path.join(STATIC, "js", "studio-cluster.js"), "utf8");
const securitySrc = fs.readFileSync(path.join(STATIC, "js", "studio-security.js"), "utf8");

function extractFn(src, name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
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

function extractVar(src, name) {
  const start = src.indexOf("var " + name + " = ");
  if (start < 0) throw new Error("var not found: " + name);
  const end = src.indexOf("\n];", start);
  return src.substring(start, end + 3);
}

// escapeHtml lives in studio-utils.js and is a global at runtime; the banner builder needs it.
eval(extractFn(fs.readFileSync(path.join(STATIC, "js", "studio-utils.js"), "utf8"), "escapeHtml"));
eval(extractVar(clusterSrc, "CLUSTER_SECURITY_CAPABILITIES"));
eval(extractFn(clusterSrc, "clusterCapabilityReadiness"));
eval(extractFn(clusterSrc, "clusterSecurityCapabilityGaps"));
eval(extractFn(securitySrc, "securityCapabilityBanner"));
eval(extractFn(securitySrc, "clusterCapabilityRefusal"));

const GROUPS = "security-groups-entry";
const TOKENS = "security-api-tokens-entry";

/** A leader's view of a cluster whose peers advertise everything. */
function readyCluster() {
  return {
    isLeader: true,
    localPeerId: "arcadedb1",
    peers: [
      { id: "arcadedb1", role: "LEADER", capabilities: [GROUPS, TOKENS, "schema-delta"], version: "26.10.1" },
      { id: "arcadedb2", role: "FOLLOWER", capabilities: [GROUPS, TOKENS, "schema-delta"], version: "26.10.1" },
    ],
  };
}

test("a fully upgraded cluster reports nothing at all", () => {
  const readiness = clusterCapabilityReadiness(readyCluster(), GROUPS);

  assert.equal(readiness.determinable, true, "a leader can answer for its peers");
  assert.equal(readiness.ready, true);
  assert.deepEqual(readiness.missing, []);
  assert.deepEqual(clusterSecurityCapabilityGaps(readyCluster()), [], "no banner on a healthy cluster");
});

test("a peer that never answered the probe is named, with the server's own reason", () => {
  const data = readyCluster();
  data.peers[1] = {
    id: "arcadedb2",
    role: "FOLLOWER",
    capabilitiesUnknownReason: "its HTTP endpoint identifies no single peer: declare its 'http' port",
  };

  const readiness = clusterCapabilityReadiness(data, GROUPS);

  assert.equal(readiness.ready, false);
  assert.equal(readiness.missing.length, 1);
  assert.equal(readiness.missing[0].id, "arcadedb2");
  assert.match(readiness.missing[0].reason, /declare its 'http' port/, "the server's reason is what the operator acts on");
});

test("a peer with no reason still gets one, so the row is never blank", () => {
  const data = readyCluster();
  data.peers[1] = { id: "arcadedb2", role: "FOLLOWER" };

  const readiness = clusterCapabilityReadiness(data, GROUPS);
  assert.equal(readiness.ready, false);
  assert.ok(readiness.missing[0].reason.length > 0);
  assert.ok(!readiness.missing[0].reason.includes("undefined"));
});

test("a peer that answers but runs an older build is told apart from one that never answered", () => {
  const data = readyCluster();
  data.peers[1] = { id: "arcadedb2", role: "FOLLOWER", capabilities: ["schema-delta"], version: "26.9.1" };

  const readiness = clusterCapabilityReadiness(data, GROUPS);
  assert.equal(readiness.ready, false);
  assert.match(readiness.missing[0].reason, /predates/, "the remedy is 'finish the upgrade', not 'restore contact'");
});

test("the two capabilities are judged separately", () => {
  const data = readyCluster();
  data.peers[1] = { id: "arcadedb2", role: "FOLLOWER", capabilities: [GROUPS], version: "26.10.0" };

  assert.equal(clusterCapabilityReadiness(data, GROUPS).ready, true, "groups are fine");
  assert.equal(clusterCapabilityReadiness(data, TOKENS).ready, false, "tokens are not");

  const gaps = clusterSecurityCapabilityGaps(data);
  assert.equal(gaps.length, 1, "only the capability that is actually missing produces a banner");
  assert.equal(gaps[0].capability, TOKENS);
  assert.match(gaps[0].what, /API-token/);
});

// The property that stops the banner being a lie: only the LEADER probes its peers. On a follower the payload
// carries a capabilities array for the local node and for nobody else, and reading that as "not ready" would
// put a red banner on every follower of a perfectly healthy cluster.
test("a follower does not claim the cluster is unready just because it did not ask", () => {
  const data = readyCluster();
  data.isLeader = false;
  data.peers[1] = { id: "arcadedb2", role: "FOLLOWER" }; // no capabilities: this node never probed it

  const readiness = clusterCapabilityReadiness(data, GROUPS);
  assert.equal(readiness.determinable, false, "a follower cannot answer for its peers");
  assert.equal(readiness.ready, true, "and must not pretend it can");
  assert.deepEqual(clusterSecurityCapabilityGaps(data), []);
});

// This renderer runs on every cluster poll, so an entry it cannot read must not take the page with it: the
// throw would blank the node cards and the banner, hiding the very readiness they exist to show.
test("a malformed peer row is skipped rather than blanking the cluster page", () => {
  const data = readyCluster();
  data.peers = [data.peers[0], null, undefined, "arcadedb9", 42, { id: "arcadedb2" }];

  const readiness = clusterCapabilityReadiness(data, GROUPS);

  assert.equal(readiness.ready, false, "the one real peer that lacks the capability is still reported");
  assert.deepEqual(
    readiness.missing.map((m) => m.id),
    ["arcadedb2"],
    "and only it: a row that is not an object cannot be judged"
  );
  assert.doesNotThrow(() => securityCapabilityBanner(clusterSecurityCapabilityGaps(data)[0]));
});

// The guard above protects the readiness calculation, which the Security page calls with its own payload.
// The cluster PAGE has consumers that predate this PR and dereference a row directly - the local-peer scan,
// renderNodeCards, renderPeerManagement - so the list is normalised once at the top of renderClusterData
// rather than guarded in each of them: a guard added per consumer is a guard the next consumer forgets.
test("the cluster page filters the peer list once, before anything reads it", () => {
  const render = extractFn(clusterSrc, "renderClusterData");
  const filterAt = render.indexOf("data.peers = data.peers.filter(");

  assert.ok(filterAt > 0, "renderClusterData must normalise data.peers");
  assert.ok(
    filterAt < render.indexOf("clusterLastData = data;"),
    "and do it before clusterLastData is published, since the leadership picker reads that later"
  );
  assert.ok(filterAt < render.indexOf("data.peers[p].id"), "and before the local-peer scan dereferences a row");
  assert.ok(filterAt < render.indexOf("renderNodeCards(data)"));
  assert.ok(filterAt < render.indexOf("renderPeerManagement(data)"));
});

test("an unclustered server, an empty payload and a missing payload all say nothing", () => {
  for (const data of [null, undefined, {}, { peers: [] }, { isLeader: true, peers: [] }]) {
    assert.deepEqual(clusterSecurityCapabilityGaps(data), [], JSON.stringify(data));
  }
});

test("the banner names the peer, the reason, the capability and the remedy", () => {
  const data = readyCluster();
  data.peers[1] = { id: "arcadedb2", role: "FOLLOWER", capabilitiesUnknownReason: "probe timed out after 2000ms" };

  const html = securityCapabilityBanner(clusterSecurityCapabilityGaps(data)[0]);

  assert.match(html, /arcadedb2/, "the lagging peer");
  assert.match(html, /probe timed out/, "why it is unknown");
  assert.match(html, /security-groups-entry/, "the capability");
  assert.match(html, /rolling upgrade/, "the remedy");
  assert.equal(securityCapabilityBanner(null), "", "a ready cluster renders no banner at all");
});

test("the banner escapes a peer id and a reason rather than injecting them", () => {
  const data = readyCluster();
  data.peers[1] = { id: '<img src=x onerror=alert(1)>', role: "FOLLOWER", capabilitiesUnknownReason: '"><script>' };

  const html = securityCapabilityBanner(clusterSecurityCapabilityGaps(data)[0]);
  assert.ok(!html.includes("<img"), "a peer id is data, not markup");
  assert.ok(!html.includes("<script>"), "and so is a probe-failure reason");
});

// ---- the 409 the operator still meets, when the cluster falls behind between the poll and the click ----

const REFUSAL_409 = JSON.stringify({
  error: "Cluster is not ready for this operation",
  exception: "com.arcadedb.server.ClusterCapabilityNotReadyException",
  exceptionArgs: "security-groups-entry|arcadedb2,arcadedb3",
});

test("the 409 is rendered from exceptionArgs, which is the half production mode still sends", () => {
  const refusal = clusterCapabilityRefusal({ status: 409, responseText: REFUSAL_409 });

  assert.ok(refusal, "the capability refusal must be recognised");
  assert.match(refusal.message, /arcadedb2,arcadedb3/, "the peers are the actionable half");
  assert.match(refusal.message, /security-groups-entry/);
  assert.match(refusal.message, /nothing has changed/, "the refusal submitted nothing, and that has to be said");
  assert.ok(!refusal.message.includes("undefined"));
});

test("a 409 that is a different conflict is left to the existing error handler", () => {
  // 'backup already running' is also a 409 on these routes; remapping it would be a worse lie than the generic
  // toast it replaces.
  const otherConflict = JSON.stringify({
    error: "Backup already in progress",
    exception: "com.arcadedb.server.ServerControlPlane$OperationInProgressException",
  });
  assert.equal(clusterCapabilityRefusal({ status: 409, responseText: otherConflict }), null);
  assert.equal(clusterCapabilityRefusal({ status: 409, responseText: "<html>Conflict</html>" }), null);
});

test("every other status, and a missing jqXHR, are not a capability refusal", () => {
  for (const status of [0, 400, 401, 403, 404, 412, 500])
    assert.equal(clusterCapabilityRefusal({ status: status, responseText: REFUSAL_409 }), null, "status " + status);
  assert.equal(clusterCapabilityRefusal(null), null);
  assert.equal(clusterCapabilityRefusal({}), null);
});

// ---- the wiring: a helper nothing calls is the same bug as no helper ----

test("the cluster page renders the readiness banner and the per-peer capabilities", () => {
  assert.match(clusterSrc, /renderClusterCapabilityReadiness\(data\);/, "renderClusterData must call the banner");
  assert.match(clusterSrc, /peerCapabilitiesLine\(peer, data\)/, "each node card must carry the peer's capabilities");

  const clusterHtml = fs.readFileSync(path.join(STATIC, "cluster.html"), "utf8");
  assert.match(clusterHtml, /id="clusterCapabilityReadiness"/, "the banner needs somewhere to render");
});

test("the security page gates the two create controls and routes their 409s", () => {
  const securityHtml = fs.readFileSync(path.join(STATIC, "security.html"), "utf8");
  assert.match(securityHtml, /id="btnCreateGroup"/, "the group control has to be addressable to be disabled");
  assert.match(securityHtml, /id="btnCreateToken"/);
  assert.match(securityHtml, /id="groupsCapabilityGate"/);
  assert.match(securityHtml, /id="tokensCapabilityGate"/);

  for (const fn of ["saveGroup", "deleteGroup", "createApiToken", "deleteApiToken"]) {
    assert.match(
      extractFn(securitySrc, fn),
      /clusterCapabilityRefusal/,
      fn + " must render the 409 as an explanation rather than a bare toast"
    );
    // The refusal IS new information about the cluster - a peer fell behind since the last poll - so all four
    // have to refresh the gate, not just the two that did. Asserted per function rather than in aggregate
    // because the asymmetry is exactly what slipped through the first time (PR #7939 review).
    assert.match(
      extractFn(securitySrc, fn),
      /refreshSecurityClusterReadiness\(\)/,
      fn + " must bring the gate up to date with what the 409 just revealed"
    );
  }

  assert.match(extractFn(securitySrc, "initSecurity"), /refreshSecurityClusterReadiness/,
    "opening the panel has to re-ask: a rolling upgrade finishes while Studio is open");
});

// A refresh can fail for reasons that say nothing about the cluster - a blip, a proxy, a leader election. If
// the failure cleared the last answer the gate would report "nothing to gate" and re-enable a control the
// leader is still refusing, which is the failure the gate exists to prevent.
test("a failed readiness refresh keeps the last answer instead of opening the gate", () => {
  const refresh = extractFn(securitySrc, "refreshSecurityClusterReadiness");
  const failHandler = refresh.slice(refresh.indexOf(".fail("), refresh.indexOf(".always("));

  assert.ok(
    !/securityClusterStatus\s*=/.test(failHandler),
    "the fail handler must not reassign securityClusterStatus: clearing it silently re-enables the controls"
  );
  assert.ok(!/globalNotify/.test(failHandler), "a standalone server answers 404 here; that is not an error to show");

  // And an answer that was never obtained still gates nothing, which is what keeps a standalone server silent.
  assert.deepEqual(clusterSecurityCapabilityGaps(null), []);
});

test("the group modal carries the gate too, because Edit is reachable while Create is disabled", () => {
  // An EDIT is replicated by the very same entry a create is, and it is opened from the table rather than from
  // the disabled Create Group button - so gating only the button would leave one door open.
  const securityHtml = fs.readFileSync(path.join(STATIC, "security.html"), "utf8");
  assert.match(securityHtml, /id="groupModalCapabilityGate"/);

  const gate = extractFn(securitySrc, "applyGroupModalCapabilityGate");
  assert.match(gate, /groupModalSaveBtn/, "the Save button is what actually submits the change");
  assert.match(gate, /security-groups-entry/);

  for (const fn of ["showCreateGroupForm", "editGroup"])
    assert.match(extractFn(securitySrc, fn), /applyGroupModalCapabilityGate\(\)/, fn + " must apply the gate");
});
