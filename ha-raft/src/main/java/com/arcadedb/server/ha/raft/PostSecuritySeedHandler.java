/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;

import io.undertow.server.HttpServerExchange;

import java.util.List;
import java.util.logging.Level;

/**
 * {@code POST /api/v1/cluster/security-seed}: asks the <b>leader</b> to replicate the cluster security documents
 * and answers with what it could not commit.
 * <p>
 * One endpoint serves the two halves of what "who seeds the security documents" turned out to be:
 * <ul>
 * <li><b>the admission report</b> (issue #7834). {@code POST /api/v1/cluster/peer} and {@code connect cluster}
 * used to run a seed of their own on the admitting node, so an admission put up to six entries in the Raft log
 * from two different JVMs - each holding only its own {@code ServerSecurity} monitor, which is the monitor that
 * exists to stop a revocation being undone by the whole document a seed carries (issue #7373). They now call
 * this instead: the leader's {@link MembershipSecuritySeeder} is the single seeder, and the {@code failedSeeds}
 * array issue #7521 made operator-facing comes back from it.</li>
 * <li><b>the re-seed of a node that came back</b> (issue #7833). A pod that restarts while it is still a Raft
 * member issues no configuration change, so no membership hook fires and nothing seeds it. If the entries it
 * missed are still in the leader's log it converges by catch-up; if the log was purged and it catches up by
 * SNAPSHOT INSTALL it does not, because the three documents live under {@code <server-root>/config/} and no
 * snapshot carries them. Such a node calls this with the fingerprints of the documents it holds.</li>
 * </ul>
 *
 * <h2>The fingerprints, and why the common case writes nothing</h2>
 * A re-seed request carries {@code fingerprints}, the caller's own {@code users} / {@code groups} /
 * {@code apiTokens} digests. The leader compares them against its own and, when all three match, answers
 * {@code upToDate} without submitting anything: a rolling restart of a cluster whose security state did not
 * change while the pod was down therefore costs one HTTP round trip per pod and no Raft entries at all. An
 * admission request omits them - the admitting node does not hold the joining peer's documents and has nothing
 * to compare - and is always seeded.
 * <p>
 * The comparison is deliberately against the leader's LIVE documents rather than against its last replicated
 * fingerprints: what the caller must converge on is what the cluster is serving now, and a leader whose live
 * document has drifted from the cluster's should seed that document rather than answer {@code upToDate} against
 * a baseline nobody is serving.
 *
 * <h2>Authentication</h2>
 * Inherited from {@link AbstractServerHttpHandler}: the {@code X-ArcadeDB-Cluster-Token} +
 * {@code X-ArcadeDB-Forwarded-User} pair every peer-to-peer cluster RPC uses, plus the root check below, which
 * that pair satisfies because peers forward as root. Root-only for the obvious reason - this replicates the
 * cluster's credentials - and because the work itself is a Raft round trip an unauthorized caller must not be
 * able to ask for repeatedly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostSecuritySeedHandler extends AbstractServerHttpHandler {

  /** The route this handler is registered at. */
  public static final String ROUTE = "/api/v1/cluster/security-seed";

  private final RaftHAPlugin plugin;

  public PostSecuritySeedHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    // The seed submits Raft entries and waits for them to commit. Never on an IO thread.
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    if (!plugin.isLeader())
      // 409 rather than a forward: the caller resolved a leader address and this node is no longer it, so the
      // right answer is "ask again", not a second hop that would have to carry the same seed semantics.
      return new ExecutionResponse(409, new JSONObject()
          .put("error", "This node is not the Raft leader; the security seed is issued by the leader")
          .put("leader", String.valueOf(raftHAServer.getLeaderId()))
          .toString());

    final ServerSecurity security = httpServer.getServer().getSecurity();
    if (security == null)
      return new ExecutionResponse(503,
          new JSONObject().put("error", "This node has no security store to seed from").toString());

    final String reason = payload.getString("reason", "a peer request");

    final JSONObject fingerprints;
    try {
      // The two-argument form substitutes the default only for an ABSENT or null field; a field that is
      // present and is not an object throws (claude-review on PR #7854). Caught so a garbled internal RPC is
      // answered 400 - "your request is wrong" - rather than 500, which says this node is.
      fingerprints = payload.getJSONObject("fingerprints", null);
    } catch (final RuntimeException e) {
      return new ExecutionResponse(400, new JSONObject()
          .put("error", "'fingerprints' must be an object of document digests: " + e.getMessage()).toString());
    }

    if (fingerprints != null && isUpToDate(security, fingerprints)) {
      LogManager.instance().log(this, Level.FINE,
          "Security seed requested for %s: the caller already holds every document this node does; nothing submitted",
          reason);
      return new ExecutionResponse(200, new JSONObject()
          .put("upToDate", true)
          .put("seeded", false)
          .put("failedSeeds", new JSONArray())
          .toString());
    }

    LogManager.instance().log(this, Level.INFO, "Seeding the cluster security documents, requested for %s", reason);

    final List<String> failedSeeds;
    try {
      failedSeeds = raftHAServer.getStateMachine().seedSecurityNowAndReport(seedReportTimeoutMs());
    } catch (final IllegalStateException e) {
      // The seed could not be run or its outcome could not be read. Reported as a failure of the REPORT, with
      // the documents unnamed, because that is exactly what is known: answering with an empty failedSeeds array
      // would tell the caller that everything committed.
      return new ExecutionResponse(503, new JSONObject()
          .put("error", "The cluster security seed could not be completed: " + e.getMessage())
          .put("seeded", false)
          .toString());
    }

    return new ExecutionResponse(failedSeeds.isEmpty() ? 200 : 503, new JSONObject()
        .put("upToDate", false)
        .put("seeded", true)
        .put("failedSeeds", new JSONArray(failedSeeds))
        .toString());
  }

  /**
   * Whether the caller already holds every document this node holds. All three or none: a partial match still
   * needs a seed, and seeding all three is what {@code ServerSecurity.seedSecurityStateClusterWide} does - one
   * document out of step is an admin-rate event, not something to build a selective path for.
   */
  private static boolean isUpToDate(final ServerSecurity security, final JSONObject fingerprints) {
    // Read with defaults rather than required: a digest that is absent cannot match, which is the answer a
    // caller that sent an incomplete set should get - seed it - and needs no error of its own.
    return security.usersFingerprint().equals(fingerprints.getString(ReplicatedSecurityFingerprintRepository.USERS, ""))
        && security.groupsFingerprint()
        .equals(fingerprints.getString(ReplicatedSecurityFingerprintRepository.GROUPS, ""))
        && security.apiTokensFingerprint()
        .equals(fingerprints.getString(ReplicatedSecurityFingerprintRepository.API_TOKENS, ""));
  }

  /**
   * How long this node waits for the seed before answering. Generously past the seed's own retry budget, so the
   * answer is the seed's outcome rather than a timeout on a seed that was still inside its budget.
   */
  private long seedReportTimeoutMs() {
    return ClusterSecuritySeedQuery.reportTimeoutMs(httpServer.getServer().getConfiguration());
  }
}
