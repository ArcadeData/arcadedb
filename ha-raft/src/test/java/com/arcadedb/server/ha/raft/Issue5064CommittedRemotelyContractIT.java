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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * #5064: the user-visible contract for a post-quorum local commit failure. When the Raft quorum durably
 * committed the transaction but the leader's LOCAL phase-2 apply fails, the application must receive
 * {@link TransactionCommittedRemotelyException} ("committed cluster-wide, do NOT retry") instead of a
 * generic commit failure - and the identities of records created in the transaction must remain valid (they
 * are the identities the cluster committed), so an application-level retry no longer INSERTS DUPLICATES of
 * already-committed records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5064CommittedRemotelyContractIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "Contract5064";

  @AfterEach
  void disarmHooks() {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
  }

  @Test
  void postQuorumLocalFailureSurfacesTheCommittedRemotelyContract() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE, 1);
    });
    // Baseline vertex: registers the property name in the dictionary so its INTERNAL transaction (fired
    // lazily from save() -> Dictionary.getIdByName) does not consume the single-shot fault below.
    leaderDb.transaction(() -> leaderDb.newVertex(VERTEX_TYPE).set("k", "baseline").save());
    assertClusterConsistency();

    // Arm a single-shot phase-2 fault: replication reaches quorum, then the local apply throws.
    final AtomicBoolean faultFired = new AtomicBoolean(false);
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = dbName -> {
      if (!faultFired.compareAndSet(false, true))
        return;
      LogManager.instance().log(Issue5064CommittedRemotelyContractIT.class, Level.INFO,
          "TEST: injecting phase-2 commit fault for db=%s on leader %d", dbName, leaderIndex);
      throw new ConcurrentModificationException("TEST fault injection: simulated post-quorum local failure (#5064)");
    };

    final MutableVertex held;
    leaderDb.begin();
    held = leaderDb.newVertex(VERTEX_TYPE);
    held.set("k", "committed-remotely");
    held.save();
    final Object identityAtSave = held.getIdentity();
    assertThat(identityAtSave).as("save() assigns the optimistic identity").isNotNull();

    try {
      leaderDb.commit();
      fail("the faulted commit must not report plain success");
    } catch (final TransactionCommittedRemotelyException e) {
      // The contract: distinct type, actionable message.
      assertThat(e.getMessage()).contains("committed cluster-wide").contains("Do NOT retry");
    }

    assertThat(faultFired.get()).as("the phase-2 fault hook must have fired").isTrue();

    // #4940-composition: the user-held record keeps the identity the cluster committed - it must NOT have
    // been reset to provisional (that reset is what turned application retries into duplicate inserts).
    assertThat(held.getIdentity())
        .as("the identity of a remotely-committed record must survive the local failure (#5064)")
        .isEqualTo(identityAtSave);

    // The data is genuinely committed: a post-step-down write drives the log forward (same pattern as
    // Issue4740Phase2ReconcileIT), then every node must serve BOTH vertices - the baseline and the
    // remotely-committed one whose local apply failed.
    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).as("A leader must be elected after the phase-2 step-down").isGreaterThanOrEqualTo(0);
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> newLeaderDb.newVertex(VERTEX_TYPE).set("k", "post-stepdown").save());

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(db.countType(VERTEX_TYPE, true))
          .as("Node %d must hold the baseline, the remotely-committed and the post-stepdown vertices", i)
          .isEqualTo(3L);
    }
  }

  /**
   * The wire-facing half of the contract (#5075 review, round 4): the same post-quorum local failure
   * surfaced through the HTTP command endpoint must map to 409 Conflict with an explicit do-not-retry
   * detail - never a retry-worthy 5xx that would invite clients and load balancers to re-drive the
   * write (the exact duplicate-insert hazard, same rationale as the DuplicatedKeyException 409 of
   * issue #4350). Complements the server-module unit test on the catch mapping
   * (Issue5064CommittedRemotelyHttpStatusTest) by proving the exception reaches that catch RAW
   * through the real Raft commit path and transaction wrapper.
   */
  @Test
  void postQuorumLocalFailureOverHttpReturns409DoNotRetry() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE, 1);
    });
    // Baseline vertex: registers the property name in the dictionary so its INTERNAL transaction does
    // not consume the single-shot fault below (same pattern as the embedded-API test above).
    leaderDb.transaction(() -> leaderDb.newVertex(VERTEX_TYPE).set("k", "baseline").save());
    assertClusterConsistency();

    final AtomicBoolean faultFired = new AtomicBoolean(false);
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = dbName -> {
      if (!faultFired.compareAndSet(false, true))
        return;
      throw new ConcurrentModificationException("TEST fault injection: simulated post-quorum local failure (#5064)");
    };

    final int leaderHttpPort = getServer(leaderIndex).getHttpServer().getPort();
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + leaderHttpPort + "/api/v1/command/" + getDatabaseName()).openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject()
          .put("language", "sql")
          .put("command", "CREATE VERTEX " + VERTEX_TYPE + " SET k = 'over-http'");
      try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
        pw.write(request.toString());
      }

      final int statusCode = connection.getResponseCode();
      final String response = readError(connection);

      assertThat(faultFired.get()).as("the phase-2 fault hook must have fired").isTrue();
      assertThat(statusCode)
          .as("committed-remotely must surface as 409 Conflict over the wire, got %d (body=%s)", statusCode, response)
          .isEqualTo(409);

      final JSONObject json = new JSONObject(response);
      assertThat(json.getString("exception")).isEqualTo(TransactionCommittedRemotelyException.class.getName());
      assertThat(json.getString("error")).isEqualTo("Transaction committed cluster-wide but the local apply failed - do not retry");
      assertThat(json.getString("detail")).contains("committed cluster-wide").contains("Do NOT retry");
    } finally {
      connection.disconnect();
    }

    // The write IS durably committed despite the 409: after the fault-triggered step-down, drive the
    // log forward and verify every node serves both the baseline and the over-http vertex.
    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).as("A leader must be elected after the phase-2 step-down").isGreaterThanOrEqualTo(0);
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> newLeaderDb.newVertex(VERTEX_TYPE).set("k", "post-stepdown").save());

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(db.countType(VERTEX_TYPE, true))
          .as("Node %d must hold the baseline, the over-http and the post-stepdown vertices", i)
          .isEqualTo(3L);
    }
  }
}
