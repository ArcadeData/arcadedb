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
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10051 (#10018): on an HA cluster the BM25 corpus counters of a FULL_TEXT index moved on the node that ran the
 * transaction and nowhere else, so replicas scored the same query with different corpus statistics and a
 * {@code REBUILD INDEX ... WITH statsOnly = true} on the leader never reached them. The transaction now carries what it did to
 * the counters and a recompute is shipped with the schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051FullTextCorpusCountersIT extends BaseRaftHATest {
  private static final String INDEX = "Doc[content]";
  private static final int    DOCS  = 30;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  private FullTextIndexMetadata metadata(final int serverIndex) {
    return LSMTreeFullTextIndex.metadataOf(getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(INDEX));
  }

  private Map<String, Float> scores(final int serverIndex) {
    final Map<String, Float> result = new HashMap<>();
    try (final ResultSet rs = getServerDatabase(serverIndex, getDatabaseName()).query("sql",
        "SELECT name, $score FROM Doc WHERE SEARCH_INDEX('" + INDEX + "', 'alpha beta') = true")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.put(row.getProperty("name"), ((Number) row.getProperty("$score")).floatValue());
      }
    }
    return result;
  }

  @Test
  void everyNodeMovesTheCountersWithTheTransactionsAndALeaderRecomputeReachesThem() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    leader.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 3");
    leader.command("sql", "CREATE PROPERTY Doc.name STRING");
    leader.command("sql", "CREATE PROPERTY Doc.content STRING");
    leader.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // The deltas travel only once every peer has said it can read them: wait for that answer, or the first inserts ride without
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(leaderRaft::canStateFullTextCorpusDeltas);

    for (int batch = 0; batch < 3; batch++) {
      final int first = batch * (DOCS / 3);
      leader.transaction(() -> {
        for (int i = 0; i < DOCS / 3; i++)
          leader.newDocument("Doc").set("name", "d" + (first + i)).set("content", "alpha beta gamma " + "delta ".repeat(i % 4)).save();
      });
    }

    // A rolled-back insert must not move any node's counters
    leader.begin();
    for (int i = 0; i < 5; i++)
      leader.newDocument("Doc").set("name", "rolledback" + i).set("content", "alpha beta").save();
    leader.rollback();

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Read BEFORE any search: a search runs the drift check, which would repair a lagging counter and hide the defect.
    // Each batch indexed documents of 3 + (i % 4) tokens
    long expectedLength = 0;
    for (int batch = 0; batch < 3; batch++)
      for (int i = 0; i < DOCS / 3; i++)
        expectedLength += 3 + (i % 4);
    final long leaderDocs = metadata(leaderIndex).getTotalDocs();
    final long leaderLength = metadata(leaderIndex).getSumDocLength();
    assertThat(leaderDocs).as("the leader counts the %d committed documents, not the rolled-back ones", DOCS).isEqualTo(DOCS);
    assertThat(leaderLength).isEqualTo(expectedLength);

    testEachServer(serverIndex -> {
      assertThat(metadata(serverIndex).getTotalDocs()).as("totalDocs on server %d", serverIndex).isEqualTo(leaderDocs);
      assertThat(metadata(serverIndex).getSumDocLength()).as("sumDocLength on server %d", serverIndex).isEqualTo(leaderLength);
    });

    // The same query ranks the same on every node
    final Map<String, Float> expected = scores(leaderIndex);
    assertThat(expected).hasSize(DOCS);
    testEachServer(serverIndex -> assertThat(scores(serverIndex)).as("scores on server %d", serverIndex).isEqualTo(expected));

    // A recompute on the leader reaches nodes whose counters have drifted
    int follower = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        follower = i;
        break;
      }
    metadata(follower).applyCorpusDelta(7, 70);
    metadata(leaderIndex).applyCorpusDelta(3, 30);

    leader.command("sql", "REBUILD INDEX `" + INDEX + "` WITH statsOnly = true");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          assertThat(metadata(serverIndex).getTotalDocs()).as("totalDocs on server %d after the recompute", serverIndex)
              .isEqualTo(leaderDocs);
          assertThat(metadata(serverIndex).getSumDocLength()).as("sumDocLength on server %d after the recompute", serverIndex)
              .isEqualTo(leaderLength);
        }));

    // The same recompute run inside a transaction the caller opened: the schema save waits for the end of that transaction
    final long epochBefore = metadata(follower).getCountersEpoch();
    metadata(follower).applyCorpusDelta(2, 20);
    leader.transaction(() -> leader.command("sql", "REBUILD INDEX `" + INDEX + "` WITH statsOnly = true"));
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final int driftedFollower = follower;
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertThat(metadata(driftedFollower).getCountersEpoch()).isGreaterThan(epochBefore);
      assertThat(metadata(driftedFollower).getTotalDocs()).isEqualTo(leaderDocs);
      assertThat(metadata(driftedFollower).getSumDocLength()).isEqualTo(leaderLength);
    });

    // A rewrite committed inside a schema session travels in the SCHEMA_ENTRY, which carries no corpus deltas: it keeps the number of
    // documents and changes only their token-length total, which a count cannot tell, so the followers must adopt the leader's counters
    final long lengthBeforeRewrite = metadata(leaderIndex).getSumDocLength();
    // two transactions of ONE session: each marks the counters, and the schema serialized when the session ends carries the lot
    leader.getSchema().bulkChange(() -> {
      for (int rewrite = 0; rewrite < 2; rewrite++) {
        final String name = "d" + rewrite;
        leader.transaction(() -> {
          try (final ResultSet rs = leader.query("sql", "SELECT FROM Doc WHERE name = '" + name + "'")) {
            rs.next().getRecord().get().asDocument().modify().set("content", "alpha beta gamma delta epsilon zeta eta theta").save();
          }
        });
      }
    });
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final long leaderLengthAfter = metadata(leaderIndex).getSumDocLength();
    assertThat(leaderLengthAfter).as("the leader counted the rewrite").isNotEqualTo(lengthBeforeRewrite);
    assertThat(metadata(leaderIndex).getTotalDocs()).isEqualTo(leaderDocs);
    testEachServer(serverIndex -> Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          assertThat(metadata(serverIndex).getTotalDocs()).as("totalDocs on server %d after the rewrite", serverIndex)
              .isEqualTo(leaderDocs);
          assertThat(metadata(serverIndex).getSumDocLength()).as("sumDocLength on server %d after the rewrite", serverIndex)
              .isEqualTo(leaderLengthAfter);
        }));
  }
}
