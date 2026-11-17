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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12367: the BM25 corpus counters of a full-text index counted 40% more documents than the type held on the leader of a
 * cluster under concurrent writers, while the followers counted exactly. Concurrent writers on every node; the raw counters of
 * every node are compared with its live document count BEFORE any search, which would repair them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12367CorpusCountersUnderConcurrentWritersIT extends BaseRaftHATest {
  private static final String INDEX = "Chunk[text]";

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

  @Test
  void theCountersOfEveryNodeCountTheDocumentsOfTheType() throws Exception {
    final int leaderIndex = requireLeaderIndex();
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    leader.command("sql", "CREATE DOCUMENT TYPE Chunk");
    leader.command("sql", "CREATE PROPERTY Chunk.text STRING");
    leader.command("sql", "CREATE INDEX ON Chunk (text) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(leaderRaft::canStateFullTextCorpusDeltas);

    final AtomicInteger failures = new AtomicInteger();
    final List<Thread> threads = new ArrayList<>();
    for (int s = 0; s < getServerCount(); s++)
      for (int t = 0; t < 3; t++) {
        final Database db = getServerDatabase(s, getDatabaseName());
        final int server = s;
        final int thread = t;
        final Thread worker = new Thread(() -> {
          for (int i = 0; i < 60; i++) {
            final int n = i;
            try {
              db.transaction(() -> {
                for (int c = 0; c < 3; c++) {
                  final MutableDocument d = db.newDocument("Chunk")
                      .set("text", "server " + server + " thread " + thread + " doc " + n + " chunk " + c + " collateral account").save();
                  d.set("embedding", new float[] { n, c }).save();
                }
              }, true, 20);
            } catch (final Exception e) {
              failures.incrementAndGet();
            }
          }
        });
        threads.add(worker);
        worker.start();
      }
    for (final Thread t : threads)
      t.join();

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final StringBuilder report = new StringBuilder("failures=" + failures.get());
    for (int i = 0; i < getServerCount(); i++)
      report.append("\nserver ").append(i).append(i == leaderIndex ? " (leader)" : "").append(": totalDocs=")
          .append(metadata(i).getTotalDocs()).append(" live=").append(getServerDatabase(i, getDatabaseName()).countType("Chunk", true));

    for (int i = 0; i < getServerCount(); i++)
      assertThat(metadata(i).getTotalDocs()).as(report.toString())
          .isEqualTo(getServerDatabase(i, getDatabaseName()).countType("Chunk", true));
  }
}
