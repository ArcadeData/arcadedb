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
import com.arcadedb.schema.FullTextIndexMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10051 (#10018): while a peer cannot read the extension section that carries the BM25 corpus deltas (a node on an older
 * build during a rolling upgrade), the leader withholds it, because an older build halts on trailing bytes it does not know. The
 * nodes that do not receive the deltas must still converge: their own drift check, run by the first search, repairs their
 * counters. Once every peer advertises the capability the deltas flow again, with no operator step.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051MixedVersionFullTextCountersIT extends BaseRaftHATest {
  private static final String INDEX = "Doc[content]";
  private static final int    DOCS  = 12;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void populateDatabase() {
  }

  private RaftHAServer raftServerOf(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer();
  }

  private FullTextIndexMetadata metadata(final int serverIndex) {
    return LSMTreeFullTextIndex.metadataOf(getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(INDEX));
  }

  private void insert(final Database leader, final int count) {
    leader.transaction(() -> {
      for (int i = 0; i < count; i++)
        leader.newDocument("Doc").set("name", "d" + i).set("content", "alpha beta gamma").save();
    });
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
  }

  @Test
  void aPeerThatCannotReadTheSectionConvergesByItsOwnDriftCheckAndTheDeltasResumeAfterTheUpgrade() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    leader.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY Doc.name STRING");
    leader.command("sql", "CREATE PROPERTY Doc.content STRING");
    leader.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // The replica now behaves like a node whose build predates the section
    raftServerOf(replicaIndex).setAdvertisedCapabilities(Set.of());
    final RaftHAServer leaderRaft = raftServerOf(leaderIndex);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> !leaderRaft.peersMissingCapability(PeerCapabilities.TX_FULLTEXT_CORPUS_DELTAS).isEmpty()
            && !leaderRaft.canStateFullTextCorpusDeltas());

    insert(leader, DOCS);

    assertThat(metadata(leaderIndex).getTotalDocs()).as("the leader counts its own transactions").isEqualTo(DOCS);
    assertThat(metadata(replicaIndex).getTotalDocs()).as("the replica was sent no deltas: its counters lag").isLessThan(DOCS);

    // The first search on the replica runs its drift check: the live count disagrees, the type is rescanned
    getServerDatabase(replicaIndex, getDatabaseName()).query("sql",
        "SELECT FROM Doc WHERE SEARCH_INDEX('" + INDEX + "', 'alpha') = true").close();
    assertThat(metadata(replicaIndex).getTotalDocs()).as("and the replica converged").isEqualTo(DOCS);
    assertThat(metadata(replicaIndex).getSumDocLength()).isEqualTo(DOCS * 3L);

    // The rolling upgrade finishes: no restart, no setting, the deltas resume by themselves
    raftServerOf(replicaIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> leaderRaft.peersMissingCapability(PeerCapabilities.TX_FULLTEXT_CORPUS_DELTAS).isEmpty()
            && leaderRaft.canStateFullTextCorpusDeltas());

    insert(leader, DOCS);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertThat(metadata(replicaIndex).getTotalDocs()).as("the replica follows the leader again, with no search").isEqualTo(2L * DOCS);
      assertThat(metadata(replicaIndex).getSumDocLength()).isEqualTo(2L * DOCS * 3);
    });
    assertThat(metadata(leaderIndex).getTotalDocs()).isEqualTo(2L * DOCS);
  }
}
