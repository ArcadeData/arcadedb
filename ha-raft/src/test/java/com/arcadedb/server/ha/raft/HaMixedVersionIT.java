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
import com.arcadedb.database.RID;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A rolling upgrade of an HA cluster, where one node still runs a build that cannot read a newer wire or record format, end to
 * end on ONE two-node cluster with an empty fixture database shared by every test ({@link SharedClusterRaftHATest}). Merged
 * from {@code Issue10051MixedVersionFullTextCountersIT} and {@code Issue10856MixedVersionFloatArrayIT}, which ran on the same
 * fixture: two servers, the stock configuration and no populated fixture types. An "old" node is built inside one JVM with
 * {@link RaftHAServer#setAdvertisedCapabilities}: the replica advertises nothing, then {@link PeerCapabilities#LOCAL} again
 * once the "upgrade" finishes.
 * <ul>
 * <li><b>#10051</b> (#10018): while a peer cannot read the extension section that carries the BM25 corpus deltas, the leader
 * withholds it. The nodes that do not receive the deltas still converge by their own drift check, run by the first search; once
 * every peer advertises the capability the deltas flow again, with no operator step.</li>
 * <li><b>#10856</b>: while a peer has not advertised {@link PeerCapabilities#FIXED_WIDTH_FLOAT_ARRAYS} the leader writes a float
 * array in the varint form every build reads, and the fixed-width form once every peer advertises it.</li>
 * </ul>
 * Why these share a cluster: both tests move the replica's advertisement in the SAME direction (nothing, then
 * {@link PeerCapabilities#LOCAL}, which is what {@code RaftHAServer} advertises from boot), and {@link #restoreAdvertisements()}
 * puts {@link PeerCapabilities#LOCAL} back on every node after EVERY test, pass or fail, logging rather than throwing so it can
 * never mask the failure. The verdicts the tests read ({@code canStateFullTextCorpusDeltas}, {@code canWriteFixedWidthFloatArrays})
 * are recomputed from the live advertisements at most a second old, nothing sticky or persisted, so an earlier test's upgrade
 * leaves no format behind: each test first waits until the leader has observed ITS downgrade before it writes. Each test
 * creates its own type (and index) named through {@link #uniqueName(String)}, and every count it asserts is of that type. No test
 * stops, restarts or isolates a node, moves leadership or changes a setting.
 * <p>
 * Siblings, each with the fixture its tests need: {@link HaMixedVersionSchemaDeltaIT} (#7219, the populated fixture database)
 * and {@link HaMixedVersionThreeNodeIT} (#7511, three servers).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaMixedVersionIT extends SharedClusterRaftHATest {
  /** #10051. */
  private static final int DOCS = 12;
  /** #10856. */
  private static final int DIM  = 256;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void populateDatabase() {
  }

  /**
   * Ends the simulated rolling upgrade a failed test left half done, so the next test starts from a uniform cluster. Logs instead
   * of throwing: the failure that matters is the test's own.
   */
  @AfterEach
  void restoreAdvertisements() {
    for (int i = 0; i < getServerCount(); i++)
      try {
        if (getServer(i) != null && getServer(i).isStarted() && getRaftPlugin(i) != null && getRaftPlugin(i).getRaftHAServer() != null)
          raftServerOf(i).setAdvertisedCapabilities(PeerCapabilities.LOCAL);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "TEST: could not restore the advertised capabilities of server %d", e, i);
      }
  }

  private RaftHAServer raftServerOf(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer();
  }

  // ------------------------------------------------------------------ BM25 corpus deltas withheld (#10051)

  /** Issue #10051, from {@code Issue10051MixedVersionFullTextCountersIT}. */
  @Test
  void aPeerThatCannotReadTheSectionConvergesByItsOwnDriftCheckAndTheDeltasResumeAfterTheUpgrade() {
    final String typeName = uniqueName("Doc");
    final String indexName = typeName + "[content]";
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    leader.command("sql", "CREATE DOCUMENT TYPE " + typeName + " BUCKETS 2");
    leader.command("sql", "CREATE PROPERTY " + typeName + ".name STRING");
    leader.command("sql", "CREATE PROPERTY " + typeName + ".content STRING");
    leader.command("sql", "CREATE INDEX ON " + typeName + " (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // The replica now behaves like a node whose build predates the section
    raftServerOf(replicaIndex).setAdvertisedCapabilities(Set.of());
    final RaftHAServer leaderRaft = raftServerOf(leaderIndex);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> !leaderRaft.peersMissingCapability(PeerCapabilities.TX_FULLTEXT_CORPUS_DELTAS).isEmpty()
            && !leaderRaft.canStateFullTextCorpusDeltas());

    insert(leader, typeName, DOCS);

    assertThat(metadata(leaderIndex, indexName).getTotalDocs()).as("the leader counts its own transactions").isEqualTo(DOCS);
    assertThat(metadata(replicaIndex, indexName).getTotalDocs()).as("the replica was sent no deltas: its counters lag").isLessThan(DOCS);

    // The first search on the replica runs its drift check: the live count disagrees, the type is rescanned
    getServerDatabase(replicaIndex, getDatabaseName()).query("sql",
        "SELECT FROM " + typeName + " WHERE SEARCH_INDEX('" + indexName + "', 'alpha') = true").close();
    assertThat(metadata(replicaIndex, indexName).getTotalDocs()).as("and the replica converged").isEqualTo(DOCS);
    assertThat(metadata(replicaIndex, indexName).getSumDocLength()).isEqualTo(DOCS * 3L);

    // The rolling upgrade finishes: no restart, no setting, the deltas resume by themselves
    raftServerOf(replicaIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> leaderRaft.peersMissingCapability(PeerCapabilities.TX_FULLTEXT_CORPUS_DELTAS).isEmpty()
            && leaderRaft.canStateFullTextCorpusDeltas());

    insert(leader, typeName, DOCS);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertThat(metadata(replicaIndex, indexName).getTotalDocs()).as("the replica follows the leader again, with no search")
          .isEqualTo(2L * DOCS);
      assertThat(metadata(replicaIndex, indexName).getSumDocLength()).isEqualTo(2L * DOCS * 3);
    });
    assertThat(metadata(leaderIndex, indexName).getTotalDocs()).isEqualTo(2L * DOCS);
  }

  private FullTextIndexMetadata metadata(final int serverIndex, final String indexName) {
    return LSMTreeFullTextIndex.metadataOf(getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(indexName));
  }

  private void insert(final Database leader, final String typeName, final int count) {
    leader.transaction(() -> {
      for (int i = 0; i < count; i++)
        leader.newDocument(typeName).set("name", "d" + i).set("content", "alpha beta gamma").save();
    });
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
  }

  // ------------------------------------------------------------------ fixed-width float arrays (#10856)

  /** Issue #10856, from {@code Issue10856MixedVersionFloatArrayIT}. */
  @Test
  void theVarintFormIsWrittenUntilEveryPeerReadsTheFixedOne() {
    final String typeName = uniqueName("Doc");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    leader.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    leader.command("sql", "CREATE PROPERTY " + typeName + ".embedding ARRAY_OF_FLOATS");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // The replica now behaves like a node whose build predates the fixed-width form
    raftServerOf(replicaIndex).setAdvertisedCapabilities(Set.of());
    final RaftHAServer leaderRaft = raftServerOf(leaderIndex);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> !leaderRaft.peersMissingCapability(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS).isEmpty()
            && !leaderRaft.canWriteFixedWidthFloatArrays());

    final float[] before = vector(1);
    final RID varint = insert(leader, typeName, before);
    assertThat(storedSize(replicaIndex, varint)).as("the varint form: 5 bytes per float").isGreaterThanOrEqualTo(5 * DIM);
    assertThat(readBack(replicaIndex, varint)).containsExactly(before);

    // The rolling upgrade finishes: no restart, no setting
    raftServerOf(replicaIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> leaderRaft.peersMissingCapability(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS).isEmpty()
            && leaderRaft.canWriteFixedWidthFloatArrays());

    final float[] after = vector(2);
    final RID fixed = insert(leader, typeName, after);
    assertThat(storedSize(replicaIndex, fixed)).as("the fixed-width form: 4 bytes per float").isLessThan(4 * DIM + 64);
    assertThat(readBack(replicaIndex, fixed)).containsExactly(after);
    assertThat(readBack(replicaIndex, varint)).as("the record written before is still read").containsExactly(before);
  }

  private RID insert(final Database leader, final String typeName, final float[] vector) {
    final AtomicReference<RID> rid = new AtomicReference<>();
    leader.transaction(() -> rid.set(leader.newDocument(typeName).set("embedding", vector).save().getIdentity()));
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
    return rid.get();
  }

  private int storedSize(final int serverIndex, final RID rid) {
    final Database database = getServerDatabase(serverIndex, getDatabaseName());
    return database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid).size();
  }

  private float[] readBack(final int serverIndex, final RID rid) {
    return (float[]) getServerDatabase(serverIndex, getDatabaseName()).lookupByRID(rid, true).asDocument().get("embedding");
  }

  private static float[] vector(final int seed) {
    final float[] vector = new float[DIM];
    for (int i = 0; i < DIM; i++)
      vector[i] = (float) Math.sin(seed * 31 + i) / 16;
    return vector;
  }
}
