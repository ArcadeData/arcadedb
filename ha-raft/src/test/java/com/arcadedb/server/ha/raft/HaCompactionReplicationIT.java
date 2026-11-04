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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How an LSM index compaction whose schema change does not fit ONE Raft entry reaches the followers, end to end on ONE
 * three-node cluster shared by every test ({@link SharedClusterRaftHATest}). Merged from
 * {@code Issue4743CompactionEntrySizeIT} and {@code Issue5443SplitCompactionEntryIT}, which ran on the same fixture:
 * three servers, a 30 s quorum timeout, automatic compaction disabled (only the explicit {@code compact()} of a test
 * runs) and {@code arcadedb.ha.appendBufferSize} shrunk to 512KB, so a small, fast index compacts into a payload larger
 * than one entry exactly as a multi-million-key index does at the stock ceiling.
 * <ul>
 * <li><b>#4743</b>: compaction replication used to ship the whole compacted file as ONE synthetic WAL entry inside ONE
 * {@code SCHEMA_ENTRY}. Ratis rejects an entry above the append buffer with a {@code StateMachineException} that makes the
 * leader step down, the retry toppled the next leader too, and the cluster churned elections forever. The compaction now
 * ships as several ordered entries, the term does not move, and every follower ends with the complete index.</li>
 * <li><b>#5443</b>, split variant: the first chunk carries {@code filesToAdd} and no schema JSON, so a follower reloaded
 * its schema from a half-delivered state, could not resolve the compacted sub-index and detached it for good, serving only
 * its mutable pages. The single-entry variant is {@link HaCompactionReplicationSingleEntryIT}.</li>
 * </ul>
 * Why these share a cluster: each test fills, indexes and compacts its own type named through
 * {@link #uniqueName(String)}, so the absolute record and index-entry counts they assert are of a type no other test
 * touches. Both compactions are the first round of their own index, which is incremental, so neither retires a file of
 * another test's index. The log assertions of #4743 read a logger installed for that test only, and its term check is
 * against the term captured at its start. No test stops, restarts or isolates a node, moves leadership or changes a
 * setting.
 * <p>
 * Siblings, each with the fixture its test needs: {@link HaCompactionReplicationSingleEntryIT} (#5443, stock append
 * buffer), {@link HaCompactionReplicationStockConfigIT} (#4743's large record, no configuration override at all),
 * {@link HaCompactionReplicationBloomFilterIT} (#5517, RAM-bounded incremental rounds) and
 * {@link HaCompactionReplicationBloomFilterFullIT} (#5517, a full round).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaCompactionReplicationIT extends SharedClusterRaftHATest {

  /** #4743 and #5443. */
  private static final int TOTAL_RECORDS = 60_000;
  private static final int TX_CHUNK      = 250;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
    // 512KB instead of the default: the index built by each test compacts into a file whose synthetic WAL is comfortably
    // above this, exactly as a multi-million-key index is above the default, so the compaction's schema change is SPLIT
    // across many Raft entries.
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "512KB");
    // Only the explicit compact() of a test may run: a background auto-compaction racing an assertion would leave the
    // nodes on different sub-index generations for reasons unrelated to these regressions.
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0); // 0 = automatic compaction disabled
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Compaction replaces the mutable index file, and its name embeds a per-node nanoTime, so the bucket-index names
    // legitimately differ across nodes after a compaction. DatabaseComparator handles that when comparing indexes (it
    // matches them structurally) but not when comparing types: LocalDocumentType.isTheSameAs delegates to
    // TypeIndex.equals, which pairs bucket indexes BY NAME. Each test asserts logical equality itself, in its body,
    // across all three nodes.
  }

  // ------------------------------------------------------------------ oversized compaction entry (#4743)

  /**
   * Issue #4743, from {@code Issue4743CompactionEntrySizeIT}.
   * <p>
   * Before the fix this test hangs and then fails replication - the oversized entry topples the leader and a follower ends
   * up without the type at all; with the fix the compaction ships as several ordered entries and every follower ends up
   * with the complete index.
   */
  @Test
  @Tag("slow")
  void compactionOfALargeIndexReplicatesWithoutTopplingTheLeader() throws Exception {
    final CapturingTestLogger log = CapturingTestLogger.install();
    try {
      runCompactionScenario(log);
    } finally {
      log.uninstall();
    }
  }

  private void runCompactionScenario(final CapturingTestLogger log) throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final long termBefore = currentTerm(leaderIndex);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());

    final String typeName = uniqueName("Address");
    final VertexType v = database.getSchema().buildVertexType().withName(typeName).withTotalBuckets(1).create();
    v.createProperty("uid", String.class);
    final String indexName = typeName + "[uid]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, typeName, "uid");

    // Small transactions: with a 512KB entry cap every individual commit must still fit one entry, so
    // the test isolates the compaction payload rather than the user transactions.
    for (int from = 0; from < TOTAL_RECORDS; from += TX_CHUNK) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(start + TX_CHUNK, TOTAL_RECORDS); i++)
          database.newVertex(typeName).set("uid", uid(i)).save();
      });
    }

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    index.scheduleCompaction();
    index.compact();

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Every record replicated, and the compacted index is live and queryable on every node.
    testEachServer(serverIndex -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(serverDb.countType(typeName, false))
          .as("every record must replicate to server %d", serverIndex).isEqualTo(TOTAL_RECORDS);
      assertThat(serverDb.getSchema().getIndexByName(indexName).countEntries())
          .as("server %d must hold the WHOLE index after a split compaction", serverIndex)
          .isEqualTo(TOTAL_RECORDS);
    });

    // Follower index completeness held back this assertion until #5443 fixed both of its causes: the
    // pages an incremental round appends to the already-existing compacted file were never replicated,
    // and a delivery-only chunk of a split schema change triggered a schema reload that detached the
    // compacted sub-index for good. everyNodeHoldsTheWholeIndexAfterASplitCompaction covers the split path directly.
    final Index leaderIdx = getServerDatabase(leaderIndex, getDatabaseName()).getSchema().getIndexByName(indexName);
    assertThat(leaderIdx.countEntries()).as("the leader's index must hold every key").isEqualTo(TOTAL_RECORDS);
    assertThat(leaderIdx.get(new Object[] { uid(12_345) }).hasNext())
        .as("a compacted key must be findable on the leader").isTrue();

    // A compaction must not cost an election. The reporter's log showed the term climbing 54 -> 63 while
    // the same oversized entry was retried; the fix must leave the term where it was.
    assertThat(currentTerm(leaderIndex))
        .as("compaction must not make the leader step down")
        .isEqualTo(termBefore);

    // The split really happened - otherwise this test proves nothing about the chunking.
    assertThat(log.countContaining("does not fit one Raft entry"))
        .as("the compaction payload must exceed one Raft entry for this test to be meaningful")
        .isPositive();

    // The heart of the reporter's cascade: a follower that could not resolve a sub-index while applying
    // the compaction used to try to DROP the index, which is a schema write a replica may not perform.
    // That failure was then retried as if transient and finally escalated the database to a snapshot
    // resync. Whatever a follower makes of a half-applied compaction, it must never get pushed into a
    // schema write from the apply path.
    //
    // NOTE: followers can still log "Invalid sub-index ..." here. That warning is a pre-existing symptom
    // of compaction replication instantiating index components around the arrival of their file content;
    // it is not introduced by the entry splitting (it reproduces on the unpatched code) and closing it is
    // separate work. What this fix guarantees is that the warning stays a warning.
    assertThat(log.countContaining("Changes to the schema must be executed on the leader server"))
        .as("a replica must never be pushed into a schema write while applying an entry")
        .isZero();
    assertThat(log.countContaining("escalating to snapshot resync"))
        .as("a compaction must not escalate a healthy database to a snapshot resync")
        .isZero();
  }

  private long currentTerm(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer().getCurrentTerm();
  }

  // ------------------------------------------------------------------ split compaction, follower completeness (#5443)

  /** Issue #5443, from {@code Issue5443SplitCompactionEntryIT}. */
  @Test
  @Tag("slow")
  void everyNodeHoldsTheWholeIndexAfterASplitCompaction() throws Exception {
    assertEveryNodeHoldsTheWholeIndexAfterACompaction(uniqueName("Address"));
  }

  /**
   * The guarantee of #5443 for the type {@code typeName}, the same scenario as
   * {@link BaseCompactionIndexCompletenessTest#assertEveryNodeHoldsTheWholeIndexAfterACompaction()} on a type of the
   * test's own: fills an indexed type, compacts it once, waits for replication and then requires every node to hold the
   * complete index - by entry count, and by looking up keys spread across the whole range, which is how the
   * incompleteness shows itself to a client.
   */
  private void assertEveryNodeHoldsTheWholeIndexAfterACompaction(final String typeName) throws Exception {
    final String indexName = typeName + "[uid]";
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());
    final VertexType v = database.getSchema().buildVertexType().withName(typeName).withTotalBuckets(1).create();
    v.createProperty("uid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, typeName, "uid");

    for (int from = 0; from < TOTAL_RECORDS; from += TX_CHUNK) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(start + TX_CHUNK, TOTAL_RECORDS); i++)
          database.newVertex(typeName).set("uid", uid(i)).save();
      });
    }

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    index.scheduleCompaction();
    index.compact();

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(serverDb.countType(typeName, false))
          .as("every record must replicate to server %d", serverIndex).isEqualTo(TOTAL_RECORDS);

      final Index serverIdx = serverDb.getSchema().getIndexByName(indexName);
      assertThat(serverIdx.countEntries())
          .as("server %d must hold the WHOLE index after the compaction, not a truncated one", serverIndex)
          .isEqualTo(TOTAL_RECORDS);
    });

    // Spot-check lookups across the whole key range on every node: a short index shows up as specific
    // keys that cannot be found, which is what makes the bug silent for a client.
    testEachServer(serverIndex -> {
      final Index serverIdx = getServerDatabase(serverIndex, getDatabaseName()).getSchema().getIndexByName(indexName);
      for (int i = 0; i < TOTAL_RECORDS; i += TOTAL_RECORDS / 40)
        assertThat(serverIdx.get(new Object[] { uid(i) }).hasNext())
            .as("key %d must be findable on server %d", i, serverIndex).isTrue();
    });
  }

  /**
   * High-entropy key so the compacted index does not compress away: the SCHEMA_ENTRY compresses its embedded WAL, and a
   * repetitive key would shrink the whole compaction below the cap, leaving the split path untested.
   */
  private static String uid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-" + i;
  }
}
