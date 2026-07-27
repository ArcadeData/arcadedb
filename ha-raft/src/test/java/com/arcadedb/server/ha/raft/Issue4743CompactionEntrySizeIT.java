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
 * Regression test for issue #4743 round 4: index-compaction replication used to ship the WHOLE newly
 * compacted index file as ONE synthetic WAL entry inside ONE {@code SCHEMA_ENTRY}, so the Raft entry
 * size grew with the index (21.5MB for the reporter's 517k-key index). Ratis rejects a single log entry
 * above {@code arcadedb.ha.appendBufferSize} with a {@code StateMachineException} whose
 * {@code leaderShouldStepDown()} is {@code true}, so past a certain index size every compaction made the
 * leader step down; the retry toppled the next leader too and the cluster churned elections forever.
 * <p>
 * The append buffer is deliberately shrunk here so a small, fast index reproduces the same condition a
 * huge index reproduces in production. Before the fix this test hangs and then fails replication; with
 * the fix the compaction ships as several ordered entries and every follower ends up with the complete
 * index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4743CompactionEntrySizeIT extends BaseRaftHATest {

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
    // 512KB instead of the 4MB default: the index built below compacts into a file whose synthetic WAL
    // is comfortably above this, exactly as a multi-million-key index is above the default. On the
    // unpatched code this test fails outright - the oversized entry topples the leader and a follower
    // ends up without the type at all.
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "512KB");
    // Only the explicit compact() below may run: a background auto-compaction racing the end-of-test
    // identity check would leave the nodes on different sub-index generations for reasons unrelated to
    // this regression.
    config.setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0); // 0 = automatic compaction disabled
  }

  @Override
  protected void populateDatabase() {
  }

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

    final VertexType v = database.getSchema().buildVertexType().withName("Address").withTotalBuckets(1).create();
    v.createProperty("uid", String.class);
    final String indexName = "Address[uid]";
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Address", "uid");

    // Small transactions: with a 512KB entry cap every individual commit must still fit one entry, so
    // the test isolates the compaction payload rather than the user transactions.
    for (int from = 0; from < TOTAL_RECORDS; from += TX_CHUNK) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(start + TX_CHUNK, TOTAL_RECORDS); i++)
          database.newVertex("Address").set("uid", uid(i)).save();
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
      assertThat(serverDb.countType("Address", false))
          .as("every record must replicate to server %d", serverIndex).isEqualTo(TOTAL_RECORDS);
      assertThat(serverDb.getSchema().getIndexByName(indexName).countEntries())
          .as("server %d must hold the WHOLE index after a split compaction", serverIndex)
          .isEqualTo(TOTAL_RECORDS);
    });

    // Follower index completeness held back this assertion until #5443 fixed both of its causes: the
    // pages an incremental round appends to the already-existing compacted file were never replicated,
    // and a delivery-only chunk of a split schema change triggered a schema reload that detached the
    // compacted sub-index for good. Issue5443SplitCompactionEntryIT covers the split path directly.
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

  @Override
  protected void checkDatabasesAreIdentical() {
    // Compaction replaces the mutable index file, and its name embeds a per-node nanoTime, so the
    // bucket-index names legitimately differ across nodes after a compaction. DatabaseComparator handles
    // that when comparing indexes (it matches them structurally) but not when comparing types:
    // LocalDocumentType.isTheSameAs delegates to TypeIndex.equals, which pairs bucket indexes BY NAME.
    // That is orthogonal to this regression - it reproduces with the stock append buffer, where the
    // compaction entry is not split at all - so the test asserts logical equality itself, in the test
    // body, across all three nodes.
  }

  /**
   * High-entropy key so the compacted index does not compress away: the SCHEMA_ENTRY compresses its
   * embedded WAL, and a repetitive key would shrink the whole compaction below the cap, leaving the
   * split path untested.
   */
  private static String uid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-" + i;
  }

  private long currentTerm(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer().getCurrentTerm();
  }
}
