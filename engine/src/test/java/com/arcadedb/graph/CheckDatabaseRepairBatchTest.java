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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6128 (1): the repair a {@code CHECK DATABASE ... FIX} performs for one type used to accumulate in a SINGLE
 * transaction - the whole scan plus every reconnected edge and every deleted record between one {@code begin()} and
 * one {@code commit()}.
 * <p>
 * On an embedded database that is merely a big transaction. On a replicated one it becomes a single Raft log entry:
 * {@code RaftTransactionBroker.replicateTransaction} submits it whole, and {@code RaftGroupCommitter.submitAndWait}
 * hard-rejects anything above {@code min(arcadedb.ha.appendBufferSize, arcadedb.ha.grpcMessageSizeMax)} - 32MB by
 * default - with a {@code ReplicatedEntryTooLargeException}. That is a {@code TransactionException} and NOT a
 * {@code NeedRetryException}, so nothing retries it: a repair large enough to matter ran for hours and then died at
 * the commit, rolling back everything it had fixed for that type. Unlike a schema entry, which #4743 taught to split,
 * a transaction entry has no splitter.
 * <p>
 * The bound is PAGES rather than repaired records, because pages are what the entry actually contains: a count of
 * records says nothing about how many distinct pages they touched, and it is the page images that the WAL carries.
 * <p>
 * Both directions are pinned. A batching run must produce more than one commit AND repair the graph exactly as the
 * single-transaction run did - a batching bug that dropped repairs would otherwise pass the commit-count assertion
 * on its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseRepairBatchTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";
  /**
   * MANY damaged hubs rather than one big one. Reconnecting a single vertex dirties only its own record plus its
   * chunk chain - about three pages however many edges it has - so a one-hub fixture cannot exercise a page budget
   * at all. Widespread damage across many vertices is both what produces real page volume and what the reported
   * case actually looked like.
   */
  private static final int    HUBS        = 40;
  private static final int    DEGREE      = 20;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test injects on-disk corruption on purpose; the post-test check would (correctly) flag it.
    return false;
  }

  /**
   * A repair whose page footprint exceeds the budget must be split across several transactions, and must still
   * rebuild the full adjacency.
   */
  @Test
  void aLargeRepairCommitsInBatches() {
    final List<RID> hubs = createBrokenHubs();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(4);
    final int commits = countCommitsDuring(this::repairVertices);

    assertThat(commits)
        .as("a repair bigger than the page budget must not be one single transaction")
        .isGreaterThan(1);

    assertRepaired(hubs);
  }

  /**
   * The budget disabled restores the historical single-transaction behaviour, so an embedded user who wants
   * all-or-nothing repair semantics can still have them.
   */
  @Test
  void theBatchBudgetCanBeDisabled() {
    final List<RID> hubs = createBrokenHubs();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(0);
    final int commits = countCommitsDuring(this::repairVertices);

    assertThat(commits).as("with the budget disabled the whole type repair is one transaction").isEqualTo(1);

    assertRepaired(hubs);
  }

  /**
   * The vertex arm on its own, which is the unit under test. Deliberately NOT {@code CHECK DATABASE FIX}: that runs
   * several passes, each already committing its own transaction, so a commit count taken across the whole statement
   * could not tell a batched repair from the passes that surround it.
   */
  private void repairVertices() {
    new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);
  }

  /** Counts the transactions that actually wrote WAL while {@code block} ran. */
  private int countCommitsDuring(final Runnable block) {
    final AtomicInteger commits = new AtomicInteger();
    final DatabaseInternal db = (DatabaseInternal) database;
    final java.util.concurrent.Callable<Void> counter = () -> {
      commits.incrementAndGet();
      return null;
    };
    db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
    try {
      block.run();
    } finally {
      db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
    }
    return commits.get();
  }

  /**
   * A hub with {@link #DEGREE} in-edges whose chain is then broken mid-way, so the FIX has to rebuild the whole
   * adjacency from the surviving edge records - the repair shape that produces real page volume.
   */
  private List<RID> createBrokenHubs() {
    // A SMALLER page size, set before the buckets are created: the budget counts PAGES, and at the 64KB default the
    // whole fixture packs into a couple of them, so no assertion about batching could mean anything. 16KB still
    // holds a full 8KB edge-list chunk (the chunk size doubles up to 8192, and a chunk cannot straddle a page).
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);

    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    // A payload per hub, so the hub records themselves span several pages instead of packing into one.
    final String payload = "x".repeat(1_500);

    final List<RID> hubs = new ArrayList<>(HUBS);
    for (int h = 0; h < HUBS; h++) {
      final int hub = h;
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        holder[0] = database.newVertex(VERTEX_TYPE).set("name", "hub" + hub).set("payload", payload).save()
            .getIdentity();
        for (int i = 0; i < DEGREE; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, holder[0].asVertex(true));
      });
      hubs.add(holder[0]);
    }

    // Break every hub's IN chain at its head, so each one has to be rebuilt from the surviving edge records.
    //
    // The head chunk is corrupted IN PLACE rather than deleted, and with many hubs that is not a detail: deleting a
    // chunk FREES its slot, and the repair then allocates the new chunks it builds into those same freed slots, so a
    // hub's dangling head pointer can come to alias a live chunk belonging to another hub. That is an artefact no
    // real corruption produces - damaged bytes do not free a slot - and it makes the repair look broken (measured:
    // 11 of 40 hubs left unrepaired, identically on unmodified code) when what is actually broken is the fixture.
    // Corrupting the record-type byte keeps the slot occupied and unreadable, which is the real shape.
    for (final RID hub : hubs) {
      final RID[] head = new RID[1];
      database.transaction(() -> head[0] = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk());
      assertThat(head[0]).as("every hub must have an in-edge list to break").isNotNull();
      corruptRecordTypeByte((DatabaseInternal) database, head[0]);
    }

    return hubs;
  }

  /** Every hub's adjacency is whole again and a follow-up check is clean. */
  private void assertRepaired(final List<RID> hubs) {
    database.transaction(() -> {
      for (final RID hub : hubs)
        assertThat(hub.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
            .as("hub %s must have its full adjacency back", hub).isEqualTo(DEGREE);
    });

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      assertThat(rs.next().<Long>getProperty("totalCorruptedRecords"))
          .as("the batched repair must leave the database clean").isZero();
    }
  }
}
