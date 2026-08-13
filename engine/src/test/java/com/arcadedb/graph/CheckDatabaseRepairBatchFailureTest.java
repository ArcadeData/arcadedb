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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6128 (1), failure path: what a batched repair leaves behind when one of its batch commits throws.
 * <p>
 * Batching means a repair can now fail with earlier batches already committed - a real change from the single
 * transaction it used to be, and the reason this deserves a test of its own rather than an argument. The failure
 * this simulates is the one the batching exists to avoid in the first place: a single batch that is still too big
 * for the replicated-entry cap, where the commit raises {@code ReplicatedEntryTooLargeException} rather than a
 * retryable exception, from inside the checker's own loop.
 * <p>
 * Two things must hold afterwards, and neither is automatic. The exception has to reach the caller rather than
 * being swallowed into a "repair completed" report, and no transaction may be left open on the thread - the
 * checker opens a fresh one after every batch, so a failure between batches otherwise abandons it and the next
 * user of that thread inherits an unrelated in-flight transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseRepairBatchFailureTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";
  private static final int    HUBS        = 20;
  private static final int    DEGREE      = 20;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // Deliberate on-disk corruption plus a deliberately failed repair; the post-test check would flag both.
    return false;
  }

  @Test
  void aFailedBatchLeavesNoOpenTransactionAndDoesNotReportSuccess() {
    createDamagedGraph();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(1);

    final DatabaseInternal db = (DatabaseInternal) database;
    final AtomicInteger commits = new AtomicInteger();
    // Fails the SECOND batch commit, so the run has genuinely committed one batch before it breaks - the state
    // this test exists to describe. Thrown from the WAL-write callback, which is inside commit().
    final Callable<Void> failSecondCommit = () -> {
      if (commits.incrementAndGet() == 2)
        throw new IllegalStateException("simulated replicated-entry rejection");
      return null;
    };

    db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
    try {
      assertThatThrownBy(() -> new GraphDatabaseChecker(db).checkVertices(VERTEX_TYPE, true, 0))
          .as("a batch that cannot commit must reach the caller, not be reported as a completed repair")
          .isInstanceOf(Exception.class);
    } finally {
      db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
    }

    assertThat(commits.get()).as("the failure must have happened mid-repair, not before it started")
        .isGreaterThanOrEqualTo(2);

    assertThat(database.isTransactionActive())
        .as("no transaction may be left open on the thread after a failed batch")
        .isFalse();

    // And the database is still usable: the next caller of this thread gets a clean slate rather than inheriting
    // an in-flight transaction from the abandoned repair.
    database.transaction(() -> assertThat(database.countType(VERTEX_TYPE, false)).isPositive());
  }

  private void createDamagedGraph() {
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

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

    // In place, not deleted: a freed slot would be reallocated by the repair and alias another hub's live chunk.
    for (final RID hub : hubs) {
      final RID[] head = new RID[1];
      database.transaction(() -> head[0] = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk());
      assertThat(head[0]).isNotNull();
      corruptRecordTypeByte((DatabaseInternal) database, head[0]);
    }
  }
}
