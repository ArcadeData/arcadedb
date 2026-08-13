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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issue #6136 (4): how many transaction levels {@code CHECK DATABASE ... FIX} consumes, pinned deliberately.
 * <p>
 * {@code DatabaseContext.DatabaseContextTL.pushTransaction} refuses once {@code transactions.size() + 1 > maxNested},
 * and {@code maxNested} is a hardcoded 3 with no configuration key and no caller anywhere in the codebase. The issue
 * reported the FIX path as sitting exactly on that cap - the HTTP handler's transaction, the arm's own, and the
 * repair batch's - which would leave no headroom at all.
 * <p>
 * IT DOES NOT, and the reason is worth recording rather than rediscovering. {@code CheckDatabaseStatement.executeSimple}
 * opens by rolling the caller's transaction back, so the handler's level is released before the check starts; the
 * arms' {@code begin()} then finds the last context inactive and REUSES it rather than pushing
 * ({@code LocalDatabase.begin} only pushes when the last transaction is active); and
 * {@code commitRepairBatchIfFull()} commits and re-begins at that same level rather than nesting under it. The path
 * runs at ONE level with two to spare.
 * <p>
 * The conclusion #6131 drew from the wrong premise still holds and is the one that matters: these are real
 * {@code commit()} calls on a real {@code TransactionContext}, so each is a genuine WAL write and, under HA, a
 * genuine replication round trip. Nesting would not have made them savepoints either - it was never the mechanism.
 * <p>
 * Pinned both ways so that a future change adding a level anywhere on this path arrives as a red build naming the
 * cap, rather than as a {@code TransactionException} in a repair someone is running against a damaged production
 * database - whose message ("Check your code if you are beginning new transactions without closing the previous one
 * by mistake") would point at a bug that is not the real one. Raising {@code maxNested} is the wrong answer: the
 * limit is a real guard against leaked transactions, and the useful thing is knowing when this path eats into it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseTransactionNestingTest extends TestHelper {
  private static final String VERTEX_TYPE     = "Node";
  private static final String EDGE_TYPE       = "Link";
  private static final int    DEGREE          = 20;
  /** What {@code DatabaseContext.DatabaseContextTL} initialises {@code maxNested} to. */
  private static final int    DEFAULT_MAX_NESTED = 3;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The fixture injects on-disk corruption on purpose; the post-test check would (correctly) flag it.
    return false;
  }

  /**
   * The deepest nesting the FIX path actually reaches, sampled at every transaction it commits - which is every
   * transaction it opens, since the repair batches, the per-type arms and the reclaim all end in a commit.
   */
  @Test
  void theFixPathRunsAtOneTransactionLevel() {
    createBrokenHub();

    final AtomicInteger deepest = new AtomicInteger();
    final DatabaseInternal db = (DatabaseInternal) database;
    final Callable<Void> sampler = () -> {
      deepest.accumulateAndGet(database.getNestedTransactions(), Math::max);
      return null;
    };

    db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, sampler);
    try {
      // The HTTP handler shape: the command arrives with a transaction already open on the thread.
      database.begin();
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
        assertThat(rs.next().<String>getProperty("operation")).isEqualTo("check database");
      }
    } finally {
      db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, sampler);
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(deepest.get()).as("the FIX path must have committed at least one transaction to sample").isPositive();
    assertThat(deepest.get())
        .as("CHECK DATABASE FIX must stay at one transaction level: it has %d of the %d the context allows, and "
            + "anything that adds a level here spends headroom that has no configuration key to restore",
            deepest.get(), DEFAULT_MAX_NESTED)
        .isEqualTo(1);
  }

  /**
   * The same statement under the tightest cap the mechanism can express. This is the assertion that bites first: a
   * new level anywhere on the path fails here with the cap named, whatever the sampled depth happens to be.
   */
  @Test
  void theFixPathSurvivesTheTightestNestingCap() {
    createBrokenHub();

    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());
    final int previous = context.getMaxNested();
    context.setMaxNested(1);
    try {
      assertThatCode(() -> {
        database.begin();
        try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
          rs.next();
        }
      }).as("CHECK DATABASE FIX must not need more than one transaction level").doesNotThrowAnyException();
    } finally {
      // The context is thread-local and the thread is reused by the rest of the suite: never leave the cap moved.
      context.setMaxNested(previous);
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(previous).as("the hardcoded default this test is measuring headroom against")
        .isEqualTo(DEFAULT_MAX_NESTED);
  }

  /** A hub whose in-edge chain is broken, so FIX has real repair work to do rather than an empty pass. */
  private void createBrokenHub() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> {
      hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity();
      for (int i = 0; i < DEGREE; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, hub[0].asVertex(true));
    });

    final RID[] head = new RID[1];
    database.transaction(() -> head[0] = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk());
    assertThat(head[0]).as("the hub must have an in-edge list to break").isNotNull();
    corruptRecordTypeByte((DatabaseInternal) database, head[0]);
  }
}
