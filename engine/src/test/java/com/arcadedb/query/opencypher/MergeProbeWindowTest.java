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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The window of the existence probe of a MERGE ({@link TransactionContext#beginMergeProbe()}) next to the other windows of a
 * transaction: a record listener suspends it, a reset ends it, and the process-wide count of open probes stays balanced through both.
 */
class MergeProbeWindowTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/MergeProbeWindowTest").create();
    database.getSchema().createVertexType("Hub");
    database.getSchema().createVertexType("Leaf");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Hub {id: 'h'})");
      database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:Leaf {id: 'l' + toString(i)})");
      database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf) CREATE (h)-[:R {k: toInteger(substring(l.id, 1))}]->(l)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
    assertThat(TransactionContext.isAnyMergeProbeOpen()).as("a probe is still counted open").isFalse();
  }

  private TransactionContext tx() {
    return ((DatabaseInternal) database).getTransactionIfExists();
  }

  /** A read listener fired while the probe runs is the application: its reads are recorded at once, the probe's own stay held back. */
  @Test
  void aReadListenerFiredDuringTheProbeIsNotInsideTheProbeWindow() {
    final List<Boolean> excusedInListener = new ArrayList<>();
    final AfterRecordReadListener listener = record -> {
      final TransactionContext tx = ((DatabaseInternal) database).getTransactionIfExists();
      excusedInListener.add(tx != null && tx.isReadsExcused());
      return record;
    };
    database.getSchema().getType("R").getEvents().registerListener(listener);
    try {
      database.begin();
      final TransactionContext tx = tx();
      database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l9'}) CREATE (h)-[:R {k: 5000}]->(l)");
      excusedInListener.clear();
      database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[:R {k: 3}]->(l)");
      assertThat(excusedInListener).as("the probe read relationships, so the listener ran").isNotEmpty();
      assertThat(excusedInListener).doesNotContain(true);
      assertThat(tx.isReadsExcused()).isFalse();
      database.rollback();
    } finally {
      database.getSchema().getType("R").getEvents().unregisterListener(listener);
    }
  }

  /** The window of the probe is suspended with the edge-append one, and given back whole. */
  @Test
  void suspendingTheEdgeAppendWindowSuspendsTheProbeWindowAndResumeGivesItBack() {
    database.begin();
    try {
      final TransactionContext tx = tx();
      assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
      tx.beginMergeProbe();
      tx.beginMergeProbe();
      assertThat(TransactionContext.isAnyMergeProbeOpen()).isTrue();
      assertThat(tx.isReadsExcused()).isTrue();
      final int suspended = tx.suspendEdgeAppend();
      assertThat(suspended).isNotNegative();
      assertThat(tx.isReadsExcused()).isFalse();
      tx.resumeEdgeAppend(suspended);
      assertThat(tx.isReadsExcused()).isTrue();
      tx.endMergeProbe(false);
      assertThat(tx.isReadsExcused()).isTrue();
      tx.endMergeProbe(false);
      assertThat(tx.isReadsExcused()).isFalse();
      assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
    } finally {
      database.rollback();
    }
  }

  /** A nested suspend, with no probe open at that point, must not give back the probe the outer one suspended. */
  @Test
  void aNestedSuspendOfAnAlreadySuspendedProbeGivesBackNothingEarly() {
    database.begin();
    final TransactionContext tx = tx();
    tx.beginMergeProbe();
    final int outer = tx.suspendEdgeAppend();
    final int inner = tx.suspendEdgeAppend();
    tx.resumeEdgeAppend(inner);
    assertThat(tx.isReadsExcused()).as("still inside the outer callback").isFalse();
    tx.resumeEdgeAppend(outer);
    assertThat(tx.isReadsExcused()).isTrue();
    tx.endMergeProbe(false);
    database.rollback();
  }

  /** A transaction reset while the probe is suspended (the resume was skipped, or the transaction ended in the callback) balances the count. */
  @Test
  void aRollbackWhileTheProbeIsSuspendedLeavesNoOpenProbe() {
    database.begin();
    final TransactionContext tx = tx();
    tx.beginMergeProbe();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isTrue();
    tx.suspendEdgeAppend();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).as("a suspended probe is still counted").isTrue();
    database.rollback();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
  }

  /** The same for a probe that was never suspended, and for one opened inside a suspended window. */
  @Test
  void aRollbackWithAnOpenProbeOrAProbeOpenedWhileSuspendedLeavesNoOpenProbe() {
    database.begin();
    TransactionContext tx = tx();
    tx.beginMergeProbe();
    database.rollback();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();

    database.begin();
    tx = tx();
    tx.beginMergeProbe();
    tx.suspendEdgeAppend();
    tx.beginMergeProbe();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isTrue();
    database.rollback();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
  }

  /** A listener that throws while the probe runs: the window is given back, the probe ends with the error, nothing stays counted. */
  @Test
  void aListenerThatThrowsDuringTheProbeLeavesNoOpenProbe() {
    final AfterRecordReadListener listener = record -> {
      throw new IllegalStateException("listener failure");
    };
    database.getSchema().getType("R").getEvents().registerListener(listener);
    try {
      database.begin();
      final TransactionContext tx = tx();
      database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l9'}) CREATE (h)-[:R {k: 5000}]->(l)");
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[:R {k: 3}]->(l)"))
          .hasStackTraceContaining("listener failure");
      assertThat(tx.isReadsExcused()).isFalse();
      assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
      database.rollback();
    } finally {
      database.getSchema().getType("R").getEvents().unregisterListener(listener);
    }
  }

  /** A probe a listener opens while the outer one is suspended decides its own held-back reads, not the outer probe's. */
  @Test
  void anInnerProbeInASuspendedListenerWindowDoesNotDecideTheOuterProbesReads() {
    database.begin();
    final TransactionContext tx = tx();
    final MutableVertex outerRead = database.newVertex("Leaf").set("id", "outer").save();
    final MutableVertex innerRead = database.newVertex("Leaf").set("id", "inner").save();
    tx.beginMergeProbe();
    assertThat(tx.deferProbeHeadFlagRead(outerRead, true)).isTrue();
    final int token = tx.suspendEdgeAppend();
    tx.beginMergeProbe();
    assertThat(tx.deferProbeHeadFlagRead(innerRead, true)).isTrue();
    tx.endMergeProbe(false);
    assertThat(innerRead.isEdgeListHeadRead(true)).as("the inner probe dropped its own read").isFalse();
    assertThat(outerRead.isEdgeListHeadRead(true)).as("the outer probe's read is not decided yet").isFalse();
    tx.resumeEdgeAppend(token);
    assertThat(tx.isReadsExcused()).isTrue();
    tx.endMergeProbe(true);
    assertThat(outerRead.isEdgeListHeadRead(true)).as("the outer probe kept its read").isTrue();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
    database.rollback();
  }

  /** A listener inside a suspended window suspends again: each resume gives back what its own suspend took, and the count ends at zero. */
  @Test
  void aDoubleSuspendWithAProbeOpenedBetweenGivesBackEveryDepthAndLeavesNoOpenProbe() {
    database.begin();
    final TransactionContext tx = tx();
    tx.beginMergeProbe();
    tx.beginMergeProbe();
    final int first = tx.suspendEdgeAppend();
    tx.beginMergeProbe();
    final int second = tx.suspendEdgeAppend();
    assertThat(tx.isReadsExcused()).isFalse();
    tx.resumeEdgeAppend(second);
    assertThat(tx.isReadsExcused()).isTrue();
    tx.endMergeProbe(false);
    assertThat(tx.isReadsExcused()).isFalse();
    tx.resumeEdgeAppend(first);
    assertThat(tx.isReadsExcused()).isTrue();
    tx.endMergeProbe(false);
    assertThat(tx.isReadsExcused()).as("the outer probe had depth 2").isTrue();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isTrue();
    tx.endMergeProbe(false);
    assertThat(tx.isReadsExcused()).isFalse();
    assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
    database.rollback();
  }

  /** A read listener that runs a MERGE of its own inside the probe (re-entrant): the count is back to zero after the statement, a commit and a rollback. */
  @Test
  void aReadListenerThatRunsAMergeInsideTheProbeLeavesNoOpenProbeAfterCommitOrRollback() {
    final AtomicBoolean inside = new AtomicBoolean();
    final AtomicBoolean fired = new AtomicBoolean();
    final AfterRecordReadListener listener = record -> {
      if (inside.compareAndSet(false, true))
        try {
          fired.set(true);
          database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l7'}) MERGE (h)-[:R {k: 7}]->(l)");
        } finally {
          inside.set(false);
        }
      return record;
    };
    database.getSchema().getType("R").getEvents().registerListener(listener);
    try {
      for (final boolean commit : new boolean[] { false, true }) {
        fired.set(false);
        database.begin();
        database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l9'}) CREATE (h)-[:R {k: 5000}]->(l)");
        database.command("opencypher", "MATCH (h:Hub {id: 'h'}), (l:Leaf {id: 'l3'}) MERGE (h)-[:R {k: 3}]->(l)");
        assertThat(fired.get()).as("the listener ran a MERGE inside the probe").isTrue();
        assertThat(tx().isReadsExcused()).isFalse();
        assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
        if (commit)
          database.commit();
        else
          database.rollback();
        assertThat(TransactionContext.isAnyMergeProbeOpen()).isFalse();
      }
    } finally {
      database.getSchema().getType("R").getEvents().unregisterListener(listener);
    }
  }
}
