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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6965: two counter documents on the SAME data page, each written by exactly ONE node
 * (the leader increments only its own document, the replica only its own), must never lose an increment.
 * <p>
 * Before the fix, both nodes validated their transaction against the same base page version and shipped a delta
 * stamped with the same next version. Raft ordered the two entries, and every node applied the second one through
 * the equal-version repair path of {@code TransactionManager.applyChanges}, splicing the second delta over the
 * first: the losing writer read back a value SMALLER than what its own acknowledged commit had written, with no
 * exception raised for either writer. The cluster could even end up with different bytes on different nodes,
 * depending on which side won the race locally.
 * <p>
 * After the fix the leader validates every entry against the versions already assigned in the Raft log, at the
 * point that decides the log order, and rejects the second writer with a retryable
 * {@link com.arcadedb.exception.ConcurrentModificationException}: the retry re-reads the fresh page and succeeds.
 * <p>
 * The type is created with a single bucket so the two tiny documents provably share a page, and the writers use
 * plain transactions (no explicit lock) because the collision is cross-node: node-local locking cannot prevent it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6965SharedPageCrossNodeWritersTest extends BaseRaftHATest {
  private static final String TYPE_NAME  = "Issue6965Counter";
  private static final int    INCREMENTS = 1000;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void singleWriterPerDocumentMustNotLoseIncrementsWhenDocumentsSharePage() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    // One bucket on purpose: both documents land on the same data page.
    leaderDb.transaction(() -> {
      final Schema schema = leaderDb.getSchema();
      final DocumentType type = schema.buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.getOrCreateProperty("name", Type.STRING);
      type.getOrCreateProperty("value", Type.LONG);
      schema.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "name");
    });
    leaderDb.transaction(() -> {
      leaderDb.newDocument(TYPE_NAME).set("name", "leader").set("value", 0L).save();
      leaderDb.newDocument(TYPE_NAME).set("name", "replica").set("value", 0L).save();
    });

    waitForReplicationIsCompleted(replicaIndex);
    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    awaitValue(1L, () -> replicaDb.lookupByKey(TYPE_NAME, "name", "replica").hasNext() ? 1L : 0L);

    final AtomicLong surfacedConflicts = new AtomicLong();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      final Future<String> leaderOutcome = pool.submit(() -> incrementLoop(leaderDb, "leader", start, surfacedConflicts));
      final Future<String> replicaOutcome = pool.submit(() -> incrementLoop(replicaDb, "replica", start, surfacedConflicts));
      start.countDown();

      final String leaderResult = leaderOutcome.get(240, TimeUnit.SECONDS);
      final String replicaResult = replicaOutcome.get(240, TimeUnit.SECONDS);

      assertThat(leaderResult).as("the leader-side single writer must never observe its own increments undone").isNull();
      assertThat(replicaResult).as("the replica-side single writer must never observe its own increments undone").isNull();
      // Both writers share one page, so the leader validates a fair share of the entries against a version the log
      // has moved past: those must surface as retryable conflicts rather than be merged.
      assertThat(surfacedConflicts.get()).as("cross-node collisions on the shared page must surface as retryable conflicts")
          .isGreaterThan(0L);
    } finally {
      pool.shutdownNow();
    }

    // Every node must hold exactly the increments each writer was acknowledged for.
    waitForReplicationIsCompleted(replicaIndex);
    for (final int serverIndex : new int[] { leaderIndex, replicaIndex }) {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      awaitValue(INCREMENTS, () -> counterValue(db, "leader"));
      awaitValue(INCREMENTS, () -> counterValue(db, "replica"));
      assertThat(counterValue(db, "leader")).as("leader counter on server %d", serverIndex).isEqualTo(INCREMENTS);
      assertThat(counterValue(db, "replica")).as("replica counter on server %d", serverIndex).isEqualTo(INCREMENTS);
    }
    assertClusterConsistency();
  }

  private static long counterValue(final Database db, final String name) {
    final Document doc = db.lookupByKey(TYPE_NAME, "name", name).next().getRecord().asDocument();
    return doc.getLong("value");
  }

  /**
   * The single writer of one document: load-modify-save, retrying every surfaced conflict the way any caller
   * would. Returns {@code null} on success, or a description of the silent loss: the base value it read back did
   * not match the value its own previous, acknowledged commit wrote.
   */
  private static String incrementLoop(final Database database, final String documentName, final CountDownLatch start,
      final AtomicLong surfacedConflicts) throws Exception {
    start.await();
    long expected = 0;
    for (int i = 0; i < INCREMENTS; i++) {
      final long[] observed = new long[1];
      for (int attempt = 0; ; attempt++) {
        try {
          database.transaction(() -> {
            final MutableDocument counter = database.lookupByKey(TYPE_NAME, "name", documentName).next().getRecord()
                .asDocument().modify();
            observed[0] = counter.getLong("value");
            counter.set("value", observed[0] + 1);
            counter.save();
          }, false, 0);
          break;
        } catch (final NeedRetryException e) {
          // A surfaced conflict is CORRECT behaviour: retry like any caller.
          surfacedConflicts.incrementAndGet();
          if (attempt >= 10_000)
            throw new IllegalStateException("Retry budget exhausted for " + documentName, e);
        }
      }
      if (observed[0] != expected)
        return "writer of '" + documentName + "' committed value " + expected + " but read back " + observed[0]
            + " on iteration " + i + " - its own acknowledged increment was silently undone (no exception was raised)";
      expected = observed[0] + 1;
    }
    return null;
  }
}
