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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6950: concurrent single-statement
 * {@code UPDATE ... SET value = value + ? RETURN BEFORE} commands on the SAME document silently lost increments.
 * <p>
 * A record's page is put under the commit-time MVCC check when the record is TAKEN for update, not when its content
 * was read; under the default READ_COMMITTED isolation a read caches no page in the transaction, so a commit landing
 * in between handed the pin the newer version and the version check had nothing left to refuse. Both writers were
 * told they had succeeded and one increment vanished, which no retry loop can repair.
 * <p>
 * The two racy methods are the reproducer from the issue: they fail within seconds without the fix. They are
 * {@code @Tag("slow")} because each drives 4,000 contended SQL statements through tens of thousands of retries, which
 * is the shape that lane exists for; the two deterministic ones are not, so the contract stays pinned in the default
 * lane. Those plant exactly the state the race leaves - a record replaced between the read and the update - so it is
 * pinned without depending on a schedule, for both the transactional and the auto-committed path.
 */
public class ConcurrentUpdateStatementLostUpdateTest extends TestHelper {
  private static final int  WRITERS               = 8;
  private static final int  INCREMENTS_PER_WRITER = 500;
  private static final long DELTA                 = 25;
  private static final int  MAX_RETRIES           = 10_000;

  @Tag("slow")
  @Test
  void concurrentUpdateStatementsInExplicitTransactionsMustNotLoseIncrements() throws Exception {
    createCounter("CounterTx");
    runConcurrentIncrements("CounterTx", true);
  }

  @Tag("slow")
  @Test
  void concurrentAutoCommittedUpdateStatementsMustNotLoseIncrements() throws Exception {
    createCounter("CounterAutoCommit");
    database.setAutoTransaction(true);
    try {
      runConcurrentIncrements("CounterAutoCommit", false);
    } finally {
      database.setAutoTransaction(false);
    }
  }

  @Test
  void anUpdateComputedFromAReplacedImageIsRefusedInsideATransaction() throws Exception {
    createCounter("CounterStaleTx");
    final RID rid = ridOfCounter("CounterStaleTx");

    database.begin();
    try {
      final MutableDocument stale = database.lookupByRID(rid, true).asDocument().modify();
      assertThat(stale.getLong("value")).isEqualTo(0L);

      // ANOTHER TRANSACTION REPLACES THE RECORD WHILE THIS ONE HOLDS THE IMAGE IT READ
      commitFromAnotherThread(rid, 25L);

      assertThatThrownBy(() -> stale.set("value", stale.getLong("value") + 25L).save())
          .as("an update computed from an image a concurrent transaction already replaced must be a retryable conflict")
          .isInstanceOf(ConcurrentModificationException.class);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(currentValue("CounterStaleTx")).as("the concurrent increment must survive").isEqualTo(25L);
  }

  @Test
  void anUpdateComputedFromAReplacedImageIsRefusedOnTheAutoCommittedPath() throws Exception {
    createCounter("CounterStaleAutoCommit");
    final RID rid = ridOfCounter("CounterStaleAutoCommit");

    database.setAutoTransaction(true);
    try {
      final MutableDocument stale = database.lookupByRID(rid, true).asDocument().modify();
      assertThat(stale.getLong("value")).isEqualTo(0L);

      commitFromAnotherThread(rid, 25L);

      assertThatThrownBy(() -> stale.set("value", stale.getLong("value") + 25L).save())
          .as("the auto-committed path opens its own transaction, so nothing had pinned the page before it either")
          .isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.setAutoTransaction(false);
    }

    assertThat(currentValue("CounterStaleAutoCommit")).as("the concurrent increment must survive").isEqualTo(25L);
  }

  private RID ridOfCounter(final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName + " WHERE name = ?", "seq")) {
      return rs.next().getElement().get().getIdentity();
    }
  }

  /** Commits a real increment of {@code rid} from another thread, so it lands as a separate committed transaction. */
  private void commitFromAnotherThread(final RID rid, final long delta) throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      other.submit(() -> database.transaction(() -> {
        final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
        doc.set("value", doc.getLong("value") + delta).save();
      })).get(60, TimeUnit.SECONDS);
    } finally {
      other.shutdownNow();
    }
  }

  private void createCounter(final String typeName) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(typeName);
      type.createProperty("name", Type.STRING);
      type.createProperty("value", Type.LONG);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      database.newDocument(typeName).set("name", "seq").set("value", 0L).save();
    });
  }

  private void runConcurrentIncrements(final String typeName, final boolean explicitTransaction) throws Exception {
    final Set<Long> beforeValues = ConcurrentHashMap.newKeySet();
    final List<Long> duplicates = new CopyOnWriteArrayList<>();
    final AtomicLong surfacedConflicts = new AtomicLong();

    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < WRITERS; i++)
        futures.add(pool.submit(() -> {
          start.await();
          for (int n = 0; n < INCREMENTS_PER_WRITER; n++) {
            final long before = incrementOnce(typeName, explicitTransaction, surfacedConflicts);
            if (!beforeValues.add(before))
              duplicates.add(before);
          }
          return null;
        }));
      start.countDown();
      for (final Future<?> future : futures)
        future.get(300, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    final long expected = (long) WRITERS * INCREMENTS_PER_WRITER * DELTA;
    final long actual = currentValue(typeName);

    assertThat(duplicates)
        .as("Two concurrent UPDATE ... RETURN BEFORE statements returned the same before-value "
            + "(both writers were handed the same counter range) without any conflict being raised. "
            + "Conflicts surfaced as NeedRetryException (and retried): %d", surfacedConflicts.get())
        .isEmpty();
    assertThat(actual)
        .as("The final counter must account for every acknowledged increment "
            + "(%d writers x %d increments x %d), but %d increments were silently lost. "
            + "Conflicts surfaced as NeedRetryException (and retried): %d",
            WRITERS, INCREMENTS_PER_WRITER, DELTA, (expected - actual) / DELTA, surfacedConflicts.get())
        .isEqualTo(expected);
  }

  private long incrementOnce(final String typeName, final boolean explicitTransaction,
      final AtomicLong surfacedConflicts) {
    for (int attempt = 0; ; attempt++) {
      try {
        final long[] before = new long[1];
        if (explicitTransaction)
          database.transaction(() -> before[0] = executeIncrement(typeName), false, 1);
        else
          before[0] = executeIncrement(typeName);
        return before[0];
      } catch (final NeedRetryException e) {
        surfacedConflicts.incrementAndGet();
        if (attempt >= MAX_RETRIES)
          throw new IllegalStateException("Retry budget exceeded", e);
      }
    }
  }

  private long executeIncrement(final String typeName) {
    try (final ResultSet rs = database.command("sql",
        "UPDATE " + typeName + " SET value = value + ? RETURN BEFORE WHERE name = ?", DELTA, "seq")) {
      return ((Number) rs.next().getProperty("value")).longValue();
    }
  }

  private long currentValue(final String typeName) {
    try (final ResultSet rs = database.query("sql",
        "SELECT value FROM " + typeName + " WHERE name = ?", "seq")) {
      return ((Number) rs.next().getProperty("value")).longValue();
    }
  }
}
