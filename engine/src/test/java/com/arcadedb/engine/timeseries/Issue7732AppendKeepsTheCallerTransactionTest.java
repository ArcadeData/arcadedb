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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7732: when a TimeSeries append's OWN commit failed, the error handling rolled back the CALLER's
 * transaction.
 * <p>
 * #7410 pinned the other direction of the same relationship - the append commits its own transaction, so the
 * caller's {@code rollback()} does not take the samples back. This is the direction nothing pinned. A failed
 * {@code commit()} has already popped the transaction it failed to commit ({@code LocalDatabase.commit()} does it
 * in a {@code finally}, and {@code DatabaseContext.popIfNotLastTransaction()} removes a nested context whenever
 * the stack holds more than one), so {@code db.isTransactionActive()} in the catch answered for the CALLER's
 * transaction, and {@code db.rollback()} discarded the caller's own uncommitted work. On the retry arm the append
 * then succeeded and returned normally, so the caller got no signal at all: the rows it had written in the same
 * transaction were simply gone.
 * <p>
 * The single-transaction case was safe by accident - with nothing underneath, the failed transaction stays on the
 * stack and is already inactive - which is why only a caller who had a transaction open was bitten.
 * <p>
 * The probe is a commit injected to fail, because that is the only observation that separates "rolled back its
 * own" from "rolled back mine": the caller's document row is written in the same transaction and its survival is
 * what the append must not decide.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7732">issue #7732</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7732AppendKeepsTheCallerTransactionTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  /**
   * Fails the next {@code commit()} that reaches the wrapped database, whatever calls it, and delegates
   * everything else. Installed as the database's wrapped instance, which is what {@code TimeSeriesShard}
   * begins and commits its append transaction on ({@code getWrappedDatabaseInstance()}); on a standalone
   * database that is normally the database itself, and under HA it is the Raft wrapper, so this stands exactly
   * where a quorum loss or a step-down would.
   */
  private static final class FailingCommits implements InvocationHandler {
    private final DatabaseInternal delegate;
    private final AtomicInteger    failuresLeft;
    private final Class<? extends RuntimeException> failWith;

    private FailingCommits(final DatabaseInternal delegate, final int failures,
        final Class<? extends RuntimeException> failWith) {
      this.delegate = delegate;
      this.failuresLeft = new AtomicInteger(failures);
      this.failWith = failWith;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
      if ("commit".equals(method.getName()) && method.getParameterCount() == 0
          && failuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        // Rolled back and popped, which is the state a real failed commit leaves: commit1stPhase has already
        // rolled the transaction back on every failure arm, and commit()'s own finally pops it. That is the
        // state the bug needed - the append's context gone, the CALLER's back on top of the stack and active -
        // and it is why nothing of this transaction reaches the pages either.
        delegate.rollback();
        throw failWith == ConcurrentModificationException.class
            ? new ConcurrentModificationException("injected commit failure")
            : new RuntimeException("injected commit failure");
      }
      return method.invoke(delegate, args);
    }
  }

  private void installFailingCommits(final int failures, final Class<? extends RuntimeException> failWith) {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    local.setWrappedDatabaseInstance(failingCommits(local.getWrappedDatabaseInstance(), failures, failWith));
  }

  private static DatabaseInternal failingCommits(final DatabaseInternal delegate, final int failures,
      final Class<? extends RuntimeException> failWith) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, new FailingCommits(delegate, failures, failWith));
  }

  private TimeSeriesEngine createAll() {
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);
    database.command("sql", "CREATE TIMESERIES TYPE Point TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");
    return ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();
  }

  private long controlRows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Control")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * The reported shape: the append's commit hits an MVCC conflict, the CME arm retries and succeeds, and the
   * caller is never told anything went wrong - so the caller's own row had better still be there to commit.
   */
  @Test
  void aRetriedAppendLeavesTheCallerTransactionIntact() throws Exception {
    final TimeSeriesEngine engine = createAll();
    installFailingCommits(1, ConcurrentModificationException.class);

    database.begin();
    database.newDocument("Control").set("k", 1).save();

    engine.appendSamples(new long[] { BASE_TS }, new Object[][] { new Object[] { 1.0d } });

    assertThat(database.isTransactionActive())
        .as("the append's failed commit must not have taken the caller's transaction with it")
        .isTrue();
    database.commit();

    assertThat(controlRows()).as("the caller's own row survived the append's internal retry").isEqualTo(1);

    database.begin();
    try {
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null))
          .as("and the retry appended the sample once, not twice")
          .hasSize(1);
    } finally {
      database.commit();
    }
  }

  /**
   * The same when the append gives up: it throws, which is the caller's signal, and the caller is still the one
   * who decides what happens to its transaction.
   */
  @Test
  void anAppendThatFailsForGoodStillLeavesTheDecisionToTheCaller() throws Exception {
    final TimeSeriesEngine engine = createAll();
    // One failure is all it takes on an arm that does not retry.
    installFailingCommits(1, RuntimeException.class);

    database.begin();
    database.newDocument("Control").set("k", 1).save();

    assertThatThrownBy(() -> engine.appendSamples(new long[] { BASE_TS }, new Object[][] { new Object[] { 1.0d } }))
        .as("a failure the append cannot retry is reported, not swallowed")
        .isInstanceOf(Exception.class);

    assertThat(database.isTransactionActive()).isTrue();
    database.commit();

    assertThat(controlRows()).as("the caller's row was never the append's to discard").isEqualTo(1);
  }

  /** With no caller transaction there is nothing underneath to protect, and the retry still works. */
  @Test
  void anAppendWithNoCallerTransactionIsUnaffected() throws Exception {
    final TimeSeriesEngine engine = createAll();
    installFailingCommits(1, ConcurrentModificationException.class);

    engine.appendSamples(new long[] { BASE_TS }, new Object[][] { new Object[] { 1.0d } });

    database.begin();
    try {
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).hasSize(1);
    } finally {
      database.commit();
    }
  }

  /**
   * The third site in {@code TimeSeriesShard}'s constructor: crash recovery, which runs on every open of a shard
   * whose bucket was left mid-compaction (claude-review on PR #7747). Unlike the two above it commits on
   * {@code database} rather than on the wrapped instance - a local repair must not be replicated - so the failure
   * is injected by handing the ENGINE a database whose commit fails, which is where that call resolves from.
   * <p>
   * The message assertion is what makes the injection exact: it is the crash-recovery commit that failed and not
   * some earlier one, or this test would prove nothing about this block.
   */
  @Test
  void aFailedCrashRecoveryCommitLeavesTheCallerTransactionIntact() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "Recovered", cols, 1);
    database.commit();

    // The state a crash mid-compaction leaves behind, which is what makes the next open run crash recovery.
    database.begin();
    engine.getShard(0).getMutableBucket().setCompactionInProgress(true);
    database.commit();

    database.begin();
    database.newDocument("Control").set("k", 1).save();

    // A second engine over the same files, as a retried initEngine() builds one: the components are registered,
    // so nothing but the recovery block commits on the way through.
    assertThatThrownBy(() -> new TimeSeriesEngine(
        failingCommits((DatabaseInternal) database, 1, RuntimeException.class), "Recovered", cols, 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Crash recovery failed for shard");

    assertThat(database.isTransactionActive())
        .as("the recovery's failed commit must not have taken the caller's transaction with it")
        .isTrue();
    database.commit();

    assertThat(controlRows()).isEqualTo(1);
  }

  /** {@code OwnTransaction} itself: the rollback is a no-op once the commit has taken its transaction away. */
  @Test
  void theOwnTransactionHelperNeverRollsBackSomebodyElsesTransaction() {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);

    db.begin();
    db.newDocument("Control").set("k", 1).save();

    final OwnTransaction nested = OwnTransaction.begin(db);
    nested.commit();
    nested.rollbackIfMine();

    assertThat(db.isTransactionActive()).isTrue();
    db.commit();
    assertThat(controlRows()).isEqualTo(1);
  }

  /** And it DOES roll back its own while that one is still there, so nothing half-written is published. */
  @Test
  void theOwnTransactionHelperStillRollsBackItsOwn() {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.getSchema().createDocumentType("Control").createProperty("k", Type.INTEGER);

    db.begin();
    db.newDocument("Control").set("k", 1).save();

    final OwnTransaction nested = OwnTransaction.begin(db);
    db.newDocument("Control").set("k", 2).save();
    nested.rollbackIfMine();

    assertThat(db.isTransactionActive()).as("the caller's transaction is still open underneath").isTrue();
    db.commit();
    assertThat(controlRows()).as("the nested row was rolled back, the caller's was not").isEqualTo(1);
  }
}
