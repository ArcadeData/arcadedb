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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.CreateEdgeStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11448: {@code CREATE EDGE ... RETRY n WAIT ms} parsed, but the statement node had no field to carry either value
 * and the planner never read them, so the clause was dropped on the floor - the statement ran once, a conflict went
 * straight to the caller, and the rendering lost the clause. A conflict here is a {@link ConcurrentModificationException}
 * raised while the edge is created: thrown by a listener for the deterministic cases, and provoked by a concurrent commit
 * for the end-to-end one.
 */
class CreateEdgeRetryWaitTest extends TestHelper {
  private final AtomicInteger attempts = new AtomicInteger();
  private       RID           from;
  private       RID           to;

  @BeforeEach
  void setUpGraph() {
    database.setAutoTransaction(true);
    database.getSchema().createVertexType("RetryV");
    database.getSchema().createEdgeType("RetryE");
    database.transaction(() -> {
      from = database.newVertex("RetryV").set("name", "from").save().getIdentity();
      to = database.newVertex("RetryV").set("name", "to").save().getIdentity();
    });
  }

  @AfterEach
  void resetDelay() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
  }

  /** Makes the first {@code failures} edge creations fail as a lost MVCC race does, counting every attempt. */
  private void failFirstAttempts(final int failures) {
    database.getSchema().getType("RetryE").getEvents().registerListener((BeforeRecordCreateListener) record -> {
      if (attempts.incrementAndGet() <= failures)
        throw new ConcurrentModificationException("provoked conflict on attempt " + attempts.get());
      return true;
    });
  }

  private long edges() {
    return database.countType("RetryE", false);
  }

  private String createEdge(final String clauses) {
    return "CREATE EDGE RetryE FROM " + from + " TO " + to + " SET w = 1" + clauses;
  }

  @Test
  void retryRunsTheStatementAgainAfterAConflict() {
    failFirstAttempts(2);

    try (final ResultSet rs = database.command("sql", createEdge(" RETRY 3"))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("w")).isEqualTo(1);
    }

    assertThat(attempts.get()).as("attempts made by RETRY 3 when the first two conflict").isEqualTo(3);
    assertThat(edges()).as("edges stored: the failed attempts were rolled back").isEqualTo(1);
  }

  @Test
  void withoutRetryTheConflictReachesTheCallerOnTheFirstAttempt() {
    failFirstAttempts(1);

    assertThatThrownBy(() -> database.command("sql", createEdge("")).close())
        .isInstanceOf(ConcurrentModificationException.class);

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(edges()).isZero();
    assertThat(database.isTransactionActive()).as("the statement's own transaction is closed").isFalse();
  }

  @Test
  void retryGivesUpAfterTheAttemptsItWasGiven() {
    failFirstAttempts(Integer.MAX_VALUE);

    assertThatThrownBy(() -> database.command("sql", createEdge(" RETRY 3 WAIT 0")).close())
        .isInstanceOf(ConcurrentModificationException.class);

    assertThat(attempts.get()).as("RETRY 3 makes three attempts, the same count COMMIT RETRY 3 makes").isEqualTo(3);
    assertThat(edges()).isZero();
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void waitPausesBetweenAttempts() {
    failFirstAttempts(1);
    // The configured backoff would also sleep: switch it off, so only WAIT can account for the pause
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);

    final long start = System.nanoTime();
    database.command("sql", createEdge(" RETRY 2 WAIT 300")).close();
    final long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(edges()).isEqualTo(1);
    // A lower bound: a stall only makes it more true
    assertThat(elapsedMs).as("time spent by RETRY 2 WAIT 300 with one conflict").isGreaterThanOrEqualTo(300);
  }

  @Test
  void anInterruptedThreadStopsRetrying() {
    // A terminated command or a shutdown interrupts the thread: the remaining attempts and their pauses must not run
    database.getSchema().getType("RetryE").getEvents().registerListener((BeforeRecordCreateListener) record -> {
      attempts.incrementAndGet();
      Thread.currentThread().interrupt();
      throw new ConcurrentModificationException("provoked conflict");
    });

    try {
      assertThatThrownBy(() -> database.command("sql", createEdge(" RETRY 5 WAIT 10000")).close())
          .isInstanceOf(ConcurrentModificationException.class);
      assertThat(Thread.currentThread().isInterrupted()).as("the interrupt is kept for the caller").isTrue();
    } finally {
      Thread.interrupted();
    }

    assertThat(attempts.get()).as("attempts after the thread was interrupted").isEqualTo(1);
    assertThat(edges()).isZero();
  }

  @Test
  void insideTheCallersTransactionTheConflictGoesToTheTransactionOwner() {
    failFirstAttempts(1);

    database.begin();
    try {
      final RID callerWork = database.newVertex("RetryV").set("name", "caller").save().getIdentity();

      assertThatThrownBy(() -> database.command("sql", createEdge(" RETRY 3")).close())
          .isInstanceOf(ConcurrentModificationException.class);

      // Re-running the statement cannot clear a conflict on a transaction it does not own, and rolling it back would
      // discard the caller's work: the owner (db.transaction(..., retries), COMMIT RETRY, the HTTP retries) retries the unit
      assertThat(attempts.get()).as("attempts inside a caller's transaction").isEqualTo(1);
      assertThat(database.isTransactionActive()).as("the caller's transaction is still open").isTrue();
      assertThat(database.lookupByRID(callerWork, true)).as("the caller's earlier work").isNotNull();
    } finally {
      database.rollback();
    }
  }

  @Test
  void retryClearsAConflictWithAConcurrentCommit() throws Exception {
    // End to end, no listener throwing: every attempt writes the source vertex in its own transaction, and on the first one
    // another thread commits a write of the same record before this attempt commits. Two transactions writing the same
    // record is the conflict no commit-time merge takes back, so the commit loses the MVCC race
    database.getSchema().getType("RetryE").getEvents().registerListener((BeforeRecordCreateListener) record -> {
      final int attempt = attempts.incrementAndGet();
      database.lookupByRID(from, true).asVertex().modify().set("attempt", attempt).save();
      if (attempt == 1) {
        final Thread concurrent = new Thread(() -> database.transaction(() ->
            database.lookupByRID(from, true).asVertex().modify().set("touched", true).save()));
        concurrent.start();
        try {
          concurrent.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    });

    database.command("sql", createEdge(" RETRY 5 WAIT 1")).close();

    assertThat(attempts.get()).as("attempts: the first lost the race with the concurrent commit").isEqualTo(2);
    assertThat(edges()).isEqualTo(1);
    assertThat(database.lookupByRID(from, true).asVertex().getBoolean("touched")).as("the concurrent commit").isTrue();
    assertThat(database.lookupByRID(from, true).asVertex().getInteger("attempt")).as("the attempt that committed").isEqualTo(2);
    assertThat(database.lookupByRID(from, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "RetryE"))
        .isEqualTo(1);
  }

  @Test
  void theStatementCarriesAndRendersRetryAndWait() {
    final CreateEdgeStatement withClauses = (CreateEdgeStatement) parse("CREATE EDGE E FROM #1:0 TO #2:0 RETRY 10 WAIT 5");
    final CreateEdgeStatement without = (CreateEdgeStatement) parse("CREATE EDGE E FROM #1:0 TO #2:0");

    assertThat(withClauses.getRetry()).isEqualTo(10);
    assertThat(withClauses.getWait()).isEqualTo(5);
    assertThat(without.getRetry()).as("RETRY absent").isEqualTo(-1);
    assertThat(without.getWait()).as("WAIT absent").isEqualTo(-1);

    assertThat(withClauses.toString()).isEqualTo("CREATE EDGE E FROM #1:0 TO #2:0 RETRY 10 WAIT 5");
    assertThat(withClauses).isNotEqualTo(without);
    assertThat(withClauses.copy()).isEqualTo(withClauses);
    assertThat(parse("CREATE EDGE E FROM #1:0 TO #2:0 RETRY 10").toString()).isEqualTo("CREATE EDGE E FROM #1:0 TO #2:0 RETRY 10");
  }

  @Test
  void waitWithoutRetryIsRefused() {
    // The documented syntax is RETRY <n> [WAIT <ms>]: a pause between retries that never happen has nothing to mean
    assertThatThrownBy(() -> parse("CREATE EDGE E FROM #1:0 TO #2:0 WAIT 5")).isInstanceOf(CommandSQLParsingException.class);
  }

  private static Statement parse(final String sql) {
    return new SQLAntlrParser(null).parse(sql);
  }
}
