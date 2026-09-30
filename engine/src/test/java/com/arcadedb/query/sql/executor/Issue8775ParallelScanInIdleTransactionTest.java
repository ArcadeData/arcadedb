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
import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8775: a filtered scan ran on the calling thread as soon as a transaction was active, even one that had
 * written nothing. The workers of a parallel scan read the committed pages, which is exactly what such a transaction
 * sees, so they may serve it. Once the transaction has written anything, or pins the pages it reads
 * (REPEATABLE_READ), the scan stays on the calling thread, which sees the transaction's own changes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8775ParallelScanInIdleTransactionTest extends TestHelper {
  private static final int    ROWS   = 20_000;
  private static final String QUERY  = "SELECT count(*) AS n FROM E WHERE grp = 5";
  private static final String CYPHER = "MATCH (e:E) WHERE e.grp = 5 RETURN count(*) AS n";

  private long expected;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    database.getSchema().createVertexType("E", 4);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("E").set("id", i, "grp", i % 100).save();
    });
    expected = ROWS / 100;
  }

  @Test
  void idleTransactionScansInParallel() {
    database.begin();
    try {
      assertThat(sqlPlan()).contains("(parallel)");
      assertThat(cypherPlan()).contains("[parallel]");
      assertThat(count()).isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void idleTransactionAfterAReadScansInParallel() {
    database.begin();
    try {
      assertThat(count()).isEqualTo(expected);
      assertThat(sqlPlan()).contains("(parallel)");
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatWroteStaysSequentialAndSeesItsWrites() {
    database.begin();
    try {
      database.newVertex("E").set("id", -1, "grp", 5).save();
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(cypherPlan()).doesNotContain("[parallel]");
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatUpdatedStaysSequentialAndSeesItsUpdate() {
    database.begin();
    try {
      database.command("sql", "UPDATE E SET grp = 5 WHERE id = 1");
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void transactionThatDeletedStaysSequential() {
    database.begin();
    try {
      database.command("sql", "DELETE FROM E WHERE id = 5");
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(count()).isEqualTo(expected - 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void repeatableReadStaysSequential() {
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(sqlPlan()).doesNotContain("(parallel)");
      assertThat(count()).isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void writeDuringIterationDoesNotChangeTheScanThatAlreadyStarted() {
    // A scan is decided at its first pull, and its workers read committed pages: what the transaction writes while it
    // drains is not fed back into it (no statement snapshot, though: a foreign commit can reach pages read later)
    database.begin();
    try {
      long rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          rs.next();
          if (rows++ == 0)
            database.newVertex("E").set("id", -1, "grp", 5).save();
        }
      }
      assertThat(rows).isEqualTo(expected);
      // the next query starts after the write: it sees it, on the calling thread
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void callerDoesNotScanUnitsItselfOnceTheTransactionHasWritten() {
    // Units the caller would claim read through the transaction and would see its writes, unlike the workers' units:
    // every row of a scan must come from the same view, so a transaction that wrote since the first pull leaves all
    // the units to the workers
    database.begin();
    try {
      long rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE grp = 5")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows++;
          // tombstone every record the scan returns that the workers have not yet read: none may be skipped
          if (rows == 1)
            database.command("sql", "UPDATE E SET grp = 7 WHERE grp = 5");
          assertThat(row.<Integer>getProperty("grp")).isEqualTo(5);
        }
      }
      assertThat(rows).isEqualTo(expected);
    } finally {
      database.rollback();
    }
  }

  @Test
  void selfFeedingUpdateInIdleTransactionMatchesTheSequentialAnswer() {
    final String update = "UPDATE E SET grp = 5 WHERE grp = 6";
    final long parallelUpdated = updatedIn(update, true);
    final long sequentialUpdated = updatedIn(update, false);
    assertThat(parallelUpdated).isEqualTo(expected).isEqualTo(sequentialUpdated);
  }

  @Test
  void deleteInIdleTransactionMatchesTheSequentialAnswer() {
    assertThat(deletedIn(true)).isEqualTo(expected).isEqualTo(deletedIn(false));
  }

  @Test
  void commitFromAnotherThreadIsSeenByTheIdleTransaction() throws InterruptedException {
    database.begin();
    try {
      assertThat(count()).isEqualTo(expected);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          database.begin();
          database.newVertex("E").set("id", -2, "grp", 5).save();
          database.commit();
        } catch (final Throwable e) {
          failure.set(e);
        }
      });
      writer.start();
      writer.join();
      assertThat(failure.get()).isNull();
      assertThat(sqlPlan()).contains("(parallel)");
      assertThat(count()).isEqualTo(expected + 1);
    } finally {
      database.rollback();
    }
  }

  private long updatedIn(final String update, final boolean parallel) {
    return inRolledBackTransaction(parallel, () -> {
      try (final ResultSet rs = database.command("sql", update)) {
        return ((Number) rs.next().getProperty("count")).longValue();
      }
    });
  }

  private long deletedIn(final boolean parallel) {
    return inRolledBackTransaction(parallel, () -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM E WHERE grp = 5")) {
        return ((Number) rs.next().getProperty("count")).longValue();
      }
    });
  }

  private long inRolledBackTransaction(final boolean parallel, final LongSupplier body) {
    final boolean previous = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.QUERY_PARALLEL_SCAN);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
    try {
      database.begin();
      try {
        return body.getAsLong();
      } finally {
        database.rollback();
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, previous);
    }
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", QUERY)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private String sqlPlan() {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + QUERY)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private String cypherPlan() {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + CYPHER)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
