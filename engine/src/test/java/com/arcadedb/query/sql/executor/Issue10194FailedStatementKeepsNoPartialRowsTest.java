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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.QueryEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #10194, third point: a multi-row statement that failed midway inside a transaction left the rows it had written before the
 * error, and a caller that caught the error and committed published them: half a statement. The fix of #10194 refused the commit of
 * such a transaction, there being no savepoint to take the statement back to. Statement savepoints (#10346) take it back: the rows
 * the statement wrote before its error are gone, and the transaction goes on. The commit is still refused where the engine cannot take
 * a statement back exactly, which is pinned by the tests of #10379 and #10482.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10194FailedStatementKeepsNoPartialRowsTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.k INTEGER").close();
    database.command("sql", "CREATE INDEX ON T (k) UNIQUE").close();
  }

  private long rows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM T")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  /** Runs the statement inside a transaction, catching its error there and committing, as the issue's reproducer does. */
  private Throwable runCaughtAndCommit(final String statement) {
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      try {
        database.command("sql", statement).close();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
    }, false, 0);
    return caught.get();
  }

  @Test
  void aCaughtMultiRowInsertIsTakenBackAndTheTransactionCommits() {
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    database.transaction(() -> {
      try {
        database.command("sql", "INSERT INTO T (id, k) VALUES (30, 23), (31, 23)").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
      database.command("sql", "INSERT INTO T SET id = 5, k = 5").close();
    }, false, 0);

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isEqualTo(1);
  }

  @Test
  void aSingleRowRefusalLeavesTheTransactionUsable() {
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
      database.command("sql", "INSERT INTO T SET id = 3, k = 3").close();
      try {
        // a duplicate of a key this transaction already claimed is refused by the statement itself, with its record retracted
        database.command("sql", "INSERT INTO T SET id = 4, k = 3").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
      database.command("sql", "INSERT INTO T SET id = 5, k = 5").close();
    });

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isEqualTo(3);
  }

  @Test
  void aCaughtMultiRowUpdateIsTakenBackAndTheTransactionCommits() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
      database.command("sql", "INSERT INTO T SET id = 3, k = 5").close();
    });

    assertThat(runCaughtAndCommit("UPDATE T SET k = 5 WHERE id < 3")).isInstanceOf(DuplicatedKeyException.class);

    try (final ResultSet rs = database.query("sql", "SELECT k FROM T ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(1);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(2);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(5);
    }
  }

  @Test
  void aStatementThatFailedBeforeItsFirstWriteLeavesTheTransactionUsable() {
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      try {
        // its first row is the refused one: the statement wrote nothing
        database.command("sql", "INSERT INTO T (id, k) VALUES (2, 1), (3, 3)").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
    });

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isEqualTo(1);
  }

  @Test
  void anUncaughtFailureStillRollsBackEverythingAsBefore() {
    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T (id, k) VALUES (30, 23), (31, 23)").close();
    })).isInstanceOf(DuplicatedKeyException.class);

    assertThat(rows()).isZero();
  }

  @Test
  void aWholeStatementThatSucceedsIsUnaffected() {
    database.transaction(() -> database.command("sql", "INSERT INTO T (id, k) VALUES (1, 1), (2, 2), (3, 3)").close());

    assertThat(rows()).isEqualTo(3);
  }

  @Test
  void aStatementThatChangedWhatAnEarlierOneUpdatedIsTakenBackAndTheEarlierUpdateStands() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      database.command("sql", "INSERT INTO T SET id = 2, k = 2").close();
    });

    database.transaction(() -> {
      // UPDATED, NOT WRITTEN YET: THE NEXT STATEMENT FINDS THE ONLY COPY OF WHAT THIS ONE DID, AND CHANGES IT
      database.command("sql", "UPDATE T SET k = 10 WHERE id = 1").close();
      try {
        database.command("sql", "UPDATE T SET k = 7 WHERE id <= 2").close();
      } catch (final DuplicatedKeyException e) {
        // carried on, as the caller of the reproducer does
      }
      database.command("sql", "INSERT INTO T SET id = 5, k = 5").close();
    }, false, 0);

    assertThat(rows()).isEqualTo(3);
    try (final ResultSet rs = database.query("sql", "SELECT k FROM T ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(10);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(2);
      assertThat(rs.next().<Number>getProperty("k").intValue()).isEqualTo(5);
    }
  }

  @Test
  void aWriteStatementFailsInsideCommandAndNotWhileTheResultIsRead() {
    database.command("sql", "CREATE DOCUMENT TYPE Src").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Src SET id = 1, k = 7").close();
      database.command("sql", "INSERT INTO Src SET id = 2, k = 7").close();
    });

    // INSERT ... FROM SELECT runs to the end inside command(): the error is raised there, where the guard sees it, and the
    // result set that comes back holds what the statement already did
    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      try {
        database.command("sql", "INSERT INTO T FROM SELECT id, k FROM Src ORDER BY id").close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
    }, false, 0);

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isZero();
  }

  @Test
  void aWriteRunThroughAnalyzeIsTakenBackLikeCommand() {
    // analyze().execute() is the entry the MCP command tool runs writes through, and it takes no CommandContext
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze("INSERT INTO T (id, k) VALUES (30, 23), (31, 23)");
    assertThat(analyzed.isIdempotent()).isFalse();

    final AtomicReference<Throwable> caught = new AtomicReference<>();
    database.transaction(() -> {
      try {
        analyzed.execute(Map.of()).close();
      } catch (final DuplicatedKeyException e) {
        caught.set(e);
      }
    }, false, 0);

    assertThat(caught.get()).isInstanceOf(DuplicatedKeyException.class);
    assertThat(rows()).isZero();
  }

  @Test
  void aReadOnlyStatementNeverBlocksTheCommit() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1, k = 1").close();
      assertThatThrownBy(() -> database.query("sql", "SELECT FROM NoSuchType")).isInstanceOf(RuntimeException.class);
    });

    assertThat(rows()).isEqualTo(1);
  }
}
