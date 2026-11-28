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
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #13683: a row of a RETURN that fails to evaluate failed the whole result before any row was delivered, because the step
 * projects a batch of rows into a buffer and an exception from row k dropped the k-1 rows already in it. A streaming executor
 * (Neo4j) hands those out first and fails when the consumer reaches the failing row.
 */
class CypherErrorAfterRowsIssue13683Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-error-after-rows-13683").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** The reported statement: the first row is 10, the failure comes with the second. */
  @Test
  void theRowsBeforeTheFailingOneAreDeliveredFirst() {
    final ResultSet result = database.query("opencypher", "UNWIND [1, 0, 2] AS x RETURN 10 / x");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Number>getProperty("10 / x").longValue()).isEqualTo(10L);

    final Throwable failure = catchThrowable(result::hasNext);
    assertThat(failure).isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("by zero");
    result.close();
  }

  /** Several rows before the failing one, drained through next() as a driver does. */
  @Test
  void everyRowBeforeTheFailingOneIsDelivered() {
    final ResultSet result = database.query("opencypher",
        "UNWIND range(1, 5) AS x RETURN CASE WHEN x = 4 THEN 1 / 0 ELSE x END AS v");
    final List<Long> seen = new ArrayList<>();

    final Throwable failure = catchThrowable(() -> {
      while (result.hasNext())
        seen.add(result.next().<Number>getProperty("v").longValue());
    });

    assertThat(seen).containsExactly(1L, 2L, 3L);
    assertThat(failure).isInstanceOf(ArithmeticErrorException.class);
    result.close();
  }

  /** The failure is raised once: a later hasNext() reads the end of the result, it does not rethrow. */
  @Test
  void theFailureIsRaisedOnce() {
    final ResultSet result = database.query("opencypher", "UNWIND [1, 0] AS x RETURN 10 / x");
    assertThat(result.next().<Number>getProperty("10 / x").longValue()).isEqualTo(10L);
    assertThat(catchThrowable(result::hasNext)).isInstanceOf(ArithmeticErrorException.class);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  /** A failure on the very first row has nothing to hold back and is raised as before. */
  @Test
  void aFailureOnTheFirstRowIsRaisedAtOnce() {
    final Throwable failure = catchThrowable(() -> {
      try (final ResultSet result = database.query("opencypher", "UNWIND [0, 1] AS x RETURN 10 / x")) {
        result.hasNext();
      }
    });
    assertThat(failure).isInstanceOf(ArithmeticErrorException.class);
  }

  /** A result without a failing row is untouched, DISTINCT included. */
  @Test
  void aResultWithoutAFailingRowIsUnchanged() {
    try (final ResultSet result = database.query("opencypher", "UNWIND [1, 2, 2, 3] AS x RETURN DISTINCT 10 / x AS v")) {
      final List<Long> seen = new ArrayList<>();
      while (result.hasNext())
        seen.add(result.next().<Number>getProperty("v").longValue());
      assertThat(seen).containsExactly(10L, 5L, 3L);
    }
  }
}
