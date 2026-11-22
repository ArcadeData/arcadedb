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

import com.arcadedb.TestHelper;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #12981: a MERGE action ({@code ON MATCH SET} / {@code ON CREATE SET}) skips a write whose
 * value the record already holds (#4474). {@code CypherValues.equalValues} called a stored INTEGER {@code 1} and a
 * supplied FLOAT {@code 1.0} (and the converse) one value, so the write was dropped and the property kept its old
 * type: {@code n.x / 2} answered {@code 0} instead of {@code 0.5}. The identical plain {@code SET} writes.
 */
class CypherMergeActionNumericKindIssue12981Test extends TestHelper {

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next();
    }
  }

  private void run(final String query) {
    database.transaction(() -> database.command("opencypher", query).close());
  }

  @Test
  void onMatchSetWritesAFloatOverAStoredInteger() {
    run("CREATE (:T {k: 1, x: 1})");
    run("MERGE (n:T {k: 1}) ON MATCH SET n.x = 1.0");

    final Result row = single("MATCH (n:T) RETURN valueType(n.x) AS t, n.x / 2 AS half");
    assertThat(row.<String>getProperty("t")).isEqualTo("FLOAT NOT NULL");
    assertThat(row.<Number>getProperty("half").doubleValue()).isEqualTo(0.5d);
  }

  @Test
  void onMatchSetWritesAnIntegerOverAStoredFloat() {
    run("CREATE (:T {k: 1, x: 1.0})");
    run("MERGE (n:T {k: 1}) ON MATCH SET n.x = 1");

    final Result row = single("MATCH (n:T) RETURN valueType(n.x) AS t, n.x / 2 AS half");
    assertThat(row.<String>getProperty("t")).isEqualTo("INTEGER NOT NULL");
    assertThat(row.<Number>getProperty("half").longValue()).isZero();
  }

  @Test
  void onCreateSetOverridesTheKindThePatternCreated() {
    run("MERGE (n:T {k: 1, x: 1}) ON CREATE SET n.x = 1.0");

    assertThat(single("MATCH (n:T) RETURN valueType(n.x) AS t").<String>getProperty("t")).isEqualTo("FLOAT NOT NULL");
  }

  @Test
  void aParameterOfTheOtherKindIsWrittenToo() {
    run("CREATE (:T {k: 1, x: 1})");
    database.transaction(() -> database.command("opencypher", "MERGE (n:T {k: 1}) ON MATCH SET n.x = $v", "v", 1.0d).close());

    assertThat(single("MATCH (n:T) RETURN valueType(n.x) AS t").<String>getProperty("t")).isEqualTo("FLOAT NOT NULL");
  }

  @Test
  void mergeActionAndPlainSetLeaveTheSameKind() {
    run("CREATE (:T {k: 1, x: 1})");
    run("MERGE (n:T {k: 1}) ON MATCH SET n.x = 1.0");
    final String afterMerge = single("MATCH (n:T) RETURN valueType(n.x) AS t").getProperty("t");

    run("MATCH (n:T) SET n.x = 1");
    run("MATCH (n:T) SET n.x = 1.0");
    assertThat(single("MATCH (n:T) RETURN valueType(n.x) AS t").<String>getProperty("t")).isEqualTo(afterMerge);
  }

  /**
   * The #4474 optimisation must survive the fix: re-asserting a value of the same kind, or an integral value of another
   * width, is still no write, and so is a number written into a property DECLARED as the other kind, which stores it as
   * that kind anyway.
   */
  @Test
  void reassertingTheSameValueStillDoesNotWrite() {
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.k INTEGER");
    database.command("sql", "CREATE PROPERTY D.price DOUBLE");
    database.command("sql", "CREATE PROPERTY D.count LONG");
    run("CREATE (:D {k: 1, price: 1.5, count: 2})");
    run("CREATE (:T {k: 1, x: 1, y: 2.5})");

    final AtomicInteger updates = new AtomicInteger();
    final AfterRecordUpdateListener listener = record -> updates.incrementAndGet();
    database.getSchema().getType("D").getEvents().registerListener(listener);
    database.getSchema().getType("T").getEvents().registerListener(listener);
    try {
      run("MERGE (n:T {k: 1}) ON MATCH SET n.x = 1, n.y = 2.5");
      run("MERGE (n:D {k: 1}) ON MATCH SET n.price = 1.5, n.count = 2");
      database.transaction(
          () -> database.command("opencypher", "MERGE (n:D {k: 1}) ON MATCH SET n.count = $c", "c", 2).close());
      assertThat(updates.get()).as("same value, same kind: no write").isZero();

      run("MERGE (n:D {k: 1}) ON MATCH SET n.price = 2");
      run("MERGE (n:D {k: 1}) ON MATCH SET n.price = 2");
      assertThat(updates.get()).as("a new value is written once, its repeat is not").isEqualTo(1);

      run("MERGE (n:T {k: 1}) ON MATCH SET n.x = 1.0");
      assertThat(updates.get()).as("a value of the other kind is written").isEqualTo(2);
    } finally {
      database.getSchema().getType("D").getEvents().unregisterListener(listener);
      database.getSchema().getType("T").getEvents().unregisterListener(listener);
    }
  }
}
