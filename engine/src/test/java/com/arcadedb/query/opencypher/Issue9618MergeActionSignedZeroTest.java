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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9618: a MERGE action ({@code ON MATCH SET} / {@code ON CREATE SET}) skips a write whose
 * value the record already holds (#4474). Once -0.0 and 0.0 became one value for matching (#9303), that skip decision
 * also called them equal, so a MERGE action silently dropped a write that flips the sign of a zero while the identical
 * plain {@code SET} performed it. The sign is observable ({@code 1/v} is -Infinity vs Infinity), so the write must
 * happen.
 */
class Issue9618MergeActionSignedZeroTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE VERTEX TYPE Z");
    database.command("sql", "CREATE PROPERTY Z.v DOUBLE");
    database.command("sql", "CREATE PROPERTY Z.tag STRING");
  }

  private void store(final double v) {
    database.transaction(() -> database.newVertex("Z").set("v", v).set("tag", "x").save());
  }

  private double storedV() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Z {tag: 'x'}) RETURN n.v AS v")) {
      return rs.next().<Number>getProperty("v").doubleValue();
    }
  }

  @Test
  void onMatchSetWritesPositiveZeroOverStoredNegativeZero() {
    store(-0.0d);
    database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = 0.0").close());
    assertThat(1 / storedV()).isEqualTo(Double.POSITIVE_INFINITY);
  }

  @Test
  void onMatchSetWritesNegativeZeroOverStoredPositiveZero() {
    store(0.0d);
    database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = -0.0").close());
    assertThat(1 / storedV()).isEqualTo(Double.NEGATIVE_INFINITY);
  }

  @Test
  void onMatchSetWithAParameterWritesTheSignOfZero() {
    store(-0.0d);
    database.transaction(
        () -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = $v", "v", 0.0d).close());
    assertThat(1 / storedV()).isEqualTo(Double.POSITIVE_INFINITY);
  }

  @Test
  void onCreateSetOverridesTheZeroThePatternCreated() {
    database.transaction(
        () -> database.command("opencypher", "MERGE (n:Z {tag: 'x', v: 0.0}) ON CREATE SET n.v = -0.0").close());
    assertThat(1 / storedV()).isEqualTo(Double.NEGATIVE_INFINITY);
  }

  @Test
  void mergeActionAndPlainSetLeaveTheSameValue() {
    store(-0.0d);
    database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = 0.0").close());
    final double afterMerge = storedV();
    database.transaction(() -> database.command("opencypher", "MATCH (n:Z) SET n.v = -0.0").close());
    database.transaction(() -> database.command("opencypher", "MATCH (n:Z) SET n.v = 0.0").close());
    assertThat(Double.doubleToRawLongBits(afterMerge)).isEqualTo(Double.doubleToRawLongBits(storedV()));
  }

  /**
   * The #4474 optimisation must survive the fix: re-asserting the same zero, or an integral value of another width, is
   * still no write.
   */
  @Test
  void reassertingTheSameValueStillDoesNotWrite() {
    store(-0.0d);
    database.command("sql", "CREATE PROPERTY Z.n LONG");
    database.transaction(() -> database.command("opencypher", "MATCH (n:Z) SET n.n = 1").close());

    final AtomicInteger updates = new AtomicInteger();
    final AfterRecordUpdateListener listener = record -> updates.incrementAndGet();
    database.getSchema().getType("Z").getEvents().registerListener(listener);
    try {
      database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = -0.0").close());
      database.transaction(
          () -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.n = $n", "n", 1).close());
      assertThat(updates.get()).isZero();

      database.transaction(() -> database.command("opencypher", "MERGE (n:Z {tag: 'x'}) ON MATCH SET n.v = 0.0").close());
      assertThat(updates.get()).isEqualTo(1);
    } finally {
      database.getSchema().getType("Z").getEvents().unregisterListener(listener);
    }
  }
}
