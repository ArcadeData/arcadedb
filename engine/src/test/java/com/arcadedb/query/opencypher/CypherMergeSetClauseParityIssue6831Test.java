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
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6831: {@code MERGE ... ON CREATE SET} / {@code ON MATCH SET} used to be served by a
 * second, hand-maintained copy of the SET clause that understood only the {@code variable.property} shape. The
 * dynamic-key form, the expression-target form, the property-value type check and the simultaneous-assignment rule
 * were all missing from that copy, so a MERGE action behaved differently from the identical stand-alone SET.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeSetClauseParityIssue6831Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypher-6831").create();
    database.getSchema().createVertexType("P");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      // A test that fails inside a caller-managed transaction leaves it open, and drop() refuses a database still in
      // use: without this the failure would be reported as the next test's setUp() error instead of its own.
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  @Test
  void onMatchSetWritesDynamicKey() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n[$k] = 5",
        Map.of("k", "score")));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.score AS score");
    assertThat(rs.next().<Number>getProperty("score").intValue()).isEqualTo(5);
  }

  @Test
  void onCreateSetWritesDynamicKey() {
    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 7}) ON CREATE SET n[$k] = 'new'",
        Map.of("k", "origin")));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P {id: 7}) RETURN n.origin AS origin");
    assertThat(rs.next().<String>getProperty("origin")).isEqualTo("new");
  }

  @Test
  void onMatchSetWritesThroughExpressionTarget() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher",
        "MERGE (n:P {id: 1}) ON MATCH SET (CASE WHEN true THEN n END).x = 1"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.x AS x");
    assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
  }

  @Test
  void onMatchSetWithFalseExpressionTargetIsANoOp() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher",
        "MERGE (n:P {id: 1}) ON MATCH SET (CASE WHEN false THEN n END).x = 1"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.x AS x");
    assertThat(rs.next().<Object>getProperty("x")).isNull();
  }

  @Test
  void onCreateSetRejectsAMapPropertyValue() {
    // The identical stand-alone SET already raises this; the MERGE action must not be the lenient one.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MERGE (n:P {id: 2}) ON CREATE SET n.p = {a: 1}")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void onMatchSetIsASimultaneousAssignment() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, x: 'left', y: 'right'})"));

    database.transaction(() -> database.command("opencypher",
        "MERGE (n:P {id: 1}) ON MATCH SET n.x = n.y, n.y = n.x"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.x AS x, n.y AS y");
    final var row = rs.next();
    assertThat(row.<String>getProperty("x")).isEqualTo("right");
    assertThat(row.<String>getProperty("y")).isEqualTo("left");
  }

  @Test
  void onMatchSetStillCopiesFromAnotherEntity() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1}), (:P {id: 2, tag: 'src'})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (src:P {id: 2}) MERGE (n:P {id: 1}) ON MATCH SET n += src"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P {tag: 'src'}) RETURN count(n) AS c");
    assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(2);
  }

  @Test
  void onMatchSetStillAddsLabels() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n:Extra"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:Extra) RETURN n.id AS id");
    assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(1);
  }

  @Test
  void onMatchSetRemovesPropertyWithNullValue() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, gone: 'x'})"));

    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n.gone = null"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.gone AS gone");
    assertThat(rs.next().<Object>getProperty("gone")).isNull();
  }

  /**
   * #4474 travels with the shared applier: re-asserting a value the record already holds must still not write, because
   * the write would bump the record's MVCC version and invalidate it for every concurrent reader that had matched it.
   * The pre-existing regression test for this asserts the absence of a ConcurrentModificationException between two
   * racing threads, which no longer fails when the optimization is removed; counting the update events the write would
   * have raised pins the behaviour directly and deterministically.
   */
  @Test
  void onMatchSetDoesNotRewriteAnUnchangedValue() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, bank: 'ACME'})"));

    final AtomicInteger updates = new AtomicInteger();
    final AfterRecordUpdateListener listener = record -> updates.incrementAndGet();
    database.getSchema().getType("P").getEvents().registerListener(listener);
    try {
      database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n.bank = 'ACME'"));
      assertThat(updates.get()).isZero();

      // The guard has to be about the value, not about SET being inert: a different value does write.
      database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n.bank = 'OTHER'"));
      assertThat(updates.get()).isEqualTo(1);
    } finally {
      database.getSchema().getType("P").getEvents().unregisterListener(listener);
    }
  }

  /**
   * The same guard must not swallow a genuine removal: an unchanged-value skip is decided per item, and a null value
   * on a property that exists is a change.
   */
  @Test
  void onMatchSetStillWritesWhenOnlyOneOfTwoItemsChanges() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, bank: 'ACME', branch: 'HQ'})"));

    database.transaction(() -> database.command("opencypher",
        "MERGE (n:P {id: 1}) ON MATCH SET n.bank = 'ACME', n.branch = 'WEST'"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.bank AS bank, n.branch AS branch");
    final var row = rs.next();
    assertThat(row.<String>getProperty("bank")).isEqualTo("ACME");
    assertThat(row.<String>getProperty("branch")).isEqualTo("WEST");
  }

  /**
   * WHICH record an expression target names is a read of the graph like any other, so it is answered from the
   * pre-clause state: n.enabled was true when the clause began, so the CASE selects n and the flag is written.
   */
  @Test
  void expressionTargetResolvesAgainstThePreClauseState() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, enabled: true})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (n:P) SET n.enabled = false, (CASE WHEN n.enabled THEN n END).flag = true"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.flag AS flag, n.enabled AS enabled");
    final var row = rs.next();
    assertThat(row.<Boolean>getProperty("flag")).isTrue();
    assertThat(row.<Boolean>getProperty("enabled")).isFalse();
  }

  /**
   * Resolving the target in phase 1 must not outrun a label write in the same clause: the rewrite deletes the record
   * the target names, so the captured vertex has to follow the move rather than be written to after its death.
   */
  @Test
  void expressionTargetFollowsALabelWriteFromTheSameClause() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (n:P) SET n:Extra, (CASE WHEN true THEN n END).x = 1"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:Extra) RETURN n.x AS x");
    assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
  }

  /** Two expression-target writes to the same record accumulate rather than the second losing the first. */
  @Test
  void twoExpressionTargetWritesToTheSameRecordAccumulate() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (n:P) SET (CASE WHEN true THEN n END).a = 1, (CASE WHEN true THEN n END).b = 2"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.a AS a, n.b AS b");
    final var row = rs.next();
    assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(1);
    assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(2);
  }

  /**
   * The map-replace and map-merge forms reach the type check through the shared applier, but this PR's whole thesis
   * is that the MERGE path cannot be assumed to behave like the stand-alone one: assert it here rather than infer it.
   */
  @Test
  void onMatchSetRejectsAScalarReplaceSource() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n = 5")))
        .rootCause()
        .hasMessageContaining("TypeError");
  }

  @Test
  void onCreateSetRejectsAScalarMergeSource() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MERGE (n:P {id: 9}) ON CREATE SET n += 'nope'")))
        .rootCause()
        .hasMessageContaining("TypeError");
  }

  @Test
  void onMatchSetRejectsANestedMapInsideAMergeSource() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n += {bad: {nested: 1}}")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }
}
