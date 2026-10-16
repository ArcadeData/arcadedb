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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.procedures.control.DoWhen;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10160: the openCypher expressions that build a list out of a sub-plan or a traversal - {@code COLLECT { ... }}, a pattern
 * comprehension - and the {@code apoc.do.when} procedure drained it into a list of their own, past the per-operation element cap
 * and the query heap budget the identical SQL subquery is refused by since #10080. Each now builds its list through
 * {@code OperationHeapLimit}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10160CypherExpressionHeapLimitTest extends TestHelper {
  private static final int  OUTER_ROWS = 100;
  private static final int  INNER_ROWS = 4_000;
  private static final long CAP        = 100L;

  /** Every statement holds all of InnerT (or every neighbour of the hub) in the list of one expression. */
  private static final List<String> OVER_CAP = List.of(//
      "MATCH (o:OuterT) WHERE o.k = 0 RETURN size(COLLECT { MATCH (m:InnerT) RETURN m.pad }) AS s",//
      "MATCH (o:OuterT) WHERE o.k = 0 RETURN size(COLLECT { MATCH (m:InnerT) RETURN m.k, m.pad }) AS s",//
      "MATCH (h:Hub) RETURN size([(h)-->(l:InnerT) | l.pad]) AS s",//
      "MATCH (h:Hub) RETURN size([(h)-[:Links*1..1]->(l) WHERE l.k >= 0 | l.pad]) AS s",//
      "CALL apoc.do.when(true, 'MATCH (m:InnerT) RETURN m.pad AS p', '') YIELD value RETURN count(value) AS s",//
      // OPTIONAL suppresses "no rows", not a refusal of the heap limits: answering a null row would read as an empty result
      "OPTIONAL CALL apoc.do.when(true, 'MATCH (m:InnerT) RETURN m.pad AS p', '') YIELD value RETURN count(value) AS s");

  @AfterEach
  void resetSettings() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void everyListBuildingExpressionFailsPastTheElementCap() {
    createData();
    setCap(CAP);
    for (final String cypher : OVER_CAP)
      assertThatThrownBy(() -> drain(cypher)).as(cypher)
          .isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  @Test
  void theSameStatementsRunBelowTheCap() {
    createData();
    setCap(100_000L);
    for (final String cypher : OVER_CAP)
      assertThat(single(cypher).<Number>getProperty("s").intValue()).as(cypher).isEqualTo(INNER_ROWS);
  }

  @Test
  void theFailureNamesTheConstruct() {
    createData();
    setCap(CAP);
    assertThatThrownBy(() -> drain(OVER_CAP.get(0))).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("COLLECT subquery");
    assertThatThrownBy(() -> drain(OVER_CAP.get(2))).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("pattern comprehension");
    assertThatThrownBy(() -> drain(OVER_CAP.get(4))).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("do.when()");
  }

  @Test
  void aCollectIsChargedToTheSharedBudgetAndReleased() {
    createData();
    setCap(-1L);
    final long baseline = QueryHeapBudget.getReservedBytes();

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    for (final String cypher : OVER_CAP)
      // MORE THAN THE WHOLE BUDGET, WHICH NO RETRY CAN HELP: A LIMIT OF THE HEAP, AS THE ELEMENT CAP IS
      assertThatThrownBy(() -> drain(cypher)).as(cypher).isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

    // with room, the same statements run, and give everything back
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
    for (final String cypher : List.of(OVER_CAP.get(0), OVER_CAP.get(2)))
      assertThat(single(cypher).<Number>getProperty("s").intValue()).as(cypher).isEqualTo(INNER_ROWS);
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  /** A list given back when it is returned: one small COLLECT per outer row never adds up to the whole of them. */
  @Test
  void aCollectEvaluatedPerRowDoesNotAccumulateItsCharge() {
    createData();
    setCap(-1L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

    assertThat(drain("MATCH (o:OuterT) RETURN size(COLLECT { MATCH (m:InnerT) WHERE m.k = o.k RETURN m.pad }) AS s"))
        .isEqualTo(OUTER_ROWS);
    assertThat(drain("MATCH (o:OuterT) RETURN size([(o)-->(l) | l.pad]) AS s")).isEqualTo(OUTER_ROWS);
  }

  /**
   * The rows of a procedure outlive its call: a chained CALL holds the stream of every input row of a batch before it reads them.
   * So apoc.do.when keeps its rows charged to the query until the stream is closed, rather than releasing them as an expression does.
   */
  @Test
  void apocDoWhenKeepsItsRowsChargedUntilItsStreamIsClosed() {
    createData();
    setCap(-1L);
    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Stream<Result> rows = new DoWhen().execute(new Object[] { true, "MATCH (m:InnerT) RETURN m.pad AS p", "" }, null,
          context);
      final QueryHeapTracker tracker = context.getQueryHeapTracker();
      assertThat(tracker.getUsedBytes()).as("charged while the stream is open").isGreaterThan(0L);

      rows.close();
      assertThat(tracker.getUsedBytes()).as("given back when it is closed").isZero();
    });
  }

  /** A stream CALL abandons - a LIMIT stops reading it, a later step fails - still gives its rows back to the shared budget. */
  @Test
  void apocDoWhenGivesItsRowsBackWhenItsStreamIsAbandoned() {
    createData();
    setCap(-1L);
    final long baseline = QueryHeapBudget.getReservedBytes();
    final String call = "UNWIND range(1, 3) AS i CALL apoc.do.when(true, 'MATCH (m:InnerT) RETURN m.pad AS p', '') YIELD value ";

    assertThat(drain(call + "RETURN value LIMIT 1")).isEqualTo(1);
    assertThat(QueryHeapBudget.getReservedBytes()).as("after a LIMIT").isLessThanOrEqualTo(baseline);

    assertThatThrownBy(() -> drain(call + "RETURN 1 / (i - i) AS boom")).isInstanceOf(ArithmeticErrorException.class);
    assertThat(QueryHeapBudget.getReservedBytes()).as("after a later step failed").isLessThanOrEqualTo(baseline);
  }

  @Test
  void theValuesAreTheOnesTheBodyProjects() {
    createData();
    setCap(100_000L);
    final Result row = single("MATCH (o:OuterT) WHERE o.k = 3 RETURN COLLECT { MATCH (m:InnerT) WHERE m.k < 2 RETURN m.k ORDER BY m.k } AS one,"
        + " COLLECT { MATCH (m:InnerT) WHERE m.k < 2 RETURN m.k, m.k * 10 ORDER BY m.k } AS two");
    assertThat(row.<List<Object>>getProperty("one")).containsExactly(0, 1);
    assertThat(row.<List<Object>>getProperty("two")).containsExactly(List.of(0, 0L), List.of(1, 10L));
  }

  private void createData() {
    database.command("sql", "CREATE VERTEX TYPE OuterT");
    database.command("sql", "CREATE VERTEX TYPE InnerT");
    database.command("sql", "CREATE VERTEX TYPE Hub");
    database.command("sql", "CREATE VERTEX TYPE Leaf");
    database.command("sql", "CREATE EDGE TYPE Links");
    final String pad = "x".repeat(500);
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub").save();
      for (int i = 0; i < INNER_ROWS; i++) {
        final MutableVertex inner = database.newVertex("InnerT").set("k", i).set("pad", pad).save();
        hub.newEdge("Links", inner);
      }
      for (int i = 0; i < OUTER_ROWS; i++) {
        final MutableVertex outer = database.newVertex("OuterT").set("k", i).save();
        outer.newEdge("Links", database.newVertex("Leaf").set("pad", "y").save());
      }
    });
  }

  private void setCap(final long cap) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, cap);
  }

  private int drain(final String cypher) {
    final int[] rows = new int[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext()) {
          rs.next();
          ++rows[0];
        }
      }
    });
    return rows[0];
  }

  private Result single(final String cypher) {
    final Result[] result = new Result[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        result[0] = rs.next();
        assertThat(rs.hasNext()).isFalse();
      }
    });
    return result[0];
  }
}
