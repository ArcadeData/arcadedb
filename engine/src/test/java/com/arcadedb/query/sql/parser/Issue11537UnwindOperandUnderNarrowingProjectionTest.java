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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.MatchExecutionPlanner;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11537: under a narrowing projection, an {@code UNWIND <operand> AS x} whose operand needs
 * the record itself ({@code @rid}, {@code out()}) or reads a field only inside a method or index argument or a
 * {@code CASE} evaluated against a projected row that lacked it, and unwound {@code null}.
 */
class Issue11537UnwindOperandUnderNarrowingProjectionTest extends TestHelper {
  private RID aliceRid;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("P");
      database.getSchema().createEdgeType("K");
      final MutableVertex alice = database.newVertex("P").set("name", "alice").save();
      final MutableVertex bob = database.newVertex("P").set("name", "bob").save();
      final MutableVertex carol = database.newVertex("P").set("name", "carol").save();
      alice.newEdge("K", bob).save();
      alice.newEdge("K", carol).save();
      aliceRid = alice.getIdentity();

      database.getSchema().createDocumentType("T");
      database.newDocument("T").set("name", "n1").set("tags", List.of("t1", "t2")).set("a", List.of("p", "q")).set("b", "q").set("s", "pq")
          .set("m", Map.of("key", List.of(1, 2))).set("k", "key").set("flag", true).save();
    });
  }

  private List<Result> rows(final String sql) {
    final List<Result> list = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      rs.forEachRemaining(list::add);
    }
    return list;
  }

  @Test
  void aRecordAttributeIsReadFromTheRecord() {
    final List<Result> result = rows("SELECT name FROM P WHERE name = 'alice' UNWIND @rid AS x");
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().<RID>getProperty("x")).isEqualTo(aliceRid);
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
  }

  @Test
  void aRecordLevelFunctionIsReadFromTheRecord() {
    final List<Result> result = rows("SELECT name FROM P WHERE name = 'alice' UNWIND out('K') AS x");
    assertThat(result).hasSize(2);
    assertThat(result.stream().map(r -> ((Vertex) r.<Object>getProperty("x")).getString("name")).toList())
        .containsExactlyInAnyOrder("bob", "carol");
    assertThat(result.getFirst().<String>getProperty("name")).isEqualTo("alice");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");

    assertThat(rows("SELECT name FROM P WHERE name = 'alice' UNWIND out('K').name AS x").stream()
        .map(r -> r.<String>getProperty("x")).toList()).containsExactlyInAnyOrder("bob", "carol");
  }

  @Test
  void aFieldReadOnlyInsideAnIndexArgumentIsSeen() {
    final List<Result> result = rows("SELECT name FROM T UNWIND m[k] AS x");
    assertThat(result.stream().map(r -> r.<Integer>getProperty("x")).toList()).containsExactly(1, 2);
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
  }

  @Test
  void aFieldReadOnlyInsideAMethodArgumentIsSeen() {
    final List<Result> result = rows("SELECT name FROM T UNWIND s.indexOf(b) AS x");
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().<Integer>getProperty("x")).isEqualTo(1);
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
  }

  @Test
  void aCaseExpressionSeesTheFieldsItReads() {
    final List<Result> result = rows("SELECT name FROM T UNWIND CASE WHEN flag = true THEN tags ELSE [] END AS x");
    assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
  }

  @Test
  void anOperandReadingARenamedProjectionAliasStillReadsTheProjectedRow() {
    // `name` is both a field of the record ("n1") and the alias of the projected `tags`: the operand reads the projected row
    final List<Result> result = rows("SELECT tags AS name FROM T UNWIND name AS x");
    assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
  }

  @Test
  void aLaterOperandReadsTheAliasAnEarlierItemWrote() {
    // the second operand reads `x`, which exists only once the first item has written it: it is not evaluated early
    final List<Result> result = rows("SELECT name FROM T UNWIND tags AS x, [x, b] AS y");
    assertThat(result).hasSize(4);
    assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
        .containsExactly("t1:t1", "t1:q", "t2:t2", "t2:q");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x", "y");
  }

  @Test
  void anOperandMixingEarlyAndLateReadsIsEvaluatedPerItem() {
    // the first operand needs the record, the second the alias the first wrote: each is evaluated where it can be
    final List<Result> result = rows("SELECT name FROM P WHERE name = 'alice' UNWIND out('K').name AS x, [x] AS y");
    assertThat(result.stream().map(r -> r.<String>getProperty("y")).toList()).containsExactlyInAnyOrder("bob", "carol");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x", "y");
  }

  @Test
  void aMatchReturnFeedsACaseOperandFromThePatternRow() {
    final List<Result> result = rows("MATCH {type: T, as: t} RETURN t.name AS n UNWIND CASE WHEN true THEN t.tags END AS x");
    assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("n", "x");
  }

  @Test
  void anAggregatedRowKeepsTheOperandOnTheProjectedRow() {
    // the aggregated row is not a record, so the operand reads the projected row: `c` is the aggregate's alias
    final List<Result> result = rows("SELECT name, count(*) AS c FROM T GROUP BY name UNWIND [name, c] AS x");
    assertThat(result.stream().map(r -> String.valueOf(r.<Object>getProperty("x"))).toList()).containsExactly("n1", "1");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "c", "x");
  }

  @Test
  void aPlannerAskedForASecondPlanDoesNotSeeItsOwnRewrite() {
    final MatchStatement statement = (MatchStatement) new SQLAntlrParser(database).parse(
        "MATCH {type: T, as: t} RETURN t.name AS n UNWIND CASE WHEN true THEN t.tags END AS x");
    final String unwindBefore = statement.getUnwind().toString();
    final MatchExecutionPlanner planner = new MatchExecutionPlanner(statement);
    for (int run = 0; run < 2; run++) {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final InternalExecutionPlan plan = planner.createExecutionPlan(context);
      final List<Result> result = new ArrayList<>();
      try (final ResultSet rs = new LocalResultSet(plan)) {
        rs.forEachRemaining(result::add);
      }
      assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).as("plan %d", run).containsExactly("t1", "t2");
    }
    assertThat(statement.getUnwind().toString()).isEqualTo(unwindBefore);

    // the SELECT planner, through the statement cache and its plan cache
    for (int run = 0; run < 2; run++)
      assertThat(rows("SELECT name FROM P WHERE name = 'alice' UNWIND out('K').name AS x").stream()
          .map(r -> r.<String>getProperty("x")).toList()).as("run %d", run).containsExactlyInAnyOrder("bob", "carol");
  }
}
