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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.MatchExecutionPlanner;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unwind operand handling in the SQL AST builder and planner.
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class SqlUnwindStatementTest {
  /**
   * Regression for #11442: the AST builder kept an UNWIND operand only when it was a bare top-level field and never kept the alias, so...
   * <p>
   * Regression tests for issue #11442: the AST builder kept an UNWIND operand only when it was a bare top-level field and
   * never kept the alias, so {@code UNWIND a.b} unwound {@code a}, {@code UNWIND :p} became a no-op clause rendering as an
   * unparseable {@code UNWIND }, and {@code UNWIND tags AS x} lost {@code x}.
   */
  @Nested
  class UnwindOperandTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("T");
        database.newDocument("T").set("name", "n1").set("tags", List.of("t1", "t2")).set("a", Map.of("b", List.of(1, 2, 3)))
            .set("$tags", List.of("d1", "d2")).save();
      });
    }

    private static SelectStatement parse(final String sql) {
      return (SelectStatement) new SQLAntlrParser(null).parse(sql);
    }

    /** Renders the statement, re-parses the rendering and checks both the text and the UNWIND node survive. */
    private static void assertRoundTrips(final String sql, final String expectedUnwind) {
      final SelectStatement first = parse(sql);
      assertThat(first.getUnwind().toString()).isEqualTo(expectedUnwind);
      final SelectStatement second = parse(first.toString());
      assertThat(second.getUnwind()).isEqualTo(first.getUnwind());
      assertThat(second.getUnwind().toString()).isEqualTo(expectedUnwind);
      assertThat(first.copy().getUnwind()).isEqualTo(first.getUnwind());
    }

    private List<Result> rows(final String sql) {
      return rows(sql, Map.of());
    }

    private List<Result> rows(final String sql, final Map<String, Object> params) {
      final List<Result> list = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, params)) {
        rs.forEachRemaining(list::add);
      }
      return list;
    }

    @Test
    void aBareFieldStillRoundTripsAndUnwindsInPlace() {
      assertRoundTrips("SELECT FROM T UNWIND tags", "UNWIND tags");
      assertRoundTrips("SELECT FROM T UNWIND tags, name", "UNWIND tags, name");
      assertThat(rows("SELECT FROM T UNWIND tags").stream().map(r -> r.<String>getProperty("tags")).toList())
          .containsExactly("t1", "t2");
    }

    @Test
    void aNestedPathWithoutAnAliasIsRejectedInsteadOfUnwindingItsHead() {
      assertThatThrownBy(() -> parse("SELECT FROM T UNWIND a.b")).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("needs an alias");
    }

    @Test
    void aParameterWithoutAnAliasIsRejectedInsteadOfBecomingANoOp() {
      assertThatThrownBy(() -> parse("SELECT FROM T UNWIND :p")).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("needs an alias");
      assertThatThrownBy(() -> parse("SELECT FROM T UNWIND tags, :p")).isInstanceOf(CommandSQLParsingException.class);
    }

    @Test
    void anAliasIsKeptAndRendered() {
      assertRoundTrips("SELECT FROM T UNWIND tags AS x", "UNWIND tags AS x");
      assertRoundTrips("SELECT FROM T UNWIND tags x", "UNWIND tags AS x");
      assertRoundTrips("SELECT FROM T UNWIND a.b AS x", "UNWIND a.b AS x");
      assertRoundTrips("SELECT FROM T UNWIND :p AS x", "UNWIND :p AS x");
      assertRoundTrips("SELECT FROM T UNWIND tags, a.b AS x", "UNWIND tags, a.b AS x");
    }

    @Test
    void anAliasedFieldWritesItsElementsIntoTheAlias() {
      final List<Result> result = rows("SELECT FROM T UNWIND tags AS x");
      assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
      // the source field is left as it was
      assertThat(result.getFirst().<List<String>>getProperty("tags")).containsExactly("t1", "t2");
    }

    @Test
    void aNestedPathUnwindsTheNestedListNotItsHead() {
      assertThat(rows("SELECT FROM T UNWIND a.b AS x").stream().map(r -> r.<Integer>getProperty("x")).toList())
          .containsExactly(1, 2, 3);
    }

    @Test
    void aParameterIsUnwound() {
      final List<Result> result = rows("SELECT FROM T UNWIND :p AS x", Map.of("p", List.<Object>of("p1", "p2")));
      assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("p1", "p2");
    }

    @Test
    void aNarrowingProjectionStillFeedsTheOperandAndKeepsTheAlias() {
      final List<Result> result = rows("SELECT name FROM T UNWIND a.b AS x");
      assertThat(result).hasSize(3);
      assertThat(result.stream().map(r -> r.<Integer>getProperty("x")).toList()).containsExactly(1, 2, 3);
      assertThat(result.getFirst().<String>getProperty("name")).isEqualTo("n1");
      assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");

      assertThat(rows("SELECT name FROM T UNWIND tags AS x").stream().map(r -> r.<String>getProperty("x")).toList())
          .containsExactly("t1", "t2");

      // the operand reads the projected row, so it sees a projection alias, as a bare field does
      assertThat(rows("SELECT a AS c FROM T UNWIND c.b AS x").stream().map(r -> r.<Integer>getProperty("x")).toList())
          .containsExactly(1, 2, 3);

      // every field the operand reads is kept for the UNWIND, not only the root of a bare path
      assertThat(rows("SELECT name FROM T UNWIND (a.b) AS x").stream().map(r -> r.<Integer>getProperty("x")).toList())
          .containsExactly(1, 2, 3);
      final List<Result> literal = rows("SELECT tags FROM T UNWIND [name, 'z'] AS x");
      assertThat(literal.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("n1", "z");
      assertThat(literal.getFirst().getPropertyNames()).containsExactlyInAnyOrder("tags", "x");

      // a back-tick quoted $name is a property the operand reads, not a context variable
      assertThat(rows("SELECT name FROM T UNWIND `$tags` AS x").stream().map(r -> r.<String>getProperty("x")).toList())
          .containsExactly("d1", "d2");

      final List<Result> parameter = rows("SELECT name FROM T UNWIND :p AS x", Map.of("p", List.<Object>of("p1", "p2")));
      assertThat(parameter.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("p1", "p2");
      assertThat(parameter.getFirst().<String>getProperty("name")).isEqualTo("n1");
    }

    @Test
    void anAliasedItemCombinesWithABareOne() {
      final List<Result> result = rows("SELECT FROM T UNWIND tags, a.b AS x");
      assertThat(result).hasSize(6);
      assertThat(result.stream().map(r -> r.getProperty("tags") + ":" + r.getProperty("x")).toList())
          .containsExactly("t1:1", "t1:2", "t1:3", "t2:1", "t2:2", "t2:3");
    }

    @Test
    void aScalarOperandIsForwardedUnderTheAlias() {
      final List<Result> result = rows("SELECT FROM T UNWIND name AS x");
      assertThat(result).hasSize(1);
      assertThat(result.getFirst().<String>getProperty("x")).isEqualTo("n1");
    }

    @Test
    void aMatchUnwindsAnAliasedOperandOfItsReturnRow() {
      final MatchStatement match = (MatchStatement) new SQLAntlrParser(null).parse(
          "MATCH {type: T, as: t} RETURN t.a AS a UNWIND a.b AS x");
      assertThat(match.getUnwind().toString()).isEqualTo("UNWIND a.b AS x");
      assertThat(((MatchStatement) new SQLAntlrParser(null).parse(match.toString())).getUnwind()).isEqualTo(match.getUnwind());

      assertThat(rows("MATCH {type: T, as: t} RETURN t.a AS a UNWIND a.b AS x").stream().map(r -> r.<Integer>getProperty("x"))
          .toList()).containsExactly(1, 2, 3);
      assertThat(rows("MATCH {type: T, as: t} RETURN t.tags AS tags UNWIND tags AS x").stream()
          .map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
      // the root of the operand is a pattern alias the RETURN does not project: it is brought in for the UNWIND only
      final List<Result> fromPatternAlias = rows("MATCH {type: T, as: t} RETURN t.name AS n UNWIND t.tags AS x");
      assertThat(fromPatternAlias.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
      assertThat(fromPatternAlias.getFirst().getPropertyNames()).containsExactlyInAnyOrder("n", "x");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("MATCH {type: T, as: t} RETURN t.a AS a UNWIND a.b"))
          .isInstanceOf(CommandSQLParsingException.class);
    }

    @Test
    void theFacetsAmbiguityStillReadsAsAnAliasedUnwind() {
      final SelectStatement s = parse("SELECT FROM T FACETS unwind: p TERMS");
      assertThat(s.getUnwind().toString()).isEqualTo("UNWIND :p AS TERMS");
      assertThat(parse(s.toString()).getUnwind()).isEqualTo(s.getUnwind());
    }
  }

  /**
   * Regression for #11537: under a narrowing projection, an UNWIND AS x whose operand needs the record itself (@rid, out()) or reads a...
   * <p>
   * Regression tests for issue #11537: under a narrowing projection, an {@code UNWIND <operand> AS x} whose operand needs
   * the record itself ({@code @rid}, {@code out()}) or reads a field only inside a method or index argument or a
   * {@code CASE} evaluated against a projected row that lacked it, and unwound {@code null}.
   */
  @Nested
  class UnwindOperandUnderNarrowingProjectionTest extends TestHelper {
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

  /**
   * Regression for #11563: under a narrowing projection, an UNWIND AS x whose operand reads a name only the projected row holds (a...
   * <p>
   * Regression tests for issue #11563: under a narrowing projection, an {@code UNWIND <operand> AS x} whose operand reads a
   * name only the projected row holds (a renamed projection alias, an earlier UNWIND item) AND needs the record itself
   * ({@code @rid}, {@code out()}) read the record part as {@code null}, because it was evaluated against the projected row
   * alone. The #11537 fix covered only an operand that reads no such name.
   */
  @Nested
  class UnwindOperandReadingAnAliasAndTheRecordTest extends TestHelper {
    private RID aliceRid;
    private RID docRid;

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
        docRid = database.newDocument("T").set("name", "n1").set("tags", List.of("t1", "t2")).set("b", "q").save().getIdentity();
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
    void anOperandReadingAnEarlierUnwindItemAndARecordAttribute() {
      final List<Result> result = rows("SELECT name FROM T UNWIND tags AS x, [x, @rid] AS y");
      assertThat(result).hasSize(4);
      assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
          .containsExactly("t1:t1", "t1:" + docRid, "t2:t2", "t2:" + docRid);
      assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x", "y");
    }

    @Test
    void anOperandReadingARenamedProjectionAliasAndARecordAttribute() {
      // `name` is the projected `tags` (not the record's "n1"), `@rid` is the record's
      final List<Result> result = rows("SELECT tags AS name FROM T UNWIND [name, @rid] AS x");
      assertThat(result).hasSize(2);
      assertThat(result.get(0).<Object>getProperty("x")).isEqualTo(List.of("t1", "t2"));
      assertThat(result.get(1).<Object>getProperty("x")).isEqualTo(docRid);
      assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
    }

    @Test
    void aRecordLevelFunctionWhosePropertyNameSpellsAComputedAlias() {
      // `name` after the dot is the edge's vertex property, not a row read, but the textual check sees it as the alias
      final List<Result> result = rows("SELECT name.toUpperCase() AS name FROM P WHERE name = 'alice' UNWIND out('K').name AS x");
      assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactlyInAnyOrder("bob", "carol");
      assertThat(result.getFirst().<String>getProperty("name")).isEqualTo("ALICE");
      assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");
    }

    @Test
    void aRecordLevelFunctionNextToARenamedAlias() {
      final List<Result> result = rows("SELECT name AS n FROM P WHERE name = 'alice' UNWIND [n, out('K').size(), @rid] AS x");
      assertThat(result.stream().map(r -> r.<Object>getProperty("x")).toList()).containsExactly("alice", 2, aliceRid);
      assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("n", "x");
    }

    @Test
    void aProjectedAliasShadowsTheRecordFieldOfTheSameName() {
      // the projection writes `b` as null: the operand reads that null, not the record's "q"
      final List<Result> result = rows("SELECT null AS b FROM T UNWIND tags AS x, [x, b, @rid] AS y");
      assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
          .containsExactly("t1:t1", "t1:null", "t1:" + docRid, "t2:t2", "t2:null", "t2:" + docRid);
    }

    @Test
    void anExcludedNameIsNotReadBackFromTheRecord() {
      // `!b` drops the column: the operand reads null for it, as it does on the row, never the record's "q"
      final List<Result> result = rows("SELECT *, !b FROM T UNWIND tags AS x, [x, b, @rid] AS y");
      assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
          .containsExactly("t1:t1", "t1:null", "t1:" + docRid, "t2:t2", "t2:null", "t2:" + docRid);
      assertThat(result.getFirst().getPropertyNames()).doesNotContain("b").noneMatch(GeneratedAlias::is);
    }

    @Test
    void aStarProjectionDropsTheGeneratedColumnsAndKeepsItsExclusions() {
      // `tags AS x` is evaluated early (#11537): under `*` its generated column and the excluded `b` used to reach the row
      final List<Result> result = rows("SELECT *, !b FROM T UNWIND tags AS x");
      assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
      for (final Result row : result) {
        assertThat(row.getPropertyNames()).contains("name", "tags", "x").doesNotContain("b").noneMatch(GeneratedAlias::is);
        assertThat(row.toJSON().keySet()).doesNotContain("b").noneMatch(GeneratedAlias::is);
      }
    }

    @Test
    void theCarriedRecordNeverReachesTheResult() {
      final List<Result> result = rows("SELECT name AS n FROM P WHERE name = 'alice' UNWIND [n, @rid] AS x");
      assertThat(result).hasSize(2);
      for (final Result row : result) {
        assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("n", "x");
        assertThat(row.toJSON().keySet()).noneMatch(GeneratedAlias::is);
      }
    }

    @Test
    void anAggregatedRowStillReadsOnlyTheProjectedRow() {
      final List<Result> result = rows("SELECT name, count(*) AS c FROM T GROUP BY name UNWIND [name, c] AS x");
      assertThat(result.stream().map(r -> String.valueOf(r.<Object>getProperty("x"))).toList()).containsExactly("n1", "1");
      assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "c", "x");
    }

    @Test
    void aMatchReturnRowIsNotARecord() {
      final List<Result> result = rows("MATCH {type: T, as: t} RETURN t.name AS n, t.tags AS g UNWIND g AS x, [x, n] AS y");
      assertThat(result.stream().map(r -> r.getProperty("x") + ":" + r.getProperty("y")).toList())
          .containsExactly("t1:t1", "t1:n1", "t2:t2", "t2:n1");
      assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("n", "g", "x", "y");
    }
  }
}
