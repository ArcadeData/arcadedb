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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL {@code IN :param} with a collection parameter must use the index and return the same rows as
 * the literal list form {@code IN [1, 2, 3]}.
 * <p>
 * The HTTP layer deserializes JSON numeric arrays into primitive arrays (long[], int[], double[])
 * via toMap(true). The indexed expansion in FetchFromIndexStep.cartesianProduct() must treat those
 * primitive arrays as multi-value keys and look each element up individually, exactly like the
 * non-indexed IN evaluator. When it did not, the whole array was used as a single index key and
 * matched nothing.
 * <p>
 * Also holds the IN / NOT IN index-planning regressions #6640, #6796 and #741 (each test keeps its issue number in
 * its {@code @DisplayName}); their fixtures are created by the private {@code create*} methods below.
 */
class InParamIndexExpansionTest extends TestHelper {

  private static final int    ROWS      = 500;
  private static final String PRESENT   = "d_eb202a039f14c8bb";
  private static final String ABSENT    = "value_matching_nothing";

  @Override
  protected void beginTest() {
    createIssueItems();
    createDocuments();
    createTransfers();
    createTracks();
  }

  private void createIssueItems() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("IssueItem");
      database.getSchema().getType("IssueItem").createProperty("code", Type.INTEGER);
      database.getSchema().buildTypeIndex("IssueItem", new String[] { "code" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false)
          .create();

      database.newDocument("IssueItem").set("code", 1, "name", "one").save();
      database.newDocument("IssueItem").set("code", 2, "name", "two").save();
      database.newDocument("IssueItem").set("code", 3, "name", "three").save();
    });
  }

  // #6796
  private void createDocuments() {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Document");
      type.createProperty("doc_id", Type.STRING);
      type.createProperty("ocr_status", Type.STRING);
      type.createProperty("tenant", Type.INTEGER);
      database.getSchema().buildTypeIndex("Document", new String[] { "doc_id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      database.getSchema().buildTypeIndex("Document", new String[] { "tenant" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });

    database.transaction(() -> {
      // Row 0 carries the reporter's exact doc_id so the "matches one row" case is deterministic.
      database.newVertex("Document").set("doc_id", PRESENT).set("ocr_status", PRESENT).set("tenant", 1).save();
      for (int i = 1; i < ROWS; i++)
        database.newVertex("Document").set("doc_id", "d_" + i).set("ocr_status", "d_" + i).set("tenant", i % 5).save();
    });
  }

  // #741
  private void createTransfers() {
    database.transaction(() -> {
      final VertexType account = database.getSchema().createVertexType("Account");
      account.createProperty("accountNumber", Type.STRING);

      final EdgeType transfer = database.getSchema().createEdgeType("TRANSFER");
      transfer.createProperty("transactionId", Type.STRING);
      transfer.createProperty("date", Type.DATE);

      // Composite unique index on (transactionId, date), mirroring the client's TRANSFER_PK
      database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withIndexName("TRANSFER_PK")
          .create();
    });

    database.transaction(() -> {
      final var a1 = database.newVertex("Account").set("accountNumber", "A1").save();
      final var a2 = database.newVertex("Account").set("accountNumber", "A2").save();

      a1.newEdge("TRANSFER", a2).set("transactionId", "S28719720", "date", "2026-01-01").save();
      a1.newEdge("TRANSFER", a2).set("transactionId", "74529884", "date", "2026-02-28").save();
      // A second transfer with the same id on another day: a leading-column seek must return both.
      a1.newEdge("TRANSFER", a2).set("transactionId", "74529884", "date", "2026-02-27").save();
      // noise edges that must NOT match the IN list
      for (int i = 0; i < 50; i++)
        a1.newEdge("TRANSFER", a2).set("transactionId", "NOISE-" + i, "date", "2026-03-%02d".formatted(i % 28 + 1)).save();
    });
  }

  // #6640
  private void createTracks() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Track");
      database.getSchema().getType("Track").createProperty("trackId", Type.LONG);
      database.getSchema().buildTypeIndex("Track", new String[] { "trackId" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .create();
    });
  }

  @Test
  void literalInListUsesIndexAndReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "select code from IssueItem where code in [1, 2, 3] order by code");
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  @Test
  void namedListParamInWithIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes order by code",
          Map.of("codes", List.of(1L, 2L, 3L)));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  // long[] reproduces the HTTP JSON deserialization that toMap(true) performs for integer arrays.
  @Test
  void namedPrimitiveLongArrayParamInWithIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes order by code",
          Map.of("codes", new long[] { 1L, 2L, 3L }));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  // int[] is another primitive-array shape toMap(true) can produce for small integer JSON arrays.
  @Test
  void namedPrimitiveIntArrayParamInWithIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes order by code",
          Map.of("codes", new int[] { 1, 2, 3 }));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  // double[] is produced for floating-point JSON arrays; values must coerce to the INTEGER index key.
  @Test
  void namedPrimitiveDoubleArrayParamInWithIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes order by code",
          Map.of("codes", new double[] { 1.0, 2.0, 3.0 }));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  @Test
  void namedObjectArrayParamInWithIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes order by code",
          Map.of("codes", new Object[] { 1L, 2L, 3L }));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  @Test
  void positionalListParamInWithIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in ? order by code",
          List.of(1L, 2L, 3L));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  @Test
  void namedListParamInExplainUsesIndex() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "explain select code from IssueItem where code in :codes",
          Map.of("codes", List.of(1L, 2L, 3L)));
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("FETCH FROM INDEX");
    });
  }

  // Control: arithmetic on the left side forces the non-indexed evaluator, which already worked.
  @Test
  void namedListParamInWithoutIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where (code + 0) in :codes order by code",
          Map.of("codes", List.of(1L, 2L, 3L)));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  @Test
  void namedListParamInWithIndexReturnsPartialRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes order by code",
          Map.of("codes", List.of(1L, 3L)));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 3);
    });
  }

  @Test
  void namedListParamInWithIndexReturnsSingleRow() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes",
          Map.of("codes", List.of(2L)));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(2);
    });
  }

  /*
   * #6796: {@code WHERE indexedProperty NOT IN [...]} returned the same rows as {@code IN} - the negation was
   * silently lost. The index planner intercepted the {@code InCondition} on an indexed property and built a
   * cursor over the values that DO match, without ever consulting the condition's {@code not} flag; the
   * unindexed sibling property of the same rows answered correctly, which is what made it look index-dependent.
   * <p>
   * A negated leaf has no complement in {@link com.arcadedb.query.sql.executor.FetchFromIndexStep}, so the only
   * correct answer is for {@code isIndexAware()} to decline whenever {@code not} is set - for every right-hand
   * shape - and let the full-scan evaluator apply the negation, which it has always done correctly.
   * <p>
   * This test locks the reporter's shape down end to end: a STRING property under a UNIQUE index, both a value
   * that matches exactly one row and a value that matches nothing, and the sibling unindexed property that has
   * to keep agreeing with it. It complements the #6640 tests below, which covers the
   * numeric/bound-parameter forms, and extends the guard to the shapes that class never reached: NOT IN under
   * an AND with a second indexed predicate, under an OR, against a subquery, and against a composite index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  @DisplayName("#6796 not in on unique indexed string excludes the listed row")
  @Test
  void notInOnUniqueIndexedStringExcludesTheListedRow() {
    database.transaction(() -> {
      assertThat(count("select from Document where doc_id in ['" + PRESENT + "']")).isEqualTo(1);
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "']")).isEqualTo(ROWS - 1);
      assertThat(count("select from Document where doc_id not in ['" + ABSENT + "']")).isEqualTo(ROWS);
    });
  }

  @DisplayName("#6796 not in agrees with the unindexed sibling property")
  @Test
  void notInAgreesWithTheUnindexedSiblingProperty() {
    database.transaction(() -> {
      // ocr_status holds exactly the same values as doc_id but carries no index: the two must never disagree.
      assertThat(count("select from Document where ocr_status not in ['" + PRESENT + "']"))
          .isEqualTo(count("select from Document where doc_id not in ['" + PRESENT + "']"));
      assertThat(count("select from Document where ocr_status not in ['" + ABSENT + "']"))
          .isEqualTo(count("select from Document where doc_id not in ['" + ABSENT + "']"));
    });
  }

  @DisplayName("#6796 not in agrees with the equivalent rewrites")
  @Test
  void notInAgreesWithTheEquivalentRewrites() {
    database.transaction(() -> {
      final long expected = ROWS - 1;
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "']")).isEqualTo(expected);
      assertThat(count("select from Document where not (doc_id in ['" + PRESENT + "'])")).isEqualTo(expected);
      assertThat(count("select from Document where doc_id <> '" + PRESENT + "'")).isEqualTo(expected);
    });
  }

  @DisplayName("#6796 not in declines the index for every right hand shape")
  @Test
  void notInDeclinesTheIndexForEveryRightHandShape() {
    database.transaction(() -> {
      assertThat(planOf("select from Document where doc_id not in ['" + PRESENT + "']")).doesNotContain("FETCH FROM INDEX");
      assertThat(planOf("select from Document where doc_id not in ('" + PRESENT + "')")).doesNotContain("FETCH FROM INDEX");
      final ResultSet explain = database.query("sql", "explain select from Document where doc_id not in :ids",
          Map.of("ids", List.of(PRESENT)));
      assertThat(explain.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("FETCH FROM INDEX");
    });
  }

  @DisplayName("#6796 not in stays negated when another indexed predicate drives the fetch")
  @Test
  void notInStaysNegatedWhenAnotherIndexedPredicateDrivesTheFetch() {
    database.transaction(() -> {
      // tenant is indexed, so the planner is free to drive the fetch from that index; the NOT IN left over as
      // a residual filter must still exclude, not include. tenant=1 covers rows 1, 6, 11, ... plus row 0.
      final long tenant1 = count("select from Document where tenant = 1");
      assertThat(count("select from Document where tenant = 1 and doc_id not in ['" + PRESENT + "']")).isEqualTo(tenant1 - 1);
      assertThat(count("select from Document where tenant = 1 and doc_id in ['" + PRESENT + "']")).isEqualTo(1);
    });
  }

  @DisplayName("#6796 not in stays negated under or")
  @Test
  void notInStaysNegatedUnderOr() {
    database.transaction(() -> {
      // Every row except the one whose doc_id is excluded, plus that row back again through the tenant leg.
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "'] or tenant = 1")).isEqualTo(ROWS);
      assertThat(count("select from Document where doc_id not in ['" + PRESENT + "'] or tenant = 99")).isEqualTo(ROWS - 1);
    });
  }

  @DisplayName("#6796 not in against a subquery excludes the matched rows")
  @Test
  void notInAgainstASubqueryExcludesTheMatchedRows() {
    database.transaction(() -> {
      assertThat(count("select from Document where doc_id not in (select doc_id from Document where tenant = 1)"))
          .isEqualTo(ROWS - count("select from Document where tenant = 1"));
    });
  }

  @DisplayName("#6796 not in on a composite index leading column excludes the listed rows")
  @Test
  void notInOnACompositeIndexLeadingColumnExcludesTheListedRows() {
    database.transaction(() -> database.getSchema().buildTypeIndex("Document", new String[] { "tenant", "doc_id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create());

    database.transaction(() -> {
      final long tenant0 = count("select from Document where tenant = 0");
      assertThat(count("select from Document where tenant not in [0]")).isEqualTo(ROWS - tenant0);
      assertThat(planOf("select from Document where tenant not in [0]")).doesNotContain("FETCH FROM INDEX");
    });
  }

  @DisplayName("#6796 not in with a null in the list excludes every row")
  @Test
  void notInWithANullInTheListExcludesEveryRow() {
    database.transaction(() -> {
      // SQL three-valued logic: a miss against a list holding NULL is UNKNOWN, not FALSE, so NOT IN keeps
      // nothing. The memoized probe records the null element separately for exactly this reason.
      assertThat(count("select from Document where doc_id not in ['" + ABSENT + "', null]")).isZero();
      assertThat(count("select from Document where doc_id in ['" + PRESENT + "', null]")).isEqualTo(1);
    });
  }

  @DisplayName("#6796 a right hand side that is not constant is still evaluated per row")
  @Test
  void aRightHandSideThatIsNotConstantIsStillEvaluatedPerRow() {
    database.transaction(() -> {
      // ocr_status is a property, so the list is a different value on every row and must not be memoized:
      // every row matches itself, so IN keeps all of them and NOT IN keeps none.
      assertThat(count("select from Document where doc_id in [ocr_status]")).isEqualTo(ROWS);
      assertThat(count("select from Document where doc_id not in [ocr_status]")).isZero();
    });
  }

  @DisplayName("#6796 a bound parameter list is memoized without losing the negation")
  @Test
  void aBoundParameterListIsMemoizedWithoutLosingTheNegation() {
    database.transaction(() -> {
      assertThat(database.query("sql", "select from Document where doc_id not in :ids", Map.of("ids", List.of(PRESENT)))
          .stream().count()).isEqualTo(ROWS - 1);
      assertThat(database.query("sql", "select from Document where doc_id in :ids", Map.of("ids", List.of(PRESENT)))
          .stream().count()).isEqualTo(1);
    });
  }

  @DisplayName("#6796 not in cost does not grow with the size of a constant list")
  @Test
  @Tag("slow")
  void notInCostDoesNotGrowWithTheSizeOfAConstantList() {
    final int extraRows = 20_000;
    database.transaction(() -> {
      for (int i = 0; i < extraRows; i++)
        database.newVertex("Document").set("doc_id", "bulk_" + i).set("tenant", i % 5).save();
    });

    // NOT IN has no index cursor to use, so the whole type is scanned with the condition as a residual filter.
    // Before the constant right-hand side was hoisted and hashed, that filter rebuilt the entire list from the
    // parse tree and rescanned it linearly for every row: 20,000 rows against a 2,000-value list took ~1s and
    // grew linearly in both factors, which put the reported 15,000-value NOT IN over a 36M-row type in the hours.
    database.transaction(() -> assertThat(count(notInQuery(200))).isEqualTo(ROWS + extraRows));

    // assertStayedUnder, not assertGaveUpWithin: the bound IS the assertion, standing in for "the per-row cost
    // no longer carries the list". Widening it would not be a safe loosening, it would delete the coverage.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    database.transaction(() -> assertThat(count(notInQuery(20_000))).isEqualTo(ROWS + extraRows));
    stopwatch.assertStayedUnder(5_000,
        "a 20,000-value NOT IN over 20,500 rows paying for the list once, not once per row");
  }

  private String notInQuery(final int listSize) {
    final StringBuilder sql = new StringBuilder("select from Document where doc_id not in [");
    for (int i = 0; i < listSize; i++) {
      if (i > 0)
        sql.append(',');
      sql.append("'missing_").append(i).append('\'');
    }
    return sql.append(']').toString();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.stream().count();
    }
  }

  private String planOf(final String query) {
    try (final ResultSet rs = database.query("sql", "explain " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  /*
   * Issue #741 (Locstat): SQL {@code IN} operator on the leading column of a composite index on an
   * EDGE type reported as not using the index.
   * <p>
   * Client scenario (verbatim query):
   * <pre>
   *   -- TRANSFER is a high-volume edge type with a composite UNIQUE index TRANSFER_PK on (transactionId STRING, date DATE)
   *   SELECT * FROM TRANSFER WHERE transactionId IN ('S28719720','74529884')  -- reported: full scan
   *   SELECT * FROM TRANSFER WHERE transactionId = '74529884' AND date = '2026-02-28' -- uses index
   * </pre>
   * The reported symptom matches #6640: the parenthesized literal-list form {@code IN (v1, v2)} did not
   * use the index (only the bracket-array {@code IN [..]} and bound-parameter {@code IN ?}/{@code :name}
   * forms did). This test pins that the parenthesized form now uses the index, including on an EDGE type
   * with a composite unique index over a STRING and a DATE column where only the leading column is filtered.
   * <p>
   * Reproducing the client's shape also surfaced a correctness bug in the path it now takes: the planner drops
   * the {@code ORDER BY} step whenever the index key covers the sort ({@code fullySorted}), on the premise that
   * {@code FetchFromIndexStep} yields entries in key order - but a multi-value key opened one cursor per {@code IN}
   * value in <em>list</em> order, so {@code WHERE transactionId IN ('S28719720','74529884') ORDER BY transactionId}
   * answered {@code S28719720} first with nothing left to put it right. The seeks are now opened in index order
   * (honouring the direction), which keeps the sort elimination and its early-exit on {@code LIMIT}.
   */

  // The client's exact syntax: parenthesized literal IN list.
  @DisplayName("#741 parenthesized in on leading column uses index")
  @Test
  void parenthesizedInOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId IN ('S28719720','74529884')");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER_PK");
    });
  }

  @DisplayName("#741 bracket in on leading column uses index")
  @Test
  void bracketInOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId IN ['S28719720','74529884']");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER_PK");
    });
  }

  @DisplayName("#741 parenthesized in returns exactly the matching edges")
  @Test
  void parenthesizedInReturnsExactlyTheMatchingEdges() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT transactionId FROM TRANSFER WHERE transactionId IN ('S28719720','74529884')");
      final List<String> ids = rs.stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(ids).containsExactlyInAnyOrder("74529884", "74529884", "S28719720");
    });
  }

  // Control: the equality form the client confirmed already uses the index.
  @DisplayName("#741 equality on leading column uses index")
  @Test
  void equalityOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId = '74529884' AND date = '2026-02-28'");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER_PK");
    });
  }

  // The OR form suggested to the client as a workaround: one seek per branch, still through the index.
  @DisplayName("#741 or of equalities on leading column uses index")
  @Test
  void orOfEqualitiesOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final String query = "SELECT transactionId FROM TRANSFER WHERE transactionId = 'S28719720' OR transactionId = '74529884'";
      final String plan = database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER_PK");
      assertThat(plan).doesNotContain("FETCH FROM TYPE");

      final List<String> ids = database.query("sql", query).stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(ids).containsExactlyInAnyOrder("74529884", "74529884", "S28719720");
    });
  }

  @DisplayName("#741 in with order by on leading column keeps the sort elimination and returns sorted rows")
  @Test
  void inWithOrderByOnLeadingColumnKeepsTheSortEliminationAndReturnsSortedRows() {
    database.transaction(() -> {
      // The list deliberately names the greater value first: before the fix the rows came back in list order.
      final String query = "SELECT transactionId, date FROM TRANSFER WHERE transactionId IN ('S28719720','74529884') ORDER BY transactionId";

      final String plan = database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER_PK");
      assertThat(plan).doesNotContain("ORDER BY");

      final List<String> ids = database.query("sql", query).stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(ids).containsExactly("74529884", "74529884", "S28719720");

      final List<String> desc = database.query("sql", query + " DESC").stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(desc).containsExactly("S28719720", "74529884", "74529884");
    });
  }

  @DisplayName("#741 in with order by on both key columns returns rows in key order")
  @Test
  void inWithOrderByOnBothKeyColumnsReturnsRowsInKeyOrder() {
    database.transaction(() -> {
      final String query = "SELECT transactionId, date FROM TRANSFER WHERE transactionId IN ('S28719720','74529884') ORDER BY transactionId, date";
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3)).doesNotContain("ORDER BY");

      final List<String> rows = database.query("sql", query).stream()
          .map(r -> r.getProperty("transactionId") + "/" + r.getProperty("date")).toList();
      assertThat(rows).containsExactly("74529884/2026-02-27", "74529884/2026-02-28", "S28719720/2026-01-01");

      final String descending = query.replace("ORDER BY transactionId, date", "ORDER BY transactionId DESC, date DESC");
      assertThat(database.query("sql", "EXPLAIN " + descending).getExecutionPlan().get().prettyPrint(0, 3)).doesNotContain("ORDER BY");
      final List<String> desc = database.query("sql", descending).stream()
          .map(r -> r.getProperty("transactionId") + "/" + r.getProperty("date")).toList();
      assertThat(desc).containsExactly("S28719720/2026-01-01", "74529884/2026-02-28", "74529884/2026-02-27");
    });
  }

  @DisplayName("#741 in on trailing column with order by returns sorted rows")
  @Test
  void inOnTrailingColumnWithOrderByReturnsSortedRows() {
    database.transaction(() -> {
      // Equality on the leading column, IN on the trailing one, listed out of order.
      final String query = "SELECT date FROM TRANSFER WHERE transactionId = '74529884' AND date IN ('2026-02-28', '2026-02-27') ORDER BY date";
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3)).doesNotContain("ORDER BY");

      final List<Object> dates = database.query("sql", query).stream().map(r -> r.getProperty("date")).toList();
      assertThat(dates).containsExactly(LocalDate.of(2026, 2, 27), LocalDate.of(2026, 2, 28));

      final List<Object> desc = database.query("sql", query + " DESC").stream().map(r -> r.getProperty("date")).toList();
      assertThat(desc).containsExactly(LocalDate.of(2026, 2, 28), LocalDate.of(2026, 2, 27));
    });
  }

  @DisplayName("#741 in with order by and limit returns the first rows in key order")
  @Test
  void inWithOrderByAndLimitReturnsTheFirstRowsInKeyOrder() {
    database.transaction(() -> {
      // With the sort step gone, LIMIT stops the index walk early: the walk must therefore start at the right end.
      final List<String> first = database.query("sql",
              "SELECT transactionId FROM TRANSFER WHERE transactionId IN ('S28719720','74529884') ORDER BY transactionId LIMIT 1")
          .stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(first).containsExactly("74529884");

      final List<String> last = database.query("sql",
              "SELECT transactionId FROM TRANSFER WHERE transactionId IN ('74529884','S28719720') ORDER BY transactionId DESC LIMIT 1")
          .stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(last).containsExactly("S28719720");
    });
  }

  @DisplayName("#741 bound parameter list with order by returns sorted rows")
  @Test
  void boundParameterListWithOrderByReturnsSortedRows() {
    database.transaction(() -> {
      final List<String> ids = database.query("sql",
              "SELECT transactionId FROM TRANSFER WHERE transactionId IN :ids ORDER BY transactionId",
              Map.of("ids", List.of("S28719720", "74529884")))
          .stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(ids).containsExactly("74529884", "74529884", "S28719720");
    });
  }

  @DisplayName("#741 repeated in values are sought once and returned once")
  @Test
  void repeatedInValuesAreSoughtOnceAndReturnedOnce() {
    database.transaction(() -> {
      final List<String> ids = database.query("sql",
              "SELECT transactionId FROM TRANSFER WHERE transactionId IN ('74529884', '74529884', 'S28719720') ORDER BY transactionId")
          .stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(ids).containsExactly("74529884", "74529884", "S28719720");
    });
  }

  @DisplayName("#741 in values are ordered as the index stores them not as written")
  @Test
  void inValuesAreOrderedAsTheIndexStoresThemNotAsWritten() {
    // A numeric key: the literals '10' (a String) and 9 (a number) must sort as numbers, 9 before 10, the way the
    // index does once it has narrowed them - not as the Strings "10" < "9".
    database.transaction(() -> {
      database.getSchema().createVertexType("OrderedTrack").createProperty("trackId", Type.LONG);
      database.getSchema().buildTypeIndex("OrderedTrack", new String[] { "trackId" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      for (long i = 0; i < 20; i++)
        database.newVertex("OrderedTrack").set("trackId", i).save();
    });
    database.transaction(() -> {
      // '10' and 10 narrow to the same key: one seek, one row.
      final String query = "SELECT trackId FROM OrderedTrack WHERE trackId IN ('10', 9, 15, '2', 10) ORDER BY trackId";
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3))
          .contains("FETCH FROM INDEX").doesNotContain("ORDER BY");
      final List<Long> ids = database.query("sql", query).stream().map(r -> r.<Long>getProperty("trackId")).toList();
      assertThat(ids).containsExactly(2L, 9L, 10L, 15L);
    });

    // A case-insensitive key: the index still answers the IN (the folded seeks find "Beta" through 'beta' and "Delta"
    // through 'DELTA'), but it iterates its FOLDED keys, which is not the order of the values ORDER BY sorts on. Since
    // issue #8766 (#8700) a folded index never stands in for the sort, so the ORDER BY step stays and the rows come
    // back in value order - "Beta" and "Delta" before "alpha" - exactly as on an unindexed type.
    database.transaction(() -> {
      database.getSchema().createVertexType("Product").createProperty("name", Type.STRING);
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) NOTUNIQUE");
      for (final String name : List.of("alpha", "Beta", "gamma", "Delta"))
        database.newVertex("Product").set("name", name).save();
    });
    database.transaction(() -> {
      final String query = "SELECT name FROM Product WHERE name.toLowerCase() IN ('delta', 'b', 'beta', 'alpha') ORDER BY name";
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3))
          .contains("FETCH FROM INDEX").contains("ORDER BY");
      final List<String> names = database.query("sql", query).stream().map(r -> r.<String>getProperty("name")).toList();
      assertThat(names).containsExactly("Beta", "Delta", "alpha");
    });
  }

  /*
   * #6640: {@code WHERE prop IN (v1, v2, ..., vN)} - the parenthesized literal-list form of IN, the most common
   * shape in hand-written SQL - never used the index. {@code InCondition.isIndexAware()} only recognised the
   * bracket-array form ({@code IN [v1,...]}) and the bound-parameter form ({@code IN ?}/{@code IN :name}); a
   * parenthesized literal list fell back to a full bucket scan that re-evaluated the whole N-item list, by
   * linear search, against every row. A 15,000-item list on an indexed property was reported at ~350 rows/sec
   * (a 36M-row production table) and reproduced here at essentially the same rate on 200,000 rows - proportional
   * to row count, not list size, which is what gives a full scan away.
   * <p>
   * Fixing that alone uncovered two further bugs the literal-list path had never been able to reach before:
   * {@link com.arcadedb.query.sql.executor.FetchFromIndexStep#init} expanding a multi-value key
   * ({@code cartesianProduct}) deep-copied the entire remaining key - including the N-item list itself - once
   * per expanded element (O(n^2)), and {@code SQLASTBuilder.visitInCondition} built the list by calling the
   * ANTLR-generated indexed accessor {@code ctx.expression(i)} in a loop, which rescans the whole parse-tree
   * child list on every call (also O(n^2)). All three combined made a 15,000-value literal {@code IN()} on an
   * indexed property ~1000x slower than the identical values passed as a bound parameter; fixed, the two forms
   * perform the same.
   * <p>
   * Making the parenthesized list index-aware also exposed a pre-existing correctness bug shared with the bracket
   * form: {@code isIndexAware()} never checked {@code not}, so {@code trackId NOT IN [2,5,9]} was already fetching
   * the LISTED rows through the index instead of excluding them (same class of bug as #6575 for the native Select
   * API - a negated leaf has no complement in {@code FetchFromIndexStep}). Fixed alongside this change by declining
   * to use the index whenever {@code not} is set, for every right-hand shape.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  @DisplayName("#6640 literal parenthesized in list uses the index")
  @Test
  void literalParenthesizedInListUsesTheIndex() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "explain select from Track where trackId in (1, 2, 3)");
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("FETCH FROM INDEX");
    });
  }

  @DisplayName("#6640 single item parenthesized in list uses the index")
  @Test
  void singleItemParenthesizedInListUsesTheIndex() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "explain select from Track where trackId in (1)");
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("FETCH FROM INDEX");
    });
  }

  @DisplayName("#6640 literal parenthesized in list returns exact rows with duplicates and misses")
  @Test
  void literalParenthesizedInListReturnsExactRowsWithDuplicatesAndMisses() {
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    database.transaction(() -> {
      // 9 is repeated in the list and 100 does not exist: the index-based fetch must neither duplicate
      // nor invent rows, exactly like the pre-fix full-scan evaluator.
      final ResultSet rs = database.query("sql", "select trackId from Track where trackId in (2, 5, 9, 9, 100) order by trackId");
      final List<Long> ids = rs.stream().map(r -> r.<Long>getProperty("trackId")).toList();
      assertThat(ids).containsExactly(2L, 5L, 9L);
    });
  }

  @DisplayName("#6640 negated parenthesized in list does not use the index and excludes the listed rows")
  @Test
  void negatedParenthesizedInListDoesNotUseTheIndexAndExcludesTheListedRows() {
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    // A negated IN has no complement in FetchFromIndexStep: every code path there builds a cursor over the
    // values that DO match, not their complement. isIndexAware() must decline whenever `not` is set, for
    // every right-hand shape - parenthesized list, bracket array, bound parameter - not just the one this
    // PR touches, otherwise the query silently returns the listed rows instead of excluding them. The
    // bracket-array and bound-parameter forms already reached the index before this PR, so this locks in
    // the guard fixing a pre-existing bug for them too, not just guarding against the new parenthesized path.
    database.transaction(() -> {
      assertNotInDeclinesTheIndexAndExcludesTheListedRows("select from Track where trackId not in (2, 5, 9)",
          "select trackId from Track where trackId not in (2, 5, 9) order by trackId");
      assertNotInDeclinesTheIndexAndExcludesTheListedRows("select from Track where trackId not in [2, 5, 9]",
          "select trackId from Track where trackId not in [2, 5, 9] order by trackId");
    });
    database.transaction(() -> {
      final ResultSet explain = database.query("sql", "explain select from Track where trackId not in :ids", Map.of("ids", List.of(2L, 5L, 9L)));
      assertThat(explain.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("FETCH FROM INDEX");

      final List<Long> ids = database.query("sql", "select trackId from Track where trackId not in :ids order by trackId", Map.of("ids", List.of(2L, 5L, 9L)))
          .stream().map(r -> r.<Long>getProperty("trackId")).toList();
      assertThat(ids).containsExactly(0L, 1L, 3L, 4L, 6L, 7L, 8L);
    });
  }

  private void assertNotInDeclinesTheIndexAndExcludesTheListedRows(final String explainQuery, final String selectQuery) {
    final ResultSet explain = database.query("sql", "explain " + explainQuery);
    assertThat(explain.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("FETCH FROM INDEX");

    final List<Long> ids = database.query("sql", selectQuery).stream().map(r -> r.<Long>getProperty("trackId")).toList();
    assertThat(ids).containsExactly(0L, 1L, 3L, 4L, 6L, 7L, 8L);
  }

  @DisplayName("#6640 literal and bound parameter forms agree on large lists")
  @Test
  @Tag("slow")
  void literalAndBoundParameterFormsAgreeOnLargeLists() {
    final int rowCount = 20_000;
    database.transaction(() -> {
      for (int i = 0; i < rowCount; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    final List<Long> chosen = new Random(42).longs(0, rowCount).distinct().limit(6_000).boxed().collect(Collectors.toList());
    final String literalInList = chosen.stream().map(String::valueOf).collect(Collectors.joining(","));

    database.transaction(() -> {
      final List<Long> viaLiteral = database.query("sql", "select from Track where trackId in (" + literalInList + ")")
          .stream().map(r -> r.<Long>getProperty("trackId")).toList();
      final List<Long> viaBoundParam = database.query("sql", "select from Track where trackId in :ids", Map.of("ids", chosen))
          .stream().map(r -> r.<Long>getProperty("trackId")).toList();

      assertThat(viaLiteral).containsExactlyInAnyOrderElementsOf(chosen);
      assertThat(viaLiteral).containsExactlyInAnyOrderElementsOf(viaBoundParam);
    });
  }

  @DisplayName("#6640 large literal in list stays near linear not quadratic")
  @Test
  @Tag("slow")
  void largeLiteralInListStaysNearLinearNotQuadratic() {
    final int rowCount = 30_000;
    database.transaction(() -> {
      for (int i = 0; i < rowCount; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    // A 10x larger literal IN-list must not take anywhere near 100x longer. Before the fix, three compounding
    // O(n^2) costs - a full bucket scan, FetchFromIndexStep.cartesianProduct() copying the whole remaining key
    // per expanded element, and the ANTLR AST builder rescanning the parse tree per list item - made a
    // 15,000-item literal IN() take ~40s where the equivalent bound parameter took ~50ms (#6640).
    database.transaction(() -> database.query("sql", buildInQuery(2_000)).close());

    // assertStayedUnder, not assertGaveUpWithin: the bound IS the assertion here, standing in for the
    // near-linear complexity claim - there is no other practical way to express "not quadratic". Widening
    // this bound would not be a safe loosening, it would quietly delete the regression coverage.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    database.transaction(() -> database.query("sql", buildInQuery(20_000)).close());
    stopwatch.assertStayedUnder(5_000,
        "a near-linear 20,000-item literal IN() against an indexed property, not the pre-fix quadratic full-scan/copy/parse cost");
  }

  private String buildInQuery(final int n) {
    final StringBuilder sql = new StringBuilder("select from Track where trackId in (");
    for (int i = 0; i < n; i++) {
      if (i > 0)
        sql.append(',');
      sql.append(i);
    }
    sql.append(')');
    return sql.toString();
  }
}
