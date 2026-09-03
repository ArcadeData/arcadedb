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
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue741InOperatorEdgeCompositeIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
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

  // The client's exact syntax: parenthesized literal IN list.
  @Test
  void parenthesizedInOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId IN ('S28719720','74529884')");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER_PK");
    });
  }

  @Test
  void bracketInOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId IN ['S28719720','74529884']");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER_PK");
    });
  }

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

  @Test
  void repeatedInValuesAreSoughtOnceAndReturnedOnce() {
    database.transaction(() -> {
      final List<String> ids = database.query("sql",
              "SELECT transactionId FROM TRANSFER WHERE transactionId IN ('74529884', '74529884', 'S28719720') ORDER BY transactionId")
          .stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(ids).containsExactly("74529884", "74529884", "S28719720");
    });
  }

  @Test
  void inValuesAreOrderedAsTheIndexStoresThemNotAsWritten() {
    // A numeric key: the literals '10' (a String) and 9 (a number) must sort as numbers, 9 before 10, the way the
    // index does once it has narrowed them - not as the Strings "10" < "9".
    database.transaction(() -> {
      database.getSchema().createVertexType("Track").createProperty("trackId", Type.LONG);
      database.getSchema().buildTypeIndex("Track", new String[] { "trackId" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      for (long i = 0; i < 20; i++)
        database.newVertex("Track").set("trackId", i).save();
    });
    database.transaction(() -> {
      // '10' and 10 narrow to the same key: one seek, one row.
      final String query = "SELECT trackId FROM Track WHERE trackId IN ('10', 9, 15, '2', 10) ORDER BY trackId";
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3))
          .contains("FETCH FROM INDEX").doesNotContain("ORDER BY");
      final List<Long> ids = database.query("sql", query).stream().map(r -> r.<Long>getProperty("trackId")).toList();
      assertThat(ids).containsExactly(2L, 9L, 10L, 15L);
    });

    // A case-insensitive key: 'b' < 'a' as bytes would put "B" first, the index folds both and answers "a" first.
    database.transaction(() -> {
      database.getSchema().createVertexType("Product").createProperty("name", Type.STRING);
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) NOTUNIQUE");
      for (final String name : List.of("alpha", "Beta", "gamma", "Delta"))
        database.newVertex("Product").set("name", name).save();
    });
    database.transaction(() -> {
      final String query = "SELECT name FROM Product WHERE name IN ('DELTA', 'B', 'beta', 'alpha') ORDER BY name";
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3))
          .contains("FETCH FROM INDEX").doesNotContain("ORDER BY");
      final List<String> names = database.query("sql", query).stream().map(r -> r.<String>getProperty("name")).toList();
      assertThat(names).containsExactly("alpha", "Beta", "Delta");
    });
  }
}
