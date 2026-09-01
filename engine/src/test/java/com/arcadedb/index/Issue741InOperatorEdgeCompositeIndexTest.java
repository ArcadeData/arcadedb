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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #741 (Locstat): SQL {@code IN} operator on the leading column of a composite index on an
 * EDGE type reported as not using the index.
 * <p>
 * Client scenario (verbatim query):
 * <pre>
 *   -- TRANSFER is a high-volume edge type with a composite index TRANSFER_PK on (transactionId, date)
 *   SELECT * FROM TRANSFER WHERE transactionId IN ('S28719720','74529884')  -- reported: full scan
 *   SELECT * FROM TRANSFER WHERE transactionId = '74529884' AND date = '2026-02-28' -- uses index
 * </pre>
 * The reported symptom matches #6640: the parenthesized literal-list form {@code IN (v1, v2)} did not
 * use the index (only the bracket-array {@code IN [..]} and bound-parameter {@code IN ?}/{@code :name}
 * forms did). This test pins that the parenthesized form now uses the index, including on an EDGE type
 * with a composite index where only the leading column is filtered.
 */
class Issue741InOperatorEdgeCompositeIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType account = database.getSchema().createVertexType("Account");
      account.createProperty("accountNumber", Type.STRING);

      final EdgeType transfer = database.getSchema().createEdgeType("TRANSFER");
      transfer.createProperty("transactionId", Type.STRING);
      transfer.createProperty("date", Type.STRING);

      // Composite index on (transactionId, date), mirroring the client's TRANSFER_PK
      database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false)
          .create();
    });

    database.transaction(() -> {
      final var a1 = database.newVertex("Account").set("accountNumber", "A1").save();
      final var a2 = database.newVertex("Account").set("accountNumber", "A2").save();

      a1.newEdge("TRANSFER", a2).set("transactionId", "S28719720", "date", "2026-01-01").save();
      a1.newEdge("TRANSFER", a2).set("transactionId", "74529884", "date", "2026-02-28").save();
      // noise edges that must NOT match the IN list
      for (int i = 0; i < 50; i++)
        a1.newEdge("TRANSFER", a2).set("transactionId", "NOISE-" + i, "date", "2026-03-" + (i % 28 + 1)).save();
    });
  }

  // The client's exact syntax: parenthesized literal IN list.
  @Test
  void parenthesizedInOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId IN ('S28719720','74529884')");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER[transactionId,date]");
    });
  }

  @Test
  void bracketInOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId IN ['S28719720','74529884']");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER[transactionId,date]");
    });
  }

  @Test
  void parenthesizedInReturnsExactlyTheMatchingEdges() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT transactionId FROM TRANSFER WHERE transactionId IN ('S28719720','74529884')");
      final List<String> ids = rs.stream().map(r -> r.<String>getProperty("transactionId")).toList();
      assertThat(ids).containsExactlyInAnyOrder("74529884", "S28719720");
    });
  }

  // Control: the equality form the client confirmed already uses the index.
  @Test
  void equalityOnLeadingColumnUsesIndex() {
    database.transaction(() -> {
      final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT * FROM TRANSFER WHERE transactionId = '74529884' AND date = '2026-02-28'");
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 3);
      assertThat(plan).contains("FETCH FROM INDEX TRANSFER[transactionId,date]");
    });
  }
}
