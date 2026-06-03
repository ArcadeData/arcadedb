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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4468: SQL `IN :param` with a collection parameter
 * returns no rows when an index is used on the left-hand field.
 *
 * Root cause: FetchFromIndexStep.cartesianProduct() only expanded Iterable<?>,
 * but JSON deserialization via toMap(true) yields primitive arrays (long[], double[])
 * which are not Iterable. The entire array was treated as a single index key,
 * matching nothing.
 */
class InParamIndexExpansionTest extends TestHelper {

  @Override
  protected void beginTest() {
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

  // Baseline: literal IN list must use the index and return all three rows.
  @Test
  void literalInListUsesIndexAndReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "select code from IssueItem where code in [1, 2, 3] order by code");
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  // Issue #4468: named List<Long> param — this is the primary failing form.
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

  // Issue #4468: named primitive long[] param — simulates HTTP JSON deserialization via toMap(true).
  @Test
  void namedPrimitiveArrayParamInWithIndexReturnsRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select code from IssueItem where code in :codes order by code",
          Map.of("codes", new long[] { 1L, 2L, 3L }));
      final List<Integer> codes = rs.stream().map(r -> r.<Integer>getProperty("code")).toList();
      assertThat(codes).containsExactly(1, 2, 3);
    });
  }

  // Issue #4468: named Object[] param — covers mixed boxed-array case.
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

  // Issue #4468: positional List<Long> param — the other reported failing form.
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

  // Confirm the execution plan uses the index for the IN :codes form.
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

  // Verify the non-indexed path (arithmetic prevents index use) still works as a control.
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

  // Partial match: only 2 of 3 codes present in the collection.
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

  // Single-element list param must also find exactly one row.
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
}
