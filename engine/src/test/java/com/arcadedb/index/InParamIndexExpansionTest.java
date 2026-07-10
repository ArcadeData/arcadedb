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
 * SQL {@code IN :param} with a collection parameter must use the index and return the same rows as
 * the literal list form {@code IN [1, 2, 3]}.
 * <p>
 * The HTTP layer deserializes JSON numeric arrays into primitive arrays (long[], int[], double[])
 * via toMap(true). The indexed expansion in FetchFromIndexStep.cartesianProduct() must treat those
 * primitive arrays as multi-value keys and look each element up individually, exactly like the
 * non-indexed IN evaluator. When it did not, the whole array was used as a single index key and
 * matched nothing.
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
}
