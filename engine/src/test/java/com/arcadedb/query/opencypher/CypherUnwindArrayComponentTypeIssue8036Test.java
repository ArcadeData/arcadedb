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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #8036: openCypher {@code UNWIND} over a native-array property returned zero
 * rows - silently dropping the whole input row - whenever the array's component type was not one of the six
 * spelled out in {@code UnwindStep}'s {@code instanceof} ladder ({@code Object[]}, {@code int[]},
 * {@code long[]}, {@code double[]}, {@code float[]}, {@code boolean[]}). {@code short[]} (an
 * {@code ARRAY_OF_SHORTS} property), {@code byte[]} (a {@code BINARY} property) and {@code char[]} matched
 * no arm, left the accumulator list empty, and produced no output row and no error.
 * <p>
 * The step now shares the decision SQL {@code UNWIND} and SQL {@code expand()} already share -
 * {@code MultiValue.isSequenceArray()} (issue #7923) - so a {@code BINARY} blob stays one opaque value and
 * every other component type is boxed reflectively. The cross-language assertions below are what pins the
 * three clauses to that one rule.
 */
class CypherUnwindArrayComponentTypeIssue8036Test {
  private static final String DB_PATH = "./target/testunwindarraycomponenttypes8036";
  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    final VertexType type = database.getSchema().createVertexType("V1");
    type.createProperty("arrs", Type.ARRAY_OF_SHORTS);
    type.createProperty("arri", Type.ARRAY_OF_INTEGERS);
    type.createProperty("arrl", Type.ARRAY_OF_LONGS);
    type.createProperty("arrf", Type.ARRAY_OF_FLOATS);
    type.createProperty("arrd", Type.ARRAY_OF_DOUBLES);
    type.createProperty("blob", Type.BINARY);

    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V1");
      v.set("arrs", new short[] { 7, 8 });
      v.set("arri", new int[] { 4, 5 });
      v.set("arrl", new long[] { 100L, 200L });
      v.set("arrf", new float[] { 1, 2, 3 });
      v.set("arrd", new double[] { 1.5, 2.5 });
      v.set("blob", new byte[] { 10, 11, 12, 13 });
      v.set("list", List.of(10, 20));
      v.save();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * The headline repro: an {@code ARRAY_OF_SHORTS} property produced zero rows because {@code short[]} had
   * no arm in the ladder.
   */
  @Test
  void unwindArrayOfShortsPropertyYieldsOneRowPerElement() {
    final List<Object> values = unwindProperty("arrs");

    assertThat(values).hasSize(2);
    assertThat(values).extracting(v -> ((Number) v).intValue()).containsExactly(7, 8);
  }

  /**
   * A {@code BINARY} property is an opaque blob, not a sequence of bytes: SQL {@code UNWIND} forwards it as
   * one row and openCypher must agree. Before the fix openCypher answered zero rows.
   */
  @Test
  void unwindBinaryPropertyYieldsOneOpaqueRow() {
    final List<Object> values = unwindProperty("blob");

    assertThat(values).hasSize(1);
    assertThat(values.getFirst()).isInstanceOf(byte[].class);
    assertThat((byte[]) values.getFirst()).containsExactly((byte) 10, (byte) 11, (byte) 12, (byte) 13);
  }

  /**
   * The parity assertion the issue asked for: for every declared array property type on one record, the
   * openCypher {@code UNWIND} row count equals the SQL {@code UNWIND} row count.
   */
  @Test
  void unwindRowCountMatchesSqlForEveryDeclaredArrayType() {
    for (final String property : List.of("arrs", "arri", "arrl", "arrf", "arrd", "blob", "list")) {
      final int cypherRows = unwindProperty(property).size();

      int sqlRows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT " + property + " FROM V1 UNWIND " + property)) {
        while (rs.hasNext()) {
          rs.next();
          sqlRows++;
        }
      }

      assertThat(cypherRows).as("row count for property '%s'", property).isEqualTo(sqlRows);
    }
  }

  /**
   * {@code FOR x IN ...} is the ISO/IEC 39075 synonym of {@code UNWIND} and builds the very same
   * {@code UnwindStep}, so it carried the same defect.
   */
  @Test
  void forInOverArrayOfShortsPropertyYieldsOneRowPerElement() {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (t:V1) FOR x IN t.arrs RETURN x")) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("x"));
    }

    assertThat(values).hasSize(2);
    assertThat(values).extracting(v -> ((Number) v).intValue()).containsExactly(7, 8);
  }

  /**
   * A standalone {@code UNWIND $param} has no previous step and takes the array straight from the parameter
   * map, so it exercises the same ladder on component types no declared property type can produce.
   */
  @Test
  void unwindPrimitiveArrayParametersYieldOneRowPerElement() {
    assertThat(unwindParameter(new short[] { 7, 8, 9 })).hasSize(3);
    assertThat(unwindParameter(new char[] { 'a', 'b' })).hasSize(2);
    assertThat(unwindParameter(new boolean[] { true, false })).hasSize(2);
    assertThat(unwindParameter(new int[] { 1, 2, 3, 4 })).hasSize(4);

    // A byte[] parameter is a BINARY blob, so it stays a single opaque value, exactly as in SQL.
    final List<Object> blob = unwindParameter(new byte[] { 1, 2, 3 });
    assertThat(blob).hasSize(1);
    assertThat(blob.getFirst()).isInstanceOf(byte[].class);
  }

  /**
   * {@code ListPredicateExpression} carries the same shape of defect through its own ladder, which covered
   * only {@code Object[]}, {@code int[]}, {@code long[]} and {@code double[]}. An {@code ARRAY_OF_FLOATS} or
   * {@code ARRAY_OF_SHORTS} property produced an empty element list, which silently turns {@code all()} into
   * vacuous truth and {@code any()} into false rather than raising anything.
   */
  @Test
  void listPredicatesSeeEveryElementOfAFloatOrShortArrayProperty() {
    assertThat(predicate("all(x IN t.arrf WHERE x > 0)")).isEqualTo(Boolean.TRUE);
    assertThat(predicate("all(x IN t.arrf WHERE x > 2)")).isEqualTo(Boolean.FALSE);
    assertThat(predicate("any(x IN t.arrf WHERE x = 3.0)")).isEqualTo(Boolean.TRUE);
    assertThat(predicate("any(x IN t.arrf WHERE x = 99.0)")).isEqualTo(Boolean.FALSE);

    assertThat(predicate("all(x IN t.arrs WHERE x > 0)")).isEqualTo(Boolean.TRUE);
    assertThat(predicate("any(x IN t.arrs WHERE x = 8)")).isEqualTo(Boolean.TRUE);
    assertThat(predicate("none(x IN t.arrs WHERE x = 99)")).isEqualTo(Boolean.TRUE);
    assertThat(predicate("single(x IN t.arrs WHERE x = 7)")).isEqualTo(Boolean.TRUE);
  }

  /**
   * Pins the one deliberate asymmetry this change introduces, so that nobody removes it by accident while
   * answering #8098. {@code UNWIND} treats a {@code BINARY} property as a single opaque value, because that is
   * what {@code MultiValue.isSequenceArray()} means and what SQL answers. The list predicates do not share that
   * rule: they reach the {@code byte[]} through {@code MultiValue.getMultiValueAsList()}, which has no
   * {@code byte[]} exclusion, so they evaluate the predicate once per byte. That is the same answer the three
   * sibling helpers - {@code ListComprehensionExpression}, {@code ReduceExpression}, {@code AllReduceExpression}
   * - already give for a {@code byte[]}, each through an explicit {@code byte[]} arm in its own ladder.
   * <p>
   * Before this change the predicates saw an EMPTY list for a {@code byte[]} instead, so {@code all()} was
   * vacuously true and {@code any()} false; the assertions below are the ones that tell those two apart.
   * #8098 is where the question "is a BINARY property a sequence to openCypher?" gets settled for every clause
   * at once.
   */
  @Test
  void listPredicatesOverABinaryPropertyEvaluateOncePerByteUntilIssue8098IsSettled() {
    // blob = { 10, 11, 12, 13 }: with an empty element list these two would answer true and false respectively.
    assertThat(predicate("all(x IN t.blob WHERE x > 100)")).isEqualTo(Boolean.FALSE);
    assertThat(predicate("any(x IN t.blob WHERE x = 11)")).isEqualTo(Boolean.TRUE);

    assertThat(predicate("all(x IN t.blob WHERE x > 0)")).isEqualTo(Boolean.TRUE);
    assertThat(predicate("none(x IN t.blob WHERE x = 10)")).isEqualTo(Boolean.FALSE);

    // UNWIND, on the same property in the same run, still answers one opaque row - the asymmetry itself.
    assertThat(unwindProperty("blob")).hasSize(1);
  }

  private List<Object> unwindProperty(final String property) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (t:V1) UNWIND t." + property + " AS x RETURN x")) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("x"));
    }
    return values;
  }

  private List<Object> unwindParameter(final Object array) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "UNWIND $arr AS x RETURN x", Map.of("arr", array))) {
      while (rs.hasNext())
        values.add(rs.next().getProperty("x"));
    }
    return values;
  }

  private Object predicate(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (t:V1) RETURN " + expression + " AS p")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      return row.getProperty("p");
    }
  }
}
