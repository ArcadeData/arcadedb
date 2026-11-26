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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FOREACH over a value that is an array in Java (the stored value of an {@code ARRAY_OF_*} property, or an array passed as
 * a parameter by the embedded API) must run its body once per element, as UNWIND, {@code size()}, list comprehensions and
 * {@code IN} already do for the same value. FOREACH used to run the body once with the whole array bound to the loop variable
 * (Neo4j 5.26 on the same data: one body run per element).
 */
class CypherForeachArrayValueTest extends TestHelper {
  private static final Type[] ARRAY_TYPES = { Type.ARRAY_OF_SHORTS, Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_LONGS, Type.ARRAY_OF_FLOATS,
      Type.ARRAY_OF_DOUBLES };

  @Test
  void foreachOverATypedArrayPropertyRunsTheBodyOncePerElement() {
    database.getSchema().createVertexType("T");
    for (final Type type : ARRAY_TYPES) {
      final String property = "p_" + type.name().toLowerCase();
      database.getSchema().getType("T").createProperty(property, type);
      database.command("opencypher", "MATCH (n:T) DETACH DELETE n");
      database.command("opencypher", "CREATE (:T {" + property + ": [1, 2, 3]})");

      database.command("opencypher", "MATCH (n:T) FOREACH (x IN n." + property + " | CREATE (:V {v: x}))");
      assertThat(values()).as("FOREACH over %s", type).containsExactly(1.0d, 2.0d, 3.0d);

      database.command("opencypher", "MATCH (v:V) DELETE v");
      database.command("opencypher", "MATCH (n:T) WITH n." + property + " AS l FOREACH (x IN l | CREATE (:V {v: x}))");
      assertThat(values()).as("FOREACH over %s through a WITH alias", type).containsExactly(1.0d, 2.0d, 3.0d);

      database.command("opencypher", "MATCH (v:V) DELETE v");
      database.command("opencypher", "MATCH (n:T) UNWIND n." + property + " AS x CREATE (:V {v: x})");
      assertThat(values()).as("UNWIND over %s", type).containsExactly(1.0d, 2.0d, 3.0d);
      database.command("opencypher", "MATCH (v:V) DELETE v");
    }
  }

  @Test
  void foreachOverAnArrayParameterRunsTheBodyOncePerElement() {
    final Object[] arrays = { new int[] { 1, 2, 3 }, new long[] { 1L, 2L, 3L }, new short[] { 1, 2, 3 }, new float[] { 1f, 2f, 3f },
        new double[] { 1d, 2d, 3d } };
    for (final Object array : arrays) {
      database.command("opencypher", "FOREACH (x IN $arr | CREATE (:V {v: x}))", Map.of("arr", array));
      assertThat(values()).as("FOREACH over a %s parameter", array.getClass().getSimpleName()).containsExactly(1.0d, 2.0d, 3.0d);
      database.command("opencypher", "MATCH (v:V) DELETE v");
    }
  }

  @Test
  void foreachOverAnEmptyArrayRunsTheBodyNever() {
    database.command("opencypher", "FOREACH (x IN $arr | CREATE (:V {v: x}))", Map.of("arr", new long[0]));
    assertThat(values()).isEmpty();
  }

  @Test
  void foreachOverABinaryParameterRunsTheBodyOnceLikeUnwind() {
    database.command("opencypher", "FOREACH (x IN $arr | CREATE (:V))", Map.of("arr", new byte[] { 1, 2, 3 }));
    database.command("opencypher", "UNWIND $arr AS x CREATE (:W)", Map.of("arr", new byte[] { 1, 2, 3 }));
    assertThat(count("V")).isEqualTo(1L);
    assertThat(count("W")).isEqualTo(1L);
  }

  private long count(final String label) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /** The values of the V nodes in ascending order, as doubles so that the integer and the floating point arrays compare alike. */
  private List<Double> values() {
    final List<Double> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) WHERE v.v IS NOT NULL RETURN v.v AS v ORDER BY v")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Object v = r.getProperty("v");
        if (v instanceof Number number)
          values.add(number.doubleValue());
        else
          values.add(Double.NaN);
      }
    }
    return values;
  }
}
