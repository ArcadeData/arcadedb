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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10917: {@code vector(value, dimension, coordinateType)} ignored its coordinate type and always built a
 * {@code float[]}. It now builds the primitive array of the coordinate type, the one a {@code VECTOR<T>(n)} property declares.
 */
class Issue10917VectorCoordinateTypeTest extends TestHelper {

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {//
      "INT8|VECTOR<INT8>(3)|byte[]", "INT16|VECTOR<INT16>(3)|short[]", "INT32|VECTOR<INT32>(3)|int[]",
      "INT|VECTOR<INT32>(3)|int[]", "INT64|VECTOR<INTEGER>(3)|long[]", "INTEGER|VECTOR<INTEGER>(3)|long[]",
      "FLOAT32|VECTOR<FLOAT32>(3)|float[]", "FLOAT64|VECTOR<FLOAT>(3)|double[]", "FLOAT|VECTOR<FLOAT>(3)|double[]" })
  void theVectorIsTheArrayOfItsCoordinateType(final String coordinate, final String vectorType, final String array) {
    try (final ResultSet rs = database.query("opencypher",
        "WITH vector([1, 2, 3], 3, " + coordinate + ") AS v RETURN v IS :: " + vectorType + " AS typed, v AS v")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("typed")).isTrue();
      final Object v = r.getProperty("v");
      assertThat(v.getClass().getSimpleName()).isEqualTo(array);
    }
  }

  @Test
  void aNarrowerCoordinateTypeIsNotAWiderOne() {
    try (final ResultSet rs = database.query("opencypher",
        "WITH vector([1, 2, 3], 3, INT32) AS i, vector([1, 2, 3], 3, INT8) AS b RETURN "
            + "i IS :: VECTOR<INT8>(3) AS intAsByte, b IS :: VECTOR<INT32>(3) AS byteAsInt, b IS :: VECTOR<FLOAT32>(3) AS byteAsFloat")) {
      final Result r = rs.next();
      assertThat(r.<Boolean>getProperty("intAsByte")).isFalse();
      assertThat(r.<Boolean>getProperty("byteAsInt")).isTrue();
      assertThat(r.<Boolean>getProperty("byteAsFloat")).isFalse();
    }
  }

  @Test
  void aCoordinateOutsideTheTypeIsRefused() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1, 300, 3], 3, INT8) AS v").next())
        .hasMessageContaining("INT8");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1, 1.5, 3], 3, INT32) AS v").next())
        .hasMessageContaining("INT32");
  }

  @Test
  void theBoundariesOfTheTypeAreKeptAndWhatIsPastThemIsRefused() {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN vector([-2147483648, 2147483647], 2, INT32) AS i, vector([-9223372036854775807, 9223372036854775807], 2, INTEGER) AS l")) {
      final Result r = rs.next();
      assertThat((int[]) r.getProperty("i")).containsExactly(Integer.MIN_VALUE, Integer.MAX_VALUE);
      assertThat((long[]) r.getProperty("l")).containsExactly(-Long.MAX_VALUE, Long.MAX_VALUE);
    }
    assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([2147483648], 1, INT32) AS v").next())
        .hasMessageContaining("INT32");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([9.223372036854775807E18], 1, INTEGER) AS v").next())
        .hasMessageContaining("INTEGER");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([0.0 / 0.0], 1, INT16) AS v").next())
        .hasMessageContaining("INT16");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1.0 / 0.0], 1, INT16) AS v").next())
        .hasMessageContaining("INT16");
  }

  @Test
  void theDimensionIsStillChecked() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN vector([1, 2], 3, INT16) AS v").next())
        .hasMessageContaining("dimension");
  }

  @Test
  void aTypedVectorFeedsTheDistanceFunctions() {
    try (final ResultSet rs = database.query("opencypher",
        "WITH vector([3, 4], 2, INT8) AS a, vector([0, 0], 2, INT16) AS b RETURN "
            + "vector_distance(a, b, EUCLIDEAN) AS d, vector_norm(a, EUCLIDEAN) AS n")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("d").doubleValue()).isEqualTo(5.0);
      assertThat(r.<Number>getProperty("n").doubleValue()).isEqualTo(5.0);
    }
  }

  /**
   * A {@code byte[]} reaching the Cypher vector functions is an INT8 vector of plain coordinates, never a quantized one: it is widened
   * as it is, not dequantized. Pinned because the SQL vector functions read the same {@code byte[]} as quantized (and refuse it).
   */
  @Test
  void aByteArrayIsReadAsPlainCoordinatesNotDequantized() {
    try (final ResultSet rs = database.query("opencypher",
        "RETURN vector_norm($v, EUCLIDEAN) AS n, vector_distance($v, $w, EUCLIDEAN) AS d",
        Map.of("v", new byte[] { 3, 4 }, "w", new byte[] { 0, 0 }))) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("n").doubleValue()).isEqualTo(5.0);
      assertThat(r.<Number>getProperty("d").doubleValue()).isEqualTo(5.0);
    }
  }

  @Test
  void aTypedVectorIsStoredAsTheConstrainedProperty() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.e IS :: VECTOR<INT32>(3)");
    command("CREATE (:L {e: vector([1, 2, 3], 3, INT32)})");
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:L) RETURN n.e IS :: VECTOR<INT32>(3) AS ok")) {
      assertThat(rs.next().<Boolean>getProperty("ok")).isTrue();
    }
  }

  private void command(final String cypher) {
    database.transaction(() -> database.command("opencypher", cypher).close());
  }
}
