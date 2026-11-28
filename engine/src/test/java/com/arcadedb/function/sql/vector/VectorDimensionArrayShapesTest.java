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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code vector.dimension} takes the argument shapes every other {@code vector.*} function takes. The HTTP handler hands a JSON array
 * to a command as a primitive array ({@code double[]} for {@code [0.1, 0.2]}, {@code long[]} for {@code [1, 2]}, issue #4148), which
 * {@code VectorUtils.toFloatArray} reads, so {@code vector.dimension(:v)} must read them too.
 */
class VectorDimensionArrayShapesTest extends TestHelper {

  private Object dimension(final Object vector) {
    try (final ResultSet rs = database.query("sql", "SELECT `vector.dimension`(?) AS r", vector)) {
      return rs.next().getProperty("r");
    }
  }

  private Object magnitude(final Object vector) {
    try (final ResultSet rs = database.query("sql", "SELECT `vector.magnitude`(?) AS r", vector)) {
      return rs.next().getProperty("r");
    }
  }

  @Test
  void everyPrimitiveArrayThatAnotherVectorFunctionReadsHasADimension() {
    final Object[] shapes = { new double[] { 0.1, 0.2, 0.3 }, new long[] { 1, 2, 3 }, new int[] { 1, 2, 3 }, new short[] { 1, 2, 3 },
        new float[] { 0.1f, 0.2f, 0.3f } };
    for (final Object shape : shapes) {
      // the sibling function accepts the shape, so the dimension of the same argument must not be refused
      assertThat(magnitude(shape)).as("magnitude of %s", shape.getClass().getSimpleName()).isNotNull();
      assertThat(dimension(shape)).as("dimension of %s", shape.getClass().getSimpleName()).isEqualTo(3);
    }
  }

  @Test
  void listsAndBoxedArraysKeepTheirDimension() {
    assertThat(dimension(List.of(1, 2, 3, 4))).isEqualTo(4);
    assertThat(dimension(new Object[] { 1.5, 2.5 })).isEqualTo(2);
    assertThat(dimension(new double[0])).isEqualTo(0);
    assertThat(dimension(new long[0])).isEqualTo(0);
  }

  @Test
  void aNonVectorIsStillRefused() {
    assertThatThrownBy(() -> dimension(5)).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Vector must be an array or list");
    assertThatThrownBy(() -> dimension(new byte[] { 1, 2 })).isInstanceOf(CommandSQLParsingException.class);
  }
}
