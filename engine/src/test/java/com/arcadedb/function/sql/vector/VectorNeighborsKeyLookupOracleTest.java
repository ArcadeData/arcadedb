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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The key of {@code vector.neighbors} that is not a vector but the id of a vertex of the indexed type (the query vector is the one stored on
 * it), and the argument count of a call that does not come through the SQL parser. The oracle is the exact order of the stored vectors by
 * distance to the vector of the vertex the key names.
 */
class VectorNeighborsKeyLookupOracleTest extends TestHelper {
  private static final int DIMENSIONS = 6;
  private static final int ROWS       = 25;

  private final List<float[]> vectors = new ArrayList<>();

  @BeforeEach
  void createCorpus() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.id STRING");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\" }");
    final Random random = new Random(11);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = random.nextFloat();
        vectors.add(vector);
        database.newVertex("Doc").set("id", "d" + i, "vector", vector).save();
      }
      database.newVertex("Doc").set("id", "novector").save();
    });
  }

  @Test
  void aKeyThatIsAVertexIdSearchesWithTheVectorStoredOnThatVertex() {
    for (final int probe : new int[] { 0, 7, 24 }) {
      final List<Integer> expected = new ArrayList<>();
      for (int i = 0; i < ROWS; i++)
        expected.add(i);
      expected.sort(Comparator.comparingDouble(i -> squaredDistance(vectors.get(probe), vectors.get(i))));

      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT id FROM (SELECT expand(vector.neighbors('Doc[vector]', ?, 5)))",
          "d" + probe)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          ids.add(row.getProperty("id"));
        }
      }

      assertThat(ids).as("the 5 nearest to the vector of d%d", probe)
          .containsExactlyElementsOf(expected.subList(0, 5).stream().map(i -> "d" + i).toList());
    }
  }

  @Test
  void aKeyNoVertexHoldsOrAVertexWithoutAVectorIsRefusedNamingTheKey() {
    for (final String key : new String[] { "nobody", "novector" })
      assertThatThrownBy(() -> database.query("sql", "SELECT expand(vector.neighbors('Doc[vector]', ?, 5))", key).close())
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("Could not find vertex with key '" + key + "'");
  }

  @Test
  void aCallOfAnotherArgumentCountIsRefusedBeforeAnyArgumentIsRead() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final SQLFunctionVectorNeighbors function = new SQLFunctionVectorNeighbors();
    for (final Object[] params : new Object[][] { {}, { "Doc[vector]" }, { "Doc[vector]", new float[DIMENSIONS] },
        { "Doc[vector]", new float[DIMENSIONS], 3, null, 5 } })
      assertThatThrownBy(() -> function.execute(null, null, null, params, context)).as("%d arguments", params.length)
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("3-4 arguments");
  }

  private static double squaredDistance(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++)
      sum += (double) (a[i] - b[i]) * (a[i] - b[i]);
    return sum;
  }
}
