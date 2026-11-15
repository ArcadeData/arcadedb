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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code db.index.vector.queryNodes} takes the {@code efSearch} and {@code rerank} options {@code vector.neighbors} takes, so
 * an openCypher caller can ask the index for the same search breadth a SQL caller can. Without them the procedure refused the
 * keys, and the only way to a wider walk was asking for more rows than wanted: at k = 100 on 20,000 768-dimension vectors,
 * the procedure found 0.79 to 0.89 of the exact top 100 where {@code vector.neighbors} with {@code efSearch: 800} found 0.97
 * to 1.00.
 */
class CypherVectorQueryNodesSearchOptionsTest extends TestHelper {
  private static final int DIMENSIONS = 32;

  @Test
  void efSearchAnswersWhatVectorNeighborsAnswersWithTheSameBreadth() {
    final float[] query = populate();

    for (final int efSearch : new int[] { 10, 50, 400 }) {
      final List<Integer> cypher = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes('Doc[v]', 20, $q, {efSearch: $ef}) YIELD node RETURN node.i AS i",
          Map.of("q", query, "ef", efSearch))) {
        while (rs.hasNext())
          cypher.add(rs.next().<Number>getProperty("i").intValue());
      }

      final List<Integer> sql = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(vector.neighbors('Doc[v]', :q, 20, {efSearch: :ef}))", Map.of("q", query, "ef", efSearch))) {
        while (rs.hasNext())
          sql.add(rs.next().<Number>getProperty("i").intValue());
      }

      assertThat(cypher).as("efSearch %d", efSearch).hasSize(20).containsExactlyElementsOf(sql);
    }
  }

  @Test
  void anEfSearchBelowOneOrANegativeRerankIsRefused() {
    final float[] query = populate();

    assertThatThrownBy(() -> drain("CALL db.index.vector.queryNodes('Doc[v]', 5, $q, {efSearch: 0}) YIELD node RETURN node", query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("efSearch must be >= 1");
    assertThatThrownBy(() -> drain("CALL db.index.vector.queryNodes('Doc[v]', 5, $q, {rerank: -1}) YIELD node RETURN node", query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank must be >= 0");
  }

  private void drain(final String cypher, final float[] query) {
    try (final ResultSet rs = database.query("opencypher", cypher, Map.of("q", query))) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private float[] populate() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.v ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON Doc (v) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: 'COSINE'}");
    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < 3_000; i++)
        database.newVertex("Doc").set("i", i, "v", vector(random)).save();
    });
    return vector(random);
  }

  private static float[] vector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = (float) random.nextGaussian();
    return v;
  }
}
