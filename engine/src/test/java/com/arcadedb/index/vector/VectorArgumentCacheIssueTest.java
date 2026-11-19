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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The vector function re-derives the query vector once per row of a scan (issue #12568).
 * <p>
 * A scan that scores every fact against one query vector calls the function once per row, and the query vector is the same
 * object every time: it is the parameter of the statement. The function converted it - a {@code List<Double>} of 1024
 * boxed doubles - into a {@code float[]} on every call, which is the same work on the same input 100k times.
 * <p>
 * This counts the conversions the cache makes, so the claim is about the number of conversions and not about a clock.
 */
class VectorArgumentCacheIssueTest extends TestHelper {

  private static final int DIM   = 1024;
  private static final int EDGES = 2_000;

  @Test
  @DisplayName("A scan prepares the query vector once, not once per row")
  void theQueryVectorIsPreparedOncePerScan() {
    final Random random = new Random(42);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE E");
      database.command("sql", "CREATE EDGE TYPE RELATES_TO");
      database.command("sql", "CREATE PROPERTY RELATES_TO.uuid STRING");
      database.command("sql", "CREATE PROPERTY RELATES_TO.fact_embedding ARRAY_OF_FLOATS");

      final var source = database.newVertex("E").set("uuid", "v0").save();
      final var target = database.newVertex("E").set("uuid", "v1").save();
      for (int i = 0; i < EDGES; i++) {
        final float[] embedding = new float[DIM];
        for (int d = 0; d < DIM; d++)
          embedding[d] = random.nextFloat();
        source.newEdge("RELATES_TO", target).set("uuid", "e" + i).set("fact_embedding", embedding).save();
      }
    });

    final CountingList query = new CountingList(DIM, random);
    final int before = query.conversions;

    int scored = 0;
    try (final ResultSet rs = database.query("cypher",
        "MATCH (n:E)-[e:RELATES_TO]->(m:E) WITH e, vector.similarity.cosine(e.fact_embedding, $q) AS score "
            + "WHERE score > 0.0 RETURN e.uuid AS uuid", java.util.Map.of("q", query))) {
      while (rs.hasNext()) {
        rs.next();
        scored++;
      }
    }

    assertThat(scored).isEqualTo(EDGES);
    // The query vector was converted once for the whole scan, not once per row: the conversion reads every element of
    // the list, so one conversion is DIM reads and one per row would be EDGES of them.
    assertThat(query.conversions - before).isEqualTo(DIM);
  }

  @Test
  @DisplayName("The cached form of a vector is the one the conversion would produce")
  void theCachedFormIsTheConvertedForm() {
    final List<Double> vector = new ArrayList<>();
    for (int i = 0; i < 8; i++)
      vector.add(i + 0.5);

    final CommandContext context = new BasicCommandContext();
    final VectorUtils.PreparedVector first = VectorArgumentCache.prepare(vector, context);
    final VectorUtils.PreparedVector second = VectorArgumentCache.prepare(vector, context);

    assertThat(second).isSameAs(first);
    assertThat(first.values()).containsExactly(0.5f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f);
  }

  @Test
  @DisplayName("A different object is a different vector")
  void aDifferentObjectIsPreparedAgain() {
    final List<Double> one = new ArrayList<>(List.of(1.0, 2.0));
    final List<Double> two = new ArrayList<>(List.of(3.0, 4.0));

    final CommandContext context = new BasicCommandContext();
    final VectorUtils.PreparedVector first = VectorArgumentCache.prepare(one, context);
    final VectorUtils.PreparedVector second = VectorArgumentCache.prepare(two, context);

    assertThat(second).isNotSameAs(first);
    assertThat(second.values()).containsExactly(3.0f, 4.0f);
  }

  /**
   * A list that counts how many times it was read element by element: the conversion reads every element, so the count is
   * the number of conversions the cache did not save.
   */
  private static final class CountingList extends ArrayList<Double> {
    private int conversions;

    private CountingList(final int size, final Random random) {
      for (int i = 0; i < size; i++)
        add(random.nextDouble());
    }

    @Override
    public Double get(final int index) {
      conversions++;
      return super.get(index);
    }
  }
}
