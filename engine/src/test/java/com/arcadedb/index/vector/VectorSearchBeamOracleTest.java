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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The width of the beam a graph walk uses, which is the recall and latency knob {@code efSearch} of {@code vector.neighbors}
 * (issue #3722): an explicit {@code efSearch} above zero, else the index's own {@code efSearch} when it set one, else the larger of
 * {@code k} and 100 on a graph of fewer than 10,000 nodes and of {@code 2k} and 100 on a larger one; and never narrower than
 * {@code k}. The beam is not visible in the answer of a small test index (every beam finds the exact neighbours), so the oracle is
 * the number of nodes the walk scored: it is a deterministic function of the graph, the query and the beam, so two requests that
 * mean the same beam score the same nodes for every query, and a wider beam scores more of them.
 */
@Tag("vector")
class VectorSearchBeamOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int QUERIES    = 12;

  @Test
  void anUnconfiguredIndexWalksTheBeamOfTheLargerOfKAndOneHundred() throws Exception {
    final LSMVectorIndex index = index("Plain", 1500, "");
    final List<float[]> queries = queries();

    final int[] byDefault = visited(index, queries, 5, -1);
    assertThat(visited(index, queries, 5, 0)).as("an efSearch of zero is no efSearch").containsExactly(byDefault);
    assertThat(visited(index, queries, 5, 100)).as("the default beam of a small k is 100").containsExactly(byDefault);

    // a k above the floor is its own beam on a small graph
    final int[] largeK = visited(index, queries, 150, -1);
    assertThat(visited(index, queries, 150, 150)).as("k = 150 asks for a beam of 150").containsExactly(largeK);
    assertThat(sum(largeK)).as("a wider beam scores more nodes").isGreaterThan(sum(byDefault));
  }

  @Test
  void anExplicitEfSearchSizesTheBeamAndNeverBelowK() throws Exception {
    final LSMVectorIndex index = index("Explicit", 1500, "");
    final List<float[]> queries = queries();

    final long narrow = sum(visited(index, queries, 5, 20));
    final long standard = sum(visited(index, queries, 5, 100));
    final long wide = sum(visited(index, queries, 5, 400));
    assertThat(narrow).as("efSearch 20 against the default of 100").isLessThan(standard);
    assertThat(standard).as("efSearch 100 against 400").isLessThan(wide);

    assertThat(visited(index, queries, 60, 10)).as("an efSearch below k is raised to k").containsExactly(visited(index, queries, 60, 60));
  }

  @Test
  void anIndexThatSetItsOwnEfSearchWalksItUnlessTheQueryNamesOne() throws Exception {
    final LSMVectorIndex index = index("Configured", 1500, ", \"efSearch\": 30");
    final List<float[]> queries = queries();

    final int[] byDefault = visited(index, queries, 5, -1);
    assertThat(visited(index, queries, 5, 30)).as("the index's efSearch of 30").containsExactly(byDefault);
    assertThat(sum(visited(index, queries, 5, 100))).as("a query's efSearch overrides it").isGreaterThan(sum(byDefault));
  }

  @Test
  void aGraphOfTenThousandNodesWalksTwiceK() throws Exception {
    final LSMVectorIndex index = index("Large", 10_000, "");
    final List<float[]> queries = queries();

    final int[] byDefault = visited(index, queries, 300, -1);
    assertThat(visited(index, queries, 300, 600)).as("exactly 10,000 nodes: 2k = 600, not k").containsExactly(byDefault);
    assertThat(sum(byDefault)).isGreaterThan(sum(visited(index, queries, 300, 300)));
  }

  /** The nodes each query's walk scored, read from the cost the index measures of its last walk (reset before each). */
  private int[] visited(final LSMVectorIndex index, final List<float[]> queries, final int k, final int efSearch)
      throws Exception {
    final Field walked = LSMVectorIndex.class.getDeclaredField("graphWalkVisited");
    walked.setAccessible(true);
    final int[] counts = new int[queries.size()];
    for (int i = 0; i < counts.length; i++) {
      walked.setInt(index, 0);
      index.findNeighborsFromVector(queries.get(i), k, efSearch);
      counts[i] = (int) (long) index.getStats().get("graphWalkVisitedAvg");
      assertThat(counts[i]).as("query %d walked the graph", i).isPositive();
    }
    return counts;
  }

  private static long sum(final int[] counts) {
    long total = 0;
    for (final int count : counts)
      total += count;
    return total;
  }

  private static List<float[]> queries() {
    final Random random = new Random(99);
    final List<float[]> queries = new ArrayList<>();
    for (int i = 0; i < QUERIES; i++)
      queries.add(vector(random));
    return queries;
  }

  private LSMVectorIndex index(final String type, final int rows, final String extraMetadata) {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON " + type + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"COSINE\"" + extraMetadata + " }");
    final Random random = new Random(5);
    database.transaction(() -> {
      for (int i = 0; i < rows; i++)
        database.newDocument(type).set("embedding", vector(random)).save();
    });
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[embedding]"))
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
    return index;
  }

  private static float[] vector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = (float) random.nextGaussian();
    vector[0] += 0.5f;
    return vector;
  }
}
