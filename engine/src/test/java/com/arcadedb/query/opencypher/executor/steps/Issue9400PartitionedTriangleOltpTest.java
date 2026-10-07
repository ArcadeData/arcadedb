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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9400: the exact path of the country-partitioned triangle count built a map per vertex per hop and reloaded the
 * edge list of a vertex once per neighbour, so it was 2 to 4 times slower than before the weighted answer of #9350, with
 * the same answer. The faster path must give the weighted answer in every shape: a vertex with one country, with
 * several (a chain that is not a function), and with parallel KNOWS edges.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9400PartitionedTriangleOltpTest extends TestHelper {
  private static final int PERSONS   = 60;
  private static final int CITIES    = 8;
  private static final int COUNTRIES = 3;

  private final int[][] knows        = new int[PERSONS][PERSONS];
  private final int[][] personCity   = new int[PERSONS][CITIES];
  private final int[][] cityCountry  = new int[CITIES][COUNTRIES];

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("City");
    database.getSchema().createVertexType("Country");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("IS_LOCATED_IN");
    database.getSchema().createEdgeType("IS_PART_OF");

    final Random rnd = new Random(9400);
    database.transaction(() -> {
      final MutableVertex[] countries = new MutableVertex[COUNTRIES];
      for (int i = 0; i < COUNTRIES; i++)
        countries[i] = database.newVertex("Country").set("id", i).save();

      final MutableVertex[] cities = new MutableVertex[CITIES];
      for (int i = 0; i < CITIES; i++) {
        cities[i] = database.newVertex("City").set("id", i).save();
        final int home = i % COUNTRIES;
        cities[i].newEdge("IS_PART_OF", countries[home]).save();
        cityCountry[i][home]++;
        // a city in two countries: the partition chain is not a function
        if (i % 4 == 0) {
          final int other = (home + 1) % COUNTRIES;
          cities[i].newEdge("IS_PART_OF", countries[other]).save();
          cityCountry[i][other]++;
        }
      }

      final MutableVertex[] persons = new MutableVertex[PERSONS];
      for (int i = 0; i < PERSONS; i++) {
        persons[i] = database.newVertex("Person").set("id", i).save();
        final int city = rnd.nextInt(CITIES);
        persons[i].newEdge("IS_LOCATED_IN", cities[city]).save();
        personCity[i][city]++;
        // a person in two cities, once in a city twice
        if (i % 7 == 0) {
          final int other = rnd.nextInt(CITIES);
          persons[i].newEdge("IS_LOCATED_IN", cities[other]).save();
          personCity[i][other]++;
        }
      }

      for (int e = 0; e < 420; e++) {
        final int a = rnd.nextInt(PERSONS), b = rnd.nextInt(PERSONS);
        if (a == b)
          continue;
        // parallel edges happen: the same pair drawn twice
        persons[a].newEdge("KNOWS", persons[b]).save();
        knows[a][b]++;
        knows[b][a]++;
      }
    });
  }

  @Test
  void weightedAnswerMatchesBruteForce() {
    final PartitionedTriangleOp op = new PartitionedTriangleOp(new String[] { "IS_LOCATED_IN", "IS_PART_OF" },
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.OUT }, "KNOWS");

    final long expected = bruteForce();
    assertThat(expected).as("the fixture must hold triangles").isPositive();
    assertThat(op.executeOLTP(database, WorkGuard.forCommandDeadline(null))).isEqualTo(expected);
  }

  /** Every ordered triple of distinct persons, an edge per pair, and the paths of each of them to one same country. */
  private long bruteForce() {
    final long[][] paths = new long[PERSONS][COUNTRIES];
    for (int p = 0; p < PERSONS; p++)
      for (int c = 0; c < CITIES; c++)
        for (int k = 0; k < COUNTRIES; k++)
          paths[p][k] += (long) personCity[p][c] * cityCountry[c][k];

    long total = 0;
    for (int a = 0; a < PERSONS; a++)
      for (int b = 0; b < PERSONS; b++) {
        if (knows[a][b] == 0)
          continue;
        for (int c = 0; c < PERSONS; c++) {
          if (knows[b][c] == 0 || knows[c][a] == 0)
            continue;
          long shared = 0;
          for (int k = 0; k < COUNTRIES; k++)
            shared += paths[a][k] * paths[b][k] * paths[c][k];
          total += (long) knows[a][b] * knows[b][c] * knows[c][a] * shared;
        }
      }
    return total;
  }
}
