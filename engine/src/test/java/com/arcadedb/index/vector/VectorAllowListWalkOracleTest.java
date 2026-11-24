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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A search with an allow-list wide enough for the graph walk (above the pre-filter share of the index) answers with the members of
 * the list and with nothing else, and with the nearest of them: the oracle is the exact ranking of the members. The walk is
 * handed the list as a {@code Bits} filter, and the code that turns the walk into rows checks the list again because JVector may
 * return a node the filter refused (the entry node of the walk), so a list that leaves out the nodes a walk starts from is the
 * case that matters: the lists here are drawn at random, 200 of them, and the queries are the vectors of the records.
 */
@Tag("vector")
class VectorAllowListWalkOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int ROWS       = 1500;
  private static final int K          = 12;

  private final List<RID>     rids    = new ArrayList<>();
  private final List<float[]> vectors = new ArrayList<>();
  private final Map<RID, float[]> byRid = new HashMap<>();

  @Test
  void aWalkOfAnAllowListAnswersOnlyWithItsMembersAndTheNearestOfThem() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);
    final LSMVectorIndex index = fill();
    // wider than the default share of the index: the plan scoring the list directly is not taken, the walk is
    final float share = database.getConfiguration().getValueAsFloat(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY);

    final Random random = new Random(77);
    int walked = 0;
    for (int round = 0; round < 200; round++) {
      final int size = (int) (ROWS * Math.min(0.95f, share + 0.05f + random.nextFloat() * 0.5f));
      final List<RID> shuffled = new ArrayList<>(rids);
      Collections.shuffle(shuffled, random);
      final Set<RID> allowed = new HashSet<>(shuffled.subList(0, size));
      final int queryRow = random.nextInt(ROWS);

      final List<Pair<RID, Float>> answer = index.findNeighborsFromVector(vectors.get(queryRow), K, ROWS, allowed);

      final List<RID> expected = allowed.stream()
          .sorted(Comparator.comparingDouble((RID rid) -> -cosine(vectors.get(queryRow), byRid.get(rid))))
          .limit(K).toList();
      assertThat(answer).as("round %d, %d members, query row %d", round, size, queryRow).extracting(Pair::getFirst)
          .containsExactlyElementsOf(expected);
      if (size > share * ROWS)
        walked++;
    }
    assertThat(walked).as("rounds whose list is wider than the pre-filter share").isEqualTo(200);
  }

  private LSMVectorIndex fill() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withMetadata(new JSONObject().put("similarity", "COSINE").put("dimensions", DIMENSIONS));
    builder.create();

    final Random random = new Random(5);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian();
        vector[0] += 0.5f;
        rids.add(database.newDocument("Doc").set("embedding", vector).save().getIdentity());
        vectors.add(vector);
        byRid.put(rids.getLast(), vector);
      }
    });
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]"))
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
    return index;
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }
}
