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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The work the delta buffer scans charge to the rebuild trigger (issue #6797) is the number of buffered rows a search scored: the
 * amortization half of the trigger compares it with what draining the buffer by a rebuild would cost, so a count that drifts from the
 * rows really scanned moves the rebuild earlier or later without any answer changing. The oracle is the number itself: a search of a
 * buffer of {@code D} rows scores {@code D}, one restricted to an allow-list scores the members of the list that are in the buffer,
 * and a build starts the count again at zero.
 */
@Tag("vector")
class VectorDeltaScanWorkOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int GRAPH_ROWS = 1200;
  private static final int DELTA_ROWS = 60;

  private final List<RID>     rids    = new ArrayList<>();
  private final List<float[]> vectors = new ArrayList<>();
  private       LSMVectorIndex index;

  @BeforeEach
  void fill() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withMetadata(new JSONObject().put("similarity", "COSINE").put("dimensions", DIMENSIONS));
    builder.create();
    insert(GRAPH_ROWS);
    index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
  }

  @Test
  void everySearchChargesTheRowsOfTheBufferItScored() {
    assertThat(work()).as("a build starts the count again").isZero();
    insert(DELTA_ROWS);
    assertThat(work()).as("rows committed to the buffer are not scans").isZero();

    index.findNeighborsFromVector(vectors.get(3), 5);
    assertThat(work()).as("one search scored every row of the buffer").isEqualTo(DELTA_ROWS);

    index.findNeighborsFromVector(vectors.get(4), 5);
    assertThat(work()).as("the second charges them again").isEqualTo(2L * DELTA_ROWS);
  }

  @Test
  void aSearchOfAnAllowListChargesTheMembersThatAreInTheBuffer() {
    insert(DELTA_ROWS);
    final Set<RID> allowed = new HashSet<>();
    // 20 rows of the buffer, 20 of the graph: only the first 20 are scored by the scan of the buffer
    allowed.addAll(rids.subList(GRAPH_ROWS, GRAPH_ROWS + 20));
    allowed.addAll(rids.subList(0, 20));

    final List<Pair<RID, Float>> answer = index.findNeighborsFromVector(vectors.get(GRAPH_ROWS + 5), 5, 50, allowed);

    assertThat(answer).extracting(Pair::getFirst).isSubsetOf(allowed);
    assertThat(work()).isEqualTo(20);
  }

  @Test
  void aBuildThatAbsorbsTheBufferStartsTheCountAgain() {
    insert(DELTA_ROWS);
    index.findNeighborsFromVector(vectors.get(3), 5);
    assertThat(work()).isPositive();

    index.buildVectorGraphNow();

    assertThat(work()).isZero();
    index.findNeighborsFromVector(vectors.get(4), 5);
    assertThat(work()).as("nothing is buffered any more").isZero();
  }

  private long work() {
    return index.getStats().get("deltaScanWorkSinceRebuild");
  }

  private void insert(final int count) {
    final Random random = new Random(11L + rids.size());
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian();
        vector[0] += 0.5f;
        rids.add(database.newDocument("Doc").set("embedding", vector).save().getIdentity());
        vectors.add(vector);
      }
    });
  }
}
