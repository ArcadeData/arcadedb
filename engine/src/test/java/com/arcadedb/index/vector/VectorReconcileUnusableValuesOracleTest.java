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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A rebuild in place ({@code REBUILD INDEX}, {@link LSMVectorIndex#reconcileWithBucket}) makes the index hold what a build from the records
 * would: a record whose value is not a vector of the index's dimensions, or holds a component that is not a finite number, is left out and
 * counted (issue #10938), where a vector of the right shape is indexed. The oracle is the set of records a reader can tell are vectors of
 * the index.
 */
class VectorReconcileUnusableValuesOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;

  @Test
  void aRecordHoldingNoVectorOfTheIndexIsLeftOutAndCountedByARebuildInPlace() {
    final Random random = new Random(5);
    final List<RID> good = new ArrayList<>();
    final List<RID> bad = new ArrayList<>();
    database.getSchema().createDocumentType("Rc", 1).createProperty("embedding", Type.ARRAY_OF_FLOATS);
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        good.add(save(vector(random)));
      bad.add(save(new float[DIMENSIONS - 3]));
      bad.add(save(new float[DIMENSIONS + 1]));
      final float[] withNaN = vector(random);
      withNaN[2] = Float.NaN;
      bad.add(save(withNaN));
      final float[] withInfinity = vector(random);
      withInfinity[5] = Float.POSITIVE_INFINITY;
      bad.add(save(withInfinity));
    });
    database.command("sql",
        "CREATE INDEX ON Rc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");
    final LSMVectorIndex bucketIndex = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Rc[embedding]"))
        .getIndexesOnBuckets()[0];
    assertThat(bucketIndex.countEntries()).as("a build holds the records that are vectors of the index").isEqualTo(good.size());

    final long written = bucketIndex.reconcileWithBucket(5, null);

    assertThat(written).as("the index already holds exactly what the records say").isZero();
    assertThat(bucketIndex.getLastBuildSkippedRecords()).as("the records that hold no vector of the index").isEqualTo(bad.size());
    assertThat(bucketIndex.countEntries()).isEqualTo(good.size());
    for (final RID rid : good)
      assertThat(bucketIndex.hasLiveVector(rid)).as("%s is indexed", rid).isTrue();
    for (final RID rid : bad)
      assertThat(bucketIndex.hasLiveVector(rid)).as("%s is not indexed", rid).isFalse();
  }

  private RID save(final float[] vector) {
    final MutableDocument document = database.newDocument("Rc").set("embedding", vector);
    document.save();
    return document.getIdentity();
  }

  private static float[] vector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat();
    return vector;
  }
}
