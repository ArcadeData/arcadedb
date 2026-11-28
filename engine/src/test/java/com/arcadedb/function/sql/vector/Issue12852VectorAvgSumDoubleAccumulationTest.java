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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #12852: {@code vector.avg} and {@code vector.sum} kept their running sum in a float. Past a few thousand rows the sum
 * stopped growing next to the addends (a million rows of 0.1 averaged 0.10095834), and an average whose sum did not fit a float
 * answered Infinity although the average did.
 */
class Issue12852VectorAvgSumDoubleAccumulationTest extends TestHelper {

  @Test
  void anAverageWhoseSumOverflowsAFloatIsTheTrueAverage() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("T").createProperty("v", Type.ARRAY_OF_FLOATS);
      database.newDocument("T").set("v", new float[] { 3.0e38f, 1f }).save();
      database.newDocument("T").set("v", new float[] { 3.0e38f, 3f }).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT vector.avg(v) AS avg, vector.sum(v) AS sum FROM T")) {
      final var row = rs.next();
      assertThat(row.<float[]>getProperty("avg")).containsExactly(3.0e38f, 2f);
      // the control: the true sum 6e38 is beyond a float, so Infinity is the answer there
      assertThat(row.<float[]>getProperty("sum")).containsExactly(Float.POSITIVE_INFINITY, 4f);
    }
  }

  @Test
  void aMillionRowsDoNotDrift() {
    final SQLFunctionVectorAvg avg = new SQLFunctionVectorAvg();
    final SQLFunctionVectorSum sum = new SQLFunctionVectorSum();
    final Object[] row = { new float[] { 0.1f, 1f } };
    for (int i = 0; i < 1_000_000; i++) {
      avg.execute(null, null, null, row, null);
      sum.execute(null, null, null, row, null);
    }

    final float[] average = (float[]) avg.getResult();
    assertThat(average[0]).isCloseTo(0.1f, within(1.0e-7f));
    assertThat(average[1]).isEqualTo(1f);
    final float[] total = (float[]) sum.getResult();
    // 1,000,000 x 0.1f = 100000.0015...: the float running sum answered 100958.34
    assertThat(total[0]).isCloseTo(100000f, within(0.5f));
    assertThat(total[1]).isEqualTo(1_000_000f);
  }

  @Test
  void aSumPastTheFloatIntegerRangeKeepsGrowing() {
    // a float stops counting at 2^24: adding 1 to 16,777,216 leaves it unchanged
    final SQLFunctionVectorAvg avg = new SQLFunctionVectorAvg();
    final Object[] row = { new float[] { 1f } };
    for (int i = 0; i < 17_000_000; i++)
      avg.execute(null, null, null, row, null);
    assertThat(((float[]) avg.getResult())[0]).isEqualTo(1f);
  }

  @Test
  void aRecommendationCentroidWhoseSumOverflowsAFloatIsTheTrueMean() {
    // vector.recommend averages its example embeddings the same way, and overflowed the same way
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().createDocumentType("E").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      rids.add(database.newDocument("E").set("embedding", new float[] { 3.0e38f, 1f }).save().getIdentity());
      rids.add(database.newDocument("E").set("embedding", new float[] { 3.0e38f, 3f }).save().getIdentity());
    });

    final float[] centroid = new SQLFunctionVectorRecommend().centroidFromRids(database, rids, "embedding", "positives");
    assertThat(centroid).containsExactly(3.0e38f, 2f);
  }
}
