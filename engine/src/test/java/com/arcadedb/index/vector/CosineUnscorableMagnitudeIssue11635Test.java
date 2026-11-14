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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for #11635: the COSINE similarity is computed in float as sum / sqrt(norm1 * norm2), so a vector whose
 * squared norm underflows or overflows a float (the extreme float vectors of Neo4j's ExtremeValuesLibrary) scored -Infinity,
 * NaN or a flat 0: a stored one ranked first for every query it was not orthogonal to. Such a vector is now as unscorable
 * as an all-zero one: left out of the index and refused as a query.
 */
class CosineUnscorableMagnitudeIssue11635Test extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aVectorTheFloatCosineCannotScoreIsNotIndexedNorAcceptedAsAQuery(final String quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE C");
      database.command("sql", "CREATE PROPERTY C.e STRING");
      database.command("sql", "CREATE PROPERTY C.v ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON C (v) LSM_VECTOR METADATA {\"dimensions\": 4, \"similarity\": \"COSINE\", "
          + "\"quantization\": \"" + quantization + "\"}");
      insert("x", 1f, 0f, 0f, 0f);
      insert("xy", 1f, 1f, 0f, 0f);
      insert("y", 0f, 1f, 0f, 0f);
      insert("tiny", 1e-25f, 1e-25f, 0f, 0f);
      insert("huge", 1e20f, 1e20f, 0f, 0f);
      insert("subnormal", Float.MIN_VALUE, Float.MIN_VALUE, Float.MIN_VALUE, Float.MIN_VALUE);
      insert("max", Float.MAX_VALUE, Float.MAX_VALUE, Float.MAX_VALUE, Float.MAX_VALUE);
    });

    // only the three vectors the float cosine can score are answered, at their exact distance
    final Map<String, Float> neighbours = neighbours(new float[] { 1f, 1f, 0f, 0f });
    assertThat(neighbours).containsOnlyKeys("xy", "x", "y");
    assertThat(neighbours.get("xy")).isCloseTo(0f, within(0.01f));
    assertThat(neighbours.get("x")).isCloseTo(0.2929f, within(0.01f));
    assertThat(neighbours.values()).allSatisfy(d -> assertThat(d).isBetween(-0.001f, 2.001f));

    // the same rule for a query: an all-zero vector was refused already, now one too small or too large is
    for (final float component : new float[] { 1e-30f, 1e-22f, 1e20f, Float.MAX_VALUE, Float.MIN_VALUE })
      assertThatThrownBy(() -> neighbours(new float[] { component, 0f, 0f, 0f })).as("component %s", component)
          .hasMessageContaining("COSINE");
    // inside the range the distances are the exact ones
    for (final float component : new float[] { 1e-9f, 1e9f })
      assertThat(neighbours(new float[] { component, 0f, 0f, 0f }).get("x")).as("component %s", component)
          .isCloseTo(0f, within(0.001f));
  }

  @ParameterizedTest
  @ValueSource(floats = { 1e20f, 1e-25f, Float.MAX_VALUE })
  void cosineSimilarityOfVectorsBeyondTheFloatNormRangeIsComputedInDouble(final float component) {
    assertThat(VectorUtils.cosineSimilarity(new float[] { component, 0f, 0f, 0f }, new float[] { component, 0f, 0f, 0f }))
        .isCloseTo(1f, within(1e-6f));
    assertThat(VectorUtils.cosineSimilarity(new float[] { component, component, 0f, 0f }, new float[] { 1f, 0f, 0f, 0f }))
        .isCloseTo(0.70710677f, within(1e-6f));
  }

  private void insert(final String name, final float... vector) {
    database.newVertex("C").set("e", name).set("v", vector).save();
  }

  private Map<String, Float> neighbours(final float[] query) {
    final Map<String, Float> result = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT e, distance FROM (SELECT expand(vector.neighbors('C[v]', ?, 10)))", (Object) query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.put(row.getProperty("e"), row.<Number>getProperty("distance").floatValue());
      }
    }
    return result;
  }
}
