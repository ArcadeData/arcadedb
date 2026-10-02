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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #8963: {@code db.index.vector.queryNodes} scored COSINE as the raw cosine (down to -1) and DOT_PRODUCT as
 * {@code (3 + dot) / 2} (1 to 2 for unit vectors), while Neo4j's procedure it mirrors returns {@code (1 + cosine) / 2}
 * in [0, 1] for both, and EUCLIDEAN was already {@code 1 / (1 + d^2)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8963VectorQueryNodesScoreRangeTest extends TestHelper {

  @Test
  void cosineScoreIsHalfOfOnePlusCosine() {
    final Map<String, Double> scores = scores("COSINE");
    assertThat(scores.get("same")).isCloseTo(1.0, within(1e-5));
    assertThat(scores.get("orthogonal")).isCloseTo(0.5, within(1e-5));
    assertThat(scores.get("opposite")).isCloseTo(0.0, within(1e-5));
  }

  @Test
  void dotProductScoreIsHalfOfOnePlusDotForUnitVectors() {
    final Map<String, Double> scores = scores("DOT_PRODUCT");
    assertThat(scores.get("same")).isCloseTo(1.0, within(1e-5));
    assertThat(scores.get("orthogonal")).isCloseTo(0.5, within(1e-5));
    assertThat(scores.get("opposite")).isCloseTo(0.0, within(1e-5));
  }

  @Test
  void euclideanScoreIsUnchanged() {
    final Map<String, Double> scores = scores("EUCLIDEAN");
    assertThat(scores.get("same")).isCloseTo(1.0, within(1e-5));
    assertThat(scores.get("orthogonal")).isCloseTo(1.0 / 3.0, within(1e-5));
    assertThat(scores.get("opposite")).isCloseTo(0.2, within(1e-5));
  }

  private Map<String, Double> scores(final String similarity) {
    final String type = "N" + similarity;
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"" + similarity + "\" }");
    database.transaction(() -> {
      database.newVertex(type).set("name", "same", "vector", new float[] { 1f, 0f }).save();
      database.newVertex(type).set("name", "orthogonal", "vector", new float[] { 0f, 1f }).save();
      database.newVertex(type).set("name", "opposite", "vector", new float[] { -1f, 0f }).save();
    });

    final Map<String, Double> result = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('" + type + "[vector]', 3, $v) YIELD node, score RETURN node.name AS name, score",
        Map.of("v", new float[] { 1f, 0f }))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        result.put(r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(result).hasSize(3);
    return result;
  }
}
