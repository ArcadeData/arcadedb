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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8962: an all-zero vector was stored and then dropped by every read path even under EUCLIDEAN, where the
 * origin is a regular point (and the nearest one to [0.01, 0.01]); a vector with a NaN or an Infinity component was
 * accepted, indexed and returned with a NaN or infinite distance, under DOT_PRODUCT ahead of every finite record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8962ZeroAndNonFiniteVectorTest extends TestHelper {

  @Test
  void euclideanReturnsTheOriginAsNearest() {
    final String type = create("EUCLIDEAN");
    database.transaction(() -> {
      database.newDocument(type).set("name", "origin", "vector", new float[] { 0f, 0f }).save();
      database.newDocument(type).set("name", "near", "vector", new float[] { 0.1f, 0.1f }).save();
      database.newDocument(type).set("name", "far", "vector", new float[] { 3f, 4f }).save();
    });

    assertThat(neighbors(type)).containsExactly("origin", "near", "far");
  }

  @Test
  void nonFiniteComponentsAreRejected() {
    for (final String similarity : List.of("EUCLIDEAN", "DOT_PRODUCT", "COSINE")) {
      final String type = create(similarity);
      for (final float bad : new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY })
        assertThatThrownBy(() -> database.transaction(() -> database.newDocument(type).set("name", "bad", "vector", new float[] { bad, 1f }).save()))
            .hasStackTraceContaining("finite");

      database.transaction(() -> database.newDocument(type).set("name", "ok", "vector", new float[] { 0.6f, 0.8f }).save());
      assertThat(neighbors(type)).containsExactly("ok");
    }
  }

  @Test
  void cosineKeepsAcceptingZeroPlaceholderVectors() {
    final String type = create("COSINE");
    database.transaction(() -> {
      database.newDocument(type).set("name", "placeholder", "vector", new float[] { 0f, 0f }).save();
      database.newDocument(type).set("name", "real", "vector", new float[] { 0.6f, 0.8f }).save();
    });

    assertThat(neighbors(type)).containsExactly("real");
  }

  private String create(final String similarity) {
    final String type = "P" + similarity;
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"" + similarity + "\" }");
    return type;
  }

  private List<String> neighbors(final String type) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name, distance FROM (SELECT expand(vectorNeighbors('" + type + "[vector]', ?, 10)))",
        (Object) new float[] { 0.01f, 0.01f })) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.getProperty("name"));
      }
    }
    return names;
  }
}
