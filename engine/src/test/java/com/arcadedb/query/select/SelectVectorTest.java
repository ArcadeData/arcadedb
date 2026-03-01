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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for vector k-NN search in the Select API.
 */
public class SelectVectorTest extends TestHelper {

  private static final int DIMENSIONS = 8;

  @Test
  void okBasicKnn() {
    createSchemaAndData();
    closeAndReopenDatabase();

    final float[] query = new float[DIMENSIONS];
    Arrays.fill(query, 0.5f);

    final List<SelectVectorResult<Vertex>> results = database.select()
        .fromType("Product")
        .nearestTo("embedding", query, 5)
        .vertices();

    assertThat(results).isNotEmpty();
    assertThat(results.size()).isLessThanOrEqualTo(5);

    // Verify distance ordering (ascending)
    for (int i = 1; i < results.size(); i++)
      assertThat(results.get(i).getDistance()).isGreaterThanOrEqualTo(results.get(i - 1).getDistance());

    // Verify each result has a document
    results.forEach(r -> assertThat(r.getDocument()).isNotNull());
  }

  @Test
  void okKnnWithPostFilter() {
    createSchemaAndData();
    closeAndReopenDatabase();

    final float[] query = new float[DIMENSIONS];
    Arrays.fill(query, 0.5f);

    final List<SelectVectorResult<Vertex>> results = database.select()
        .fromType("Product")
        .nearestTo("embedding", query, 50)
        .where().property("category").eq().value("electronics")
        .vectorVertices();

    assertThat(results).isNotEmpty();
    results.forEach(r -> assertThat(r.getDocument().getString("category")).isEqualTo("electronics"));
  }

  @Test
  void okKnnRequestMoreThanAvailable() {
    createSchemaAndData();
    closeAndReopenDatabase();

    final float[] query = new float[DIMENSIONS];
    Arrays.fill(query, 0.5f);

    final List<SelectVectorResult<Vertex>> results = database.select()
        .fromType("Product")
        .nearestTo("embedding", query, 1000)
        .vertices();

    assertThat(results).hasSizeLessThanOrEqualTo(50);
  }

  @Test
  void errorMissingIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE NoIndex IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY NoIndex.vec IF NOT EXISTS ARRAY_OF_FLOATS");
    });

    assertThatThrownBy(() -> database.select()
        .fromType("NoIndex")
        .nearestTo("vec", new float[] { 1, 2, 3 }, 5)
        .vertices())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No index found");
  }

  @Test
  void errorMissingFromType() {
    assertThatThrownBy(() -> database.select()
        .nearestTo("embedding", new float[] { 1, 2, 3 }, 5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FromType must be set");
  }

  private void createSchemaAndData() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Product IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Product.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.category IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.embedding IF NOT EXISTS ARRAY_OF_FLOATS");

      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Product (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 8,
            "similarity" : "EUCLIDEAN",
            "maxConnections" : 16,
            "beamWidth" : 100
          }""");

      final Random rng = new Random(42);
      for (int i = 0; i < 50; i++) {
        final float[] vec = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vec[j] = rng.nextFloat();
        final String category = i < 25 ? "electronics" : "clothing";
        database.newVertex("Product").set("name", "Product" + i, "category", category, "embedding", vec).save();
      }
    });
  }

  private void closeAndReopenDatabase() {
    final String dbPath = database.getDatabasePath();
    database.close();
    database = new DatabaseFactory(dbPath).open();
  }
}
