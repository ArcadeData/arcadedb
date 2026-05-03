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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for dotted nested-field {@code groupBy} on vector search functions (issue #4072).
 * <p>
 * Verifies that {@code groupBy: 'metadata.author'} resolves a nested {@code Map}-typed property
 * across {@code vector.neighbors}, {@code vector.sparseNeighbors}, and {@code vector.fuse}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorNestedGroupByTest extends TestHelper {

  private static final String TYPE_NAME = "NestedGroupedDoc";
  private static final String DENSE_IDX  = "NestedGroupedDoc[embedding]";
  private static final String SPARSE_IDX = "NestedGroupedDoc[tokens,weights]";

  private static final int    DENSE_DIM = 8;
  private static final int    SPARSE_DIM = 50;

  private void buildSchema() {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(TYPE_NAME);
      t.createProperty("metadata", Type.MAP);
      t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      t.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      t.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DENSE_DIM)
          .withSimilarity("COSINE")
          .create();

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(SPARSE_DIM)
          .create();
    });
  }

  /**
   * 100 docs split across 5 authors and 4 categories nested under {@code metadata}. Some docs are
   * deliberately inserted without {@code metadata} at all so we can test the missing-path branch.
   */
  private void seedFixture() {
    database.transaction(() -> {
      final Random rnd = new Random(42L);
      for (int i = 0; i < 100; i++) {
        final float[] dense = new float[DENSE_DIM];
        for (int d = 0; d < DENSE_DIM; d++) dense[d] = rnd.nextFloat();

        final int[]   tokens = new int[3];
        final float[] weights = new float[3];
        final HashSet<Integer> picked = new HashSet<>();
        for (int z = 0; z < 3; z++) {
          int dim;
          do {
            dim = rnd.nextInt(SPARSE_DIM);
          } while (!picked.add(dim));
          tokens[z] = dim;
          weights[z] = 0.1f + rnd.nextFloat();
        }

        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("embedding", dense);
        doc.set("tokens", tokens);
        doc.set("weights", weights);

        // 90 documents have a populated `metadata` map; 10 are deliberately missing the field.
        if (i < 90) {
          final Map<String, Object> meta = new HashMap<>();
          meta.put("author", "author_" + (i % 5));
          meta.put("category", "cat_" + (i % 4));
          doc.set("metadata", meta);
        }
        doc.save();
      }
    });
  }

  @Test
  void denseGroupByNestedAuthor() {
    buildSchema();
    seedFixture();

    final float[] queryVec = new float[DENSE_DIM];
    queryVec[0] = 1.0f;

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`(?, ?, ?, { groupBy: 'metadata.author', groupSize: 1 }))",
        DENSE_IDX, queryVec, 5);

    final Set<Object> seenAuthors = new HashSet<>();
    int rowCount = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final Map<String, Object> meta = (Map<String, Object>) row.getProperty("metadata");
      final Object author = meta == null ? null : meta.get("author");
      assertThat(seenAuthors.add(author))
          .as("author %s appeared more than once", author).isTrue();
      rowCount++;
    }
    assertThat(rowCount).isPositive();
    // 5 distinct authors at most. Missing-metadata rows would land in the null group.
    assertThat(seenAuthors.size()).isLessThanOrEqualTo(5 + 1);
  }

  @Test
  void sparseGroupByNestedAuthor() {
    buildSchema();
    seedFixture();

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, { groupBy: 'metadata.author', groupSize: 1 }))",
        SPARSE_IDX, new int[] { 0, 1, 2 }, new float[] { 1.0f, 1.0f, 1.0f }, 5);

    final Set<Object> seenAuthors = new HashSet<>();
    int rowCount = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final Map<String, Object> meta = (Map<String, Object>) row.getProperty("metadata");
      final Object author = meta == null ? null : meta.get("author");
      assertThat(seenAuthors.add(author))
          .as("author %s appeared more than once", author).isTrue();
      rowCount++;
    }
    assertThat(rowCount).isPositive();
    assertThat(seenAuthors.size()).isLessThanOrEqualTo(5 + 1);
  }

  @Test
  void fuseGroupByNestedAuthor() {
    buildSchema();
    seedFixture();

    final float[] queryVec = new float[DENSE_DIM];
    queryVec[0] = 1.0f;

    final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.fuse`(
            `vector.neighbors`(?, ?, ?),
            `vector.sparseNeighbors`(?, ?, ?, ?),
            { fusion: 'RRF', groupBy: 'metadata.author', groupSize: 1 }
        ))""",
        DENSE_IDX, queryVec, 20,
        SPARSE_IDX, new int[] { 0, 1, 2 }, new float[] { 1.0f, 1.0f, 1.0f }, 20);

    final Set<Object> seenAuthors = new HashSet<>();
    int rowCount = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final Map<String, Object> meta = (Map<String, Object>) row.getProperty("metadata");
      final Object author = meta == null ? null : meta.get("author");
      assertThat(seenAuthors.add(author))
          .as("author %s appeared more than once", author).isTrue();
      rowCount++;
    }
    assertThat(rowCount).isPositive();
    assertThat(seenAuthors.size()).isLessThanOrEqualTo(5 + 1);
  }

  @Test
  void missingNestedPathFallsIntoNullGroup() {
    buildSchema();
    // Only insert documents that LACK metadata so every record's `metadata.author` is null.
    database.transaction(() -> {
      final Random rnd = new Random(7L);
      for (int i = 0; i < 10; i++) {
        final float[] dense = new float[DENSE_DIM];
        for (int d = 0; d < DENSE_DIM; d++) dense[d] = rnd.nextFloat();

        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("embedding", dense);
        doc.set("tokens", new int[] { 0 });
        doc.set("weights", new float[] { 1.0f });
        doc.save();
      }
    });

    final float[] queryVec = new float[DENSE_DIM];
    queryVec[0] = 1.0f;

    // groupSize=1 with all docs in the same (null) group must collapse to a single result.
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`(?, ?, ?, { groupBy: 'metadata.author', groupSize: 1 }))",
        DENSE_IDX, queryVec, 5);

    int rowCount = 0;
    while (rs.hasNext()) {
      rs.next();
      rowCount++;
    }
    assertThat(rowCount).as("all docs share the null group, groupSize=1 caps to one row").isEqualTo(1);
  }

  @Test
  void flatPropertyStillWorks() {
    // Backwards compatibility: a non-dotted property name behaves identically to before.
    buildSchema();
    database.transaction(() -> {
      final DocumentType t = database.getSchema().getType(TYPE_NAME);
      t.createProperty("source_file", Type.STRING);
      final Random rnd = new Random(99L);
      for (int i = 0; i < 30; i++) {
        final float[] dense = new float[DENSE_DIM];
        for (int d = 0; d < DENSE_DIM; d++) dense[d] = rnd.nextFloat();
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("embedding", dense);
        doc.set("tokens", new int[] { 0 });
        doc.set("weights", new float[] { 1.0f });
        doc.set("source_file", "file_" + (i % 6));
        doc.save();
      }
    });

    final float[] queryVec = new float[DENSE_DIM];
    queryVec[0] = 1.0f;

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`(?, ?, ?, { groupBy: 'source_file', groupSize: 1 }))",
        DENSE_IDX, queryVec, 6);

    final Set<String> sources = new HashSet<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      sources.add((String) row.getProperty("source_file"));
    }
    assertThat(sources).hasSize(6);
  }
}
