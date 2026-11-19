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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12035: renaming a type moves the per-bucket sub-indexes of an LSM_VECTOR index to the new type name, like every other index
 * kind, so the schema catalog keeps resolving their type and never attributes them to a later type created under the old name.
 */
class TypeRenameMovesVectorIndexTest extends TestHelper {

  @Test
  void renamingTheIndexedTypeMovesTheBucketIndexToTheNewName() {
    createVectorType("Doc");
    insert("Doc", 1, 1.0f, 0.0f);

    database.command("sql", "ALTER TYPE Doc NAME Chunk");

    assertThat(typeNamesOfIndexes()).containsOnlyKeys("Chunk[vec]", bucketIndexName("Chunk"))
        .allSatisfy((name, typeName) -> assertThat(typeName).isEqualTo("Chunk"));
    assertThat(nearest("Chunk[vec]")).isEqualTo(1);

    // A new type under the old name owns none of the renamed type's indexes
    database.command("sql", "CREATE VERTEX TYPE Doc");
    assertThat(typeNamesOfIndexes()).allSatisfy((name, typeName) -> assertThat(typeName).isEqualTo("Chunk"));
    database.command("sql", "DROP TYPE Doc");
    assertThat(typeNamesOfIndexes()).allSatisfy((name, typeName) -> assertThat(typeName).isEqualTo("Chunk"));

    reopenDatabase();
    assertThat(typeNamesOfIndexes()).allSatisfy((name, typeName) -> assertThat(typeName).isEqualTo("Chunk"));
    assertThat(nearest("Chunk[vec]")).isEqualTo(1);
  }

  @Test
  void renamingASubTypeMovesTheInheritedBucketIndexToTheNewName() {
    createVectorType("A");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS A");
    insert("B", 1, 1.0f, 0.0f);

    database.command("sql", "ALTER TYPE B NAME C");

    final String onSubTypeBucket = bucketIndexName("C");
    assertThat(typeNamesOfIndexes()).containsEntry(onSubTypeBucket, "C").containsEntry("A[vec]", "A");
    assertThat(nearest("A[vec]")).isEqualTo(1);

    reopenDatabase();
    assertThat(typeNamesOfIndexes()).containsEntry(onSubTypeBucket, "C").containsEntry("A[vec]", "A");
  }

  private void createVectorType(final String type) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".vec ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON " + type + " (vec) LSM_VECTOR METADATA {dimensions: 2, similarity: 'EUCLIDEAN'}");
  }

  private void insert(final String type, final int n, final float x, final float y) {
    database.transaction(() -> database.newVertex(type).set("n", n).set("vec", new float[] { x, y }).save());
  }

  /** The name of the vector index sub-index on the first bucket of {@code type}. */
  private String bucketIndexName(final String type) {
    final int bucketId = database.getSchema().getType(type).getBucketIds(false).getFirst();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof TypeIndex typeIndex)
        for (final Index onBucket : typeIndex.getIndexesOnBuckets())
          if (onBucket.getAssociatedBucketId() == bucketId)
            return onBucket.getName();
    throw new AssertionError("No index on the bucket of " + type);
  }

  /** Index name to type name, as the schema catalog answers them. */
  private Map<String, String> typeNamesOfIndexes() {
    final Map<String, String> names = new TreeMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT name, typeName FROM schema:indexes")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        names.put(row.getProperty("name"), row.getProperty("typeName"));
      }
    }
    return names;
  }

  private int nearest(final String indexName) {
    try (final ResultSet rs = database.query("sql",
        "SELECT n FROM (SELECT expand(vectorNeighbors('" + indexName + "', [1.0, 0.0], 5)))")) {
      return rs.next().<Integer>getProperty("n");
    }
  }
}
