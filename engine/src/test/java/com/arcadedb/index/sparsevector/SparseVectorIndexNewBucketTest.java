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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A sparse-vector index must follow its type onto a bucket added after the index was created and onto a sub type created
 * afterwards. Both go through {@code createBucketIndex} with the key types the existing index reports, which for the sparse
 * index were the key types of its internal registration shell (INTEGER, LINK, FLOAT) rather than the types of the two indexed
 * properties, so the sparse factory refused them: {@code ALTER TYPE ... BUCKET +} and {@code CREATE TYPE ... EXTENDS} threw
 * "Sparse vector index requires 2 properties".
 */
class SparseVectorIndexNewBucketTest extends TestHelper {
  private static final String TYPE_NAME    = "NewBucketSparseDoc";
  private static final String EXTRA_BUCKET = TYPE_NAME + "_extra";

  private void createTypeAndIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 2);
      type.createProperty("id", Type.LONG);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" }).withSparseVectorType().withDimensions(16).create();
    });
  }

  private List<Long> neighbours(final int token) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        TYPE_NAME + "[tokens,weights]", new int[] { token }, new float[] { 1f }, 10)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        ids.add(((Number) r.getProperty("id")).longValue());
      }
    }
    return ids;
  }

  @Test
  void bucketAddedAfterTheIndexCarriesTheIndex() {
    createTypeAndIndex();
    database.transaction(() -> database.newDocument(TYPE_NAME).set("id", 1L).set("tokens", new int[] { 3 }).set("weights", new float[] { 1f }).save());

    database.getSchema().getType(TYPE_NAME).addBucket(database.getSchema().createBucket(EXTRA_BUCKET));

    final DocumentType type = database.getSchema().getType(TYPE_NAME);
    assertThat(type.getBuckets(false)).hasSize(3);
    assertThat(type.getAllIndexes(false)).hasSize(1);
    assertThat(type.getAllIndexes(false).iterator().next().getIndexesOnBuckets()).as("one sub index per bucket").hasSize(3);

    // a record of the new bucket is indexed and found
    database.transaction(() -> database.newDocument(TYPE_NAME).set("id", 2L).set("tokens", new int[] { 3 }).set("weights", new float[] { 2f })
        .save(EXTRA_BUCKET));
    assertThat(neighbours(3)).containsExactlyInAnyOrder(1L, 2L);

    reopenDatabase();
    assertThat(neighbours(3)).containsExactlyInAnyOrder(1L, 2L);
  }

  @Test
  void subTypeCreatedAfterTheIndexInheritsTheIndex() {
    createTypeAndIndex();

    database.command("sql", "CREATE DOCUMENT TYPE NewBucketSparseSub EXTENDS " + TYPE_NAME).close();

    assertThat(database.getSchema().existsType("NewBucketSparseSub")).isTrue();
    database.transaction(() -> database.newDocument("NewBucketSparseSub").set("id", 5L).set("tokens", new int[] { 7 }).set("weights", new float[] { 1f }).save());
    assertThat(neighbours(7)).containsExactly(5L);
  }

  @Test
  void indexReportsTheTypesOfItsProperties() {
    createTypeAndIndex();

    final TypeIndex index = database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next();
    assertThat(index.getKeyTypes()).containsExactly(Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_FLOATS);
    for (final IndexInternal bucketIndex : index.getIndexesOnBuckets())
      assertThat(bucketIndex.getKeyTypes()).containsExactly(Type.ARRAY_OF_INTEGERS, Type.ARRAY_OF_FLOATS);

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes WHERE name = ?", index.getName())) {
      assertThat(rs.hasNext()).isTrue();
      final List<String> keyTypes = rs.next().getProperty("keyTypes");
      assertThat(keyTypes).containsExactly(Type.ARRAY_OF_INTEGERS.name(), Type.ARRAY_OF_FLOATS.name());
    }
  }
}
