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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7057 for the third vector entry point. {@code vector.sparseNeighbors} fans out over the type's sparse
 * sub-indexes exactly the way {@code db.index.vector.queryNodes} and {@code vector.neighbors} do - one search per
 * sub-index, concatenate, sort, truncate at k - and so duplicated the same way when a sub-index was listed twice.
 * It is also reachable through {@code vector.fuse}, which takes both functions as pluggable input sources.
 */
class SparseNeighborsDuplicateSubIndexTest extends TestHelper {
  private static final String TYPE_NAME = "SparseDoc";
  private static final int    DOCS      = 60;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("uuid", Type.STRING);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++) {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("uuid", "d" + i);
        doc.set("tokens", new int[] { i % 50, (i + 7) % 50, (i + 13) % 50 });
        doc.set("weights", new float[] { 1.0f, 0.5f, 0.25f });
        doc.save();
      }
    });
  }

  @Test
  void sparseNeighborsReturnsDistinctRecordsWhenTheSameSubIndexIsAttachedTwice() {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME)
        .getPolymorphicIndexByProperties("tokens", "weights");
    final IndexInternal[] before = typeIndex.getIndexesOnBuckets();
    for (final IndexInternal bucketIndex : before)
      typeIndex.addIndexOnBucket(bucketIndex);

    assertThat(typeIndex.getIndexesOnBuckets())
        .as("the fixture has to actually plant the multiplicity, or the assertion below cannot fail")
        .hasSize(before.length * 2);

    final List<String> uuids = topK(6);

    assertThat(uuids).hasSize(6);
    assertThat(new LinkedHashSet<>(uuids)).as("rows %s must all be distinct records", uuids).hasSize(6);
  }

  /**
   * The sparse counterpart of {@code vectorNeighborsSurvivesAnUnboundedLimit}: k is caller-supplied and only
   * checked for {@code <= 0}, and {@code RidHashSet} allocates its backing arrays up front, so the dedup set has to
   * be sized from the candidates actually fetched. Sized from k, {@link Integer#MAX_VALUE} rounds up to a negative
   * capacity and throws {@code NegativeArraySizeException} before a single record is read.
   */
  @Test
  void sparseNeighborsSurvivesAnUnboundedLimit() {
    final List<String> uuids = topK(Integer.MAX_VALUE);

    assertThat(uuids).isNotEmpty();
    assertThat(new LinkedHashSet<>(uuids)).hasSameSizeAs(uuids);
  }

  /** Control: the ordinary path is untouched. */
  @Test
  void sparseNeighborsStillAnswersWithoutTheDuplication() {
    final List<String> uuids = topK(6);

    assertThat(uuids).hasSize(6);
    assertThat(new LinkedHashSet<>(uuids)).hasSize(6);
  }

  private List<String> topK(final int k) {
    final List<String> uuids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        TYPE_NAME + "[tokens,weights]", new int[] { 3, 10, 16 }, new float[] { 1.0f, 0.5f, 0.25f }, k)) {
      while (rs.hasNext())
        uuids.add(rs.next().getProperty("uuid"));
    }
    return uuids;
  }
}
