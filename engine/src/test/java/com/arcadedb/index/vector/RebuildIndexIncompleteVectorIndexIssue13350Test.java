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
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code REBUILD INDEX} of an LSM_VECTOR index that lacks the bucket index of a bucket (what a server killed during {@code DROP INDEX} leaves, and the
 * state the startup line tells the user to repair with {@code REBUILD INDEX}) failed with "cannot await a fill": the in-place rebuild gives the missing
 * bucket index a fill flag, which only the LSM_TREE, FULL_TEXT, GEOSPATIAL and HASH bucket indexes keep (issue #13350). The in-place rebuild now refuses
 * such an index, so {@code REBUILD INDEX} drops it and creates it again, as it does for every other refusal.
 */
class RebuildIndexIncompleteVectorIndexIssue13350Test extends TestHelper {
  private static final String INDEX   = "Doc[embedding]";
  private static final int    RECORDS = 200;

  @Test
  void rebuildIndexRepairsAVectorIndexMissingABucketIndex() {
    database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS 4");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("Doc").set("embedding", new float[] { i, 1f }).save();
    });
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final Bucket bucket = database.getSchema().getType("Doc").getBuckets(false).getFirst();
    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      if (onBucket.getAssociatedBucketId() == bucket.getFileId()) {
        database.getSchema().dropIndex(onBucket.getName());
        break;
      }
    assertThat(index.isComplete()).as("precondition").isFalse();

    database.command("sql", "REBUILD INDEX `" + INDEX + "`");

    final TypeIndex rebuilt = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(rebuilt.isComplete()).isTrue();
    assertThat(rebuilt.isValid()).isTrue();
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT expand(vector.neighbors('" + INDEX + "', ?, ?))", List.of(0f, 1f), RECORDS + 10)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    assertThat(rows).as("the rebuilt index answers every record").isEqualTo(RECORDS);
  }
}
