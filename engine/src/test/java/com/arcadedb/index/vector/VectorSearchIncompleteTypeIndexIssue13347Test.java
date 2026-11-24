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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A vector index without the bucket index of a bucket that holds records is not valid ({@link TypeIndex#isValid()}), and the engine says so
 * at startup, but {@code vector.neighbors}, {@code db.index.vector.queryNodes} and the Cypher {@code SEARCH} clause resolved the bucket indexes
 * they search without asking, and answered the records of the other buckets with no error (issue #13347). A server killed during
 * {@code DROP INDEX} of a multi-bucket vector index leaves exactly this state. There is no scan to fall back on, so the search refuses and says
 * that the index has to be created again, as {@code lookupByKey} does for the same state (#11303). ({@code REBUILD INDEX} does not repair it either: it fails with "cannot await a fill", issue #13350.)
 * <p>
 * The state is built here the way the repair tests of #11303 build it: the Java API drops one bucket index by its own name.
 */
class VectorSearchIncompleteTypeIndexIssue13347Test extends TestHelper {
  private static final String INDEX   = "Doc[embedding]";
  private static final int    RECORDS = 200;
  private static final int    BUCKETS = 4;

  @BeforeEach
  void createDocs() {
    database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS " + BUCKETS);
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("Doc").set("embedding", new float[] { i, 1f }).save();
    });
  }

  @Test
  void aSearchOfAnIndexMissingABucketWithRecordsRefusesInEveryEntryPoint() {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(neighbors(RECORDS + 10)).as("precondition: the complete index answers every record").isEqualTo(RECORDS);

    final Bucket bucket = database.getSchema().getType("Doc").getBuckets(false).getFirst();
    assertThat(database.countBucket(bucket.getName())).as("the bucket holds records").isPositive();
    database.getSchema().dropIndex(bucketIndexOf(index, bucket).getName());
    assertThat(index.isValid()).isFalse();

    final Throwable sql = catchThrowable(() -> neighbors(RECORDS + 10));
    assertThat(sql).as("vector.neighbors").hasMessageContaining(INDEX).hasMessageContaining(bucket.getName()).hasMessageContaining("create it again");

    final Throwable queryNodes = catchThrowable(() -> drainCypher(
        "CALL db.index.vector.queryNodes('" + INDEX + "', 10, $q) YIELD node RETURN node"));
    assertThat(queryNodes).as("db.index.vector.queryNodes").hasMessageContaining(INDEX).hasMessageContaining("create it again");

    final Throwable search = catchThrowable(() -> drainCypher(
        "MATCH (d:Doc) SEARCH d IN (VECTOR INDEX `" + INDEX + "` FOR $q LIMIT 10) RETURN d"));
    assertThat(search).as("SEARCH clause").hasMessageContaining(INDEX).hasMessageContaining("create it again");

    // dropping and creating the index again repairs it, and the search answers every record
    recreateIndex();
    assertThat(neighbors(RECORDS + 10)).isEqualTo(RECORDS);
  }

  @Test
  void aBucketWithoutRecordsDoesNotMakeTheSearchRefuse() {
    // ALTER TYPE ... BUCKET + adds an empty bucket and then its bucket index: in between the index is incomplete, and nothing is missing from it
    database.command("sql", "CREATE BUCKET Doc_extra");
    database.command("sql", "ALTER TYPE Doc BUCKET +Doc_extra");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final Bucket extra = database.getSchema().getBucketByName("Doc_extra");
    database.getSchema().dropIndex(bucketIndexOf(index, extra).getName());
    assertThat(index.isComplete()).isFalse();
    assertThat(database.countBucket("Doc_extra")).isZero();

    assertThat(neighbors(RECORDS + 10)).isEqualTo(RECORDS);

    // left complete for the integrity check of the test base class
    recreateIndex();
    assertThat(((TypeIndex) database.getSchema().getIndexByName(INDEX)).isComplete()).isTrue();
  }

  private void recreateIndex() {
    database.command("sql", "DROP INDEX `" + INDEX + "`");
    database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
  }

  private int neighbors(final int k) {
    return drain("sql", "SELECT expand(vector.neighbors('" + INDEX + "', ?, ?))", List.of(0f, 1f), k);
  }

  private int drain(final String language, final String statement, final Object... args) {
    int rows = 0;
    try (final ResultSet rs = database.query(language, statement, args)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  private int drainCypher(final String statement) {
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher", statement, Map.of("q", List.of(0f, 1f)))) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  private static IndexInternal bucketIndexOf(final TypeIndex index, final Bucket bucket) {
    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      if (onBucket.getAssociatedBucketId() == bucket.getFileId())
        return onBucket;
    throw new IllegalStateException("No bucket index for bucket " + bucket.getName());
  }
}
