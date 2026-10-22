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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.BucketLSMVectorIndexBuilder;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10438: an {@code LSM_VECTOR} index created in the same transaction as the writes it should
 * cover - the shape of a single {@code sqlscript} request over HTTP, which the handler wraps in one transaction -
 * answered every search with no rows. The vector families were excluded from sharing the caller's transaction
 * ({@code Schema.INDEX_TYPE.buildCanShareCallerTransaction}), so their build ran in a nested transaction of its own,
 * which cannot see the caller's uncommitted inserts (reported as {@code totalIndexed: 0}) and reads the committed image
 * of records the caller has updated (reported as {@code totalIndexed: 3} with no vector to index). The caller's writes
 * predate the index, so none of them had queued an index entry either: the index stayed empty after the commit.
 * <p>
 * One test per entry point that decides whether a build shares the caller's transaction: the type-level builder
 * (SQL {@code CREATE INDEX} and the Java API), the bucket-level builder, and the propagation of a super type's indexes.
 */
class Issue10438VectorIndexCreatedInWritingScriptTest extends TestHelper {

  private static final float[] QUERY = { 0.9f, 0.1f, 0.0f };

  /** The second reproduction of the issue: records inserted with their vectors, then the index, in one script. */
  @Test
  void indexCreatedAfterInsertsInSameScriptIsSearchable() {
    final ResultSet created = database.command("sqlscript", """
        CREATE VERTEX TYPE Acc3; CREATE PROPERTY Acc3.behavior ARRAY_OF_FLOATS;
        BEGIN;
        INSERT INTO Acc3 SET behavior=[0.9,0.1,0.0]; INSERT INTO Acc3 SET behavior=[0.1,0.9,0.0];
        CREATE INDEX ON Acc3 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'};
        COMMIT;
        """);
    assertThat(created.hasNext()).isTrue();

    assertEntries("Acc3[behavior]", 2);
    assertThat(neighbors("Acc3[behavior]")).hasSize(2);
  }

  /** The first reproduction of the issue: vectors set by UPDATE on committed records, then the index, in one script. */
  @Test
  void indexCreatedAfterUpdatesInSameScriptIsSearchable() {
    database.command("sqlscript",
        "CREATE VERTEX TYPE Acc; BEGIN; INSERT INTO Acc SET id='A'; INSERT INTO Acc SET id='B'; INSERT INTO Acc SET id='X'; COMMIT;");
    database.command("sqlscript", """
        CREATE PROPERTY Acc.behavior ARRAY_OF_FLOATS;
        BEGIN;
        UPDATE Acc SET behavior = [0.9, 0.1, 0.0] WHERE id IN ['A','B'];
        UPDATE Acc SET behavior = [0.85, 0.15, 0.05] WHERE id = 'X';
        CREATE INDEX ON Acc (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'};
        COMMIT;
        """);

    assertEntries("Acc[behavior]", 3);
    assertThat(neighbors("Acc[behavior]")).hasSize(3);
  }

  /** The totalIndexed the CREATE INDEX reports must agree with what the index can find. */
  @Test
  void totalIndexedAgreesWithTheSearch() {
    database.command("sql", "CREATE VERTEX TYPE Acc5");
    database.command("sql", "CREATE PROPERTY Acc5.behavior ARRAY_OF_FLOATS");
    final long[] totalIndexed = new long[1];
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Acc5 SET behavior=[0.9,0.1,0.0]");
      database.command("sql", "INSERT INTO Acc5 SET behavior=[0.1,0.9,0.0]");
      try (final ResultSet rs = database.command("sql",
          "CREATE INDEX ON Acc5 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}")) {
        totalIndexed[0] = ((Number) rs.next().getProperty("totalIndexed")).longValue();
      }
    });

    assertThat(totalIndexed[0]).isEqualTo(2L);
    assertEntries("Acc5[behavior]", 2);
    assertThat(neighbors("Acc5[behavior]")).hasSize(2);
  }

  /** The index sees the caller's own writes before the commit too (the overlay of issue #7378). */
  @Test
  void indexCreatedInsideTransactionAnswersBeforeTheCommit() {
    database.command("sql", "CREATE VERTEX TYPE Acc6");
    database.command("sql", "CREATE PROPERTY Acc6.behavior ARRAY_OF_FLOATS");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Acc6 SET behavior=[0.9,0.1,0.0]");
      database.command("sql", "INSERT INTO Acc6 SET behavior=[0.1,0.9,0.0]");
      database.command("sql", "CREATE INDEX ON Acc6 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");

      assertThat(neighbors("Acc6[behavior]")).hasSize(2);
    });

    assertThat(neighbors("Acc6[behavior]")).hasSize(2);
  }

  /** A record the caller deleted in the same transaction must not be indexed from its committed image. */
  @Test
  void recordDeletedInSameTransactionIsNotIndexed() {
    database.command("sql", "CREATE VERTEX TYPE Acc7");
    database.command("sql", "CREATE PROPERTY Acc7.behavior ARRAY_OF_FLOATS");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Acc7 SET id = 'keep', behavior=[0.9,0.1,0.0]");
      database.command("sql", "INSERT INTO Acc7 SET id = 'gone', behavior=[0.1,0.9,0.0]");
    });

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Acc7 WHERE id = 'gone'");
      database.command("sql", "CREATE INDEX ON Acc7 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    });

    final List<Result> rows = neighbors("Acc7[behavior]");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<String>getProperty("id")).isEqualTo("keep");
    assertThat(database.getSchema().getIndexByName("Acc7[behavior]").countEntries()).isEqualTo(1L);
  }

  /**
   * Committed records are indexed exactly once when the index is built inside a transaction holding unrelated writes.
   * Such a build shares the transaction, so its entries are still queued when {@code CREATE INDEX} returns: the eager
   * graph build the statement used to run right after it found no entry on the index pages, recovered the records
   * through its document-scan fallback, and the commit then added every one of them a second time.
   */
  @Test
  void committedRecordsAreIndexedOnceWhenTheTransactionHoldsUnrelatedWrites() {
    database.command("sql", "CREATE DOCUMENT TYPE Acc9");
    database.command("sql", "CREATE PROPERTY Acc9.behavior ARRAY_OF_FLOATS");
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Acc9 SET behavior=[0.9,0.1,0.0]");
      database.command("sql", "INSERT INTO Acc9 SET behavior=[0.1,0.9,0.0]");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Unrelated SET x = 1");
      database.command("sql", "INSERT INTO Acc9 SET behavior=[0.5,0.5,0.0]");
      database.command("sql", "CREATE INDEX ON Acc9 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    });

    assertEntries("Acc9[behavior]", 3);
    assertThat(neighbors("Acc9[behavior]")).hasSize(3);

    reopenDatabase();
    assertEntries("Acc9[behavior]", 3);
    assertThat(neighbors("Acc9[behavior]")).hasSize(3);
  }

  /** A rolled-back script leaves the index with nothing of the rolled-back records, and usable for later writes. */
  @Test
  void rolledBackTransactionLeavesNoEntries() {
    database.command("sql", "CREATE VERTEX TYPE Acc8");
    database.command("sql", "CREATE PROPERTY Acc8.behavior ARRAY_OF_FLOATS");
    database.begin();
    database.command("sql", "INSERT INTO Acc8 SET behavior=[0.9,0.1,0.0]");
    database.command("sql", "CREATE INDEX ON Acc8 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    database.rollback();

    assertThat(neighbors("Acc8[behavior]")).isEmpty();

    database.transaction(() -> database.command("sql", "INSERT INTO Acc8 SET behavior=[0.9,0.1,0.0]"));
    assertThat(neighbors("Acc8[behavior]")).hasSize(1);
  }

  /** A type with several buckets: every bucket's sub-index covers the records the transaction wrote into it. */
  @Test
  void multiBucketTypeIndexCreatedAfterInsertsInSameTransactionCoversEveryBucket() {
    final DocumentType type = database.getSchema().createDocumentType("MultiDoc", 3);
    type.createProperty("behavior", Type.ARRAY_OF_FLOATS);

    database.transaction(() -> {
      for (int i = 0; i < 9; i++)
        database.newDocument("MultiDoc").set("behavior", new float[] { 0.9f, 0.1f, i / 10f }).save();
      database.command("sql", "CREATE INDEX ON MultiDoc (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("MultiDoc[behavior]");
    assertThat(typeIndex.getIndexesOnBuckets()).hasSize(3);
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      assertThat(bucketIndex.countEntries()).as("entries of %s", bucketIndex.getName())
          .isEqualTo(database.countBucket(database.getSchema().getBucketById(bucketIndex.getAssociatedBucketId()).getName()));
    assertEntries("MultiDoc[behavior]", 9);

    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(vectorNeighbors('MultiDoc[behavior]', [0.9,0.1,0.0], 9))")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    assertThat(rows).hasSize(9);
  }

  /** Bucket-level builder: the second caller of the shared-transaction decision. */
  @Test
  void bucketIndexCreatedAfterInsertsInSameTransactionIsSearchable() {
    final DocumentType type = database.getSchema().createDocumentType("BucketDoc", 1);
    type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
    final String bucketName = type.getBuckets(false).getFirst().getName();

    final LSMVectorIndex[] index = new LSMVectorIndex[1];
    database.transaction(() -> {
      database.newDocument("BucketDoc").set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
      database.newDocument("BucketDoc").set("embedding", new float[] { 0.1f, 0.9f, 0.0f }).save();

      final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
          .buildBucketIndex("BucketDoc", bucketName, new String[] { "embedding" })
          .withType(Schema.INDEX_TYPE.LSM_VECTOR);
      builder.withDimensions(3).withSimilarity("COSINE");
      index[0] = (LSMVectorIndex) builder.create();
    });

    assertThat(index[0].countEntries()).isEqualTo(2L);
    final List<Pair<RID, Float>> found = index[0].findNeighborsFromVector(QUERY, 3);
    assertThat(found).hasSize(2);
  }

  /** Super-type propagation: the third caller of the shared-transaction decision (issue #6359). */
  @Test
  void indexPropagatedFromSuperTypeInSameTransactionCoversTheWrites() {
    database.command("sql", "CREATE DOCUMENT TYPE Parent");
    database.command("sql", "CREATE PROPERTY Parent.behavior ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Parent (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    database.command("sql", "CREATE DOCUMENT TYPE Child");
    database.command("sql", "CREATE PROPERTY Child.behavior ARRAY_OF_FLOATS");

    database.transaction(() -> {
      final MutableDocument first = database.newDocument("Child");
      first.set("behavior", new float[] { 0.9f, 0.1f, 0.0f }).save();
      database.newDocument("Child").set("behavior", new float[] { 0.1f, 0.9f, 0.0f }).save();
      database.getSchema().getType("Child").addSuperType("Parent");
    });

    assertEntries("Parent[behavior]", 2);
    assertThat(neighbors("Parent[behavior]")).hasSize(2);
  }

  /** The sparse vector family carried the same exclusion. */
  @Test
  void sparseIndexCreatedAfterInsertsInSameTransactionIsSearchable() {
    final DocumentType type = database.getSchema().createDocumentType("SparseDoc");
    type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
    type.createProperty("weights", Type.ARRAY_OF_FLOATS);

    database.transaction(() -> {
      database.newDocument("SparseDoc").set("tokens", new int[] { 3, 10 }).set("weights", new float[] { 1.0f, 0.5f }).save();
      database.newDocument("SparseDoc").set("tokens", new int[] { 10, 16 }).set("weights", new float[] { 0.5f, 0.25f }).save();

      database.getSchema().buildTypeIndex("SparseDoc", new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();
    });

    assertThat(database.getSchema().getIndexByName("SparseDoc[tokens,weights]").countEntries())
        .as("a sparse index counts postings: two records of two dimensions each").isEqualTo(4L);

    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        "SparseDoc[tokens,weights]", new int[] { 3, 10, 16 }, new float[] { 1.0f, 0.5f, 0.25f }, 10)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    assertThat(rows).hasSize(2);
  }

  /**
   * Asserted BEFORE any search: a search over an index whose graph is not built yet can rebuild an in-memory graph
   * from the bucket, which answers that one search while nothing was ever persisted in the index (a reopen finds it
   * empty again).
   */
  private void assertEntries(final String indexName, final long expected) {
    assertThat(database.getSchema().getIndexByName(indexName).countEntries()).isEqualTo(expected);
  }

  private List<Result> neighbors(final String indexName) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(vectorNeighbors('" + indexName + "', [0.9,0.1,0.0], 3))")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
