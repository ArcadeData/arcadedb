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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10450: since #10438 an index build joined the caller's transaction whenever that
 * transaction held ANY pending write, so a {@code CREATE INDEX} over a large committed bucket, in a script that had also
 * written something unrelated, queued one entry per record in that transaction until the commit instead of taking the
 * chunked build of its own transaction. A build now joins the caller's transaction only when that transaction has
 * written to a bucket the index covers.
 * <p>
 * The observable difference is what a ROLLBACK of the caller leaves behind: a build that ran in a transaction of its own
 * has committed its entries, one that joined the caller's transaction loses them with it. One test per entry point that
 * makes the decision (type-level builder, bucket-level builder, super-type propagation), plus the cases that must still
 * share.
 */
class Issue10450IndexBuildWithUnrelatedPendingWritesTest extends TestHelper {

  /** SQL {@code CREATE INDEX ... LSM_VECTOR}: the case the issue names. */
  @Test
  void vectorIndexBuildDoesNotJoinATransactionThatWroteOnlyElsewhere() {
    createCommittedVectorRecords("Vec", 3);
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");

    database.begin();
    database.command("sql", "INSERT INTO Unrelated SET x = 1");
    database.command("sql", "CREATE INDEX ON Vec (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    database.rollback();

    assertThat(database.getSchema().getIndexByName("Vec[behavior]").countEntries()).isEqualTo(3L);
    assertThat(neighbors("Vec[behavior]")).hasSize(3);
    assertThat(database.countType("Unrelated", false)).isZero();

    reopenDatabase();
    assertThat(database.getSchema().getIndexByName("Vec[behavior]").countEntries()).isEqualTo(3L);
    assertThat(neighbors("Vec[behavior]")).hasSize(3);
  }

  /** The Java type-level builder reports where it built. */
  @Test
  void typeBuilderReportsItDidNotBuildInTheCallerTransaction() {
    database.command("sql", "CREATE DOCUMENT TYPE KeyedApi");
    database.command("sql", "CREATE PROPERTY KeyedApi.id INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated2");
    database.transaction(() -> {
      database.newDocument("KeyedApi").set("id", 1).save();
      database.newDocument("KeyedApi").set("id", 2).save();
    });

    final TypeIndexBuilder[] builder = new TypeIndexBuilder[1];
    database.transaction(() -> {
      database.newDocument("Unrelated2").set("x", 1).save();
      builder[0] = database.getSchema().buildTypeIndex("KeyedApi", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE);
      builder[0].create();
    });

    assertThat(builder[0].isBuiltInCallerTransaction()).isFalse();
    assertThat(database.getSchema().getIndexByName("KeyedApi[id]").countEntries()).isEqualTo(2L);
  }

  /** ... and a write to the indexed type still makes it join the caller's transaction. */
  @Test
  void typeBuilderReportsItBuiltInTheCallerTransactionAfterAWriteToTheIndexedType() {
    database.command("sql", "CREATE DOCUMENT TYPE KeyedApi2");
    database.command("sql", "CREATE PROPERTY KeyedApi2.id INTEGER");

    final TypeIndexBuilder[] builder = new TypeIndexBuilder[1];
    database.transaction(() -> {
      database.newDocument("KeyedApi2").set("id", 1).save();
      builder[0] = database.getSchema().buildTypeIndex("KeyedApi2", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE);
      builder[0].create();
    });

    assertThat(builder[0].isBuiltInCallerTransaction()).isTrue();
    assertThat(database.getSchema().getIndexByName("KeyedApi2[id]").countEntries()).isEqualTo(1L);
  }

  /** Every family follows the same rule: an LSM_TREE index is built in its own transaction too. */
  @Test
  void lsmTreeIndexBuildDoesNotJoinATransactionThatWroteOnlyElsewhere() {
    database.command("sql", "CREATE DOCUMENT TYPE Keyed");
    database.command("sql", "CREATE PROPERTY Keyed.id INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated3");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Keyed").set("id", i).save();
    });

    database.begin();
    database.command("sql", "INSERT INTO Unrelated3 SET x = 1");
    database.command("sql", "CREATE INDEX ON Keyed (id) UNIQUE");
    database.rollback();

    assertThat(database.getSchema().getIndexByName("Keyed[id]").countEntries()).isEqualTo(10L);
    try (final ResultSet rs = database.query("sql", "SELECT FROM Keyed WHERE id = 7")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  /** A write to ANY bucket the index covers - here a subtype's, reached polymorphically - still joins the transaction. */
  @Test
  void writeToASubtypeBucketStillJoinsTheTransaction() {
    createCommittedVectorRecords("VecParent", 2);
    database.command("sql", "CREATE DOCUMENT TYPE VecChild EXTENDS VecParent");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO VecChild SET behavior=[0.8,0.2,0.0]");
      database.command("sql",
          "CREATE INDEX ON VecParent (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    });

    assertThat(database.getSchema().getIndexByName("VecParent[behavior]").countEntries()).isEqualTo(3L);
    assertThat(neighbors("VecParent[behavior]")).hasSize(3);
  }

  /**
   * A pending update of a committed record in the indexed type still joins the transaction: the entries of the pending
   * versions are queued on it, and the rollback undoes them. The committed records are indexed on their own (issue #10490),
   * so the index still describes them afterwards.
   */
  @Test
  void updateInTheIndexedTypeStillJoinsTheTransaction() {
    createCommittedVectorRecords("VecUpd", 2);

    database.begin();
    database.command("sql", "UPDATE VecUpd SET behavior = [0.1,0.9,0.0]");
    database.command("sql", "CREATE INDEX ON VecUpd (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    database.rollback();

    assertThat(database.getSchema().getIndexByName("VecUpd[behavior]").countEntries())
        .as("the committed records are indexed whatever the caller does").isEqualTo(2L);
    assertThat(neighbors("VecUpd[behavior]")).hasSize(2);
  }

  /** Bucket-level builder: a write to ANOTHER bucket of the same type is unrelated to this bucket's index. */
  @Test
  void bucketIndexBuildDoesNotJoinATransactionThatWroteToAnotherBucket() {
    final DocumentType type = database.getSchema().createDocumentType("BucketVec", 2);
    type.createProperty("behavior", Type.ARRAY_OF_FLOATS);
    final String indexed = type.getBuckets(false).getFirst().getName();
    final String other = type.getBuckets(false).get(1).getName();
    database.transaction(() -> {
      database.newDocument("BucketVec").set("behavior", new float[] { 0.9f, 0.1f, 0.0f }).save(indexed);
      database.newDocument("BucketVec").set("behavior", new float[] { 0.1f, 0.9f, 0.0f }).save(indexed);
    });

    final Index[] index = new Index[1];
    database.begin();
    database.newDocument("BucketVec").set("behavior", new float[] { 0.5f, 0.5f, 0.0f }).save(other);
    final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
        .buildBucketIndex("BucketVec", indexed, new String[] { "behavior" })
        .withType(Schema.INDEX_TYPE.LSM_VECTOR);
    builder.withDimensions(3).withSimilarity("COSINE");
    index[0] = builder.create();
    database.rollback();

    assertThat(index[0].countEntries()).isEqualTo(2L);
  }

  /** Bucket-level builder: a write to the indexed bucket itself still joins the transaction. */
  @Test
  void bucketIndexBuildJoinsATransactionThatWroteToItsBucket() {
    final DocumentType type = database.getSchema().createDocumentType("BucketVec2", 1);
    type.createProperty("behavior", Type.ARRAY_OF_FLOATS);
    final String indexed = type.getBuckets(false).getFirst().getName();

    final Index[] index = new Index[1];
    database.transaction(() -> {
      database.newDocument("BucketVec2").set("behavior", new float[] { 0.9f, 0.1f, 0.0f }).save();
      final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
          .buildBucketIndex("BucketVec2", indexed, new String[] { "behavior" })
          .withType(Schema.INDEX_TYPE.LSM_VECTOR);
      builder.withDimensions(3).withSimilarity("COSINE");
      index[0] = builder.create();
    });

    assertThat(index[0].countEntries()).isEqualTo(1L);
  }

  /** Super-type propagation: the subtype's buckets are what is indexed, so a write elsewhere does not join. */
  @Test
  void propagatedIndexBuildDoesNotJoinATransactionThatWroteOnlyElsewhere() {
    database.command("sql", "CREATE DOCUMENT TYPE PParent");
    database.command("sql", "CREATE PROPERTY PParent.behavior ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON PParent (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
    createCommittedVectorRecords("PChild", 2);
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated4");

    database.begin();
    database.command("sql", "INSERT INTO Unrelated4 SET x = 1");
    database.getSchema().getType("PChild").addSuperType("PParent");
    database.rollback();

    assertThat(database.getSchema().getIndexByName("PParent[behavior]").countEntries()).isEqualTo(2L);
    assertThat(neighbors("PParent[behavior]")).hasSize(2);
  }

  /** The transaction-side half of the decision, asked directly: which files a transaction has written to. */
  @Test
  void transactionReportsTheFilesItWroteTo() {
    final DocumentType written = database.getSchema().createDocumentType("WrittenTo", 1);
    final DocumentType updated = database.getSchema().createDocumentType("UpdatedOnly", 1);
    final DocumentType untouched = database.getSchema().createDocumentType("Untouched", 1);
    final int writtenBucket = written.getBuckets(false).getFirst().getFileId();
    final int updatedBucket = updated.getBuckets(false).getFirst().getFileId();
    final int untouchedBucket = untouched.getBuckets(false).getFirst().getFileId();

    final RID[] committed = new RID[1];
    database.transaction(() -> committed[0] = database.newDocument("UpdatedOnly").set("x", 1).save().getIdentity());

    database.transaction(() -> {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      assertThat(tx.hasChangesInFiles(setOf(writtenBucket, updatedBucket, untouchedBucket))).isFalse();

      database.newDocument("WrittenTo").set("x", 1).save();
      assertThat(tx.hasChangesInFiles(setOf(writtenBucket))).isTrue();
      assertThat(tx.hasChangesInFiles(setOf(updatedBucket, untouchedBucket))).isFalse();

      database.lookupByRID(committed[0], true).asDocument().modify().set("x", 2).save();
      assertThat(tx.hasChangesInFiles(setOf(updatedBucket))).isTrue();
      assertThat(tx.hasChangesInFiles(setOf(untouchedBucket))).isFalse();
      assertThat(tx.hasChangesInFiles(new IntHashSet())).isFalse();
    });
  }

  private static IntHashSet setOf(final int... values) {
    final IntHashSet set = new IntHashSet();
    for (final int value : values)
      set.add(value);
    return set;
  }

  private void createCommittedVectorRecords(final String typeName, final int count) {
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    database.command("sql", "CREATE PROPERTY " + typeName + ".behavior ARRAY_OF_FLOATS");
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument(typeName).set("behavior", new float[] { 0.9f, 0.1f, i / 10f }).save();
    });
  }

  private List<Result> neighbors(final String indexName) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(vectorNeighbors('" + indexName + "', [0.9,0.1,0.0], 10))")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
