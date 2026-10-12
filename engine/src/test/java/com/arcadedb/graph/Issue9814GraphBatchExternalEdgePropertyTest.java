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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9814: {@code GraphBatch.createEdgeRecordsBulk} serialized the edge records of a single-bucket flush with its own
 * template serializer and wrote them straight to the bucket, a path that never reaches {@code BinarySerializer} and so never
 * honors {@code Property.isExternal()}: an EXTERNAL edge property written through {@code GraphBatch} was stored INLINE in the
 * edge record, silently breaking the contract (small primary records, large values in the paired bucket). Reads worked, so
 * nothing errored; only a {@code REBUILD TYPE} moved the values out afterwards.
 * <p>
 * Resolving it for an INHERITED EXTERNAL property surfaced the same blind spot in the ordinary write path: the serializer looked
 * the property up in the record's own type only, so a subtype's record kept the value inline although the subtype owns a paired
 * bucket for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9814GraphBatchExternalEdgePropertyTest extends TestHelper {

  private static final String BLOB = "q".repeat(500);

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType v = database.getSchema().createVertexType("V9814", 1);
      v.createProperty("id", Type.INTEGER);
    });
  }

  private RID[] vertices(final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids[i] = database.newVertex("V9814").set("id", i).save().getIdentity();
    });
    return rids;
  }

  /** Records held by the paired external buckets of {@code typeName}. */
  private long externalRecords(final String typeName) {
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
    long total = 0;
    for (final Bucket bucket : type.getBuckets(false)) {
      final Integer externalBucketId = type.getExternalBucketIdFor(bucket.getFileId());
      if (externalBucketId != null)
        total += database.getSchema().getBucketById(externalBucketId).count();
    }
    return total;
  }

  private EdgeType edgeTypeWithExternalBlob(final String name) {
    final EdgeType type = database.getSchema().createEdgeType(name, 1);
    type.createProperty("blob", Type.STRING).setExternal(true);
    return type;
  }

  private String blobOfFirstEdge(final RID from, final String edgeType) {
    final String[] blob = new String[1];
    database.transaction(
        () -> blob[0] = from.asVertex().getEdges(Vertex.DIRECTION.OUT, edgeType).iterator().next().getString("blob"));
    return blob[0];
  }

  @Test
  void externalPropertyDeclaredOnTheEdgeTypeIsStoredInThePairedBucket() {
    edgeTypeWithExternalBlob("E9814");
    final RID[] v = vertices(2);

    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(v[0], "E9814", v[1], "blob", BLOB);
    }

    assertThat(externalRecords("E9814")).as("the value must live in the paired external bucket").isEqualTo(1);
    assertThat(blobOfFirstEdge(v[0], "E9814")).isEqualTo(BLOB);
  }

  @Test
  void theBatchStoresTheEdgeExactlyAsTheOrdinaryApiDoes() {
    edgeTypeWithExternalBlob("EBatch9814");
    edgeTypeWithExternalBlob("EApi9814");
    final RID[] v = vertices(2);

    try (final GraphBatch batch = database.batch().build()) {
      for (int i = 0; i < 5; i++)
        batch.newEdge(v[0], "EBatch9814", v[1], "blob", BLOB + i);
    }
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        v[0].asVertex().modify().newEdge("EApi9814", v[1], "blob", BLOB + i).save();
    });

    assertThat(externalRecords("EApi9814")).isEqualTo(5);
    assertThat(externalRecords("EBatch9814")).isEqualTo(externalRecords("EApi9814"));
  }

  @Test
  void externalPropertyInheritedFromASuperTypeIsExternalizedToo() {
    edgeTypeWithExternalBlob("EParent9814");
    database.getSchema().buildEdgeType().withName("EChild9814").withSuperType("EParent9814").withTotalBuckets(1).create();
    final RID[] v = vertices(2);

    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(v[0], "EChild9814", v[1], "blob", BLOB);
    }

    assertThat(externalRecords("EChild9814")).isEqualTo(1);
    assertThat(blobOfFirstEdge(v[0], "EChild9814")).isEqualTo(BLOB);
  }

  /**
   * The ordinary write path had the same blind spot: the serializer looked the property up in the record's own type only, so a
   * value of an EXTERNAL property inherited from a super type was stored INLINE although the subtype owns a paired bucket for it.
   */
  @Test
  void ordinarySaveExternalizesAPropertyInheritedFromASuperType() {
    database.getSchema().createDocumentType("DParent9814", 1).createProperty("blob", Type.STRING).setExternal(true);
    database.getSchema().buildDocumentType().withName("DChild9814").withSuperType("DParent9814").withTotalBuckets(1).create();
    edgeTypeWithExternalBlob("EParentApi9814");
    database.getSchema().buildEdgeType().withName("EChildApi9814").withSuperType("EParentApi9814").withTotalBuckets(1).create();
    final RID[] v = vertices(2);

    database.transaction(() -> {
      database.newDocument("DChild9814").set("blob", BLOB).save();
      v[0].asVertex().modify().newEdge("EChildApi9814", v[1], "blob", BLOB).save();
    });

    assertThat(externalRecords("DChild9814")).isEqualTo(1);
    assertThat(externalRecords("EChildApi9814")).isEqualTo(1);
    assertThat(database.query("sql", "SELECT blob FROM DChild9814").next().<String>getProperty("blob")).isEqualTo(BLOB);
  }

  /** Records written inline before the property turned EXTERNAL keep reading, and move out of the record when they are updated. */
  @Test
  void anOldInlineRecordOfASubtypeStillReadsAndMovesOutWhenUpdated() {
    final DocumentType parent = database.getSchema().createDocumentType("DParentOld9814", 1);
    parent.createProperty("blob", Type.STRING);
    database.getSchema().buildDocumentType().withName("DChildOld9814").withSuperType("DParentOld9814").withTotalBuckets(1).create();
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument("DChildOld9814").set("blob", BLOB + "0").save().getIdentity();
      rids[1] = database.newDocument("DChildOld9814").set("blob", BLOB + "1").save().getIdentity();
    });

    parent.getProperty("blob").setExternal(true);
    assertThat(externalRecords("DChildOld9814")).as("both records are still inline").isZero();

    database.transaction(() -> {
      final MutableDocument updated = rids[0].asDocument().modify();
      updated.set("blob", BLOB + "updated");
      updated.save();
    });

    assertThat(externalRecords("DChildOld9814")).as("only the updated record moved out").isEqualTo(1);
    database.transaction(() -> {
      assertThat(rids[0].asDocument().getString("blob")).isEqualTo(BLOB + "updated");
      assertThat(rids[1].asDocument().getString("blob")).isEqualTo(BLOB + "1");
    });
  }

  @Test
  void externalAndInlinePropertiesOfOneEdgeAreSplitCorrectly() {
    final EdgeType type = edgeTypeWithExternalBlob("EMixed9814");
    type.createProperty("weight", Type.INTEGER);
    final RID[] v = vertices(2);

    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(v[0], "EMixed9814", v[1], "blob", BLOB, "weight", 7, "undeclared", "kept");
    }

    assertThat(externalRecords("EMixed9814")).isEqualTo(1);
    database.transaction(() -> {
      final Edge edge = v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "EMixed9814").iterator().next();
      assertThat(edge.getString("blob")).isEqualTo(BLOB);
      assertThat(edge.getInteger("weight")).isEqualTo(7);
      assertThat(edge.getString("undeclared")).isEqualTo("kept");
    });
  }

  @Test
  void aUniqueIndexOnTheEdgeStillHoldsWhenTheFlushTakesTheOrdinaryPath() {
    final EdgeType type = edgeTypeWithExternalBlob("EIndexed9814");
    type.createProperty("key", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "EIndexed9814", "key");
    final RID[] v = vertices(3);

    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(v[0], "EIndexed9814", v[1], "blob", BLOB, "key", 1);
      batch.newEdge(v[0], "EIndexed9814", v[2], "blob", BLOB, "key", 2);
    }

    assertThat(externalRecords("EIndexed9814")).isEqualTo(2);
    assertThat(database.query("sql", "SELECT FROM EIndexed9814 WHERE key = 2").stream().count()).isEqualTo(1);
  }

  @Test
  void aTypeWithoutExternalPropertiesKeepsTheBulkPathAndPairsNoBucket() {
    database.getSchema().createEdgeType("EPlain9814", 1).createProperty("blob", Type.STRING);
    final RID[] v = vertices(2);

    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(v[0], "EPlain9814", v[1], "blob", BLOB);
    }

    assertThat(externalRecords("EPlain9814")).isZero();
    assertThat(blobOfFirstEdge(v[0], "EPlain9814")).isEqualTo(BLOB);
  }

  @Test
  void externalValuesSurviveAReopen() {
    edgeTypeWithExternalBlob("EReopen9814");
    final RID[] v = vertices(2);

    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(v[0], "EReopen9814", v[1], "blob", BLOB);
    }
    reopenDatabase();

    assertThat(externalRecords("EReopen9814")).isEqualTo(1);
    assertThat(database.query("sql", "SELECT blob FROM EReopen9814").next().<String>getProperty("blob")).isEqualTo(BLOB);
  }
}
