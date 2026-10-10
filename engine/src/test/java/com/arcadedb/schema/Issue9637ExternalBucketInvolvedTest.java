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
import com.arcadedb.engine.Bucket;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9637: a type's paired {@code <bucket>_ext} bucket, which holds the values of its EXTERNAL properties, was in neither
 * {@link DocumentType#getInvolvedBuckets()} nor the per-file map {@link Schema#getInvolvedTypeByBucketId(int)} answers from.
 * That map is what the server compiles a group's per-type grants into, so an unlisted {@code _ext} file id fell through to
 * default-allow; and it is what an explicit type lock collects its files from, so the {@code _ext} bucket was never locked.
 */
class Issue9637ExternalBucketInvolvedTest extends TestHelper {

  @Test
  void externalBucketOfADocumentTypeResolvesToItsType() {
    final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
    type.createProperty("blob", Type.STRING).setExternal(true);

    assertExternalBucketsInvolved("Doc");
  }

  @Test
  void externalBucketOfAVertexTypeResolvesToItsTypeAlongsideTheEdgeBuckets() {
    final VertexType type = database.getSchema().createVertexType("V", 1);
    type.createProperty("blob", Type.STRING).setExternal(true);

    assertExternalBucketsInvolved("V");
    assertThat(database.getSchema().getType("V").getInvolvedBuckets().stream().map(Bucket::getName))
        .as("the edge-list buckets stay involved").contains("V_0_out_edges", "V_0_in_edges");
  }

  @Test
  void externalBucketStaysMappedAcrossReopen() {
    final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
    type.createProperty("blob", Type.STRING).setExternal(true);

    reopenDatabase();

    assertExternalBucketsInvolved("Doc");
  }

  @Test
  void subtypeExternalBucketResolvesToTheSubtype() {
    final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
    parent.createProperty("blob", Type.STRING).setExternal(true);
    database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent").withTotalBuckets(1).create();

    assertExternalBucketsInvolved("Parent");
    assertExternalBucketsInvolved("Child");
  }

  @Test
  void reclaimedExternalBucketIsNoLongerInvolved() {
    final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
    type.createProperty("blob", Type.STRING).setExternal(true);
    final int extFileId = externalBucketIds((LocalDocumentType) type).getFirst();

    type.getProperty("blob").setExternal(false);
    ((LocalDocumentType) type).reclaimEmptyExternalBuckets();

    assertThat(database.getSchema().getType("Doc").getInvolvedBuckets().stream().map(Bucket::getFileId)).doesNotContain(extFileId);
    assertThat(database.getSchema().getInvolvedTypeByBucketId(extFileId)).isNull();
  }

  /** The explicit type lock collects the type's involved buckets: a write that reaches the paired bucket must be covered. */
  @Test
  void explicitTypeLockCoversTheExternalBucket() {
    final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
    type.createProperty("blob", Type.STRING).setExternal(true);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.acquireLock().type("Doc").lock();
      rid[0] = database.newDocument("Doc").set("blob", "x".repeat(500)).save().getIdentity();
    });

    database.transaction(() -> assertThat(rid[0].asDocument().getString("blob")).isEqualTo("x".repeat(500)));
  }

  /** Same through LOCK BUCKET on the primary bucket: the write lands in the paired bucket too. */
  @Test
  void explicitBucketLockCoversTheExternalBucket() {
    final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
    type.createProperty("blob", Type.STRING).setExternal(true);
    final String bucketName = type.getBuckets(false).getFirst().getName();

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.acquireLock().bucket(bucketName).lock();
      rid[0] = database.newDocument("Doc").set("blob", "y".repeat(500)).save().getIdentity();
    });

    database.transaction(() -> assertThat(rid[0].asDocument().getString("blob")).isEqualTo("y".repeat(500)));
  }

  /**
   * Locking a parent type locks its subtypes' primary buckets: their paired buckets, owned by the subtype, come with them.
   * The EXTERNAL property is declared on the subtype itself: an inherited one is written inline today (issue #9683).
   */
  @Test
  void explicitParentTypeLockCoversTheSubtypeExternalBucket() {
    database.getSchema().createDocumentType("Parent", 1);
    final DocumentType child = database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent").withTotalBuckets(1)
        .create();
    child.createProperty("blob", Type.STRING).setExternal(true);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.acquireLock().type("Parent").lock();
      rid[0] = database.newDocument("Child").set("blob", "z".repeat(500)).save().getIdentity();
    });

    database.transaction(() -> {
      assertThat(rid[0].asDocument().getString("blob")).isEqualTo("z".repeat(500));
      assertThat(((DatabaseInternal) database).getSerializer().findExistingExternalRids(database, rid[0].asDocument()))
          .as("the value went to the subtype's paired bucket").containsKey("blob");
    });
  }

  private void assertExternalBucketsInvolved(final String typeName) {
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
    final List<Integer> extIds = externalBucketIds(type);
    assertThat(extIds).as("type '%s' owns a paired external bucket", typeName).isNotEmpty();

    final List<Integer> involved = type.getInvolvedBuckets().stream().map(Bucket::getFileId).toList();
    for (final int extId : extIds) {
      assertThat(involved).as("the paired external bucket is one of the type's involved buckets").contains(extId);
      assertThat(database.getSchema().getInvolvedTypeByBucketId(extId)).as("the per-file map resolves the _ext bucket to its type")
          .isSameAs(type);
      // The primary-bucket map stays primary-only: ensureExternalBucketFor relies on it to refuse adopting another type's bucket.
      assertThat(database.getSchema().getTypeByBucketId(extId)).isNull();
    }
  }

  private static List<Integer> externalBucketIds(final LocalDocumentType type) {
    return type.getBuckets(false).stream().map(b -> type.getExternalBucketIdFor(b.getFileId())).filter(id -> id != null)
        .toList();
  }
}
