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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for {@code REBUILD INDEX *} against a bucket sub-index whose schema entry lost its bucket
 * association (metadata {@code associatedBucketId == -1}). This happens when a sub-index stays registered in the
 * schema {@code indexMap} but the loader could not re-link it to a bucket. Before the fix the rebuild died on
 * {@code getBucketById(-1)} ("Bucket with id '-1' was not found"), which also aborted the whole {@code *} sweep so no
 * healthy index was rebuilt. The rebuild now self-heals the association from the {@code <bucketName>_<uniqueId>}
 * naming convention, and the {@code *} sweep is resilient to a single failing index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildIndexOrphanBucketTest extends TestHelper {
  private static final String TYPE_NAME = "User";

  @Test
  void toJSONOfOrphanedSubIndexDoesNotCrashAndKeepsRelinkableBucketName() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(2).create();
      type.createProperty("name", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "name" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("name").getFirst();
    final LSMTreeIndex subIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    // the bucket name the loader uses to relink is the <bucketName> prefix of the sub-index name
    final String expectedBucketName = subIndex.getName().substring(0, subIndex.getName().lastIndexOf('_'));

    // Break the association: toJSON() used to call getBucketById(-1) here and crash the whole schema save.
    subIndex.getMetadata().associatedBucketId = -1;

    final JSONObject[] json = new JSONObject[1];
    assertThatCode(() -> json[0] = subIndex.toJSON()).as("serializing an orphaned sub-index must not throw").doesNotThrowAnyException();
    // and it must still emit the bucket name (recovered from the naming convention) so the loader can relink it
    assertThat(json[0].getString("bucket")).isEqualTo(expectedBucketName);
  }

  @Test
  void rebuildAllSelfHealsBucketIndexWithLostBucketAssociation() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(2).create();
      type.createProperty("name", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "name" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument(TYPE_NAME).set("name", "u" + i).save();
    });

    // Corrupt one bucket sub-index: drop its bucket association, reproducing the orphaned-metadata state that
    // makes getAssociatedBucketId() return -1 at rebuild time.
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("name").getFirst();
    final IndexInternal subIndex = typeIndex.getIndexesOnBuckets()[0];
    final String orphanName = subIndex.getName();
    subIndex.getMetadata().associatedBucketId = -1;
    assertThat(subIndex.getAssociatedBucketId()).as("precondition: sub-index reports no associated bucket").isEqualTo(-1);

    // REBUILD INDEX * must not die on getBucketById(-1); it re-links the sub-index from its name and rebuilds.
    try (final ResultSet rs = database.command("sql", "REBUILD INDEX *")) {
      final Result r = rs.next();
      final List<String> rebuilt = r.getProperty("indexes");
      assertThat(rebuilt).as("the orphaned sub-index must be rebuilt").contains(orphanName);
      assertThat(r.<List<String>>getProperty("failedIndexes")).as("no index should fail").isNull();
    }

    // The association is restored on every bucket sub-index...
    final TypeIndex rebuiltTypeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("name").getFirst();
    for (final Index bucketIndex : rebuiltTypeIndex.getIndexesOnBuckets())
      assertThat(bucketIndex.getAssociatedBucketId()).as("bucket association must be valid after rebuild").isGreaterThanOrEqualTo(0);

    // ...and the index still resolves every record.
    for (int i = 0; i < 50; i++) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE name = 'u" + i + "'")) {
        assertThat(rs.hasNext()).as("record u" + i + " must be findable through the rebuilt index").isTrue();
      }
    }
  }
}
