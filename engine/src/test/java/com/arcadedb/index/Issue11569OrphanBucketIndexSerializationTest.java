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
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11569: a bucket index whose bucket association was lost ({@code associatedBucketId == -1}: corrupt, orphaned or
 * imported metadata) made {@code toJSON()} call {@code getBucketById(-1)}, so serializing its type - and with it every schema
 * save, whatever the change being saved - raised "Bucket with id '-1' was not found". The LSM tree had already been fixed;
 * the hash, vector, sparse-vector, full-text and geospatial indexes had not.
 * <p>
 * A census over {@link Schema.INDEX_TYPE}, the authority that defines the index kinds: every kind is created, orphaned and
 * serialized, and a kind this test does not know how to create fails it, so the next index kind cannot ship without the
 * guarantee.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11569OrphanBucketIndexSerializationTest extends TestHelper {

  @Test
  void everyIndexKindSerializesAnOrphanedBucketIndex() {
    final Set<Schema.INDEX_TYPE> reached = EnumSet.noneOf(Schema.INDEX_TYPE.class);

    for (final Schema.INDEX_TYPE kind : Schema.INDEX_TYPE.values()) {
      final String typeName = "Orphan" + kind.name().replace("_", "");
      createIndexOfKind(kind, typeName);

      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
      final List<TypeIndex> typeIndexes = new ArrayList<>(type.getAllIndexes(false));
      assertThat(typeIndexes).as("one index of kind %s on %s", kind, typeName).hasSize(1);
      final TypeIndex typeIndex = typeIndexes.getFirst();
      assertThat(typeIndex.getType()).isEqualTo(kind);

      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
        final String expectedBucket = database.getSchema().getBucketById(bucketIndex.getAssociatedBucketId()).getName();
        final int associatedBucketId = bucketIndex.getMetadata().associatedBucketId;
        bucketIndex.getMetadata().associatedBucketId = -1;
        try {
          final JSONObject json = bucketIndex.toJSON();
          // recovered from the <bucketName>_<uniqueId> name of the index file, so the loader can relink the orphan
          assertThat(json.getString("bucket")).as("bucket of the orphaned %s index %s", kind, bucketIndex.getName())
              .isEqualTo(expectedBucket);
          assertThat(type.toJSON().getJSONObject("indexes").length()).as("the type of the orphaned %s index serializes", kind)
              .isEqualTo(typeIndex.getIndexesOnBuckets().length);
          assertThat(database.getSchema().getEmbedded().toJSON().getJSONObject("types").has(typeName))
              .as("the schema holding the orphaned %s index serializes", kind).isTrue();
        } finally {
          // put back, or the CHECK DATABASE run after every test reports the type index as lacking the index of that bucket
          bucketIndex.getMetadata().associatedBucketId = associatedBucketId;
        }
      }
      reached.add(kind);
    }

    assertThat(reached).as("the census reaches every Schema.INDEX_TYPE").containsExactlyInAnyOrder(Schema.INDEX_TYPE.values());
  }

  @Test
  void anOrphanWhoseNameNamesNoBucketSerializesWithAnEmptyBucket() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    final Schema schema = database.getSchema();
    assertThat(IndexInternal.bucketNameForJSON(schema, -1, "NoSuchBucket_123")).isEmpty();
    assertThat(IndexInternal.bucketNameForJSON(schema, -1, "noUnderscore")).isEmpty();
    assertThat(IndexInternal.bucketNameForJSON(schema, -1, null)).isEmpty();
    // a stale id that names no bucket falls back to the name
    final String docBucket = schema.getType("Doc").getBuckets(false).getFirst().getName();
    assertThat(IndexInternal.bucketNameForJSON(schema, Integer.MAX_VALUE, docBucket + "_42")).isEqualTo(docBucket);
    assertThat(IndexInternal.bucketNameForJSON(schema, schema.getBucketByName(docBucket).getFileId(), "whatever"))
        .isEqualTo(docBucket);
  }

  private void createIndexOfKind(final Schema.INDEX_TYPE kind, final String typeName) {
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName + " BUCKETS 2");
    final String create = switch (kind) {
      case LSM_TREE -> {
        database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
        yield "CREATE INDEX ON " + typeName + " (p) NOTUNIQUE";
      }
      case HASH -> {
        database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
        yield "CREATE INDEX ON " + typeName + " (p) NOTUNIQUE_HASH";
      }
      case FULL_TEXT -> {
        database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
        yield "CREATE INDEX ON " + typeName + " (p) FULL_TEXT";
      }
      case GEOSPATIAL -> {
        database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
        yield "CREATE INDEX ON " + typeName + " (p) GEOSPATIAL";
      }
      case COLUMNAR -> {
        database.command("sql", "CREATE PROPERTY " + typeName + ".p STRING");
        yield "CREATE INDEX ON " + typeName + " (p) COLUMNAR";
      }
      case LSM_VECTOR -> {
        database.command("sql", "CREATE PROPERTY " + typeName + ".p ARRAY_OF_FLOATS");
        yield "CREATE INDEX ON " + typeName + " (p) LSM_VECTOR METADATA { dimensions: 4, similarity: 'COSINE' }";
      }
      case LSM_SPARSE_VECTOR -> {
        database.command("sql", "CREATE PROPERTY " + typeName + ".tokens ARRAY_OF_INTEGERS");
        database.command("sql", "CREATE PROPERTY " + typeName + ".weights ARRAY_OF_FLOATS");
        yield "CREATE INDEX ON " + typeName + " (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }";
      }
      // no default: a new Schema.INDEX_TYPE constant fails to compile here, which is the point of the census
    };
    database.command("sql", create);
  }
}
