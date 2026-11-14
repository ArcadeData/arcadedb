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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11577: the persisted definition of an index is written per bucket, and the one of a bucket that a
 * subtype owns names the subtype ({@code "typeName": "E"} for an index on {@code P} with {@code E extends P}).
 * {@code withPersistedMetadata} took that type name over the builder's own, so restoring such an entry, as the JSONL
 * importer does for every entry of the parent type, created a second LSM_VECTOR index on the subtype.
 */
class PersistedIndexDefinitionOfSubtypeBucketIssue11577Test extends TestHelper {

  @Test
  void restoringTheEntryOfASubtypeBucketBuildsTheIndexOnTheBuilderType() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE E EXTENDS P");
    database.command("sql", "CREATE PROPERTY P.vec ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON P (vec) LSM_VECTOR METADATA {dimensions: 3, similarity: 'EUCLIDEAN'}");

    JSONObject subtypeEntry = null;
    for (final IndexInternal onBucket : ((TypeIndex) database.getSchema().getIndexByName("P[vec]")).getIndexesOnBuckets()) {
      final JSONObject entry = onBucket.toJSON();
      if ("E".equals(entry.getString("typeName")))
        subtypeEntry = entry;
    }
    assertThat(subtypeEntry).as("the entry of the bucket E owns names E").isNotNull();

    database.command("sql", "DROP INDEX `P[vec]`");

    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("P", new String[] { "vec" })
        .withType(Schema.INDEX_TYPE.LSM_VECTOR).withLSMVectorType();
    builder.withPersistedMetadata(subtypeEntry);
    builder.withIgnoreIfExists(true);
    final TypeIndex created = builder.create();

    assertThat(created.getName()).isEqualTo("P[vec]");
    assertThat(created.getTypeName()).isEqualTo("P");
    assertThat(created.getPropertyNames()).containsExactly("vec");
    assertThat(vectorIndexNames()).containsExactly("P[vec]");
  }

  private List<String> vectorIndexNames() {
    final List<String> names = new ArrayList<>();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof TypeIndex && index.getType() == Schema.INDEX_TYPE.LSM_VECTOR)
        names.add(index.getName());
    return names;
  }
}
