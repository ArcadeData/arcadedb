/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10093: a sub type of an indexed type holds sub-indexes on its own buckets, and which type they are registered under was
 * not the same on a database that had just created them and on one that loaded the schema. The creation registers each one with the
 * type that OWNS the bucket (the sub type), under the wrapper of the type that declares the index. The load registered it with the
 * type whose {@code indexes} section listed it - the declaring type - so after any restart the super type held the sub type's
 * bucket indexes and the sub type held none. Under Raft HA the leader keeps what it created and every follower loads what it is
 * sent, so the comparison that ends every HA test reported the two as configured differently, and a node's layout depended on
 * whether it had restarted since.
 * <p>
 * The tests read the registration itself ({@code bucketIndexesByBucket}), which the schema JSON does not carry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10093InheritedSubIndexOwnerTest extends TestHelper {

  /** The registration of every type: its name, then the bucket id and the sub-indexes registered under that bucket. */
  private Map<String, Map<Integer, List<String>>> registrations() {
    final Map<String, Map<Integer, List<String>>> result = new TreeMap<>();
    for (final DocumentType type : database.getSchema().getTypes()) {
      final Map<Integer, List<String>> byBucket = new TreeMap<>();
      for (final Map.Entry<Integer, List<IndexInternal>> entry : ((LocalDocumentType) type).bucketIndexesByBucket.entrySet()) {
        final List<String> names = new ArrayList<>();
        for (final IndexInternal index : entry.getValue())
          names.add(index.getName());
        byBucket.put(entry.getKey(), names);
      }
      result.put(type.getName(), byBucket);
    }
    return result;
  }

  private void createIndexedTypeWithSubType() {
    database.command("sql", "CREATE VERTEX TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.id LONG");
    database.command("sql", "CREATE INDEX ON Base (id) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Sub EXTENDS Base");
  }

  @Test
  void aSubIndexIsRegisteredWithTheTypeThatOwnsItsBucket() {
    createIndexedTypeWithSubType();

    final LocalDocumentType base = (LocalDocumentType) database.getSchema().getType("Base");
    final LocalDocumentType sub = (LocalDocumentType) database.getSchema().getType("Sub");
    assertThat(sub.getBuckets(false)).isNotEmpty();

    for (final Bucket bucket : sub.getBuckets(false)) {
      assertThat(sub.bucketIndexesByBucket).as("Sub owns bucket %s", bucket.getName()).containsKey(bucket.getFileId());
      assertThat(base.bucketIndexesByBucket).as("Base does not own bucket %s", bucket.getName())
          .doesNotContainKey(bucket.getFileId());
    }
  }

  @Test
  void theRegistrationIsTheSameAfterTheSchemaIsLoaded() {
    createIndexedTypeWithSubType();
    final Map<String, Map<Integer, List<String>>> created = registrations();

    reopenDatabase();

    assertThat(registrations()).as("what the database registered when it created the sub-indexes, against what it loads")
        .isEqualTo(created);
  }

  @Test
  void theWrapperOfTheDeclaringTypeStillCoversEverySubIndex() {
    createIndexedTypeWithSubType();
    reopenDatabase();

    final TypeIndex wrapper = database.getSchema().getType("Base").getIndexByProperties("id");
    assertThat(wrapper).isNotNull();
    final List<Integer> covered = new ArrayList<>();
    for (final Index index : wrapper.getIndexesOnBuckets())
      covered.add(index.getAssociatedBucketId());
    assertThat(covered).containsExactlyInAnyOrderElementsOf(database.getSchema().getType("Base").getBucketIds(true));

    // The sub type has no wrapper of its own: its sub-indexes belong to the one of Base
    assertThat(database.getSchema().getType("Sub").getIndexByProperties("id")).isNull();
    for (final IndexInternal index : wrapper.getIndexesOnBuckets())
      assertThat(index.getTypeIndex()).isSameAs(wrapper);
  }

  @Test
  void aRecordOfTheSubTypeIsIndexedAfterTheSchemaIsLoaded() {
    createIndexedTypeWithSubType();
    reopenDatabase();

    database.transaction(() -> database.newVertex("Sub").set("id", 7L).save());

    assertThat(database.query("sql", "SELECT FROM Base WHERE id = 7").stream().count()).isEqualTo(1L);
    assertThat(database.getSchema().getType("Base").getIndexByProperties("id").countEntries()).isEqualTo(1L);
  }
}
