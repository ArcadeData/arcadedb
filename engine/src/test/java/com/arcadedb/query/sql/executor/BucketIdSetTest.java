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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The contract of {@link BucketIdSet} (issue #12758): held by bucket file id, so a rename of the type cannot take a bucket out of
 * it, and answering by name only as a view of the names the buckets have now.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BucketIdSetTest extends TestHelper {

  @Test
  void aBucketStaysInTheSetWhenTheTypeIsRenamed() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("B12758").withTotalBuckets(3).create();
    final List<Bucket> buckets = type.getBuckets(false);
    final BucketIdSet set = new BucketIdSet(database.getSchema(), idsOf(buckets, true));
    final String oldName = buckets.getFirst().getName();

    // REPETITIONS COLLAPSE
    assertThat(set).hasSize(3);
    assertThat(set.contains(oldName)).isTrue();

    database.command("sql", "ALTER TYPE B12758 NAME B12758r");

    final String newName = buckets.getFirst().getName();
    assertThat(newName).isNotEqualTo(oldName);
    for (final Bucket bucket : buckets) {
      assertThat(BucketIdSet.includes(set, bucket)).isTrue();
      assertThat(set.containsId(bucket.getFileId())).isTrue();
    }
    // THE VIEW BY NAME FOLLOWS THE NAMES THE BUCKETS HAVE NOW
    assertThat(set.contains(newName)).isTrue();
    assertThat(set.contains(oldName)).isFalse();
    assertThat(new ArrayList<>(set)).containsExactlyInAnyOrderElementsOf(buckets.stream().map(Bucket::getName).toList());
    assertThat(BucketIdSet.idsOf(set, database.getSchema())).containsExactlyInAnyOrderElementsOf(
        buckets.stream().map(Bucket::getFileId).toList());
  }

  @Test
  void aDroppedBucketIsNoLongerCountedNorListed() {
    final Schema schema = database.getSchema();
    final Bucket kept = schema.createBucket("kept12758");
    final Bucket dropped = schema.createBucket("dropped12758");
    final BucketIdSet set = new BucketIdSet(schema, new int[] { kept.getFileId(), dropped.getFileId() });
    assertThat(set).hasSize(2);

    schema.dropBucket("dropped12758");

    assertThat(set).hasSize(1);
    assertThat(set).containsExactly("kept12758");
    assertThat(set.contains("dropped12758")).isFalse();
    assertThat(set.isEmpty()).isFalse();
  }

  @Test
  void filterKeepsTheIdsThatPass() {
    final Schema schema = database.getSchema();
    final Bucket a = schema.createBucket("a12758");
    final Bucket b = schema.createBucket("b12758");
    final BucketIdSet set = new BucketIdSet(schema, new int[] { a.getFileId(), b.getFileId() });

    final BucketIdSet onlyA = set.filter(id -> id == a.getFileId());
    assertThat(onlyA.containsId(a.getFileId())).isTrue();
    assertThat(onlyA.containsId(b.getFileId())).isFalse();
    assertThat(onlyA.idList()).containsExactly(a.getFileId());
    assertThat(set.filter(id -> false)).isEmpty();
  }

  @Test
  void includesAndContainsOnOtherShapes() {
    final Schema schema = database.getSchema();
    final Bucket bucket = schema.createBucket("c12758");
    final BucketIdSet set = new BucketIdSet(schema, new int[] { bucket.getFileId() });

    // NULL STANDS FOR EVERY BUCKET, AS DOES "*" IN A SET OF NAMES; A BUCKET ID SET HAS NO WILDCARD
    assertThat(BucketIdSet.includes(null, bucket)).isTrue();
    assertThat(BucketIdSet.includes(Set.of("*"), bucket)).isTrue();
    assertThat(BucketIdSet.includes(Set.of("c12758"), bucket)).isTrue();
    assertThat(BucketIdSet.includes(Set.of("other"), bucket)).isFalse();
    assertThat(set.contains("*")).isFalse();
    assertThat(set.contains("unknown12758")).isFalse();
    assertThat(set.contains((Object) bucket.getFileId())).isFalse();
    assertThat(BucketIdSet.idsOf(Set.of("c12758"), schema)).containsExactly(bucket.getFileId());

    // READ-ONLY: THE PLANNER HANDS IT AROUND AS THE BUCKETS OF THE TARGET
    assertThatThrownBy(() -> set.add("x")).isInstanceOf(UnsupportedOperationException.class);
  }

  private static int[] idsOf(final List<Bucket> buckets, final boolean twice) {
    final int[] ids = new int[twice ? buckets.size() * 2 : buckets.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = buckets.get(i % buckets.size()).getFileId();
    return ids;
  }
}
