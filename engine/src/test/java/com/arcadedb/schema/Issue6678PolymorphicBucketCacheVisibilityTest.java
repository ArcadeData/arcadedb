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
import com.arcadedb.engine.Bucket;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6678: {@code LocalDocumentType.cachedPolymorphicBuckets}/{@code cachedPolymorphicBucketIds} are published
 * by copy-on-write reassignment ({@code addBucketInternal}, {@code removeBucket}, {@code updatePolymorphicBucketsCache})
 * but read lock-free on the query-planning hot path ({@code getBuckets(true)}/{@code getBucketIds(true)}). A plain
 * (non-{@code volatile}) field gives the Java Memory Model no happens-before edge between a schema-mutation thread's
 * reassignment and a planning thread's read, so the planner is not guaranteed to ever observe a concurrent
 * {@code ALTER TYPE ... BUCKET} - the same publication gap the sibling {@code LocalSchema.bucketId2TypeMap} pattern
 * exists to avoid.
 * <p>
 * The fix makes both fields {@code volatile}. Issue #7033 extends it to the non-polymorphic siblings {@code buckets}/
 * {@code bucketIds}: same copy-on-write reassignment on the adjacent lines, same lock-free accessor (the other branch
 * of the same ternary), read by {@code SelectExecutionPlanner}'s partition pruning and {@code FetchFromSchemaTypesStep}
 * through {@code getBuckets(false)}/{@code getBucketIds(false)}. The first test pins the fix itself, so a future edit
 * cannot silently drop the modifier from any of the four. The second is a functional regression for the concrete
 * scenario: adding/removing a bucket must be reflected by both branches of {@code getBuckets}/{@code getBucketIds}
 * immediately afterward.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6678PolymorphicBucketCacheVisibilityTest extends TestHelper {

  @Test
  void bucketListFieldsMustBeVolatileForCrossThreadPlanningReads() throws Exception {
    for (final String fieldName : new String[] { "buckets", "cachedPolymorphicBuckets", "bucketIds", "cachedPolymorphicBucketIds" }) {
      final Field field = LocalDocumentType.class.getDeclaredField(fieldName);
      assertThat(Modifier.isVolatile(field.getModifiers()))
          .as(fieldName + " is copy-on-write reassigned under schema mutation and read lock-free by query planning - it "
              + "must be volatile so planning threads have a happens-before edge against the writer (issues #6678, #7033)")
          .isTrue();
    }
  }

  @Test
  void addingThenRemovingABucketIsReflectedByThePolymorphicCache() {
    database.transaction(() -> database.getSchema().createDocumentType("Product", 1));

    database.transaction(() -> {
      final DocumentType type = database.getSchema().getType("Product");
      final int before = type.getBuckets(true).size();

      final Bucket newBucket = database.getSchema().createBucket("Product_extra");
      type.addBucket(newBucket);

      assertThat(type.getBuckets(true)).hasSize(before + 1).contains(newBucket);
      assertThat(type.getBucketIds(true)).contains(newBucket.getFileId());
      assertThat(type.getBuckets(false)).hasSize(before + 1).contains(newBucket);
      assertThat(type.getBucketIds(false)).contains(newBucket.getFileId());

      type.removeBucket(newBucket);

      assertThat(type.getBuckets(true)).hasSize(before).doesNotContain(newBucket);
      assertThat(type.getBucketIds(true)).doesNotContain(newBucket.getFileId());
      assertThat(type.getBuckets(false)).hasSize(before).doesNotContain(newBucket);
      assertThat(type.getBucketIds(false)).doesNotContain(newBucket.getFileId());
    });
  }
}
