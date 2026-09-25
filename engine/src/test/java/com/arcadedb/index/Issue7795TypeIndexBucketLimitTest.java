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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7795: {@code TypeIndex.get(keys, limit)} asked each bucket sub-index for
 * {@code collected - limit} rows instead of {@code limit - collected}. The value was always negative, which every
 * sub-index reads as "no limit", so each bucket materialized all its entries for the key and the type index threw the
 * surplus away.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7795TypeIndexBucketLimitTest extends TestHelper {
  private static final int BUCKETS  = 4;
  private static final int PER_KEY  = 40;

  @Test
  void eachBucketIsAskedOnlyForTheRowsStillMissing() {
    final DocumentType type = seed();
    final TypeIndex real = (TypeIndex) database.getSchema().getIndexByName("Item[key]");

    // THE SAME BUCKET INDEXES, BEHIND A PROXY THAT RECORDS THE LIMIT EACH ONE IS ASKED FOR
    final List<Integer> limits = new ArrayList<>();
    final TypeIndex probe = new TypeIndex("probe", type);
    for (final IndexInternal bucketIndex : real.getIndexesOnBuckets())
      probe.addIndexOnBucket(recordingLimits(bucketIndex, limits));

    database.transaction(() -> {
      for (final int limit : new int[] { 1, 5, 15, PER_KEY - 1 }) {
        limits.clear();
        final List<Object> rows = new ArrayList<>();
        probe.get(new Object[] { "k" }, limit).forEachRemaining(rows::add);

        assertThat(rows).hasSize(limit);
        assertThat(limits).as("limit %d", limit).isNotEmpty();
        assertThat(limits.getFirst()).as("the first bucket is asked for the whole limit").isEqualTo(limit);
        for (final int asked : limits)
          assertThat(asked).as("a bucket asked for %d rows under limit %d", asked, limit).isBetween(1, limit);
      }

      // NO LIMIT STAYS NO LIMIT
      limits.clear();
      final List<Object> all = new ArrayList<>();
      probe.get(new Object[] { "k" }, -1).forEachRemaining(all::add);
      assertThat(all).hasSize(PER_KEY);
      assertThat(limits).hasSize(BUCKETS).containsOnly(-1);
    });
  }

  /** The row count the real type index answers is unchanged by the fix. */
  @Test
  void theTypeIndexStillAnswersExactlyTheLimit() {
    seed();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Item[key]");
    database.transaction(() -> {
      for (final int limit : new int[] { 1, 7, PER_KEY, PER_KEY + 10 }) {
        final List<Object> rows = new ArrayList<>();
        index.get(new Object[] { "k" }, limit).forEachRemaining(rows::add);
        assertThat(rows).hasSize(Math.min(limit, PER_KEY));
      }
    });
  }

  private DocumentType seed() {
    final DocumentType[] type = new DocumentType[1];
    database.transaction(() -> {
      type[0] = database.getSchema().createDocumentType("Item", BUCKETS);
      type[0].createProperty("key", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Item", "key");
      for (int i = 0; i < PER_KEY; i++)
        database.newDocument("Item").set("key", "k").save();
    });
    // THE KEY'S ROWS MUST BE SPREAD, OR THERE IS NO SECOND BUCKET TO PUSH A LIMIT INTO
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("Item[key]")).getIndexesOnBuckets())
      assertThat(bucketIndex.countEntries()).isPositive();
    return type[0];
  }

  private static IndexInternal recordingLimits(final IndexInternal delegate, final List<Integer> limits) {
    return (IndexInternal) Proxy.newProxyInstance(IndexInternal.class.getClassLoader(), new Class<?>[] { IndexInternal.class },
        (proxy, method, args) -> {
          if (method.getName().equals("setTypeIndex"))
            // THE REAL BUCKET INDEX KEEPS POINTING AT ITS REAL TYPE INDEX
            return null;
          if (method.getName().equals("get") && args != null && args.length == 2 && args[1] instanceof Integer limit)
            limits.add(limit);
          try {
            return method.invoke(delegate, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }
}
