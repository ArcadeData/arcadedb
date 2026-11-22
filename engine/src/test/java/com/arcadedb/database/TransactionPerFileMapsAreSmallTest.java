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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.LongLongHashMap;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13005: the per-file maps a transaction keeps (the change stamps of the files it wrote and the index registration
 * sequence of each bucket it first wrote to) hold a handful of entries, so a transaction that writes starts them small. They
 * were created at the {@link LongLongHashMap} default of 1024 slots, two 8 KB arrays per map filled and thrown away with every
 * autocommit write.
 */
class TransactionPerFileMapsAreSmallTest extends TestHelper {
  private static final int SMALL = 64;

  @Test
  void aTransactionThatWritesOneBucketStartsItsPerFileMapsSmall() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("OneBucket", 1);
    database.begin();
    try {
      database.newDocument(type.getName()).set("a", 1).save();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

      assertThat(capacityOf(tx, "fileStamps")).as("change stamps of the files written").isLessThanOrEqualTo(SMALL);
      assertThat(capacityOf(tx, "bucketFirstWriteSequences")).as("index registration sequence per bucket written")
          .isLessThanOrEqualTo(SMALL);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aTransactionThatWritesManyBucketsGrowsTheMapsAndKeepsEveryBucket() throws Exception {
    final int buckets = 32;
    final DocumentType type = database.getSchema().createDocumentType("ManyBuckets", buckets);
    assertThat(type.getBuckets(false)).hasSize(buckets);

    database.begin();
    try {
      for (final com.arcadedb.engine.Bucket bucket : type.getBuckets(false))
        database.newDocument(type.getName()).set("a", 1).save(bucket.getName());
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

      assertThat(capacityOf(tx, "fileStamps")).as("one stamp per bucket written").isGreaterThan(buckets);
      assertThat(capacityOf(tx, "bucketFirstWriteSequences")).as("one sequence per bucket written").isGreaterThan(buckets);
      assertThat(tx.getModifiedPages()).isPositive();
    } finally {
      database.rollback();
    }
  }

  private static int capacityOf(final TransactionContext tx, final String field) throws Exception {
    final Field mapField = TransactionContext.class.getDeclaredField(field);
    mapField.setAccessible(true);
    final LongLongHashMap map = (LongLongHashMap) mapField.get(tx);
    assertThat(map).as(field).isNotNull();
    final Field keys = LongLongHashMap.class.getDeclaredField("keys");
    keys.setAccessible(true);
    return ((long[]) keys.get(map)).length;
  }
}
