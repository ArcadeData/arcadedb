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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * Regression test for #3615: concurrent index compaction causes IndexOutOfBoundsException
 * because LocalSchema.files (ArrayList) was accessed without synchronization from multiple
 * async compaction threads calling registerFile()/removeFile() simultaneously.
 */
@ResourceLock("GlobalConfiguration")
class ConcurrentCompactionTest extends TestHelper {
  private static final int TYPES_COUNT = 8;
  private static final int RECORDS_PER_TYPE = 5_000;

  @Test
  void concurrentCompactionDoesNotCorruptFilesList() throws Exception {
    final int origRam = GlobalConfiguration.INDEX_COMPACTION_RAM_MB.getValueAsInteger();
    final int origMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();
    try {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(1);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

      // Create multiple types each with their own indexes
      for (int t = 0; t < TYPES_COUNT; t++) {
        final String typeName = "Type_" + t;
        database.transaction(() -> {
          final var type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(2).create();
          type.createProperty("key", String.class);
          type.createProperty("value", Integer.class);
          database.getSchema().buildTypeIndex(typeName, new String[] { "key" })
              .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(64 * 1024).create();
          database.getSchema().buildTypeIndex(typeName, new String[] { "value" })
              .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(64 * 1024).create();
        });
      }

      // Insert records into all types
      for (int t = 0; t < TYPES_COUNT; t++) {
        final String typeName = "Type_" + t;
        database.transaction(() -> {
          for (int i = 0; i < RECORDS_PER_TYPE; i++) {
            final MutableDocument doc = database.newDocument(typeName);
            doc.set("key", "key_" + i);
            doc.set("value", i);
            doc.save();
          }
        });
      }

      // Force concurrent compaction on all indexes simultaneously
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch doneLatch = new CountDownLatch(TYPES_COUNT);

      for (int t = 0; t < TYPES_COUNT; t++) {
        final String typeName = "Type_" + t;
        new Thread(() -> {
          try {
            startLatch.await();
            for (final Index index : database.getSchema().getIndexes()) {
              if (index instanceof TypeIndex && index.getName().startsWith(typeName)) {
                ((IndexInternal) index).scheduleCompaction();
                ((IndexInternal) index).compact();
              }
            }
          } catch (final Throwable e) {
            error.compareAndSet(null, e);
          } finally {
            doneLatch.countDown();
          }
        }).start();
      }

      // Release all threads at once
      startLatch.countDown();
      assertThat(doneLatch.await(60, TimeUnit.SECONDS)).isTrue();

      assertThat(error.get()).as("Concurrent compaction should not throw: %s",
          error.get() != null ? error.get().getMessage() : "").isNull();

      // Verify data integrity after concurrent compaction
      for (int t = 0; t < TYPES_COUNT; t++) {
        final String typeName = "Type_" + t;
        database.transaction(() -> {
          assertThat(database.countType(typeName, false)).isEqualTo(RECORDS_PER_TYPE);
        });
      }

    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(origRam);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(origMinPages);
    }
  }
}
