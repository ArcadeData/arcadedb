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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.WALFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #3615: concurrent index compaction causes IndexOutOfBoundsException
 * because LocalSchema.files (ArrayList) was accessed without synchronization from multiple
 * async compaction threads calling registerFile()/removeFile() simultaneously.
 */
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

  // Issue #2702: sequential CREATE INDEX must wait for async compaction instead of failing with NeedRetryException
  @Test
  void sequentialIndexCreation() throws Exception {
    final int totalRecords = 100_000;
    final int indexPageSize = 64 * 1024;
    final int compactionRamMb = 1;
    final int parallel = 4;
    final String typeName = "Rating";

    final int originalCompactionRamMB = GlobalConfiguration.INDEX_COMPACTION_RAM_MB.getValueAsInteger();
    final int originalMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();

    try {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(compactionRamMb);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

      database.transaction(() -> {
        final DocumentType type = database.getSchema()
            .buildDocumentType()
            .withName(typeName)
            .withTotalBuckets(parallel)
            .create();

        type.createProperty("userId", Integer.class);
        type.createProperty("movieId", Integer.class);
        type.createProperty("rating", Float.class);
        type.createProperty("timestamp", Long.class);
      });

      database.setReadYourWrites(false);
      database.async().setCommitEvery(50000);
      database.async().setParallelLevel(parallel);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);

      database.async().transaction(() -> {
        for (int i = 0; i < totalRecords; i++) {
          final MutableDocument doc = database.newDocument(typeName);
          doc.set("userId", i % 10000);
          doc.set("movieId", i % 5000);
          doc.set("rating", (i % 10) / 2.0f);
          doc.set("timestamp", System.currentTimeMillis());
          doc.save();
        }
      });

      database.async().waitCompletion();

      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex(typeName, new String[] { "userId" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(indexPageSize)
            .create();
      });

      assertThat(database.getSchema().getIndexByName(typeName + "[userId]")).isNotNull();

      final IndexInternal firstIndex = (IndexInternal) database.getSchema().getIndexByName(typeName + "[userId]");
      firstIndex.scheduleCompaction();

      ((DatabaseInternal) database).isAsyncProcessing();

      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex(typeName, new String[] { "movieId" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(indexPageSize)
            .create();
      });

      assertThat(database.getSchema().getIndexByName(typeName + "[movieId]")).isNotNull();

      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex(typeName, new String[] { "rating" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(indexPageSize)
            .create();
      });

      assertThat(database.getSchema().getIndexByName(typeName + "[rating]")).isNotNull();

      assertThat(database.lookupByKey(typeName, new String[] { "userId" }, new Object[] { 100 }).hasNext()).isTrue();
      assertThat(database.lookupByKey(typeName, new String[] { "movieId" }, new Object[] { 50 }).hasNext()).isTrue();
      assertThat(database.lookupByKey(typeName, new String[] { "rating" }, new Object[] { 2.5f }).hasNext()).isTrue();

    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(originalCompactionRamMB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalMinPages);
    }
  }

  // Issue #2702: CREATE INDEX from a worker thread also waits for async compaction to drain
  @Test
  void indexCreationWaitsForAsyncCompaction() throws Exception {
    final int totalRecords = 100_000;
    final int indexPageSize = 64 * 1024;
    final int compactionRamMb = 1;
    final int parallel = 4;

    final int originalCompactionRamMB = GlobalConfiguration.INDEX_COMPACTION_RAM_MB.getValueAsInteger();
    final int originalMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();

    try {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(compactionRamMb);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

      database.transaction(() -> {
        final DocumentType type = database.getSchema()
            .buildDocumentType()
            .withName("TestType")
            .withTotalBuckets(parallel)
            .create();

        type.createProperty("id", Integer.class);
        type.createProperty("value", String.class);
      });

      database.setReadYourWrites(false);
      database.async().setCommitEvery(50000);
      database.async().setParallelLevel(parallel);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);

      database.async().transaction(() -> {
        for (int i = 0; i < totalRecords; i++) {
          final MutableDocument doc = database.newDocument("TestType");
          doc.set("id", i);
          doc.set("value", "value-" + i);
          doc.save();
        }
      });

      database.async().waitCompletion();

      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex("TestType", new String[] { "id" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(indexPageSize)
            .create();
      });

      final IndexInternal firstIndex = (IndexInternal) database.getSchema().getIndexByName("TestType[id]");
      firstIndex.scheduleCompaction();

      Thread.sleep(100);

      ((DatabaseInternal) database).isAsyncProcessing();

      final AtomicBoolean indexCreationCompleted = new AtomicBoolean(false);
      final CountDownLatch creationLatch = new CountDownLatch(1);

      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          database.transaction(() -> {
            database.getSchema()
                .buildTypeIndex("TestType", new String[] { "value" })
                .withType(Schema.INDEX_TYPE.LSM_TREE)
                .withUnique(false)
                .withPageSize(indexPageSize)
                .create();
          });
          indexCreationCompleted.set(true);
          creationLatch.countDown();
        }
      }, 0);

      creationLatch.await();

      assertThat(database.getSchema().getIndexByName("TestType[id]")).isNotNull();
      assertThat(database.getSchema().getIndexByName("TestType[value]")).isNotNull();
      assertThat(indexCreationCompleted.get()).isTrue();

    } finally {
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(originalCompactionRamMB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalMinPages);
    }
  }
}
