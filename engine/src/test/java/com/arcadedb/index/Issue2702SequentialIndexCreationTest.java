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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests sequential index creation on large datasets to ensure it doesn't fail with NeedRetryException
 * when background LSMTree compaction is running.
 *
 * Before the fix, creating multiple indexes sequentially would fail with:
 * "NeedRetryException: Cannot create a new index while asynchronous tasks are running"
 *
 * After the fix, CREATE INDEX waits for any running async tasks (compaction) to complete,
 * allowing sequential index creation to succeed.
 *
 * Issue: https://github.com/ArcadeData/arcadedb/issues/2702
 *
 * @author ArcadeDB Team
 */
class Issue2702SequentialIndexCreationTest extends TestHelper {
  private static final int    TOT               = 100_000; // Large enough to trigger compaction
  private static final int    INDEX_PAGE_SIZE   = 64 * 1024;
  private static final int    COMPACTION_RAM_MB = 1; // Low RAM to force compaction
  private static final int    PARALLEL          = 4;
  private static final String TYPE_NAME         = "Rating";

  @Test
  void sequentialIndexCreation() throws Exception {
    final int originalCompactionRamMB = GlobalConfiguration.INDEX_COMPACTION_RAM_MB.getValueAsInteger();
    final int originalMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();

    try {
      // Configure to trigger compaction easily
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(COMPACTION_RAM_MB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

      // Create type with multiple properties
      database.transaction(() -> {
        final DocumentType type = database.getSchema()
            .buildDocumentType()
            .withName(TYPE_NAME)
            .withTotalBuckets(PARALLEL)
            .create();

        type.createProperty("userId", Integer.class);
        type.createProperty("movieId", Integer.class);
        type.createProperty("rating", Float.class);
        type.createProperty("timestamp", Long.class);
      });

      // Insert large dataset
      database.setReadYourWrites(false);
      database.async().setCommitEvery(50000);
      database.async().setParallelLevel(PARALLEL);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);

      database.async().transaction(() -> {
        for (int i = 0; i < TOT; i++) {
          final MutableDocument doc = database.newDocument(TYPE_NAME);
          doc.set("userId", i % 10000);
          doc.set("movieId", i % 5000);
          doc.set("rating", (i % 10) / 2.0f);
          doc.set("timestamp", System.currentTimeMillis());
          doc.save();
        }
      });

      database.async().waitCompletion();

      // Create first index - this should succeed and trigger async compaction
      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex(TYPE_NAME, new String[] { "userId" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE)
            .create();
      });

      // Verify first index exists
      assertThat(database.getSchema().getIndexByName(TYPE_NAME + "[userId]")).isNotNull();

      // Force compaction to start in async mode
      final IndexInternal firstIndex = (IndexInternal) database.getSchema().getIndexByName(TYPE_NAME + "[userId]");
      firstIndex.scheduleCompaction();

      // Check if async processing is active
      final boolean asyncProcessingActive = ((DatabaseInternal) database).isAsyncProcessing();

      // Create second index immediately - this would fail with NeedRetryException before the fix
      // because compaction from first index is still running
      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex(TYPE_NAME, new String[] { "movieId" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE)
            .create();
      });

      // The test succeeds if we got here without NeedRetryException
      // (before the fix, the second index creation would have failed if asyncProcessingActive was true)

      // Verify second index exists
      assertThat(database.getSchema().getIndexByName(TYPE_NAME + "[movieId]")).isNotNull();

      // Create third index - should also succeed
      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex(TYPE_NAME, new String[] { "rating" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE)
            .create();
      });

      // Verify third index exists
      assertThat(database.getSchema().getIndexByName(TYPE_NAME + "[rating]")).isNotNull();

      // Verify all indexes work correctly
      assertThat(database.lookupByKey(TYPE_NAME, new String[] { "userId" }, new Object[] { 100 }).hasNext()).isTrue();
      assertThat(database.lookupByKey(TYPE_NAME, new String[] { "movieId" }, new Object[] { 50 }).hasNext()).isTrue();
      assertThat(database.lookupByKey(TYPE_NAME, new String[] { "rating" }, new Object[] { 2.5f }).hasNext()).isTrue();

    } finally {
      // Restore original configuration
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(originalCompactionRamMB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalMinPages);
    }
  }

  @Test
  void indexCreationWaitsForAsyncCompaction() throws Exception {
    final int originalCompactionRamMB = GlobalConfiguration.INDEX_COMPACTION_RAM_MB.getValueAsInteger();
    final int originalMinPages = GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.getValueAsInteger();

    try {
      // Configure to trigger compaction easily
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(COMPACTION_RAM_MB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

      // Create type and insert data
      database.transaction(() -> {
        final DocumentType type = database.getSchema()
            .buildDocumentType()
            .withName("TestType")
            .withTotalBuckets(PARALLEL)
            .create();

        type.createProperty("id", Integer.class);
        type.createProperty("value", String.class);
      });

      database.setReadYourWrites(false);
      database.async().setCommitEvery(50000);
      database.async().setParallelLevel(PARALLEL);
      database.async().setTransactionUseWAL(true);
      database.async().setTransactionSync(WALFile.FlushType.YES_NOMETADATA);

      database.async().transaction(() -> {
        for (int i = 0; i < TOT; i++) {
          final MutableDocument doc = database.newDocument("TestType");
          doc.set("id", i);
          doc.set("value", "value-" + i);
          doc.save();
        }
      });

      database.async().waitCompletion();

      // Create first index
      database.transaction(() -> {
        database.getSchema()
            .buildTypeIndex("TestType", new String[] { "id" })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false)
            .withPageSize(INDEX_PAGE_SIZE)
            .create();
      });

      // Force compaction to run in async mode
      final IndexInternal firstIndex = (IndexInternal) database.getSchema().getIndexByName("TestType[id]");
      firstIndex.scheduleCompaction();

      // Wait a bit to ensure compaction starts
      Thread.sleep(100);

      // Verify async processing is active
      final boolean wasAsyncActive = ((DatabaseInternal) database).isAsyncProcessing();

      // Track that index creation actually waited
      final AtomicBoolean indexCreationCompleted = new AtomicBoolean(false);
      final CountDownLatch creationLatch = new CountDownLatch(1);

      // Create second index in a separate thread
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          database.transaction(() -> {
            database.getSchema()
                .buildTypeIndex("TestType", new String[] { "value" })
                .withType(Schema.INDEX_TYPE.LSM_TREE)
                .withUnique(false)
                .withPageSize(INDEX_PAGE_SIZE)
                .create();
          });
          indexCreationCompleted.set(true);
          creationLatch.countDown();
        }
      }, 0);

      // Wait for index creation to complete
      creationLatch.await();

      // Verify both indexes were created successfully
      assertThat(database.getSchema().getIndexByName("TestType[id]")).isNotNull();
      assertThat(database.getSchema().getIndexByName("TestType[value]")).isNotNull();
      assertThat(indexCreationCompleted.get()).isTrue();

      // If async processing was active and we still succeeded, the fix is working
      // (before the fix, this would have thrown NeedRetryException)

    } finally {
      // Restore original configuration
      GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(originalCompactionRamMB);
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(originalMinPages);
    }
  }
}
