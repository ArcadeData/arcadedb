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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #4333: LocalSchema.migratedFileIds must be persisted in schema.json so that
 * after a restart the WAL recovery path can distinguish safe compaction-migration skips from
 * genuinely unexpected missing files.
 */
class MigratedFileIdsPersistenceTest {

  private static final String DB_PATH   = "target/databases/MigratedFileIdsPersistenceTest";
  private static final int    PAGE_SIZE = 2 * 1024;

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    factory = new DatabaseFactory(DB_PATH);
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    else
      FileUtils.deleteRecursively(new File(DB_PATH));
  }

  // Migration map populated by compaction must be present in schema after close + reopen.
  @Test
  void migratedFileIdsArePersisted() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Item").withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex("Item", new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withPageSize(PAGE_SIZE)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Item").set("id", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[id]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(bucketIndex.getMutableIndex().getTotalPages())
        .as("Mutable index must have >= 2 pages for compaction to proceed")
        .isGreaterThanOrEqualTo(2);

    final int oldMutableFid = bucketIndex.getMutableIndex().getFileId();

    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    new Thread(() -> {
      try {
        bucketIndex.scheduleCompaction();
        final boolean compacted = bucketIndex.compact();
        if (!compacted)
          error.set(new AssertionError("compact() returned false"));
      } catch (final Throwable e) {
        error.set(e);
      } finally {
        done.countDown();
      }
    }, "compaction-thread").start();

    assertThat(done.await(30, TimeUnit.SECONDS)).as("Compaction did not complete in time").isTrue();
    assertThat(error.get()).as("Compaction threw: %s", error.get()).isNull();

    final Integer newFid = database.getSchema().getEmbedded().getMigratedFileId(oldMutableFid);
    assertThat(newFid).as("Migration entry must be present after compaction").isNotNull();

    // Close and reopen to verify the map survives serialisation/deserialisation.
    database.close();
    database = factory.open();

    final Integer restoredNewFid = database.getSchema().getEmbedded().getMigratedFileId(oldMutableFid);
    assertThat(restoredNewFid)
        .as("migratedFileIds must survive close+reopen (oldFileId=%d)", oldMutableFid)
        .isNotNull()
        .isEqualTo(newFid);
  }

  // Migration map must round-trip: same old->new entries before and after serialisation.
  @Test
  void migratedFileIdsRoundTrip() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Widget").withTotalBuckets(1).create();
      type.createProperty("code", Integer.class);
      database.getSchema().buildTypeIndex("Widget", new String[] { "code" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withPageSize(PAGE_SIZE)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Widget").set("code", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Widget[code]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    final int oldMutableFid = bucketIndex.getMutableIndex().getFileId();

    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    new Thread(() -> {
      try {
        bucketIndex.scheduleCompaction();
        bucketIndex.compact();
      } catch (final Throwable e) {
        error.set(e);
      } finally {
        done.countDown();
      }
    }, "compaction-thread").start();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(error.get()).isNull();

    final LocalSchema schemaBefore = database.getSchema().getEmbedded();
    final Integer newFidBefore = schemaBefore.getMigratedFileId(oldMutableFid);
    assertThat(newFidBefore).isNotNull();

    database.close();
    database = factory.open();

    final LocalSchema schemaAfter = database.getSchema().getEmbedded();
    assertThat(schemaAfter.getMigratedFileId(oldMutableFid))
        .as("Round-tripped migration entry must equal pre-close value")
        .isEqualTo(newFidBefore);
  }
}
