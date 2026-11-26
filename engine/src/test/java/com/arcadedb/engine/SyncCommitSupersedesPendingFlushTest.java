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
package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13441: a page committed with asynchronous flush and then rewritten by a commit with synchronous flush (a
 * database whose async flush is turned off after {@code create()}) used to leave the older copy in the flush pipeline,
 * and that copy reached the file AFTER the synchronous write, reverting the page on disk. On the dictionary page that
 * re-registered every property name in another order on the next open, so every record decoded under shifted names.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class SyncCommitSupersedesPendingFlushTest {
  private static final List<String> PROPERTIES     = List.of("name", "email", "age", "city", "country");
  private static final String       DATABASE_PATH  = "target/databases/" + SyncCommitSupersedesPendingFlushTest.class.getSimpleName();
  private static final String       BLOCKER_PATH   = DATABASE_PATH + "-blocker";

  @AfterEach
  void tearDown() {
    PageManager.setPageWriteFaultInjector(null);
    for (final String path : new String[] { DATABASE_PATH, BLOCKER_PATH }) {
      final DatabaseFactory factory = new DatabaseFactory(path);
      if (factory.exists())
        factory.open().drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  /**
   * The older copy is still QUEUED when the synchronous write happens: the flush thread is held on a batch of another
   * database, so the batch of {@code create()} waits behind it.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aQueuedCopyOfTheDictionaryPageDoesNotRevertTheSynchronousWrite() throws Exception {
    final Database blocker = new DatabaseFactory(BLOCKER_PATH).create();
    try {
      final CountDownLatch held = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final String blockerName = blocker.getName();
      PageManager.setPageWriteFaultInjector(pageId -> {
        if (isFlushThread() && blockerName.equals(pageId.getDatabase().getName())) {
          held.countDown();
          awaitQuietly(release, 30);
        }
      });

      blocker.transaction(() -> blocker.getSchema().createDocumentType("Blocker"));
      assertThat(held.await(30, TimeUnit.SECONDS)).as("the flush thread must be held on the blocker's batch").isTrue();

      try {
        final Database db = new DatabaseFactory(DATABASE_PATH).create();
        final Map<String, Integer> before = declareSchemaSynchronously(db);
        // The queued copy of create() is written now, after the synchronous rewrites of the dictionary page
        release.countDown();
        db.close();
        assertSurvivesReopen(before);
      } finally {
        release.countDown();
      }
    } finally {
      PageManager.setPageWriteFaultInjector(null);
      blocker.drop();
    }
  }

  /**
   * The older copy is IN FLIGHT when the synchronous write starts: the flush thread is inside the write of dictionary
   * page 0 of {@code create()}, held there for a while. The synchronous write must land after it, never before.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anInFlightCopyOfTheDictionaryPageDoesNotRevertTheSynchronousWrite() throws Exception {
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    PageManager.setPageWriteFaultInjector(pageId -> {
      if (isFlushThread() && pageId.getFileId() == 0 && pageId.getPageNumber() == 0 && DATABASE_PATH.endsWith(
          pageId.getDatabase().getName())) {
        held.countDown();
        awaitQuietly(release, 30);
      }
    });

    final Thread releaser = new Thread(() -> {
      // Released by time, not by the test thread: with the fix the synchronous write waits for this very write
      awaitQuietly(held, 30);
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      release.countDown();
    });
    releaser.start();
    try {
      final Database db = new DatabaseFactory(DATABASE_PATH).create();
      final Map<String, Integer> before = declareSchemaSynchronously(db);
      assertThat(held.getCount()).as("the flush thread must have been held on dictionary page 0").isZero();
      release.countDown();
      db.close();
      assertSurvivesReopen(before);
    } finally {
      release.countDown();
      releaser.join();
    }
  }

  /** The application's sequence of the issue: async flush turned off right after {@code create()}, then the schema. */
  private static Map<String, Integer> declareSchemaSynchronously(final Database db) {
    db.setAsyncFlush(false);
    final VertexType type = db.getSchema().createVertexType("Item");
    for (final String p : PROPERTIES)
      type.createProperty(p, Type.STRING);
    db.transaction(() -> {
      for (int v = 0; v < 3; v++) {
        final var vertex = db.newVertex("Item");
        for (final String p : PROPERTIES)
          vertex.set(p, p + "-" + v);
        vertex.save();
      }
    });
    return new TreeMap<>(dictionary(db));
  }

  private void assertSurvivesReopen(final Map<String, Integer> before) {
    PageManager.setPageWriteFaultInjector(null);
    try (final Database db = new DatabaseFactory(DATABASE_PATH).open()) {
      assertThat(new TreeMap<>(dictionary(db))).as("the dictionary must be the one written before close").isEqualTo(before);
      final Vertex first = db.iterateType("Item", false).next().asVertex();
      for (final String p : PROPERTIES)
        assertThat(first.getString(p)).as("property '%s' must decode under its own name", p).startsWith(p + "-");
    }
  }

  private static Map<String, Integer> dictionary(final Database db) {
    return ((DatabaseInternal) db).getSchema().getEmbedded().getDictionary().getDictionaryMap();
  }

  private static boolean isFlushThread() {
    return Thread.currentThread() instanceof PageManagerFlushThread;
  }

  private static void awaitQuietly(final CountDownLatch latch, final int seconds) {
    try {
      latch.await(seconds, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
