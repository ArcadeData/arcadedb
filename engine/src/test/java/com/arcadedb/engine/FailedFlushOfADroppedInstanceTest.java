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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A page whose asynchronous flush fails is kept by the flush thread ({@code failedFlushPages}) so every read finds it, keyed by its
 * {@link PageId}, whose database part compares by PATH. The failure of a page of an instance that is dropped while the flush thread
 * is writing it is reported AFTER the drop purged the pages of that instance, so the page was registered for an instance that is gone
 * and nothing ever removed it: the retry skips pages of a closed database. The next instance created on the same path then found
 * it as the flush-queue image of ITS page: its first commit failed with {@code DatabaseIsClosedException} (the page's own, closed,
 * database is asked for the file manager by the commit-time version probe), and a database whose creation failed that way is left
 * on the disk without the type its creator had just declared.
 */
class FailedFlushOfADroppedInstanceTest extends TestHelper {

  @Test
  void aFlushFailureReportedAfterTheDropIsNotKeptForTheNextInstanceOnThePath() throws Exception {
    // Another database stays open for the whole test, as on a server: with none open the page manager shuts down and drops the flush
    // thread with everything it holds, which would hide the stale page
    final DatabaseFactory otherFactory = new DatabaseFactory(getDatabasePath() + "-other");
    if (otherFactory.exists())
      otherFactory.open().drop();
    final Database other = otherFactory.create();
    try {
      reproduce(other);
    } finally {
      PageManager.setPageWriteFaultInjector(null);
      other.drop();
      otherFactory.close();
    }
  }

  private void reproduce(final Database other) throws Exception {
    final DatabaseInternal first = (DatabaseInternal) database;
    final PageManager pageManager = first.getPageManager();

    // The flush thread is held inside the write of the first page of the first instance, and fails it once released
    final CountDownLatch inWrite = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    PageManager.setPageWriteFaultInjector(pageId -> {
      if (pageId.getDatabase() != first)
        return;
      inWrite.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("injected: the write of a page of a dropped instance fails");
    });

    first.transaction(() -> {
      first.getSchema().createDocumentType("Doc");
      first.newDocument("Doc").set("value", "dropped").save();
    });
    assertThat(inWrite.await(60, TimeUnit.SECONDS)).as("the flush thread must be writing a page of the first instance").isTrue();

    // The drop purges the pages of the instance while the flush thread is still inside that write
    first.drop();
    assertThat(first.isOpen()).isFalse();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(first.getDatabasePath())).isNull();

    // The write fails now: the failure of a page of an instance that is gone
    release.countDown();

    // The flush thread is single: once it has written a page of another database, what it held for the first one is settled
    other.transaction(() -> {
      other.getSchema().createDocumentType("Marker");
      other.newDocument("Marker").set("value", 1).save();
    });
    pageManager.waitAllPagesOfDatabaseAreFlushed((DatabaseInternal) other);
    PageManager.setPageWriteFaultInjector(null);

    // The next instance on the path declares the same type: the pages it touches are the ones the dropped instance had. Its first
    // commit used to fail with DatabaseIsClosedException, asked of the dropped instance by the version probe of the stale page
    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc");
      database.newDocument("Doc").set("value", "recreated").save();
    });

    assertThat(database.countType("Doc", false)).isEqualTo(1);
    assertThat(database.query("sql", "select value from Doc").next().<String>getProperty("value")).isEqualTo("recreated");

    assertThat(pageManager.getStats().pagesFailedToFlush)
        .as("a page of a dropped instance must not be kept as a failed flush: nothing will ever write it or release it")
        .isZero();
  }
}
