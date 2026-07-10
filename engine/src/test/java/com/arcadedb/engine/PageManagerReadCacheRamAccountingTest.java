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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #4390: putPageInReadCache must adjust totalReadCacheRAM by the delta
 * (newPage.physicalSize - prevPage.physicalSize) when replacing an existing cache entry,
 * so that the counter never drifts and eviction fires correctly.
 */
class PageManagerReadCacheRamAccountingTest {

  private static final String DB_PATH   = "./target/databases/PageManagerRamTest";
  private static final int    RECORDS   = 2000;
  private static final int    UPDATES   = 3;
  private static final int    PAYLOAD_KB = 10;
  private static final int    MAX_RAM_MB = 4;

  private Database db;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    // Set small RAM limit BEFORE opening the database so PageManager.startup() (run on the 0 -> 1 acquire) picks it up
    GlobalConfiguration.MAX_PAGE_RAM.setValue(MAX_RAM_MB);
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    db = factory.create();
    db.getSchema().createDocumentType("Item");
  }

  @AfterEach
  void tearDown() {
    try {
      if (db != null && db.isOpen())
        db.drop();
    } finally {
      GlobalConfiguration.MAX_PAGE_RAM.reset();
    }
  }

  @Test
  void readCacheRamIsConsistentAfterPageReplacements() {
    // INSERT initial records (fills cache pages with large payloads to exceed 4MB cache)
    db.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final MutableDocument doc = db.newDocument("Item");
        doc.set("val", i);
        doc.set("payload", "x".repeat(PAYLOAD_KB * 1024));
        doc.save();
      }
    });

    // UPDATE every record multiple times, causing the same pages to be replaced in the cache.
    // This is the core test: if putPageInReadCache does NOT account for page replacement,
    // the counter will drift and stay below the actual memory used.
    for (int round = 0; round < UPDATES; round++) {
      final int r = round;
      db.transaction(() -> {
        try (final ResultSet rs = db.query("sql", "SELECT FROM Item")) {
          while (rs.hasNext()) {
            final MutableDocument doc = rs.next().getRecord().get().asDocument(true).modify();
            doc.set("val", r);
            doc.save();
          }
        }
      });
    }

    final PageManager.PPageManagerStats stats = ((DatabaseInternal) db).getPageManager().getStats();

    // 1. readCacheRAM must be non-negative and > 0 (we have data in cache)
    assertThat(stats.readCacheRAM)
        .as("readCacheRAM must be positive (cache is holding pages)")
        .isGreaterThan(0);

    // 2. The counter must be consistent with the page entries actually held.
    //    Each page contributes exactly its physicalSize to the total, so:
    //    readCacheRAM / readCachePages = average page size (should be ~64KB or ~256KB for external pages)
    assertThat(stats.readCachePages)
        .as("readCachePages must be > 0")
        .isGreaterThan(0);

    final long avgPageSize = stats.readCacheRAM / stats.readCachePages;
    assertThat(avgPageSize)
        .as("average page size (RAM / pages) must be in plausible range for bucket pages")
        .isBetween(32_000L, 512_000L);  // 32KB to 512KB per page on average

    // 3. Eviction must have fired at least once given the limited cache and large dataset.
    assertThat(stats.evictionRuns)
        .as("eviction must have run at least once to handle the overflowing cache")
        .isGreaterThan(0);
  }
}
