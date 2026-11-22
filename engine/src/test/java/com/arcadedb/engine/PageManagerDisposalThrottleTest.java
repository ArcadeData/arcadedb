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
 * Pins the contract of the page-cache disposal check that runs on every page access: below the RAM cap it never
 * evicts, and above it the check is throttled to one eviction pass per window but still fires again once the
 * window has passed (the throttle reads the coarse clock, not a clock call per page access, #8523).
 */
class PageManagerDisposalThrottleTest {
  private static final String DB_PATH    = "./target/databases/PageManagerDisposalThrottleTest";
  private static final int    PAYLOAD_KB = 10;

  private Database db;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
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

  private void open(final int maxRamMb) {
    GlobalConfiguration.MAX_PAGE_RAM.setValue(maxRamMb);
    db = new DatabaseFactory(DB_PATH).create();
    db.getSchema().createDocumentType("Item");
  }

  private void insert(final int records) {
    db.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final MutableDocument doc = db.newDocument("Item");
        doc.set("val", i);
        doc.set("payload", "x".repeat(PAYLOAD_KB * 1024));
        doc.save();
      }
    });
  }

  private void scan() {
    try (final ResultSet rs = db.query("sql", "SELECT FROM Item")) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private long evictionRuns() {
    return ((DatabaseInternal) db).getPageManager().getStats().evictionRuns;
  }

  @Test
  void belowTheCapNothingIsEvicted() {
    open(256);
    // the counter is the JVM's: it keeps what earlier tests (and their small caps) ran, so count from here
    final long before = evictionRuns();
    insert(200);
    for (int i = 0; i < 5; i++)
      scan();

    assertThat(evictionRuns() - before).as("a cache far below its RAM cap has nothing to free").isZero();
  }

  @Test
  void aboveTheCapEvictionFiresAgainOnceTheThrottleWindowPassed() throws InterruptedException {
    open(4);
    final long before = evictionRuns();
    insert(1000);
    scan();
    final long afterFirstPressure = evictionRuns();
    assertThat(afterFirstPressure - before).as("the cache is over its cap: eviction must have run").isGreaterThan(0);

    // let the 100 ms throttle window pass, then keep the cache over the cap: the check must run a pass again
    Thread.sleep(250);
    insert(1000);
    scan();

    assertThat(evictionRuns()).as("eviction must run again after the throttle window, not stay latched off")
        .isGreaterThan(afterFirstPressure);
  }
}
