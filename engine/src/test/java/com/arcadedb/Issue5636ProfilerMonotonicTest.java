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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5636 item 2: {@link Profiler} summed the per-database counters of the CURRENTLY OPEN
 * databases only, so closing or dropping one made the JVM-wide totals go BACKWARDS.
 * <p>
 * That is not just cosmetic. Those totals are exported to Micrometer, and a Prometheus counter that decreases is read
 * as a counter <i>reset</i>: every database close fabricated a rate() spike on the next scrape. Studio showed the same
 * thing more directly - the query and transaction counters visibly dropped when a database was dropped.
 * <p>
 * Assertions are all {@code >=} deltas rather than exact values: {@code Profiler.INSTANCE} is a JVM singleton, so
 * anything else running in the same fork contributes too. The regression is a DECREASE, which {@code >=} catches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5636ProfilerMonotonicTest {

  private static final String DB_PATH  = "target/databases/issue5636-profiler-monotonic";
  private static final int    QUERIES  = 25;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void closingADatabaseDoesNotRewindTheJvmWideCounters() {
    final long queriesBefore = profilerCount("queries");
    final long writeTxBefore = profilerCount("writeTx");

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("k", 1).save());
      for (int i = 0; i < QUERIES; i++)
        db.query("sql", "select from Doc").close();

      final long queriesOpen = profilerCount("queries");
      final long writeTxOpen = profilerCount("writeTx");
      assertThat(queriesOpen).as("the open database's queries must be counted")
          .isGreaterThanOrEqualTo(queriesBefore + QUERIES);
      assertThat(writeTxOpen).as("the open database's write transactions must be counted")
          .isGreaterThan(writeTxBefore);

      db.close();

      assertThat(profilerCount("queries")).as("closing a database must not rewind the JVM-wide query counter")
          .isGreaterThanOrEqualTo(queriesOpen);
      assertThat(profilerCount("writeTx")).as("closing a database must not rewind the JVM-wide write-tx counter")
          .isGreaterThanOrEqualTo(writeTxOpen);
    }

    // Reopen and drop: the drop path unregisters too, and must not rewind either.
    final long queriesAfterClose = profilerCount("queries");
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      db.query("sql", "select from Doc").close();
      final long queriesReopened = profilerCount("queries");
      assertThat(queriesReopened).isGreaterThan(queriesAfterClose);

      db.drop();

      assertThat(profilerCount("queries")).as("dropping a database must not rewind the JVM-wide query counter")
          .isGreaterThanOrEqualTo(queriesReopened);
    }
  }

  /**
   * A database that is never registered - or is unregistered twice - must not double-count. Guards the
   * {@code if (!databases.remove(database)) return;} short-circuit that makes the fold idempotent.
   */
  @Test
  void unregisteringTwiceFoldsTheCountersOnlyOnce() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      db.getSchema().createDocumentType("Doc");
      for (int i = 0; i < QUERIES; i++)
        db.query("sql", "select from Doc").close();

      final long beforeClose = profilerCount("queries");
      db.close();
      final long afterFirst = profilerCount("queries");

      // The database is already gone from the registry; a second unregister must be a no-op, not a second fold.
      Profiler.INSTANCE.unregisterDatabase((com.arcadedb.database.DatabaseInternal) db);
      assertThat(profilerCount("queries")).as("a repeated unregister must not fold the same counters in again")
          .isEqualTo(afterFirst);
      assertThat(afterFirst).isGreaterThanOrEqualTo(beforeClose);
    }

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      factory.open().drop();
    }
  }

  private static long profilerCount(final String key) {
    final JSONObject json = Profiler.INSTANCE.toJSON();
    return json.getJSONObject(key).getLong("count", 0L);
  }
}
