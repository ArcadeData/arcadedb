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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.CrashingChildJvm;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A process killed before the flush wrote any page of a type whose properties are indexed (issue #12506). The records and the
 * index entries both come back from the WAL, but the records name their properties by dictionary id: while the schema load ahead
 * of the replay registered the names it did not find in the dictionary (issue #12111), the replay skipped the dead process's
 * dictionary pages as superseded, the records came back without their indexed properties, and CHECK DATABASE reported every index
 * entry as naming a record that does not hold its key.
 * <p>
 * The writes run in a child JVM ended by {@code Runtime.halt()}, what a process kill leaves on disk: an in-process kill of the
 * same writes does not reproduce it.
 */
class RecoveryIndexedPropertiesNotFlushedTest {
  private static final String DB_PATH               = "target/databases/RecoveryIndexedPropertiesNotFlushedTest";
  private static final int    CHILD_TIMEOUT_SECONDS = 120;
  private static final int    RECORDS               = 3_000;

  @BeforeEach
  @AfterEach
  void cleanup() {
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // a failed recovery leaves the database unopenable: removed on disk below
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * The child: declares an indexed type, writes, updates and deletes with the page flush held for the whole run, so the data
   * files hold none of its pages at the kill and only the WAL can bring the committed work back, then halts.
   */
  public static void main(final String[] args) throws Exception {
    // every page stays in memory: no cap, or the commits would wait for a flush that never comes
    GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM.setValue(0);
    final DatabaseFactory factory = new DatabaseFactory(args[0]);
    final Database database = factory.open();
    ((DatabaseInternal) database).getPageManager().suspendFlushAndExecute(database, () -> {
      database.getSchema().createDocumentType("T").createProperty("k", Integer.class);
      database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
      database.command("sql", "CREATE PROPERTY T.g STRING");
      database.command("sql", "CREATE INDEX ON T (g) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument("T").set("k", i).set("g", "g" + (i % 7)).save();
      });
      database.transaction(() -> database.command("sql", "UPDATE T SET g = 'moved' WHERE k % 3 = 0"));
      database.transaction(() -> database.command("sql", "DELETE FROM T WHERE k % 5 = 0"));
      System.out.println("written");
      Runtime.getRuntime().halt(CrashingChildJvm.CRASH_EXIT_CODE);
    });
  }

  @Test
  void theIndexesAgreeWithTheRecordsAfterAKillBeforeAnyFlush() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      factory.create().close();

      final CrashingChildJvm.Result child = CrashingChildJvm.run(RecoveryIndexedPropertiesNotFlushedTest.class,
          CHILD_TIMEOUT_SECONDS, new File(DB_PATH).getAbsolutePath());
      assertThat(child.exitCode()).as("child output: %s", child.output()).isEqualTo(CrashingChildJvm.CRASH_EXIT_CODE);
      assertThat(child.output()).contains("written");

      final Database reopened = factory.open();
      try {
        assertThat(reopened.getSchema().existsIndex("T[k]")).isTrue();
        assertThat(reopened.getSchema().existsIndex("T[g]")).isTrue();

        // the records hold their indexed properties again: counted by a scan, not through an index
        assertThat(count(reopened, "SELECT count(@rid) AS c FROM T")).isEqualTo(2_400);
        assertThat(count(reopened, "SELECT count(@rid) AS c FROM T WHERE k.asString() IS NOT NULL AND g.asString() IS NOT NULL"))
            .isEqualTo(2_400);

        // and the indexes answer as the records do
        assertThat(count(reopened, "SELECT count(*) AS c FROM T WHERE k = 7")).isEqualTo(1);
        assertThat(count(reopened, "SELECT count(*) AS c FROM T WHERE k = 15")).isZero();
        assertThat(count(reopened, "SELECT count(*) AS c FROM T WHERE g = 'moved'")).isEqualTo(800);

        try (final ResultSet check = reopened.command("sql", "CHECK DATABASE")) {
          final Result row = check.next();
          assertThat(row.<Collection<?>>getProperty("warnings")).as("index entries that disagree with their records").isEmpty();
          assertThat(row.<Collection<?>>getProperty("corruptedRecords")).isEmpty();
        }
      } finally {
        reopened.close();
      }
    } finally {
      factory.close();
    }
  }

  private static long count(final Database database, final String sql) {
    try (final ResultSet rows = database.query("sql", sql)) {
      return rows.next().<Number>getProperty("c").longValue();
    }
  }
}
