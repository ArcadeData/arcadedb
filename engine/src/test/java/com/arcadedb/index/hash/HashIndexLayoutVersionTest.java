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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5712: the buckets of a hash index are no longer kept sorted (version 2: append + 1-byte slot tags), while the
 * indexes created before keep the sorted layout (version 1) and must still open and work. Every scenario runs on all the
 * layouts, with a tiny page so that splits and overflow chains are exercised. Version 3 (issue #9228) adds RID lists to the
 * bucket pages of version 2, and every scenario runs on it too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Execution(ExecutionMode.SAME_THREAD)
class HashIndexLayoutVersionTest extends TestHelper {
  private static final int PAGE_SIZE = 1_024;

  @BeforeEach
  void startWithTheCurrentLayout() {
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;
  }

  @AfterEach
  void restoreLayout() {
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;
  }

  @ParameterizedTest
  @ValueSource(ints = { HashIndexBucket.LEGACY_SORTED_VERSION, HashIndexBucket.INLINE_RIDS_VERSION, HashIndexBucket.CURRENT_VERSION })
  void uniqueIndexInsertLookupRemove(final int layout) {
    final Map<String, Integer> expected = createAndFill("UNIQUE_HASH", layout, 4_000, 4_000);
    assertThat(layoutOf("U")).isEqualTo(layout);
    verify(expected, 4_000);

    // remove one key out of three, then put them back with another value
    final Random random = new Random(7);
    database.transaction(() -> {
      for (final String key : expected.keySet().toArray(new String[0]))
        if (random.nextInt(3) == 0) {
          database.command("sql", "DELETE FROM U WHERE k = ?", key).close();
          expected.remove(key);
        }
    });
    verify(expected, 4_000);

    database.transaction(() -> {
      for (int i = 0; i < 4_000; i += 3) {
        final String key = "key-" + i;
        if (!expected.containsKey(key)) {
          database.command("sql", "INSERT INTO U SET k = ?, v = ?", key, -i).close();
          expected.put(key, -i);
        }
      }
    });
    verify(expected, 4_000);
  }

  @ParameterizedTest
  @ValueSource(ints = { HashIndexBucket.LEGACY_SORTED_VERSION, HashIndexBucket.INLINE_RIDS_VERSION, HashIndexBucket.CURRENT_VERSION })
  void notUniqueIndexKeepsEveryRidAndRemovesOne(final int layout) {
    // 40 keys with 150 records each: the entries outgrow the page and spill into extra entries and overflow pages
    createAndFill("NOTUNIQUE_HASH", layout, 6_000, 40);
    assertThat(layoutOf("U")).isEqualTo(layout);
    assertThat(count("SELECT count(*) AS c FROM U WHERE k = 'key-5'")).isEqualTo(150);

    database.transaction(() -> database.command("sql", "DELETE FROM U WHERE k = 'key-5' AND v < 3000").close());
    assertThat(count("SELECT count(*) AS c FROM U WHERE k = 'key-5'")).isEqualTo(75);
    assertThat(count("SELECT count(*) AS c FROM U WHERE k = 'key-6'")).isEqualTo(150);

    database.transaction(() -> database.command("sql", "DELETE FROM U WHERE k = 'key-7'").close());
    assertThat(count("SELECT count(*) AS c FROM U WHERE k = 'key-7'")).isEqualTo(0);
    assertThat(count("SELECT count(*) AS c FROM U")).isEqualTo(6_000 - 75 - 150);
  }

  @ParameterizedTest
  @ValueSource(ints = { HashIndexBucket.LEGACY_SORTED_VERSION, HashIndexBucket.INLINE_RIDS_VERSION, HashIndexBucket.CURRENT_VERSION })
  void indexSurvivesReopen(final int layout) {
    final Map<String, Integer> expected = createAndFill("UNIQUE_HASH", layout, 3_000, 3_000);
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;

    reopenDatabase();

    // a file keeps the layout it was created with: the version is part of its name
    assertThat(layoutOf("U")).isEqualTo(layout);
    verify(expected, 3_000);

    database.transaction(() -> database.command("sql", "INSERT INTO U SET k = 'after-reopen', v = 1").close());
    assertThat(count("SELECT count(*) AS c FROM U WHERE k = 'after-reopen'")).isEqualTo(1);
  }

  @Test
  void rebuildMovesALegacyIndexToTheCurrentLayout() {
    final Map<String, Integer> expected = createAndFill("UNIQUE_HASH", HashIndexBucket.LEGACY_SORTED_VERSION, 3_000, 3_000);
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;
    assertThat(layoutOf("U")).isEqualTo(HashIndexBucket.LEGACY_SORTED_VERSION);

    database.command("sql", "REBUILD INDEX *").close();

    assertThat(layoutOf("U")).isEqualTo(HashIndexBucket.CURRENT_VERSION);
    verify(expected, 3_000);
  }

  @Test
  void defaultPageSizeFollowsTheKeyWidth() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE W");
      database.command("sql", "CREATE PROPERTY W.n LONG");
      database.command("sql", "CREATE PROPERTY W.s STRING");
      database.command("sql", "CREATE INDEX ON W (n) UNIQUE_HASH").close();
      database.command("sql", "CREATE INDEX ON W (s) UNIQUE_HASH").close();
      database.command("sql", "CREATE INDEX ON W (n, s) NOTUNIQUE_HASH").close();
    });
    for (final TypeIndex index : database.getSchema().getType("W").getAllIndexes(false)) {
      final int expected = index.getPropertyNames().equals(List.of("n")) ?
          HashIndexBucket.DEF_PAGE_SIZE :
          HashIndexBucket.DEF_VARIABLE_KEY_PAGE_SIZE;
      assertThat(index.getSubIndexes().get(0).getPageSize()).as(index.getPropertyNames().toString()).isEqualTo(expected);
    }
  }

  /** A legacy index built with the old 64 KB default takes the new default when rebuilt, any other size is kept. */
  @Test
  void rebuildOfALegacyIndexReplacesTheOldDefaultPageSizeOnly() {
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.LEGACY_SORTED_VERSION;
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE A");
      database.command("sql", "CREATE PROPERTY A.k STRING");
      database.command("sql", "CREATE DOCUMENT TYPE B");
      database.command("sql", "CREATE PROPERTY B.k STRING");
      database.getSchema().buildTypeIndex("A", new String[] { "k" }).withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(HashIndexBucket.LEGACY_DEF_PAGE_SIZE).create();
      database.getSchema().buildTypeIndex("B", new String[] { "k" }).withType(Schema.INDEX_TYPE.HASH).withUnique(true)
          .withPageSize(8_192).create();
    });
    HashIndex.HashIndexFactoryHandler.layoutVersion = HashIndexBucket.CURRENT_VERSION;

    database.command("sql", "REBUILD INDEX *").close();

    assertThat(database.getSchema().getType("A").getAllIndexes(false).iterator().next().getSubIndexes().get(0).getPageSize())
        .isEqualTo(HashIndexBucket.DEF_VARIABLE_KEY_PAGE_SIZE);
    assertThat(database.getSchema().getType("B").getAllIndexes(false).iterator().next().getSubIndexes().get(0).getPageSize())
        .isEqualTo(8_192);
  }

  /** A file of a layout version this server does not know is refused with a clear message, not read as the current one. */
  @Test
  void aFileOfAnUnknownLayoutVersionIsRefused() {
    createAndFill("UNIQUE_HASH", HashIndexBucket.CURRENT_VERSION, 100, 100);
    final String databasePath = database.getDatabasePath();
    database.close();

    final File[] files = new File(databasePath).listFiles((dir, name) -> name.endsWith(".uhashidx"));
    assertThat(files).hasSize(1);
    final File original = files[0];
    final File future = new File(original.getParentFile(), original.getName().replace(".v3.", ".v4."));
    assertThat(original.renameTo(future)).isTrue();

    assertThatThrownBy(() -> new DatabaseFactory(databasePath).open()).hasStackTraceContaining("page layout version 4");

    // put the file back so the fixture can close and drop the database
    assertThat(future.renameTo(original)).isTrue();
    database = new DatabaseFactory(databasePath).open();
  }

  /** Two different keys filed under the same tag must be told apart by the full key comparison. */
  @ParameterizedTest
  @ValueSource(ints = { HashIndexBucket.LEGACY_SORTED_VERSION, HashIndexBucket.INLINE_RIDS_VERSION, HashIndexBucket.CURRENT_VERSION })
  void keysSharingATagAreNotConfused(final int layout) {
    HashIndex.HashIndexFactoryHandler.layoutVersion = layout;
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE C");
      database.command("sql", "CREATE PROPERTY C.n LONG");
      database.command("sql", "CREATE INDEX ON C (n) UNIQUE_HASH").close();
    });
    final HashIndex index = (HashIndex) database.getSchema().getType("C").getAllIndexes(false).iterator().next().getSubIndexes()
        .get(0);

    // two keys with the same tag
    long first = 1;
    long second = -1;
    for (long candidate = 2; second < 0; candidate++)
      if (HashIndexBucket.tagOf(index.bucket.hashKeys(new Object[] { candidate })) == HashIndexBucket.tagOf(
          index.bucket.hashKeys(new Object[] { first })))
        second = candidate;

    final long a = first;
    final long b = second;
    database.transaction(() -> database.command("sql", "INSERT INTO C SET n = ?, v = 'a'", a).close());
    assertThat(count("SELECT count(*) AS c FROM C WHERE n = " + b)).isZero();

    database.transaction(() -> database.command("sql", "INSERT INTO C SET n = ?, v = 'b'", b).close());
    assertThat(count("SELECT count(*) AS c FROM C WHERE n = " + a)).isEqualTo(1);
    assertThat(count("SELECT count(*) AS c FROM C WHERE n = " + b)).isEqualTo(1);

    database.transaction(() -> database.command("sql", "DELETE FROM C WHERE n = ?", a).close());
    assertThat(count("SELECT count(*) AS c FROM C WHERE n = " + a)).isZero();
    assertThat(count("SELECT count(*) AS c FROM C WHERE n = " + b)).isEqualTo(1);
  }

  /** Delete and re-insert on the same pages again and again: the last slot keeps swapping into the freed ones. */
  @ParameterizedTest
  @ValueSource(ints = { HashIndexBucket.LEGACY_SORTED_VERSION, HashIndexBucket.INLINE_RIDS_VERSION, HashIndexBucket.CURRENT_VERSION })
  void deleteAndReinsertChurn(final int layout) {
    final Map<String, Integer> expected = createAndFill("UNIQUE_HASH", layout, 1_500, 1_500);
    final Random random = new Random(11);
    for (int round = 0; round < 15; round++) {
      final int r = round;
      database.transaction(() -> {
        for (int i = 0; i < 400; i++) {
          final String key = "key-" + random.nextInt(1_500);
          if (expected.remove(key) != null)
            database.command("sql", "DELETE FROM U WHERE k = ?", key).close();
          else {
            database.command("sql", "INSERT INTO U SET k = ?, v = ?", key, r).close();
            expected.put(key, r);
          }
        }
      });
      verify(expected, 1_500);
    }
  }

  /** Readers without the file lock run while a writer appends: the write order of an insert must never show a half entry. */
  @Test
  @Timeout(120) // a hang detector, not a latency bound
  void concurrentReadsDuringInsertsOnTheCurrentLayout() throws Exception {
    createAndFill("UNIQUE_HASH", HashIndexBucket.CURRENT_VERSION, 1_000, 1_000);
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread reader = new Thread(() -> {
      try {
        while (!stop.get())
          for (int i = 0; i < 1_000 && !stop.get(); i += 7)
            if (count("SELECT count(*) AS c FROM U WHERE k = 'key-" + i + "'") != 1)
              throw new AssertionError("key-" + i + " not found while inserting");
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    reader.start();
    try {
      for (int batch = 0; batch < 20; batch++) {
        final int from = 1_000 + batch * 100;
        database.transaction(() -> {
          for (int i = from; i < from + 100; i++)
            database.command("sql", "INSERT INTO U SET k = ?, v = ?", "key-" + i, i).close();
        });
      }
    } finally {
      stop.set(true);
      reader.join();
    }
    assertThat(failure.get()).isNull();
    assertThat(count("SELECT count(*) AS c FROM U")).isEqualTo(3_000);
  }

  /** Slots of different keys sharing a tag must not be mistaken for each other. */
  @ParameterizedTest
  @ValueSource(ints = { HashIndexBucket.LEGACY_SORTED_VERSION, HashIndexBucket.INLINE_RIDS_VERSION, HashIndexBucket.CURRENT_VERSION })
  void aMissNeverMatches(final int layout) {
    createAndFill("UNIQUE_HASH", layout, 5_000, 5_000);
    for (int i = 0; i < 5_000; i++)
      assertThat(count("SELECT count(*) AS c FROM U WHERE k = 'missing-" + i + "'")).isZero();
  }

  private Map<String, Integer> createAndFill(final String indexType, final int layout, final int records, final int distinctKeys) {
    HashIndex.HashIndexFactoryHandler.layoutVersion = layout;
    final Map<String, Integer> expected = new HashMap<>();
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE U");
      database.command("sql", "CREATE PROPERTY U.k STRING");
      database.getSchema().buildTypeIndex("U", new String[] { "k" }).withType(Schema.INDEX_TYPE.HASH)
          .withUnique(indexType.startsWith("UNIQUE")).withPageSize(PAGE_SIZE).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final String key = "key-" + (i % distinctKeys);
        database.command("sql", "INSERT INTO U SET k = ?, v = ?", key, i).close();
        if (indexType.startsWith("UNIQUE"))
          expected.put(key, i);
      }
    });
    return expected;
  }

  private void verify(final Map<String, Integer> expected, final int universe) {
    for (final Map.Entry<String, Integer> e : expected.entrySet())
      try (final ResultSet rs = database.query("sql", "SELECT v FROM U WHERE k = ?", e.getKey())) {
        assertThat(rs.hasNext()).as(e.getKey()).isTrue();
        assertThat(rs.next().<Integer>getProperty("v")).as(e.getKey()).isEqualTo(e.getValue());
        assertThat(rs.hasNext()).isFalse();
      }
    assertThat(count("SELECT count(*) AS c FROM U")).isEqualTo(expected.size());
    for (int i = 0; i < universe; i++)
      if (!expected.containsKey("key-" + i))
        assertThat(count("SELECT count(*) AS c FROM U WHERE k = 'key-" + i + "'")).isZero();
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Long>getProperty("c");
    }
  }

  private int layoutOf(final String typeName) {
    final TypeIndex typeIndex = database.getSchema().getType(typeName).getAllIndexes(false).iterator().next();
    return typeIndex.getSubIndexes().get(0).getComponent().getVersion();
  }
}
