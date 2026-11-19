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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema of a crashed database is loaded before its WAL is replayed, so a page that load writes moves past the versions the WAL
 * holds for it, and the replay skips the dead process's changes to it as superseded (issue #12111, where the page was the
 * dictionary's). Every kind of component the load builds is declared here, some before the crash and some in the WAL only, and at
 * the replay every page has to be the one on disk.
 */
class RecoveryLoadWritesNoPageTest {
  private static final String  DB_PATH   = "target/databases/RecoveryLoadWritesNoPageTest";
  // name.fileId.pageSize.vVersion.extension
  private static final Pattern FILE_NAME = Pattern.compile("^.+\\.\\d+\\.(\\d+)\\.v\\d+\\.\\w+$");

  @BeforeEach
  @AfterEach
  void cleanup() throws ReflectiveOperationException {
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    setBeforeReplayHook(null);
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theLoadAheadOfTheReplayWritesNoPage() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database created = factory.create();
      for (final String ddl : new String[] { "CREATE DOCUMENT TYPE Doc", "CREATE PROPERTY Doc.id INTEGER",
          "CREATE PROPERTY Doc.content STRING", "CREATE PROPERTY Doc.code STRING", "CREATE PROPERTY Doc.vec ARRAY_OF_FLOATS",
          "CREATE PROPERTY Doc.location STRING", "CREATE PROPERTY Doc.brand STRING", "CREATE INDEX ON Doc (id) UNIQUE",
          "CREATE INDEX ON Doc (content) FULL_TEXT", "CREATE INDEX ON Doc (code) UNIQUE_HASH",
          "CREATE INDEX ON Doc (vec) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE'}",
          "CREATE INDEX ON Doc (location) GEOSPATIAL", "CREATE INDEX ON Doc (brand) COLUMNAR", "CREATE VERTEX TYPE N",
          "CREATE PROPERTY N.id INTEGER", "CREATE INDEX ON N (id) UNIQUE", "CREATE EDGE TYPE R", "CREATE PROPERTY R.w DOUBLE",
          "CREATE DOCUMENT TYPE Sparse", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS",
          "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS",
          "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }",
          "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1" })
        created.command("sql", ddl);
      addRows(created, 0, 40);
      created.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (N) EDGE TYPES (R) PROPERTIES (w)");
      created.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT id, code FROM Doc");
      created.close();

      final DatabaseInternal crashed = (DatabaseInternal) factory.open();
      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(crashed, true))
          .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();
      // In the WAL only: a type, property names and indexes the disk has never seen, and changes to every component above
      for (final String ddl : new String[] { "CREATE DOCUMENT TYPE Doc2", "CREATE PROPERTY Doc2.k STRING",
          "CREATE PROPERTY Doc2.k2 STRING", "CREATE INDEX ON Doc2 (k) NOTUNIQUE", "CREATE PROPERTY Doc.extra STRING",
          "CREATE INDEX ON Doc (extra) NOTUNIQUE" })
        crashed.command("sql", ddl);
      addRows(crashed, 40, 200);
      crashed.transaction(() -> crashed.newDocument("Doc2").set("k", "a", "k2", "b").save());
      crashed.command("sql", "CREATE MATERIALIZED VIEW mv2 AS SELECT k FROM Doc2");
      crashed.kill();
      crashed.close();

      final Map<String, int[]> onDisk = pageVersionsOnDisk();
      final Map<String, String> written = new TreeMap<>();
      setBeforeReplayHook(opening -> written.putAll(pagesNotAsOnDisk(opening, onDisk)));

      final Database reopened = factory.open();
      try {
        assertThat(written).as("pages the load wrote ahead of the replay (file: page:disk->now)").isEmpty();
        assertThat(((LocalDatabase) reopened).getTransactionManager().isLastRecoveryIncomplete()).isFalse();
        assertThat(reopened.countType("Doc", false)).isEqualTo(200);
        assertThat(reopened.countType("N", false)).isEqualTo(200);
        try (final ResultSet rows = reopened.query("sql", "SELECT k, k2 FROM Doc2")) {
          assertThat(rows.stream().map(r -> r.getProperty("k") + "/" + r.getProperty("k2")).toList()).containsExactly("a/b");
        }
      } finally {
        reopened.close();
      }
    }
  }

  private static void addRows(final Database database, final int from, final int to) {
    database.transaction(() -> {
      for (int i = from; i < to; i++) {
        database.newDocument("Doc")
            .set("id", i, "content", "word" + i + " common text", "code", "c" + i, "vec", new float[] { i, 1, 2, 3 + i }, "location",
                "POINT (" + (i % 90) + " " + (i % 45) + ")", "brand", "b" + (i % 7)).save();
        final MutableVertex vertex = database.newVertex("N").set("id", i).save();
        if (i > from)
          vertex.newEdge("R", database.lookupByKey("N", "id", i - 1).next().getRecord().asVertex(), "w", (double) i);
        database.newDocument("Sparse").set("tokens", new int[] { i % 16, (i + 1) % 16 }, "weights", new float[] { 0.5f, 0.25f })
            .save();
        database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", 1_000L + i, "h" + (i % 3), (double) i);
      }
    });
  }

  /** The version of every page of every paginated file, as the header at the start of each page on disk has it. */
  private static Map<String, int[]> pageVersionsOnDisk() throws IOException {
    final Map<String, int[]> versions = new HashMap<>();
    for (final File file : new File(DB_PATH).listFiles()) {
      final Matcher matcher = FILE_NAME.matcher(file.getName());
      if (!matcher.matches())
        continue;
      final int pageSize = Integer.parseInt(matcher.group(1));
      try (final RandomAccessFile raw = new RandomAccessFile(file, "r")) {
        final int[] pages = new int[(int) (raw.length() / pageSize)];
        for (int page = 0; page < pages.length; page++) {
          raw.seek((long) page * pageSize);
          pages[page] = raw.readInt();
        }
        versions.put(file.getName(), pages);
      }
    }
    return versions;
  }

  /** The pages whose most recent version, cached or queued for the flush included, is not the one on disk. */
  private static Map<String, String> pagesNotAsOnDisk(final LocalDatabase database, final Map<String, int[]> onDisk) {
    final Map<String, String> differing = new HashMap<>();
    try {
      for (final ComponentFile componentFile : database.getFileManager().getFiles()) {
        if (!(componentFile instanceof PaginatedComponentFile file))
          continue;
        final int[] disk = onDisk.getOrDefault(file.getFileName(), new int[0]);
        int pages = Math.max(disk.length, (int) file.getTotalPages());
        if (database.getSchema().getFileByIdIfExists(file.getFileId()) instanceof PaginatedComponent component)
          pages = Math.max(pages, component.getTotalPages());
        final StringBuilder changes = new StringBuilder();
        for (int page = 0; page < pages; page++) {
          final int now = database.getPageManager()
              .getMostRecentVersionOfPage(new PageId(database, file.getFileId(), page), file.getPageSize());
          // a page past the end of the file reads as version 0 and is not there either
          final int before = page < disk.length ? disk[page] : 0;
          if (now != before)
            changes.append(' ').append(page).append(':').append(before).append("->").append(now);
        }
        if (!changes.isEmpty())
          differing.put(file.getFileName(), changes.toString().trim());
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return differing;
  }

  private static void setBeforeReplayHook(final Consumer<LocalDatabase> hook) throws ReflectiveOperationException {
    final Field field = LocalDatabase.class.getDeclaredField("TEST_BEFORE_RECOVERY_REPLAY_HOOK");
    field.setAccessible(true);
    field.set(null, hook);
  }
}
