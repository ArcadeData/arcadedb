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
package com.arcadedb.server;

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7701: a logical (JSONL) export carrying TIMESERIES samples IS restorable into a database a client only
 * reaches over the network - through {@code IMPORT DATABASE}, which the client issues and the server runs.
 * <p>
 * The issue reads the gap the other way round, as {@code JsonlImporterFormat} being typed against
 * {@code DatabaseInternal} end to end and {@code loadTimeSeriesSamples} writing through
 * {@code LocalTimeSeriesType.getEngine()}, which a remote schema has no equivalent of. That is true, and it is not
 * specific to TIMESERIES: {@code AbstractImporter} opens its target with a local {@code DatabaseFactory} and every
 * record type it loads goes through {@code DatabaseInternal}, so the CLIENT-SIDE importer has never driven a
 * {@code RemoteDatabase} for a document, a vertex or an edge either. Teaching it a second, non-embedded write path
 * would be a throughput and transaction design of its own, and it would duplicate a path that already exists.
 * <p>
 * The path that exists is the one below. {@code IMPORT DATABASE} is a SQL statement, so a {@code RemoteDatabase}
 * can issue it like any other command; the server executes it against its own embedded database, where the
 * importer is exactly where it needs to be. What #7689 and #7702 removed were the two DDL-side reasons the type
 * itself could not be recreated on that path - the per-column codecs a restore names because they are not
 * re-derivable (issue #5475), and the column ORDER, which is what the positional samples are read by. This drives
 * a type carrying both, so the claim is checked rather than argued.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7701">issue #7701</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7701RemoteTimeSeriesLogicalRestoreIT extends BaseGraphServerTest {

  private static final String TYPE_NAME    = "remotereading";
  private static final String RESTORED_DB  = "ts-logical-restore";
  private static final String EXPORT_FILE  = "ts-logical-restore.jsonl.tgz";
  /** {@code EXPORT DATABASE} always writes under 'exports/' relative to the server process' working directory. */
  private static final File   EXPORT_DIR   = new File("./exports");

  @AfterEach
  void dropTheRestoredDatabase() {
    try {
      final RemoteServer server = remoteServer();
      if (server.exists(RESTORED_DB))
        server.drop(RESTORED_DB);
    } finally {
      FileUtils.deleteRecursively(new File(EXPORT_DIR, EXPORT_FILE));
    }
  }

  private int httpPort() {
    // The port the test server actually bound, not the 2480 the range starts at: a server already listening there
    // would otherwise take these requests and fail the test as an authentication error rather than a port conflict.
    return getServer(0).getHttpServer().getPort();
  }

  private RemoteServer remoteServer() {
    return new RemoteServer("127.0.0.1", httpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private RemoteDatabase remote(final String databaseName) {
    return new RemoteDatabase("127.0.0.1", httpPort(), databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  /**
   * The whole round trip, driven end to end by a client that holds nothing but {@code RemoteDatabase} handles:
   * create the type, write samples, export, create a second database, import into it, read the samples back.
   * <p>
   * The type interleaves its TAG and FIELD columns on purpose. That is the layout whose restore used to be
   * refused outright, because the DDL could not spell the order and the samples are positional arrays
   * (issues #7702/#7740); {@code f1} and {@code f2} share a data type with nothing between them but {@code t1},
   * so a shift would read back as plausible numbers rather than as an error.
   */
  @Test
  void aTimeSeriesTypeAndItsSamplesSurviveAnExportAndImportDrivenEntirelyRemotely() {
    try (final RemoteDatabase source = remote(getDatabaseName())) {
      source.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts "
          + "FIELDS (f1 DOUBLE CODEC DICTIONARY) TAGS (t1 STRING) FIELDS (f2 DOUBLE) SHARDS 1");

      for (int i = 1; i <= 3; i++)
        source.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = " + (i * 1_000L)
            + ", f1 = " + (i + 0.5) + ", t1 = 'west', f2 = " + (i * 10 + 0.5));

      source.command("sql", "EXPORT DATABASE 'file://" + EXPORT_FILE + "' WITH format = 'jsonl', overwrite = true");
    }

    final File exported = new File(EXPORT_DIR, EXPORT_FILE);
    assertThat(exported).as("the export the client asked for has to be on the server").exists();

    remoteServer().create(RESTORED_DB);

    try (final RemoteDatabase restored = remote(RESTORED_DB)) {
      restored.command("sql", "IMPORT DATABASE 'file://" + exported.getAbsolutePath() + "'");

      // The schema, read back through the remote schema - so the assertion is about what a remote client sees.
      final TimeSeriesType type = (TimeSeriesType) restored.getSchema().getType(TYPE_NAME);
      assertThat(columnNames(type))
          .as("a sample is a positional array, so the restored order has to be the exported one")
          .containsExactly("ts", "f1", "t1", "f2");

      // And the samples themselves, under the names they were written to.
      final List<Result> rows = rows(restored.query("sql",
          "SELECT ts, f1, t1, f2 FROM " + TYPE_NAME + " ORDER BY ts"));
      assertThat(rows).hasSize(3);
      for (int i = 0; i < rows.size(); i++) {
        // The timestamps are asserted as an ascending sequence rather than as epoch millis: the remote result
        // carries a datetime rendered in the database's own zone, and what this test is about is which COLUMN
        // each value landed in, not how a timestamp is formatted on the wire.
        assertThat(rows.get(i).<Comparable>getProperty("ts")).as("row %d keeps its timestamp", i).isNotNull();
        if (i > 0)
          assertThat(rows.get(i).<Comparable>getProperty("ts"))
              .isGreaterThan(rows.get(i - 1).getProperty("ts"));
        assertThat(rows.get(i).<Number>getProperty("f1").doubleValue()).isEqualTo(i + 1 + 0.5);
        assertThat(rows.get(i).<String>getProperty("t1")).isEqualTo("west");
        assertThat(rows.get(i).<Number>getProperty("f2").doubleValue()).isEqualTo((i + 1) * 10 + 0.5);
      }
    }
  }

  /**
   * The per-column codec a restore names because it cannot be re-derived (issue #5475) arrives on the restored
   * type too, which is the property that made this path unusable before issue #7689: the DDL the restore issues
   * had no way to say it, so a re-derived default would have silently re-encoded the column.
   */
  @Test
  void anExplicitCodecSurvivesTheRemoteRestore() {
    try (final RemoteDatabase source = remote(getDatabaseName())) {
      source.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts CODEC SIMPLE8B "
          + "TAGS (t1 STRING) FIELDS (f1 DOUBLE CODEC DICTIONARY) SHARDS 1");
      source.command("sql", "INSERT INTO " + TYPE_NAME + " SET ts = 1000, t1 = 'west', f1 = 1.5");
      source.command("sql", "EXPORT DATABASE 'file://" + EXPORT_FILE + "' WITH format = 'jsonl', overwrite = true");
    }

    remoteServer().create(RESTORED_DB);

    try (final RemoteDatabase restored = remote(RESTORED_DB)) {
      restored.command("sql",
          "IMPORT DATABASE 'file://" + new File(EXPORT_DIR, EXPORT_FILE).getAbsolutePath() + "'");

      final TimeSeriesType type = (TimeSeriesType) restored.getSchema().getType(TYPE_NAME);
      assertThat(type.getTsColumn("ts").getCompressionHint().name()).isEqualTo("SIMPLE8B");
      assertThat(type.getTsColumn("f1").getCompressionHint().name()).isEqualTo("DICTIONARY");
    }
  }

  private static List<String> columnNames(final TimeSeriesType type) {
    final List<String> names = new ArrayList<>(type.getTsColumns().size());
    for (final ColumnDefinition column : type.getTsColumns())
      names.add(column.getName());
    return names;
  }

  private static List<Result> rows(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    while (resultSet.hasNext())
      rows.add(resultSet.next());
    return rows;
  }
}
