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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7899: the JSONL export writes a TIMESERIES sample in the layout the ENGINE hands it - timestamp first,
 * then the non-TIMESTAMP columns in schema order - while {@code JsonlImporterFormat.loadTimeSeriesSamples} read
 * the same array as if it followed the SCHEMA, taking the timestamp from the TIMESTAMP column's schema position.
 * The two descriptions coincide only when the TIMESTAMP column is declared first, and since issue #7702 the
 * grammar can spell a declaration where it is not.
 * <p>
 * Two shapes, because they fail differently and only one of them is loud:
 * <ul>
 *   <li>a STRING column before the timestamp: the restore died with a bare {@code ClassCastException} out of
 *       {@code loadTimeSeriesSamples}, so the export could not be restored at all;</li>
 *   <li>a NUMERIC column before the timestamp: no error at all. The timestamp was stored as that column's value
 *       and its value as the timestamp, and every later read - retention, compaction bucketing, downsampling,
 *       range scans, {@code latest}, PromQL - operated on a timestamp the data never had.</li>
 * </ul>
 * The assertions read the restored samples through {@code TimeSeriesEngine.query}, whose layout is timestamp
 * first by definition, so they cannot be satisfied by a read path that makes the mirror-image mistake.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7899">issue #7899</a>
 */
class Issue7899JsonlNonFirstTimestampRoundTripTest {
  private static final String SOURCE_PATH = "target/databases/issue7899-jsonl-source";
  private static final String TARGET_PATH = "target/databases/issue7899-jsonl-target";
  private static final String FILE        = "target/issue7899-jsonl.jsonl.tgz";

  private static final long TS = 1_700_000_000_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  /** The loud shape: a STRING TAG declared before the TIMESTAMP column. */
  @Test
  void aStringTagDeclaredBeforeTheTimestampRoundTrips() throws Exception {
    exportOf("CREATE TIMESERIES TYPE M TAGS (host STRING) TIMESTAMP ts FIELDS (v DOUBLE)",
        "INSERT INTO M SET ts = " + TS + ", host = 'srv-1', v = 42.5");

    restore();

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final LocalTimeSeriesType type = (LocalTimeSeriesType) target.getSchema().getType("M");
      final List<Object[]> rows = type.getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst()).as("the restored engine row, timestamp first by definition")
          .containsExactly(TS, "srv-1", 42.5);
    }
  }

  /** The silent shape: a LONG FIELD declared before the TIMESTAMP column - two numbers, no exception. */
  @Test
  void aNumericFieldDeclaredBeforeTheTimestampRoundTripsWithoutSwappingTheTimestamp() throws Exception {
    exportOf("CREATE TIMESERIES TYPE N FIELDS (seq LONG) TIMESTAMP ts FIELDS (v DOUBLE)",
        "INSERT INTO N SET ts = " + TS + ", seq = 7, v = 42.5");

    restore();

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final LocalTimeSeriesType type = (LocalTimeSeriesType) target.getSchema().getType("N");
      final List<Object[]> rows = type.getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).hasSize(1);
      final Object[] row = rows.getFirst();
      assertThat((long) row[0]).as("the restored timestamp, not the value of 'seq'").isEqualTo(TS);
      assertThat(((Number) row[1]).longValue()).as("'seq', not the timestamp").isEqualTo(7L);
      assertThat(row[2]).isEqualTo(42.5);
    }
  }

  /**
   * The wire format itself. The exporter emits the engine's layout, and the importer now reads it as such, so
   * an export written by an earlier build of a timestamp-FIRST type - byte-identical under either reading - is
   * still restored correctly and the fix needs no format-version bump.
   */
  @Test
  void theSampleArrayIsWrittenInEngineRowOrderTimestampFirst() throws Exception {
    exportOf("CREATE TIMESERIES TYPE M TAGS (host STRING) TIMESTAMP ts FIELDS (v DOUBLE)",
        "INSERT INTO M SET ts = " + TS + ", host = 'srv-1', v = 42.5");

    JSONArray sample = null;
    for (final String line : readExportedLines()) {
      final JSONObject json = new JSONObject(line);
      if ("ts".equals(json.getString("t")))
        sample = json.getJSONObject("c").getJSONArray("s").getJSONArray(0);
    }
    assertThat(sample).isNotNull();
    assertThat(sample.getLong(0)).as("position 0 is the timestamp, as every engine row has it").isEqualTo(TS);
    assertThat(sample.getString(1)).isEqualTo("srv-1");
    assertThat(sample.getDouble(2)).isEqualTo(42.5);
  }

  private void exportOf(final String ddl, final String... inserts) {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", ddl);
      source.begin();
      for (final String insert : inserts)
        source.command("sql", insert);
      source.commit();
    }
    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE).exists()).isTrue();
  }

  private void restore() throws Exception {
    new Importer(("-url " + new File(FILE).getAbsolutePath() + " -database " + TARGET_PATH
        + " -forceDatabaseCreate true").split(" ")).load();
  }

  private List<String> readExportedLines() throws Exception {
    final List<String> lines = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(FILE)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null)
        lines.add(line);
    }
    return lines;
  }
}
