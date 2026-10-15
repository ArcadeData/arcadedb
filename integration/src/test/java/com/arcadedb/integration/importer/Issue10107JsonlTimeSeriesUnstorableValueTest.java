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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.Exporter;
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
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10107: {@code JsonlImporterFormat.loadTimeSeriesSamples} was the third caller of {@code TimeSeriesEngine.appendBatch}
 * and the only one that handed the engine the raw values of its source. A JSONL sample holding TEXT for a typed column - what a
 * hand-built or third-party file produces for an INTEGER, BOOLEAN or DATE tag - reached {@code ObjectColumnsRowSource} and
 * threw {@code Cannot convert a value of type String to the INTEGER column ...}, wrapped in {@code Failed to append timeseries
 * samples}: the whole chunk was lost and the import aborted without naming the row.
 * <p>
 * It now converts through {@code TimeSeriesStorableRows}, the conversion the line protocol, gRPC and the Prometheus remote write
 * share: text a column can store is stored as the value it names, and a value no column can store is refused by name
 * ({@code <type>.<column> (tag|field): <reason>}) under the importer's own row-error policy - the import fails in the default
 * mode, and with {@code -onRowError skip} that sample alone is dropped and counted while the rest of its chunk lands.
 * <p>
 * The hand-built file is an export of a real database with some values of its {@code ts} line rewritten as text, so the rest of
 * the file is exactly what the exporter writes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10107JsonlTimeSeriesUnstorableValueTest {
  private static final String SOURCE_PATH = "target/databases/issue10107-jsonl-source";
  private static final String TARGET_PATH = "target/databases/issue10107-jsonl-target";
  private static final String EXPORT      = "target/issue10107-export.jsonl.tgz";
  private static final String EDITED      = "target/issue10107-edited.jsonl.tgz";

  private static final long TS = 1_700_000_000_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(EXPORT).delete();
    new File(EDITED).delete();
  }

  @Test
  void textAColumnCanStoreIsStoredAsTheValueItNames() throws Exception {
    exportThreeSamples();
    // sample layout is the engine row: [ts, host, port, up, v]
    rewrite(sample -> {
      final long timestamp = sample.getLong(0);
      if (timestamp == TS) {
        sample.put(2, "80");
        // THE TIMESTAMP IS A COLUMN LIKE THE OTHERS: TEXT OF DIGITS IS THE INSTANT IT NAMES
        sample.put(0, String.valueOf(TS));
      }
      if (timestamp == TS + 1_000)
        sample.put(3, "false");
      if (timestamp == TS + 2_000)
        sample.put(4, "2.75");
    });

    final Map<String, Object> report = new Importer(importArguments(false)).load();

    // THE REPORT CARRIES "errors" ONLY WHEN THERE WAS ONE
    assertThat(report).containsEntry("createdTimeSeriesSamples", 3L).doesNotContainKey("errors");
    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final List<Object[]> rows = rowsOf(target);
      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0)[2]).intValue()).as("'80' in the INTEGER tag").isEqualTo(80);
      assertThat(rows.get(1)[3]).as("'false' in the BOOLEAN tag").isEqualTo(false);
      assertThat(rows.get(2)[3]).as("a value that was already typed is untouched").isEqualTo(true);
      // THE PARSE THE IMPORTER USED TO DO ITSELF FOR A DOUBLE NOW BELONGS TO THE SHARED CONVERSION
      assertThat(((Number) rows.get(2)[4]).doubleValue()).as("'2.75' in the DOUBLE field").isEqualTo(2.75);
    }
  }

  @Test
  void aValueNoColumnCanStoreFailsTheImportAndNamesTheColumn() throws Exception {
    exportThreeSamples();
    rewrite(sample -> {
      if (sample.getLong(0) == TS + 2_000)
        sample.put(2, "eighty-two");
    });

    assertThatThrownBy(() -> new Importer(importArguments(false)).load()).isInstanceOf(ImportException.class)
        .satisfies(e -> assertThat(messagesOf(e)).contains("T.port (tag)"));
  }

  @Test
  void withSkipOnRowErrorOnlyTheRefusedSampleIsDroppedAndCounted() throws Exception {
    exportThreeSamples();
    rewrite(sample -> {
      if (sample.getLong(0) == TS + 2_000)
        sample.put(2, "eighty-two");
    });

    final Map<String, Object> report = new Importer(importArguments(true)).load();

    assertThat(report).as("one sample refused, the two before it in the same chunk kept").containsEntry("errors", 1L)
        .containsEntry("createdTimeSeriesSamples", 2L);
    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final List<Object[]> rows = rowsOf(target);
      assertThat(rows).hasSize(2);
      assertThat(rows).extracting(row -> ((Number) row[0]).longValue()).containsExactly(TS, TS + 1_000);
    }
  }

  /** A timestamp the column cannot store refuses its sample alone, like any other value: the chunk is not lost with it. */
  @Test
  void aTimestampNoColumnCanStoreRefusesOnlyItsSample() throws Exception {
    exportThreeSamples();
    rewrite(sample -> {
      if (sample.getLong(0) == TS + 1_000)
        sample.put(0, "not-a-time");
    });

    assertThatThrownBy(() -> new Importer(importArguments(false)).load()).isInstanceOf(ImportException.class)
        .satisfies(e -> assertThat(messagesOf(e)).contains("T.ts (timestamp)"));

    final Map<String, Object> report = new Importer(importArguments(true)).load();

    assertThat(report).as("one sample refused, the other two of its chunk kept").containsEntry("errors", 1L)
        .containsEntry("createdTimeSeriesSamples", 2L);
    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      assertThat(rowsOf(target)).extracting(row -> ((Number) row[0]).longValue()).containsExactly(TS, TS + 2_000);
    }
  }

  /** A sample shorter than the type is a malformed chunk, refused by name before a value is read, whatever the row-error policy. */
  @Test
  void aSampleShorterThanTheTypeIsRefusedByNameBeforeAnythingIsRead() throws Exception {
    exportThreeSamples();
    rewrite(sample -> {
      if (sample.getLong(0) == TS + 1_000)
        sample.remove(4);
    });

    assertThatThrownBy(() -> new Importer(importArguments(false)).load()).isInstanceOf(ImportException.class)
        .satisfies(e -> assertThat(messagesOf(e)).contains("TIMESERIES sample 1 of type 'T' has 4 value(s)"));
  }

  private void exportThreeSamples() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE TIMESERIES TYPE T TAGS (host STRING, port INTEGER, up BOOLEAN) TIMESTAMP ts FIELDS (v DOUBLE)");
      source.begin();
      source.command("sql", "INSERT INTO T SET ts = " + TS + ", host = 'a', port = 1, up = true, v = 1.5");
      source.command("sql", "INSERT INTO T SET ts = " + (TS + 1_000) + ", host = 'b', port = 2, up = true, v = 2.5");
      source.command("sql", "INSERT INTO T SET ts = " + (TS + 2_000) + ", host = 'c', port = 3, up = true, v = 3.5");
      source.commit();
    }
    new Exporter(("-f " + EXPORT + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(EXPORT).exists()).isTrue();
  }

  /** Rewrites every sample of the export's {@code ts} line with {@code edit}, leaving every other line as exported. */
  private void rewrite(final Consumer<JSONArray> edit) throws Exception {
    final List<String> lines = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(EXPORT)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        final JSONObject json = new JSONObject(line);
        if ("ts".equals(json.getString("t"))) {
          final JSONArray samples = json.getJSONObject("c").getJSONArray("s");
          for (int i = 0; i < samples.length(); i++)
            edit.accept(samples.getJSONArray(i));
          line = json.toString();
        }
        lines.add(line);
      }
    }
    try (final Writer writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(EDITED)), StandardCharsets.UTF_8)) {
      for (final String line : lines)
        writer.write(line + "\n");
    }
  }

  private static String[] importArguments(final boolean skipOnRowError) {
    return ("-url " + new File(EDITED).getAbsolutePath() + " -database " + TARGET_PATH + " -forceDatabaseCreate true"
        + (skipOnRowError ? " -onRowError skip" : "")).split(" ");
  }

  private static List<Object[]> rowsOf(final Database target) throws Exception {
    return ((LocalTimeSeriesType) target.getSchema().getType("T")).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }

  private static String messagesOf(final Throwable throwable) {
    final StringBuilder all = new StringBuilder();
    for (Throwable t = throwable; t != null; t = t.getCause())
      all.append(t.getMessage()).append('\n');
    return all.toString();
  }
}
