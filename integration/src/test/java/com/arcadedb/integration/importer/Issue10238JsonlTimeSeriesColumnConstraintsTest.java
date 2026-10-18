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
import com.arcadedb.schema.Property;
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

/**
 * Issue #10238: the constraints of a TIMESERIES column are enforced by every write path, so a JSONL restore has to bring them back.
 * The importer restored only the CUSTOM metadata of a column, whose property the type's builder creates bare, and a restored type
 * accepted what the source refused. The constraints now travel, and the import holds its own samples to them under the row-error
 * policy; a DEFAULT or HIDDEN recorded by a database written before they were refused on such a column is dropped with a warning,
 * so the backup still restores.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10238JsonlTimeSeriesColumnConstraintsTest {
  private static final String SOURCE_PATH = "target/databases/issue10238-jsonl-source";
  private static final String TARGET_PATH = "target/databases/issue10238-jsonl-target";
  private static final String EXPORT      = "target/issue10238-export.jsonl.tgz";
  private static final String EDITED      = "target/issue10238-edited.jsonl.tgz";

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
  void theColumnConstraintsAreRestoredAndHoldTheImportedSamples() throws Exception {
    export();
    // sample layout is the engine row: [ts, host, v]
    edit(json -> {
      if ("ts".equals(json.getString("t"))) {
        final JSONArray samples = json.getJSONObject("c").getJSONArray("s");
        for (int i = 0; i < samples.length(); i++)
          if (samples.getJSONArray(i).getLong(0) == TS + 1_000)
            samples.getJSONArray(i).put(2, 500.0d); // over the MAX the source declared
      }
    });

    final Map<String, Object> report = new Importer(importArguments(true)).load();
    assertThat(report).containsEntry("errors", 1L).containsEntry("createdTimeSeriesSamples", 1L);

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final LocalTimeSeriesType type = (LocalTimeSeriesType) target.getSchema().getType("T");
      final Property v = type.getProperty("v");
      assertThat(v.getMin()).isEqualTo("0");
      assertThat(v.getMax()).isEqualTo("100");
      assertThat(type.getProperty("host").getRegexp()).isEqualTo("[a-z]+");
      assertThat(type.getProperty("host").isMandatory()).isTrue();
      assertThat(type.getProperty("v").isNotNull()).isTrue();

      assertThat(type.getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)).extracting(row -> ((Number) row[0]).longValue())
          .containsExactly(TS);
    }
  }

  /** An export of a database written before DEFAULT and HIDDEN were refused on a TIMESERIES column still restores. */
  @Test
  void aDefaultOrHiddenFromAnOlderExportIsDroppedAndTheRestoreCompletes() throws Exception {
    export();
    edit(json -> {
      if ("schema".equals(json.getString("t"))) {
        final JSONObject properties = json.getJSONObject("c").getJSONObject("types").getJSONObject("T").getJSONObject("properties");
        properties.getJSONObject("v").put("default", 42);
        properties.getJSONObject("host").put("hidden", true);
      }
    });

    final Map<String, Object> report = new Importer(importArguments(false)).load();
    assertThat(report).containsEntry("createdTimeSeriesSamples", 2L).doesNotContainKey("errors");
    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final LocalTimeSeriesType type = (LocalTimeSeriesType) target.getSchema().getType("T");
      assertThat(type.getProperty("v").getDefaultValue()).isNull();
      assertThat(type.getProperty("host").isHidden()).isFalse();
      assertThat(type.getProperty("v").getMax()).isEqualTo("100");
    }
  }

  private void export() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");
      source.command("sql", "ALTER PROPERTY T.v MIN 0");
      source.command("sql", "ALTER PROPERTY T.v MAX 100");
      source.command("sql", "ALTER PROPERTY T.v NOTNULL true");
      source.command("sql", "ALTER PROPERTY T.host REGEXP '[a-z]+'");
      source.command("sql", "ALTER PROPERTY T.host MANDATORY true");
      source.command("sql", "INSERT INTO T SET ts = " + TS + ", host = 'a', v = 1.5");
      source.command("sql", "INSERT INTO T SET ts = " + (TS + 1_000) + ", host = 'b', v = 2.5");
    }
    new Exporter(("-f " + EXPORT + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(EXPORT).exists()).isTrue();
  }

  /** Rewrites the export line by line with {@code edit}. */
  private void edit(final Consumer<JSONObject> edit) throws Exception {
    final List<String> lines = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(EXPORT)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        final JSONObject json = new JSONObject(line);
        edit.accept(json);
        lines.add(json.toString());
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
}
