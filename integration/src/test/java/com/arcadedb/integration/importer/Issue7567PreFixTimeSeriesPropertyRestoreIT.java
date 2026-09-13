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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7567, the restore side. {@code CREATE PROPERTY} on a TIMESERIES type and
 * {@code ALTER PROPERTY ... CUSTOM role} are now refused, and a database written before that was true can carry
 * both. Its JSONL export carries them too, and the importer restores a type's properties through exactly the API
 * that now refuses them - so a restore that simply passed the export through would fail on precisely the databases
 * that have the defect, turning a usable backup into an unusable one.
 * <p>
 * The importer therefore drops both on the way in, with a warning, and finishes the restore. The stray property
 * never held a value (no exported sample carries one - the exporter writes the declared columns) and the CUSTOM
 * {@code role} never meant anything, so nothing is lost with them.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7567PreFixTimeSeriesPropertyRestoreIT {
  private static final String SOURCE_PATH = "target/databases/issue7567-jsonl-source";
  private static final String TARGET_PATH = "target/databases/issue7567-jsonl-target";
  private static final String FILE        = "target/issue7567-jsonl.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void aPreFixExportStillRestores() throws Exception {
    createSourceDatabase();
    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    rewriteSchemaLineAsWrittenBeforeTheFix();

    new Importer(("-url " + new File(FILE).getAbsolutePath() + " -database " + TARGET_PATH
        + " -forceDatabaseCreate true").split(" ")).load();

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final DocumentType restored = target.getSchema().getType("Sensor");
      assertThat(restored).isInstanceOf(LocalTimeSeriesType.class);

      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) restored;
      assertThat(tsType.getTsColumnNames()).containsExactly("ts", "host", "value");

      assertThat(restored.existsProperty("humidity"))
          .as("a property no column backs must not be restored onto a TIMESERIES type").isFalse();
      assertThat(restored.getProperty("value").getCustomValue("role"))
          .as("the meaningless CUSTOM role must be dropped").isNull();
      assertThat(restored.getProperty("value").getCustomValue("unit"))
          .as("every other CUSTOM key is still restored").isEqualTo("celsius");

      assertThat(target.query("sql", "SELECT count(*) AS c FROM Sensor").next().<Number>getProperty("c").longValue())
          .isEqualTo(3L);
    }
  }

  private void createSourceDatabase() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      source.command("sql", "ALTER PROPERTY Sensor.value CUSTOM unit = 'celsius'");

      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) source.getSchema().getType("Sensor");
      source.begin();
      tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L, 3_000L },
          new Object[] { "h1", "h1", "h2" },
          new Object[] { 1.5, 2.5, 3.5 });
      source.commit();
    }
  }

  /**
   * Puts back into the export exactly what a pre-fix database would have carried: a property on the TIMESERIES type
   * that no column backs, and a CUSTOM {@code role} on a column that does. Both are injected rather than created
   * through the API, because the API no longer produces either.
   */
  private void rewriteSchemaLineAsWrittenBeforeTheFix() throws Exception {
    final List<String> lines = new ArrayList<>();
    boolean patched = false;
    for (final String line : readExportedLines()) {
      final JSONObject json = new JSONObject(line);
      if ("schema".equals(json.getString("t"))) {
        final JSONObject sensor = json.getJSONObject("c").getJSONObject("types").getJSONObject("Sensor");
        final JSONObject properties = sensor.getJSONObject("properties");
        properties.put("humidity", new JSONObject().put("type", "DOUBLE"));
        properties.getJSONObject("value").getJSONObject("custom").put("role", "TAG");
        lines.add(json.toString());
        patched = true;
      } else
        lines.add(line);
    }
    assertThat(patched).as("the export must carry a schema line to patch").isTrue();
    writeExportedLines(lines);
  }

  private void writeExportedLines(final List<String> lines) throws Exception {
    try (final Writer writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(FILE)),
        StandardCharsets.UTF_8)) {
      for (final String line : lines)
        writer.write(line + "\n");
    }
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
