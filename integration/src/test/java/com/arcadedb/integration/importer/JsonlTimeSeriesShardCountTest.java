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
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
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
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shard count of a TIMESERIES type read from a JSONL export goes through the same checks as one declared in SQL or by the
 * builder: an export that names more shards than a type can have is refused before the type is created, and the shard count a
 * type was exported with is restored as it was.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class JsonlTimeSeriesShardCountTest {
  private static final String SOURCE_PATH = "target/databases/jsonl-shard-count-source";
  private static final String TARGET_PATH = "target/databases/jsonl-shard-count-target";
  private static final String EXPORT      = "target/jsonl-shard-count-export.jsonl.tgz";
  private static final String EDITED      = "target/jsonl-shard-count-edited.jsonl.tgz";

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
  void anExportNamingMoreShardsThanTheMaximumIsRefusedAndNoTypeIsCreated() throws Exception {
    export();
    edit(json -> setShardCount(json, TimeSeriesTypeBuilder.MAX_SHARDS + 1));

    assertThatThrownBy(() -> new Importer(importArguments()).load()).hasStackTraceContaining("shard count");

    assertThat(typeNamesOfTarget()).doesNotContain("T");
  }

  @Test
  void anExportNamingTheLargestIntegerAsShardCountIsRefusedAndNoTypeIsCreated() throws Exception {
    export();
    edit(json -> setShardCount(json, Integer.MAX_VALUE));

    assertThatThrownBy(() -> new Importer(importArguments()).load()).hasStackTraceContaining("shard count");

    assertThat(typeNamesOfTarget()).doesNotContain("T");
  }

  @Test
  void theShardCountOfAnExportWithinTheMaximumIsRestored() throws Exception {
    export();
    edit(json -> {
    });

    new Importer(importArguments()).load();

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      assertThat(((TimeSeriesType) target.getSchema().getType("T")).getShardCount()).isEqualTo(2);
    }
  }

  private List<String> typeNamesOfTarget() {
    if (!new File(TARGET_PATH).exists())
      return List.of();
    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      return target.getSchema().getTypes().stream().map(type -> type.getName()).toList();
    }
  }

  private static void setShardCount(final JSONObject line, final int shardCount) {
    if ("schema".equals(line.getString("t")))
      line.getJSONObject("c").getJSONObject("types").getJSONObject("T").put("shardCount", shardCount);
  }

  private void export() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
      source.command("sql", "INSERT INTO T SET ts = " + TS + ", host = 'a', v = 1.5");
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

  private static String[] importArguments() {
    return ("-url " + new File(EDITED).getAbsolutePath() + " -database " + TARGET_PATH + " -forceDatabaseCreate true").split(" ");
  }
}
