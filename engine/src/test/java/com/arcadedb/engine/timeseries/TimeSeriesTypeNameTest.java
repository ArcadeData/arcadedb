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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The name of a TIMESERIES type becomes part of the file names of its engine (one mutable bucket and one sealed file per shard, and
 * the tag dictionary), so it has to address files of the database directory and nothing else. These tests pin that a name carrying
 * a path separator is refused when the type is created, renamed to, or read back from a stored schema, that no file is written
 * below the database directory, and that every other name keeps working.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TimeSeriesTypeNameTest extends TestHelper {
  private File subdirectory;

  @Override
  protected void beginTest() {
    // a directory inside the database one: the files of a type must be written beside the others, never below it
    subdirectory = new File(getDatabasePath(), "group");
    assertThat(subdirectory.mkdirs()).isTrue();
  }

  @Test
  void aNameWithAForwardSlashIsRefusedAtCreation() {
    final String name = "group/Readings";

    assertThatThrownBy(
        () -> database.command("sql", "CREATE TIMESERIES TYPE `" + name + "` TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("Invalid TIMESERIES type name");

    assertThat(database.getSchema().existsType(name)).isFalse();
    assertThat(subdirectory.list()).isEmpty();
  }

  @Test
  void aNameWithABackslashIsRefusedAtCreation() {
    final String name = "group\\Readings";

    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts").withTag("host", Type.STRING)
        .withField("v", Type.DOUBLE).withShards(2).create()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Invalid TIMESERIES type name");

    assertThat(database.getSchema().existsType(name)).isFalse();
    assertThat(subdirectory.list()).isEmpty();
    assertThat(new File(getDatabasePath()).list((dir, fileName) -> fileName.contains("Readings"))).isEmpty();
  }

  @Test
  void aNameWithAControlCharacterIsRefusedAtCreation() {
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("Read\u0000ings").withTimestamp("ts")
        .withField("v", Type.DOUBLE).create()).isInstanceOf(SchemaException.class).hasMessageContaining("Invalid TIMESERIES type name");
    assertThat(database.getSchema().existsType("Read\u0000ings")).isFalse();
  }

  @Test
  void aRenameToANameWithAPathSeparatorIsRefusedAndTheSamplesStay() {
    database.command("sql", "CREATE TIMESERIES TYPE Readings TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    database.transaction(() -> database.command("sql", "INSERT INTO Readings SET ts = 1000, host = 'a', v = 1.5"));
    final String[] before = readingsFileNames();

    final String forward = "group/Moved";
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Readings NAME `" + forward + "`")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Invalid TIMESERIES type name");
    // a backslash is written doubled inside a quoted identifier
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Readings NAME `a\\\\b`")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Invalid TIMESERIES type name");

    assertThat(subdirectory.list()).isEmpty();
    assertThat(readingsFileNames()).containsExactlyInAnyOrder(before);
    assertThat(database.getSchema().existsType("Readings")).isTrue();
    assertThat(database.getSchema().existsType(forward)).isFalse();
    assertThat(count("Readings")).isEqualTo(1L);

    // an ordinary rename of the same type still moves the files and keeps the samples
    database.command("sql", "ALTER TYPE Readings NAME Readings2");
    assertThat(database.getSchema().existsType("Readings")).isFalse();
    assertThat(count("Readings2")).isEqualTo(1L);
    assertThat(subdirectory.list()).isEmpty();
  }

  @Test
  void ordinaryNamesKeepWorkingThroughCreateRenameAndDrop() {
    for (final String name : List.of("weather.temp", "cpu-load", "Città", "_under_score_", "dots..inside")) {
      database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts").withTag("host", Type.STRING).withField("v", Type.DOUBLE)
          .withShards(2).create();
      database.transaction(() -> database.command("sql", "INSERT INTO `" + name + "` SET ts = 1000, host = 'a', v = 1.5"));
      assertThat(count("`" + name + "`")).isEqualTo(1L);

      final String renamed = name + "_renamed";
      database.command("sql", "ALTER TYPE `" + name + "` NAME `" + renamed + "`");
      assertThat(database.getSchema().existsType(name)).isFalse();
      assertThat(count("`" + renamed + "`")).isEqualTo(1L);

      database.getSchema().dropType(renamed);
      assertThat(database.getSchema().existsType(renamed)).isFalse();
      assertThat(new File(getDatabasePath()).list((dir, fileName) -> fileName.startsWith(name))).isEmpty();
    }
    assertThat(subdirectory.list()).isEmpty();
  }

  @Test
  void aStoredSchemaNamingATypeWithAPathSeparatorOpensItWithoutStorage() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Honest TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    database.transaction(() -> database.command("sql", "INSERT INTO Honest SET ts = 1000, host = 'a', v = 1.5"));
    database.close();

    final File schemaFile = new File(getDatabasePath(), "schema.json");
    final String original = Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8);
    try {
      final JSONObject root = new JSONObject(original);
      final JSONObject types = root.getJSONObject("types");
      types.put("group/Honest", types.getJSONObject("Honest"));
      types.remove("Honest");
      Files.writeString(schemaFile.toPath(), root.toString(2), StandardCharsets.UTF_8);

      // the type stays in the schema, as one whose engine could not start does, and no file is created for it
      database = factory.open();
      final LocalTimeSeriesType stored = (LocalTimeSeriesType) database.getSchema().getType("group/Honest");
      assertThat(stored.isEngineAvailable()).isFalse();
      assertThat(stored.getEngineUnavailableReason()).contains("Invalid TIMESERIES type name");
      assertThat(subdirectory.list()).isEmpty();

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE `group/Honest` NAME Honest2")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Invalid TIMESERIES type name");

      database.getSchema().dropType("group/Honest");
      assertThat(database.getSchema().existsType("group/Honest")).isFalse();
      assertThat(subdirectory.list()).isEmpty();
      database.close();
    } finally {
      Files.writeString(schemaFile.toPath(), original, StandardCharsets.UTF_8);
    }

    reopenDatabase();
    assertThat(database.getSchema().existsType("Honest")).isTrue();
    assertThat(count("Honest")).isEqualTo(1L);
    assertThat(subdirectory.list()).isEmpty();
  }

  private long count(final String type) {
    try (final ResultSet resultSet = database.query("sql", "SELECT count() AS c FROM " + type)) {
      return resultSet.next().<Number>getProperty("c").longValue();
    }
  }

  /** The files of the database directory that belong to the type {@code Readings}. */
  private String[] readingsFileNames() {
    final String[] names = new File(getDatabasePath()).list((dir, fileName) -> fileName.startsWith("Readings"));
    return names == null ? new String[0] : names;
  }
}
