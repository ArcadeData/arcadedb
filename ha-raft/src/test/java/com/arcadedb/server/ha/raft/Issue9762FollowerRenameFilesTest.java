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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9762: the follower half of a replicated type rename, {@code ArcadeStateMachine.renameFiles}, driven against a real
 * database: it moves the file the way the leader did, skips a file that already carries the name (a replay), and refuses a
 * file id it does not have rather than load a schema naming files that are not there.
 */
class Issue9762FollowerRenameFilesTest {

  @TempDir
  Path dir;

  private DatabaseFactory  factory;
  private DatabaseInternal database;

  @BeforeEach
  void open() {
    factory = new DatabaseFactory(dir.resolve("db").toString());
    database = (DatabaseInternal) factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE BUCKET R_extra");
    database.transaction(() -> database.newDocument("R").set("id", 1).save());
  }

  @AfterEach
  void close() {
    if (database != null && database.isOpen())
      database.drop();
    factory.close();
  }

  @Test
  void aFileIsMovedUnderItsNewComponentName() throws Exception {
    final Bucket bucket = database.getSchema().getType("R").getBuckets(false).getFirst();
    final String newName = "R2" + bucket.getName().substring(1);

    ArcadeStateMachine.renameFiles(database, Map.of(bucket.getFileId(), newName));

    assertThat(database.getFileManager().getFile(bucket.getFileId()).getComponentName()).isEqualTo(newName);
    assertThat(new File(database.getDatabasePath()).list()).anyMatch(n -> n.startsWith(newName + "."));
    assertThat(bucket.getName()).as("the component follows its file").isEqualTo(newName);
  }

  @Test
  void aReplayOfAnAppliedRenameIsSkipped() throws Exception {
    final Bucket bucket = database.getSchema().getType("R").getBuckets(false).getFirst();
    final String newName = "R2" + bucket.getName().substring(1);
    ArcadeStateMachine.renameFiles(database, Map.of(bucket.getFileId(), newName));

    ArcadeStateMachine.renameFiles(database, Map.of(bucket.getFileId(), newName));

    assertThat(database.getFileManager().getFile(bucket.getFileId()).getComponentName()).isEqualTo(newName);
  }

  /** A crash part way through an apply: the replay finds the first file already moved and moves the rest. */
  @Test
  void aReplayAfterAPartialApplyMovesWhatIsLeft() throws Exception {
    database.command("sql", "ALTER TYPE R BUCKET +R_extra");
    final Bucket first = database.getSchema().getType("R").getBuckets(false).getFirst();
    final Bucket second = database.getSchema().getBucketByName("R_extra");
    final String firstNewName = "R2" + first.getName().substring(1);
    final Map<Integer, String> renames = new LinkedHashMap<>();
    renames.put(first.getFileId(), firstNewName);
    renames.put(second.getFileId(), "R2_extra");
    ArcadeStateMachine.renameFiles(database, Map.of(first.getFileId(), firstNewName));

    ArcadeStateMachine.renameFiles(database, renames);

    assertThat(database.getFileManager().getFile(first.getFileId()).getComponentName()).isEqualTo(firstNewName);
    assertThat(database.getFileManager().getFile(second.getFileId()).getComponentName()).isEqualTo("R2_extra");
  }

  /**
   * Issue #10749: the rename of a TIMESERIES type records the components of its engine, and a follower moves them by file id like
   * any other. The sealed blocks of a shard are in a file beside its mutable bucket that is not a component, so the move of the
   * bucket has to carry it: otherwise the follower, loading the schema that names the new type, opens an empty sealed store under
   * the new name and every sealed sample of the type is gone there. Applied twice, as a replay after a crash would.
   */
  @Test
  void theEngineFilesOfATimeSeriesTypeMoveWithTheirSealedBlocks() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Metric TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    for (int i = 0; i < 6; i++)
      database.command("sql", "INSERT INTO Metric SET ts = ?, host = ?, v = ?", 1000L * (i + 1), "h" + (i % 2), (double) i);
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Metric")).getEngine();
    engine.compactAll();
    database.command("sql", "INSERT INTO Metric SET ts = 9000, host = 'h1', v = 9.0");

    final Map<Integer, String> renames = new LinkedHashMap<>();
    for (int i = 0; i < engine.getShardCount(); i++)
      renames.put(engine.getShard(i).getMutableBucket().getFileId(), "Measure_shard_" + i);
    renames.put(engine.getTagDictionary().getFileId(), "Measure_tags");

    ArcadeStateMachine.renameFiles(database, renames);
    ArcadeStateMachine.renameFiles(database, renames);

    final File directory = new File(database.getDatabasePath());
    assertThat(directory.list()).noneMatch(n -> n.startsWith("Metric_"))
        .contains("Measure_shard_0.ts.sealed", "Measure_shard_1.ts.sealed");

    // the schema the leader published names the type by its new name: loaded as the follower loads it, from the file
    database.close();
    final File schemaFile = new File(directory, "schema.json");
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath()));
    final JSONObject types = schema.getJSONObject("types");
    types.put("Measure", types.remove("Metric"));
    Files.writeString(schemaFile.toPath(), schema.toString());
    database = (DatabaseInternal) factory.open();

    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM Measure")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(7);
    }
  }

  @Test
  void aFileIdThisNodeDoesNotHaveIsRefusedAsADivergence() {
    assertThatThrownBy(() -> ArcadeStateMachine.renameFiles(database, Map.of(9_999, "Nowhere_0")))
        .isInstanceOf(SchemaException.class).hasMessageContaining("diverged");
  }
}
