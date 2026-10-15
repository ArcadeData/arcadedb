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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.FullSchemaLoadReason;
import com.arcadedb.schema.Type;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.SchemaFullLoadStats;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10020: a replica that applies a schema entry the incremental refresh cannot express silently ran a full
 * {@code LocalSchema.load()} on its Ratis apply thread - no reason, no duration, no metric - so an operator seeing
 * apply-lag spikes after DDL had nothing to tie them to. The fallback now says why, how long it took, and counts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10020SchemaFullLoadFallbackTest {
  private static final String DB = "issue10020";

  @TempDir
  private Path               serverDir;
  private LocalDatabase      database;
  private CapturingTestLogger logger;

  @BeforeEach
  void setUp() {
    database = (LocalDatabase) new DatabaseFactory(serverDir.resolve(DB).toString()).create();
    database.transaction(() -> database.getSchema().createDocumentType("Seed").createProperty("id", Type.STRING));
    logger = CapturingTestLogger.install();
  }

  @AfterEach
  void tearDown() {
    logger.uninstall();
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.reset();
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void anEntryTheIncrementalRefreshCanExpressTakesNoFullLoad() {
    final DatabaseBoundStateMachine stateMachine = new DatabaseBoundStateMachine(database);

    stateMachine.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry(Collections.emptyMap())), 7L, false);

    final SchemaFullLoadStats stats = stateMachine.getSchemaFullLoadRecorder().snapshot();
    for (final FullSchemaLoadReason reason : FullSchemaLoadReason.values())
      assertThat(stats.fallbacks(reason)).as("fallbacks for %s", reason).isZero();
    assertThat(logger.countFormattedContaining("FULL schema load")).isZero();
  }

  @Test
  void switchingTheIncrementalRefreshOffIsReportedAsSuch() {
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
    final DatabaseBoundStateMachine stateMachine = new DatabaseBoundStateMachine(database);

    stateMachine.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry(Collections.emptyMap())), 7L, false);

    final SchemaFullLoadStats stats = stateMachine.getSchemaFullLoadRecorder().snapshot();
    assertThat(stats.fallbacks(FullSchemaLoadReason.INCREMENTAL_DISABLED)).isEqualTo(1L);
    assertThat(stats.completed(FullSchemaLoadReason.INCREMENTAL_DISABLED)).isEqualTo(1L);
    assertThat(stats.totalNanos(FullSchemaLoadReason.INCREMENTAL_DISABLED)).isPositive();
    assertThat(totalFallbacks(stats)).as("no other reason is counted").isEqualTo(1L);
  }

  @Test
  void aRenameEntryIsReportedWithItsReasonItsDatabaseItsEntryIndexAndItsDuration() {
    final DatabaseBoundStateMachine stateMachine = new DatabaseBoundStateMachine(database);
    final int bucketFileId = database.getSchema().getType("Seed").getBuckets(false).get(0).getFileId();
    // A rename onto the name the file already carries is the idempotent replay of an earlier move: it moves nothing, but
    // an entry that carries renames is exactly what the incremental refresh declines
    final Map<Integer, String> renames = Map.of(bucketFileId, ((DatabaseInternal) database).getFileManager().getFile(
        bucketFileId).getComponentName());

    stateMachine.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry(renames)), 42L, false);

    final SchemaFullLoadStats stats = stateMachine.getSchemaFullLoadRecorder().snapshot();
    assertThat(stats.fallbacks(FullSchemaLoadReason.FILE_RENAMED)).isEqualTo(1L);
    assertThat(stats.completed(FullSchemaLoadReason.FILE_RENAMED)).isEqualTo(1L);
    assertThat(stats.totalNanos(FullSchemaLoadReason.FILE_RENAMED)).isPositive();
    assertThat(totalFallbacks(stats)).isEqualTo(1L);

    final List<String> infos = logger.formattedAt(Level.INFO);
    assertThat(infos).anySatisfy(line -> assertThat(line)
        .contains("FULL schema load")
        .contains("'" + DB + "'")
        .contains("entry 42")
        .contains(FullSchemaLoadReason.FILE_RENAMED.getTag())
        .containsPattern("in \\d+ ms"));
  }

  @Test
  void everyFallbackIsCountedAndTheDurationsAccumulate() {
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);
    final DatabaseBoundStateMachine stateMachine = new DatabaseBoundStateMachine(database);

    for (int i = 0; i < 3; i++)
      stateMachine.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry(Collections.emptyMap())), 10L + i, false);

    final SchemaFullLoadStats stats = stateMachine.getSchemaFullLoadRecorder().snapshot();
    assertThat(stats.fallbacks(FullSchemaLoadReason.INCREMENTAL_DISABLED)).isEqualTo(3L);
    assertThat(stats.completed(FullSchemaLoadReason.INCREMENTAL_DISABLED)).isEqualTo(3L);
  }

  @Test
  void aRecorderSharedWithTheServerSurvivesTheStateMachineBeingReplaced() {
    // RaftHAServer builds a new state machine on every in-place restart; the counters are cumulative since process start
    final SchemaFullLoadRecorder shared = new SchemaFullLoadRecorder();
    GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.setValue(false);

    final DatabaseBoundStateMachine first = new DatabaseBoundStateMachine(database);
    first.setSchemaFullLoadRecorder(shared);
    first.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry(Collections.emptyMap())), 1L, false);

    final DatabaseBoundStateMachine second = new DatabaseBoundStateMachine(database);
    second.setSchemaFullLoadRecorder(shared);
    second.applySchemaEntry(RaftLogEntryCodec.decode(schemaEntry(Collections.emptyMap())), 2L, false);

    assertThat(shared.snapshot().fallbacks(FullSchemaLoadReason.INCREMENTAL_DISABLED)).isEqualTo(2L);
  }

  private ByteString schemaEntry(final Map<Integer, String> filesToRename) {
    return RaftLogEntryCodec.encodeSchemaEntry(DB, database.getSchema().getEmbedded().toJSON().toString(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), false, Collections.emptyList(), null, filesToRename);
  }

  private static long totalFallbacks(final SchemaFullLoadStats stats) {
    long total = 0;
    for (final FullSchemaLoadReason reason : FullSchemaLoadReason.values())
      total += stats.fallbacks(reason);
    return total;
  }

  private static class DatabaseBoundStateMachine extends ArcadeStateMachine {
    private final DatabaseInternal database;

    DatabaseBoundStateMachine(final DatabaseInternal database) {
      this.database = database;
    }

    @Override
    DatabaseInternal databaseFor(final String databaseName) {
      return database;
    }
  }
}
