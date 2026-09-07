/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.engine.Component;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6988.
 * <p>
 * {@code ArcadeStateMachine.applySchemaEntry} used to end every committed DDL entry with a full
 * {@code LocalSchema.load()}, which drops every {@code Component} in the database and re-instantiates one per file,
 * reading page 0 of each. The cost of building a schema was therefore O(entries x total files) - quadratic in the
 * number of types, all of it serialized on the single Ratis apply thread (issue #6982: 1209 types took about
 * 2h53m to reach the followers).
 * <p>
 * The regression is pinned by IDENTITY, not by elapsed time: a component the entry did not touch must survive the
 * apply as the very same instance. Before the fix every applied entry replaced all of them, so the assertion below
 * fails on the first extra type created; after it, the instance count the follower pays per entry is bounded by
 * what that entry actually changed. An elapsed-time budget could express the same claim only at a type count that
 * would take hours to build, and would then be at the mercy of whatever else the machine is running.
 */
class Issue6988IncrementalSchemaApplyIT extends BaseRaftHATest {

  private static final int TYPE_COUNT = 40;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void followerKeepsUntouchedComponentsAcrossManySchemaEntries() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Every DDL lambda below is idempotent on purpose. A committed Raft entry whose acknowledgement is lost - a
    // leadership transfer mid-commit is enough, and cluster formation is exactly when that happens - makes
    // LocalDatabase.transaction() re-run the lambda, and schema changes are not rolled back with the records.
    leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateDocumentType("Issue6988Anchor"));
    assertClusterConsistency();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final LocalSchema replicaSchema = replicaDb.getSchema().getEmbedded();

    final int anchorFileId = replicaSchema.getType("Issue6988Anchor").getBuckets(false).getFirst().getFileId();
    final Component anchorBefore = replicaSchema.getFileByIdIfExists(anchorFileId);
    assertThat(anchorBefore).as("the follower must have registered the anchor type's bucket").isNotNull();

    // One transaction per type, so each one becomes its own committed SCHEMA_ENTRY on the follower.
    for (int i = 0; i < TYPE_COUNT; i++) {
      final int index = i;
      leaderDb.transaction(() -> {
        final DocumentType type = leaderDb.getSchema().getOrCreateDocumentType("Issue6988Type_" + index);
        type.getOrCreateProperty("id", Type.STRING);
        type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      });
    }

    assertClusterConsistency();

    for (int i = 0; i < TYPE_COUNT; i++) {
      final String typeName = "Issue6988Type_" + i;
      assertThat(replicaDb.getSchema().existsType(typeName)).as("replica should have type %s", typeName).isTrue();
      assertThat(replicaDb.getSchema().getType(typeName).existsProperty("id")).isTrue();
      assertThat(replicaDb.getSchema().getType(typeName).getAllIndexes(false))
          .as("replica should have the index of type %s registered", typeName)
          .isNotEmpty();
    }

    assertThat(replicaSchema.getFileByIdIfExists(anchorFileId))
        .as("applying %d schema entries must not re-instantiate a component none of them touched", TYPE_COUNT)
        .isSameAs(anchorBefore);
  }

  @Test
  void replicatedIndexesStillAnswerLookupsAfterAnIncrementalApply() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // See the note in followerKeepsUntouchedComponentsAcrossManySchemaEntries: DDL must be replay-safe here.
    leaderDb.transaction(() -> {
      final DocumentType type = leaderDb.getSchema().getOrCreateDocumentType("Issue6988Indexed");
      type.getOrCreateProperty("code", Type.STRING);
      type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "code");
    });

    // A few more entries after the index creation, so the index registration has to survive later incremental applies.
    for (int i = 0; i < 5; i++) {
      final int index = i;
      leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateDocumentType("Issue6988Filler_" + index));
    }

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++)
        leaderDb.newDocument("Issue6988Indexed").set("code", "code-" + i).save();
    });

    assertClusterConsistency();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(replicaDb.getSchema().getType("Issue6988Indexed").getAllIndexes(false)).isNotEmpty();

    try (final ResultSet result = replicaDb.query("sql", "SELECT FROM Issue6988Indexed WHERE code = ?", "code-42")) {
      assertThat(result.hasNext()).as("the replicated index must resolve a lookup on the follower").isTrue();
      assertThat(result.next().<String>getProperty("code")).isEqualTo("code-42");
    }
  }

  @Test
  void droppingATypeStillFallsBackToTheFullRebuild() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.transaction(() -> {
      leaderDb.getSchema().getOrCreateDocumentType("Issue6988Doomed");
      leaderDb.getSchema().getOrCreateDocumentType("Issue6988Survivor");
    });
    assertClusterConsistency();

    // A DROP retires files, which the incremental path refuses: the entry must go through the full rebuild and the
    // follower must end up with the type gone and everything else intact.
    leaderDb.transaction(() -> {
      if (leaderDb.getSchema().existsType("Issue6988Doomed"))
        leaderDb.getSchema().dropType("Issue6988Doomed");
    });
    assertClusterConsistency();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(replicaDb.getSchema().existsType("Issue6988Doomed")).isFalse();
    assertThat(replicaDb.getSchema().existsType("Issue6988Survivor")).isTrue();
  }
}
