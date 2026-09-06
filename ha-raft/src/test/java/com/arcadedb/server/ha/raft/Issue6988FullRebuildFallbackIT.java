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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Companion of {@link Issue6988IncrementalSchemaApplyIT}: proves the safety valve of issue #6988 actually reaches
 * the apply path. With {@code arcadedb.ha.schemaIncrementalApply=false} the follower must go back to the full
 * {@code LocalSchema.load()} for every entry - so the component instance a later entry did not touch IS replaced -
 * while the replicated schema itself stays correct.
 * <p>
 * Without this test the setting could be silently unread and nothing would notice, because both values produce a
 * correct schema; only the instance identity tells them apart.
 */
class Issue6988FullRebuildFallbackIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY, false);
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void disablingTheSettingRestoresTheFullRebuildPerEntry() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Idempotent DDL: a leadership transfer mid-commit makes LocalDatabase.transaction() re-run the lambda.
    leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateDocumentType("Issue6988FallbackAnchor"));
    assertClusterConsistency();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final LocalSchema replicaSchema = replicaDb.getSchema().getEmbedded();

    final int anchorFileId = replicaSchema.getType("Issue6988FallbackAnchor").getBuckets(false).getFirst().getFileId();
    final Component anchorBefore = replicaSchema.getFileByIdIfExists(anchorFileId);
    assertThat(anchorBefore).isNotNull();

    for (int i = 0; i < 3; i++) {
      final int index = i;
      leaderDb.transaction(() -> {
        final DocumentType type = leaderDb.getSchema().getOrCreateDocumentType("Issue6988FallbackType_" + index);
        type.getOrCreateProperty("id", Type.STRING);
      });
    }

    assertClusterConsistency();

    for (int i = 0; i < 3; i++)
      assertThat(replicaDb.getSchema().existsType("Issue6988FallbackType_" + i)).isTrue();

    assertThat(replicaSchema.getFileByIdIfExists(anchorFileId))
        .as("with the incremental apply disabled the follower must rebuild every component on each entry")
        .isNotSameAs(anchorBefore);
  }
}
