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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4077.
 * <p>
 * When a database schema serialized to JSON exceeds the 64KB modified-UTF-8 limit
 * imposed by {@code DataOutputStream.writeUTF}, schema replication used to fail on
 * the leader with {@code IllegalStateException: Failed to encode SCHEMA entry ->
 * encoded string ... too long: N bytes}. Followers then logged
 * "WAL version gap on follower - state divergence detected, triggering snapshot
 * resync" and the cluster diverged.
 * <p>
 * This test creates many vertex types with multiple properties so the schema JSON
 * grows beyond the legacy 64KB limit (matching the user-reported ~120KB schema),
 * then verifies the schema is replicated successfully to all replicas.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftLargeSchemaReplicationIT extends BaseRaftHATest {

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
  void schemaJsonAbove64KbIsReplicated() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Build a schema large enough to exceed the legacy 64KB writeUTF limit.
    // Each type contributes ~250-300 bytes to the JSON (name, parent, properties).
    // 400 types is enough to reliably push the serialized schema past 100KB,
    // matching the ~120KB schema reported in issue #4077.
    final int typeCount = 400;

    leaderDb.transaction(() -> {
      final Schema schema = leaderDb.getSchema();
      for (int i = 0; i < typeCount; i++) {
        final DocumentType type = schema.createVertexType("LargeSchemaType_" + i);
        type.createProperty("id_" + i, Type.STRING);
        type.createProperty("name_" + i, Type.STRING);
        type.createProperty("timestamp_" + i, Type.DATETIME);
        type.createProperty("payload_" + i, Type.STRING);
        type.createProperty("counter_" + i, Type.LONG);
      }
    });

    // Confirm the schema JSON we produced actually exceeds the 64KB limit that used to break encoding.
    final int schemaSize = leaderDb.getSchema().getEmbedded().toJSON().toString()
        .getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    assertThat(schemaSize)
        .as("Schema JSON size should exceed the legacy DataOutputStream.writeUTF 64KB cap to exercise the regression")
        .isGreaterThan(65_535);

    assertClusterConsistency();

    // Verify all types replicated to the follower
    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final Schema replicaSchema = replicaDb.getSchema();

    for (int i = 0; i < typeCount; i++) {
      final String typeName = "LargeSchemaType_" + i;
      assertThat(replicaSchema.existsType(typeName))
          .as("Replica should have type %s", typeName)
          .isTrue();
      final DocumentType replicaType = replicaSchema.getType(typeName);
      assertThat(replicaType.existsProperty("id_" + i)).isTrue();
      assertThat(replicaType.existsProperty("name_" + i)).isTrue();
      assertThat(replicaType.existsProperty("timestamp_" + i)).isTrue();
      assertThat(replicaType.existsProperty("payload_" + i)).isTrue();
      assertThat(replicaType.existsProperty("counter_" + i)).isTrue();
    }
  }
}
