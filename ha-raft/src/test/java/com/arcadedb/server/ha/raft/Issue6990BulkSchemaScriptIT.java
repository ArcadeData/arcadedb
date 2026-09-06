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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6990, end to end: a DDL script has to reach the followers as ONE Raft entry, not one per statement.
 * <p>
 * The Raft entry boundary is the outermost {@code recordFileChanges} session on the leader's calling thread, and every
 * DDL entry point opens one of its own. A script is neither a session nor a transaction, so nothing collapsed them:
 * twelve statements were twelve entries, twelve serialized copies of the full schema and twelve synchronous quorum
 * round trips, each taken with the database write lock held. #6982 measured a 1209-type schema at roughly three hours.
 * <p>
 * WHAT IS COUNTED. {@link ArcadeStateMachine#TEST_SCHEMA_ENTRY_COUNTER} counts the PUBLISHING schema entries a node
 * applies - the ones that tell it to reload its schema. The leader returns from {@code applySchemaEntry} before the
 * counter on the {@code originatedLocally} path, so with two servers the count is the follower's, and it is exactly
 * the number this issue is about. The control run, with the batching turned off, is what gives the number meaning: it
 * is the same script measured on the code path this change replaces.
 */
@Tag("slow")
class Issue6990BulkSchemaScriptIT extends BaseRaftHATest {

  /**
   * Four statements per type, so the count separates "one per statement" from "one per script" by a wide margin.
   */
  private static final int TYPES              = 3;
  private static final int STATEMENTS_PER_TYPE = 4;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = null;
  }

  private static String ddlScript(final String prefix) {
    final StringBuilder script = new StringBuilder();
    for (int i = 0; i < TYPES; i++)
      script.append("CREATE VERTEX TYPE ").append(prefix).append(i).append(";\n")
          .append("CREATE PROPERTY ").append(prefix).append(i).append(".name STRING;\n")
          .append("CREATE PROPERTY ").append(prefix).append(i).append(".age INTEGER;\n")
          .append("CREATE INDEX ON ").append(prefix).append(i).append(" (name) NOTUNIQUE;\n");
    return script.toString();
  }

  private void assertReplicated(final int serverIndex, final String prefix) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    for (int i = 0; i < TYPES; i++) {
      final String typeName = prefix + i;
      assertThat(db.getSchema().existsType(typeName)).as("type %s on server %d", typeName, serverIndex).isTrue();
      final DocumentType type = db.getSchema().getType(typeName);
      assertThat(type.getPropertyNames()).as("properties of %s on server %d", typeName, serverIndex)
          .contains("name", "age");
      assertThat(db.getSchema().existsIndex(typeName + "[name]"))
          .as("index of %s on server %d", typeName, serverIndex).isTrue();
    }
  }

  @Test
  void aDdlScriptReachesTheFollowerAsOneSchemaEntry() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // CONTROL: the pre-existing path, one session per statement. Measured first so the comparison is against this
    // cluster on this run and not against a remembered number.
    leaderDb.getConfiguration().setValue(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT, false);
    final ArcadeStateMachine.SchemaEntryRecorder unbatched = new ArcadeStateMachine.SchemaEntryRecorder();
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = unbatched;
    try {
      leaderDb.command("sqlscript", ddlScript("Unbatched"));
      waitForAllServers();
    } finally {
      leaderDb.getConfiguration().setValue(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT, true);
    }

    assertReplicated(followerIndex, "Unbatched");
    assertThat(unbatched.count())
        .as("without the bulk scope the follower applies at least one schema entry per statement")
        .isGreaterThanOrEqualTo(TYPES * STATEMENTS_PER_TYPE);

    // THE FIX: the same script, batched.
    final ArcadeStateMachine.SchemaEntryRecorder batched = new ArcadeStateMachine.SchemaEntryRecorder();
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = batched;
    leaderDb.command("sqlscript", ddlScript("Batched"));
    waitForAllServers();

    assertThat(batched.count())
        .as("a DDL-only script must reach the follower as ONE schema entry, whatever its statement count")
        .isEqualTo(1);

    // One entry is only worth anything if it carried the whole schema.
    assertReplicated(leaderIndex, "Batched");
    assertReplicated(followerIndex, "Batched");
  }

  /**
   * The failure contract, which is the half that matters for correctness rather than for speed. Schema DDL has no
   * rollback, so a script that throws part way cannot be undone on the leader; what the scope guarantees is that the
   * follower ends up holding exactly what the leader holds. Aborting the session instead would publish nothing, and
   * the leader would keep the prefix while the follower had none of it - a divergence the per-statement path never
   * produced.
   */
  @Test
  void aScriptThatFailsHalfWayLeavesTheFollowerHoldingWhatTheLeaderHolds() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    final ArcadeStateMachine.SchemaEntryRecorder failed = new ArcadeStateMachine.SchemaEntryRecorder();
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = failed;

    assertThatThrownBy(() -> leaderDb.command("sqlscript", """
        CREATE VERTEX TYPE PartialA;
        CREATE PROPERTY PartialA.name STRING;
        CREATE PROPERTY NoSuchTypeAtAll.name STRING;
        CREATE VERTEX TYPE PartialB;
        """))
        .as("the caller is still told which statement failed")
        .hasMessageContaining("NoSuchTypeAtAll");

    waitForAllServers();

    assertThat(failed.count())
        .as("the prefix a failed script completed is published as ONE entry, not one per statement that succeeded")
        .isEqualTo(1);

    for (final int serverIndex : new int[] { leaderIndex, followerIndex }) {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(db.getSchema().existsType("PartialA"))
          .as("the prefix that succeeded is on server %d", serverIndex).isTrue();
      assertThat(db.getSchema().getType("PartialA").getPropertyNames())
          .as("the prefix's properties are on server %d", serverIndex).contains("name");
      assertThat(db.getSchema().existsType("PartialB"))
          .as("nothing after the failing statement ran, on server %d either", serverIndex).isFalse();
    }
  }
}
