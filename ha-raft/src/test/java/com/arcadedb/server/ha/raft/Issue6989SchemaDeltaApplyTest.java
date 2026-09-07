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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6989, follower side: a {@code SCHEMA_ENTRY} carrying a delta must leave the receiver's schema where the
 * whole document would have left it.
 * <p>
 * Driven against REAL {@code LocalSchema} documents rather than hand-built JSON, because that is where the risk
 * lives: {@link SchemaDelta} diffs the schema document generically, and the shape that has to survive is the one
 * {@code LocalSchema.toJSON()} actually produces - nested type/property/index maps, a settings section, and root
 * scalars - not the shape a test author imagines.
 * <p>
 * The two databases are built by running the SAME DDL, so their bucket ids and file names line up the way they do
 * on a leader and a follower that have applied the same entries. Only property-level changes are exercised for
 * that reason: a change that creates a file needs the {@code createNewFiles} half of {@code applySchemaEntry},
 * which is not what this test is about.
 */
class Issue6989SchemaDeltaApplyTest {

  private static final String DB = "mydb";

  @TempDir
  private Path serverDir;

  private LocalDatabase leader;
  private LocalDatabase follower;

  @BeforeEach
  void setUp() {
    leader = (LocalDatabase) new DatabaseFactory(serverDir.resolve("db-leader").toString()).create();
    follower = (LocalDatabase) new DatabaseFactory(serverDir.resolve("db-follower").toString()).create();
    seed(leader);
    seed(follower);
  }

  @AfterEach
  void tearDown() {
    if (leader != null && leader.isOpen())
      leader.close();
    if (follower != null && follower.isOpen())
      follower.close();
  }

  private static void seed(final LocalDatabase db) {
    db.transaction(() -> {
      final Schema schema = db.getSchema();
      for (int i = 0; i < 25; i++) {
        schema.createVertexType("Seed_" + i);
        schema.getType("Seed_" + i).createProperty("id_" + i, Type.STRING);
        schema.getType("Seed_" + i).createProperty("name_" + i, Type.STRING);
      }
    });
  }

  @Test
  void aDeltaLeavesTheFollowerWhereTheWholeDocumentWouldHave() throws Exception {
    final JSONObject base = leader.getSchema().getEmbedded().toJSON();

    leader.transaction(() -> leader.getSchema().getType("Seed_3").createProperty("added", Type.INTEGER));
    final JSONObject updated = leader.getSchema().getEmbedded().toJSON();

    final SchemaDelta.Payload payload = new SchemaDelta.Payload(base.getLong("schemaVersion"),
        SchemaDelta.compute(base, updated).toString());

    assertThat(payload.deltaJson().length())
        .as("the delta must be a fraction of the document it replaces")
        .isLessThan(updated.toString().length() / 3);

    applyOnFollower(payload);

    final Schema followerSchema = follower.getSchema();
    assertThat(followerSchema.getType("Seed_3").existsProperty("added")).isTrue();
    assertThat(followerSchema.getType("Seed_3").getProperty("added").getType()).isEqualTo(Type.INTEGER);
    // Nothing else moved: the untouched types are still there, with the properties they had.
    for (int i = 0; i < 25; i++) {
      assertThat(followerSchema.existsType("Seed_" + i)).isTrue();
      assertThat(followerSchema.getType("Seed_" + i).existsProperty("name_" + i)).isTrue();
      if (i != 3)
        assertThat(followerSchema.getType("Seed_" + i).existsProperty("added"))
            .as("type Seed_%d was not part of the change", i).isFalse();
    }
    // At least, not exactly: the reload re-saves the schema when it repaired anything, and every save moves
    // versionSerial on. That drift is why the applier treats the delta's base version as a diagnostic.
    assertThat(follower.getSchema().getEmbedded().getVersion())
        .as("the follower took the leader's schema version from the delta")
        .isGreaterThanOrEqualTo(updated.getLong("schemaVersion"));
  }

  @Test
  void aDroppedTypeIsGoneOnTheFollower() throws Exception {
    final JSONObject base = leader.getSchema().getEmbedded().toJSON();

    leader.transaction(() -> leader.getSchema().dropType("Seed_11"));
    final JSONObject updated = leader.getSchema().getEmbedded().toJSON();

    applyOnFollower(new SchemaDelta.Payload(base.getLong("schemaVersion"),
        SchemaDelta.compute(base, updated).toString()));

    assertThat(follower.getSchema().existsType("Seed_11")).isFalse();
    assertThat(follower.getSchema().existsType("Seed_10")).isTrue();
    assertThat(follower.getSchema().existsType("Seed_12")).isTrue();
  }

  /**
   * Drives the production applier over a real encode/decode round trip, so the test cannot pass on a payload the
   * wire would not have carried.
   */
  private void applyOnFollower(final SchemaDelta.Payload payload) throws Exception {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        Collections.emptyList(), payload);

    new ArcadeStateMachine().applySchemaDelta(follower, RaftLogEntryCodec.decode(entry));

    // The reload applySchemaEntry does right after, and what makes the merged document the live schema.
    final LocalSchema schema = follower.getSchema().getEmbedded();
    schema.load(ComponentFile.MODE.READ_WRITE, true);
  }
}
