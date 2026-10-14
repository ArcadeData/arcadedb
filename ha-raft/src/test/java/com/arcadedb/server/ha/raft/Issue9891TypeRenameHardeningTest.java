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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9891, the HA half of the hardening of a replicated type rename (#9762):
 * <ul>
 *   <li>item 1 - the {@code schema-file-renames} refusal has no override, deliberately: a down peer refuses a rename
 *       with the security entries' override switched off too, and the refusal says so;</li>
 *   <li>item 3 - a follower that crashed between moving the renamed files and loading the schema naming them boots on
 *       the OLD schema with the files already under their new names, and the replay of the entry must still land it
 *       where the leader is;</li>
 *   <li>item 4 - a rename whose publishing entry fails AFTER the callback ran stays on the leader: a definite refusal
 *       quarantines it (#9555), an unknown outcome does not.</li>
 * </ul>
 */
class Issue9891TypeRenameHardeningTest {
  private static final int RECORDS = 10;

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private LocalDatabase      leaderDb;
  private ArcadeStateMachine stateMachine;

  @AfterEach
  void tearDown() {
    for (final LocalDatabase database : new LocalDatabase[] { db, leaderDb })
      if (database != null && database.isOpen())
        database.drop();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Item 1: no override

  @Test
  void aDownPeerRefusesARenameEvenWithTheSecurityEntryOverrideSwitchedOff() {
    db = seeded(tempDir.resolve("item1"));
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final FakeRaftHAServer raft = leader(broker).returns("peersMissingCapabilityNow", List.of("peer-down"));
    raft.getConfiguration().setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, false);
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    server.getConfiguration().setValue(GlobalConfiguration.HA_SECURITY_ENTRY_CAPABILITY_GATE, false);
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(server, db, raft);
    final Map<Integer, String> before = componentNames(db);

    assertThatThrownBy(() -> replicated.getSchema().getType("R").rename("R2"))
        .isInstanceOf(ClusterCapabilityNotReadyException.class)
        .hasMessageContaining(PeerCapabilities.SCHEMA_FILE_RENAMES)
        .hasMessageContaining("peer-down")
        .hasMessageContaining("no setting that overrides this refusal");

    assertThat(componentNames(db)).as("refused before a single file moved").isEqualTo(before);
    assertThat(db.getSchema().existsType("R")).isTrue();
    assertThat(broker.calls("replicateSchema")).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Item 4: failure after the publishing callback ran

  @Test
  void aRenameWhosePublishingEntryIsRefusedQuarantinesTheLeader() {
    db = seeded(tempDir.resolve("item4refused"));
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker().fails("replicateSchema",
        new NeedRetryException("Refused a schema change on database 'item4refused' allocated in Raft term 7"));
    final FakeRaftHAServer raft = leader(broker);
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db, raft);

    assertThatThrownBy(() -> replicated.getSchema().getType("R").rename("R2")).isInstanceOf(NeedRetryException.class);

    assertThat(shippedRenames(broker)).as("the entry carried the renames").isNotEmpty();
    // The callback ran, so the rename is not rolled back: it stays here, and the quarantine is what repairs it
    assertThat(db.getSchema().existsType("R2")).isTrue();
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();
    assertThat(db.getFileManager().getRecordedRenames()).isEmpty();
  }

  /** Dispatched and unanswered: the entry may still commit, and then the local rename is exactly the committed one. */
  @Test
  void aRenameWhosePublishingEntryHasAnUnknownOutcomeIsNotQuarantined() {
    db = seeded(tempDir.resolve("item4unknown"));
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker().fails("replicateSchema",
        new ReplicationDispatchedTimeoutException("Group commit timed out (entry was dispatched to Raft)"));
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db,
        leader(broker));

    assertThatThrownBy(() -> replicated.getSchema().getType("R").rename("R2"))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);

    assertThat(shippedRenames(broker)).isNotEmpty();
    assertThat(db.getSchema().existsType("R2")).isTrue();
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Item 3: follower crash between the file move and the schema load

  @Test
  void aFollowerThatCrashedAfterMovingTheFilesConvergesOnTheReplay() throws Exception {
    crashWindowConverges(false, false);
  }

  /** The crash came part way through the moves: the replay finds some files moved and some not. */
  @Test
  void aFollowerThatCrashedPartWayThroughTheMovesConvergesOnTheReplay() throws Exception {
    crashWindowConverges(true, false);
  }

  /**
   * The same crash with the rename shipped as a schema DELTA (#6989): the boot-time load re-saved the old schema without
   * the buckets it could not find, so the delta lands on a document that is not the base it was computed against.
   */
  @Test
  void aFollowerThatCrashedAfterMovingTheFilesConvergesOnTheReplayOfADelta() throws Exception {
    crashWindowConverges(false, true);
  }

  private void crashWindowConverges(final boolean partial, final boolean delta) throws Exception {
    // The follower is a byte copy of the leader, the way a replica holds the same files under the same ids and names
    final Path leaderPath = tempDir.resolve("leader");
    final Path followerPath = tempDir.resolve("follower");
    seeded(leaderPath).close();
    copyDirectory(leaderPath, followerPath);
    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    db = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
    assertThat(componentNames(db)).isEqualTo(componentNames(leaderDb));

    // The leader's side: the renames its session recorded and the schema it published
    final JSONObject baseSchema = leaderDb.getSchema().getEmbedded().toJSON();
    final Map<Integer, String> renames;
    final FileManager leaderFiles = leaderDb.getFileManager();
    assertThat(leaderFiles.startRecordingChanges()).isTrue();
    try {
      leaderDb.getSchema().getType("R").rename("R2");
      renames = leaderFiles.getRecordedRenames();
    } finally {
      leaderFiles.stopRecordingChanges();
    }
    assertThat(renames).as("buckets and index files").hasSize(4);
    final JSONObject leaderSchema = leaderDb.getSchema().getEmbedded().toJSON();
    final SchemaDelta.Payload schemaDelta = delta ?
        new SchemaDelta.Payload(baseSchema.getLong("schemaVersion"), SchemaDelta.compute(baseSchema, leaderSchema).toString()) :
        null;
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(db.getName(), delta ? "" : leaderSchema.toString(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), false, Collections.emptyList(), schemaDelta, renames);

    // The follower moves the files - all of them, or only the first - and crashes before the schema load
    final Map<Integer, String> movedBeforeCrash = new LinkedHashMap<>();
    for (final Map.Entry<Integer, String> rename : renames.entrySet()) {
      movedBeforeCrash.put(rename.getKey(), rename.getValue());
      if (partial)
        break;
    }
    ArcadeStateMachine.renameFiles(db, movedBeforeCrash);
    db.kill();
    db.close();

    // Boot: the schema on disk is the old one, naming files that are now under the new names
    db = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
    assertThat(db.getSchema().existsType("R")).as("the old schema.json is what boots").isTrue();
    assertThat(db.getSchema().existsType("R2")).isFalse();
    for (final Map.Entry<Integer, String> moved : movedBeforeCrash.entrySet())
      assertThat(db.getFileManager().getFile(moved.getKey()).getComponentName()).isEqualTo(moved.getValue());

    // The replay of the uncommitted-to-the-state-machine entry
    new DatabaseBoundStateMachine(db).applySchemaEntry(RaftLogEntryCodec.decode(entry), 42L, false);

    assertConverged(renames);

    db.close();
    db = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
    assertConverged(renames);
  }

  private void assertConverged(final Map<Integer, String> renames) {
    assertThat(db.getSchema().existsType("R")).isFalse();
    assertThat(db.getSchema().existsType("R2")).isTrue();
    assertThat(db.getSchema().getType("R2").getBuckets(false)).hasSize(2);
    assertThat(db.getSchema().getType("R2").getAllIndexes(false)).hasSize(1);
    for (final Map.Entry<Integer, String> rename : renames.entrySet())
      assertThat(db.getFileManager().getFile(rename.getKey()).getComponentName()).isEqualTo(rename.getValue());
    assertThat(new File(db.getDatabasePath()).list()).noneMatch(n -> n.startsWith("R_"));
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM R2")) {
      assertThat(rs.next().<Long>getProperty("c")).as("no record lost").isEqualTo(RECORDS);
    }
    try (final ResultSet rs = db.query("sql", "SELECT FROM R2 WHERE id = 7")) {
      assertThat(rs.hasNext()).as("the index answers under the new name").isTrue();
    }
    assertThat(componentNames(db)).isEqualTo(componentNames(leaderDb));
  }

  // ---------------------------------------------------------------------------------------------------------------

  private static LocalDatabase seeded(final Path path) {
    final LocalDatabase database = (LocalDatabase) new DatabaseFactory(path.toString()).create();
    database.command("sql", "CREATE DOCUMENT TYPE R BUCKETS 2");
    database.command("sql", "CREATE PROPERTY R.id INTEGER");
    database.command("sql", "CREATE INDEX ON R (id) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("R").set("id", i).save();
    });
    return database;
  }

  private static void copyDirectory(final Path from, final Path to) throws IOException {
    try (final Stream<Path> paths = Files.walk(from)) {
      for (final Path source : (Iterable<Path>) paths::iterator) {
        final Path target = to.resolve(from.relativize(source));
        if (Files.isDirectory(source))
          Files.createDirectories(target);
        else
          Files.copy(source, target);
      }
    }
  }

  private FakeRaftHAServer leader(final FakeRaftTransactionBroker broker) {
    stateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }

      @Override
      ClientId localClientId() {
        return null;
      }

      @Override
      long currentRaftTerm() {
        return 7L;
      }
    };
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true).currentTerm(7L)
        .transactionBroker(broker);
    stateMachine.setRaftHAServer(raft);
    return raft;
  }

  @SuppressWarnings("unchecked")
  private static Map<Integer, String> shippedRenames(final FakeRaftTransactionBroker broker) {
    final List<List<Object>> calls = broker.calls("replicateSchema");
    assertThat(calls).hasSize(1);
    return (Map<Integer, String>) calls.getFirst().get(9);
  }

  private static Map<Integer, String> componentNames(final Database database) {
    final Map<Integer, String> names = new HashMap<>();
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles())
      if (file != null && file.getComponentName() != null)
        names.put(file.getFileId(), file.getComponentName());
    return names;
  }

  /** A state machine with no server attached, whose only database is the one this test opened. */
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
