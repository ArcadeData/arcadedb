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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10124: a replicated {@code ALTER DATABASE} whose {@code configuration.json} write fails is in force in memory and
 * not on disk. The entry must stay in the Raft log until the file carries it - so a restart replays it and nobody has to
 * reissue the change - and the node must write it again on its own once the volume takes the write, so a database whose
 * configuration rarely changes does not keep the log from being compacted until the next change to the same settings.
 * <p>
 * Every test makes {@code configuration.json} of {@link #DB_NAME} a non-empty directory, which fails both the read and the
 * atomic replace as a read-only or full volume would, and "fixes the volume" by removing it. The entries that carry the
 * applied position past the failed one go to a second database, {@link #OTHER_DB}, so that nothing but the retry or the
 * replay can write the first database's file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10124HeldConfigurationRetryTest {

  private static final String DB_NAME        = "db10124";
  private static final String OTHER_DB       = "other10124";
  private static final long   FAILED_INDEX   = 5L;
  private static final String FIVE_BUCKETS   = "{\"configuration\":{\"typeDefaultBuckets\":5},\"removed\":[]}";
  private static final long   INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(SecurityDurabilityFloor.RETRY_INTERVAL_MS);

  @TempDir
  Path tempDir;

  private final AtomicLong         clock = new AtomicLong(1_000_000_000L);
  private       ArcadeDBServer     server;
  private       LocalDatabase      database;
  private       LocalDatabase      other;
  private       RaftStorage        storage;
  private       ArcadeStateMachine sm;

  @BeforeEach
  void setUp() throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    server = new ArcadeDBServer(config);
    database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve(DB_NAME).toString()).create();
    other = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve(OTHER_DB).toString()).create();
    server.registerDatabase(DB_NAME, database);
    server.registerDatabase(OTHER_DB, other);
    storage = openStorage(RaftStorage.StartupOption.FORMAT);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (sm != null)
      sm.close();
    storage.close();
    server.stop();
    if (database.isOpen())
      database.drop();
    if (other.isOpen())
      other.drop();
  }

  /** The issue's expectation: the restart replays the entry and writes the file, with nothing reissued. */
  @Test
  void aRestartReplaysTheConfigurationTheFailedWriteLost() throws Exception {
    breakVolume();
    sm = stateMachine();
    assertThat(apply(DB_NAME, FIVE_BUCKETS, FAILED_INDEX)).as("the write fails").isTrue();
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":7},\"removed\":[]}", 9L)).isFalse();
    assertThat(sm.readAppliedIndexCounter()).as("the applied position moved past the failed entry").isEqualTo(9L);

    // A graceful stop always takes this snapshot, which is what used to let Ratis compact the entry away
    assertThat(sm.takeSnapshot()).as("the checkpoint stops right below the entry the file does not carry")
        .isEqualTo(FAILED_INDEX - 1);
    sm.close();
    sm = null;
    storage.close();

    fixVolume();
    storage = openStorage(RaftStorage.StartupOption.RECOVER);
    sm = stateMachine();
    assertThat(sm.readAppliedIndexCounter()).as("the restart replays from the held entry").isEqualTo(FAILED_INDEX - 1);

    // What Ratis feeds the restarted state machine: the retained entries above the marker, in order
    assertThat(apply(DB_NAME, FIVE_BUCKETS, FAILED_INDEX)).isFalse();
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":7},\"removed\":[]}", 9L)).isFalse();

    assertThat(savedSettings().getInt("typeDefaultBuckets")).as("the replay wrote the file").isEqualTo(5);
    assertThat(sm.takeSnapshot()).as("the file carries the entry now, so the checkpoint covers everything").isEqualTo(9L);
  }

  /**
   * The defect a hold alone leaves: on a database whose configuration does not change again, nothing but a restart wrote the
   * file, so the Raft log grew until one. The retry writes it once the volume takes the write, one interval after the failure.
   */
  @Test
  void theHeldConfigurationIsWrittenAgainOnceTheVolumeIsFixed() throws Exception {
    breakVolume();
    sm = stateMachine();
    assertThat(apply(DB_NAME, FIVE_BUCKETS, FAILED_INDEX)).isTrue();

    fixVolume();
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":7},\"removed\":[]}", 6L)).isFalse();
    assertThat(sm.takeSnapshot()).as("paced: the first retry is one interval after the failure").isEqualTo(FAILED_INDEX - 1);
    assertThat(savedSettings().has("typeDefaultBuckets")).isFalse();

    clock.addAndGet(INTERVAL_NANOS);
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":8},\"removed\":[]}", 7L)).isFalse();

    assertThat(savedSettings().getInt("typeDefaultBuckets")).as("the retry wrote the held setting").isEqualTo(5);
    assertThat(sm.takeSnapshot()).as("written, so the hold lifts").isEqualTo(7L);
  }

  /** While the volume is still broken the retry fails, and the hold stays: the restart still has the entry to replay. */
  @Test
  void aRetryThatFailsKeepsTheHold() throws Exception {
    breakVolume();
    sm = stateMachine();
    assertThat(apply(DB_NAME, FIVE_BUCKETS, FAILED_INDEX)).isTrue();

    for (long index = FAILED_INDEX + 1; index < FAILED_INDEX + 4; index++) {
      clock.addAndGet(INTERVAL_NANOS);
      assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":" + index + "},\"removed\":[]}", index))
          .as("a failed retry never fails the entry that ran it").isFalse();
    }
    assertThat(sm.takeSnapshot()).isEqualTo(FAILED_INDEX - 1);

    fixVolume();
    clock.addAndGet(INTERVAL_NANOS);
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":99},\"removed\":[]}", 20L)).isFalse();
    assertThat(sm.takeSnapshot()).isEqualTo(20L);
  }

  /**
   * The retry writes what the cluster committed, not what memory holds: memory may also hold a change made on this node
   * that is still on its way through the replication, and must not reach the file ahead of its own entry.
   */
  @Test
  void theRetryWritesTheCommittedValueNotAnUnreplicatedOneInMemory() throws Exception {
    breakVolume();
    sm = stateMachine();
    assertThat(apply(DB_NAME, FIVE_BUCKETS, FAILED_INDEX)).isTrue();
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(5);

    // A statement on this node put a value in force that its entry has not committed yet
    database.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 9);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 11);

    fixVolume();
    clock.addAndGet(INTERVAL_NANOS);
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":7},\"removed\":[]}", 6L)).isFalse();

    final JSONObject saved = savedSettings();
    assertThat(saved.getInt("typeDefaultBuckets")).as("the committed value, not the one in flight").isEqualTo(5);
    assertThat(saved.has("txRetries")).as("a setting no held entry named is not written").isFalse();
    assertThat(database.pendingConfigurationChange(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey()))
        .as("the change in flight is still pending, so its own entry still carries it").isNotNull();
    assertThat(sm.takeSnapshot()).isEqualTo(6L);
  }

  /**
   * Two failed entries of the same database: the retry writes what the replay of both would leave, the later value where
   * both named a setting, and the hold, at the lower index, lifts only then.
   */
  @Test
  void twoFailedEntriesAreWrittenAsTheReplayWouldLeaveThem() throws Exception {
    breakVolume();
    sm = stateMachine();
    assertThat(apply(DB_NAME, "{\"configuration\":{\"typeDefaultBuckets\":5,\"txRetries\":4},\"removed\":[]}",
        FAILED_INDEX)).isTrue();
    assertThat(apply(DB_NAME, "{\"configuration\":{\"typeDefaultBuckets\":7},\"removed\":[\"asyncWorkerThreads\"]}",
        FAILED_INDEX + 1)).isTrue();

    fixVolume();
    // A setting the file held before the incident, which the second entry removes
    database.writeConfigurationChanges("{\"configuration\":{\"asyncWorkerThreads\":3},\"removed\":[]}");

    clock.addAndGet(INTERVAL_NANOS);
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":7},\"removed\":[]}", 9L)).isFalse();

    final JSONObject saved = savedSettings();
    assertThat(saved.getInt("typeDefaultBuckets")).as("the later of the two values").isEqualTo(7);
    assertThat(saved.getInt("txRetries")).as("named by the first entry only").isEqualTo(4);
    assertThat(saved.has("asyncWorkerThreads")).as("removed by the second entry").isFalse();
    assertThat(sm.takeSnapshot()).isEqualTo(9L);
  }

  /** A setting one held entry removes and a later held entry sets again ends set, as the replay of both would leave it. */
  @Test
  void aSettingRemovedAndThenSetAgainIsWrittenWithTheLaterValue() throws Exception {
    breakVolume();
    sm = stateMachine();
    assertThat(apply(DB_NAME, "{\"configuration\":{},\"removed\":[\"txRetries\"]}", FAILED_INDEX)).isTrue();
    assertThat(apply(DB_NAME, "{\"configuration\":{\"txRetries\":4},\"removed\":[]}", FAILED_INDEX + 1)).isTrue();

    fixVolume();
    // The value the file held before the incident
    database.writeConfigurationChanges("{\"configuration\":{\"txRetries\":2},\"removed\":[]}");

    clock.addAndGet(INTERVAL_NANOS);
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":7},\"removed\":[]}", 9L)).isFalse();

    assertThat(savedSettings().getInt("txRetries")).as("set again by the later entry, not removed").isEqualTo(4);
    assertThat(sm.takeSnapshot()).isEqualTo(9L);
  }

  /** A later entry that writes some of the held settings leaves the retry only the others, with their committed values. */
  @Test
  void aLaterEntryThatWritesPartOfTheHeldSettingsLeavesTheRestToTheRetry() throws Exception {
    breakVolume();
    sm = stateMachine();
    assertThat(apply(DB_NAME, "{\"configuration\":{\"typeDefaultBuckets\":5,\"txRetries\":4},\"removed\":[]}",
        FAILED_INDEX)).isTrue();

    fixVolume();
    assertThat(apply(DB_NAME, "{\"configuration\":{\"typeDefaultBuckets\":6},\"removed\":[]}", 6L)).isFalse();
    assertThat(sm.takeSnapshot()).as("txRetries is still missing from the file").isEqualTo(FAILED_INDEX - 1);

    clock.addAndGet(INTERVAL_NANOS);
    assertThat(apply(OTHER_DB, "{\"configuration\":{\"txRetries\":7},\"removed\":[]}", 7L)).isFalse();

    final JSONObject saved = savedSettings();
    assertThat(saved.getInt("typeDefaultBuckets")).as("the later entry's value is not rolled back").isEqualTo(6);
    assertThat(saved.getInt("txRetries")).isEqualTo(4);
    assertThat(sm.takeSnapshot()).isEqualTo(7L);
  }

  // ---- drivers ----

  private File configurationFile() {
    return new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
  }

  /** {@code configuration.json} as a non-empty directory: reading and replacing it both fail, as a read-only volume would. */
  private void breakVolume() throws IOException {
    final File file = configurationFile();
    Files.deleteIfExists(file.toPath());
    assertThat(file.mkdirs()).isTrue();
    assertThat(new File(file, "x").createNewFile()).isTrue();
  }

  private void fixVolume() throws IOException {
    final File file = configurationFile();
    Files.delete(new File(file, "x").toPath());
    Files.delete(file.toPath());
  }

  private JSONObject savedSettings() throws IOException {
    final File file = configurationFile();
    if (!file.isFile())
      return new JSONObject();
    return new JSONObject(Files.readString(file.toPath())).getJSONObject("configuration", new JSONObject());
  }

  /** Applies a configuration entry; true when the apply failed. */
  private boolean apply(final String databaseName, final String changes, final long index) {
    return sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(databaseName, changes), index))
        .isCompletedExceptionally();
  }

  private RaftStorage openStorage(final RaftStorage.StartupOption option) throws IOException {
    return RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile()).setOption(option).build();
  }

  private ArcadeStateMachine stateMachine() throws IOException {
    final ArcadeStateMachine machine = new ArcadeStateMachine();
    machine.setServer(server);
    machine.setConfigurationRetryClock(clock::get);
    machine.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
    return machine;
  }

  private TransactionContext rawEntry(final ByteString payload, final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  /** Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(Issue10124HeldConfigurationRetryTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
