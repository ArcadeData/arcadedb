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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.ha.raft.RaftLogEntryCodec.TsSealedChunk;
import com.arcadedb.server.security.ReplicatedSecurityConfigPersistenceException;
import com.arcadedb.server.security.ReplicatedUsersPersistenceException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10086: an entry that did not advance {@code lastAppliedIndex} must leave behind one of a
 * quarantine, a replay floor or a halt, because those are the only three things {@code takeSnapshot()} consults before
 * telling Ratis "state up to here is durable, you may purge the log up to it". Without one of them a later committed
 * entry for any database advances the index over the skipped one, the checkpoint covers it, Ratis purges an entry this
 * node never applied, and the persisted position moves with it so the restart does not replay it either.
 * <p>
 * The table is over the dispositions by which an apply can finish without advancing the index, rather than one test per
 * trigger, so the next throw site on the apply path is covered by construction: each row fails an apply, applies a LATER
 * entry that moves the index past it, and asks {@code takeSnapshot()} for a checkpoint, which must stop short of the
 * skipped entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10086SkippedEntryHoldsTheCheckpointTest {

  private static final String DB_NAME          = "db10086";
  private static final long   SKIPPED_INDEX    = 5L;
  private static final long   LATER_INDEX      = 9L;
  private static final String USERS_JSON       = "[{\"name\":\"root\",\"password\":\"x\"}]";
  private static final String GROUPS_JSON      = "{\"groups\":{}}";
  private static final String API_TOKENS_JSON  = "{\"tokens\":[]}";
  private static final String CONFIGURATION    = "{\"configuration\":{\"typeDefaultBuckets\":5},\"removed\":[]}";
  private static final String TYPE_NAME        = "Cpu";

  @TempDir
  Path tempDir;

  private int                prevRetries;
  private int                prevDelay;
  private RaftStorage        storage;
  private ArcadeStateMachine sm;

  @BeforeEach
  void setUp() throws IOException {
    prevRetries = GlobalConfiguration.TX_RETRIES.getValueAsInteger();
    prevDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRIES.setValue(1);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(0);
    storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT).build();
  }

  @AfterEach
  void tearDown() throws IOException {
    GlobalConfiguration.TX_RETRIES.setValue(prevRetries);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(prevDelay);
    Thread.interrupted();
    if (sm != null)
      sm.close();
    storage.close();
  }

  /** How an apply fails without advancing the applied index. */
  enum Skipped {
    /** A page-version race that outlasts the retry budget, on a database. */
    RETRY_EXHAUSTED_ON_A_DATABASE,
    /** The same, on an entry that names no database. */
    RETRY_EXHAUSTED_WITHOUT_A_DATABASE,
    /** The retry loop interrupted, which is a node going down: the entry is left for the replay. */
    RETRY_INTERRUPTED,
    /** Any {@link ReplicationException} nobody has dealt with, which is what a future throw site raises. */
    BARE_REPLICATION_EXCEPTION_ON_A_DATABASE,
    /** A sealed slice that does not continue the sequence staged so far. */
    SEALED_SLICE_SEQUENCE_BROKEN,
    /** The security users file could not be written. */
    SECURITY_USERS_NOT_PERSISTED,
    /** The security groups file could not be written. */
    SECURITY_GROUPS_NOT_PERSISTED,
    /** The API tokens file could not be written. */
    SECURITY_API_TOKENS_NOT_PERSISTED
  }

  @ParameterizedTest
  @EnumSource(Skipped.class)
  void aSkippedEntryIsNeverCheckpointedOver(final Skipped skipped) throws Exception {
    sm = stateMachine(securityServer(skipped));

    final Throwable failure = failTheApply(skipped);
    // An interrupt is how a node going down ends the retry loop: it must not leak into the entries applied next
    Thread.interrupted();
    assertThat(failure).as("the apply fails, so the index does not move over it").isInstanceOf(ReplicationException.class);
    assertThat(sm.readAppliedIndexCounter()).isLessThan(SKIPPED_INDEX);

    // A later committed entry, of another kind of document, applies and carries the index past the skipped one
    final CompletableFuture<?> later = sm.applyTransaction(securityEntry(laterKind(skipped), LATER_INDEX));
    assertThat(later.isCompletedExceptionally()).as("the later entry applies").isFalse();
    assertThat(sm.readAppliedIndexCounter()).isEqualTo(LATER_INDEX);

    assertThat(sm.takeSnapshot())
        .as("%s: the checkpoint must stop short of the entry this node did not apply (quarantine, replay floor or halt)", skipped)
        .isLessThan(SKIPPED_INDEX);

    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("%s: only a failure that left the database unknown quarantines it", skipped)
        .isEqualTo(quarantinesTheDatabase(skipped));
    assertThat(sm.isHaltedAfterCriticalError()).as("%s must not take the node down", skipped).isFalse();
  }

  /** A failure on a database that leaves its state unknown is quarantined; a node going down or a document is held instead. */
  private static boolean quarantinesTheDatabase(final Skipped skipped) {
    return skipped == Skipped.RETRY_EXHAUSTED_ON_A_DATABASE || skipped == Skipped.BARE_REPLICATION_EXCEPTION_ON_A_DATABASE
        || skipped == Skipped.SEALED_SLICE_SEQUENCE_BROKEN;
  }

  /** A document in force in memory whose file could not be written is replayed by the restart until a later one lands. */
  @Test
  void aLaterEntryOfTheSameDocumentSupersedesTheUnpersistedOne() throws Exception {
    sm = stateMachine(securityServer(Skipped.SECURITY_USERS_NOT_PERSISTED));

    assertThat(sm.applyTransaction(securityEntry(Kind.USERS, SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();
    assertThat(sm.applyTransaction(securityEntry(Kind.GROUPS, 6L)).isCompletedExceptionally()).isFalse();
    assertThat(sm.takeSnapshot()).as("another document does not heal the users file").isLessThan(SKIPPED_INDEX);

    assertThat(sm.applyTransaction(securityEntry(Kind.USERS, LATER_INDEX)).isCompletedExceptionally())
        .as("the volume is writable again").isFalse();
    assertThat(sm.takeSnapshot())
        .as("a whole document written after the failed one carries everything the failed one did: nothing left to replay")
        .isEqualTo(LATER_INDEX);
  }

  /** An entry whose precondition no longer holds installs nothing, so it carries nothing the failed one did not. */
  @Test
  void aStaleLaterEntryOfTheSameDocumentDoesNotReleaseTheHold() {
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("applyReplicatedUsers", args -> {
      if (args.length == 1)
        throw new ReplicatedUsersPersistenceException("could NOT be persisted", new IOException("No space left on device"));
      return false;
    });
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.security(security);
    sm = stateMachine(server);

    assertThat(sm.applyTransaction(securityEntry(Kind.USERS, SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();
    final CompletableFuture<?> stale = sm.applyTransaction(
        rawEntry(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON, "a-fingerprint-nothing-holds"), LATER_INDEX));
    assertThat(stale.isCompletedExceptionally()).as("a superseded entry is applied, as a no-op").isFalse();
    assertThat(sm.readAppliedIndexCounter()).isEqualTo(LATER_INDEX);

    assertThat(sm.takeSnapshot()).as("nothing was written, so the failed entry is still the one to replay").isLessThan(SKIPPED_INDEX);
  }

  /** A hold at or below what a Raft install covered is dropped: the installed copy carries what the entry did. */
  @Test
  void aRaftInstallPastTheHeldEntryDropsTheHold() {
    sm = stateMachine(securityServer(Skipped.SECURITY_USERS_NOT_PERSISTED));

    assertThat(sm.applyTransaction(securityEntry(Kind.USERS, SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();
    assertThat(sm.applyTransaction(securityEntry(Kind.GROUPS, LATER_INDEX)).isCompletedExceptionally()).isFalse();

    sm.recordInstalledRaftBoundary(SKIPPED_INDEX - 1);
    assertThat(sm.takeSnapshot()).as("an install short of the entry does not cover it").isLessThan(SKIPPED_INDEX);

    sm.recordInstalledRaftBoundary(SKIPPED_INDEX);
    assertThat(sm.takeSnapshot()).as("an install that reaches the entry carries its effect").isEqualTo(LATER_INDEX);
  }

  /** A configuration entry rewrites only the settings it names, so the held one waits for every one of them. */
  @Test
  void aHeldConfigurationIsReleasedOnlyOnceEverySettingItNamedIsWrittenAgain() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeDBServer server = new ArcadeDBServer(config);
    final LocalDatabase database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve(DB_NAME).toString())
        .create();
    server.registerDatabase(DB_NAME, database);
    try {
      final File file = new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
      Files.deleteIfExists(file.toPath());
      assertThat(file.mkdirs()).isTrue();
      assertThat(new File(file, "x").createNewFile()).isTrue();

      sm = stateMachine(server);
      assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME, CONFIGURATION),
          SKIPPED_INDEX)).isCompletedExceptionally()).isTrue();

      // The volume is fixed
      Files.delete(new File(file, "x").toPath());
      Files.delete(file.toPath());

      assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME,
          "{\"configuration\":{\"txRetries\":7},\"removed\":[]}"), 6L)).isCompletedExceptionally()).isFalse();
      assertThat(sm.takeSnapshot()).as("another setting does not write the one that is missing").isLessThan(SKIPPED_INDEX);

      assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME, CONFIGURATION),
          LATER_INDEX)).isCompletedExceptionally()).isFalse();
      assertThat(sm.takeSnapshot()).as("the file carries every setting the held entry named").isEqualTo(LATER_INDEX);
    } finally {
      server.stop();
      if (database.isOpen())
        database.drop();
    }
  }

  /** A failure on a database that is already quarantined has its disposition: it is not charged to the swallow budget again. */
  @Test
  void aQuarantinedDatabaseStillPassesThroughUntouched() {
    sm = stateMachine(FakeArcadeDBServer.create());
    sm.markStateDiverged(DB_NAME);
    final ReplicationException settled = new ReplicationException("already settled");

    assertThatThrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
      throw settled;
    })).isSameAs(settled);
    assertThat(sm.divergedSwallowedErrorCount()).as("a settled disposition is not charged to the swallow budget").isZero();
  }

  /** The configuration of a database whose {@code configuration.json} cannot be written stays replayable. */
  @Test
  void aConfigurationThatCouldNotBePersistedHoldsTheCheckpoint() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeDBServer server = new ArcadeDBServer(config);
    final LocalDatabase unwritable = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve(DB_NAME).toString())
        .create();
    final LocalDatabase healthy = (LocalDatabase) new DatabaseFactory(tempDir.resolve("databases").resolve("healthy").toString())
        .create();
    server.registerDatabase(DB_NAME, unwritable);
    server.registerDatabase("healthy", healthy);
    try {
      // configuration.json is a non-empty directory: reading and replacing it both fail, as a read-only volume would
      final File asDirectory = new File(unwritable.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
      Files.deleteIfExists(asDirectory.toPath());
      assertThat(asDirectory.mkdirs()).isTrue();
      assertThat(new File(asDirectory, "x").createNewFile()).isTrue();

      sm = stateMachine(server);
      assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry(DB_NAME, CONFIGURATION),
          SKIPPED_INDEX)).isCompletedExceptionally()).as("the write fails").isTrue();
      assertThat(sm.applyTransaction(rawEntry(RaftLogEntryCodec.encodeDatabaseConfigurationEntry("healthy", CONFIGURATION),
          LATER_INDEX)).isCompletedExceptionally()).as("another database is unaffected").isFalse();
      assertThat(sm.readAppliedIndexCounter()).isEqualTo(LATER_INDEX);

      assertThat(sm.takeSnapshot())
          .as("the configuration is in force in memory but not on disk, so a restart has to replay it")
          .isLessThan(SKIPPED_INDEX);
    } finally {
      server.stop();
      if (unwritable.isOpen())
        unwritable.drop();
      if (healthy.isOpen())
        healthy.drop();
    }
  }

  // ---- drivers ----

  private enum Kind {
    USERS, GROUPS, API_TOKENS
  }

  /** A later document of a kind other than the one that failed, so the failed one is not superseded by it. */
  private static Kind laterKind(final Skipped skipped) {
    return skipped == Skipped.SECURITY_GROUPS_NOT_PERSISTED ? Kind.USERS : Kind.GROUPS;
  }

  private Throwable failTheApply(final Skipped skipped) throws Exception {
    switch (skipped) {
    case RETRY_EXHAUSTED_ON_A_DATABASE:
      return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
        throw new ConcurrentModificationException("persistent page race");
      }));
    case RETRY_EXHAUSTED_WITHOUT_A_DATABASE:
      return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, () -> {
        throw new ConcurrentModificationException("persistent page race");
      }));
    case RETRY_INTERRUPTED:
      GlobalConfiguration.TX_RETRY_DELAY.setValue(50);
      Thread.currentThread().interrupt();
      return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
        throw new ConcurrentModificationException("page race");
      }));
    case BARE_REPLICATION_EXCEPTION_ON_A_DATABASE:
      return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
        throw new ReplicationException("Corrupted WAL transaction entry: invalid page count -1");
      }));
    case SEALED_SLICE_SEQUENCE_BROKEN:
      final LocalDatabase database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("sealed").toString()).create();
      try {
        database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME
            + " TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 1");
        final byte[] bytes = new byte[64];
        // Starts at offset 32 with nothing staged: the sequence it belongs to never reached this node
        final TsSealedChunk chunk = new TsSealedChunk(TYPE_NAME, 0, TYPE_NAME + "_shard_0.ts.sealed", 128L, 0L, 32L, bytes, false);
        return thrownBy(() -> sm.applyWithRetry(SKIPPED_INDEX, DB_NAME, () -> {
          try {
            sm.applySealedChunks(database, List.of(chunk));
          } catch (final IOException e) {
            throw new IllegalStateException(e);
          }
        }));
      } finally {
        database.drop();
      }
    case SECURITY_USERS_NOT_PERSISTED:
      return failedFuture(securityEntry(Kind.USERS, SKIPPED_INDEX));
    case SECURITY_GROUPS_NOT_PERSISTED:
      return failedFuture(securityEntry(Kind.GROUPS, SKIPPED_INDEX));
    case SECURITY_API_TOKENS_NOT_PERSISTED:
      return failedFuture(securityEntry(Kind.API_TOKENS, SKIPPED_INDEX));
    }
    throw new IllegalStateException("unhandled " + skipped);
  }

  private static Throwable thrownBy(final Runnable action) {
    try {
      action.run();
    } catch (final Throwable t) {
      return t;
    }
    throw new AssertionError("the apply did not fail");
  }

  private Throwable failedFuture(final TransactionContext entry) {
    final CompletableFuture<?> future = sm.applyTransaction(entry);
    assertThat(future.isCompletedExceptionally()).isTrue();
    try {
      future.join();
    } catch (final java.util.concurrent.CompletionException e) {
      return e.getCause();
    }
    throw new AssertionError("the apply did not fail");
  }

  /** A server whose security layer cannot persist the document the row names, once, and accepts everything else. */
  private static ArcadeDBServer securityServer(final Skipped skipped) {
    final AtomicBoolean failed = new AtomicBoolean();
    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("applyReplicatedUsers", args -> {
      if (skipped == Skipped.SECURITY_USERS_NOT_PERSISTED && failed.compareAndSet(false, true))
        throw new ReplicatedUsersPersistenceException("could NOT be persisted", new IOException("No space left on device"));
      return null;
    });
    security.on("applyReplicatedGroups", args -> {
      if (skipped == Skipped.SECURITY_GROUPS_NOT_PERSISTED && failed.compareAndSet(false, true))
        throw new ReplicatedSecurityConfigPersistenceException("could NOT be persisted", new IOException("No space left on device"));
      return null;
    });
    security.on("applyReplicatedApiTokens", args -> {
      if (skipped == Skipped.SECURITY_API_TOKENS_NOT_PERSISTED && failed.compareAndSet(false, true))
        throw new ReplicatedSecurityConfigPersistenceException("could NOT be persisted", new IOException("No space left on device"));
      return null;
    });
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.security(security);
    return server;
  }

  private ArcadeStateMachine stateMachine(final ArcadeDBServer server) {
    final ArcadeStateMachine machine = new ArcadeStateMachine();
    machine.setServer(server);
    try {
      machine.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    return machine;
  }

  private TransactionContext securityEntry(final Kind kind, final long index) {
    return rawEntry(switch (kind) {
      case USERS -> RaftLogEntryCodec.encodeSecurityUsersEntry(USERS_JSON);
      case GROUPS -> RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS_JSON);
      case API_TOKENS -> RaftLogEntryCodec.encodeSecurityApiTokensEntry(API_TOKENS_JSON);
    }, index);
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
    return (RaftServer) Proxy.newProxyInstance(Issue10086SkippedEntryHoldsTheCheckpointTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
