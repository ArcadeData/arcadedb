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
package com.arcadedb.server.ha.raft.ratis;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6991.
 * <p>
 * ArcadeDB's Raft snapshot is the set of database files on disk; the {@code snapshot.<term>_<index>}
 * file Ratis rediscovers is a zero-byte marker with no {@code .md5} companion by design, so
 * {@code SimpleStateMachineStorage.cleanupOldSnapshots()} logs "Snapshot file ... has missing MD5 file."
 * at WARNING for every marker, on every checkpoint and every restart. That line is pure noise here and
 * reads to an operator like a corrupted snapshot.
 * <p>
 * It cannot be silenced with a level pin in {@code arcadedb-log.properties} the way the
 * {@code GrpcLogAppender} retry flood is, because the same logger also carries
 * "Failed to updateLatestSnapshot from ...", a genuine I/O failure on the snapshot directory that must
 * stay visible. Hence the per-message {@link RatisSnapshotDigestWarningFilter}.
 * <p>
 * These tests drive the real Ratis {@link SimpleStateMachineStorage} through a real
 * {@link ArcadeStateMachine} over a real {@link RaftStorage} in a temp directory - no mocked logging
 * stack - and count what actually reaches a {@link Handler} on the Ratis logger.
 */
class RatisSnapshotDigestWarningFilterTest {
  private static final String MISSING_DIGEST_TEXT = "has missing MD5 file";

  private Logger            ratisLogger;
  private Filter            originalFilter;
  private Level             originalLevel;
  private boolean           originalUseParentHandlers;
  private CollectingHandler handler;

  /**
   * Everything {@link #initializedStateMachine(Path)} opens, closed in {@link #restoreTheRatisLogger()}.
   * {@link ArcadeStateMachine#close()} shuts down its lifecycle and snapshot-install executors, which
   * would otherwise outlive every test method that created one.
   */
  private final List<Closeable> opened = new ArrayList<>();

  @BeforeEach
  void attachHandlerToTheRatisLogger() {
    ratisLogger = Logger.getLogger(RatisSnapshotDigestWarningFilter.RATIS_SNAPSHOT_STORAGE_LOGGER);
    originalFilter = ratisLogger.getFilter();
    originalLevel = ratisLogger.getLevel();
    originalUseParentHandlers = ratisLogger.getUseParentHandlers();

    // A pristine, self-contained logger: no inherited filter, everything loggable, nothing escaping to
    // the console handlers of a full-suite run.
    ratisLogger.setFilter(null);
    ratisLogger.setLevel(Level.ALL);
    ratisLogger.setUseParentHandlers(false);

    handler = new CollectingHandler();
    ratisLogger.addHandler(handler);
  }

  @AfterEach
  void restoreTheRatisLogger() throws IOException {
    // Close in reverse order: each state machine before the storage it was initialized against.
    for (int i = opened.size() - 1; i >= 0; i--)
      opened.get(i).close();
    opened.clear();

    ratisLogger.removeHandler(handler);
    ratisLogger.setFilter(originalFilter);
    ratisLogger.setLevel(originalLevel);
    ratisLogger.setUseParentHandlers(originalUseParentHandlers);
  }

  /**
   * Arming check: the warning really is emitted by the real code path, so the suppression assertions
   * below cannot pass vacuously.
   */
  @Test
  void withoutTheFilterTheMissingDigestWarningReachesTheLog(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = initializedStateMachine(tempDir);
    // initialize() arms the filter in production; drop it to observe the raw Ratis behaviour.
    ratisLogger.setFilter(null);

    registerMarkerAt(sm, 1L, 7293L);

    assertThat(handler.messagesContaining(MISSING_DIGEST_TEXT))
        .as("stock Ratis warns once per md5-less snapshot marker")
        .isNotEmpty();
  }

  @Test
  void initializeArmsTheFilterAndTheMissingDigestWarningIsSuppressed(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = initializedStateMachine(tempDir);

    assertThat(ratisLogger.getFilter())
        .as("ArcadeStateMachine.initialize() installs the per-message filter")
        .isInstanceOf(RatisSnapshotDigestWarningFilter.class);

    registerMarkerAt(sm, 1L, 7293L);

    // The marker really was written, so the code path that warns really did run.
    assertThat(sm.getStateMachineStorage().getLatestSnapshot().getIndex()).isEqualTo(7293L);
    assertThat(handler.messagesContaining(MISSING_DIGEST_TEXT))
        .as("the by-design missing-digest warning must not reach the log")
        .isEmpty();
  }

  /**
   * Every checkpoint runs the warn loop again over the md5-less marker it just wrote, so the warning
   * recurs for as long as the node keeps checkpointing - the growth reported in the issue. (Before
   * #7209 it grew per checkpoint AND per accumulated marker, because none were ever pruned; now the
   * directory holds one, so it is once per checkpoint.) All of it stays hidden.
   */
  @Test
  void everyRediscoveredMarkerStaysSuppressed(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = initializedStateMachine(tempDir);

    registerMarkerAt(sm, 1L, 7293L);
    registerMarkerAt(sm, 1L, 7400L);

    assertThat(handler.messagesContaining(MISSING_DIGEST_TEXT)).isEmpty();
  }

  /**
   * The second call site is Ratis's own: {@code StateMachineUpdater.takeSnapshot()} calls
   * {@code cleanupOldSnapshots(snapshotRetentionPolicy)} straight after the state machine's
   * {@code takeSnapshot()}, with no ArcadeDB code in between. The filter is scoped to the logger rather
   * than to a call site, so that path is covered too - this pins it.
   */
  @Test
  void theRatisInitiatedCleanupIsSuppressedToo(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = initializedStateMachine(tempDir);
    registerMarkerAt(sm, 1L, 7293L);

    // Exactly what StateMachineUpdater.takeSnapshot() invokes.
    sm.getStateMachineStorage().cleanupOldSnapshots(new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 1;
      }
    });

    assertThat(handler.messagesContaining(MISSING_DIGEST_TEXT)).isEmpty();
  }

  /**
   * The production sequence: {@code ArcadeStateMachine.initialize()} installs, and then
   * {@code RaftHAServer.start()} installs again a moment later on the same JVM. The second call must
   * leave the first filter in place rather than wrap it, or every subsequent restart in a long-lived
   * process would add another layer. Unlike {@link #installIsIdempotentAndDoesNotStackFilters}, this
   * drives the first install through the real state machine and then re-checks suppression end to end.
   */
  @Test
  void theSecondInstallFromRaftHAServerStartChangesNothing(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = initializedStateMachine(tempDir);
    final Filter afterStateMachineInit = ratisLogger.getFilter();
    assertThat(afterStateMachineInit).isInstanceOf(RatisSnapshotDigestWarningFilter.class);

    // What RaftHAServer.start() does, next to its org.apache.ratis level pin.
    RatisSnapshotDigestWarningFilter.install();

    assertThat(ratisLogger.getFilter()).isSameAs(afterStateMachineInit);
    assertThat(((RatisSnapshotDigestWarningFilter) ratisLogger.getFilter()).getDelegate())
        .as("the second install must not wrap the first")
        .isNull();

    registerMarkerAt(sm, 1L, 7293L);
    assertThat(handler.messagesContaining(MISSING_DIGEST_TEXT)).isEmpty();
  }

  @Test
  void aGenuineStorageWarningFromTheSameLoggerStillReachesTheLog(@TempDir final Path tempDir) throws Exception {
    initializedStateMachine(tempDir);

    // The other WARNING this logger emits (SimpleStateMachineStorage.loadLatestSnapshot): a real I/O
    // failure reading the snapshot directory, which the acceptance criteria require to stay visible.
    LoggerFactory.getLogger(SimpleStateMachineStorage.class)
        .warn("Failed to updateLatestSnapshot from {}", tempDir);

    assertThat(handler.messagesContaining("Failed to updateLatestSnapshot")).isNotEmpty();
  }

  @Test
  void onlyWarningAndBelowIsSuppressed() {
    final RatisSnapshotDigestWarningFilter filter = new RatisSnapshotDigestWarningFilter(null);

    assertThat(filter.isLoggable(new LogRecord(Level.WARNING, "Snapshot file x has missing MD5 file."))).isFalse();
    assertThat(filter.isLoggable(new LogRecord(Level.INFO, "Snapshot file x has missing MD5 file."))).isFalse();
    // A future Ratis that raised the same text to SEVERE would still be heard.
    assertThat(filter.isLoggable(new LogRecord(Level.SEVERE, "Snapshot file x has missing MD5 file."))).isTrue();
    assertThat(filter.isLoggable(new LogRecord(Level.WARNING, "Failed to updateLatestSnapshot from /x"))).isTrue();
  }

  @Test
  void aNullMessageIsNotSuppressed() {
    final RatisSnapshotDigestWarningFilter filter = new RatisSnapshotDigestWarningFilter(null);

    assertThat(filter.isLoggable(new LogRecord(Level.WARNING, null))).isTrue();
  }

  @Test
  void installIsIdempotentAndDoesNotStackFilters() {
    RatisSnapshotDigestWarningFilter.install();
    final Filter first = ratisLogger.getFilter();

    RatisSnapshotDigestWarningFilter.install();

    assertThat(ratisLogger.getFilter()).isSameAs(first);
    assertThat(((RatisSnapshotDigestWarningFilter) first).getDelegate()).isNull();
  }

  @Test
  void aPreexistingFilterIsChainedNotReplaced() {
    ratisLogger.setFilter(record -> !record.getMessage().contains("dropped-by-the-operator"));

    RatisSnapshotDigestWarningFilter.install();

    final Filter installed = ratisLogger.getFilter();
    assertThat(installed).isInstanceOf(RatisSnapshotDigestWarningFilter.class);
    assertThat(installed.isLoggable(new LogRecord(Level.WARNING, "Snapshot file x has missing MD5 file."))).isFalse();
    assertThat(installed.isLoggable(new LogRecord(Level.WARNING, "dropped-by-the-operator"))).isFalse();
    assertThat(installed.isLoggable(new LogRecord(Level.WARNING, "Failed to updateLatestSnapshot from /x"))).isTrue();
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * A real {@link ArcadeStateMachine} initialized against a real, formatted {@link RaftStorage} rooted
   * at {@code tempDir}, exactly as Ratis initializes it at boot.
   */
  private ArcadeStateMachine initializedStateMachine(final Path tempDir) throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());

    final RaftStorage raftStorage = RaftStorage.newBuilder()
        .setDirectory(tempDir.resolve("raft-storage").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
    opened.add(raftStorage);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    opened.add(sm);
    sm.setServer(new ArcadeDBServer(config));
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
    return sm;
  }

  /**
   * Reproduces one production checkpoint: the state machine writes a real zero-byte
   * {@code snapshot.<term>_<index>} marker through its own registration path, and then Ratis's
   * {@code StateMachineUpdater.takeSnapshot()} calls {@code cleanupOldSnapshots(snapshotRetentionPolicy)}
   * on it with no ArcadeDB code in between (ratis-server 3.3.0, {@code StateMachineUpdater.java:301}).
   * <p>
   * Both halves are needed. {@code cleanupOldSnapshots()} is the only place the "has missing MD5 file."
   * warning is emitted, and #7209 removed ArcadeDB's own call to it from {@code registerSnapshotMarker}
   * - it deleted nothing, because Ratis's retention needs an {@code .md5} companion ArcadeDB does not
   * write. Registration alone would therefore reach no warn loop at all, and every suppression
   * assertion built on this helper would pass whether the filter worked or not.
   */
  private static void registerMarkerAt(final ArcadeStateMachine sm, final long term, final long index) throws Exception {
    final Method m = ArcadeStateMachine.class.getDeclaredMethod("registerSnapshotMarker", long.class, long.class);
    m.setAccessible(true);
    assertThat((Boolean) m.invoke(sm, term, index)).as("snapshot marker written").isTrue();

    sm.getStateMachineStorage().cleanupOldSnapshots(new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 1;
      }
    });
  }

  /**
   * Minimal {@link RaftServer} stub: {@code BaseStateMachine.initialize()} only needs a non-null
   * {@code getId()}; nothing else is reached on this path.
   */
  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        RatisSnapshotDigestWarningFilterTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  /**
   * Records everything the Ratis logger actually publishes, i.e. everything that survived the filter.
   */
  private static final class CollectingHandler extends Handler {
    private final List<String> messages = new CopyOnWriteArrayList<>();

    private CollectingHandler() {
      setLevel(Level.ALL);
    }

    @Override
    public void publish(final LogRecord record) {
      if (record.getMessage() != null)
        messages.add(record.getMessage());
    }

    private List<String> messagesContaining(final String text) {
      return messages.stream().filter(m -> m.contains(text)).toList();
    }

    @Override
    public void flush() {
      // nothing buffered
    }

    @Override
    public void close() {
      // nothing to release
    }
  }
}
