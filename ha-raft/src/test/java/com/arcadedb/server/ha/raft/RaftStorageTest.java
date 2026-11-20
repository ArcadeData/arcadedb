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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.AwaitToRun;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Raft storage and log: persistence across restarts, strict storage, bounded log cache, close/eviction and log writer recovery.
 */
class RaftStorageTest {

  /**
   * Regression test for issue #4835: the Raft storage directory was wiped and re-FORMATted on every
   * {@code RaftHAServer.start()} because {@code HA_RAFT_PERSIST_STORAGE} defaults to {@code false}.
   * In Kubernetes the storage lives on a PersistentVolume that survives pod restarts, so wiping it
   * forced a full resync and, on a single-seed cluster, risked re-forming a fresh empty cluster
   * (data loss / split-brain).
   * <p>
   * The fix makes Kubernetes deployments persist storage by default while still honoring an explicit
   * {@code raftPersistStorage=false}. This test exercises {@link RaftStorageFiles#resolvePersistStorage}
   * directly across the full decision matrix, without standing up a Ratis cluster.

   */
  @Nested
  class RaftStoragePersistence {

    private final String persistKey = GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getKey();

    @AfterEach
    void clearSystemProperty() {
      System.clearProperty(persistKey);
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void nonK8sDefaultsToPersistent() {
      // The default is now durable everywhere (not just K8s): wiping the Raft log on restart can turn a
      // lagging follower into a permanently diverged node on a cold restart, so storage is preserved
      // unless the operator explicitly opts into ephemeral. Outside Kubernetes, with nothing set, persist.
      final ContextConfiguration config = new ContextConfiguration();
      assertThat(RaftStorageFiles.resolvePersistStorage(config)).isTrue();
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void nonK8sHonorsExplicitEphemeral() {
      // A throwaway/test cluster can still opt into ephemeral storage explicitly.
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
      assertThat(RaftStorageFiles.resolvePersistStorage(config)).isFalse();
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void nonK8sHonorsExplicitPersist() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, true);
      assertThat(RaftStorageFiles.resolvePersistStorage(config)).isTrue();
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void k8sDefaultsToPersistent() {
      // The core fix: under Kubernetes, with no explicit raftPersistStorage, storage must be preserved.
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_K8S, true);
      assertThat(RaftStorageFiles.resolvePersistStorage(config)).isTrue();
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void k8sHonorsExplicitEphemeralViaContext() {
      // An operator who really wants ephemeral storage in K8s can still opt out explicitly.
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_K8S, true);
      config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
      assertThat(RaftStorageFiles.resolvePersistStorage(config)).isFalse();
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void k8sHonorsExplicitEphemeralViaSystemProperty() {
      System.setProperty(persistKey, "false");
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_K8S, true);
      assertThat(RaftStorageFiles.resolvePersistStorage(config)).isFalse();
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void k8sExplicitPersistStaysPersistent() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_K8S, true);
      config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, true);
      assertThat(RaftStorageFiles.resolvePersistStorage(config)).isTrue();
    }

    // Regression for #4835: RaftStoragePersistence
    @Test
    void isExplicitlyConfiguredDetectsContextAndSystemProperty() {
      final ContextConfiguration config = new ContextConfiguration();
      assertThat(RaftStorageFiles.isExplicitlyConfigured(config, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)).isFalse();

      config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
      assertThat(RaftStorageFiles.isExplicitlyConfigured(config, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)).isTrue();

      final ContextConfiguration empty = new ContextConfiguration();
      System.setProperty(persistKey, "false");
      assertThat(RaftStorageFiles.isExplicitlyConfigured(empty, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)).isTrue();
    }
  }

  /**
   * Regression test for item 2 of issue #7296.
   * <p>
   * {@code RaftHAServer.resolvePersistStorage} read {@code arcadedb.ha.raftPersistStorage} straight off
   * {@code System.getProperty} with {@code Boolean.parseBoolean}, which maps everything that is not the literal
   * {@code "true"} to {@code false}. So {@code -Darcadedb.ha.raftPersistStorage=yes} - an operator AFFIRMING that
   * the Raft log must survive a restart - silently made the storage ephemeral instead, and that setting's own
   * description says what that costs: wiping the Raft log on every restart turns a follower that was merely lagging
   * into a permanently diverged node on a full-cluster cold restart.
   * <p>
   * The property now goes through the same strict parse every other reader of raw configuration text uses. A value
   * that cannot be read is reported and the DEFAULT kept, which for this setting is the durable side.

   */
  @Nested
  class RaftPersistStorageStrict {

    private static final String KEY = GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getKey();

    @AfterEach
    void clearProperty() {
      System.clearProperty(KEY);
    }

    // Regression for #7296: RaftPersistStorageStrict
    @Test
    void anUnreadablePropertyKeepsTheDurableDefaultInsteadOfSilentlyDisablingPersistence() {
      System.setProperty(KEY, "yes");

      assertThat(RaftStorageFiles.resolvePersistStorage(new ContextConfiguration()))
          .as("'yes' is not boolean text: refuse it and keep the durable default, never read it as 'wipe the log'")
          .isTrue();
    }

    // Regression for #7296: RaftPersistStorageStrict
    @Test
    void aTypoKeepsTheDurableDefaultToo() {
      System.setProperty(KEY, "flase");

      assertThat(RaftStorageFiles.resolvePersistStorage(new ContextConfiguration())).isTrue();
    }

    /** An explicit, readable {@code false} is still honoured: a throwaway/test cluster may really want ephemeral. */
    // Regression for #7296: RaftPersistStorageStrict
    @Test
    void anExplicitFalseIsStillHonoured() {
      System.setProperty(KEY, "false");

      assertThat(RaftStorageFiles.resolvePersistStorage(new ContextConfiguration())).isFalse();
    }

    // Regression for #7296: RaftPersistStorageStrict
    @Test
    void anExplicitTrueIsStillHonoured() {
      System.setProperty(KEY, "TRUE");

      assertThat(RaftStorageFiles.resolvePersistStorage(new ContextConfiguration())).isTrue();
    }

    /** With no system property, the ContextConfiguration still decides, exactly as before. */
    // Regression for #7296: RaftPersistStorageStrict
    @Test
    void theConfigurationStillDecidesWhenNoPropertyIsSet() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);

      assertThat(RaftStorageFiles.resolvePersistStorage(configuration)).isFalse();
    }
  }

  /**
   * Issue #9549: a node restarted with a long Raft log tail ran out of a 1GB heap within seconds, before it had applied a
   * single entry of the catch-up. Ratis loads the last {@code raft.server.log.segment.cache.num.max} segment files into
   * the heap while it opens the log, regardless of the byte budget, and that count was the Ratis default of six. The log
   * written here ends with a non-empty open segment, which is one of those files, so the closed segments that come back
   * cached are one fewer than the configured count.
   * <p>
   * These tests write a real segmented Raft log, reopen it the way a restarted server does, and count the closed segments
   * that came back already decoded in the heap: reading the first entry of each, newest first, a cached segment answers
   * from the cache and an uncached one is a cache miss. Reading newest first keeps the eviction thread out of the count,
   * since only a miss wakes it and every cached segment has been read before the first miss.
   * <p>
   * Written against Apache Ratis 3.3.1: the cache-miss metric, the {@code log_<start>-<end>} file name of a closed segment
   * and the load-time caching rule are Ratis internals. If an upgrade turns the control case red, re-check
   * {@code SegmentedRaftLog.loadLogSegments} before touching {@link RaftPropertiesBuilder#cachedClosedSegmentsMax}.
   */
  @Nested
  class RaftLogCacheBound {
    private static final Pattern CLOSED_SEGMENT = Pattern.compile("log_(\\d+)-(\\d+)");
    private static final int     SEGMENTS       = 10;
    private static final int     ENTRY_BYTES    = 100 * 1024;

    @TempDir
    File tempDir;

    // Regression for #9549: RaftLogCacheBound
    @Test
    void ratisDefaultsLoadSixClosedSegmentsOnOpen() throws Exception {
      // the control: without the bound, the reopened log holds the Ratis default number of closed segments
      final RaftProperties writeProps = smallSegmentProperties(new ContextConfiguration());
      writeLog(writeProps);

      final RaftProperties ratisDefaults = smallSegmentProperties(new ContextConfiguration());
      ratisDefaults.unset(RaftServerConfigKeys.Log.SEGMENT_CACHE_NUM_MAX_KEY);
      ratisDefaults.unset(RaftServerConfigKeys.Log.SEGMENT_CACHE_SIZE_MAX_KEY);
      assertThat(cachedClosedSegmentsAfterReopen(ratisDefaults)).isEqualTo(RaftServerConfigKeys.Log.SEGMENT_CACHE_NUM_MAX_DEFAULT - 1);
    }

    // Regression for #9549: RaftLogCacheBound
    @Test
    void reopenedLogKeepsOnlyTheClosedSegmentsTheConfiguredBudgetAllows() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_LOG_CACHE_SIZE, "4MB"); // open segment + 3 closed 1MB segments
      final RaftProperties props = smallSegmentProperties(config);
      writeLog(props);

      assertThat(RaftServerConfigKeys.Log.segmentCacheNumMax(props)).isEqualTo(3);
      assertThat(cachedClosedSegmentsAfterReopen(props)).isEqualTo(2);
    }

    // Regression for #9549: RaftLogCacheBound
    @Test
    void reopenedLogKeepsOnlyTheClosedSegmentsTheHeapAllowsByDefault() throws Exception {
      // default budget: an eighth of the heap. A 24MB "heap" gives 3MB, the open segment and two closed 1MB segments
      final RaftProperties props = RaftPropertiesBuilder.build(smallSegmentConfig(new ContextConfiguration()), 24L * 1024 * 1024);
      writeLog(props);

      assertThat(RaftServerConfigKeys.Log.segmentCacheNumMax(props)).isEqualTo(2);
      assertThat(cachedClosedSegmentsAfterReopen(props)).isEqualTo(1);
    }

    private static ContextConfiguration smallSegmentConfig(final ContextConfiguration config) {
      config.setValue(GlobalConfiguration.HA_LOG_SEGMENT_SIZE, "1MB");
      config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "512KB");
      config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "1MB");
      return config;
    }

    private static RaftProperties smallSegmentProperties(final ContextConfiguration config) {
      return RaftPropertiesBuilder.build(smallSegmentConfig(config), 1024L * 1024 * 1024);
    }

    private void writeLog(final RaftProperties props) throws Exception {
      try (final RaftStorageImpl storage = newStorage(RaftStorage.StartupOption.FORMAT);
          final SegmentedRaftLog log = newLog(storage, props)) {
        log.open(RaftLog.INVALID_LOG_INDEX, null);
        final byte[] payload = new byte[ENTRY_BYTES];
        long index = 0;
        while (closedSegments(storage).size() < SEGMENTS) {
          final LogEntryProto entry = LogEntryProto.newBuilder().setTerm(1).setIndex(index++)
              .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(payload)))
              .build();
          log.appendEntry(entry).get(30, TimeUnit.SECONDS);
        }
        awaitCacheEvictionIdle(log.getName()); // every segment roll wakes it too
      }
    }

    private int cachedClosedSegmentsAfterReopen(final RaftProperties props) throws Exception {
      try (final RaftStorageImpl storage = newStorage(RaftStorage.StartupOption.RECOVER);
          final SegmentedRaftLog log = newLog(storage, props)) {
        log.open(RaftLog.INVALID_LOG_INDEX, null);
        final List<long[]> closed = closedSegments(storage);
        assertThat(closed).hasSizeGreaterThanOrEqualTo(SEGMENTS);

        final var misses = log.getRaftLogMetrics().getRegistry().counter(SegmentedRaftLogMetrics.RAFT_LOG_CACHE_MISS_COUNT);
        final long missesBefore = misses.getCount();
        for (int i = closed.size() - 1; i >= 0; i--)
          assertThat(log.get(closed.get(i)[0])).isNotNull();
        final int cached = (int) (closed.size() - (misses.getCount() - missesBefore));
        awaitCacheEvictionIdle(log.getName());
        return cached;
      }
    }

    /**
     * Every cache miss and every segment roll wakes the log's cache-eviction thread, and Ratis 3.3.1 {@code SegmentedRaftLog.close()} holds the
     * log's write lock while it joins that thread, which may itself be waiting for the write lock to evict: closing with
     * an eviction in flight hangs forever. Wait until the thread is back to waiting for a signal, and stays there.
     */
    private static void awaitCacheEvictionIdle(final String logName) throws InterruptedException {
      final String prefix = logName.substring(0, logName.lastIndexOf('-')) + "-cacheEviction";
      int idleObservations = 0;
      for (int attempt = 0; attempt < 600 && idleObservations < 5; attempt++) {
        boolean idle = true;
        for (final var thread : Thread.getAllStackTraces().entrySet())
          if (thread.getKey().getName().startsWith(prefix) && !waitsForSignal(thread.getValue()))
            idle = false;
        idleObservations = idle ? idleObservations + 1 : 0;
        Thread.sleep(50);
      }
      assertThat(idleObservations).as("cache eviction thread of %s never went idle", logName).isEqualTo(5);
    }

    private static boolean waitsForSignal(final StackTraceElement[] stack) {
      for (final StackTraceElement frame : stack) {
        if (frame.getMethodName().equals("checkAndEvictCache"))
          return false;
        if (frame.getClassName().endsWith("AwaitForSignal"))
          return true;
      }
      return false;
    }

    private RaftStorageImpl newStorage(final RaftStorage.StartupOption option) throws Exception {
      final RaftStorageImpl storage = StorageImplUtils.newRaftStorage(tempDir, SizeInBytes.valueOf(0), option,
          RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION);
      storage.initialize();
      return storage;
    }

    private static SegmentedRaftLog newLog(final RaftStorage storage, final RaftProperties props) {
      return SegmentedRaftLog.newBuilder()
          .setMemberId(RaftGroupMemberId.valueOf(RaftPeerId.valueOf("n9549"), RaftGroupId.randomId()))
          .setStorage(storage)
          .setProperties(props)
          .build();
    }

    /** @return {start, end} of each closed segment file, oldest first */
    private static List<long[]> closedSegments(final RaftStorage storage) throws Exception {
      final List<long[]> result = new ArrayList<>();
      for (final LogSegmentPath path : LogSegmentPath.getLogSegmentPaths(storage)) {
        final Matcher matcher = CLOSED_SEGMENT.matcher(path.getPath().getFileName().toString());
        if (matcher.matches())
          result.add(new long[] { Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)) });
      }
      return result;
    }
  }

  /**
   * Issue #9556: Apache Ratis 3.3.1 {@code SegmentedRaftLog.close()} takes the log's write lock and then joins the
   * cache-eviction thread. If that thread was woken just before and is waiting for the same write lock to evict, the join
   * never returns. These tests set that interleaving up deterministically: the closing thread takes the write lock
   * itself, reads segments that miss the cache until the eviction thread is parked on the lock, and only then closes.
   * <p>
   * The control case proves the hang on the Ratis build in use. When an upgrade carrying RATIS-2719 turns it red, delete
   * {@link RaftLogCacheEviction} and this class.
   */
  @Timeout(value = 5, unit = TimeUnit.MINUTES) // hang detector only
  @Nested
  class RaftLogCloseEvictionDeadlock {
    private static final Pattern CLOSED_SEGMENT = Pattern.compile("log_(\\d+)-(\\d+)");
    private static final int     SEGMENTS       = 10;
    private static final int     ENTRY_BYTES    = 100 * 1024;

    @TempDir
    File tempDir;

    private final List<Thread> stuck = new ArrayList<>();

    @AfterEach
    void unwindStuckClosers() throws InterruptedException {
      // A closer stuck in AwaitToRun.close() unwinds on interrupt: the join gives up, close() releases the write lock and
      // the eviction thread finishes. Without this a hung control case would keep the log's files open.
      for (final Thread thread : stuck) {
        thread.interrupt();
        thread.join(TimeUnit.SECONDS.toMillis(30));
      }
    }

    // Regression for #9556: RaftLogCloseEvictionDeadlock
    @Test
    void ratisCloseHangsWhenTheEvictionThreadWaitsForTheWriteLock() throws Exception {
      final RaftProperties props = writeLog();
      try (final RaftStorageImpl storage = newStorage(RaftStorage.StartupOption.RECOVER)) {
        final SegmentedRaftLog log = newLog(storage, props);
        log.open(RaftLog.INVALID_LOG_INDEX, null);

        final Thread closer = closeUnderWriteLockWithEvictionParked(log, storage);
        closer.join(TimeUnit.SECONDS.toMillis(5)); // expected to time out: no stall discount needed
        assertThat(closer.isAlive()).as("Ratis close() returned although the eviction thread waited for its write lock; "
            + "if the Ratis upgrade carries RATIS-2719, remove RaftLogCacheEviction").isTrue();
        assertThat(stackOf(closer)).as("the closer waits for the eviction thread").contains("AwaitToRun.close");
      }
    }

    // Regression for #9556: RaftLogCloseEvictionDeadlock
    @Test
    void closeCompletesWhenTheEvictionThreadWasStoppedFirst() throws Exception {
      final RaftProperties props = writeLog();
      try (final RaftStorageImpl storage = newStorage(RaftStorage.StartupOption.RECOVER)) {
        final SegmentedRaftLog log = newLog(storage, props);
        log.open(RaftLog.INVALID_LOG_INDEX, null);

        assertThat(RaftLogCacheEviction.stop(log, TimeUnit.SECONDS.toMillis(30))).isTrue();
        assertThat(evictionThreads(log)).isEmpty();

        // the same sequence as the control case: the cache misses now signal nobody, and the close has nothing to join
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread closer = new Thread(() -> {
          try (final AutoCloseableLock ignored = log.writeLock()) {
            readClosedSegmentsOldestFirst(log, storage);
            log.close();
          } catch (final Throwable t) {
            failure.set(t);
          }
        }, "issue9556-closer");
        closer.start();
        closer.join(TimeUnit.SECONDS.toMillis(60));
        if (closer.isAlive())
          stuck.add(closer);
        assertThat(closer.isAlive()).as("close() hung after the eviction thread was stopped").isFalse();
        assertThat(failure.get()).isNull();
      }
    }

    // Regression for #9556: RaftLogCloseEvictionDeadlock
    @Test
    void stopGivesUpWhileAnotherThreadHoldsTheWriteLock() throws Exception {
      final RaftProperties props = writeLog();
      try (final RaftStorageImpl storage = newStorage(RaftStorage.StartupOption.RECOVER)) {
        final SegmentedRaftLog log = newLog(storage, props);
        log.open(RaftLog.INVALID_LOG_INDEX, null);

        final CountDownLatch parked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread holder = new Thread(() -> {
          try (final AutoCloseableLock ignored = log.writeLock()) {
            readClosedSegmentsOldestFirst(log, storage);
            awaitEvictionParkedOnTheLock(log);
            parked.countDown();
            release.await();
          } catch (final Throwable t) {
            parked.countDown();
          }
        }, "issue9556-lock-holder");
        holder.start();
        assertThat(parked.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(evictionThreads(log)).as("eviction thread parked on the write lock").isNotEmpty();

        // bounded: the eviction thread cannot exit while the lock is held, and stop() must not wait for it forever
        assertThat(RaftLogCacheEviction.stop(log, 500)).isFalse();

        release.countDown();
        holder.join(TimeUnit.SECONDS.toMillis(30));
        awaitNoEvictionThread(log);
        // the stop that gave up still went through once the lock was free: close() has nothing left to join
        log.close();
      }
    }

    // Regression for #9556: RaftLogCloseEvictionDeadlock
    @Test
    void stopBeforeCloseStopsTheEvictionThreadOfARunningServer() throws Exception {
      // the entry point RaftHAServer calls before each of its Ratis server closes (stop, in-place restart, aborted restart)
      final RaftPeer peer = RaftPeer.newBuilder().setId("n9556").setAddress("127.0.0.1:" + allocateFreePorts(1)[0]).build();
      final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peer);
      final RaftProperties properties = new RaftProperties();
      RaftServerConfigKeys.setStorageDir(properties, List.of(new File(tempDir, "server")));
      GrpcConfigKeys.Server.setPort(properties, NetUtils.createSocketAddr(peer.getAddress()).getPort());
      RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);

      final RaftServer server = RaftServer.newBuilder().setServerId(peer.getId()).setGroup(group)
          .setStateMachine(new BaseStateMachine()).setProperties(properties).build();
      try {
        server.start();
        final RaftLog raftLog = server.getDivision(group.getGroupId()).getRaftLog();
        assertThat(raftLog).isInstanceOf(SegmentedRaftLog.class);
        final SegmentedRaftLog log = (SegmentedRaftLog) raftLog;
        assertThat(evictionThreads(log)).as("a running server has an eviction thread").isNotEmpty();

        RaftLogCacheEviction.stopBeforeClose(server);

        assertThat(evictionThreads(log)).isEmpty();
        assertThat(server.getLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);
      } finally {
        server.close();
      }
      assertThat(server.getLifeCycleState()).isEqualTo(LifeCycle.State.CLOSED);
    }

    /**
     * The field is read reflectively, which the GraalVM native image allows only for registered fields: without the entry
     * the workaround switches itself off in the native binary only.
     */
    // Regression for #9556: RaftLogCloseEvictionDeadlock
    @Test
    void nativeImageRegistersTheEvictionField() throws Exception {
      assertThat(SegmentedRaftLog.class.getDeclaredField("cacheEviction").getType()).isEqualTo(AwaitToRun.class);

      final Path metadata = Path.of("..", "native", "src", "main", "resources", "META-INF", "native-image", "com.arcadedb",
          "arcadedb-native", "reachability-metadata.json");
      final JSONArray reflection = new JSONObject(Files.readString(metadata)).getJSONArray("reflection");
      final Set<String> fields = new HashSet<>();
      for (int i = 0; i < reflection.length(); i++) {
        final JSONObject entry = reflection.getJSONObject(i);
        if (SegmentedRaftLog.class.getName().equals(entry.opt("type"))) {
          final JSONArray declared = entry.getJSONArray("fields", new JSONArray());
          for (int f = 0; f < declared.length(); f++)
            fields.add(declared.getJSONObject(f).getString("name", ""));
        }
      }
      assertThat(fields).as("register SegmentedRaftLog.cacheEviction in " + metadata).contains("cacheEviction");
    }

    // Regression for #9556: RaftLogCloseEvictionDeadlock
    @Test
    void stopIgnoresALogThatIsNotSegmented() {
      assertThat(RaftLogCacheEviction.stop(null, 100)).isFalse();
    }

    /**
     * Starts a thread that takes the log's write lock, reads closed segments that miss the cache until the eviction
     * thread wakes and parks on that lock, then closes the log while still holding it.
     */
    private Thread closeUnderWriteLockWithEvictionParked(final SegmentedRaftLog log, final RaftStorage storage)
        throws InterruptedException {
      final CountDownLatch closing = new CountDownLatch(1);
      final Thread closer = new Thread(() -> {
        try (final AutoCloseableLock ignored = log.writeLock()) {
          readClosedSegmentsOldestFirst(log, storage);
          awaitEvictionParkedOnTheLock(log);
          closing.countDown();
          log.close();
        } catch (final Throwable t) {
          closing.countDown();
        }
      }, "issue9556-closer");
      closer.setDaemon(true);
      stuck.add(closer);
      closer.start();
      assertThat(closing.await(60, TimeUnit.SECONDS)).isTrue();
      return closer;
    }

    private static void readClosedSegmentsOldestFirst(final SegmentedRaftLog log, final RaftStorage storage) throws Exception {
      // the newest segments are cached on open; the oldest miss, load into the cache, push it over budget and wake the
      // eviction thread
      for (final long[] segment : closedSegments(storage))
        assertThat(log.get(segment[0])).isNotNull();
    }

    private static void awaitEvictionParkedOnTheLock(final SegmentedRaftLog log) throws InterruptedException {
      for (int attempt = 0; attempt < 600; attempt++) {
        for (final Map.Entry<Thread, StackTraceElement[]> entry : evictionThreads(log).entrySet())
          if (entry.getKey().getState() == Thread.State.WAITING && contains(entry.getValue(), "checkAndEvictCache"))
            return;
        Thread.sleep(50);
      }
      throw new AssertionError("the cache-eviction thread of " + log.getName() + " never waited for the write lock");
    }

    private static void awaitNoEvictionThread(final SegmentedRaftLog log) throws InterruptedException {
      for (int attempt = 0; attempt < 600 && !evictionThreads(log).isEmpty(); attempt++)
        Thread.sleep(50);
      assertThat(evictionThreads(log)).isEmpty();
    }

    private static Map<Thread, StackTraceElement[]> evictionThreads(final SegmentedRaftLog log) {
      final String logName = log.getName();
      final String prefix = logName.substring(0, logName.lastIndexOf('-')) + "-cacheEviction";
      final Map<Thread, StackTraceElement[]> result = new HashMap<>();
      for (final Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet())
        if (entry.getKey().getName().startsWith(prefix) && !entry.getKey().getName().endsWith("-stop") && entry.getKey().isAlive())
          result.put(entry.getKey(), entry.getValue());
      return result;
    }

    private static boolean contains(final StackTraceElement[] stack, final String methodName) {
      for (final StackTraceElement frame : stack)
        if (frame.getMethodName().equals(methodName))
          return true;
      return false;
    }

    private static String stackOf(final Thread thread) {
      final StringBuilder sb = new StringBuilder();
      for (final StackTraceElement frame : thread.getStackTrace())
        sb.append(frame.getClassName(), frame.getClassName().lastIndexOf('.') + 1, frame.getClassName().length())
            .append('.').append(frame.getMethodName()).append('\n');
      return sb.toString();
    }

    /** Writes ten closed 1MB segments and returns properties that cache only three of them. */
    private RaftProperties writeLog() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_LOG_SEGMENT_SIZE, "1MB");
      config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "512KB");
      config.setValue(GlobalConfiguration.HA_WRITE_BUFFER_SIZE, "1MB");
      config.setValue(GlobalConfiguration.HA_LOG_CACHE_SIZE, "4MB");
      final RaftProperties props = RaftPropertiesBuilder.build(config, 1024L * 1024 * 1024);

      try (final RaftStorageImpl storage = newStorage(RaftStorage.StartupOption.FORMAT);
          final SegmentedRaftLog log = newLog(storage, props)) {
        log.open(RaftLog.INVALID_LOG_INDEX, null);
        // writing rolls segments, and every roll wakes the eviction thread: stop it first so this close cannot hang
        RaftLogCacheEviction.stop(log, TimeUnit.SECONDS.toMillis(30));
        final byte[] payload = new byte[ENTRY_BYTES];
        long index = 0;
        while (closedSegments(storage).size() < SEGMENTS) {
          final LogEntryProto entry = LogEntryProto.newBuilder().setTerm(1).setIndex(index++)
              .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(payload)))
              .build();
          log.appendEntry(entry).get(30, TimeUnit.SECONDS);
        }
      }
      return props;
    }

    private RaftStorageImpl newStorage(final RaftStorage.StartupOption option) throws Exception {
      final RaftStorageImpl storage = StorageImplUtils.newRaftStorage(tempDir, SizeInBytes.valueOf(0), option,
          RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION);
      storage.initialize();
      return storage;
    }

    private static SegmentedRaftLog newLog(final RaftStorage storage, final RaftProperties props) {
      return SegmentedRaftLog.newBuilder()
          .setMemberId(RaftGroupMemberId.valueOf(RaftPeerId.valueOf("n9556"), RaftGroupId.randomId()))
          .setStorage(storage)
          .setProperties(props)
          .build();
    }

    /** @return {start, end} of each closed segment file, oldest first */
    private static List<long[]> closedSegments(final RaftStorage storage) throws Exception {
      final List<long[]> result = new ArrayList<>();
      for (final LogSegmentPath path : LogSegmentPath.getLogSegmentPaths(storage)) {
        final Matcher matcher = CLOSED_SEGMENT.matcher(path.getPath().getFileName().toString());
        if (matcher.matches())
          result.add(new long[] { Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)) });
      }
      return result;
    }
  }

  /**
   * Regression tests for issue #7037 (follow-up to #5345): a follower whose Raft log writer failed persistently -
   * {@code No space left on device} being the reported cause - stays {@code RUNNING} while rejecting every append,
   * and nothing recovered it without an operator restart. The state machine now keeps the failure Ratis reports
   * through {@code notifyLogFailed}, and the health monitor restarts the server in place once the volume has room,
   * bounded per episode so a failure that is not about space does not restart the node on every tick forever.

   */
  @Nested
  class LogWriterRecovery {

    /** A target whose log writer failure and volume state the test drives; a restart clears the failure unless pinned. */
    static final class FakeTarget implements HealthMonitor.HealthTarget {
      final    AtomicInteger restarts        = new AtomicInteger();
      volatile String        logFailure      = null;
      volatile boolean       storageWritable = true;
      volatile boolean       failureSurvivesRestart = false;

      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return LifeCycle.State.RUNNING;
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
        restarts.incrementAndGet();
        if (!failureSurvivesRestart)
          logFailure = null; // a restart builds a fresh state machine, which is what clears the mark
      }

      @Override
      public String getRaftLogFailure() {
        return logFailure;
      }

      @Override
      public boolean isRaftStorageWritable() {
        return storageWritable;
      }
    }

    private static HealthMonitor monitor(final FakeTarget target, final int restartThreshold, final AtomicLong clock) {
      final HealthMonitor monitor = new HealthMonitor(target, 1000L, 0L, 0L, false, 0, restartThreshold);
      monitor.setClock(clock::get);
      return monitor;
    }

    // Regression for #7037: LogWriterRecovery
    @Test
    void healthyLogWriterNeverRestarts() {
      final FakeTarget target = new FakeTarget();
      final HealthMonitor monitor = monitor(target, 10, new AtomicLong(1_000L));

      monitor.tick();
      monitor.tick();

      assertThat(target.restarts.get()).isZero();
    }

    // Regression for #7037: LogWriterRecovery
    @Test
    void restartIsDeferredWhileTheVolumeIsStillFullAndFiredOnceItHasRoom() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(1_000L);
      final HealthMonitor monitor = monitor(target, 10, clock);

      target.logFailure = "at index 4946: java.io.IOException: No space left on device";
      target.storageWritable = false;
      monitor.tick();
      monitor.tick();
      assertThat(target.restarts.get()).as("a restart on a full volume wedges again at the first append").isZero();

      // The periodic compaction freed the segments below the applied index.
      target.storageWritable = true;
      monitor.tick();
      assertThat(target.restarts.get()).isEqualTo(1);
      assertThat(target.logFailure).isNull();

      monitor.tick();
      assertThat(target.restarts.get()).as("the fresh state machine carries no failure: nothing more to do").isEqualTo(1);
    }

    // Regression for #7037: LogWriterRecovery
    @Test
    void restartsAreBoundedPerEpisodeWhenTheFailureIsNotAboutSpace() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(1_000L);
      final HealthMonitor monitor = monitor(target, 2, clock);

      target.logFailure = "at index 12: java.io.IOException: Input/output error";
      target.failureSurvivesRestart = true; // a dying disk: every restart fails the same way

      for (int i = 0; i < 6; i++) {
        monitor.tick();
        clock.addAndGet(1_000L);
      }
      assertThat(target.restarts.get()).as("the crash-loop budget bounds the in-place restarts").isEqualTo(2);

      // Time alone never re-arms an incident that is still going on: the failure is present on every tick.
      clock.addAndGet(2 * HealthMonitor.LOG_FAILURE_EPISODE_RESET_MS);
      monitor.tick();
      monitor.tick();
      assertThat(target.restarts.get()).as("a persisting failure keeps the exhausted budget exhausted").isEqualTo(2);
    }

    // Regression for #7037: LogWriterRecovery
    @Test
    void budgetReArmsOnlyAfterTheWriterHasBeenHealthyForTheResetWindow() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(1_000L);
      final HealthMonitor monitor = monitor(target, 2, clock);

      // Two incidents in quick succession: each restart clears the mark, the disk fills again right away.
      for (int i = 0; i < 2; i++) {
        target.logFailure = "at index " + i + ": java.io.IOException: No space left on device";
        monitor.tick();
        clock.addAndGet(1_000L);
      }
      assertThat(target.restarts.get()).isEqualTo(2);

      // The failure comes back within the window: same incident, budget spent, no restart.
      target.logFailure = "at index 9: java.io.IOException: No space left on device";
      monitor.tick();
      assertThat(target.restarts.get()).isEqualTo(2);
      target.logFailure = null; // an operator restart cleared it

      // The writer stays healthy for the whole window: the incident is over.
      for (int i = 0; i < 3; i++) {
        monitor.tick();
        clock.addAndGet(HealthMonitor.LOG_FAILURE_EPISODE_RESET_MS / 2);
      }

      // A new incident much later is recovered again.
      target.logFailure = "at index 40: java.io.IOException: No space left on device";
      monitor.tick();
      assertThat(target.restarts.get()).isEqualTo(3);
      assertThat(target.logFailure).isNull();
    }

    // Regression for #7037: LogWriterRecovery
    @Test
    void aZeroThresholdKeepsTheLegacyUnboundedBehaviour() {
      final FakeTarget target = new FakeTarget();
      final HealthMonitor monitor = monitor(target, 0, new AtomicLong(1_000L));

      target.logFailure = "at index 12: java.io.IOException: Input/output error";
      target.failureSurvivesRestart = true;
      for (int i = 0; i < 4; i++)
        monitor.tick();

      assertThat(target.restarts.get()).isEqualTo(4);
    }

    // Regression for #7037: LogWriterRecovery
    @Test
    void stateMachineKeepsTheFirstFailureRatisReports() {
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      assertThat(sm.getRaftLogFailure()).isNull();

      sm.notifyLogFailed(new IOException("No space left on device"), LogEntryProto.newBuilder().setTerm(3).setIndex(4946).build());
      final ArcadeStateMachine.RaftLogFailure first = sm.getRaftLogFailure();
      assertThat(first).isNotNull();
      assertThat(first.index()).isEqualTo(4946L);
      assertThat(first.describe()).contains("at index 4946").contains("No space left on device");

      // Ratis reports every later task against the pinned exception; the first failure is the one that matters.
      sm.notifyLogFailed(new IOException("Log already failed at index 4946"), LogEntryProto.newBuilder().setTerm(3).setIndex(4947).build());
      assertThat(sm.getRaftLogFailure()).isSameAs(first);

      // A whole-segment failure carries no entry.
      final ArcadeStateMachine other = new ArcadeStateMachine();
      other.notifyLogFailed(new IOException("No space left on device"), null);
      assertThat(other.getRaftLogFailure().index()).isEqualTo(-1L);
      assertThat(other.getRaftLogFailure().describe()).startsWith("on a log segment");
    }
  }
}
