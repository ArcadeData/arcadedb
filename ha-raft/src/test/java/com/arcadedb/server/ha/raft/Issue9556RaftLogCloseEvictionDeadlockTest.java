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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;

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
class Issue9556RaftLogCloseEvictionDeadlockTest {
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
