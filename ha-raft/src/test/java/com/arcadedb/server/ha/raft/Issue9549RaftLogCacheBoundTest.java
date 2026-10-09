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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

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
 */
class Issue9549RaftLogCacheBoundTest {
  private static final Pattern CLOSED_SEGMENT = Pattern.compile("log_(\\d+)-(\\d+)");
  private static final int     SEGMENTS       = 10;
  private static final int     ENTRY_BYTES    = 100 * 1024;

  @TempDir
  File tempDir;

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

  @Test
  void reopenedLogKeepsOnlyTheClosedSegmentsTheConfiguredBudgetAllows() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_LOG_CACHE_SIZE, "4MB"); // open segment + 3 closed 1MB segments
    final RaftProperties props = smallSegmentProperties(config);
    writeLog(props);

    assertThat(RaftServerConfigKeys.Log.segmentCacheNumMax(props)).isEqualTo(3);
    assertThat(cachedClosedSegmentsAfterReopen(props)).isEqualTo(2);
  }

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
      return (int) (closed.size() - (misses.getCount() - missesBefore));
    }
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
