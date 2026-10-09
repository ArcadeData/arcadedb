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

import com.arcadedb.log.LogManager;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.util.AwaitToRun;

import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.util.logging.Level;

/**
 * Works around a deadlock in Apache Ratis 3.3.1 {@code SegmentedRaftLog.close()} (issue #9556, upstream RATIS-2719).
 * <p>
 * {@code close()} takes the log's write lock and then closes the cache-eviction {@link AwaitToRun}, which interrupts and
 * JOINS the eviction thread. When that thread was woken just before (a cache miss or a segment roll) and the cache was
 * over budget, it is parked in {@code writeLock()} inside {@code checkAndEvictCache()}. {@code ReentrantReadWriteLock.lock()}
 * ignores the interrupt, so the join never returns and the server close hangs forever, holding the write lock.
 * <p>
 * The upstream fix closes the eviction thread BEFORE taking the write lock. This class does the same thing from the
 * outside: it closes the {@code cacheEviction} {@link AwaitToRun} of every division's log while the caller holds no
 * lock, so the eviction thread can take the write lock, finish its pass and exit. {@code AwaitToRun.close()} is
 * idempotent, so the {@code close()} that Ratis runs later has nothing left to join. A signal sent after this point
 * (a cache miss during the rest of the shutdown) is a no-op: the cache simply stops being trimmed for the last few
 * milliseconds of the server's life.
 * <p>
 * The field is private, so it is read reflectively. A Ratis build without it (an upgrade that renamed it, or a native
 * image that did not register it) disables the workaround once, at WARNING, and the close proceeds exactly as before.
 * Remove this class once the Ratis dependency carries RATIS-2719.
 */
final class RaftLogCacheEviction {

  /**
   * How long {@link #stopBeforeClose} waits for each eviction thread to stop. A pass normally takes microseconds; the wait
   * only runs long when another thread holds the log's write lock, which a healthy server releases quickly.
   */
  static final long STOP_WAIT_MS = 10_000L;

  private static final String FIELD_NAME = "cacheEviction";

  private static volatile Field   cacheEvictionField;
  private static volatile boolean fieldUnavailable;

  private RaftLogCacheEviction() {
  }

  /**
   * Stops the cache-eviction thread of the log of every division of {@code server}, so the {@code close()} that follows
   * cannot deadlock with it. Never throws: a server that is already closed, or a division that cannot be read, is
   * skipped.
   */
  static void stopBeforeClose(final RaftServer server) {
    if (server == null)
      return;
    final Iterable<RaftGroupId> groupIds;
    try {
      groupIds = server.getGroupIds();
    } catch (final Throwable t) {
      LogManager.instance().log(RaftLogCacheEviction.class, Level.FINE, "Cannot list the Ratis groups before close: %s",
          t.toString());
      return;
    }
    for (final RaftGroupId groupId : groupIds) {
      final RaftLog log;
      try {
        log = server.getDivision(groupId).getRaftLog();
      } catch (final Throwable t) {
        // Not readable (never started, already closed or removed): Ratis will not close a log for it either
        continue;
      }
      stop(log, STOP_WAIT_MS);
    }
  }

  /**
   * Stops the cache-eviction thread of {@code log} and waits up to {@code timeoutMs} for it to exit.
   *
   * @return true when the log has no eviction thread left that its {@code close()} would join, false when it is not a
   * {@link SegmentedRaftLog}, when the field cannot be read, or when the thread did not stop in time
   */
  static boolean stop(final RaftLog log, final long timeoutMs) {
    final AwaitToRun eviction = evictionOf(log);
    if (eviction == null)
      return false;

    // AwaitToRun.close() joins without a bound. Run it on its own thread so a write lock held elsewhere (in the worst
    // case by a Ratis-initiated close that is already deadlocked) cannot hang the caller too.
    final Thread stopper = new Thread(eviction::close, eviction + "-stop");
    stopper.setDaemon(true);
    stopper.start();
    try {
      stopper.join(timeoutMs);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (stopper.isAlive()) {
      LogManager.instance().log(RaftLogCacheEviction.class, Level.WARNING,
          "The Raft log cache-eviction thread %s did not stop within %dms because another thread holds the log's write "
              + "lock; the Raft log close may hang (issue #9556)", eviction, timeoutMs);
      return false;
    }
    return true;
  }

  private static AwaitToRun evictionOf(final RaftLog log) {
    if (!(log instanceof SegmentedRaftLog) || fieldUnavailable)
      return null;
    try {
      Field field = cacheEvictionField;
      if (field == null) {
        field = SegmentedRaftLog.class.getDeclaredField(FIELD_NAME);
        field.setAccessible(true);
        cacheEvictionField = field;
      }
      return field.get(log) instanceof AwaitToRun eviction ? eviction : null;
    } catch (final ReflectiveOperationException | InaccessibleObjectException | SecurityException e) {
      fieldUnavailable = true;
      LogManager.instance().log(RaftLogCacheEviction.class, Level.WARNING,
          "Cannot read the Raft log cache-eviction thread (%s); closing a Raft log with an eviction in flight may hang "
              + "(issue #9556)", e.toString());
      return null;
    }
  }
}
