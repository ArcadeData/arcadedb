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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The transactions this node originated that are in flight between replication and the publication of their pages,
 * keyed by database and WAL transaction id (issue #6965). See {@link LocalCommit} for the handshake.
 * <p>
 * Bounded by construction: a transaction leaves the registry when the apply thread claims it or the committing thread
 * withdraws it, and every exit of the commit path does one or the other (a {@code finally} withdraws whatever is
 * still registered).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LocalCommitRegistry {
  // Per database, keyed by WAL transaction id: no key object is built on the commit path beyond the boxed id.
  private final Map<String, ConcurrentHashMap<Long, LocalCommit>> byDatabase = new ConcurrentHashMap<>();

  /**
   * @return {@code false} when a transaction with the same id is already registered for the database - the WAL
   * transaction id is a per-database counter, so that is a bug, and the caller must not proceed on a slot it does
   * not own
   */
  boolean register(final LocalCommit commit) {
    final ConcurrentHashMap<Long, LocalCommit> commits = byDatabase.computeIfAbsent(commit.databaseName(),
        k -> new ConcurrentHashMap<>());
    return commits.putIfAbsent(commit.walTxId(), commit) == null;
  }

  /**
   * Hands the transaction to the apply thread, if it is the one that shipped exactly these bytes and its committing
   * thread has not withdrawn it in the meantime.
   *
   * @return the claimed transaction, or {@code null} when nothing matching is registered or it was withdrawn
   */
  LocalCommit claim(final String databaseName, final long walTxId, final byte[] walData) {
    final ConcurrentHashMap<Long, LocalCommit> commits = byDatabase.get(databaseName);
    if (commits == null || commits.isEmpty())
      return null;
    final LocalCommit commit = commits.get(walTxId);
    if (commit == null || !Arrays.equals(commit.walData(), walData) || !commit.claim())
      return null;
    commits.remove(walTxId, commit);
    return commit;
  }

  /**
   * Takes the transaction back from the apply thread, if it has not claimed it yet.
   *
   * @return {@code false} when the apply thread already owns it: the entry committed and its outcome must be awaited
   */
  boolean withdraw(final LocalCommit commit) {
    if (!commit.withdraw())
      return false;
    final ConcurrentHashMap<Long, LocalCommit> commits = byDatabase.get(commit.databaseName());
    if (commits != null)
      commits.remove(commit.walTxId(), commit);
    return true;
  }

  int size() {
    int size = 0;
    for (final ConcurrentHashMap<Long, LocalCommit> commits : byDatabase.values())
      size += commits.size();
    return size;
  }

  /** Age in milliseconds of the oldest registration still waiting for the apply thread, {@code 0} when none. */
  long oldestRegisteredMs() {
    long oldest = Long.MAX_VALUE;
    for (final ConcurrentHashMap<Long, LocalCommit> commits : byDatabase.values())
      for (final LocalCommit commit : commits.values())
        if (commit.registeredAtMs() < oldest)
          oldest = commit.registeredAtMs();
    return oldest == Long.MAX_VALUE ? 0L : Math.max(0L, System.currentTimeMillis() - oldest);
  }
}
