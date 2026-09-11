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

import com.arcadedb.log.LogManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * The transactions this node originated that are in flight between replication and the publication of their pages,
 * keyed by database and WAL transaction id (issue #6965). See {@link LocalCommit} for the handshake.
 * <p>
 * Bounded by construction: a transaction leaves the registry when the apply thread claims it or the committing thread
 * withdraws it, and every exit of the commit path does one or the other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LocalCommitRegistry {
  private final Map<String, LocalCommit> commits = new ConcurrentHashMap<>();

  private static String key(final String databaseName, final long walTxId) {
    return databaseName + "/" + walTxId;
  }

  LocalCommit register(final LocalCommit commit) {
    final LocalCommit previous = commits.put(key(commit.databaseName(), commit.walTxId()), commit);
    if (previous != null)
      // The WAL transaction id is a per-database counter, so this cannot happen; if it ever does, the earlier
      // transaction is the one that lost its slot, and its committing thread will treat the failed withdraw as a
      // claim it must wait for. Say so, loudly.
      LogManager.instance().log(this, Level.SEVERE,
          "Transaction id %d on database '%s' was registered twice for replication; the earlier registration is lost",
          commit.walTxId(), commit.databaseName());
    return commit;
  }

  /**
   * Hands the transaction to the apply thread, if its committing thread has not withdrawn it in the meantime.
   *
   * @return the claimed transaction, or {@code null} when nothing is registered under that id or it was withdrawn
   */
  LocalCommit claim(final String databaseName, final long walTxId) {
    if (commits.isEmpty())
      return null;
    final String key = key(databaseName, walTxId);
    final LocalCommit commit = commits.get(key);
    if (commit == null || !commit.claim())
      return null;
    commits.remove(key, commit);
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
    commits.remove(key(commit.databaseName(), commit.walTxId()), commit);
    return true;
  }

  int size() {
    return commits.size();
  }

  /** Age in milliseconds of the oldest registration still waiting for the apply thread, {@code 0} when none. */
  long oldestRegisteredMs() {
    long oldest = Long.MAX_VALUE;
    for (final LocalCommit commit : commits.values())
      if (commit.registeredAtMs() < oldest)
        oldest = commit.registeredAtMs();
    return oldest == Long.MAX_VALUE ? 0L : Math.max(0L, System.currentTimeMillis() - oldest);
  }
}
