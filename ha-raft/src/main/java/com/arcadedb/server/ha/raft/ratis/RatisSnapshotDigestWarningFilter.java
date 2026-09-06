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

import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Suppresses the single by-design WARNING that Apache Ratis's
 * {@code org.apache.ratis.statemachine.impl.SimpleStateMachineStorage} emits for every ArcadeDB
 * snapshot marker (issue #6991):
 *
 * <pre>
 * Snapshot file SingleFileSnapshotInfo(t:1, i:7293):[.../sm/snapshot.1_7293] has missing MD5 file.
 * </pre>
 *
 * <p>ArcadeDB's real snapshot is the set of database files on disk - every committed transaction is
 * already durably flushed by the {@code TransactionManager} - so the file Ratis rediscovers is a
 * zero-byte placeholder whose <em>name</em> carries the {@code (term, index)} that Ratis's
 * snapshot-index bookkeeping and log-purge contract point at. No {@code .md5} companion is written on
 * purpose, and ArcadeDB never exercises Ratis's chunk-verification path (a follower resyncs over HTTP
 * through {@code DatabaseReconciler}), so the missing digest is expected and harmless. Ratis has no way
 * to know that, and operators reasonably read the warning as a corrupted or incomplete snapshot.
 *
 * <p><b>Why this is a filter and not a line in {@code arcadedb-log.properties}.</b> The
 * {@code GrpcLogAppender} retry flood is silenced wholesale with a {@code .level = SEVERE} entry there,
 * because that logger has nothing else worth hearing at WARNING. This one does: the same
 * {@code SimpleStateMachineStorage} logger also emits {@code "Failed to updateLatestSnapshot from ..."}
 * plus a directory listing when it cannot read the snapshot directory, which is a genuine I/O failure
 * that must stay visible. A {@code java.util.logging} properties file cannot express a per-message
 * threshold, so the suppression is scoped to the one message instead of to the whole logger.
 *
 * <p>Only records at {@link Level#WARNING} or below are dropped: a future Ratis release that raised the
 * same text to {@link Level#SEVERE} would still be heard.
 *
 * <p><b>What this depends on.</b> Two things, both of which fail <em>open</em> (the warning comes back)
 * rather than silently swallowing something else:
 * <ul>
 *   <li>Ratis logs through SLF4J, and {@code ha-raft} binds SLF4J to {@code java.util.logging} with
 *       {@code slf4j-jdk14}, so the record really does reach a JUL {@link Filter}. Swapping in a binding
 *       that does not terminate in JUL (Logback, Log4j2) leaves this filter inert; the suppression would
 *       then have to be re-expressed in that backend's configuration.</li>
 *   <li>The Ratis message text. The match is on a substring shared by the raw SLF4J pattern and by the
 *       formatted message the bridge produces, so it holds whether the record carries one or the other,
 *       but a Ratis release that reworded the line would resurface the warning rather than hide a
 *       different one.</li>
 * </ul>
 *
 * @see com.arcadedb.server.ha.raft.ArcadeStateMachine
 */
public final class RatisSnapshotDigestWarningFilter implements Filter {
  /** The Ratis logger that emits the warning. Matches the class name Ratis logs under. */
  public static final String RATIS_SNAPSHOT_STORAGE_LOGGER = "org.apache.ratis.statemachine.impl.SimpleStateMachineStorage";

  /**
   * Substring shared by the SLF4J pattern {@code "Snapshot file {} has missing MD5 file."} and by the
   * formatted message the SLF4J-to-JUL bridge produces, so the match works either way.
   */
  static final String MISSING_DIGEST_TEXT = "has missing MD5 file";

  private static final Object INSTALL_LOCK = new Object();

  /**
   * A strong reference to the configured logger. {@code java.util.logging.LogManager} keeps only a weak
   * reference to it, so without this an unreferenced logger can be collected and re-created without the
   * filter, silently bringing the noise back.
   */
  @SuppressWarnings("unused")
  private static Logger pinnedLogger;

  private final Filter delegate;

  RatisSnapshotDigestWarningFilter(final Filter delegate) {
    this.delegate = delegate;
  }

  /**
   * Installs the filter on the Ratis snapshot-storage logger, chaining to whatever filter that logger
   * already carried. Idempotent and safe to call from several places and threads: a second call on an
   * already-filtered logger is a no-op, so filters never stack.
   */
  public static void install() {
    synchronized (INSTALL_LOCK) {
      final Logger logger = Logger.getLogger(RATIS_SNAPSHOT_STORAGE_LOGGER);
      pinnedLogger = logger;
      if (logger.getFilter() instanceof RatisSnapshotDigestWarningFilter)
        return;
      logger.setFilter(new RatisSnapshotDigestWarningFilter(logger.getFilter()));
    }
  }

  @Override
  public boolean isLoggable(final LogRecord record) {
    if (isMissingDigestWarning(record))
      return false;
    return delegate == null || delegate.isLoggable(record);
  }

  private static boolean isMissingDigestWarning(final LogRecord record) {
    if (record.getLevel().intValue() > Level.WARNING.intValue())
      return false;
    final String message = record.getMessage();
    return message != null && message.contains(MISSING_DIGEST_TEXT);
  }

  /**
   * The filter this one chains to, or {@code null} when the logger carried none.
   */
  Filter getDelegate() {
    return delegate;
  }
}
