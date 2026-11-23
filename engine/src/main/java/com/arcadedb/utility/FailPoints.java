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
package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Named points in the engine where a test can make an operation fail, stall or end the process, so a failure in the middle of a
 * commit or a write is a reproducible case instead of a matter of timing. Nothing is armed in production: a point costs one read of
 * a volatile field, and {@link #hit} returns at once while no point is armed.
 * <p>
 * A test arms a point with {@link #arm}, and disarms it with {@link #disarm} or {@link #disarmAll}; a point can also be armed in a
 * child JVM from the system property {@code arcadedb.failPoints}, a comma-separated list of {@code name=action} where the action is
 * {@code throw} or {@code halt} (the exit code 73 a killed test child uses).
 * <p>
 * <b>For tests only.</b> The system property is not a setting: a server started with it fails or ends the process at the points it
 * names, so it must never be set on a deployment. A point armed from it is logged as a WARNING when this class is loaded, so a
 * property set by mistake shows in the log.
 */
public final class FailPoints {
  /** Before a commit writes its WAL record: the transaction has validated its pages and holds its file locks. */
  public static final String COMMIT_BEFORE_WAL     = "commit.beforeWal";
  /**
   * After a commit wrote its WAL record and before it publishes its pages. Not reached by a group commit
   * ({@code arcadedb.txWalGroupCommit} with {@code arcadedb.txWalFlush} 1 or 2), which only queues its record here and writes it
   * after the publication.
   */
  public static final String COMMIT_AFTER_WAL      = "commit.afterWal";
  /** After a commit published its pages, before it releases its file locks. */
  public static final String COMMIT_AFTER_PUBLISH  = "commit.afterPublish";
  /**
   * When a record is written into a bucket page, inside the transaction, once per record created: {@code LocalBucket.createRecord}
   * and every record of {@code LocalBucket.createRecordsBulk} (the graph batch), whether the bulk appends it or hands it to the
   * allocator. Not reached by the engine moving the content of an existing record behind a placeholder pointer, which creates no
   * record.
   */
  public static final String BUCKET_CREATE_RECORD  = "bucket.createRecord";
  /**
   * When a record is updated in a bucket page, inside the transaction, once per update ({@code LocalBucket.updateRecord}). Not
   * reached again by the update rewriting the content behind a placeholder pointer.
   */
  public static final String BUCKET_UPDATE_RECORD  = "bucket.updateRecord";
  /**
   * When a record is deleted from a bucket page, inside the transaction, once per record deleted: {@code LocalBucket.deleteRecord}
   * (both overloads) and {@code LocalBucket.retractRecord} (the engine undoing a write its indexing refused). Not reached by the
   * delete freeing a placeholder content or the chunks of a multi-page record, nor by {@code CHECK DATABASE FIX} clearing a
   * corrupted slot.
   */
  public static final String BUCKET_DELETE_RECORD  = "bucket.deleteRecord";

  /** Exit code of {@link #HALT}, the one a killed test child reports. */
  public static final int HALT_EXIT_CODE = 73;

  /** Throws a {@link FailPointException}. */
  public static final Action THROW = point -> {
    throw new FailPointException(point);
  };

  /** Ends the process as a kill would: no shutdown hook, no close. */
  public static final Action HALT = point -> Runtime.getRuntime().halt(HALT_EXIT_CODE);

  private static final Map<String, Action> ARMED = new ConcurrentHashMap<>();
  private static volatile boolean          active;

  static {
    armFromProperty(System.getProperty("arcadedb.failPoints"));
    if (active)
      LogManager.instance().log(FailPoints.class, Level.WARNING,
          "Fail points armed from the system property 'arcadedb.failPoints': %s. The property is for tests only: an armed point "
              + "fails or ends the process when the engine reaches it. Remove it from the JVM options of any deployment",
          new TreeSet<>(ARMED.keySet()));
  }

  /**
   * Arms the points listed in a {@code name[=throw|halt],...} value. A malformed entry (an empty name or an unknown action) is
   * logged and skipped, so a typo in the JVM option cannot fail the class initialization and with it every write.
   */
  static void armFromProperty(final String configured) {
    if (configured == null || configured.isBlank())
      return;
    for (final String entry : configured.split(",")) {
      if (entry.isBlank())
        continue;
      final String[] nameAndAction = entry.trim().split("=", 2);
      final String name = nameAndAction[0].trim();
      final String actionName = nameAndAction.length > 1 ? nameAndAction[1].trim() : "throw";
      final Action action = switch (actionName) {
        case "throw" -> THROW;
        case "halt" -> HALT;
        default -> null;
      };
      if (name.isEmpty() || action == null) {
        LogManager.instance().log(FailPoints.class, Level.WARNING,
            "Ignoring malformed fail point entry '%s' in the system property 'arcadedb.failPoints'", entry);
        continue;
      }
      arm(name, action);
    }
  }

  private FailPoints() {
  }

  /** Called by the engine at the point named {@code point}: runs its action when it is armed. */
  public static void hit(final String point) {
    if (!active)
      return;
    final Action action = ARMED.get(point);
    if (action != null)
      action.run(point);
  }

  // The mutations are serialized, so active always ends up matching ARMED: a disarmAll() that cleared the map cannot reset active
  // after an arm() that ran in between, and hit() keeps its one volatile read while nothing is armed
  public static synchronized void arm(final String point, final Action action) {
    ARMED.put(point, action);
    active = true;
  }

  public static synchronized void disarm(final String point) {
    ARMED.remove(point);
    active = !ARMED.isEmpty();
  }

  public static synchronized void disarmAll() {
    ARMED.clear();
    active = false;
  }

  /** What an armed point does when it is reached. */
  @FunctionalInterface
  public interface Action {
    void run(String point);
  }

  /** Raised by a point armed with {@link #THROW}. */
  public static final class FailPointException extends RuntimeException {
    public FailPointException(final String point) {
      super("Fail point '" + point + "' reached");
    }
  }
}
