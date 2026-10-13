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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * The configuration changes (issue #9763) one database submitted whose outcome is unknown - dispatched to Raft, then the
 * wait gave up ({@link ReplicationDispatchedTimeoutException}) - and the work of finding out what became of them
 * (issue #9904).
 * <p>
 * The change stays in force in memory on its originator, because it may yet commit and then the originator applies it
 * like every other node. If it never does, nothing else would ever take it out: the originator reads a value the rest
 * of the cluster does not, and, being still pending against {@code configuration.json}, it would ride the next change
 * made on this node. So:
 * <ul>
 *   <li>while unresolved, its settings are held back from every later change ({@link #withoutUnresolved}), as long as
 *   they still hold the value that was submitted - a setting changed again since is a new intent and travels;</li>
 *   <li>a resolver per change finds the fate. Committed shows as the changes in the file, which is what the apply
 *   writes. Otherwise it submits an empty configuration entry behind the first and waits for it to be applied here: the
 *   log is ordered, so an entry the cluster committed ahead of it is applied before it, and one that is still not in
 *   the file is not going to be. Then the settings go back to what the file holds. Should such an entry commit
 *   regardless (a request delayed in flight), its apply here installs the value again, so the node still converges.</li>
 * </ul>
 * One short-lived daemon resolver thread per database works through the list: the event is the rare end of a quorum-wait
 * timeout, and the thread ends when the list is empty, the database closes or the node shuts down.
 */
final class UnresolvedConfigurationChanges {
  /** What the resolver needs from the database it works for. */
  interface Environment {
    /** Whether the file holds every change of the document: the entry was applied on this node. */
    boolean isSaved(String changes) throws IOException;

    /**
     * Returns once an entry submitted now has been applied on this node, or throws when none could be committed (the
     * cluster is not reachable yet).
     */
    void awaitLaterEntryApplied();

    /** Puts the settings of the document that are still as it left them back to what the file holds. */
    void restore(String changes) throws IOException;

    /** Whether the database is still open on a node that is still running. */
    boolean isActive();
  }

  /** The entry that changes nothing, submitted behind a change of unknown outcome; the apply treats it as a no-op. */
  static final String FENCE_DOCUMENT = "{\"configuration\":{},\"removed\":[]}";

  private static final long FIRST_RETRY_MS = 200L;
  private static final long MAX_RETRY_MS   = 5_000L;
  /** Attempts after which a resolution that keeps failing is reported again (about a minute at the maximum delay). */
  private static final int  REPORT_EVERY   = 12;

  private final String       databaseName;
  private final Object       lock;
  private final Environment  environment;
  private final List<String> unresolved = new ArrayList<>();
  /** Whether the resolver thread of this database is running; guarded by {@link #lock}. */
  private boolean            resolving;

  /**
   * @param lock the lock the owner computes and submits its changes under: a resolution is atomic with respect to
   *             them, so a change cannot be computed between the file check and the restore
   */
  UnresolvedConfigurationChanges(final String databaseName, final Object lock, final Environment environment) {
    this.databaseName = databaseName;
    this.lock = lock;
    this.environment = environment;
  }

  /** Registers a change whose outcome is unknown, without resolving it (the caller drives that). */
  // @VisibleForTesting
  void hold(final String changes) {
    synchronized (lock) {
      unresolved.add(changes);
    }
  }

  /** Registers a change whose outcome is unknown and starts finding out, on the one resolver thread of this database. */
  void add(final String changes) {
    synchronized (lock) {
      unresolved.add(changes);
      if (resolving)
        return;
      resolving = true;
    }
    // A raw daemon thread rather than a pool: this is the rare end of a quorum-wait timeout, one thread per database that
    // exits when the list is empty, and a pool would hold a thread for the node's lifetime for an event that mostly never
    // happens. It never touches the JDK common pool.
    final Thread resolver = new Thread(this::resolveAll, "arcadedb-raft-config-fate-" + databaseName);
    resolver.setDaemon(true);
    resolver.start();
  }

  int size() {
    synchronized (lock) {
      return unresolved.size();
    }
  }

  /**
   * {@code pending} (a {@code LocalDatabase.pendingConfigurationChanges()} document) without the settings an unresolved
   * change still holds at the value it submitted, or {@code null} when nothing is left. To be called under the lock.
   */
  String withoutUnresolved(final String pending) {
    if (pending == null || unresolved.isEmpty())
      return pending;

    final JSONObject document = new JSONObject(pending);
    final JSONObject changed = document.getJSONObject("configuration", new JSONObject());
    final JSONArray removed = document.getJSONArray("removed", new JSONArray());
    final List<String> removedKeys = new ArrayList<>();
    for (int i = 0; i < removed.length(); i++)
      removedKeys.add(removed.getString(i));

    for (final String held : unresolved) {
      final JSONObject heldDocument = new JSONObject(held);
      final JSONObject heldChanged = heldDocument.getJSONObject("configuration", new JSONObject());
      for (final String key : heldChanged.keySet())
        if (changed.has(key) && String.valueOf(changed.get(key)).equals(String.valueOf(heldChanged.get(key))))
          changed.remove(key);
      final JSONArray heldRemoved = heldDocument.getJSONArray("removed", new JSONArray());
      for (int i = 0; i < heldRemoved.length(); i++)
        removedKeys.remove(heldRemoved.getString(i));
    }

    if (changed.keySet().isEmpty() && removedKeys.isEmpty())
      return null;
    return new JSONObject().put("configuration", changed).put("removed", new JSONArray(removedKeys)).toString();
  }

  /**
   * One pass of the resolution of one change, which drops it from the list when settled and throws when it cannot be.
   * The file holding the submitted values counts as committed: by value, not by entry, which is enough since the value
   * is the same whichever entry wrote it.
   */
  void tryResolve(final String changes) throws IOException {
    if (environment.isSaved(changes)) {
      forget(changes);
      return;
    }
    environment.awaitLaterEntryApplied();
    synchronized (lock) {
      if (!environment.isSaved(changes)) {
        environment.restore(changes);
        LogManager.instance().log(this, Level.WARNING,
            "A configuration change of database '%s' whose outcome was unknown did not commit: the settings went back to "
                + "what its configuration file holds", databaseName);
      }
      unresolved.remove(changes);
    }
  }

  private void resolveAll() {
    try {
      resolveUntilSettled();
    } catch (final Throwable t) {
      // Any other exit has already given the thread up under the lock, in the same block that decided to
      synchronized (lock) {
        resolving = false;
      }
      throw t;
    }
  }

  /**
   * Works through the list. Every normal exit clears {@link #resolving} in the same block, under the lock, that saw the
   * list empty or decided to stop, so an {@link #add} cannot slip between "nothing left" and "no resolver": it either
   * lands in the list before the block (and is seen) or finds the flag clear and starts a resolver of its own.
   */
  private void resolveUntilSettled() {
    long delay = FIRST_RETRY_MS;
    int failures = 0;
    while (true) {
      final List<String> batch;
      synchronized (lock) {
        if (unresolved.isEmpty() || !environment.isActive()) {
          if (!unresolved.isEmpty())
            LogManager.instance().log(this, Level.WARNING,
                "Database '%s' is closing or the node is shutting down with %d configuration change(s) of unknown outcome "
                    + "unresolved: they are in memory only and are gone with the next restart", databaseName,
                unresolved.size());
          resolving = false;
          return;
        }
        batch = new ArrayList<>(unresolved);
      }
      // The log is ordered, so a change that cannot be settled yet holds the later ones back too: stop at the first
      for (final String changes : batch) {
        try {
          tryResolve(changes);
          failures = 0;
          delay = FIRST_RETRY_MS;
        } catch (final Exception e) {
          if (failures++ % REPORT_EVERY == 0)
            LogManager.instance().log(this, Level.WARNING,
                "The outcome of a configuration change of database '%s' is still unknown, retrying: %s", databaseName,
                e.getMessage());
          break;
        }
      }
      try {
        Thread.sleep(delay);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        synchronized (lock) {
          resolving = false;
        }
        return;
      }
      delay = Math.min(delay * 2, MAX_RETRY_MS);
    }
  }

  private void forget(final String changes) {
    synchronized (lock) {
      unresolved.remove(changes);
    }
  }
}
