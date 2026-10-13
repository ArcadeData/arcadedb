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
 * The resolver is a short-lived daemon thread per change: the event is the rare end of a quorum-wait timeout, and the
 * thread ends with the answer, the database closing or the node shutting down.
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

  private static final long FIRST_RETRY_MS = 200L;
  private static final long MAX_RETRY_MS   = 5_000L;

  private final String            databaseName;
  private final Object            lock;
  private final Environment       environment;
  private final List<String>      unresolved = new ArrayList<>();

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
  void hold(final String changes) {
    synchronized (lock) {
      unresolved.add(changes);
    }
  }

  /** Registers a change whose outcome is unknown and starts finding out. */
  void add(final String changes) {
    hold(changes);
    final Thread resolver = new Thread(() -> resolve(changes), "arcadedb-raft-config-fate-" + databaseName);
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

  /** One pass of the resolution; {@code true} when the change is settled and was dropped from the list. */
  boolean tryResolve(final String changes) throws IOException {
    if (environment.isSaved(changes)) {
      forget(changes);
      return true;
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
    return true;
  }

  private void resolve(final String changes) {
    long delay = FIRST_RETRY_MS;
    while (environment.isActive()) {
      try {
        if (tryResolve(changes))
          return;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE,
            "The outcome of a configuration change of database '%s' is still unknown, retrying: %s", databaseName,
            e.getMessage());
      }
      try {
        Thread.sleep(delay);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
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
