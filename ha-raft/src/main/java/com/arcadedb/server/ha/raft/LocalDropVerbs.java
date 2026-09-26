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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The drops this node's OWN {@code drop database} verb is waiting on, so the apply of the entry it submitted knows
 * the per-database maintenance slot is already held on its behalf (issue #8035).
 * <p>
 * {@code ArcadeStateMachine.applyDropDatabaseEntry} reserves {@code Operation.DROP} before it closes and removes a
 * database, so a backup, export or import running on this node is waited out (bounded) rather than torn down. On the
 * node that issued the verb that reservation would wait on ITSELF: {@code ServerControlPlane.dropDatabase} holds
 * {@code DROP} (or a restore holds {@code RESTORE}) on its request thread while
 * {@code RaftReplicatedDatabase.dropInReplicas} waits for exactly this apply, and the reservation is not reentrant.
 * Bounded, that wait would still cost every HA drop the whole budget plus a false warning, and would outlast the
 * verb's own wait for the local apply. The verb registers here for the length of that wait instead, and the apply
 * runs its destructive section under the registration.
 * <p>
 * A {@link Registration} is what closes the hand-off: the apply holds its monitor across the destructive section and
 * the verb takes the same monitor to deregister, so a verb whose wait timed out cannot release its slot - and let a
 * backup in - while the apply is still closing and renaming the database it was covering.
 * <p>
 * Owned by {@link RaftHAServer}, not by the state machine, so a registration survives the state machine being
 * rebuilt by a Ratis restart in the middle of a drop.
 */
final class LocalDropVerbs {

  /** One local verb awaiting the drop of one database. */
  static final class Registration {
    private boolean awaiting = true;
  }

  private final Map<String, Registration> awaiting = new ConcurrentHashMap<>();

  /**
   * Registers a local verb that holds this database's maintenance slot and is about to submit and wait on its drop.
   * The caller must {@link #release} it from a {@code finally}.
   */
  Registration register(final String databaseName) {
    final Registration registration = new Registration();
    awaiting.put(databaseName, registration);
    return registration;
  }

  /**
   * Withdraws a registration. Blocks while an apply is running its destructive section under it, which is what keeps
   * the verb's maintenance slot held until that section is done.
   */
  void release(final String databaseName, final Registration registration) {
    synchronized (registration) {
      registration.awaiting = false;
    }
    awaiting.remove(databaseName, registration);
  }

  /**
   * Runs {@code section} if a local verb is registered for this database, holding the registration so the verb
   * cannot withdraw it - and release its slot - until the section returns.
   *
   * @return {@code false}, without running anything, when no local verb is waiting on this drop
   */
  boolean runUnderLocalVerb(final String databaseName, final Runnable section) {
    final Registration registration = awaiting.get(databaseName);
    if (registration == null)
      return false;
    synchronized (registration) {
      if (!registration.awaiting)
        return false;
      section.run();
      return true;
    }
  }

  /** Whether a local verb is currently registered for this database. For tests. */
  boolean isAwaiting(final String databaseName) {
    return awaiting.containsKey(databaseName);
  }
}
