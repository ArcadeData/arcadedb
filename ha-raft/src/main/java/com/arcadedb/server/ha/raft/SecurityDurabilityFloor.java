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
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;
import com.arcadedb.server.security.ServerSecurity;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Keeps a replicated security entry in the Raft log for as long as what it installed on this node has not reached the
 * disk (issue #7752).
 * <p>
 * A security entry is applied in two writes that live outside every database: the document itself
 * ({@code server-users.jsonl}, {@code server-groups.json}, {@code server-api-tokens.json}) and the record that the
 * cluster installed it ({@code server-security-cluster.json}), which is the baseline the compare-and-set of issue #7509
 * judges the next entry against. Neither write may fail the apply (issue #7137), so either can be lost while the node
 * keeps running. The restart that followed used to read back the stale file - a document, or a baseline, the cluster had
 * moved past - because the snapshot checkpoint had long since let Ratis compact the entry away: the node refused the
 * entries its peers accepted (issue #7536), or installed the ones they refused (issue #7752), and the document change had
 * to be reissued by hand (issue #7227).
 * <p>
 * The Raft log is the durable copy of all of it, so this keeps it: the index of the first entry of each document whose
 * effects are not durable here is a floor {@code ArcadeStateMachine.takeSnapshot()} clamps its checkpoint below, the same
 * way it does for an entry left for replay (issue #9550). A restart then replays that entry, judged against the
 * baseline as it stood right before it - which the record file still holds, since a failed write leaves it in place -
 * so the node comes back on its peers' baseline by construction.
 * <p>
 * A document's floor clears as soon as it is durable again, whether through a later entry of the same kind or through
 * {@link #retryIfDue}, which re-persists what is in force at most once per {@link #RETRY_INTERVAL_MS} while something is
 * pinned. The retry runs as entries are applied, which is also the only time the log grows: so once the volume is
 * writable again the pin lifts within one interval of the next entry, rather than at the next security change, and a
 * cluster that applies nothing keeps a pin that costs nothing.
 * <p>
 * Written on the Raft apply thread only (and by a snapshot install, which runs while the apply thread is paused); read
 * by {@code takeSnapshot()} on any thread, hence the atomic array.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SecurityDurabilityFloor {
  static final String[] DOCUMENTS = { ReplicatedSecurityFingerprintRepository.USERS,
      ReplicatedSecurityFingerprintRepository.GROUPS, ReplicatedSecurityFingerprintRepository.API_TOKENS };

  /** How often a pinned document is written again while the apply thread is busy with other entries. */
  static final long RETRY_INTERVAL_MS = 30_000L;
  /** Every how many failed retries the pin is reported again, so it is not one SEVERE line lost in hours of log. */
  static final int  REPORT_EVERY_RETRIES = 10;

  private static final long NONE = Long.MAX_VALUE;

  // Per document, in DOCUMENTS order: the index of the first entry whose effects are not durable here, or NONE.
  private final    AtomicLongArray pinned = new AtomicLongArray(DOCUMENTS.length);
  private volatile boolean         anyPinned;
  private final    LongSupplier    nanoClock;
  // Told the document whose pin was just released, so a hold the state machine keeps for the same entry goes with it
  private final    Consumer<String> onDurableAgain;
  // Plain, not volatile: read and written only by afterInstall and retryIfDue, which both run on the Raft apply thread
  private          long            nextRetryNanos;
  private          long            pinnedSinceNanos;
  private          int             failedRetries;

  SecurityDurabilityFloor() {
    this(System::nanoTime, document -> {
    });
  }

  /** With the clock {@link #retryIfDue} paces itself on, so a test can advance it rather than wait for it. */
  SecurityDurabilityFloor(final LongSupplier nanoClock) {
    this(nanoClock, document -> {
    });
  }

  /**
   * @param onDurableAgain called with the document whose pin was just released: the state machine releases the hold it
   *                       keeps for a document write that failed (issue #10086), which only a later entry of the same
   *                       document would release otherwise - so a volume fixed under a quiet cluster stopped pinning the
   *                       log only at the next security change
   */
  SecurityDurabilityFloor(final LongSupplier nanoClock, final Consumer<String> onDurableAgain) {
    this.nanoClock = nanoClock;
    this.onDurableAgain = onDurableAgain;
    for (int i = 0; i < DOCUMENTS.length; i++)
      pinned.set(i, NONE);
  }

  /**
   * After the security entry at {@code index} installed {@code document} - whether or not its writes succeeded: pins the
   * entry when they did not, and clears every document that is durable now. A superseded entry installed nothing, so it
   * changes no pin and does not call this.
   */
  void afterInstall(final String document, final long index, final ServerSecurity security) {
    final int slot = slotOf(document);
    if (slot >= 0 && !security.isReplicatedDocumentDurable(document) && pinned.get(slot) == NONE) {
      pinned.set(slot, index);
      if (!anyPinned) {
        // The write that just failed was the first attempt, so the first retry is one interval away
        pinnedSinceNanos = nanoClock.getAsLong();
        nextRetryNanos = pinnedSinceNanos + TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MS);
        failedRetries = 0;
      }
      anyPinned = true;
      LogManager.instance().log(this, Level.SEVERE,
          "The replicated security entry at index %d (%s) is in force on this node but did not fully reach the disk. "
              + "No snapshot checkpoint will cover it until it does, so a restart replays it from the Raft log instead "
              + "of reading back a document the cluster has moved past (issue #7752). The write is retried every %d s "
              + "while this node applies entries; the Raft log grows until then. Free or fix the configuration volume",
          index, document, TimeUnit.MILLISECONDS.toSeconds(RETRY_INTERVAL_MS));
    }
    refresh(security);
  }

  /** Clears every document whose effects are durable now. */
  void refresh(final ServerSecurity security) {
    if (!anyPinned)
      return;

    boolean stillPinned = false;
    for (int i = 0; i < DOCUMENTS.length; i++) {
      final long index = pinned.get(i);
      if (index == NONE)
        continue;
      if (security.isReplicatedDocumentDurable(DOCUMENTS[i])) {
        pinned.set(i, NONE);
        onDurableAgain.accept(DOCUMENTS[i]);
        LogManager.instance().log(this, Level.INFO,
            "The replicated %s pinned since index %d is durable on this node again; snapshot checkpoints may cover it",
            DOCUMENTS[i], index);
      } else
        stillPinned = true;
    }
    anyPinned = stillPinned;
  }

  /**
   * Writes the pinned documents again from what is in force, at most once per {@link #RETRY_INTERVAL_MS}. Costs one
   * volatile read while nothing is pinned, which is always but during an incident, so it may run for every entry.
   * <p>
   * On the apply thread on purpose, blocking included. It is the single writer of these files - every replicated security
   * document is written by the apply that installs it - and a retry from another thread would race those writes, with
   * the record then naming a document that is not the last one installed. A volume that hangs this write hangs the
   * apply of the next security entry just the same, so the retry adds one attempt per interval to a stall that is
   * already the volume's, not a new one.
   */
  void retryIfDue(final ServerSecurity security) {
    if (!anyPinned || security == null)
      return;

    final long now = nanoClock.getAsLong();
    if (now - nextRetryNanos < 0)
      return;
    nextRetryNanos = now + TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MS);

    try {
      security.retryReplicatedSecurityPersistence();
      refresh(security);
      if (anyPinned && ++failedRetries % REPORT_EVERY_RETRIES == 0)
        LogManager.instance().log(this, Level.WARNING,
            "Replicated security documents are still not durable on this node after %d retries over %d s: the Raft log "
                + "has not been compacted past index %d since then, and grows until the configuration volume takes the "
                + "write again (issue #7752)",
            failedRetries, TimeUnit.NANOSECONDS.toSeconds(now - pinnedSinceNanos), floor());
    } catch (final RuntimeException e) {
      // Runs inside applyTransaction, whose catch-all halts the node: a retry that cannot run is a retry for later
      LogManager.instance().log(this, Level.WARNING, "Could not write the pinned security documents again: %s", e,
          e.getMessage());
    }
  }

  /**
   * The index {@code takeSnapshot()} must checkpoint below, or {@link Long#MAX_VALUE} when nothing is pinned. The slots
   * are read without a lock, so a read racing the apply thread can mix one slot's old value with another's new one. That
   * is safe for the snapshot: a slot is only ever set to an index the applied position has not yet moved past, or
   * cleared once its document is durable, so a torn read errs only towards a floor that holds the checkpoint back one
   * more round.
   */
  long floor() {
    long floor = NONE;
    for (int i = 0; i < DOCUMENTS.length; i++)
      floor = Math.min(floor, pinned.get(i));
    return floor;
  }

  /**
   * Drops every pin a snapshot install at {@code installedIndex} has covered. The entries below an install's boundary
   * are not replayed any more, so a pin there would only stop every later checkpoint (one below the marker the install
   * wrote is skipped) without protecting anything.
   *
   * @return true when a pin was dropped, so the caller has the cluster re-send the documents instead: the entries that
   * installed them are gone, and what this node would read back after a restart is still the stale file
   */
  boolean dropCoveredByInstall(final long installedIndex) {
    boolean dropped = false;
    boolean stillPinned = false;
    for (int i = 0; i < DOCUMENTS.length; i++) {
      final long index = pinned.get(i);
      if (index == NONE)
        continue;
      if (index <= installedIndex) {
        pinned.set(i, NONE);
        dropped = true;
      } else
        stillPinned = true;
    }
    anyPinned = stillPinned;
    return dropped;
  }

  private static int slotOf(final String document) {
    for (int i = 0; i < DOCUMENTS.length; i++)
      if (DOCUMENTS[i].equals(document))
        return i;
    return -1;
  }
}
