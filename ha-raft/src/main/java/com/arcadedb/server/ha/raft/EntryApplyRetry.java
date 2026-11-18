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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ha.raft.ConfigurationEntryApplier.HeldConfiguration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

/**
 * How an entry that fails to apply is handled: retried while the failure can clear on its own, held for the replay with
 * the checkpoint kept below it when this node cannot make it durable now, or - for an unexpected error - quarantined and
 * resynced from the leader rather than halting the node. The entries held are what keeps the snapshot from moving past
 * an index this node has not applied.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which runs every entry it applies through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class EntryApplyRetry {
  private final ArcadeStateMachine sm;

  // The lowest index of an entry this node did not apply (or could not make durable) and holds for the replay, by what
  // it is held for (issue #10086): a security document ("security:<entry type>") whose file could not be written, or a retry
  // loop that gave up on an entry no database could be quarantined for. The same rule as replayFloor - takeSnapshot()
  // checkpoints below the lowest - except that a security document heals: the entry of the same document that follows and
  // persists supersedes the held one, so the hold is released then (releaseHeldEntry) instead of keeping the log from being
  // purged until the process ends. The two retry keys do NOT heal: nothing supersedes an entry that could not be applied, so
  // they hold until a restart replays it or a Raft install covers it, and takeSnapshot() says so while they do.
  // In memory only: after a restart the replay itself is the cure.
  final ConcurrentHashMap<String, Long> heldEntryFloors = new ConcurrentHashMap<>();

  // What an entry is held for, besides one key per security document
  private static final String                HELD_RETRY_EXHAUSTED   = "retry-exhausted";

  private static final String                HELD_INTERRUPTED_RETRY = "retry-interrupted";

  private static final long              HELD_REMINDER_INTERVAL_MS = 300_000L;

  // When takeSnapshot() last reminded that a hold keeps the Raft log from being purged, so a hold that lasts is not forgotten
  private volatile long                      heldCheckpointReminderMs;

  EntryApplyRetry(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Convenience overload that runs the dispatch without scoping the diverged-state guard to a
   * specific database (equivalent to {@code applyWithRetry(index, null, applyAction)}). Used where
   * the entry has no single target database.
   */
  // @VisibleForTesting
  void applyWithRetry(final long index, final Runnable applyAction) {
    applyWithRetry(index, null, applyAction);
  }

  /**
   * Runs the apply dispatch with bounded in-place retry for transient/retryable conditions.
   * <p>
   * A {@link NeedRetryException} (e.g. an MVCC {@link com.arcadedb.exception.ConcurrentModificationException}
   * from a page-version race) is NOT state divergence: the apply is deterministic and idempotent
   * (page-version / file-existence guards), so a retry can win the race. We retry up to
   * {@link GlobalConfiguration#TX_RETRIES} times; only if the condition persists do we escalate to a
   * {@link ReplicationException}, which {@link ArcadeStateMachine#applyTransaction} turns into a snapshot resync.
   * Crucially, a retryable error never reaches the fatal {@code catch (Throwable)} branch that stops
   * the server - "retry" must never mean "crash the node".
   * <p>
   * Note on backoff: {@link GlobalConfiguration#TX_RETRY_DELAY} defaults to 100ms and was tuned for
   * MVCC contention among many concurrent user-transaction threads. The Raft {@code StateMachineUpdater}
   * is a single sequential thread, so a smaller (or zero) delay is perfectly safe on this path and only
   * reduces the worst-case latency per entry; the value is read live so it can be tuned independently.
   *
   * @param index        the Raft log index being applied (diagnostics only)
   * @param databaseName the database the entry targets, used to scope the diverged-state guard
   *                     (may be {@code null} for entry types without a single target database)
   * @param applyAction  the apply dispatch to run
   * @throws ReplicationException if the retryable condition persists after all attempts
   */
  // @VisibleForTesting
  void applyWithRetry(final long index, final String databaseName, final Runnable applyAction) {
    final int maxRetries = Math.max(0, sm.server != null
        ? sm.server.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES)
        : GlobalConfiguration.TX_RETRIES.getValueAsInteger());
    final int retryDelay = sm.server != null
        ? sm.server.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY)
        : GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();

    NeedRetryException lastRetry = null;
    boolean interrupted = false;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        applyAction.run();
        return;
        // Catch the whole NeedRetryException hierarchy on purpose: the subclass normally reachable here
        // is the engine's MVCC ConcurrentModificationException (page-version race), which a retry can
        // win, and the broad type stays forward-compatible with future retryable errors.
        //
        // ServerIsNotTheLeaderException is the one exception (issue #4743): it means the apply tried to
        // WRITE the schema, which this node is not allowed to do while replaying someone else's entry.
        // That is deterministic - retrying re-runs the identical illegal write - so the old bounded
        // retry burned four attempts and then escalated a healthy database to a full snapshot resync
        // over a condition a resync cannot fix. Report it as a plain apply failure so the per-database
        // quarantine below decides, once, with the real error in the log.
      } catch (final ServerIsNotTheLeaderException e) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Raft apply at index %d attempted a schema write, which is only legal on the leader. This is deterministic, "
                + "so it is NOT retried: %s",
            index, e.getMessage());
        handleUnexpectedApplyError(index, databaseName, e);
      } catch (final NeedRetryException e) {
        lastRetry = e;
        LogManager.instance().log(sm, Level.WARNING,
            "Retryable error applying Raft log entry at index %d (attempt %d/%d): %s",
            index, attempt + 1, maxRetries + 1, e.getMessage());
        if (attempt < maxRetries && retryDelay > 0) {
          try {
            Thread.sleep(1 + ThreadLocalRandom.current().nextInt(retryDelay));
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            LogManager.instance().log(sm, Level.WARNING,
                "Raft apply retry interrupted at index %d after %d attempt(s); aborting retry loop (likely shutdown)",
                index, attempt + 1);
            interrupted = true;
            break;
          }
        }
      } catch (final ReplicationException re) {
        // A resync-required signal for a database that is already quarantined (e.g. the WAL-gap escalation from
        // applyTxEntry, which quarantines before it throws), or an entry held for the replay: propagate it unchanged so it
        // reaches applyTransaction's catch (ReplicationException) handler without being re-wrapped or counted against the
        // bounded-escalation budget below. ANY OTHER ReplicationException is a skipped entry that left nothing behind: no
        // quarantine, no replay floor, no halt - the three things takeSnapshot() consults - so the next committed entry
        // would carry the applied index over it and the checkpoint would let Ratis purge an entry this node never applied
        // (issue #10086). Those take the disposition an unexpected error takes, so a throw site added later is covered.
        if (hasDisposition(databaseName, re))
          throw re;
        throw dispositionForUnhandled(index, databaseName, re);
      } catch (final RuntimeException t) {
        // Catch RuntimeException (not Throwable) on purpose: applyAction is a Runnable, so the only
        // things it can throw are RuntimeException or Error. JVM Errors (OutOfMemoryError,
        // StackOverflowError, ...) mean the JVM itself is unstable and must never be swallowed as a
        // recoverable resync condition - leaving them uncaught lets them propagate unchanged to
        // applyTransaction's fatal halt path so the node stops loudly rather than masking a corrupt
        // runtime.
        //
        // A database closed under the apply thread while the node is shutting down (issue #9550) is not a divergence:
        // the entry is left for the replay on restart instead of quarantining the database, and charges nothing to the
        // swallow budget below. Checked first, and only here on the failure path, so a healthy apply pays nothing for it.
        if (sm.isClosedUnderShutdown(databaseName, t))
          throw sm.leaveForReplay(index, databaseName, t);
        try {
          handleUnexpectedApplyError(index, databaseName, t);
        } catch (final RaftLogEntryDecodeException escalated) {
          // handleUnexpectedApplyError rethrows the ORIGINAL error once the swallow budget is exhausted, so a
          // node that can never resync halts instead of degrading silently. Since issue #7495 that original can
          // be a RaftLogEntryDecodeException raised INSIDE this lambda (applyTxEntry's WAL decode), and
          // applyTransaction catches that type separately - as the handler for an unreadable ENVELOPE, which is
          // decoded before applyWithRetry is ever called. Letting this one reach that catch would run
          // handleUnexpectedApplyError a second time for a single failure, charging the swallow twice against
          // the very budget that just tripped and logging the escalation twice. It has been handled here, so
          // hand the fatal path the same failure under a type that catch does not claim.
          throw new IllegalStateException(escalated.getMessage(), escalated);
        }
      }
    }

    // The retryable condition persisted across all attempts. Escalate to a resync (recoverable) -
    // never fall through to the fatal catch (Throwable) branch that stops the server.
    final String message = "Retryable error persisted at index " + index + " after " + (maxRetries + 1)
        + " attempts; escalating to snapshot resync";
    if (!interrupted && databaseName != null && !databaseName.isEmpty()) {
      // The escalation the message promises: the entry was skipped, so the database is quarantined and resynced from the
      // leader, which is what holds the checkpoint back (issue #10086). Always throws a ReplicationException with the last
      // retryable error as its cause, or that error itself once the swallow budget of a stuck database is spent.
      handleUnexpectedApplyError(index, databaseName, lastRetry);
    }
    // An entry no database can be quarantined for - it names none, or the loop ended because the node is going down -
    // is held for the replay on restart instead (issue #10086)
    throw holdEntryForReplay(interrupted ? HELD_INTERRUPTED_RETRY : HELD_RETRY_EXHAUSTED, index, message, lastRetry);
  }

  /**
   * Whether a {@link ReplicationException} that left {@link #applyWithRetry} already carries the disposition the checkpoint
   * needs (issue #10086): the entry is held for the replay, or the database it names is quarantined.
   */
  private boolean hasDisposition(final String databaseName, final ReplicationException re) {
    return re instanceof EntryLeftForReplayException
        || databaseName != null && !databaseName.isEmpty() && sm.isDatabaseDiverged(databaseName);
  }

  /**
   * The disposition of a {@link ReplicationException} nothing has dealt with (issue #10086), as an unexpected error gets it:
   * the database it names is quarantined and resynced, and an entry that names none halts the node, because a mutation
   * nobody can apply must never be stepped over quietly (issue #4798). Returns the exception to throw.
   * <p>
   * Deliberately not the policy of a retry loop that gave up on an entry with no database, which is held instead: that is a
   * transient condition (a page-version race) on an entry that is well formed, so replaying it is the cure and halting the
   * node would turn a conflict into an outage. An exception nobody classified is a failure this code has no model of, so it
   * takes the halt that was always the answer to those. An apply that IS fail-safe says so itself, by holding its entry
   * ({@link #holdEntryForReplay}), as the security documents do.
   */
  private RuntimeException dispositionForUnhandled(final long index, final String databaseName, final ReplicationException re) {
    try {
      handleUnexpectedApplyError(index, databaseName, re);
    } catch (final ReplicationException quarantined) {
      if (quarantined != re)
        return quarantined;
      // The original again: the swallow budget is spent, or the entry names no database. applyTransaction catches a
      // ReplicationException as a recoverable resync, so the fatal path takes it under a type it does not claim.
      return new IllegalStateException(re.getMessage(), re);
    } catch (final RuntimeException fatal) {
      return fatal;
    }
    return re;
  }

  /**
   * Holds the checkpoint below the entry at {@code index} so that a restart replays it (issue #10086): the entry did not
   * advance the applied index, no database could be quarantined for it, and a later entry will. Registered BEFORE the
   * exception is thrown, so the apply thread has raised it before any later entry moves the index - the order
   * {@link ArcadeStateMachine#takeSnapshot()} relies on. The hold is released by {@link #releaseHeldEntry} when something supersedes the entry.
   *
   * @param key     what the entry is held for; the lowest index per key stands
   * @param message what the entry's failure says
   * @param cause   the failure, or {@code null}
   */
  EntryLeftForReplayException holdEntryForReplay(final String key, final long index, final String message,
      final Throwable cause) {
    final long lowest = heldEntryFloors.merge(key, index, Math::min);
    return heldEntry(key, index, lowest == index, message, cause);
  }

  /** The exception of an entry held for the replay, with the one SEVERE that says so; the hold itself is already registered. */
  EntryLeftForReplayException heldEntry(final String what, final long index, final boolean first, final String message,
      final Throwable cause) {
    LogManager.instance().log(sm, first ? Level.SEVERE : Level.FINE,
        "Raft entry at index %d was not applied or not made durable (%s): no snapshot checkpoint will cover it until it is "
            + "superseded, so a restart replays it (issue #10086): %s", index, what, message);
    return new EntryLeftForReplayException(message, cause);
  }

  /** Releases the hold of {@code key}: a later entry superseded the held one (issue #10086). */
  void releaseHeldEntry(final String key) {
    if (!heldEntryFloors.isEmpty())
      heldEntryFloors.remove(key);
  }

  /**
   * The lowest index of an entry held for the replay, {@link Long#MAX_VALUE} when none is. A hold at or below the boundary
   * a Raft install covered is dropped on the way: the installed copy carries what the entry did (issue #10086).
   */
  // @VisibleForTesting
  long lowestHeldEntry() {
    if (heldEntryFloors.isEmpty() && sm.configurationEntries.heldConfigurations.isEmpty())
      return Long.MAX_VALUE;
    final long boundary = sm.installedRaftBoundary.get();
    long lowest = Long.MAX_VALUE;
    for (final Map.Entry<String, Long> entry : heldEntryFloors.entrySet()) {
      final long index = entry.getValue();
      if (index <= boundary)
        heldEntryFloors.remove(entry.getKey(), entry.getValue());
      else if (index < lowest)
        lowest = index;
    }
    for (final Map.Entry<String, HeldConfiguration> entry : sm.configurationEntries.heldConfigurations.entrySet()) {
      final long index = entry.getValue().index();
      if (index <= boundary)
        sm.configurationEntries.heldConfigurations.remove(entry.getKey(), entry.getValue());
      else if (index < lowest)
        lowest = index;
    }
    return lowest;
  }

  /**
   * Says, at most once per {@link #HELD_REMINDER_INTERVAL_MS}, that entries held for the replay keep the Raft log from being
   * purged (issue #10086): a hold that nothing supersedes - a retry loop that gave up - lasts until a restart replays the
   * entry or a Raft install covers it, and the log grows meanwhile, which the hold's own SEVERE has long scrolled past.
   */
  void remindThatEntriesAreHeld(final long lowest) {
    final long now = System.currentTimeMillis();
    if (now - heldCheckpointReminderMs < HELD_REMINDER_INTERVAL_MS)
      return;
    heldCheckpointReminderMs = now;
    final StringBuilder held = new StringBuilder();
    for (final Map.Entry<String, Long> entry : heldEntryFloors.entrySet())
      held.append(held.isEmpty() ? "" : ", ").append(entry.getKey()).append('@').append(entry.getValue());
    for (final Map.Entry<String, HeldConfiguration> entry : sm.configurationEntries.heldConfigurations.entrySet())
      held.append(held.isEmpty() ? "" : ", ").append("config:").append(entry.getKey()).append('@')
          .append(entry.getValue().index());
    LogManager.instance().log(sm, Level.WARNING,
        "The Raft log is not checkpointed past index %d because entries are held for the replay (%s): it grows until a later "
            + "entry supersedes them, a restart replays them or a Raft install covers them (issue #10086)", lowest - 1, held);
  }

  static String heldSecurityKey(final RaftLogEntryType type) {
    return "security:" + type;
  }

  /**
   * Terminal handling for an apply error that no retry can fix. Always throws.
   * <p>
   * Per-database quarantine (issue #4797): a single {@link ArcadeStateMachine} multiplexes every
   * database on the node, so tripping the node-wide critical halt for one entry would freeze the apply
   * pipeline for ALL co-located databases. When the failing entry targets a single database
   * ({@code databaseName} non-null and non-empty) the failure is isolable: quarantine that database
   * (mark it diverged and trigger a targeted snapshot resync) and report the error as a recoverable
   * {@link ReplicationException} instead of the fatal {@code catch (Throwable)} path that would halt
   * the server. The node stays up, healthy databases keep replicating, and only the affected database
   * is reinstalled from the leader. This subsumes the earlier issue #4740 behaviour (an unexpected
   * error on an already-diverged database is a resync condition): the only change is that the FIRST
   * unexpected error on a healthy database now quarantines it rather than halting the node.
   * <p>
   * A sole voter does not quarantine (issue #9308): nothing could ever lift the quarantine there, so the error is
   * raised as a {@link ReplicationException} with a SEVERE alert and the node keeps serving. The failed entry stays in
   * the Raft log, so a restart that replays the log from the last snapshot marker applies it again, after the entries
   * that succeeded in between were already applied before it: a deterministic failure fails the same way again, while
   * a transient one (an I/O error, a lock timeout) now succeeds out of its original order. Either way the database may
   * no longer match the log, which is what the alert tells the operator to check.
   * <p>
   * Entries with no single target database ({@code databaseName} null or empty, e.g. a
   * {@code SECURITY_USERS_ENTRY}) are NOT isolable to one database's state, so their failure still
   * propagates to the node-wide fatal halt.
   */
  void handleUnexpectedApplyError(final long index, final String databaseName, final RuntimeException t) {
    if (databaseName != null && !databaseName.isEmpty()) {
      // A quarantine is lifted by the leadership hand-off and the targeted resync from the next leader. A sole voter
      // has neither, and with one voter there is no other copy for this one to be diverged FROM, so the quarantine
      // would protect nothing and never lift: it would keep the whole node out of the ready set and stop the Raft log
      // from being checkpointed until the volume fills (issue #9308, the same reasoning #8940 applied to the
      // forceSnapshot replay guard). Alert instead and keep serving. The entry still FAILS, so its submitter learns the
      // apply did not happen; only the quarantine, the hand-off and the swallow budget (which exists for a node waiting
      // on a resync, and there is none here) are skipped. A database that is ALREADY quarantined - restored from disk,
      // or raised while the node still had peers - keeps its quarantine and the routing below.
      // isSoleVoter() cannot throw here and mask t: the membership read it relies on (getCommittedPeersOrNull) degrades
      // to the declared server list on any exception, and a declared multi-node list answers false, the old routing.
      // It is also true while a cluster that will grow is still one committed voter (a single-seed bootstrap, or a
      // membership change down to one voter): a peer that joins later installs its copy FROM this node, so it inherits
      // the same state rather than diverging from it. The diverged check and the voter check are two reads, not one
      // atomic decision; a quarantine raised in between only sends this error down the old routing below.
      final RaftHAServer raftHA = sm.raftHAServer;
      if (raftHA != null && !sm.isDatabaseDiverged(databaseName) && raftHA.isSoleVoter()) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Unexpected error applying Raft entry for database '%s' at index %d, and this node is the only voter, so "
                + "there is no peer to resync it from. It is NOT quarantined (that could never be lifted here and would "
                + "keep the node out of service); the entry is reported as failed and later entries keep applying on top "
                + "of a state that is MISSING it, so from here on the database may be inconsistent with the Raft log and "
                + "a later entry that depends on this one can fail or produce a different state. Check the database "
                + "(CHECK DATABASE) and restore it from a backup if it is damaged: %s",
            databaseName, index, t.getMessage());
        throw new ReplicationException("Apply error on database '" + databaseName + "' at index " + index
            + "; this node is the only voter, so the database is not quarantined", t);
      }

      // Mark the database diverged on the first error so subsequent errors for it route here too.
      // quarantineDatabase() returns true only the first time, which is when we kick off the targeted resync. The cause
      // recorded with it is what the operator-facing alert says (issue #7741): an entry this node cannot decode
      // is a corrupt local log segment, not a replication fault, and pointing at the leader for it wastes the
      // one person who can see the bad disk.
      if (sm.quarantine.quarantineDatabase(databaseName,
          t instanceof RaftLogEntryDecodeException ? DivergenceCause.UNDECODABLE_LOG_ENTRY : DivergenceCause.APPLY_ERROR)) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Unexpected error applying Raft entry for database '%s' at index %d; quarantining the database and "
                + "triggering a targeted snapshot resync instead of halting the node (issue #4797): %s",
            databaseName, index, t.getMessage());
        sm.triggerDatabaseResync(databaseName);
        sm.resync.handOffLeadershipIfLeader(databaseName);
      } else {
        LogManager.instance().log(sm, Level.SEVERE,
            "Unexpected error at index %d while database '%s' is quarantined (snapshot resync in progress); "
                + "treating as resync condition: %s",
            index, databaseName, t.getMessage());
      }
      // Bounded escalation: a node that can never resync (no stable leader) must not swallow
      // errors forever and degrade silently. Once the swallow count exceeds the threshold, let
      // the error propagate to the fatal halt path so a truly stuck node surfaces loudly.
      if (sm.quarantine.divergedSwallowedErrors.incrementAndGet() > ArcadeStateMachine.MAX_DIVERGED_SWALLOWED_ERRORS) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Quarantined database '%s' swallowed over %d unexpected errors without resyncing (index %d); escalating to fatal halt: %s",
            databaseName, ArcadeStateMachine.MAX_DIVERGED_SWALLOWED_ERRORS, index, t.getMessage());
        throw t;
      }
      throw new ReplicationException(
          "Apply error on database '" + databaseName + "' at index " + index + "; per-database snapshot resync in progress", t);
    }

    // Node-scoped entry (no single target database): there is no per-database state to quarantine and no
    // targeted resync that would repair it, so an unexpected error here still escalates to the node-wide halt
    // (issue #4798's reasoning: never silently skip a committed mutation). An apply that is provably fail-safe
    // classifies its OWN failure as recoverable before it reaches here - see applySecurityUsersEntry (issue
    // #7137) - so the entries that arrive at this line are the ones for which a halt is the honest answer.
    // Say which class of entry it was: without it, the SEVERE at the fatal branch names only an index.
    LogManager.instance().log(sm, Level.SEVERE,
        "Unexpected error applying a node-scoped Raft entry at index %d (the entry targets no single database, so "
            + "there is nothing to quarantine); escalating to the node-wide halt: %s", index, t.getMessage());
    throw t;
  }
}
