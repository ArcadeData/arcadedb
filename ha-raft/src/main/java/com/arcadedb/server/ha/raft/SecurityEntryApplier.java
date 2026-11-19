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
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ReplicatedSecurityConfigPersistenceException;
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;
import com.arcadedb.server.security.ReplicatedUsersPersistenceException;
import com.arcadedb.server.security.SecurityGroupFileRepository;
import com.arcadedb.server.security.SecurityUserFileRepository;
import java.util.logging.Level;

/**
 * Applies the security entries of the Raft log - users, groups and API tokens - to this server's security files. An entry
 * whose effect has not reached the disk is held, so the snapshot never covers a security change this node could lose.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which dispatches each entry to the class that applies it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SecurityEntryApplier {
  private final ArcadeStateMachine sm;

  SecurityEntryApplier(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Applies a replicated user list.
   * <p>
   * A local PERSISTENCE failure here - {@code ServerSecurity.applyReplicatedUsers} cannot write
   * {@code server-users.jsonl} because the config volume is full, read-only or NFS-hiccuping - is deliberately
   * NOT a node-halt condition (issue #7137). Every other failure of that method still is; see the note at the
   * catch below. It reaches {@code handleUnexpectedApplyError} with the empty
   * database name the codec gives this entry, which skips the per-database quarantine of #4797 and lands in
   * the node-wide {@code catch (Throwable)} halt; and because the halt leaves the applied index untouched on
   * purpose, the next start replays the same entry and halts again. With an environmental cause that is an
   * indefinite crash loop of the whole node - every co-located database - triggered by nothing worse than a
   * password change on the leader.
   * <p>
   * Failing the entry is enough because nothing here can diverge the databases this node replicates: the
   * failure is confined to one file of server-local configuration. It is also not a security hole, because
   * {@code applyReplicatedUsers} publishes the new list in memory BEFORE reporting the write failure, so a
   * revoked account or a changed password takes effect on this node immediately - only the durability of that
   * change is outstanding.
   * <p>
   * <b>The failing entry is held for the replay until a later one supersedes it</b> (issues #7227, #10086, #7752). It
   * does not record itself as applied, but it does not halt the node either - that is the whole point of this arm -
   * so the NEXT entry moves {@link ArcadeStateMachine#lastAppliedIndex} past it, and {@link ArcadeStateMachine#takeSnapshot()} checkpoints from
   * that counter. Left at that, whether the entry is ever replayed depended entirely on the SNAPSHOT MARKER, which
   * is the only thing {@link ArcadeStateMachine#reinitialize()} seeds the replay position from (see {@code ha-raft/CLAUDE.md}):
   * once any snapshot past this index was taken the entry was gone for good, a graceful {@code stop()} takes
   * one unconditionally, as does {@code RaftLogCompactionScheduler} on its interval, and a revoked account came
   * back after the restart with nothing to say so. So the entry is held ({@link BootstrapPasses#holdEntryForReplay}): the checkpoint
   * stops below it, a restart replays it onto a volume that has been fixed and writes the file, and the hold is released
   * when the next entry of the same document persists, since the list is applied whole and that one carries everything
   * this one did. Reissuing the user change on the leader writes the file as well.
   * <p>
   * {@link SecurityDurabilityFloor} covers what the hold cannot see (issue #7752): the compare-and-set record written
   * after the document, whose failure is swallowed. It pins the entry until {@code
   * ServerSecurity.isReplicatedDocumentDurable} says both are on disk, rewrites them from memory while the node applies
   * entries, and releases the hold above too once the document is durable again, so a fixed volume stops pinning the
   * log without waiting for the next user change.
   * Pinned by {@code AppliedIndexCheckpointTest.AppliedIndexDurability.SkippedEntryHoldsTheCheckpoint} for the hold and its release, by
   * {@code SecurityEntryApplyFailureTest} for the record and the retry, by
   * {@code SecurityEntryApplyFailureTest} for the applied counters, which still move, and by
   * {@code SecurityEntryApplyFailureTest} for both branches of the restart once a later entry has
   * superseded the failed one.
   * <p>
   * The classification lives here, at the apply site, rather than in the generic handler: whether a failure
   * can diverge replicated state is a property of the apply, not of the entry's database scoping, so a future
   * node-scoped entry that CAN diverge still reaches the halt it needs.
   *
   * @return false when the entry carried a compare-and-set precondition that no longer holds, so the user list
   * was deliberately NOT installed (issue #7509). The decision is the same on every node, because the payload
   * and the state it is compared against are both replicated and applies are ordered
   */
  boolean applySecurityUsersEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(sm, Level.WARNING, "SECURITY_USERS_ENTRY has null payload, skipping");
      return true;
    }
    try {
      // An entry with no precondition takes the unconditional apply verbatim - that is a seed, and it is also
      // every entry a node that predates issue #7509 wrote. Only a conditional entry goes through the
      // compare-and-set overload, so nothing about the pre-#7509 path changes shape.
      final String precondition = decoded.securityPrecondition();
      if (precondition == null)
        sm.server.getSecurity().applyReplicatedUsers(payload);
      else if (!sm.server.getSecurity().applyReplicatedUsers(payload, precondition))
        return false;
      sm.runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, index);
      sm.securityDurabilityFloor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, index, sm.server.getSecurity());
    } catch (final ReplicatedUsersPersistenceException e) {
      // In force in memory before the write failed, so it is what this node enforces: installed (issue #8317).
      sm.runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, index);
      // And not durable, so the entry stays replayable until it is (issue #7752).
      sm.securityDurabilityFloor.afterInstall(ReplicatedSecurityFingerprintRepository.USERS, index, sm.server.getSecurity());
      LogManager.instance().log(sm, Level.SEVERE,
          "Could not fully apply a replicated user list on this node: %s. The node keeps running and, when the "
              + "list reached memory, is already enforcing it - but it is not durable in '%s' yet, so no snapshot "
              + "checkpoint covers this entry and a restart replays it (issue #7752). The usual cause is a local write "
              + "failure: check that the configuration directory holding that file is writable and has free space",
          e, e.getMessage(), SecurityUserFileRepository.FILE_NAME);
      // The message has to match what actually happened, because it is the half most likely to travel - into
      // another node's log, an HA status payload, an incident writeup - without the SEVERE above beside it.
      // "Stays up with its previous users" was true before the ordering fix and is now the opposite of the
      // guarantee this fix exists to provide (issue #7137).
      throw sm.applyRetry.holdEntryForReplay(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_USERS_ENTRY), index,
          "Failed to persist the replicated user list locally; the node is already enforcing the new list in "
              + "memory, only its durability to disk failed", e);
    }
    // A whole document written: any earlier entry of it that could not be written is superseded (issue #10086)
    sm.applyRetry.releaseHeldEntry(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_USERS_ENTRY));
    // Note what is NOT caught: a payload this node cannot parse, or a user entry it cannot construct, throws
    // from applyReplicatedUsers BEFORE any mutation. That is not "the disk is full", it is "this node cannot
    // read a committed entry its peers applied", and it still reaches the node-wide halt - the case #4798
    // argues must never be skipped quietly. Catching RuntimeException here would have downgraded it silently.
    HALog.log(sm, HALog.DETAILED, "Applied SECURITY_USERS_ENTRY (%d bytes)", payload.length());
    return true;
  }

  /**
   * Applies a replicated {@code server-groups.json} document (issue #7373).
   * <p>
   * Failure classification is the same split {@link #applySecurityUsersEntry} makes, and for the same reason: a
   * local WRITE failure happens after the document is already in force on this node, so nothing it replicates can
   * diverge and halting would turn a full or read-only config volume into a crash loop - report it and stay up.
   * A document this node cannot READ is the opposite case: it is a committed entry the peers applied and this one
   * cannot, which must not be skipped quietly (issue #4798), so it is deliberately NOT caught here and reaches the
   * node-wide halt.
   * <p>
   * The durability left outstanding by the caught case is carried by the Raft log - see the long note on
   * {@link #applySecurityUsersEntry}, which applies verbatim (issue #7752).
   */
  boolean applySecurityGroupsEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(sm, Level.WARNING, "SECURITY_GROUPS_ENTRY has null payload, skipping");
      return true;
    }
    try {
      final String precondition = decoded.securityPrecondition();
      if (precondition == null)
        sm.server.getSecurity().applyReplicatedGroups(payload);
      else if (!sm.server.getSecurity().applyReplicatedGroups(payload, precondition))
        return false;
      sm.runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, index);
      sm.securityDurabilityFloor.afterInstall(ReplicatedSecurityFingerprintRepository.GROUPS, index, sm.server.getSecurity());
    } catch (final ReplicatedSecurityConfigPersistenceException e) {
      sm.runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, index);
      sm.securityDurabilityFloor.afterInstall(ReplicatedSecurityFingerprintRepository.GROUPS, index, sm.server.getSecurity());
      LogManager.instance().log(sm, Level.SEVERE,
          "Could not fully apply a replicated group document on this node: %s. The node keeps running and is "
              + "already authorizing against the new groups - but they are not durable in '%s' yet, so no snapshot "
              + "checkpoint covers this entry and a restart replays it (issue #7752). The usual cause is a local write "
              + "failure: check that the configuration directory holding that file is writable and has free space",
          e, e.getMessage(), SecurityGroupFileRepository.FILE_NAME);
      throw sm.applyRetry.holdEntryForReplay(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_GROUPS_ENTRY), index,
          "Failed to persist the replicated group document locally; the node is already authorizing against the "
              + "new groups in memory, only their durability to disk failed", e);
    }
    sm.applyRetry.releaseHeldEntry(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_GROUPS_ENTRY));
    HALog.log(sm, HALog.DETAILED, "Applied SECURITY_GROUPS_ENTRY (%d bytes)", payload.length());
    return true;
  }

  /**
   * Applies a replicated {@code server-api-tokens.json} document (issue #7373). Same failure classification as
   * {@link #applySecurityGroupsEntry}.
   * <p>
   * Worth being explicit about what the non-halting arm means here, because this entry can carry a REVOCATION: the
   * new token set is in force on this node from the moment the apply returns, so the revoked token stops
   * authenticating here even when the write failed. What is outstanding is only durability, and since issue #7752 the
   * Raft log carries it: no snapshot checkpoint covers the entry until the token set is on disk, so a restart replays
   * the revocation rather than reading the stale file back.
   */
  boolean applySecurityApiTokensEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(sm, Level.WARNING, "SECURITY_API_TOKENS_ENTRY has null payload, skipping");
      return true;
    }
    try {
      final String precondition = decoded.securityPrecondition();
      if (precondition == null)
        sm.server.getSecurity().applyReplicatedApiTokens(payload);
      else if (!sm.server.getSecurity().applyReplicatedApiTokens(payload, precondition))
        return false;
      sm.runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, index);
      sm.securityDurabilityFloor.afterInstall(ReplicatedSecurityFingerprintRepository.API_TOKENS, index, sm.server.getSecurity());
    } catch (final ReplicatedSecurityConfigPersistenceException e) {
      sm.runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, index);
      sm.securityDurabilityFloor.afterInstall(ReplicatedSecurityFingerprintRepository.API_TOKENS, index, sm.server.getSecurity());
      LogManager.instance().log(sm, Level.SEVERE,
          "Could not fully apply a replicated API-token document on this node: %s. The node keeps running and is "
              + "already enforcing the new token set - a revoked token does NOT authenticate here any more - but it "
              + "is not durable in '%s' yet, so no snapshot checkpoint covers this entry and a restart replays it (issue "
              + "#7752). The usual cause is a local write failure: check that the configuration directory holding that "
              + "file is writable and has free space",
          e, e.getMessage(), ApiTokenConfiguration.FILE_NAME);
      throw sm.applyRetry.holdEntryForReplay(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY), index,
          "Failed to persist the replicated API-token document locally; the node is already enforcing the new token "
              + "set in memory, only its durability to disk failed", e);
    }
    sm.applyRetry.releaseHeldEntry(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY));
    HALog.log(sm, HALog.DETAILED, "Applied SECURITY_API_TOKENS_ENTRY (%d bytes)", payload.length());
    return true;
  }
}
