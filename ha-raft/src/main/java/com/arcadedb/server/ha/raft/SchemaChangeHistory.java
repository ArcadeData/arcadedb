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

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * What this node knows of the schema changes it applied, per database: the index of the last one, the most recent ones,
 * and the term the schema session open on the leader was bound to. It is what refuses a schema proposal or a restore
 * validated against a schema that changed since (issues #8686, #9547).
 * <p>
 * Split out of {@link ArcadeStateMachine}, which records each schema change it applies and consults this before it lets
 * an entry into the log.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SchemaChangeHistory {
  private final ArcadeStateMachine sm;

  /**
   * Per database, the Raft log index of the last schema-changing entry this node applied (issue #8686). The leader holds a
   * transaction entry up against it: one whose originator says it was prepared at an older index was built against a
   * schema the cluster has moved past, and is refused. Deliberately an index of the LOG and not the schema's own version
   * counter: that counter also moves on purely local saves, so two nodes at the same schema legitimately disagree on it,
   * whereas every node that has applied entry N agrees on N. Absent (never recorded) means "nothing to compare against",
   * as it does after a restart until the next schema change is applied - a narrow window in which behaviour is exactly what
   * it was before the check existed. Only ever moves forward.
   */
  final ConcurrentHashMap<String, AtomicLong> lastSchemaChangeIndex = new ConcurrentHashMap<>();

  /**
   * Per database, the most recent schema-changing entries this node applied, newest {@link #RECENT_SCHEMA_CHANGES_KEPT} of them (issue
   * #10067); see {@link SchemaChangeLog}.
   */
  final ConcurrentHashMap<String, SchemaChangeLog> recentSchemaChanges = new ConcurrentHashMap<>();

  private static final int RECENT_SCHEMA_CHANGES_KEPT = 256;

  /**
   * The highest log index up to which this instance cannot tell what the schema did (issue #10067): the position it was seeded at
   * when it started (a new instance after a restart, or {@link ArcadeStateMachine#reinitialize()} after an install) and the one a snapshot install or
   * a resync jumped to. The entries up to there were not applied here one by one, so no schema change among them is in
   * {@link #lastSchemaChangeIndex} or {@link #recentSchemaChanges}; reading those as "nothing changed" would be reading an absence
   * of memory as an absence of change. A stretch of the log is vouched for only from this index on.
   */
  private final AtomicLong schemaHistoryFloor = new AtomicLong(-1L);

  /**
   * Per database, the Raft term the schema session currently open on this leader was bound to (issue #9547): the DDL or
   * compaction whose {@code SCHEMA_ENTRY} may be appended right now. Registered by
   * {@code RaftReplicatedDatabase} once it holds the database's file recording session, which is exclusive per database,
   * so there is at most one entry per name. {@link ArcadeStateMachine#startTransaction} refuses a schema entry this node submitted when no
   * session is bound for its database, or when the binding is to a term that is no longer the current one.
   * <p>
   * The file ids a session allocates come from this node's own {@code FileManager}, so they are only safe to publish in
   * the term whose committed history that file manager already reflected when the session began. An entry the Ratis
   * client resends after a leadership change, landing on this node again once it is re-elected, would otherwise be
   * appended after a colliding allocation the interim leader committed, and every node would hold one of the two files.
   */
  private final ConcurrentHashMap<String, Long> schemaSessionTerms = new ConcurrentHashMap<>();

  SchemaChangeHistory(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Why the leader must not append a {@code SCHEMA_ENTRY} for {@code databaseName}, or {@code null} when it may (issue
   * #9547).
   * <p>
   * Every producer of a schema entry - a DDL through {@code recordFileChanges}, an index or TimeSeries compaction through
   * {@code runWithCompactionReplication}, and the instalments, sealed slices and retirements inside those - runs only on
   * the leader, and allocates the ids of the files it creates from that node's own {@code FileManager}. Such an entry is
   * therefore only valid when it is appended by the node that allocated its ids, in the term that node bound the session
   * to. Ratis does not know that: its client resends a request it got no answer for to whichever node leads NOW. An
   * entry an isolated leader submitted reached the next leader that way, after that leader had committed its own
   * compaction under the same file id, and every node in the cluster then held one of the two files and quarantined
   * the database on the other.
   * <ul>
   *   <li><b>Another node's entry</b> is refused outright: no node submits a schema entry unless it is the leader, so one
   *       arriving from someone else's client is the resend of a node that lost leadership.</li>
   *   <li><b>This node's own entry</b> is refused when no session is bound for its database (a resend that outlived its
   *       session, or a session that could not read the term when it began) or the session was bound to an earlier term (a resend that found this node re-elected, with the
   *       interim leader's entries committed ahead of it).</li>
   * </ul>
   * A resend in the SAME term is not refused here, and needs not be: one term has one leader, so it reaches the node
   * that already has the request, whose Ratis retry cache answers it by client and call id without calling this method
   * again. The binding is per database and term rather than per session for that reason.
   * <p>
   * The term is read here, before Ratis appends: if this node is deposed in between, Ratis itself refuses the append,
   * because a leader only appends in its own term.
   * <p>
   * Refused before Ratis appends anything, through the context, so the entry reaches no node and costs Ratis nothing (the
   * same channel the #6965 page-version refusal uses). The proposer sees a definite {@link NeedRetryException}: its session
   * fails, and a DDL caller retries against the current leader.
   */
  // @VisibleForTesting
  NeedRetryException staleSchemaProposalRefusal(final String databaseName, final boolean isLocalOrigin) {
    if (!isLocalOrigin)
      return new NeedRetryException("Refused a schema change on database '" + databaseName + "' proposed by another "
          + "node: schema changes are only proposed by the leader, so this is the resend of a node that has lost the "
          + "leadership, and the file ids it allocated may already name other files here. Please retry on the leader");

    final Long sessionTerm = schemaSessionTerms.get(databaseName);
    if (sessionTerm == null)
      return new NeedRetryException("Refused a schema change on database '" + databaseName + "' proposed by this node "
          + "that is not bound to a Raft term: either the resend of a session that has already ended, whose file ids "
          + "may have been taken since, or a session that started while this node could not read its Raft term. Please "
          + "retry");

    final long term = sm.currentRaftTerm();
    if (sessionTerm != term)
      return new NeedRetryException("Refused a schema change on database '" + databaseName + "' allocated in Raft term "
          + sessionTerm + " while this leader is in term " + term + ": entries committed by the leaders in between can "
          + "already use the file ids it allocated. Please retry");
    return null;
  }

  /**
   * Why the leader must not append a drop or an install of {@code databaseName} fenced to {@code restoreFenceTerm}, or {@code null}
   * when it may (issue #10974). A restore fences them to the term in which its leader held the database: from another node the
   * entry is the resend of a leader that lost the leadership during the restore, and in another term this leader was elected again
   * after an interim leader accepted writes; either way the swap it completes would destroy them. An entry without the fence (a
   * drop or an install no restore made, or one written by a build without the section) is not refused here.
   * <p>
   * The term is read here, before Ratis appends: if this node is deposed in between, Ratis itself refuses the append, because a
   * leader only appends in its own term. Refused through the context, so the entry reaches no node.
   */
  // @VisibleForTesting
  NeedRetryException staleRestoreRefusal(final String databaseName, final long restoreFenceTerm, final boolean isLocalOrigin) {
    if (restoreFenceTerm < 0)
      return null;
    if (!isLocalOrigin)
      return new NeedRetryException("Refused the drop or install of database '" + databaseName + "' of a restore run by another "
          + "node: a restore replaces a database from the leader, so this is the resend of a node that lost the leadership while "
          + "restoring it, and the writes accepted since would be destroyed. Please retry the restore on the leader");
    final long term = sm.currentRaftTerm();
    if (term != restoreFenceTerm)
      return new NeedRetryException("Refused the drop or install of database '" + databaseName + "' of a restore that held it in "
          + "Raft term " + restoreFenceTerm + " while this leader is in term " + term + ": the leaders in between may have accepted "
          + "writes the restore would destroy. Please retry the restore");
    return null;
  }

  /**
   * Binds the schema session that has just claimed {@code databaseName}'s file recording session on this leader to
   * {@code term} (issue #9547). See {@link #staleSchemaProposalRefusal}. Only one session per database can hold the
   * recording session, so a previous binding left behind is replaced rather than counted.
   */
  void bindSchemaSession(final String databaseName, final long term) {
    final Long previous = schemaSessionTerms.put(databaseName, term);
    if (previous != null)
      // Only a session that never reached its unbind leaves one behind, so the exclusivity this map relies on was broken.
      LogManager.instance().log(sm, Level.WARNING,
          "Schema session on database '%s' replaced a binding to Raft term %d that was never ended", databaseName,
          previous);
  }

  /** Ends the binding taken by {@link #bindSchemaSession}; a binding to another term is left alone. */
  void unbindSchemaSession(final String databaseName, final long term) {
    schemaSessionTerms.remove(databaseName, term);
  }

  /**
   * Whether {@code decoded} publishes a schema document or delta (issue #8686): what a transaction prepared under the old
   * schema can be stale against, such as an index it does not maintain. Deliberately NOT a files-only entry (an LSM index
   * compaction swapping files) nor a WAL-only or sealed-store one: those change no schema and are frequent under load, and
   * refusing every in-flight replica transaction at each of them would fail callers that do not retry. The non-final
   * slices of a split schema change only deliver pages; the final one publishes.
   */
  // @VisibleForTesting
  static boolean changesSchema(final RaftLogEntryCodec.DecodedEntry decoded) {
    if (decoded.type() != RaftLogEntryType.SCHEMA_ENTRY || decoded.moreChunksFollow())
      return false;
    return decoded.schemaDelta() != null || (decoded.schemaJson() != null && !decoded.schemaJson().isEmpty());
  }

  /** Records that a schema-changing entry was applied at {@code index}; the position only moves forward. */
  // @VisibleForTesting
  void recordSchemaChangeApplied(final String databaseName, final long index) {
    if (databaseName == null)
      return;
    lastSchemaChangeIndex.computeIfAbsent(databaseName, name -> new AtomicLong(-1L)).accumulateAndGet(index, Math::max);
    recentSchemaChanges.computeIfAbsent(databaseName, name -> new SchemaChangeLog(RECENT_SCHEMA_CHANGES_KEPT)).add(index);
  }

  /** Raises {@link #schemaHistoryFloor}: this instance cannot tell what the schema did up to {@code index}. Never lowered. */
  // @VisibleForTesting
  void schemaHistoryIncompleteUpTo(final long index) {
    schemaHistoryFloor.accumulateAndGet(index, Math::max);
  }

  /**
   * Whether every schema change applied after log index {@code fromExclusive} is in this instance's memory, so that an absence of
   * one is an absence of change (issue #10067). False for a position before what this instance was seeded at, or before an install
   * or a resync jumped past it: a restart, an in-place Ratis restart or an install replaces what was remembered with nothing.
   */
  boolean schemaHistoryCovers(final long fromExclusive) {
    return fromExclusive >= schemaHistoryFloor.get();
  }

  /** The log index of the last schema-changing entry this node applied on the database, or {@code -1} when it recorded none. */
  long lastSchemaChangeIndexOf(final String databaseName) {
    final AtomicLong last = databaseName != null ? lastSchemaChangeIndex.get(databaseName) : null;
    return last != null ? last.get() : -1L;
  }

  /**
   * How many schema-changing entries this node applied on the database with a log index in {@code (fromExclusive, toInclusive]}
   * (issue #10067). A replica that asked the leader for ONE schema change reads it over the stretch from before the request to the
   * moment the change was applied here: one means that change was the only one, and so the transaction it is running was not
   * overtaken by anybody else's.
   *
   * @return the count, or {@link Integer#MAX_VALUE} when the retained history does not reach back to {@code fromExclusive} and so
   * cannot vouch for the stretch
   */
  int schemaChangesAppliedBetween(final String databaseName, final long fromExclusive, final long toInclusive) {
    if (!schemaHistoryCovers(fromExclusive))
      return Integer.MAX_VALUE;
    final SchemaChangeLog recent = databaseName != null ? recentSchemaChanges.get(databaseName) : null;
    return recent != null ? recent.countBetween(fromExclusive, toInclusive) : 0;
  }
}
