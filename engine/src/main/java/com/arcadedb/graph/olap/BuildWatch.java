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
package com.arcadedb.graph.olap;

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What a Graph Analytical View's full CSR build raced with (issue #8378).
 * <p>
 * The build scan is read-committed and takes as long as the graph is big, so a transaction that commits while it
 * runs may or may not be in what it read. The view arms its change listeners and opens one of these BEFORE the
 * scan starts; until the build publishes, every relevant commit is recorded here instead of being lost.
 * <p>
 * Under {@code SYNCHRONOUS} the committed deltas are buffered and re-applied on top of the CSR the build
 * publishes. Whether a buffered edge change is already in that CSR is answered EXACTLY, by edge identity:
 * <ol>
 *   <li>the transaction registers the source vertex of every edge it creates or deletes ({@link #watchSource}),
 *   from the record listener, so before it commits;</li>
 *   <li>the scan reads a vertex's out-edges and only then asks whether the vertex is watched: if it is, it
 *   reports the edges it put into the CSR ({@link #observed}).</li>
 * </ol>
 * A vertex the scan found unwatched was read before the registration, so before the change committed: none of
 * its buffered changes are in the CSR. A watched one carries the edge identities the scan saw, which says which of
 * its buffered additions were captured and which of its buffered deletions were already missing. The answer is
 * handed to {@link DeltaOverlay#merge} edge by edge ({@link DeltaOverlay.ExactScanAnswer}), where the compaction path
 * can only compare pair multiplicities against its pre-compaction snapshot (issues #7042, #7884).
 * <p>
 * The same answer applies to a delta whose commit callback arrives after the build published (its transaction
 * committed during the scan, but was delivered late), so the view keeps it for as long as the CSR it describes
 * stays the base.
 * <p>
 * A transaction already running when a view's first build arms the listeners reported its earlier changes to nobody,
 * as it would to any listener armed mid-transaction: the window this closes is the scan's, not that one.
 * <p>
 * The registrations are not bounded the way the buffered deltas are: past the overflow the view publishes STALE, yet
 * sources keep being registered until the build finishes. That is one entry per changed source for the length of one
 * scan, dropped with the watch, and bounding it would cost a check on every record event instead.
 * <p>
 * Under {@code OFF} and {@code ASYNCHRONOUS} a relevant commit during the build only marks this watch, without taking
 * the view's monitor: the build then publishes STALE ({@code OFF}) or rebuilds once published ({@code ASYNCHRONOUS}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BuildWatch implements CSRBuilder.ScanObserver, DeltaOverlay.ExactScanAnswer {
  private static final Map<RID, RID> NOTHING_SEEN = Collections.emptyMap();

  // Past this many buffered deltas the build is published STALE rather than reconciled: the same bound the
  // compaction path puts on its own buffer.
  private final int maxBufferedDeltas;

  // Sources whose out-edges a transaction changed while this watch was open. Concurrent: registered from the
  // committing threads, read by the scan.
  private final Set<RID>                     watchedSources = ConcurrentHashMap.newKeySet();
  // Per watched source the scan reached AFTER its registration: the out-edges it put into the CSR, edge RID ->
  // target RID. A source registered after the scan passed it has no entry, which is itself the answer. The value maps
  // are written by the scan thread, then read and trimmed by account() under the view's monitor, which the build's
  // publication also takes: that hand-off is what orders the two, not the concurrency of the outer map.
  private final Map<RID, Map<RID, RID>>      observedSources = new ConcurrentHashMap<>();

  // Guarded by this
  private boolean                            open           = true;
  private boolean                            overflowed;
  // The watch of the build that superseded this one, which a late registration is forwarded to
  private BuildWatch                         successor;
  private final List<TxDelta>                bufferedDeltas = new ArrayList<>();

  // Set by the OFF-mode commit callback while the build is in flight
  private volatile boolean                   relevantCommit;

  // Bound when the build publishes; read and written under the view's monitor only from then on
  private Map<String, CSRAdjacencyIndex>     csrPerType;
  // The answers for the delta being merged, by EdgeDelta instance: set by account(), read by the merge right after
  private final Map<TxDelta.EdgeDelta, Boolean> answers = new IdentityHashMap<>();

  BuildWatch(final int maxBufferedDeltas) {
    this.maxBufferedDeltas = maxBufferedDeltas;
  }

  /**
   * Called from the record listener, before the transaction commits. Forwarded to the watch of the build that
   * superseded this one, if any; a no-op once the build has published.
   */
  void watchSource(final RID source) {
    final BuildWatch next;
    synchronized (this) {
      if (open) {
        watchedSources.add(source);
        return;
      }
      next = successor;
    }
    if (next != null)
      next.watchSource(source);
  }

  /**
   * Closes this watch in favour of the one of a build that supersedes it, handing it every registration made so far.
   * They were made before {@code next} opened, so before its scan started: a transaction that registered here and
   * commits during the new scan is still known there, and so is one that registers here after this call.
   */
  synchronized void handOverTo(final BuildWatch next) {
    open = false;
    successor = next;
    next.watchedSources.addAll(watchedSources);
  }

  @Override
  public boolean isWatched(final RID source) {
    return watchedSources.contains(source);
  }

  @Override
  public void observed(final RID source, final List<RID> edges, final List<RID> targets) {
    final Map<RID, RID> seen;
    if (edges.isEmpty())
      seen = NOTHING_SEEN;
    else {
      seen = new HashMap<>(edges.size() * 2);
      for (int i = 0; i < edges.size(); i++)
        seen.put(edges.get(i), targets.get(i));
    }
    observedSources.putIfAbsent(source, seen);
  }

  /**
   * Buffers a committed delta for re-application on the published CSR.
   *
   * @return false once the build has published: the caller applies the delta to the view as usual
   */
  synchronized boolean offer(final TxDelta delta) {
    if (!open)
      return false;
    if (bufferedDeltas.size() >= maxBufferedDeltas)
      overflowed = true;
    else if (!overflowed)
      bufferedDeltas.add(delta);
    return true;
  }

  /**
   * Records that a relevant commit raced this build, for the build to act on when it publishes.
   *
   * @return false when the build has already published: the caller acts on the commit itself
   */
  synchronized boolean markRelevantCommit() {
    if (!open)
      return false;
    relevantCommit = true;
    return true;
  }

  boolean hadRelevantCommit() {
    return relevantCommit;
  }

  /**
   * Stops registrations and buffering.
   *
   * @return the deltas buffered while the build ran, in commit-callback order
   */
  synchronized List<TxDelta> close() {
    open = false;
    return bufferedDeltas;
  }

  synchronized boolean hasOverflowed() {
    return overflowed;
  }

  /** Whether any edge change raced with the scan, i.e. whether the published CSR needs this watch to reconcile. */
  boolean hasWatchedSources() {
    return !watchedSources.isEmpty();
  }

  /**
   * Binds this watch to the CSR its build produced, from which point it answers the merge. The view drops the watch
   * whenever that CSR stops being the base.
   */
  void bindTo(final Map<String, CSRAdjacencyIndex> csrPerType) {
    this.csrPerType = csrPerType;
  }

  Map<String, CSRAdjacencyIndex> getCsrPerType() {
    return csrPerType;
  }

  /**
   * Answers, for each edge change of one delta, whether the scan read it. Must be called for every delta, in order,
   * right before it is merged against the CSR this watch is bound to.
   */
  void account(final TxDelta delta) {
    answers.clear();
    // Only the sources registered while the scan ran can differ from what it read: every other change committed after
    // it, so the bookkeeping, and its memory, stays bounded by the changes the scan actually raced with
    for (final TxDelta.EdgeDelta ed : delta.addedEdges) {
      if (!watchedSources.contains(ed.source))
        continue;
      if (sawEdge(ed))
        answers.put(ed, Boolean.TRUE);
    }
    for (final TxDelta.EdgeDelta ed : delta.deletedEdges) {
      if (!watchedSources.contains(ed.source))
        continue;
      // An edge added after the scan and deleted again never reaches this answer: the merge withdraws its overlay
      // addition by identity first. One the scan captured is in what it saw, so its deletion is news to the base.
      final Map<RID, RID> seen = observedSources.get(ed.source);
      if (seen != null && !Objects.equals(seen.get(ed.rid), ed.target))
        answers.put(ed, Boolean.TRUE);
    }
    // Only now: the edge is gone, so a later edge the engine creates in its recycled RID is a different one
    for (final TxDelta.EdgeDelta ed : delta.deletedEdges) {
      final Map<RID, RID> seen = observedSources.get(ed.source);
      if (seen != null && seen != NOTHING_SEEN)
        seen.remove(ed.rid);
    }
  }

  @Override
  public boolean capturedByScan(final TxDelta.EdgeDelta addition) {
    return answers.containsKey(addition);
  }

  @Override
  public boolean absorbedByScan(final TxDelta.EdgeDelta deletion) {
    return answers.containsKey(deletion);
  }

  private boolean sawEdge(final TxDelta.EdgeDelta ed) {
    final Map<RID, RID> seen = observedSources.get(ed.source);
    return seen != null && Objects.equals(seen.get(ed.rid), ed.target);
  }
}
