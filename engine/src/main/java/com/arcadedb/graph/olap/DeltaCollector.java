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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Detects when a transaction affects vertex/edge types covered by the GAV and handles updates.
 * <p>
 * Behavior depends on the view's {@link GraphAnalyticalView.UpdateMode}:
 * <ul>
 *   <li><b>SYNCHRONOUS</b>: Collects detailed changes (TxDelta) during the transaction,
 *       then applies them to the overlay on commit — no stale window.</li>
 *   <li><b>ASYNCHRONOUS</b>: Only detects that a relevant change occurred,
 *       then triggers an async rebuild on commit.</li>
 *   <li><b>OFF</b>: Only detects relevance, marks the view as STALE on commit.</li>
 * </ul>
 */
class DeltaCollector implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private static final AtomicInteger ANONYMOUS_COUNTER = new AtomicInteger();

  // Past this many edge property changes in one transaction, they stop being tracked individually. See
  // trackEdgeUpdate().
  private static final int          MAX_TRACKED_EDGE_UPDATES = 1024;

  private final GraphAnalyticalView view;
  private final String              callbackKey;

  // Only used in SYNCHRONOUS mode: per-thread delta tracking.
  // Uses ConcurrentHashMap keyed by thread ID instead of ThreadLocal to allow complete
  // cleanup on close() — ThreadLocal entries leak in long-lived thread pools (e.g., HTTP server).
  private final ConcurrentHashMap<Long, TxDelta> perThreadDeltas;

  DeltaCollector(final GraphAnalyticalView view) {
    this.view = view;
    this.callbackKey = "gav-delta-" + (view.getName() != null ? view.getName() : "anon-" + ANONYMOUS_COUNTER.getAndIncrement());
    this.perThreadDeltas = view.getUpdateMode() == GraphAnalyticalView.UpdateMode.SYNCHRONOUS
        ? new ConcurrentHashMap<>()
        : null;
  }

  @Override
  public void onAfterCreate(final Record record) {
    if (!isRelevant(record))
      return;

    if (perThreadDeltas != null) {
      // SYNCHRONOUS: collect detailed changes
      final TxDelta delta = getOrCreateDelta();
      if (record instanceof Vertex vertex)
        delta.addedVertices.add(new TxDelta.VertexDelta(vertex.getIdentity(), extractProperties(vertex)));
      else if (record instanceof Edge edge)
        delta.addedEdges.add(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn(), edge.getIdentity(),
            extractMaterialisedEdgeProperties(edge)));
      scheduleSyncCallback(delta);
    } else {
      scheduleAsyncCallback();
    }
  }

  @Override
  public void onAfterUpdate(final Record record) {
    if (!isRelevant(record))
      return;

    if (perThreadDeltas != null) {
      // SYNCHRONOUS: collect property changes
      if (record instanceof Vertex vertex) {
        final TxDelta delta = getOrCreateDelta();
        delta.updatedProperties.put(vertex.getIdentity(), extractProperties(vertex));
        scheduleSyncCallback(delta);
      } else if (record instanceof Edge edge) {
        // Reported with its new values rather than as a bare "something changed" flag: an edge the overlay
        // itself holds carries its values there and can simply take the new ones, while an edge in the base
        // CSR is addressed by a column slot nothing maps back from its RID and needs the rebuild the flag used
        // to force unconditionally. DeltaOverlay.merge() is where the two are told apart, since it is what
        // knows which edges the overlay holds. See issues #4513 and #6315.
        final TxDelta delta = getOrCreateDelta();
        trackEdgeUpdate(delta, edge);
        scheduleSyncCallback(delta);
      }
    } else {
      scheduleAsyncCallback();
    }
  }

  @Override
  public void onAfterDelete(final Record record) {
    if (!isRelevant(record))
      return;

    if (perThreadDeltas != null) {
      // SYNCHRONOUS: collect deletions
      final TxDelta delta = getOrCreateDelta();
      if (record instanceof Vertex vertex)
        delta.deletedVertices.add(vertex.getIdentity());
      else if (record instanceof Edge edge)
        delta.deletedEdges.add(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn(), edge.getIdentity()));
      scheduleSyncCallback(delta);
    } else {
      scheduleAsyncCallback();
    }
  }

  private boolean isRelevant(final Record record) {
    if (!view.isBuilt())
      return false;
    if (record instanceof Vertex vertex)
      return view.coversVertexType(vertex.getTypeName());
    if (record instanceof Edge edge)
      return view.coversEdgeType(edge.getTypeName());
    return false;
  }

  // The callback captures the live `delta` reference, not a snapshot. Subsequent record events
  // in the same transaction append to the same delta. addAfterCommitCallbackIfAbsent ensures
  // the callback is registered only once (keyed by callbackKey), and the frozen copy is made
  // at commit time — after all record events have fired — so it captures the complete delta set.
  private void scheduleSyncCallback(final TxDelta delta) {
    try {
      final DatabaseInternal dbInternal = (DatabaseInternal) view.getDatabase();
      if (dbInternal.isTransactionActive()) {
        dbInternal.getTransaction().addAfterCommitCallbackIfAbsent(callbackKey, () -> {
          final TxDelta frozen = new TxDelta();
          frozen.addedVertices.addAll(delta.addedVertices);
          frozen.deletedVertices.addAll(delta.deletedVertices);
          frozen.addedEdges.addAll(delta.addedEdges);
          frozen.deletedEdges.addAll(delta.deletedEdges);
          frozen.updatedProperties.putAll(delta.updatedProperties);
          frozen.updatedEdges.putAll(delta.updatedEdges);
          frozen.forceEdgePropertyRebuild = delta.forceEdgePropertyRebuild;
          delta.clear();
          perThreadDeltas.remove(Thread.currentThread().getId());
          if (!frozen.isEmpty())
            view.applyDelta(frozen);
        });
      }
    } catch (final DatabaseIsClosedException e) {
      LogManager.instance().log(this, Level.FINE, "SYNC delta collection skipped (database closing): %s", e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "SYNC delta collection failed for GraphAnalyticalView '%s'", e, view.getName());
    }
  }

  private void scheduleAsyncCallback() {
    try {
      final DatabaseInternal dbInternal = (DatabaseInternal) view.getDatabase();
      if (dbInternal.isTransactionActive())
        dbInternal.getTransaction().addAfterCommitCallbackIfAbsent(callbackKey, view::onRelevantCommit);
    } catch (final DatabaseIsClosedException e) {
      LogManager.instance().log(this, Level.FINE, "ASYNC delta collection skipped (database closing): %s", e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "ASYNC delta collection failed for GraphAnalyticalView '%s'", e, view.getName());
    }
  }

  private TxDelta getOrCreateDelta() {
    final long tid = Thread.currentThread().getId();
    final TxDelta existing = perThreadDeltas.get(tid);
    if (existing != null) {
      // If there's an existing delta but the commit callback is no longer registered
      // (because a previous transaction rolled back and reset() cleared the callback keys),
      // discard the stale delta to avoid leaking rolled-back changes into this transaction.
      final DatabaseInternal dbInternal = (DatabaseInternal) view.getDatabase();
      if (dbInternal.isTransactionActive() && !dbInternal.getTransaction().hasCallbackKey(callbackKey))
        existing.clear();
      return existing;
    }
    final TxDelta fresh = new TxDelta();
    perThreadDeltas.put(tid, fresh);
    return fresh;
  }

  /**
   * Releases all per-thread delta state. Must be called when the collector is deregistered
   * to prevent memory leaks in long-lived thread pools (e.g., HTTP server threads).
   */
  void close() {
    if (perThreadDeltas != null)
      perThreadDeltas.clear();
  }

  /**
   * Records one edge property change, or gives up on recording them individually.
   * <p>
   * Tracking each one is what lets {@link DeltaOverlay#merge} apply an update to an edge the overlay itself
   * holds without marking the base columns out of date (issue #6315). It is worth an entry per edge only while
   * there are few of them: a transaction rewriting a million edges' weights would otherwise hold a million of
   * these, where the old boolean flag held nothing. Past the cap the delta says only "the columns are out of
   * date", which is what a bulk rewrite means anyway - the rebuild it forces is going to reread every value
   * regardless, and until it lands the view serves no edge properties at all, so no stale one can escape.
   */
  private void trackEdgeUpdate(final TxDelta delta, final Edge edge) {
    // A view that materialises no edge property columns has nothing that can go out of date when an edge's
    // properties change, and nothing to rebuild them from - the base CSR holds the topology, which an update
    // does not touch. It was rebuilt anyway, on every single edge update, because #4513's flag was raised
    // before anyone asked whether there were columns at all. Which property changed is still not asked, and
    // cannot be: the listener is handed the record, not a diff against its previous values, so a view that
    // materialises `weight` cannot tell an update of `weight` from an update of `label` and has to assume the
    // worse of the two.
    final String[] materialised = view.getEdgePropertyFilter();
    if (materialised == null || materialised.length == 0)
      return;
    if (delta.forceEdgePropertyRebuild)
      return;
    if (delta.updatedEdges.size() >= MAX_TRACKED_EDGE_UPDATES) {
      delta.forceEdgePropertyRebuild = true;
      delta.updatedEdges.clear();
      return;
    }
    delta.updatedEdges.put(edge.getIdentity(), new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(),
        edge.getIn(), edge.getIdentity(), extractMaterialisedEdgeProperties(edge)));
  }

  /**
   * Reads the values of the edge properties the view materialises columns for, or {@code null} when it
   * materialises none.
   * <p>
   * Captured here, at commit time, rather than looked up from the record when an algorithm asks: the overlay
   * outlives the transaction, and an added edge has no column slot to be read from - the columns were built
   * with the base CSR - so this is what lets the view answer for it exactly instead of at a default weight
   * (issue #6315). Only the materialised names are kept, so an edge with fifty properties on a view that
   * indexes one costs one entry.
   */
  private Map<String, Object> extractMaterialisedEdgeProperties(final Edge edge) {
    final String[] materialised = view.getEdgePropertyFilter();
    if (materialised == null || materialised.length == 0)
      return null;

    Map<String, Object> props = null;
    for (final String name : materialised) {
      final Object value = edge.get(name);
      if (value == null)
        continue;
      // Sized so the whole filter fits without a resize: HashMap's argument is the bucket count, and it grows
      // once past load factor x capacity, not past capacity.
      if (props == null)
        props = new HashMap<>((int) (materialised.length / 0.75f) + 1);
      props.put(name, value);
    }
    return props;
  }

  private static Map<String, Object> extractProperties(final Document doc) {
    final Set<String> names = doc.getPropertyNames();
    if (names.isEmpty())
      return Collections.emptyMap();
    final Map<String, Object> props = new HashMap<>(names.size());
    for (final String name : names)
      props.put(name, doc.get(name));
    return props;
  }
}
