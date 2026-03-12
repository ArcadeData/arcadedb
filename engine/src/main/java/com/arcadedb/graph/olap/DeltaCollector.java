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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

  private final GraphAnalyticalView view;
  private final String              callbackKey;

  // Only used in SYNCHRONOUS mode: per-thread delta tracking.
  // Uses ConcurrentHashMap keyed by thread ID instead of ThreadLocal to allow complete
  // cleanup on close() — ThreadLocal entries leak in long-lived thread pools (e.g., HTTP server).
  private final ConcurrentHashMap<Long, TxDelta> perThreadDeltas;

  DeltaCollector(final GraphAnalyticalView view) {
    this.view = view;
    this.callbackKey = "gav-delta-" + (view.getName() != null ? view.getName() : System.identityHashCode(view));
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
        delta.addedEdges.add(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn()));
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
        delta.deletedEdges.add(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn()));
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
          delta.clear();
          perThreadDeltas.remove(Thread.currentThread().threadId());
          if (!frozen.isEmpty())
            view.applyDelta(frozen);
        });
      }
    } catch (final com.arcadedb.exception.DatabaseIsClosedException e) {
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
    } catch (final com.arcadedb.exception.DatabaseIsClosedException e) {
      LogManager.instance().log(this, Level.FINE, "ASYNC delta collection skipped (database closing): %s", e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "ASYNC delta collection failed for GraphAnalyticalView '%s'", e, view.getName());
    }
  }

  private TxDelta getOrCreateDelta() {
    return perThreadDeltas.computeIfAbsent(Thread.currentThread().threadId(), k -> new TxDelta());
  }

  /**
   * Releases all per-thread delta state. Must be called when the collector is deregistered
   * to prevent memory leaks in long-lived thread pools (e.g., HTTP server threads).
   */
  void close() {
    if (perThreadDeltas != null)
      perThreadDeltas.clear();
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
