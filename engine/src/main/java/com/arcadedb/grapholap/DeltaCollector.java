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
package com.arcadedb.grapholap;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.*;

/**
 * Listens to record changes during a transaction and collects them into a {@link TxDelta}.
 * On transaction commit, the delta is applied to the {@link GraphAnalyticalView}'s overlay.
 * <p>
 * Implements all three after-record listeners so the view only needs one registration point.
 * Uses ThreadLocal to collect per-thread changes (one transaction per thread).
 */
class DeltaCollector implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private final GraphAnalyticalView view;
  private final ThreadLocal<TxDelta> currentDelta = ThreadLocal.withInitial(TxDelta::new);

  DeltaCollector(final GraphAnalyticalView view) {
    this.view = view;
  }

  @Override
  public void onAfterCreate(final Record record) {
    if (!view.isReady())
      return;

    if (record instanceof Vertex vertex) {
      if (!view.coversVertexType(vertex.getTypeName()))
        return;
      final TxDelta delta = currentDelta.get();
      final Map<String, Object> props = extractProperties(vertex);
      delta.addedVertices.add(new TxDelta.VertexDelta(vertex.getIdentity(), props));
      scheduleCommitCallback(delta);

    } else if (record instanceof Edge edge) {
      if (!view.coversEdgeType(edge.getTypeName()))
        return;
      final TxDelta delta = currentDelta.get();
      delta.addedEdges.add(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn()));
      scheduleCommitCallback(delta);
    }
  }

  @Override
  public void onAfterUpdate(final Record record) {
    if (!view.isReady())
      return;

    if (record instanceof Vertex vertex) {
      if (!view.coversVertexType(vertex.getTypeName()))
        return;
      final TxDelta delta = currentDelta.get();
      delta.updatedProperties.put(vertex.getIdentity(), extractProperties(vertex));
      scheduleCommitCallback(delta);
    }
  }

  @Override
  public void onAfterDelete(final Record record) {
    if (!view.isReady())
      return;

    if (record instanceof Vertex vertex) {
      if (!view.coversVertexType(vertex.getTypeName()))
        return;
      final TxDelta delta = currentDelta.get();
      delta.deletedVertices.add(vertex.getIdentity());
      scheduleCommitCallback(delta);

    } else if (record instanceof Edge edge) {
      if (!view.coversEdgeType(edge.getTypeName()))
        return;
      final TxDelta delta = currentDelta.get();
      delta.deletedEdges.add(new TxDelta.EdgeDelta(edge.getTypeName(), edge.getOut(), edge.getIn()));
      scheduleCommitCallback(delta);
    }
  }

  private void scheduleCommitCallback(final TxDelta delta) {
    try {
      final DatabaseInternal dbInternal = (DatabaseInternal) view.getDatabase();
      if (dbInternal.isTransactionActive()) {
        final String callbackId = "gav-delta-" + (view.getName() != null ? view.getName() : System.identityHashCode(view));
        dbInternal.getTransaction().addAfterCommitCallbackIfAbsent(callbackId, () -> {
          final TxDelta frozen = new TxDelta();
          frozen.addedVertices.addAll(delta.addedVertices);
          frozen.deletedVertices.addAll(delta.deletedVertices);
          frozen.addedEdges.addAll(delta.addedEdges);
          frozen.deletedEdges.addAll(delta.deletedEdges);
          frozen.updatedProperties.putAll(delta.updatedProperties);
          delta.clear();
          if (!frozen.isEmpty())
            view.applyDelta(frozen);
        });
      }
    } catch (final Exception e) {
      // Not in a transaction context or database is closing — skip
    }
  }

  private Map<String, Object> extractProperties(final Document doc) {
    final Set<String> names = doc.getPropertyNames();
    if (names.isEmpty())
      return Collections.emptyMap();
    final Map<String, Object> props = new HashMap<>(names.size());
    for (final String name : names)
      props.put(name, doc.get(name));
    return props;
  }
}
