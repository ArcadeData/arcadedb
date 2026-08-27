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

import java.util.*;

/**
 * Captures vertex/edge changes from a single transaction for incremental GAV update.
 * Populated by {@link DeltaCollector} during the transaction, then frozen and applied
 * to the {@link DeltaOverlay} after commit.
 * <p>
 * Not thread-safe — accessed only by the owning transaction thread.
 */
class TxDelta {
  final List<VertexDelta>          addedVertices    = new ArrayList<>();
  final Set<RID>                   deletedVertices  = new HashSet<>();
  final List<EdgeDelta>            addedEdges       = new ArrayList<>();
  final List<EdgeDelta>            deletedEdges     = new ArrayList<>();
  final Map<RID, Map<String, Object>> updatedProperties = new HashMap<>();

  // Covered edges whose properties changed, with the values they changed to. An edge THIS overlay window
  // added carries its values in its own overlay entry, so such an update is applied there and costs nothing
  // (which is the ordinary `newEdge(...).save()`, an insert that reports one create and one update). An edge
  // already in the base CSR is addressed by a column slot instead, and nothing maps that slot back from its
  // RID, so an update to one leaves the columns holding a value the database no longer has and only a rebuild
  // can repair them - which DeltaOverlay.merge() decides, being the one place that knows which of the two this
  // is. See issues #4513 and #6315.
  final List<EdgeDelta>            updatedEdges     = new ArrayList<>();

  // Set only by the synthetic delta GraphAnalyticalView uses to schedule the follow-up rebuild an edge
  // property update buffered during a compaction needs (#4513). It stands for no edge in particular, so it
  // marks the columns out of date outright rather than being resolved against the overlay's own additions.
  boolean                          forceEdgePropertyRebuild = false;

  /**
   * True when this transaction changed a covered edge's properties, however the change is spelled: the
   * individual edges while there were few enough of them to track, or the bare flag {@link DeltaCollector}
   * falls back to past its cap. Asked through one method rather than open-coded, because a caller that
   * remembers only the first spelling silently skips exactly the deltas that need the rebuild most - a bulk
   * rewrite - and the view is then left unable to serve edge properties until some later commit happens to
   * re-trigger it.
   */
  boolean hasEdgePropertyChanges() {
    return !updatedEdges.isEmpty() || forceEdgePropertyRebuild;
  }

  boolean isEmpty() {
    return addedVertices.isEmpty() && deletedVertices.isEmpty()
        && addedEdges.isEmpty() && deletedEdges.isEmpty()
        && updatedProperties.isEmpty() && !hasEdgePropertyChanges();
  }

  void clear() {
    addedVertices.clear();
    deletedVertices.clear();
    addedEdges.clear();
    deletedEdges.clear();
    updatedProperties.clear();
    updatedEdges.clear();
    forceEdgePropertyRebuild = false;
  }

  static class VertexDelta {
    final RID                rid;
    final Map<String, Object> properties;

    VertexDelta(final RID rid, final Map<String, Object> properties) {
      this.rid = rid;
      this.properties = properties;
    }
  }

  static class EdgeDelta {
    final String edgeType;
    final RID    source;
    final RID    target;
    // The deleted/added edge's own identity. For deletedEdges this is what lets DeltaOverlay.merge()
    // tell "the same edge reported twice" (replayed across merges, or emitted twice within one TxDelta -
    // must not double-count) apart from "two distinct parallel edges between the same pair" (must each
    // count, see issue #6769).
    final RID    rid;
    // For addedEdges and updatedEdges, the values of the edge properties the view materialises columns for, or
    // null when it materialises none. An added edge has no column slot of its own - the columns were built with
    // the base CSR - so this is the only place its weight can be read from while the overlay is the view's
    // representation of it (issue #6315). Null for deletedEdges, which are never asked for a value.
    final Map<String, Object> properties;

    EdgeDelta(final String edgeType, final RID source, final RID target, final RID rid) {
      this(edgeType, source, target, rid, null);
    }

    EdgeDelta(final String edgeType, final RID source, final RID target, final RID rid,
        final Map<String, Object> properties) {
      this.edgeType = edgeType;
      this.source = source;
      this.target = target;
      this.rid = rid;
      this.properties = properties;
    }
  }
}
