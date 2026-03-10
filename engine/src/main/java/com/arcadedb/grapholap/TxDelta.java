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

  boolean isEmpty() {
    return addedVertices.isEmpty() && deletedVertices.isEmpty()
        && addedEdges.isEmpty() && deletedEdges.isEmpty()
        && updatedProperties.isEmpty();
  }

  void clear() {
    addedVertices.clear();
    deletedVertices.clear();
    addedEdges.clear();
    deletedEdges.clear();
    updatedProperties.clear();
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

    EdgeDelta(final String edgeType, final RID source, final RID target) {
      this.edgeType = edgeType;
      this.source = source;
      this.target = target;
    }
  }
}
