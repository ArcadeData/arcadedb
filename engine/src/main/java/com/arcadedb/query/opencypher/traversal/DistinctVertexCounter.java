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
package com.arcadedb.query.opencypher.traversal;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * The vertices a fused {@code count(DISTINCT v)} over the walks of a distinct-endpoint expansion has counted so far (issues #10548,
 * #10557): by dense node id where a Graph Analytical View maps them, so a vertex met both through the view and through the records is
 * counted once, by RID otherwise. Shared by the cost-based operator and the step-based executor, which count the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DistinctVertexCounter {
  private final GAVDistinctEndpointTraverser view;
  private final BitSet                       ids = new BitSet();
  private       Set<RID>                     rids;
  private       long                         count;

  /** @param view the view the walks read, or null when they all walk the records */
  public DistinctVertexCounter(final GAVDistinctEndpointTraverser view) {
    this.view = view;
  }

  public boolean contains(final Vertex vertex) {
    final int nodeId = view != null ? view.nodeIdOf(vertex) : -1;
    return nodeId >= 0 ? ids.get(nodeId) : rids != null && rids.contains(vertex.getIdentity());
  }

  public void add(final Vertex vertex) {
    final int nodeId = view != null ? view.nodeIdOf(vertex) : -1;
    if (nodeId >= 0) {
      if (!ids.get(nodeId)) {
        ids.set(nodeId);
        ++count;
      }
      return;
    }
    if (rids == null)
      rids = new HashSet<>();
    if (rids.add(vertex.getIdentity()))
      ++count;
  }

  /**
   * Counts the end vertices of the walk from {@code start} through the view straight into the node ids counted so far, with no
   * vertex built (see {@link GAVDistinctEndpointTraverser#countInto}).
   *
   * @return false when there is no view or it does not map {@code start}, and the caller has to walk the records
   */
  public boolean countThroughView(final Vertex start, final boolean excludeStart, final String targetLabel) {
    if (view == null)
      return false;
    final long added = view.countInto(start, excludeStart, targetLabel, ids);
    if (added < 0)
      return false;
    count += added;
    return true;
  }

  public long getCount() {
    return count;
  }
}
