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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;

/**
 * Lightweight vertex reference for GAV-accelerated traversal.
 * Stores only the RID and CSR nodeId, avoiding the expensive OLTP vertex load.
 * <p>
 * Used by {@link GAVExpandAll} for intermediate hops where vertex properties
 * are not needed — only the identity and CSR adjacency matter.
 * <p>
 * If properties ARE needed, call {@link #resolve(Database)} to load the full vertex.
 */
public final class GAVVertexReference {
  private final RID rid;
  private final int nodeId;
  private final GraphTraversalProvider provider;

  public GAVVertexReference(final RID rid, final int nodeId, final GraphTraversalProvider provider) {
    this.rid = rid;
    this.nodeId = nodeId;
    this.provider = provider;
  }

  public RID getIdentity() {
    return rid;
  }

  public int getNodeId() {
    return nodeId;
  }

  public GraphTraversalProvider getProvider() {
    return provider;
  }

  /**
   * Loads the full vertex from OLTP storage. Call only when properties are needed.
   */
  public Vertex resolve(final Database database) {
    return (Vertex) database.lookupByRID(rid, true);
  }
}
