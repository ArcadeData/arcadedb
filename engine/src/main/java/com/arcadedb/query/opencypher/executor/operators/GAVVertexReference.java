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
 * Properties can be read directly from the GAV column store via {@link #get(String)},
 * avoiding OLTP I/O entirely when the property is materialized in the GAV.
 * Falls back to full vertex load via {@link #resolve(Database)} only when needed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
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
   * Reads a property from the GAV column store. Returns null if the property
   * is not materialized in the GAV (caller should fall back to {@link #resolve(Database)}).
   */
  public Object get(final String propertyName) {
    return provider.getProperty(nodeId, propertyName);
  }

  /**
   * Returns the vertex type name from the schema via bucket ID lookup.
   */
  public String getTypeName(final Database database) {
    return database.getSchema().getTypeByBucketId(rid.getBucketId()).getName();
  }

  /**
   * Loads the full vertex from OLTP storage. Call only when column store doesn't have the needed property.
   */
  public Vertex resolve(final Database database) {
    return (Vertex) database.lookupByRID(rid, true);
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj instanceof GAVVertexReference other)
      return rid.equals(other.rid);
    return false;
  }

  @Override
  public String toString() {
    return "GAVRef(" + rid + ",#" + nodeId + ")";
  }
}
