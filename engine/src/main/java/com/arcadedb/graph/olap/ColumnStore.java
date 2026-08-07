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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Flat columnar property storage indexed by dense node IDs.
 * <p>
 * Each property is stored as a typed {@link Column} — a flat primitive array
 * (int[], long[], double[]) or dictionary-encoded string array. This layout is
 * optimal for:
 * <ul>
 *   <li>Sequential scans: properties are contiguous in memory, cache-line friendly</li>
 *   <li>Vectorized processing: flat arrays are SIMD-friendly via JVM auto-vectorization</li>
 *   <li>Zero GC pressure: no boxing, no per-record object overhead</li>
 *   <li>Selective reads: only touched columns are loaded into CPU cache</li>
 * </ul>
 * <p>
 * Null values are tracked per column via a compact {@code long[]} bitset
 * (1 bit per node, 64 nodes per long).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ColumnStore {
  private final Map<String, Column> columns;
  private final int                 nodeCount;

  // Single-slot memoization of the last resolved (name, column) pair. Query loops
  // (e.g. GAVExpandAll reading a far-endpoint property once per traversed edge) call
  // getColumn()/getValue() with the SAME propertyName across many rows in a row, so
  // caching the last resolution turns a HashMap probe per row into a String#equals
  // check. `columns` is only ever populated during CSR build (see CSRBuilder), never
  // mutated afterward, so a cached entry can never go stale; and since both the name
  // and the column travel together inside one record referenced through a single
  // volatile field, a racing reader can only ever see a fully-formed pair, never one
  // field from an older resolution and the other from a newer one.
  private volatile CachedColumn lastResolved;

  private record CachedColumn(String name, Column column) {
  }

  public ColumnStore(final int nodeCount) {
    this.nodeCount = nodeCount;
    this.columns = new HashMap<>();
  }

  /**
   * Creates and registers a new column of the given type.
   */
  public Column createColumn(final String name, final Column.Type type) {
    final Column column = new Column(name, type, nodeCount);
    columns.put(name, column);
    return column;
  }

  /**
   * Returns the column for the given property name, or null if not present.
   */
  public Column getColumn(final String name) {
    final CachedColumn cached = lastResolved;
    if (cached != null && cached.name().equals(name))
      return cached.column();

    final Column column = columns.get(name);
    lastResolved = new CachedColumn(name, column);
    return column;
  }

  /**
   * Returns the property value for a given node, or null if not set or column doesn't exist.
   */
  public Object getValue(final int nodeId, final String propertyName) {
    final Column column = getColumn(propertyName);
    if (column == null || column.isNull(nodeId))
      return null;

    switch (column.getType()) {
    case INT:
      return column.getInt(nodeId);
    case LONG:
      return column.getLong(nodeId);
    case DOUBLE:
      return column.getDouble(nodeId);
    case STRING:
      return column.getString(nodeId);
    default:
      return null;
    }
  }

  /**
   * Returns the set of property names in this store.
   */
  public Set<String> getPropertyNames() {
    return Collections.unmodifiableSet(columns.keySet());
  }

  public int getNodeCount() {
    return nodeCount;
  }

  public int getColumnCount() {
    return columns.size();
  }

  /**
   * Returns approximate memory usage in bytes.
   */
  public long getMemoryUsageBytes() {
    long total = 0;
    for (final Column col : columns.values())
      total += col.getMemoryUsageBytes();
    return total;
  }
}
