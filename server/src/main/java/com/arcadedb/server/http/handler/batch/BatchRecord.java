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
package com.arcadedb.server.http.handler.batch;

/**
 * Mutable record struct reused across parsed lines to minimize GC pressure.
 * The {@code properties} array holds alternating key-value pairs (String key, Object value).
 */
public class BatchRecord {

  public enum Kind { VERTEX, EDGE }

  public Kind     kind;
  public String   typeName;
  public String   tempId;      // client-assigned temporary ID (vertices), nullable
  public String   fromRef;     // edge source: temp ID or "#bucket:pos"
  public String   toRef;       // edge destination: temp ID or "#bucket:pos"
  public Object[] properties;  // alternating [key, value, key, value, ...]
  public int      propertyCount; // number of property PAIRS (actual array usage = propertyCount * 2)

  public BatchRecord() {
    properties = new Object[32]; // initial capacity for 16 properties
  }

  public void reset() {
    kind = null;
    typeName = null;
    tempId = null;
    fromRef = null;
    toRef = null;
    propertyCount = 0;
    // No need to null-out the properties array; propertyCount controls the valid range
  }

  public void addProperty(final String key, final Object value) {
    final int idx = propertyCount * 2;
    if (idx + 1 >= properties.length) {
      final Object[] bigger = new Object[properties.length * 2];
      System.arraycopy(properties, 0, bigger, 0, properties.length);
      properties = bigger;
    }
    properties[idx] = key;
    properties[idx + 1] = value;
    propertyCount++;
  }

  /**
   * Returns a copy of the properties as an Object[] suitable for GraphBatch.createVertices().
   * Format: [key1, value1, key2, value2, ...]
   */
  public Object[] copyProperties() {
    final int len = propertyCount * 2;
    final Object[] copy = new Object[len];
    System.arraycopy(properties, 0, copy, 0, len);
    return copy;
  }

  /**
   * Returns properties as a varargs-compatible Object[] for GraphBatch.newEdge().
   */
  public Object[] copyEdgeProperties() {
    return copyProperties();
  }
}
