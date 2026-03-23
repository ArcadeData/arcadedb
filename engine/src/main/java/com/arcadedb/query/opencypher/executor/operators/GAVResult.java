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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Zero-map Result implementation for GAV pipeline operators.
 * Uses parallel arrays (names[] + values[]) instead of LinkedHashMap, eliminating
 * HashMap, Node[], and Map.Entry allocations per row.
 * <p>
 * For a typical 4-variable result, this saves ~320 bytes per row vs ResultInternal:
 * <ul>
 *   <li>No LinkedHashMap object (64 bytes)</li>
 *   <li>No Node[8] backing array (96 bytes)</li>
 *   <li>No Map.Entry objects (4 × 48 = 192 bytes)</li>
 * </ul>
 * <p>
 * The names array is shared across all rows in a batch (interned by the operator),
 * so only the values array is allocated per row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GAVResult implements Result {
  private final String[] names;   // shared across rows (interned)
  private final Object[] values;  // one per row

  /**
   * @param names  variable names (shared/interned across all rows in the batch)
   * @param values property values for this row (one per name)
   */
  public GAVResult(final String[] names, final Object[] values) {
    this.names = names;
    this.values = values;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getProperty(final String name) {
    for (int i = 0; i < names.length; i++)
      if (names[i].equals(name))
        return (T) values[i];
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getProperty(final String name, final Object defaultValue) {
    for (int i = 0; i < names.length; i++)
      if (names[i].equals(name))
        return values[i] != null ? (T) values[i] : (T) defaultValue;
    return (T) defaultValue;
  }

  @Override
  public Record getElementProperty(final String name) {
    final Object v = getProperty(name);
    return v instanceof Record ? (Record) v : null;
  }

  @Override
  public Set<String> getPropertyNames() {
    final Set<String> result = new HashSet<>(names.length);
    for (final String n : names)
      if (n != null)
        result.add(n);
    return result;
  }

  @Override
  public Optional<RID> getIdentity() {
    return Optional.empty();
  }

  @Override
  public boolean isElement() {
    return false;
  }

  @Override
  public Optional<Document> getElement() {
    return Optional.empty();
  }

  @Override
  public Document toElement() {
    throw new UnsupportedOperationException("GAVResult is a projection, not an element");
  }

  @Override
  public Optional<Record> getRecord() {
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return true;
  }

  @Override
  public Object getMetadata(final String key) {
    return null;
  }

  @Override
  public Set<String> getMetadataKeys() {
    return Set.of();
  }

  @Override
  public boolean hasProperty(final String varName) {
    for (int i = 0; i < names.length; i++)
      if (names[i].equals(varName) && values[i] != null)
        return true;
    return false;
  }

  @Override
  public Map<String, Object> toMap() {
    final Map<String, Object> map = new java.util.LinkedHashMap<>(names.length);
    for (int i = 0; i < names.length; i++)
      if (names[i] != null)
        map.put(names[i], values[i]);
    return map;
  }

  @Override
  public Database getDatabase() {
    return null;
  }
}
