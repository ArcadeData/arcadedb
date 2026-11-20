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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.query.opencypher.procedures.CypherProcedure;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The argument parsing the {@code db.analyticalView.*} procedures share (issue #11438). The work itself is in
 * {@link com.arcadedb.graph.olap.GraphAnalyticalViewCommands}, shared with the SQL statements.
 */
abstract class AbstractDbAnalyticalViewProcedure implements CypherProcedure {

  /** The view name: a string that is not blank. */
  protected String extractName(final Object arg) {
    if (!(arg instanceof String name) || name.isBlank())
      throw new IllegalArgumentException(getName() + "(): the view name must be a non-blank string, got " + describe(arg));
    return name;
  }

  /**
   * A list of names: null for none given, a single string, or a list of strings. An empty list is kept empty, which
   * {@code properties} reads as "copy no property".
   */
  protected String[] extractNames(final Object arg, final String paramName) {
    return switch (arg) {
      case null -> null;
      case String s -> new String[] { s };
      case Collection<?> coll -> {
        final String[] names = new String[coll.size()];
        int i = 0;
        for (final Object o : coll) {
          if (!(o instanceof String s) || s.isBlank())
            throw new IllegalArgumentException(getName() + "(): " + paramName + " must hold non-blank strings, got " + describe(o));
          names[i++] = s;
        }
        yield names;
      }
      default -> throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a string or a list of strings, got " + describe(arg));
    };
  }

  /**
   * The configuration map: a key outside {@code allowedKeys} is refused by name rather than ignored, as the {@code algo.*}
   * procedures refuse it (issue #11145).
   */
  @SuppressWarnings("unchecked")
  protected Map<String, Object> extractConfig(final Object arg, final Set<String> allowedKeys) {
    if (arg == null)
      return Map.of();
    if (!(arg instanceof Map<?, ?> map))
      throw new IllegalArgumentException(getName() + "(): config must be a map, got " + describe(arg));
    for (final Object key : map.keySet())
      if (!allowedKeys.contains(key))
        throw new IllegalArgumentException(
            getName() + "(): unknown config key '" + key + "', expected one of " + new TreeSet<>(allowedKeys));
    return (Map<String, Object>) map;
  }

  protected boolean extractBoolean(final Map<String, Object> config, final String key) {
    final Object value = config.get(key);
    if (value == null)
      return false;
    if (value instanceof Boolean b)
      return b;
    throw new IllegalArgumentException(getName() + "(): " + key + " must be a boolean, got " + describe(value));
  }

  @Override
  public boolean isWriteProcedure() {
    return true;
  }

  /** Create, rebuild and drop change the schema, as their SQL DDL statements do. */
  @Override
  public boolean isSchemaProcedure() {
    return true;
  }

  protected static String describe(final Object value) {
    return value == null ? "null" : value.getClass().getSimpleName() + " '" + value + "'";
  }
}
