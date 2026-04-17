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
package com.arcadedb.function.sql;

import com.arcadedb.exception.CommandSQLParsingException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Read-only wrapper over a {@code Map<String, Object>} used by SQL functions that accept a trailing options map in place of
 * multiple positional optional arguments.
 * <p>
 * Typical usage inside a SQL function:
 * <pre>
 *   private static final Set&lt;String&gt; OPTS = Set.of("efSearch", "filter");
 *
 *   if (params.length &gt;= 4 &amp;&amp; params[3] instanceof Map&lt;?, ?&gt; rawMap) {
 *     final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTS);
 *     efSearch = opts.getInt("efSearch", -1);
 *     filter   = opts.getList("filter");
 *   }
 * </pre>
 * Unknown keys are rejected eagerly with {@link CommandSQLParsingException} to catch typos. Getters coerce numeric / boolean /
 * string types and fall back to the provided default when the key is absent.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class FunctionOptions {
  private final String              functionName;
  private final Map<String, Object> options;

  @SuppressWarnings("unchecked")
  public FunctionOptions(final String functionName, final Map<?, ?> options, final Set<String> allowedKeys) {
    this.functionName = functionName;
    this.options = options == null ? Collections.emptyMap() : (Map<String, Object>) options;

    for (final Object rawKey : this.options.keySet()) {
      final String key = rawKey == null ? null : rawKey.toString();
      if (key == null || !allowedKeys.contains(key))
        throw new CommandSQLParsingException(
            "Unknown option '" + key + "' for function '" + functionName + "'. Allowed: " + new TreeSet<>(allowedKeys));
    }
  }

  public boolean isEmpty() {
    return options.isEmpty();
  }

  public boolean containsKey(final String key) {
    return options.containsKey(key);
  }

  public Object get(final String key) {
    return options.get(key);
  }

  public int getInt(final String key, final int defaultValue) {
    final Object v = options.get(key);
    if (v == null)
      return defaultValue;
    if (v instanceof Number n)
      return n.intValue();
    try {
      return Integer.parseInt(v.toString());
    } catch (final NumberFormatException e) {
      throw new CommandSQLParsingException(
          "Option '" + key + "' for function '" + functionName + "' must be an integer, got: " + v);
    }
  }

  public long getLong(final String key, final long defaultValue) {
    final Object v = options.get(key);
    if (v == null)
      return defaultValue;
    if (v instanceof Number n)
      return n.longValue();
    try {
      return Long.parseLong(v.toString());
    } catch (final NumberFormatException e) {
      throw new CommandSQLParsingException(
          "Option '" + key + "' for function '" + functionName + "' must be a long, got: " + v);
    }
  }

  public double getDouble(final String key, final double defaultValue) {
    final Object v = options.get(key);
    if (v == null)
      return defaultValue;
    if (v instanceof Number n)
      return n.doubleValue();
    try {
      return Double.parseDouble(v.toString());
    } catch (final NumberFormatException e) {
      throw new CommandSQLParsingException(
          "Option '" + key + "' for function '" + functionName + "' must be a number, got: " + v);
    }
  }

  public boolean getBoolean(final String key, final boolean defaultValue) {
    final Object v = options.get(key);
    if (v == null)
      return defaultValue;
    if (v instanceof Boolean b)
      return b;
    return Boolean.parseBoolean(v.toString());
  }

  public String getString(final String key, final String defaultValue) {
    final Object v = options.get(key);
    return v == null ? defaultValue : v.toString();
  }

  public List<?> getList(final String key) {
    final Object v = options.get(key);
    if (v == null)
      return null;
    if (v instanceof List<?> l)
      return l;
    throw new CommandSQLParsingException(
        "Option '" + key + "' for function '" + functionName + "' must be a list, got: " + v.getClass().getSimpleName());
  }
}
