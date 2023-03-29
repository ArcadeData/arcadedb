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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class contains statistics about graph structure and query execution.
 * <p>
 * To obtain a copy of this object, use
 *
 * @author Luigi Dell'Aquila
 */
public class QueryStats {

  public final Map<String, Long> stats = new ConcurrentHashMap<>();

  public static QueryStats get(final Database db) {
    return new QueryStats();//TODO
  }

  public long getIndexStats(final String indexName, final int params, final boolean range, final boolean additionalRange) {
    final String key = generateKey("INDEX", indexName, String.valueOf(params), String.valueOf(range), String.valueOf(additionalRange));
    final Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public void pushIndexStats(final String indexName, final int params, final boolean range, final boolean additionalRange, final Long value) {
    final String key = generateKey("INDEX", indexName, String.valueOf(params), String.valueOf(range), String.valueOf(additionalRange));
    pushValue(key, value);
  }

  public long getAverageOutEdgeSpan(final String vertexClass, final String edgeClass) {
    final String key = generateKey(vertexClass, "-", edgeClass, "->");
    final Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public long getAverageInEdgeSpan(final String vertexClass, final String edgeClass) {
    final String key = generateKey(vertexClass, "<-", edgeClass, "-");
    final Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public long getAverageBothEdgeSpan(final String vertexClass, final String edgeClass) {
    final String key = generateKey(vertexClass, "-", edgeClass, "-");
    final Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public void pushAverageOutEdgeSpan(final String vertexClass, final String edgeClass, final Long value) {
    final String key = generateKey(vertexClass, "-", edgeClass, "->");
    pushValue(key, value);
  }

  public void pushAverageInEdgeSpan(final String vertexClass, final String edgeClass, final Long value) {
    final String key = generateKey(vertexClass, "<-", edgeClass, "-");
    pushValue(key, value);
  }

  public void pushAverageBothEdgeSpan(final String vertexClass, final String edgeClass, final Long value) {
    final String key = generateKey(vertexClass, "-", edgeClass, "-");
    pushValue(key, value);
  }

  private void pushValue(final String key, final Long value) {
    if (value == null) {
      return;
    }
    Long val = stats.get(key);

    if (val == null) {
      val = value;
    } else {
      //refine this ;-)
      val = ((Double) ((val * .9) + (value * .1))).longValue();
      if (value > 0 && val == 0) {
        val = 1L;
      }
    }
    stats.put(key, val);
  }

  protected String generateKey(final String... keys) {
    final StringBuilder result = new StringBuilder();
    for (final String s : keys) {
      result.append(".->");
      result.append(s);
    }
    return result.toString();
  }
}
