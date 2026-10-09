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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.function.sql.math.SQLFunctionMathAbstract;
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.ShortestPathFinder;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.FileUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Weighted point-to-point shortest path answered by a Customizable Contraction Hierarchy when a Graph Analytical View
 * keeps one for the weight and edge types asked for, and by bidirectional Dijkstra otherwise (issue #9437). Returns the
 * vertices of the path, like {@code dijkstra()}.
 * <p>
 * The weight follows {@link EdgeWeight}: the edge property's numeric value, 1 for an edge without one, and an edge whose
 * value is negative, NaN or infinite is not walked.
 * <pre>
 *   SELECT cchShortestPath($a, $b, 'distance')
 *   SELECT cchShortestPath($a, $b, 'distance', 'BOTH')
 *   SELECT cchShortestPath($a, $b, 'distance', { direction: 'OUT', edgeTypeNames: ['ROAD'] })
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionCCHShortestPath extends SQLFunctionMathAbstract {
  public static final String NAME = "cchShortestPath";

  private static final Set<String> OPTIONS = Set.of("direction", "edgeTypeNames");

  public SQLFunctionCCHShortestPath() {
    this(NAME);
  }

  /**
   * For the functions that answer through the same engine under another name, {@code duanSSSP()} (issue #9443): they
   * extend this class rather than copying it, so the weight rule, the options and the fallback chain cannot drift apart.
   * Every message names {@link #getName()}, not {@link #NAME}, for that reason.
   */
  protected SQLFunctionCCHShortestPath(final String name) {
    super(name);
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public List<RID> execute(final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {
    final Document record = currentRecord != null ? (Document) currentRecord.getRecord() : null;
    final RID source = vertexOf(params[0], record, "sourceVertex");
    final RID destination = vertexOf(params[1], record, "destinationVertex");
    final String weightProperty = params.length > 2 && params[2] != null ? FileUtils.getStringContent(params[2]) : "weight";

    Vertex.DIRECTION direction = Vertex.DIRECTION.OUT;
    String[] edgeTypes = null;
    if (params.length > 3 && params[3] != null) {
      if (params[3] instanceof Map<?, ?> rawMap) {
        final FunctionOptions options = new FunctionOptions(getName(), rawMap, OPTIONS);
        if (options.containsKey("direction"))
          direction = toDirection(options.get("direction"), getName());
        if (options.containsKey("edgeTypeNames"))
          edgeTypes = toStringArray(options.get("edgeTypeNames"));
      } else
        direction = toDirection(params[3], getName());
    }

    final ShortestPathFinder.Result result = ShortestPathFinder.find(context.getDatabase(), source, destination,
        weightProperty, direction, edgeTypes, WorkGuard.forCommand(context, getName() + "()"), context);
    return result == null ? new ArrayList<>() : new ArrayList<>(result.vertices());
  }

  private static RID vertexOf(final Object param, final Document record, final String name) {
    Object value = param;
    if (MultiValue.isMultiValue(value)) {
      if (MultiValue.getSize(value) > 1)
        throw new IllegalArgumentException("Only one " + name + " is allowed");
      value = MultiValue.getFirstValue(value);
      if (value instanceof Result result && result.isElement())
        value = result.getElement().get();
    }
    if (record != null && value instanceof String field)
      value = record.get(field);
    if (value instanceof Identifiable identifiable && identifiable.getRecord() instanceof Vertex vertex)
      return vertex.getIdentity();
    throw new IllegalArgumentException("The " + name + " must be a vertex record");
  }

  private static Vertex.DIRECTION toDirection(final Object value, final String functionName) {
    if (value instanceof Vertex.DIRECTION direction)
      return direction;
    try {
      return Vertex.DIRECTION.valueOf(value.toString().trim().toUpperCase(Locale.ENGLISH));
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid direction '" + value + "' for " + functionName + "(): use OUT, IN or BOTH");
    }
  }

  private static String[] toStringArray(final Object value) {
    if (value == null)
      return null;
    if (value instanceof String s)
      return new String[] { s };
    if (value instanceof String[] array)
      return array;
    if (value instanceof Collection<?> collection) {
      final List<String> result = new ArrayList<>(collection.size());
      for (final Object item : collection)
        if (item != null)
          result.add(item.toString());
      return result.toArray(new String[0]);
    }
    return new String[] { value.toString() };
  }

  @Override
  public String getSyntax() {
    return "cchShortestPath(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName> [, <direction> | { direction, edgeTypeNames }])";
  }
}
