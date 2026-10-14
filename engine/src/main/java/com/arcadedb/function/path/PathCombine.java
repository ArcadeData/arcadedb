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
package com.arcadedb.function.path;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * path.combine(path1, path2) - Joins two paths into one, the second continuing from where the first ends (APOC's
 * {@code apoc.path.combine}).
 * <p>
 * The end node of {@code path1} must be the start node of {@code path2}; a {@code null} path leaves the other one as
 * the result. Paths of graph elements - the paths a {@code MATCH} binds, the maps the path procedures yield, alternating
 * node/relationship lists - combine into a real path, which {@code nodes()}, {@code relationships()}, {@code length()}
 * and the drivers read as one. The single-argument form {@code path.combine([p1, p2, ...])} joins a list of paths in
 * order.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathCombine extends AbstractPathFunction {
  @Override
  protected String getSimpleName() {
    return "combine";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Combine two paths, the second starting where the first ends, into a single path";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args.length == 2)
      return combine(args[0], args[1]);

    final Object arg = args[0];
    if (arg == null)
      return null;
    if (asTraversalPath(arg) == null && arg instanceof Collection<?> paths) {
      // A LIST OF PATHS, JOINED IN ORDER
      Object combined = null;
      for (final Object path : paths)
        combined = combine(combined, path);
      return combined;
    }
    return asPathValue(arg);
  }

  /**
   * The path {@code value} is: a real path when its elements are graph elements, the value itself when it is a path
   * of other elements (a node/relationship map, an alternating list). Anything else is not a path and is refused, so a
   * value that is not one never comes back out as a "combined path".
   */
  private static Object asPathValue(final Object value) {
    final TraversalPath path = asTraversalPath(value);
    if (path != null)
      return path;
    if (value instanceof Map<?, ?> map && map.get("nodes") instanceof List<?>)
      return value;
    if (value instanceof List<?> list && list.size() % 2 == 1)
      return value;
    throw new CommandExecutionException("path.combine() expects paths, got " + describe(value));
  }

  private static Object combine(final Object first, final Object second) {
    if (first == null)
      return second == null ? null : asPathValue(second);
    if (second == null)
      return asPathValue(first);
    final TraversalPath firstPath = asTraversalPath(asPathValue(first));
    final TraversalPath secondPath = asTraversalPath(asPathValue(second));
    if (firstPath != null && secondPath != null) {
      final Object end = firstPath.getEndVertex().getIdentity();
      final Object start = secondPath.getStartVertex().getIdentity();
      if (!Objects.equals(end, start))
        throw notConnected(end, start);
      return new TraversalPath(firstPath, secondPath);
    }
    return combineMaps(first, second);
  }

  /**
   * The path maps of elements that are not graph elements (a path built by hand): joined node list and relationship
   * list, under the same connection rule.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> combineMaps(final Object first, final Object second) {
    final List<Object> firstNodes = new ArrayList<>();
    final List<Object> firstRels = new ArrayList<>();
    final List<Object> secondNodes = new ArrayList<>();
    final List<Object> secondRels = new ArrayList<>();
    if (!split(first, firstNodes, firstRels) || !split(second, secondNodes, secondRels))
      throw new CommandExecutionException("path.combine() expects two paths, got " + describe(first) + " and " + describe(second));

    if (!firstNodes.isEmpty() && !secondNodes.isEmpty()) {
      final Object end = identityOf(firstNodes.get(firstNodes.size() - 1));
      final Object start = identityOf(secondNodes.get(0));
      if (!Objects.equals(end, start))
        throw notConnected(end, start);
      secondNodes.remove(0);
    }
    firstNodes.addAll(secondNodes);
    firstRels.addAll(secondRels);

    final Map<String, Object> result = new LinkedHashMap<>();
    result.put("_type", "path");
    result.put("nodes", firstNodes);
    result.put("relationships", firstRels);
    result.put("length", firstRels.size());
    return result;
  }

  @SuppressWarnings("unchecked")
  private static boolean split(final Object path, final List<Object> nodes, final List<Object> rels) {
    if (path instanceof TraversalPath traversal) {
      nodes.addAll(traversal.getVertices());
      rels.addAll(traversal.getEdges());
      return true;
    }
    if (path instanceof Map<?, ?> map && map.get("nodes") instanceof List<?> mapNodes) {
      nodes.addAll(mapNodes);
      if (map.get("relationships") instanceof List<?> mapRels)
        rels.addAll(mapRels);
      return true;
    }
    if (path instanceof List<?> elements) {
      // ALTERNATING NODE / RELATIONSHIP LIST
      for (int i = 0; i < elements.size(); i++)
        (i % 2 == 0 ? nodes : rels).add(elements.get(i));
      return true;
    }
    return false;
  }

  private static Object identityOf(final Object node) {
    return node instanceof Identifiable identifiable ? identifiable.getIdentity() : node;
  }

  private static CommandExecutionException notConnected(final Object end, final Object start) {
    return new CommandExecutionException(
        "path.combine(): the paths do not connect, the first ends at " + end + " and the second starts at " + start);
  }

  private static String describe(final Object value) {
    return value == null ? "null" : value.getClass().getSimpleName();
  }
}
