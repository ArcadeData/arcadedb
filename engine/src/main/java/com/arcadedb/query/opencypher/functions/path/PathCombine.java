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
package com.arcadedb.query.opencypher.functions.path;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * path.combine(paths) - Combine multiple paths into a single path.
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
    return 1;
  }

  @Override
  public String getDescription() {
    return "Combine multiple paths into a single path";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final List<Object> paths = toList(args[0]);
    if (paths.isEmpty())
      return null;

    final List<Object> allNodes = new ArrayList<>();
    final List<Object> allRels = new ArrayList<>();

    for (final Object pathObj : paths) {
      if (pathObj instanceof Map) {
        final Map<String, Object> pathMap = (Map<String, Object>) pathObj;
        final List<Object> nodes = (List<Object>) pathMap.get("nodes");
        final List<Object> rels = (List<Object>) pathMap.get("relationships");

        if (nodes != null) {
          // Skip first node if we already have nodes (to avoid duplicate)
          if (allNodes.isEmpty()) {
            allNodes.addAll(nodes);
          } else if (nodes.size() > 1) {
            allNodes.addAll(nodes.subList(1, nodes.size()));
          }
        }
        if (rels != null) {
          allRels.addAll(rels);
        }
      } else if (pathObj instanceof List) {
        // Assume alternating node/rel list
        final List<?> elements = (List<?>) pathObj;
        for (int i = 0; i < elements.size(); i++) {
          if (i % 2 == 0) {
            if (allNodes.isEmpty() || i > 0) {
              allNodes.add(elements.get(i));
            }
          } else {
            allRels.add(elements.get(i));
          }
        }
      }
    }

    final Map<String, Object> result = new HashMap<>();
    result.put("_type", "path");
    result.put("nodes", allNodes);
    result.put("relationships", allRels);
    result.put("length", allRels.size());

    return result;
  }

  @SuppressWarnings("unchecked")
  private List<Object> toList(final Object input) {
    if (input instanceof List)
      return (List<Object>) input;

    if (input instanceof Collection)
      return new ArrayList<>((Collection<?>) input);

    final List<Object> result = new ArrayList<>();
    result.add(input);
    return result;
  }
}
