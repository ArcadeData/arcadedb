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

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * path.slice(path, from, [to]) - Extract a subpath from a path.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathSlice extends AbstractPathFunction {
  @Override
  protected String getSimpleName() {
    return "slice";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Extract a subpath from index 'from' to 'to' (based on relationship count)";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final int from = args[1] != null ? ((Number) args[1]).intValue() : 0;

    List<Object> nodes;
    List<Object> rels;

    if (args[0] instanceof Map) {
      final Map<String, Object> pathMap = (Map<String, Object>) args[0];
      nodes = (List<Object>) pathMap.get("nodes");
      rels = (List<Object>) pathMap.get("relationships");
    } else if (args[0] instanceof List) {
      // Assume alternating node/rel list
      nodes = new ArrayList<>();
      rels = new ArrayList<>();
      final List<?> elements = (List<?>) args[0];
      for (int i = 0; i < elements.size(); i++) {
        if (i % 2 == 0) {
          nodes.add(elements.get(i));
        } else {
          rels.add(elements.get(i));
        }
      }
    } else {
      return null;
    }

    if (nodes == null || rels == null)
      return null;

    final int relCount = rels.size();
    final int to = args.length > 2 && args[2] != null ? ((Number) args[2]).intValue() : relCount;

    if (from >= relCount || from >= to)
      return null;

    final int actualFrom = Math.max(0, from);
    final int actualTo = Math.min(relCount, to);

    final List<Object> slicedNodes = new ArrayList<>();
    final List<Object> slicedRels = new ArrayList<>();

    // Nodes are indexed: node[i] -rel[i]-> node[i+1]
    for (int i = actualFrom; i <= actualTo && i < nodes.size(); i++) {
      slicedNodes.add(nodes.get(i));
    }
    for (int i = actualFrom; i < actualTo && i < rels.size(); i++) {
      slicedRels.add(rels.get(i));
    }

    final Map<String, Object> result = new HashMap<>();
    result.put("_type", "path");
    result.put("nodes", slicedNodes);
    result.put("relationships", slicedRels);
    result.put("length", slicedRels.size());

    return result;
  }
}
