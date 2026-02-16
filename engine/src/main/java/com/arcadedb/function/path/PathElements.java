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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * path.elements(path) - Get all elements (nodes and relationships) from a path.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathElements extends AbstractPathFunction {
  @Override
  protected String getSimpleName() {
    return "elements";
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
    return "Get all elements (alternating nodes and relationships) from a path";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return Collections.emptyList();

    List<Object> nodes;
    List<Object> rels;

    if (args[0] instanceof Map) {
      final Map<String, Object> pathMap = (Map<String, Object>) args[0];
      nodes = (List<Object>) pathMap.get("nodes");
      rels = (List<Object>) pathMap.get("relationships");
    } else if (args[0] instanceof List) {
      // Already a list of elements
      return args[0];
    } else {
      return Collections.emptyList();
    }

    if (nodes == null)
      return Collections.emptyList();

    final List<Object> elements = new ArrayList<>();

    // Interleave nodes and relationships
    for (int i = 0; i < nodes.size(); i++) {
      elements.add(nodes.get(i));
      if (rels != null && i < rels.size()) {
        elements.add(rels.get(i));
      }
    }

    return elements;
  }
}
