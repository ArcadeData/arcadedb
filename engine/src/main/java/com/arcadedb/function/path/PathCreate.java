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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * path.create(start, relationships) - Create a path from a start node and a list of relationships.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathCreate extends AbstractPathFunction {
  @Override
  protected String getSimpleName() {
    return "create";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Create a path from a start node and a list of relationships";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final Object startNode = args[0];
    final List<Object> relationships = toList(args[1]);

    // Create path representation
    final Map<String, Object> path = new HashMap<>();
    path.put("_type", "path");

    final List<Object> nodes = new ArrayList<>();
    final List<Object> rels = new ArrayList<>();

    nodes.add(startNode);
    if (relationships != null) {
      rels.addAll(relationships);
    }

    path.put("nodes", nodes);
    path.put("relationships", rels);
    path.put("length", rels.size());

    return path;
  }

  @SuppressWarnings("unchecked")
  private List<Object> toList(final Object input) {
    if (input == null)
      return new ArrayList<>();

    if (input instanceof List)
      return (List<Object>) input;

    if (input instanceof Collection)
      return new ArrayList<>((Collection<?>) input);

    final List<Object> result = new ArrayList<>();
    result.add(input);
    return result;
  }
}
