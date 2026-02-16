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
package com.arcadedb.function.graph;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.*;

/**
 * nodes() function - returns all nodes in a path.
 */
public class NodesFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "nodes";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("nodes() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof TraversalPath)
      return new ArrayList<>(((TraversalPath) args[0]).getVertices());
    if (args[0] instanceof List) {
      final List<?> path = (List<?>) args[0];
      final List<Vertex> nodes = new ArrayList<>();
      for (final Object element : path)
        if (element instanceof Vertex)
          nodes.add((Vertex) element);
      return nodes;
    }
    return Collections.emptyList();
  }
}
