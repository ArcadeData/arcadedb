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
package com.arcadedb.function.node;

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * node.degree.in(node, [relTypes]) - Get the in-degree of a node.
 * <p>
 * {@code relTypes} is APOC's relationship pattern, read as {@link NodeDegree} reads it with the direction fixed to
 * {@code IN} (issue #12103): a blank one counts every incoming relationship, {@code 'KNOWS|LIKES'} sums the incoming
 * relationships of each alternative, and an alternative whose marker agrees with the direction ({@code '<KNOWS'}) counts like
 * a bare {@code 'KNOWS'}. An alternative marked the other way ({@code 'KNOWS>'}) names no incoming relationship and counts 0,
 * as it does in {@code node.degree(n, 'KNOWS>', 'IN')}. APOC itself reads the argument as one type name, so a plain type name
 * answers the same in both.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class NodeDegreeIn extends AbstractNodeFunction {
  @Override
  protected String getSimpleName() {
    return "degree.in";
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
    return "Get the in-degree (number of incoming relationships) of a node";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Vertex vertex = toVertex(args[0]);
    if (vertex == null)
      return 0L;

    // The incoming side of a unidirectional edge type is counted from the edges that end in the node (issue #8629)
    return countEdges(context, vertex, patternArgument(args, 1), Vertex.DIRECTION.IN);
  }
}
