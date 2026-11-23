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
 * node.relationship.exists(node, [relTypes], [direction]) - Check if a node has relationships.
 * <p>
 * {@code relTypes} is APOC's relationship pattern ({@code 'KNOWS>|<LIKES'}, {@code '>'}, a blank one for every relationship,
 * issue #12103). {@code direction}, which APOC does not have, narrows the pattern to that direction: an alternative without a
 * marker is followed in it, and a marked one that points the other way ({@code 'KNOWS>'} with {@code 'IN'}) names no
 * relationship. The answer is the first alternative that has a relationship, the ones behind it are not counted.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class NodeRelationshipExists extends AbstractNodeFunction {
  @Override
  protected String getSimpleName() {
    return "relationship.exists";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Check if a node has any relationships, optionally filtered by type and direction";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Vertex vertex = toVertex(args[0]);
    if (vertex == null)
      return false;
    return hasEdges(context, vertex, patternArgument(args, 1), directionArgument(args, 2));
  }
}
