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
package com.arcadedb.query.opencypher.functions.rel;

import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * rel.type(relationship) - Get the type of a relationship.
 *
 * @author ArcadeDB Team
 */
public class RelType extends AbstractRelFunction {
  @Override
  protected String getSimpleName() {
    return "type";
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
    return "Get the type name of a relationship";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Edge edge = toEdge(args[0]);
    if (edge == null)
      return null;

    return edge.getTypeName();
  }
}
