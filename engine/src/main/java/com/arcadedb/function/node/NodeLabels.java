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
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * node.labels(node) - Get the labels of a node.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class NodeLabels extends AbstractNodeFunction {
  @Override
  protected String getSimpleName() {
    return "labels";
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
    return "Get the labels (types) of a node";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Vertex vertex = toVertex(args[0]);
    if (vertex == null)
      return Collections.emptyList();

    final DocumentType type = vertex.getType();
    final List<String> labels = new ArrayList<>();
    labels.add(type.getName());

    // Include parent types (supertypes)
    for (final DocumentType superType : type.getSuperTypes()) {
      labels.add(superType.getName());
    }

    return labels;
  }
}
