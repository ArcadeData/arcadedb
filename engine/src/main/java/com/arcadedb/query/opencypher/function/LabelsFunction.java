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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * labels() function - returns the labels of a node.
 * <p>
 * For vertices with multiple labels (composite types), returns all labels
 * sorted alphabetically. For single-label vertices, returns a list with
 * the type name.
 */
public class LabelsFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "labels";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1) {
      throw new CommandExecutionException("labels() requires exactly one argument");
    }
    if (args[0] == null)
      return null;
    DeletedEntityMarker.checkNotDeleted(args[0]);
    if (args[0] instanceof Vertex vertex) {
      // Use the Labels helper class which handles composite types
      return Labels.getLabels(vertex);
    }
    if (args[0] instanceof Result result && result.getElement().isPresent() && result.getElement().get() instanceof Vertex v)
      return Labels.getLabels(v);
    throw new CommandExecutionException("InvalidArgumentValue: labels() requires a node argument, got: " +
        args[0].getClass().getSimpleName());
  }
}
