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
import com.arcadedb.graph.Edge;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * type() function - returns the type of a relationship.
 */
public class TypeFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "type";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1) {
      throw new CommandExecutionException("type() requires exactly one argument");
    }
    if (args[0] == null)
      return null;
    if (args[0] instanceof Edge)
      return ((Edge) args[0]).getTypeName();
    if (args[0] instanceof DeletedEntityMarker) {
      final String relType = ((DeletedEntityMarker) args[0]).getRelationshipType();
      if (relType != null)
        return relType;
      DeletedEntityMarker.checkNotDeleted(args[0]);
    }
    // Type validation: type() only works on relationships
    throw new CommandExecutionException(
        "TypeError: type() requires a relationship argument, got " + args[0].getClass().getSimpleName());
  }
}
