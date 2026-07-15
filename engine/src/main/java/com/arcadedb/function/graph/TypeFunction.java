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
import com.arcadedb.exception.CommandSemanticException;
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
    // Type validation: type() only works on relationships. This is a client-side type error (the query
    // passes a node/scalar where a relationship is expected), classified by the openCypher TCK as a
    // runtime TypeError/InvalidArgumentValue. Throw a CommandSemanticException so the transport layer
    // reports it as a 400 client error with the descriptive message, not a 500 transaction-commit
    // failure. See issue #5204.
    // The valueType() hint is deliberate: type() is regularly mistaken for a value-type introspection
    // function, which is what valueType() does (issue #5292).
    throw new CommandSemanticException(
        "TypeError: type() requires a relationship argument, got " + args[0].getClass().getSimpleName()
            + ". Use valueType() to inspect the type of a value");
  }
}
