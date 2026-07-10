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
package com.arcadedb.function.convert;

import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;
import java.util.Map;

/**
 * toString() function - converts a value to a string.
 */
public class ToStringFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "toString";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("toString() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String || args[0] instanceof Number || args[0] instanceof Boolean)
      return args[0].toString();
    // Temporal types
    if (args[0] instanceof CypherTemporalValue)
      return args[0].toString();
    // Unsupported types: List, Map, Node, Relationship, Path. This is a client-side type error (the
    // query passes the wrong value type to a scalar function), classified by the openCypher TCK as a
    // runtime TypeError/InvalidArgumentValue. Throw a CommandSemanticException so the transport layer
    // reports it as a 400 client error with the descriptive message, not a 500 transaction-commit
    // failure. See issue #5203.
    if (args[0] instanceof List || args[0] instanceof Map
        || args[0] instanceof Vertex || args[0] instanceof Edge
        || args[0] instanceof Document || args[0] instanceof TraversalPath)
      throw new CommandSemanticException("TypeError: InvalidArgumentValue - toString() cannot convert " + args[0].getClass().getSimpleName());
    return args[0].toString();
  }
}
