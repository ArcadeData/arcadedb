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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;
import java.util.Map;

/**
 * valueType() function - returns a string representation of the type of a value.
 * Returns Cypher type names: INTEGER, FLOAT, STRING, BOOLEAN, NULL, LIST, MAP, NODE, RELATIONSHIP, PATH, etc.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ValueTypeFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "valueType";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("valueType() requires exactly one argument");
    final Object value = args[0];
    return switch (value) {
      case null -> "NULL";
      case Long l -> "INTEGER NOT NULL";
      case Integer i -> "INTEGER NOT NULL";
      case Short s -> "INTEGER NOT NULL";
      case Byte b -> "INTEGER NOT NULL";
      case Double d -> "FLOAT NOT NULL";
      case Float f -> "FLOAT NOT NULL";
      case String s -> "STRING NOT NULL";
      case Boolean b -> "BOOLEAN NOT NULL";
      case Vertex vertex -> "NODE NOT NULL";
      case Edge edge -> "RELATIONSHIP NOT NULL";
      case List list -> "LIST<ANY> NOT NULL";
      case Map map -> "MAP NOT NULL";
      default -> value.getClass().getSimpleName().toUpperCase() + " NOT NULL";
    };
  }
}
