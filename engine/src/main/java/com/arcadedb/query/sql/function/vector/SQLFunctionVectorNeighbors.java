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
package com.arcadedb.query.sql.function.vector;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.vector.JVectorIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Returns the K neighbors from a vertex. This function requires a vector index has been created beforehand.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorNeighbors extends SQLFunctionAbstract {
  public static final String NAME = "vectorNeighbors";

  /**\
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionVectorNeighbors() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Index index = context.getDatabase().getSchema().getIndexByName(params[0].toString());
    if (index == null)
      throw new CommandSQLParsingException("Index '" + params[0].toString() + "' not found");

    if (!(index instanceof JVectorIndex))
      throw new CommandSQLParsingException("Index '" + index.getName() + "' is not a vector index (found: " + index.getClass().getSimpleName() + ")");

    final JVectorIndex vIndex = (JVectorIndex) index;

    Object key = params[1];
    if (key == null)
      throw new CommandSQLParsingException("key is null");

    // Convert key to float array for JVector search
    float[] queryVector;
    switch (key) {
    case List<?> list -> {
      queryVector = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        Object value = list.get(i);
        if (value instanceof Number number) {
          queryVector[i] = number.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector element must be numeric, found: " + value.getClass().getSimpleName());
        }
      }
    }
    case Object[] array -> {
      queryVector = new float[array.length];
      for (int i = 0; i < array.length; i++) {
        if (array[i] instanceof Number number) {
          queryVector[i] = number.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector element must be numeric, found: " + array[i].getClass().getSimpleName());
        }
      }
    }
    case float[] floatArray -> queryVector = floatArray;
    default -> throw new CommandSQLParsingException(
        "Key must be a vector (List, array, or float[]), found: " + key.getClass().getSimpleName());
    }

    final int limit = params[2] instanceof Number n ? n.intValue() : Integer.parseInt(params[2].toString());

    final List<Pair<Identifiable, Float>> neighbors = vIndex.findNeighbors(queryVector, limit);

    final ArrayList<Object> result = new ArrayList<>(neighbors.size());
    for (Pair<Identifiable, Float> n : neighbors)
      result.add(Map.of("vertex", n.getFirst(), "distance", n.getSecond()));
    return result;
  }

  public String getSyntax() {
    return "vectorNeighbors(<index-name>, <key>, <k>)";
  }
}
