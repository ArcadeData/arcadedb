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
import com.arcadedb.index.vector.HnswVectorIndex;
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

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionVectorNeighbors() {
    super(NAME);
  }

  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    if (iParams == null || iParams.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Index index = iContext.getDatabase().getSchema().getIndexByName(iParams[0].toString());
    if (!(index instanceof HnswVectorIndex))
      throw new CommandSQLParsingException("Index '" + index.getName() + "' is not a vector index (found: " + index.getClass().getSimpleName() + ")");

    final HnswVectorIndex vIndex = (HnswVectorIndex) index;

    Object key = iParams[1];
    if (key == null)
      throw new CommandSQLParsingException("key is null");

    if( key instanceof List list)
      key = list.toArray();

    final int limit = iParams[2] instanceof Number n ? n.intValue() : Integer.parseInt(iParams[2].toString());

    final List<Pair<Vertex, ? extends Number>> neighbors = vIndex.findNeighborsFromVector(key, limit, null);

    final ArrayList<Object> result = new ArrayList<>(neighbors.size());
    for (Pair<Vertex, ? extends Number> n : neighbors)
      result.add(Map.of("vertex", n.getFirst(), "distance", n.getSecond()));
    return result;
  }

  public String getSyntax() {
    return "vectorNeighbors(<index-name>, <key>, <k>)";
  }
}
