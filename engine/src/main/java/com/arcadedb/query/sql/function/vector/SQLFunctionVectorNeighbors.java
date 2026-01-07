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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Returns the K neighbors from a vertex. This function requires a vector index has been created beforehand.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorNeighbors extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorNeighbors";

  public SQLFunctionVectorNeighbors() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Index index = context.getDatabase().getSchema().getIndexByName(params[0].toString());

    Object key = params[1];
    if (key == null)
      throw new CommandSQLParsingException("key is null");

    if (key instanceof List<?> list)
      key = list.toArray();

    final int limit = params[2] instanceof Number n ? n.intValue() : Integer.parseInt(params[2].toString());

    // Handle LSMVectorIndex
    if (index instanceof TypeIndex typeIndex) {
      final var bucketIndexes = typeIndex.getIndexesOnBuckets();
      if (bucketIndexes != null && bucketIndexes.length > 0 && bucketIndexes[0] instanceof LSMVectorIndex lsmIndex) {
        return executeWithLSMVector(lsmIndex, key, limit, context);
      }
    }

    throw new CommandSQLParsingException(
        "Index '" + index.getName() + "' is not a vector index (found: " + index.getClass().getSimpleName() + ")");
  }

  private Object executeWithLSMVector(final LSMVectorIndex lsmIndex, final Object key, final int limit,
      final CommandContext context) {
    // Get the vector from the vertex identified by key
    float[] queryVector = null;

    // If key is already a vector, use it directly
    if (key instanceof float[] floatArray) {
      queryVector = floatArray;
    } else if (key instanceof Object[] objArray && objArray.length > 0 && objArray[0] instanceof Number) {
      // Convert array of numbers to float array
      queryVector = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        queryVector[i] = ((Number) objArray[i]).floatValue();
      }
    } else {
      // Key is a vertex identifier - fetch the vertex and get its vector
      final String keyStr = key.toString();
      final String typeName = lsmIndex.getTypeName();
      final String vectorProperty = lsmIndex.getPropertyNames().getFirst();
      final String idProperty = lsmIndex.getIdPropertyName();

      // Query for the vertex by the configured ID property
      final ResultSet rs = context.getDatabase().query("sql",
          "SELECT " + vectorProperty + " FROM " + typeName + " WHERE " + idProperty + " = ? LIMIT 1", keyStr);

      if (rs.hasNext()) {
        final var result = rs.next();
        queryVector = result.getProperty(vectorProperty);
      }
      rs.close();

      if (queryVector == null) {
        throw new CommandSQLParsingException("Could not find vertex with key '" + keyStr + "' or extract vector");
      }
    }

    // Perform vector search using the new method that returns scores directly
    final List<Pair<RID, Float>> neighbors = lsmIndex.findNeighborsFromVector(queryVector, limit);
    final ArrayList<Object> result = new ArrayList<>(neighbors.size());

    for (final Pair<RID, Float> neighbor : neighbors) {
      final RID rid = neighbor.getFirst();
      final Vertex vertex = rid.asVertex();
      final float distance = neighbor.getSecond();
      result.add(Map.of("vertex", vertex, "distance", distance));
    }

    return result;
  }

  public String getSyntax() {
    return "vectorNeighbors(<index-name>, <key-or-vector>, <k>)";
  }
}
