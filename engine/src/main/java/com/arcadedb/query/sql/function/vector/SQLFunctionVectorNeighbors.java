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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.HnswVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Returns the K neighbors from a vertex. This function requires a vector index has been created beforehand.
 * Supports both HnswVectorIndex and LSMVectorIndex.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorNeighbors extends SQLFunctionAbstract {
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

    // Handle HnswVectorIndex (legacy)
    if (index instanceof HnswVectorIndex vIndex) {
      final List<Pair<Vertex, ? extends Number>> neighbors = vIndex.findNeighborsFromId(key, limit, null);
      final ArrayList<Object> result = new ArrayList<>(neighbors.size());
      for (Pair<Vertex, ? extends Number> n : neighbors)
        result.add(Map.of("vertex", n.getFirst(), "distance", n.getSecond()));
      return result;
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

      // Query for the vertex by the ID property (usually "name")
      final ResultSet rs = context.getDatabase().query("sql",
          "SELECT " + vectorProperty + " FROM " + typeName + " WHERE name = ? LIMIT 1", keyStr);

      if (rs.hasNext()) {
        final var result = rs.next();
        queryVector = result.getProperty(vectorProperty);
      }
      rs.close();

      if (queryVector == null) {
        throw new CommandSQLParsingException("Could not find vertex with key '" + keyStr + "' or extract vector");
      }
    }

    // Perform vector search
    final IndexCursor cursor = lsmIndex.get(new Object[] { queryVector }, limit);
    final ArrayList<Object> result = new ArrayList<>();

    while (cursor.hasNext()) {
      final Identifiable id = cursor.next();
      if (id != null) {
        final RID rid = id.getIdentity();
        final Vertex vertex = rid.asVertex();

        // Calculate distance (we don't have it from the cursor, so we'll compute it)
        final Object vectorObj = vertex.get(lsmIndex.getPropertyNames().get(0));
        if (vectorObj instanceof float[] vertexVector) {
          final float distance = calculateDistance(queryVector, vertexVector, lsmIndex.getSimilarityFunction().name());
          result.add(Map.of("vertex", vertex, "distance", distance));
        }
      }
    }

    return result;
  }

  private float calculateDistance(final float[] v1, final float[] v2, final String similarityFunction) {
    switch (similarityFunction) {
    case "COSINE":
      return 1.0f - cosineSimilarity(v1, v2);
    case "EUCLIDEAN":
      return euclideanDistance(v1, v2);
    case "DOT_PRODUCT":
      return -dotProduct(v1, v2); // Negative because higher dot product = closer
    default:
      return euclideanDistance(v1, v2);
    }
  }

  private float cosineSimilarity(final float[] v1, final float[] v2) {
    float dot = 0.0f, norm1 = 0.0f, norm2 = 0.0f;
    for (int i = 0; i < v1.length; i++) {
      dot += v1[i] * v2[i];
      norm1 += v1[i] * v1[i];
      norm2 += v2[i] * v2[i];
    }
    return (float) (dot / (Math.sqrt(norm1) * Math.sqrt(norm2)));
  }

  private float euclideanDistance(final float[] v1, final float[] v2) {
    float sum = 0.0f;
    for (int i = 0; i < v1.length; i++) {
      final float diff = v1[i] - v2[i];
      sum += diff * diff;
    }
    return (float) Math.sqrt(sum);
  }

  private float dotProduct(final float[] v1, final float[] v2) {
    float sum = 0.0f;
    for (int i = 0; i < v1.length; i++) {
      sum += v1[i] * v2[i];
    }
    return sum;
  }

  public String getSyntax() {
    return "vectorNeighbors(<index-name>, <key-or-vector>, <k>)";
  }
}
