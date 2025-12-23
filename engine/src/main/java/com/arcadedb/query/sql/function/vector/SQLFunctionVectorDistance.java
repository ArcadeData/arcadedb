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
import com.arcadedb.index.vector.distance.DistanceFunctionFactory;
import com.arcadedb.query.sql.executor.CommandContext;
import com.github.jelmerk.knn.DistanceFunction;

/**
 * Calculates the distance between two vectors using the specified distance algorithm.
 * Supports various distance metrics: COSINE, EUCLIDEAN, MANHATTAN, CHEBYSHEV, CANBERRA, BRAY_CURTIS, CORRELATION, INNER_PRODUCT.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorDistance extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorDistance";

  public SQLFunctionVectorDistance() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector1 = params[0];
    final Object vector2 = params[1];
    final String algorithm = params[2].toString();

    if (vector1 == null || vector2 == null)
      throw new CommandSQLParsingException("Vectors cannot be null");

    // Resolve to float[] arrays
    final float[] v1 = toFloatArray(vector1);
    final float[] v2 = toFloatArray(vector2);

    if (v1.length != v2.length)
      throw new CommandSQLParsingException("Vectors must have the same dimension");

    // Get the distance function
    final DistanceFunction distanceFunction = resolveDistanceFunction(algorithm);
    if (distanceFunction == null)
      throw new CommandSQLParsingException("Unknown distance algorithm: " + algorithm);

    // Calculate and return distance
    return distanceFunction.distance(v1, v2);
  }


  private DistanceFunction resolveDistanceFunction(final String algorithm) {
    // Try exact name first
    DistanceFunction func = DistanceFunctionFactory.getImplementationByName(algorithm);
    if (func != null)
      return func;

    // Map common names to implementation names
    final String normalized = algorithm.toUpperCase();
    final String implName = switch (normalized) {
      case "COSINE" -> "FloatCosine";
      case "EUCLIDEAN" -> "FloatEuclidean";
      case "MANHATTAN" -> "FloatManhattan";
      case "CHEBYSHEV" -> "FloatChebyshev";
      case "CANBERRA" -> "FloatCanberra";
      case "BRAY_CURTIS", "BRAY-CURTIS" -> "FloatBrayCurtis";
      case "CORRELATION" -> "FloatCorrelation";
      case "INNER_PRODUCT", "INNER-PRODUCT" -> "FloatInnerProduct";
      default -> null;
    };

    if (implName != null)
      return DistanceFunctionFactory.getImplementationByName(implName);

    return null;
  }

  public String getSyntax() {
    return NAME + "(<vector1>, <vector2>, <algorithm>)";
  }
}
