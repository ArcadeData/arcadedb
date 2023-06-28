/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.index.vector.distance;

import com.github.jelmerk.knn.DistanceFunction;
import com.github.jelmerk.knn.DistanceFunctions;

import java.util.*;

/**
 * Factory for distance functions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DistanceFunctionFactory {
  private static final Map<String, DistanceFunction> implementations = new HashMap<>();

  static {
    implementations.put("FloatCosineDistance", DistanceFunctions.FLOAT_COSINE_DISTANCE);
    implementations.put("FloatInnerProduct", DistanceFunctions.FLOAT_INNER_PRODUCT);
    implementations.put("FloatEuclideanDistance", DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE);
    implementations.put("FloatCanberraDistance", DistanceFunctions.FLOAT_CANBERRA_DISTANCE);
    implementations.put("FloatBrayCurtisDistance", DistanceFunctions.FLOAT_BRAY_CURTIS_DISTANCE);
    implementations.put("FloatCorrelationDistance", DistanceFunctions.FLOAT_CORRELATION_DISTANCE);
    implementations.put("FloatManhattanDistance", DistanceFunctions.FLOAT_MANHATTAN_DISTANCE);
    implementations.put("FloatChebyshevDistance", new ChebyshevDistance.FloatChebyshevDistance());
    implementations.put("DoubleCosineDistance", DistanceFunctions.DOUBLE_COSINE_DISTANCE);
    implementations.put("DoubleInnerProduct", DistanceFunctions.DOUBLE_INNER_PRODUCT);
    implementations.put("DoubleEuclideanDistance", DistanceFunctions.DOUBLE_EUCLIDEAN_DISTANCE);
    implementations.put("DoubleCanberraDistance", DistanceFunctions.DOUBLE_CANBERRA_DISTANCE);
    implementations.put("DoubleBrayCurtisDistance", DistanceFunctions.DOUBLE_BRAY_CURTIS_DISTANCE);
    implementations.put("DoubleCorrelationDistance", DistanceFunctions.DOUBLE_CORRELATION_DISTANCE);
    implementations.put("DoubleManhattanDistance", DistanceFunctions.DOUBLE_MANHATTAN_DISTANCE);
    implementations.put("DoubleChebyshevDistance", new ChebyshevDistance.DoubleChebyshevDistance());
    implementations.put("FloatSparseVectorInnerProduct", DistanceFunctions.FLOAT_SPARSE_VECTOR_INNER_PRODUCT);
    implementations.put("DoubleSparseVectorInnerProduct", DistanceFunctions.DOUBLE_SPARSE_VECTOR_INNER_PRODUCT);
  }

  public static DistanceFunction getImplementation(final String name) {
    return implementations.get(name);
  }
}
