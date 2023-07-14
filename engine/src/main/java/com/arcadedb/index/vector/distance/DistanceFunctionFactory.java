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
  private static final Map<String, DistanceFunction> implementationsByName      = new HashMap<>();
  private static final Map<String, DistanceFunction> implementationsByClassName = new HashMap<>();

  static {
    registerImplementation("FloatCosine", DistanceFunctions.FLOAT_COSINE_DISTANCE);
    registerImplementation("FloatInnerProduct", DistanceFunctions.FLOAT_INNER_PRODUCT); // EXPECTED NORMALIZATION OF VECTORS
    registerImplementation("FloatEuclidean", DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE);
    registerImplementation("FloatCanberra", DistanceFunctions.FLOAT_CANBERRA_DISTANCE);
    registerImplementation("FloatBrayCurtis", DistanceFunctions.FLOAT_BRAY_CURTIS_DISTANCE);
    registerImplementation("FloatCorrelation", DistanceFunctions.FLOAT_CORRELATION_DISTANCE);
    registerImplementation("FloatManhattan", DistanceFunctions.FLOAT_MANHATTAN_DISTANCE);
    registerImplementation("FloatChebyshev", new ChebyshevDistance.FloatChebyshevDistance());
    registerImplementation("DoubleCosine", DistanceFunctions.DOUBLE_COSINE_DISTANCE);
    registerImplementation("DoubleInnerProduct", DistanceFunctions.DOUBLE_INNER_PRODUCT); // EXPECTED NORMALIZATION OF VECTORS
    registerImplementation("DoubleEuclidean", DistanceFunctions.DOUBLE_EUCLIDEAN_DISTANCE);
    registerImplementation("DoubleCanberra", DistanceFunctions.DOUBLE_CANBERRA_DISTANCE);
    registerImplementation("DoubleBrayCurtis", DistanceFunctions.DOUBLE_BRAY_CURTIS_DISTANCE);
    registerImplementation("DoubleCorrelation", DistanceFunctions.DOUBLE_CORRELATION_DISTANCE);
    registerImplementation("DoubleManhattan", DistanceFunctions.DOUBLE_MANHATTAN_DISTANCE);
    registerImplementation("DoubleChebyshev", new ChebyshevDistance.DoubleChebyshevDistance());
  }

  public static DistanceFunction getImplementationByName(final String name) {
    return implementationsByName.get(name.toLowerCase());
  }

  public static DistanceFunction getImplementationByClassName(final String name) {
    return implementationsByClassName.get(name);
  }

  public static void registerImplementation(final String name, final DistanceFunction impl) {
    implementationsByName.put(name.toLowerCase(), impl);
    implementationsByClassName.put(impl.getClass().getSimpleName(), impl);
  }
}
