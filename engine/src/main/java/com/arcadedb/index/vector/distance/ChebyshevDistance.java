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

/**
 * Implementation of {@link DistanceFunction} that calculates the chebyshev distance.
 *
 * @author @gramian
 */
public class ChebyshevDistance {
  public static class FloatChebyshevDistance implements DistanceFunction<float[], Float> {
    /**
     * Calculates the chebyshev distance.
     *
     * @param u Left vector.
     * @param v Right vector.
     *
     * @return Chebyshev distance between u and v.
     */
    @Override
    public Float distance(final float[] u, final float[] v) {
      float max = 0.0F;
      for (int i = 0; i < u.length; i++) {
        max = Math.max(max, Math.abs(u[i] - v[i]));
      }
      return max;
    }
  }

  public static class DoubleChebyshevDistance implements DistanceFunction<double[], Double> {
    /**
     * Calculates the chebyshev distance.
     *
     * @param u Left vector.
     * @param v Right vector.
     *
     * @return Chebyshev distance between u and v.
     */
    @Override
    public Double distance(final double[] u, final double[] v) {
      double max = 0.0D;
      for (int i = 0; i < u.length; i++) {
        max = Math.max(max, Math.abs(u[i] - v[i]));
      }
      return max;
    }
  }
}
