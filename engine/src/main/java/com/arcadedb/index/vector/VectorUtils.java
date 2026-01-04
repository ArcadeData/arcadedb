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
package com.arcadedb.index.vector;

/**
 * This work is derived from the excellent work made by Jelmer Kuperus on https://github.com/jelmerk/hnswlib.
 * <p>
 * Misc utility methods for dealing with vectors.
 */
public final class VectorUtils {

  private VectorUtils() {
  }

  /**
   * Calculates the magnitude of the vector.
   *
   * @param vector The vector to calculate magnitude for.
   *
   * @return The magnitude.
   */
  public static double magnitude(final double[] vector) {
    double magnitude = 0.0f;
    for (double aDouble : vector) {
      magnitude += aDouble * aDouble;
    }
    return Math.sqrt(magnitude);
  }

  /**
   * Turns vector to unit vector.
   *
   * @param vector The vector to normalize.
   *
   * @return the input vector as a unit vector
   */
  public static double[] normalize(final double[] vector) {

    final double[] result = new double[vector.length];

    final double normFactor = 1 / magnitude(vector);
    for (int i = 0; i < vector.length; i++) {
      result[i] = vector[i] * normFactor;
    }
    return result;
  }

  /**
   * Calculates the magnitude of the vector.
   *
   * @param vector The vector to calculate magnitude for.
   *
   * @return The magnitude.
   */
  public static float magnitude(final float[] vector) {
    float magnitude = 0.0f;
    for (float aFloat : vector) {
      magnitude += aFloat * aFloat;
    }
    return (float) Math.sqrt(magnitude);
  }

  /**
   * Turns vector to unit vector.
   *
   * @param vector The vector to normalize.
   *
   * @return the input vector as a unit vector
   */
  public static float[] normalize(final float[] vector) {
    final float[] result = new float[vector.length];

    final float normFactor = 1 / magnitude(vector);
    for (int i = 0; i < vector.length; i++) {
      result[i] = vector[i] * normFactor;
    }
    return result;
  }

  public static float[] convertToFloatArray(final Object vectorObj) {
    if (vectorObj instanceof float[] f)
      return f;
    else if (vectorObj instanceof java.util.List<?> list) {
      final float[] vector = new float[list.size()];
      for (int i = 0; i < list.size(); i++)
        vector[i] = ((Number) list.get(i)).floatValue();
      return vector;
    }
    return null;
  }
}
