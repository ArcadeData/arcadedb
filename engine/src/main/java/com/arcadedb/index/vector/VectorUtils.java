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

import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.util.List;

import static io.github.jbellis.jvector.vector.VectorUtil.cosine;
import static io.github.jbellis.jvector.vector.VectorUtil.squareL2Distance;

/**
 * Centralized utility methods for vector operations.
 * All vector computation logic (magnitude, cosine similarity, distances, etc.)
 * is consolidated here to avoid duplication across SQL and OpenCypher functions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class VectorUtils {

  private VectorUtils() {
  }

  /**
   * Converts various object types to a float array.
   * Handles: float[], double[], Object[] (of Number), List (of Number).
   *
   * @param vectorObj the object to convert
   *
   * @return float array representation
   *
   * @throws IllegalArgumentException if the input type is not supported or contains non-numeric elements
   */
  public static float[] toFloatArray(final Object vectorObj) {
    if (vectorObj instanceof float[] f)
      return f;
    if (vectorObj instanceof double[] d) {
      final float[] result = new float[d.length];
      for (int i = 0; i < d.length; i++)
        result[i] = (float) d[i];
      return result;
    }
    if (vectorObj instanceof Object[] objArray) {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num)
          result[i] = num.floatValue();
        else
          throw new IllegalArgumentException("Vector elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
      }
      return result;
    }
    if (vectorObj instanceof List<?> list) {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num)
          result[i] = num.floatValue();
        else
          throw new IllegalArgumentException("Vector elements must be numbers, found: " + elem.getClass().getSimpleName());
      }
      return result;
    }
    throw new IllegalArgumentException("Vector must be an array or list, found: " + vectorObj.getClass().getSimpleName());
  }

  /**
   * Converts various object types to a float array.
   * Returns null on unsupported types instead of throwing an exception.
   *
   * @param vectorObj the object to convert (float[], List, etc.)
   *
   * @return float array representation, or null if the type is not supported
   *
   * @deprecated use {@link #toFloatArray(Object)} instead
   */
  @Deprecated
  public static float[] convertToFloatArray(final Object vectorObj) {
    if (vectorObj instanceof float[] f)
      return f;
    if (vectorObj instanceof List<?> list) {
      final float[] vector = new float[list.size()];
      for (int i = 0; i < list.size(); i++)
        vector[i] = ((Number) list.get(i)).floatValue();
      return vector;
    }
    return null;
  }

  /**
   * Calculates the magnitude (L2 norm) of a float vector.
   *
   * @param vector the vector
   *
   * @return the magnitude
   */
  public static float magnitude(final float[] vector) {
    double sum = 0.0;
    for (final float v : vector)
      sum += (double) v * v;
    return (float) Math.sqrt(sum);
  }

  /**
   * Calculates the magnitude (L2 norm) of a double vector.
   *
   * @param vector the vector
   *
   * @return the magnitude
   */
  public static double magnitude(final double[] vector) {
    double sum = 0.0;
    for (final double v : vector)
      sum += v * v;
    return Math.sqrt(sum);
  }

  /**
   * Normalizes a float vector to unit length.
   *
   * @param vector the vector to normalize
   *
   * @return a new unit vector in the same direction, or the original vector if magnitude is zero
   */
  public static float[] normalize(final float[] vector) {
    final float mag = magnitude(vector);
    if (mag == 0.0f)
      return vector;

    final float[] result = new float[vector.length];
    final float normFactor = 1.0f / mag;
    for (int i = 0; i < vector.length; i++)
      result[i] = vector[i] * normFactor;
    return result;
  }

  /**
   * Normalizes a double vector to unit length.
   *
   * @param vector the vector to normalize
   *
   * @return a new unit vector in the same direction, or the original vector if magnitude is zero
   */
  public static double[] normalize(final double[] vector) {
    final double mag = magnitude(vector);
    if (mag == 0.0)
      return vector;

    final double[] result = new double[vector.length];
    final double normFactor = 1.0 / mag;
    for (int i = 0; i < vector.length; i++)
      result[i] = vector[i] * normFactor;
    return result;
  }

  /**
   * Calculates cosine similarity between two vectors.
   * Uses JVector's SIMD-optimized implementation when available, with a scalar fallback.
   *
   * @param v1 first vector
   * @param v2 second vector
   *
   * @return cosine similarity value between -1 and 1
   */
  public static float cosineSimilarity(final float[] v1, final float[] v2) {
    try {
      final VectorizationProvider vp = VectorizationProvider.getInstance();
      final VectorFloat<?> jv1 = vp.getVectorTypeSupport().createFloatVector(v1);
      final VectorFloat<?> jv2 = vp.getVectorTypeSupport().createFloatVector(v2);
      return cosine(jv1, jv2);
    } catch (final Exception e) {
      // Fallback to scalar implementation
      double dotProduct = 0.0;
      double normA = 0.0;
      double normB = 0.0;
      for (int i = 0; i < v1.length; i++) {
        dotProduct += v1[i] * v2[i];
        normA += (double) v1[i] * v1[i];
        normB += (double) v2[i] * v2[i];
      }
      final double mag = Math.sqrt(normA) * Math.sqrt(normB);
      if (mag == 0.0)
        return 0.0f;
      return (float) (dotProduct / mag);
    }
  }

  /**
   * Calculates the dot product of two vectors.
   * Uses scalar implementation for typical vectors, JVector SIMD for large vectors (4096+).
   *
   * @param v1 first vector
   * @param v2 second vector
   *
   * @return the dot product
   */
  public static float dotProduct(final float[] v1, final float[] v2) {
    if (v1.length >= 4096) {
      try {
        final VectorizationProvider vp = VectorizationProvider.getInstance();
        final VectorFloat<?> jv1 = vp.getVectorTypeSupport().createFloatVector(v1);
        final VectorFloat<?> jv2 = vp.getVectorTypeSupport().createFloatVector(v2);
        return VectorUtil.dotProduct(jv1, jv2);
      } catch (final Exception e) {
        // Fallback to scalar
      }
    }
    double result = 0.0;
    for (int i = 0; i < v1.length; i++)
      result += v1[i] * v2[i];
    return (float) result;
  }

  /**
   * Calculates the Euclidean (L2) distance between two vectors.
   * Uses JVector's SIMD-optimized implementation when available, with a scalar fallback.
   *
   * @param v1 first vector
   * @param v2 second vector
   *
   * @return the Euclidean distance
   */
  public static float l2Distance(final float[] v1, final float[] v2) {
    try {
      final VectorizationProvider vp = VectorizationProvider.getInstance();
      final VectorFloat<?> jv1 = vp.getVectorTypeSupport().createFloatVector(v1);
      final VectorFloat<?> jv2 = vp.getVectorTypeSupport().createFloatVector(v2);
      final float squaredDistance = squareL2Distance(jv1, jv2);
      return (float) Math.sqrt(Math.max(0.0f, squaredDistance));
    } catch (final Exception e) {
      // Fallback to scalar implementation
      double sum = 0.0;
      for (int i = 0; i < v1.length; i++) {
        final double diff = v1[i] - v2[i];
        sum += diff * diff;
      }
      return (float) Math.sqrt(sum);
    }
  }

  /**
   * Calculates the Manhattan (L1) distance between two vectors.
   *
   * @param v1 first vector
   * @param v2 second vector
   *
   * @return the Manhattan distance
   */
  public static float manhattanDistance(final float[] v1, final float[] v2) {
    float distance = 0.0f;
    for (int i = 0; i < v1.length; i++)
      distance += Math.abs(v1[i] - v2[i]);
    return distance;
  }

  /**
   * Validates that two vectors have the same dimension.
   *
   * @param v1 first vector
   * @param v2 second vector
   *
   * @throws IllegalArgumentException if dimensions don't match
   */
  public static void validateSameDimension(final float[] v1, final float[] v2) {
    if (v1.length != v2.length)
      throw new IllegalArgumentException("Vectors must have the same dimension, found: " + v1.length + " and " + v2.length);
  }
}
