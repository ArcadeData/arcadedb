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

import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

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

  /**
   * One-time WARNING gate for {@link #dequantizeInt8ToFloat(byte[])} on byte {@code -128} inputs.
   * Cohere/OpenAI int8 endpoints emit {@code [-127, 127]} only; encountering a {@code -128}
   * indicates a non-Cohere/OpenAI byte source where the silent clamp to {@code -127} (preserving
   * unit-norm for COSINE) is a meaningful asymmetric correction. Logged at most once per process.
   */
  private static final AtomicBoolean DEQUANTIZE_MIN128_WARNED = new AtomicBoolean(false);

  private VectorUtils() {
  }

  /**
   * Checks if a float vector is all zeros. Used to filter invalid vectors that would
   * cause NaN in cosine similarity computation.
   */
  public static boolean isZeroVector(final float[] vector) {
    for (final float v : vector)
      if (v != 0.0f)
        return false;
    return true;
  }

  /**
   * Dequantizes a signed int8 byte array into float values using the Cohere/OpenAI calibration
   * convention: {@code value / 127.0f}. Used on the read path when an HNSW index is built over a
   * {@link VectorEncoding#INT8}-encoded property; JVector 4.0.0-rc.8 still requires
   * {@code float32} for HNSW build/search internally (see
   * <a href="https://github.com/datastax/jvector/issues/665">datastax/jvector#665</a>). The
   * conversion is lossless within the source's int8 resolution.
   * <p>
   * Java's {@code byte} range is {@code [-128, 127]}, but the Cohere/OpenAI calibration only emits
   * {@code [-127, 127]}: a raw {@code -128} would dequantize to {@code -1.0079f} and break the
   * unit-norm assumption COSINE similarity relies on. The implementation therefore clamps
   * {@code -128} up to {@code -127} so a third-party caller passing a non-Cohere/OpenAI byte source
   * still produces well-behaved scores.
   *
   * @param int8 the signed byte vector (one byte per dimension)
   *
   * @return a float vector of the same length where each element equals
   *         {@code max(int8[i], -127) / 127.0f}
   */
  public static float[] dequantizeInt8ToFloat(final byte[] int8) {
    final float[] result = new float[int8.length];
    boolean sawMin128 = false;
    for (int i = 0; i < int8.length; i++) {
      // Java promotes byte to int for comparison and arithmetic, so the literal -127 / 127 are
      // compared against the sign-extended byte value (-128..127). The explicit (int) cast makes
      // the intent obvious: clamp the [-128] edge case up to -127 before the divide.
      final int b = (int) int8[i];
      if (b < -127)
        sawMin128 = true;
      result[i] = (b < -127 ? -127 : b) / 127.0f;
    }
    if (sawMin128 && DEQUANTIZE_MIN128_WARNED.compareAndSet(false, true))
      LogManager.instance().log(VectorUtils.class, Level.WARNING,
          "INT8 dequantization encountered byte value -128 and clamped it to -127 "
              + "(Cohere/OpenAI calibration emits [-127, 127] only). Subsequent occurrences will not be logged. "
              + "Verify the byte source if precise [-128, 127] handling is required.");
    return result;
  }

  /**
   * Converts various object types to a float array.
   * Handles: float[], double[], Object[] (of Number), List (of Number), and String literal (e.g.
   * {@code "[1.0, 2.0]"}).
   * <p>
   * <b>byte[] is intentionally rejected.</b> A signed-int8 byte sequence has no inherent meaning
   * outside an {@link VectorEncoding#INT8} index context: a raw {@code byte[]} could be a binary
   * key, a stored blob, or genuine int8-quantized vector data, and silently dequantizing by
   * {@code v / 127.0f} for non-vector callers would produce nonsense floats with no error signal.
   * Callers operating in an INT8 index context must use {@link #toFloatArray(Object,VectorEncoding)}
   * (which dispatches based on the encoding) or call {@link #dequantizeInt8ToFloat(byte[])}
   * directly. SQL math/utility functions (vector.add, vector.cosineSimilarity, etc.) call this
   * non-encoded variant and therefore reject {@code byte[]} - users must dequantize first.
   *
   * @param vectorObj the object to convert
   *
   * @return float array representation
   *
   * @throws IllegalArgumentException if the input type is not supported (including {@code byte[]})
   *                                  or contains non-numeric elements
   */
  public static float[] toFloatArray(final Object vectorObj) {
    if (vectorObj instanceof float[] f)
      return f;
    if (vectorObj instanceof byte[])
      throw new IllegalArgumentException(
          "byte[] vector is only supported in an INT8-encoded index context. Use "
              + "VectorUtils.toFloatArray(value, VectorEncoding.INT8) or "
              + "VectorUtils.dequantizeInt8ToFloat(byte[]) explicitly.");
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
    if (vectorObj instanceof String s) {
      final String trimmed = s.trim();
      final String inner = trimmed.startsWith("[") && trimmed.endsWith("]") ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
      if (inner.isEmpty())
        return new float[0];
      final String[] parts = inner.split(",");
      final float[] result = new float[parts.length];
      for (int i = 0; i < parts.length; i++)
        result[i] = Float.parseFloat(parts[i].trim());
      return result;
    }
    throw new IllegalArgumentException("Vector must be an array or list, found: " + vectorObj.getClass().getSimpleName());
  }

  /**
   * Encoding-aware variant of {@link #toFloatArray(Object)} for callers operating in an index
   * context. When {@code encoding == INT8} a {@code byte[]} input is dequantized via
   * {@link #dequantizeInt8ToFloat(byte[])}; for any other encoding the call delegates to the
   * strict {@link #toFloatArray(Object)} and a {@code byte[]} input is rejected as in non-index
   * contexts. All other input types (float[], double[], Object[], List, String) behave the same
   * regardless of encoding.
   *
   * @param vectorObj the object to convert
   * @param encoding  the index encoding context (FLOAT32 to reject byte[], INT8 to dequantize)
   *
   * @return float array representation
   *
   * @throws IllegalArgumentException if the input type is not supported, the input is a
   *                                  {@code byte[]} but {@code encoding} is not INT8, or the
   *                                  input contains non-numeric elements
   */
  public static float[] toFloatArray(final Object vectorObj, final VectorEncoding encoding) {
    if (encoding == VectorEncoding.INT8 && vectorObj instanceof byte[] b)
      return dequantizeInt8ToFloat(b);
    return toFloatArray(vectorObj);
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
