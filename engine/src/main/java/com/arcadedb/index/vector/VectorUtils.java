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
import java.util.Locale;
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
   * Checks if a JVector vector is all zeros, for callers that already hold the converted representation and
   * would otherwise have to materialize a {@code float[]} copy just to run the check.
   */
  public static boolean isZeroVector(final VectorFloat<?> vector) {
    for (int i = 0; i < vector.length(); i++)
      if (vector.get(i) != 0.0f)
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
          """
          INT8 dequantization encountered byte value -128 and clamped it to -127 \
          (Cohere/OpenAI calibration emits [-127, 127] only). Subsequent occurrences will not be logged. \
          Verify the byte source if precise [-128, 127] handling is required.""");
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
    return toFloatArray(vectorObj, false);
  }

  /**
   * Variant of {@link #toFloatArray(Object)} that maps {@code null} collection elements to
   * {@link Float#NaN} instead of throwing. Used by the validity-check functions
   * {@code vector.hasNaN} and {@code vector.hasInf} (issue #3099): an invalid SQL math op such as
   * {@code sqrt(-1.0)} is coerced to {@code NULL} inside a collection literal, so
   * {@code vector.hasNaN([1.0, sqrt(-1.0), 3.0])} must detect it as a NaN rather than crashing the
   * conversion with a {@code NullPointerException}. By substituting {@code NaN} for {@code null},
   * {@code hasNaN} naturally returns {@code true} and {@code hasInf} naturally ignores it (NaN is not
   * infinite), keeping both functions symmetric without per-function null handling.
   *
   * @param vectorObj the object to convert
   *
   * @return float array representation, with null elements replaced by {@link Float#NaN}
   */
  public static float[] toFloatArrayNaNForNull(final Object vectorObj) {
    return toFloatArray(vectorObj, true);
  }

  private static float[] toFloatArray(final Object vectorObj, final boolean nullElementAsNaN) {
    if (vectorObj instanceof float[] f)
      return f;
    if (vectorObj instanceof byte[])
      throw new IllegalArgumentException(
          """
          byte[] vector is only supported in an INT8-encoded index context. Use \
          VectorUtils.toFloatArray(value, VectorEncoding.INT8) or \
          VectorUtils.dequantizeInt8ToFloat(byte[]) explicitly.""");
    if (vectorObj instanceof double[] d) {
      final float[] result = new float[d.length];
      for (int i = 0; i < d.length; i++)
        result[i] = (float) d[i];
      return result;
    }
    if (vectorObj instanceof long[] l) {
      // Issue #4148: integer-only JSON arrays now arrive as long[] from the HTTP path.
      final float[] result = new float[l.length];
      for (int i = 0; i < l.length; i++)
        result[i] = l[i];
      return result;
    }
    if (vectorObj instanceof int[] in) {
      final float[] result = new float[in.length];
      for (int i = 0; i < in.length; i++)
        result[i] = in[i];
      return result;
    }
    if (vectorObj instanceof Object[] objArray) {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++)
        result[i] = elementToFloat(objArray[i], nullElementAsNaN);
      return result;
    }
    if (vectorObj instanceof List<?> list) {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++)
        result[i] = elementToFloat(list.get(i), nullElementAsNaN);
      return result;
    }
    if (vectorObj instanceof String s) {
      final String trimmed = s.trim();
      final String inner = trimmed.startsWith("[") && trimmed.endsWith("]") ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
      final String cleaned = inner.trim();
      if (cleaned.isEmpty())
        return new float[0];
      // Split on commas, semicolons and/or whitespace so every asString()/vector.toString() format
      // round-trips: comma-separated (COMPACT/PYTHON/JULIA/NUMPY), space-separated (MATLAB),
      // semicolon-separated (MATLAB_COLUMN) and multi-line (PRETTY). This is intentionally lenient: a
      // string mixing separators (e.g. "1.0, 2.0; 3.0") is accepted even though no format emits one, so
      // hand-written input parses without fuss.
      final String[] parts = cleaned.split("[,;\\s]+");
      final float[] result = new float[parts.length];
      for (int i = 0; i < parts.length; i++)
        result[i] = Float.parseFloat(parts[i]);
      return result;
    }
    throw new IllegalArgumentException("Vector must be an array or list, found: " + vectorObj.getClass().getSimpleName());
  }

  /**
   * Converts a single collection element to a float. A {@code null} element either becomes
   * {@link Float#NaN} (when {@code nullElementAsNaN} is set, for the validity-check functions) or
   * triggers a clear {@link IllegalArgumentException} - never a {@link NullPointerException} from
   * calling {@code getClass()} on a null (issue #3099).
   */
  private static float elementToFloat(final Object elem, final boolean nullElementAsNaN) {
    if (elem instanceof Number num)
      return num.floatValue();
    if (elem == null) {
      if (nullElementAsNaN)
        return Float.NaN;
      throw new IllegalArgumentException("Vector elements must be numbers, found: null");
    }
    throw new IllegalArgumentException("Vector elements must be numbers, found: " + elem.getClass().getSimpleName());
  }

  /**
   * Human-readable string formats for a vector, shared by the {@code vector.toString()} SQL function and
   * the {@code asString()} SQL method.
   * <ul>
   *   <li>{@code COMPACT}: single line {@code [1.0, 2.0, 3.0]} (default)</li>
   *   <li>{@code PRETTY}: one element per line</li>
   *   <li>{@code PYTHON}: Python list literal {@code [1.0, 2.0, 3.0]}</li>
   *   <li>{@code MATLAB}: space-separated row vector {@code [1.0 2.0 3.0]}</li>
   *   <li>{@code MATLAB_COLUMN}: semicolon-separated column vector {@code [1.0; 2.0; 3.0]}</li>
   *   <li>{@code JULIA}: Julia vector literal {@code [1.0, 2.0, 3.0]}</li>
   *   <li>{@code NUMPY}: bare comma-separated {@code 1.0, 2.0, 3.0} (no brackets), suitable for
   *       {@code numpy.fromstring(..., sep=",")}</li>
   * </ul>
   */
  public enum StringFormat {
    COMPACT,
    PRETTY,
    PYTHON,
    MATLAB,
    MATLAB_COLUMN,
    JULIA,
    NUMPY
  }

  /**
   * Parses a (case-insensitive) format name into a {@link StringFormat}.
   *
   * @throws IllegalArgumentException with the list of supported formats when the name is unknown
   */
  public static StringFormat parseStringFormat(final String name) {
    try {
      return StringFormat.valueOf(name.toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown format: " + name + ". Supported: COMPACT, PRETTY, PYTHON, MATLAB, MATLAB_COLUMN, JULIA, NUMPY");
    }
  }

  /**
   * Renders a vector to a string using the given {@link StringFormat}. An empty vector renders as
   * {@code []} (or an empty string for {@code NUMPY}).
   */
  public static String formatVector(final float[] vector, final StringFormat format) {
    if (format == StringFormat.PRETTY) {
      // PRETTY builds its own newline-delimited layout and does not use the single-line separator below.
      final StringBuilder sb = new StringBuilder("[\n");
      for (int i = 0; i < vector.length; i++) {
        sb.append("  ").append(vector[i]);
        if (i < vector.length - 1)
          sb.append(",");
        sb.append("\n");
      }
      return sb.append("]").toString();
    }

    final String separator = switch (format) {
      case MATLAB -> " ";
      case MATLAB_COLUMN -> "; ";
      default -> ", ";
    };
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vector.length; i++) {
      if (i > 0)
        sb.append(separator);
      sb.append(vector[i]);
    }
    // NUMPY emits a bare comma-separated list (no brackets) for numpy.fromstring(); all others bracket it.
    return format == StringFormat.NUMPY ? sb.toString() : "[" + sb + "]";
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
   * Converts various object types to an int array. Handles: {@code int[]}, {@code long[]},
   * {@code Integer[]}, {@code Object[]} of {@link Number}, and {@code List} of {@link Number}.
   * Used for sparse-vector dimension indices and any other call site that needs a primitive
   * {@code int[]} from a heterogeneous JSON / SQL parameter source. Mirrors
   * {@link #toFloatArray(Object)} and includes the {@code long[]} branch for the issue #4148
   * HTTP path that now returns integer-only JSON arrays as {@code long[]}.
   *
   * @param arrayObj the object to convert
   *
   * @return int array representation
   *
   * @throws IllegalArgumentException if the input type is not supported or contains non-numeric
   *                                  elements
   */
  public static int[] toIntArray(final Object arrayObj) {
    if (arrayObj instanceof int[] in)
      return in;
    if (arrayObj instanceof long[] src) {
      final int[] out = new int[src.length];
      for (int i = 0; i < src.length; i++)
        out[i] = (int) src[i];
      return out;
    }
    if (arrayObj instanceof Integer[] arr) {
      final int[] out = new int[arr.length];
      for (int i = 0; i < arr.length; i++)
        out[i] = arr[i];
      return out;
    }
    if (arrayObj instanceof Object[] objArray) {
      final int[] out = new int[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num)
          out[i] = num.intValue();
        else
          throw new IllegalArgumentException("Array elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
      }
      return out;
    }
    if (arrayObj instanceof List<?> list) {
      final int[] out = new int[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num)
          out[i] = num.intValue();
        else
          throw new IllegalArgumentException("Array elements must be numbers, found: " + elem.getClass().getSimpleName());
      }
      return out;
    }
    throw new IllegalArgumentException("Array must be int[], long[], Integer[], Object[] or List<Number>, found: "
        + arrayObj.getClass().getSimpleName());
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
    // Issue #4583: a zero-magnitude vector yields an undefined cosine (0/0). The JVector SIMD path
    // returns NaN (and throws an AssertionError when run with -ea) while the scalar fallback below
    // returns 0.0f, so the same query could rank differently depending on whether the JVM has the
    // Vector API enabled, and NaN would poison Float.compare ordering. Guard up front and return a
    // consistent 0.0f sentinel (distance 1.0 for the 1 - score callers). isZeroVector short-circuits
    // on the first non-zero element, so this is O(1) for the common non-degenerate case.
    if (isZeroVector(v1) || isZeroVector(v2))
      return 0.0f;
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
        // Cast operands to double before multiplying so the dot-product accumulates at the same
        // precision as normA/normB instead of forming a float product first (issue #4583).
        dotProduct += (double) v1[i] * v2[i];
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
