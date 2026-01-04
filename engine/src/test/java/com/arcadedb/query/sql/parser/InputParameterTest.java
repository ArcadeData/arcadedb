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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for InputParameter to verify that primitive arrays are not converted to PCollection.
 * This is critical for performance, especially for vector operations with float[] parameters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InputParameterTest {

  @Test
  void testFloatArrayNotConvertedToPCollection() {
    // Given: A float array parameter (common for vector operations)
    final float[] floatArray = new float[]{1.0f, 2.0f, 3.0f};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(floatArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(float[].class);
    assertThat(result).isSameAs(floatArray);
  }

  @Test
  void testIntArrayNotConvertedToPCollection() {
    // Given: An int array parameter
    final int[] intArray = new int[]{1, 2, 3};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(intArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(int[].class);
    assertThat(result).isSameAs(intArray);
  }

  @Test
  void testDoubleArrayNotConvertedToPCollection() {
    // Given: A double array parameter
    final double[] doubleArray = new double[]{1.0, 2.0, 3.0};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(doubleArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(double[].class);
    assertThat(result).isSameAs(doubleArray);
  }

  @Test
  void testLongArrayNotConvertedToPCollection() {
    // Given: A long array parameter
    final long[] longArray = new long[]{1L, 2L, 3L};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(longArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(long[].class);
    assertThat(result).isSameAs(longArray);
  }

  @Test
  void testShortArrayNotConvertedToPCollection() {
    // Given: A short array parameter
    final short[] shortArray = new short[]{1, 2, 3};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(shortArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(short[].class);
    assertThat(result).isSameAs(shortArray);
  }

  @Test
  void testByteArrayNotConvertedToPCollection() {
    // Existing behavior should still work
    final byte[] byteArray = new byte[]{1, 2, 3};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(byteArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(byte[].class);
    assertThat(result).isSameAs(byteArray);
  }

  @Test
  void testBooleanArrayNotConvertedToPCollection() {
    // Given: A boolean array parameter
    final boolean[] booleanArray = new boolean[]{true, false, true};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(booleanArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(boolean[].class);
    assertThat(result).isSameAs(booleanArray);
  }

  @Test
  void testCharArrayNotConvertedToPCollection() {
    // Given: A char array parameter
    final char[] charArray = new char[]{'a', 'b', 'c'};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(charArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(char[].class);
    assertThat(result).isSameAs(charArray);
  }

  @Test
  void testWrapperFloatArrayNotConvertedToPCollection() {
    // Float[] (boxed) should also be excluded for consistency
    final Float[] floatWrapperArray = new Float[]{1.0f, 2.0f, 3.0f};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(floatWrapperArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(Float[].class);
    assertThat(result).isSameAs(floatWrapperArray);
  }

  @Test
  void testWrapperIntegerArrayNotConvertedToPCollection() {
    // Integer[] (boxed) should also be excluded for consistency
    final Integer[] integerWrapperArray = new Integer[]{1, 2, 3};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(integerWrapperArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(Integer[].class);
    assertThat(result).isSameAs(integerWrapperArray);
  }

  @Test
  void testWrapperDoubleArrayNotConvertedToPCollection() {
    // Double[] (boxed) should also be excluded for consistency
    final Double[] doubleWrapperArray = new Double[]{1.0, 2.0, 3.0};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(doubleWrapperArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(Double[].class);
    assertThat(result).isSameAs(doubleWrapperArray);
  }

  @Test
  void testWrapperByteArrayNotConvertedToPCollection() {
    // Byte[] (boxed) should also be excluded (was already excluded before)
    final Byte[] byteWrapperArray = new Byte[]{1, 2, 3};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(byteWrapperArray);

    // Then: Should return the array as-is, not converted to PCollection
    assertThat(result).isInstanceOf(Byte[].class);
    assertThat(result).isSameAs(byteWrapperArray);
  }

  @Test
  void testObjectArrayStillConvertedToPCollection() {
    // String[] should still be converted to PCollection (existing behavior for object arrays)
    final String[] stringArray = new String[]{"a", "b", "c"};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(stringArray);

    // Then: Should be converted to PCollection, not passed as-is
    assertThat(result).isInstanceOf(PCollection.class);
    assertThat(result).isNotSameAs(stringArray);
  }

  @Test
  void testEmptyFloatArrayNotConvertedToPCollection() {
    // Edge case: Empty float array
    final float[] emptyArray = new float[0];
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(emptyArray);

    // Then: Should return the array as-is
    assertThat(result).isInstanceOf(float[].class);
    assertThat(result).isSameAs(emptyArray);
  }

  @Test
  void testSingleElementFloatArrayNotConvertedToPCollection() {
    // Edge case: Single element float array
    final float[] singleElementArray = new float[]{42.0f};
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(singleElementArray);

    // Then: Should return the array as-is
    assertThat(result).isInstanceOf(float[].class);
    assertThat(result).isSameAs(singleElementArray);
  }

  @Test
  void testLargeFloatArrayNotConvertedToPCollection() {
    // Performance test: Large float array (typical vector size)
    final float[] largeArray = new float[128];
    for (int i = 0; i < 128; i++) {
      largeArray[i] = i * 1.0f;
    }
    final InputParameter param = new InputParameter(-1);

    // When: Converting to parsed tree
    final Object result = param.toParsedTree(largeArray);

    // Then: Should return the array as-is (critical for vector performance)
    assertThat(result).isInstanceOf(float[].class);
    assertThat(result).isSameAs(largeArray);
  }
}
