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
package com.arcadedb.serializer;

import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance validation test for VarHandleComparator vs PureJavaComparator.
 */
public class VarHandlePerformanceBenchmark {

  private static final int ARRAY_LENGTH = 1000;
  private static final int ITERATIONS   = 3000;

  public static void main(String[] args) {
    System.out.println("VarHandle Performance Test");
    System.out.println("==========================");
    System.out.println("Generating random byte arrays...");

    final byte[][] arrays = new byte[ARRAY_LENGTH][];
    final Random random = new Random(42);
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      final String uuid = UUID.randomUUID().toString();
      arrays[i] = uuid.getBytes();
    }

    System.out.println("\nComparator being tested: " + UnsignedBytesComparator.BEST_COMPARATOR);

    long varHandleTime = 0;
    long pureJavaTime = 0;

    for (int iteration = 0; iteration < ITERATIONS; iteration++) {
      System.out.println("\n--- Iteration " + (iteration + 1) + " ---");

      // Test VarHandle (BEST_COMPARATOR)
      final ByteArrayComparator varHandle = UnsignedBytesComparator.BEST_COMPARATOR;
      final long varHandleStart = System.nanoTime();
      int varHandleResults = 0;
      for (int i = 0; i < ARRAY_LENGTH; i++) {
        for (int k = 0; k < ARRAY_LENGTH; k++) {
          final int result = varHandle.compare(arrays[i], arrays[k]);
          varHandleResults += Integer.signum(result);
        }
      }
      final long varHandleEnd = System.nanoTime();
      final long varHandleDuration = (varHandleEnd - varHandleStart) / 1_000_000; // Convert to ms
      varHandleTime += varHandleDuration;
      System.out.println("VarHandle: " + varHandleDuration + "ms (results: " + varHandleResults + ")");

      // Test PureJava
      final ByteArrayComparator pureJava = UnsignedBytesComparator.PURE_JAVA_COMPARATOR;
      final long pureJavaStart = System.nanoTime();
      int pureJavaResults = 0;
      for (int i = 0; i < ARRAY_LENGTH; i++) {
        for (int k = 0; k < ARRAY_LENGTH; k++) {
          final int result = pureJava.compare(arrays[i], arrays[k]);
          pureJavaResults += Integer.signum(result);
        }
      }
      final long pureJavaEnd = System.nanoTime();
      final long pureJavaDuration = (pureJavaEnd - pureJavaStart) / 1_000_000; // Convert to ms
      pureJavaTime += pureJavaDuration;
      System.out.println("PureJava: " + pureJavaDuration + "ms (results: " + pureJavaResults + ")");

      // Verify results match
      assertThat(varHandleResults).isEqualTo(pureJavaResults);
    }

    final long avgVarHandle = varHandleTime / ITERATIONS;
    final long avgPureJava = pureJavaTime / ITERATIONS;
    final double speedup = (double) avgPureJava / avgVarHandle;

    System.out.println("\n=== SUMMARY ===");
    System.out.println("Average VarHandle time: " + avgVarHandle + "ms");
    System.out.println("Average PureJava time: " + avgPureJava + "ms");
    System.out.println("Speedup: " + String.format("%.2fx", speedup));

    // VarHandle should be faster than or equal to PureJava
    assertThat(avgVarHandle).isLessThanOrEqualTo(avgPureJava * 2); // Allow 2x tolerance
    System.out.println("\n✓ Performance test passed!");
  }
}
