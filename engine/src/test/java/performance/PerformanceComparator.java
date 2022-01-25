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
package performance;

import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.UnsignedBytesComparator;

import java.util.Comparator;
import java.util.UUID;

public class PerformanceComparator {
  private static final int REPEAT_TEST  = 3;
  private static final int ARRAY_LENGTH = 10000;

  public static void main(String[] args) throws Exception {
    new PerformanceComparator().run();
  }

  private void run() {
    System.out.println("Generating random strings...");

    final String[] array = new String[ARRAY_LENGTH];
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      array[i] = UUID.randomUUID().toString();
    }

    for (int t = 0; t < REPEAT_TEST; t++)
      test(array);
  }

  private void test(String[] array) {
    System.out.println("Starting comparison matrix for Java Std...");

    long beginTime = System.currentTimeMillis();

    final int[][] results1 = new int[ARRAY_LENGTH][ARRAY_LENGTH];
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      for (int k = 0; k < ARRAY_LENGTH; k++)
        results1[i][k] = BinaryComparator.compareBytes(array[i].getBytes(), array[k].getBytes());
    }

    System.out.println("Comparison matrix finishes in " + (System.currentTimeMillis() - beginTime) + "ms");

    System.out.println("Starting comparison matrix for Java Unsafe is available...");

    beginTime = System.currentTimeMillis();

    final Comparator<byte[]> comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    final int[][] results2 = new int[ARRAY_LENGTH][ARRAY_LENGTH];
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      for (int k = 0; k < ARRAY_LENGTH; k++)
        results2[i][k] = comparator.compare(array[i].getBytes(), array[k].getBytes());
    }

    System.out.println("Comparison matrix finishes in " + (System.currentTimeMillis() - beginTime) + "ms");

    for (int i = 0; i < ARRAY_LENGTH; i++)
      for (int k = 0; k < ARRAY_LENGTH; k++)
        if (results1[i][k] < 0 && results2[i][k] >= 0 || results1[i][k] > 0 && results2[i][k] <= 0)
          System.out.println("Error on results");
  }
}
