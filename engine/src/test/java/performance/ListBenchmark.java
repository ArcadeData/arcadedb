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
package performance;

import java.util.*;

public class ListBenchmark {
  private final static int REPEAT = 100;
  private final static int TOTAL  = 1_000_000;

  public static void main(String[] args) {
    final List<Integer> list = new ArrayList<>(TOTAL);

    for (int i = 0; i < TOTAL; i++)
      list.add(i);

    // WARM UP
    Result result = getResult(list);
    System.out.println("WARMUP: Total array: " + result.totalArray() + " (" + result.totalNumbers() + ")");
    System.out.println("WARMUP: Total iterator: " + result.totalIterator() + " (" + result.totalNumbers() + ")");

    // BENCHMARK
    result = getResult(list);

    System.out.println("Total array: " + result.totalArray() + " (" + result.totalNumbers() + ")");
    System.out.println("Total iterator: " + result.totalIterator() + " (" + result.totalNumbers() + ")");
  }

  private static Result getResult(final List<Integer> list) {
    long totalNumbers = 0L; // THIS IS JUST TO AVOID COMPILER OPTIMIZATION AND REMOVAL OF THE ENTIRE LINE
    long totalArray = 0L;
    long totalIterator = 0L;

    for (int k = 0; k < 100; k++) {
      long begin = System.currentTimeMillis();

      for (int j = 0; j < REPEAT; j++)
        for (Integer i : list)
          totalNumbers += i; // THIS IS JUST TO AVOID COMPILER OPTIMIZATION AND REMOVAL OF THE ENTIRE LINE

      totalIterator += System.currentTimeMillis() - begin;

      begin = System.currentTimeMillis();

      for (int j = 0; j < REPEAT; j++)
        for (int i = 0; i < TOTAL; i++)
          totalNumbers += list.get(i); // THIS IS JUST TO AVOID COMPILER OPTIMIZATION AND REMOVAL OF THE ENTIRE LINE

      totalArray += System.currentTimeMillis() - begin;
    }
    Result result = new Result(totalNumbers, totalArray, totalIterator);
    return result;
  }

  private record Result(long totalNumbers, long totalArray, long totalIterator) {
  }
}
