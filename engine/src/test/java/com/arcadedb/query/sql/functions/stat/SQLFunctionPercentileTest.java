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
package com.arcadedb.query.sql.functions.stat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.arcadedb.query.sql.function.stat.SQLFunctionPercentile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;


public class SQLFunctionPercentileTest {

  private SQLFunctionPercentile percentile;

  @BeforeEach
  public void beforeMethod() {
    percentile =
        new SQLFunctionPercentile();
  }

  @Test
  public void testEmpty() {
    Object result = percentile.getResult();
    assertNull(result);
  }

  @Test
  public void testSingleValueLower() {
    percentile.execute(null, null, null, new Object[] {10, .25}, null);
    assertEquals(10, percentile.getResult());
  }

  @Test
  public void testSingleValueUpper() {
    percentile.execute(null, null, null, new Object[] {10, .75}, null);
    assertEquals(10, percentile.getResult());
  }

  @Test
  public void test50thPercentileOdd() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .5}, null);
    }

    Object result = percentile.getResult();
    assertEquals(3.0, result);
  }

  @Test
  public void test50thPercentileOddWithNulls() {
    Integer[] scores = {null, 1, 2, null, 3, 4, null, 5};

    for (Integer s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .5}, null);
    }

    Object result = percentile.getResult();
    assertEquals(3.0, result);
  }

  @Test
  public void test50thPercentileEven() {
    int[] scores = {1, 2, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .5}, null);
    }

    Object result = percentile.getResult();
    assertEquals(3.0, result);
  }

  @Test
  public void testFirstQuartile() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .25}, null);
    }

    Object result = percentile.getResult();
    assertEquals(1.5, result);
  }

  @Test
  public void testThirdQuartile() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .75}, null);
    }

    Object result = percentile.getResult();
    assertEquals(4.5, result);
  }

  @Test
  public void testMultiQuartile() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .25, .75}, null);
    }

    List<Number> result = (List<Number>) percentile.getResult();
    assertEquals(1.5, result.get(0).doubleValue(), 0);
    assertEquals(4.5, result.get(1).doubleValue(), 0);
  }
}
