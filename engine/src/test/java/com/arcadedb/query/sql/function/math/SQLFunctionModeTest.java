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
package com.arcadedb.query.sql.function.math;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLFunctionModeTest {

  private SQLFunctionMode mode;

  @BeforeEach
  public void setup() {
    mode = new SQLFunctionMode();
  }

  @Test
  public void testEmpty() {
    final Object result = mode.getResult();
    assertThat(result).isNull();
  }

  @Test
  public void testSingleMode() {
    final int[] scores = { 1, 2, 3, 3, 3, 2 };

    for (final int s : scores) {
      mode.execute(null, null, null, new Object[] { s }, null);
    }

    final Object result = mode.getResult();
    assertThat((int) ((List<Integer>) result).getFirst()).isEqualTo(3);
  }

  @Test
  public void testMultiMode() {
    final int[] scores = { 1, 2, 3, 3, 3, 2, 2 };

    for (final int s : scores) {
      mode.execute(null, null, null, new Object[] { s }, null);
    }

    final Object result = mode.getResult();
    final List<Integer> modes = (List<Integer>) result;
    assertThat(modes.size()).isEqualTo(2);
    assertThat(modes.contains(2)).isTrue();
    assertThat(modes.contains(3)).isTrue();
  }

  @Test
  public void testMultiValue() {
    final List[] scores = new List[2];
    scores[0] = Arrays.asList(1, 2, null, 3, 4);
    scores[1] = Arrays.asList(1, 1, 1, 2, null);

    for (final List s : scores) {
      mode.execute(null, null, null, new Object[] { s }, null);
    }

    final Object result = mode.getResult();
    assertThat((int) ((List<Integer>) result).getFirst()).isEqualTo(1);
  }
}
