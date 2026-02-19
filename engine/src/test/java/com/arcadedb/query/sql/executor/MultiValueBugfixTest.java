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
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for MultiValue bugs that were fixed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MultiValueBugfixTest {

  /**
   * Regression test for bug in MultiValue.add() when adding array to array.
   * Previously, the code was copying to the wrong array (iObject instead of copy),
   * causing ArrayIndexOutOfBoundsException.
   *
   * Bug was at line 500:
   * System.arraycopy(iToAdd, 0, iObject, Array.getLength(iObject), Array.getLength(iToAdd));
   *
   * Fixed to:
   * System.arraycopy(iToAdd, 0, copy, Array.getLength(iObject), Array.getLength(iToAdd));
   */
  @Test
  void addArrayToArrayBugfix() {
    final Object[] original = { "a" };
    final Object[] toAdd = { "b", "c" };

    final Object result = MultiValue.add(original, toAdd);

    assertThat(result).isInstanceOf(Object[].class);
    final Object[] resultArray = (Object[]) result;
    assertThat(resultArray).hasSize(3);
    assertThat(resultArray[0]).isEqualTo("a");
    assertThat(resultArray[1]).isEqualTo("b");
    assertThat(resultArray[2]).isEqualTo("c");
  }

  /**
   * Test that adding empty array to array works correctly.
   */
  @Test
  void addEmptyArrayToArray() {
    final Object[] original = { "a", "b" };
    final Object[] toAdd = {};

    final Object result = MultiValue.add(original, toAdd);

    assertThat(result).isInstanceOf(Object[].class);
    final Object[] resultArray = (Object[]) result;
    assertThat(resultArray).hasSize(2);
    assertThat(resultArray[0]).isEqualTo("a");
    assertThat(resultArray[1]).isEqualTo("b");
  }

  /**
   * Test that adding array to empty array works correctly.
   */
  @Test
  void addArrayToEmptyArray() {
    final Object[] original = {};
    final Object[] toAdd = { "x", "y" };

    final Object result = MultiValue.add(original, toAdd);

    assertThat(result).isInstanceOf(Object[].class);
    final Object[] resultArray = (Object[]) result;
    assertThat(resultArray).hasSize(2);
    assertThat(resultArray[0]).isEqualTo("x");
    assertThat(resultArray[1]).isEqualTo("y");
  }

  /**
   * Test that adding larger array to smaller array works correctly.
   */
  @Test
  void addLargeArrayToSmallArray() {
    final Object[] original = { "a" };
    final Object[] toAdd = { "b", "c", "d", "e", "f" };

    final Object result = MultiValue.add(original, toAdd);

    assertThat(result).isInstanceOf(Object[].class);
    final Object[] resultArray = (Object[]) result;
    assertThat(resultArray).hasSize(6);
    assertThat(resultArray[0]).isEqualTo("a");
    assertThat(resultArray[1]).isEqualTo("b");
    assertThat(resultArray[2]).isEqualTo("c");
    assertThat(resultArray[3]).isEqualTo("d");
    assertThat(resultArray[4]).isEqualTo("e");
    assertThat(resultArray[5]).isEqualTo("f");
  }
}
