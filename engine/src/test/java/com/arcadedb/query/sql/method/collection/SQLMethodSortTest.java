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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodSortTest {
  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodSort();
  }

  @Test
  void testSortedList() {
    final List<Number> listin = List.of(1, 2, 3);
    final Object result = method.execute(listin, null, null, null);
    assertThat(result).isInstanceOf(List.class);
    assertThat(result).isEqualTo(listin);
  }

  @Test
  void testReverseSortedList() {
    final List<Number> listin = List.of(3, 2, 1);
    final Object result = method.execute(listin, null, null, new Boolean[] { false });
    assertThat(result).isInstanceOf(List.class);
    assertThat(result).isEqualTo(listin);
  }

  @Test
  void testUnsortedList() {
    final List<Number> listin = List.of(3, 2, 1);
    final List<Number> listout = List.of(1, 2, 3);
    final Object result = method.execute(listin, null, null, null);
    assertThat(result).isInstanceOf(List.class);
    assertThat(result).isEqualTo(listout);
  }

  @Test
  void testCharacterList() {
    final List<String> listin = List.of("z", "A", "b");
    final List<String> listout = List.of("A", "b", "z");
    final Object result = method.execute(listin, null, null, null);
    assertThat(result).isInstanceOf(List.class);
    assertThat(result).isEqualTo(listout);
  }

  @Test
  void testWordList() {
    final List<String> listin = List.of("z1", "z2", "a");
    final List<String> listout = List.of("a", "z1", "z2");
    final Object result = method.execute(listin, null, null, null);
    assertThat(result).isInstanceOf(List.class);
    assertThat(result).isEqualTo(listout);
  }

  @Test
  void testDoubleList() {
    final List<Number> listin = List.of(3.0, 2.2, 0.0001);
    final List<Number> listout = List.of(0.0001, 2.2, 3.0);
    final Object result = method.execute(listin, null, null, null);
    assertThat(result).isInstanceOf(List.class);
    assertThat(result).isEqualTo(listout);
  }

  @Test
  void testScalar() {
    final Integer listin = 3;
    final Integer listout = 3;
    final Object result = method.execute(listin, null, null, null);
    assertThat(result).isInstanceOf(Number.class);
    assertThat(result).isEqualTo(listout);
  }
}
