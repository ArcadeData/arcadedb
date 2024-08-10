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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodTrimSuffixTest {
  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodTrimSuffix();
  }

  @Test
  void testAllNullPreservation() {
    final Object result = method.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void testBaseNullPreservation() {
    final Object result = method.execute(null, null, null, new Object[] {"Bye"});
    assertThat(result).isNull();
  }

  @Test
  void testArgNullPreservation() {
    final Object result = method.execute("Hello World", null, null, null);
    assertThat(result).isEqualTo("Hello World");
  }

  @Test
  void testIdentity() {
    final Object result = method.execute("Hello World", null, null, new Object[] {"Bye"});
    assertThat(result).isEqualTo("Hello World");
  }

  @Test
  void testTrim() {
    final Object result = method.execute("Hello World", null, null, new Object[] {"World"});
    assertThat(result).isEqualTo("Hello ");
  }

  @Test
  void testEmptyArg() {
    final Object result = method.execute("Hello World", null, null, new Object[] {""});
    assertThat(result).isEqualTo("Hello World");
  }

  @Test
  void testEmptyBase() {
    final Object result = method.execute("", null, null, new Object[] {"Bye"});
    assertThat(result).isEqualTo("");
  }
}
