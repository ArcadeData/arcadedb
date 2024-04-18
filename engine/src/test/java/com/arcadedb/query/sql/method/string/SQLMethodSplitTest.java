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
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodSplitTest {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodSplit();
  }

  @Test
  void testNull() {
    //null ithis
    Object result = method.execute(null, null, null, new Object[] { "," });
    assertThat(result).isNull();

    //null prefix
    result = method.execute("first, second", null, null, null);
    assertThat(result).isEqualTo("first, second");
  }

  @Test
  void testSplitByComma() {
    //null separator
    final Object result = method.execute("first,second", null, null, new Object[] { "," });
    assertThat(result).isInstanceOf(String[].class);
    final String[] splitted = (String[]) result;
    assertThat(splitted).hasSize(2).contains("first", "second");
  }

  //@Test
  void perfTestSystemSplitVsCodeUtils() {
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
      perfSystemSplit();
    System.out.println(System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
      perfCodeUtils();
    System.out.println(System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
      perfSystemSplit();
    System.out.println(System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
      perfCodeUtils();
    System.out.println(System.currentTimeMillis() - start);

  }

  private void perfSystemSplit() {
    final String rid = "#12:23231";
    for (int i = 0; i < 10_000_000; i++) {
      final String[] parts = rid.split(":", 2);
      Assertions.assertEquals(2, parts.length);
    }
  }

  private void perfCodeUtils() {
    final String rid = "#12:23231";
    for (int i = 0; i < 10_000_000; i++) {
      final List<String> parts = CodeUtils.split(rid, ':', 2);
      Assertions.assertEquals(2, parts.size());
    }
  }
}
