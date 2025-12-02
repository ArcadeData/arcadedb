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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionSquareRootTest {

  private SQLFunctionSquareRoot function;

  @BeforeEach
  void setup() {
    function = new SQLFunctionSquareRoot();
  }

  @Test
  void empty() {
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void testNull() {
    function.execute(null, null, null, new Object[] { null }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveInteger() {
    function.execute(null, null, null, new Object[] { 4 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(2);
  }

  @Test
  void negativeInteger() {
    function.execute(null, null, null, new Object[] { -4 }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveLong() {
    function.execute(null, null, null, new Object[] { 4L }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Long).isTrue();
    assertThat(result).isEqualTo(2L);
  }

  @Test
  void negativeLong() {
    function.execute(null, null, null, new Object[] { -4L }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveShort() {
    function.execute(null, null, null, new Object[] { (short) 4 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Short).isTrue();
    assertThat((short) 2).isEqualTo(result);
  }

  @Test
  void negativeShort() {
    function.execute(null, null, null, new Object[] { (short) -4 }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveDouble() {
    function.execute(null, null, null, new Object[] { 4.0D }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Double).isTrue();
    assertThat(result).isEqualTo(2.0D);
  }

  @Test
  void negativeDouble() {
    function.execute(null, null, null, new Object[] { -4.0D }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveFloat() {
    function.execute(null, null, null, new Object[] { 4.0F }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Float).isTrue();
    assertThat(result).isEqualTo(2.0F);
  }

  @Test
  void negativeFloat() {
    function.execute(null, null, null, new Object[] { -4.0F }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveBigDecimal() {
    function.execute(null, null, null, new Object[] { new BigDecimal("4.0") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigDecimal).isTrue();
    assertThat(new BigDecimal("2")).isEqualTo(result);
  }

  @Test
  void negativeBigDecimal() {
    function.execute(null, null, null, new Object[] { BigDecimal.valueOf(-4.0D) }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveBigInteger() {
    function.execute(null, null, null, new Object[] { new BigInteger("4") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigInteger).isTrue();
    assertThat(new BigInteger("2")).isEqualTo(result);
  }

  @Test
  void negativeBigInteger() {
    function.execute(null, null, null, new Object[] { new BigInteger("-4") }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void nonNumber() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[]{"abc"}, null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromQuery() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testSqrtFunction", (db) -> {
      final ResultSet result = db.query("sql", "select sqrt(4.0) as sqrt");
      assertThat(((Number) result.next().getProperty("sqrt")).floatValue()).isEqualTo(2.0F);
    });
  }
}
