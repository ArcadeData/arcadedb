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

/**
 * Tests the absolute value function. The key is that the mathematical abs function is correctly
 * applied and that values retain their types.
 *
 * @author Michael MacFadden
 */
class SQLFunctionAbsoluteValueTest {

  private SQLFunctionAbsoluteValue function;

  @BeforeEach
  void setup() {
    function = new SQLFunctionAbsoluteValue();
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
    function.execute(null, null, null, new Object[] { 10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(10);
  }

  @Test
  void negativeInteger() {
    function.execute(null, null, null, new Object[] { -10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(10);
  }

  @Test
  void positiveLong() {
    function.execute(null, null, null, new Object[] { 10L }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Long).isTrue();
    assertThat(result).isEqualTo(10L);
  }

  @Test
  void negativeLong() {
    function.execute(null, null, null, new Object[] { -10L }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Long).isTrue();
    assertThat(result).isEqualTo(10L);
  }

  @Test
  void positiveShort() {
    function.execute(null, null, null, new Object[] { (short) 10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Short).isTrue();
    assertThat((short) 10).isEqualTo(result);
  }

  @Test
  void negativeShort() {
    function.execute(null, null, null, new Object[] { (short) -10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Short).isTrue();
    assertThat((short) 10).isEqualTo(result);
  }

  @Test
  void positiveDouble() {
    function.execute(null, null, null, new Object[] { 10.5D }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Double).isTrue();
    assertThat(result).isEqualTo(10.5D);
  }

  @Test
  void negativeDouble() {
    function.execute(null, null, null, new Object[] { -10.5D }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Double).isTrue();
    assertThat(result).isEqualTo(10.5D);
  }

  @Test
  void positiveFloat() {
    function.execute(null, null, null, new Object[] { 10.5F }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Float).isTrue();
    assertThat(result).isEqualTo(10.5F);
  }

  @Test
  void negativeFloat() {
    function.execute(null, null, null, new Object[] { -10.5F }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Float).isTrue();
    assertThat(result).isEqualTo(10.5F);
  }

  @Test
  void positiveBigDecimal() {
    function.execute(null, null, null, new Object[] { new BigDecimal("10.5") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigDecimal).isTrue();
    assertThat(new BigDecimal("10.5")).isEqualTo(result);
  }

  @Test
  void negativeBigDecimal() {
    function.execute(null, null, null, new Object[] { BigDecimal.valueOf(-10.5D) }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigDecimal).isTrue();
    assertThat(new BigDecimal("10.5")).isEqualTo(result);
  }

  @Test
  void positiveBigInteger() {
    function.execute(null, null, null, new Object[] { new BigInteger("10") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigInteger).isTrue();
    assertThat(new BigInteger("10")).isEqualTo(result);
  }

  @Test
  void negativeBigInteger() {
    function.execute(null, null, null, new Object[] { new BigInteger("-10") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigInteger).isTrue();
    assertThat(new BigInteger("10")).isEqualTo(result);
  }

  @Test
  void nonNumber() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[]{"abc"}, null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fromQuery() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testAbsFunction", (db) -> {
      final ResultSet result = db.query("sql", "select abs(-45.4) as abs");
      assertThat(((Number) result.next().getProperty("abs")).floatValue()).isEqualTo(45.4F);
    });
  }
}
