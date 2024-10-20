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
package com.arcadedb.query.sql.functions.math;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.math.SQLFunctionSquareRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLFunctionSquareRootTest {

    private SQLFunctionSquareRoot function;

    @BeforeEach
    public void setup() {
        function = new SQLFunctionSquareRoot();
    }

    @Test
    public void testEmpty() {
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testNull() {
        function.execute(null, null, null, new Object[]{null}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testPositiveInteger() {
        function.execute(null, null, null, new Object[]{4}, null);
        final Object result = function.getResult();
      assertThat(result instanceof Integer).isTrue();
      assertThat(result).isEqualTo(2);
    }

    @Test
    public void testNegativeInteger() {
        function.execute(null, null, null, new Object[]{-4}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testPositiveLong() {
        function.execute(null, null, null, new Object[]{4L}, null);
        final Object result = function.getResult();
      assertThat(result instanceof Long).isTrue();
      assertThat(result).isEqualTo(2L);
    }

    @Test
    public void testNegativeLong() {
        function.execute(null, null, null, new Object[]{-4L}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testPositiveShort() {
        function.execute(null, null, null, new Object[]{(short) 4}, null);
        final Object result = function.getResult();
      assertThat(result instanceof Short).isTrue();
      assertThat((short) 2).isEqualTo(result);
    }

    @Test
    public void testNegativeShort() {
        function.execute(null, null, null, new Object[]{(short) -4}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testPositiveDouble() {
        function.execute(null, null, null, new Object[]{4.0D}, null);
        final Object result = function.getResult();
      assertThat(result instanceof Double).isTrue();
      assertThat(result).isEqualTo(2.0D);
    }

    @Test
    public void testNegativeDouble() {
        function.execute(null, null, null, new Object[]{-4.0D}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testPositiveFloat() {
        function.execute(null, null, null, new Object[]{4.0F}, null);
        final Object result = function.getResult();
      assertThat(result instanceof Float).isTrue();
      assertThat(result).isEqualTo(2.0F);
    }

    @Test
    public void testNegativeFloat() {
        function.execute(null, null, null, new Object[]{-4.0F}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testPositiveBigDecimal() {
        function.execute(null, null, null, new Object[]{new BigDecimal("4.0")}, null);
        final Object result = function.getResult();
      assertThat(result instanceof BigDecimal).isTrue();
      assertThat(new BigDecimal("2")).isEqualTo(result);
    }

    @Test
    public void testNegativeBigDecimal() {
        function.execute(null, null, null, new Object[]{BigDecimal.valueOf(-4.0D)}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testPositiveBigInteger() {
        function.execute(null, null, null, new Object[]{new BigInteger("4")}, null);
        final Object result = function.getResult();
      assertThat(result instanceof BigInteger).isTrue();
      assertThat(new BigInteger("2")).isEqualTo(result);
    }

    @Test
    public void testNegativeBigInteger() {
        function.execute(null, null, null, new Object[]{new BigInteger("-4")}, null);
        final Object result = function.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testNonNumber() {
        try {
            function.execute(null, null, null, new Object[]{"abc"}, null);
          fail("Expected  IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testFromQuery() throws Exception {
        TestHelper.executeInNewDatabase("./target/databases/testSqrtFunction", (db) -> {
            final ResultSet result = db.query("sql", "select sqrt(4.0) as sqrt");
            assertThat(((Number) result.next().getProperty("sqrt")).floatValue()).isEqualTo(2.0F);
        });
    }
}
