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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.math.SQLFunctionAbsoluteValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Tests the absolute value function. The key is that the mathematical abs function is correctly
 * applied and that values retain their types.
 *
 * @author Michael MacFadden
 */
public class SQLFunctionAbsoluteValueTest {

    private SQLFunctionAbsoluteValue function;

    @BeforeEach
    public void setup() {
        function = new SQLFunctionAbsoluteValue();
    }

    @Test
    public void testEmpty() {
        Object result = function.getResult();
        assertNull(result);
    }

    @Test
    public void testNull() {
        function.execute(null, null, null, new Object[]{null}, null);
        Object result = function.getResult();
        assertNull(result);
    }

    @Test
    public void testPositiveInteger() {
        function.execute(null, null, null, new Object[]{10}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Integer);
        assertEquals(result, 10);
    }

    @Test
    public void testNegativeInteger() {
        function.execute(null, null, null, new Object[]{-10}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Integer);
        assertEquals(result, 10);
    }

    @Test
    public void testPositiveLong() {
        function.execute(null, null, null, new Object[]{10L}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Long);
        assertEquals(result, 10L);
    }

    @Test
    public void testNegativeLong() {
        function.execute(null, null, null, new Object[]{-10L}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Long);
        assertEquals(result, 10L);
    }

    @Test
    public void testPositiveShort() {
        function.execute(null, null, null, new Object[]{(short) 10}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Short);
        assertEquals(result, (short) 10);
    }

    @Test
    public void testNegativeShort() {
        function.execute(null, null, null, new Object[]{(short) -10}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Short);
        assertEquals(result, (short) 10);
    }

    @Test
    public void testPositiveDouble() {
        function.execute(null, null, null, new Object[]{10.5D}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Double);
        assertEquals(result, 10.5D);
    }

    @Test
    public void testNegativeDouble() {
        function.execute(null, null, null, new Object[]{-10.5D}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Double);
        assertEquals(result, 10.5D);
    }

    @Test
    public void testPositiveFloat() {
        function.execute(null, null, null, new Object[]{10.5F}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Float);
        assertEquals(result, 10.5F);
    }

    @Test
    public void testNegativeFloat() {
        function.execute(null, null, null, new Object[]{-10.5F}, null);
        Object result = function.getResult();
        assertTrue(result instanceof Float);
        assertEquals(result, 10.5F);
    }

    @Test
    public void testPositiveBigDecimal() {
        function.execute(null, null, null, new Object[]{new BigDecimal("10.5")}, null);
        Object result = function.getResult();
        assertTrue(result instanceof BigDecimal);
        assertEquals(result, new BigDecimal("10.5"));
    }

    @Test
    public void testNegativeBigDecimal() {
        function.execute(null, null, null, new Object[]{BigDecimal.valueOf(-10.5D)}, null);
        Object result = function.getResult();
        assertTrue(result instanceof BigDecimal);
        assertEquals(result, new BigDecimal("10.5"));
    }

    @Test
    public void testPositiveBigInteger() {
        function.execute(null, null, null, new Object[]{new BigInteger("10")}, null);
        Object result = function.getResult();
        assertTrue(result instanceof BigInteger);
        assertEquals(result, new BigInteger("10"));
    }

    @Test
    public void testNegativeBigInteger() {
        function.execute(null, null, null, new Object[]{new BigInteger("-10")}, null);
        Object result = function.getResult();
        assertTrue(result instanceof BigInteger);
        assertEquals(result, new BigInteger("10"));
    }

    @Test
    public void testNonNumber() {
        try {
            function.execute(null, null, null, new Object[]{"abc"}, null);
            Assertions.fail("Expected  IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testFromQuery() throws Exception {
        TestHelper.executeInNewDatabase("./target/databases/testAbsFunction", (db) -> {
            ResultSet result = db.query("sql", "select abs(-45.4) as abs");
            assertEquals(45.4F, ((Number) result.next().getProperty("abs")).floatValue());
        });
    }
}
