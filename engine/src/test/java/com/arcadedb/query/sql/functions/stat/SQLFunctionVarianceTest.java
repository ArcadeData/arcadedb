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

import static org.junit.jupiter.api.Assertions.assertNull;

import com.arcadedb.query.sql.function.stat.SQLFunctionVariance;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SQLFunctionVarianceTest {

    private SQLFunctionVariance variance;

    @BeforeEach
    public void setup() {
        variance = new SQLFunctionVariance();
    }

    @Test
    public void testEmpty() {
        Object result = variance.getResult();
        assertNull(result);
    }

    @Test
    public void testVariance() {
        Integer[] scores = {4, 7, 15, 3};

        for (Integer s : scores) {
            variance.execute(null, null, null, new Object[]{s}, null);
        }

        Object result = variance.getResult();
        Assertions.assertEquals(22.1875, result);
    }

    @Test
    public void testVariance1() {
        Integer[] scores = {4, 7};

        for (Integer s : scores) {
            variance.execute(null, null, null, new Object[]{s}, null);
        }

        Object result = variance.getResult();
        Assertions.assertEquals(2.25, result);
    }

    @Test
    public void testVariance2() {
        Integer[] scores = {15, 3};

        for (Integer s : scores) {
            variance.execute(null, null, null, new Object[]{s}, null);
        }

        Object result = variance.getResult();
        Assertions.assertEquals(36.0, result);
    }
}
