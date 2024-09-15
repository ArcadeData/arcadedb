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

import com.arcadedb.query.sql.function.math.SQLFunctionVariance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLFunctionVarianceTest {

    private SQLFunctionVariance variance;

    @BeforeEach
    public void setup() {
        variance = new SQLFunctionVariance();
    }

    @Test
    public void testEmpty() {
        final Object result = variance.getResult();
      assertThat(result).isNull();
    }

    @Test
    public void testVariance() {
        final Integer[] scores = {4, 7, 15, 3};

        for (final Integer s : scores) {
            variance.execute(null, null, null, new Object[]{s}, null);
        }

        final Object result = variance.getResult();
      assertThat(result).isEqualTo(22.1875);
    }

    @Test
    public void testVariance1() {
        final Integer[] scores = {4, 7};

        for (final Integer s : scores) {
            variance.execute(null, null, null, new Object[]{s}, null);
        }

        final Object result = variance.getResult();
      assertThat(result).isEqualTo(2.25);
    }

    @Test
    public void testVariance2() {
        final Integer[] scores = {15, 3};

        for (final Integer s : scores) {
            variance.execute(null, null, null, new Object[]{s}, null);
        }

        final Object result = variance.getResult();
      assertThat(result).isEqualTo(36.0);
    }
}
