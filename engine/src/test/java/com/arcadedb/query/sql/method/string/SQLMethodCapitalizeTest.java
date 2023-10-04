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

class SQLMethodCapitalizeTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodCapitalize();
    }

    @Test
    void testNullReturnedAsNull() {
        final Object result = method.execute(null, null, null, null, null);
        assertThat(result).isNull();
    }


    @Test
    void testCapitalizeEmpty() {
        final Object result = method.execute(null, null, null, "", null);
        assertThat(result).isEqualTo("");

    }

    @Test
    void testCapitalizeIdentity() {
        final Object result = method.execute(null, null, null, "Capitalize This", null);
        assertThat(result).isEqualTo("Capitalize This");

    }

    @Test
    void testCapitalizeLower() {
        final Object result = method.execute(null, null, null, "CAPITALIZE THIS", null);
        assertThat(result).isEqualTo("Capitalize This");

    }

    @Test
    void testCapitalizeUpper() {
        final Object result = method.execute(null, null, null, "capitalize this", null);
        assertThat(result).isEqualTo("Capitalize This");

    }

    @Test
    void testCapitalizeSingle() {
        final Object result = method.execute(null, null, null, "c t", null);
        assertThat(result).isEqualTo("C T");

    }

    @Test
    void testCapitalizeNumbers() {
        final Object result = method.execute(null, null, null, "111 222", null);
        assertThat(result).isEqualTo("111 222");

    }
}
