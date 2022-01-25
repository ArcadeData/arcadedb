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
package com.arcadedb.query.sql.method.misc;

import static org.assertj.core.api.Assertions.assertThat;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SQLMethodSplitTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodSplit();
    }

    @Test
    void testNull() {
        //null ithis
        Object result = method.execute(null, null, null, null, new Object[]{","});
        assertThat(result).isNull();

        //null prefix
        result = method.execute("first, second", null, null, null, null);
        assertThat(result).isEqualTo("first, second");
    }

    @Test
    void testSplitByComma() {

        //null separator
        Object result = method.execute("first,second", null, null, null, new Object[]{","});
        assertThat(result).isInstanceOf(String[].class);
        String[] splitted = (String[]) result;
        assertThat(splitted).hasSize(2)
                .contains("first", "second");
    }

}
