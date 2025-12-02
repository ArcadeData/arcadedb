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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.method.string.SQLMethodSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.*;

class SQLMethodJoinTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodJoin();
    }

    @Test
    void nullValue() {
        Object result = method.execute(null, null, null, null);
        assertThat(result).isNull();
    }

    @Test
    void joinEmptyList() {
        final Object result = method.execute(Collections.emptyList(), null, null, null);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("");
    }

    @Test
    void joinAnInteger() {
        final Object result = method.execute(10, null, null, null);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("10");
    }

    @Test
    void joinByDefaultSeparator() {
        final Object result = method.execute(List.of("first", "second"), null, null, null);
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("first,second");
    }

    @Test
    void joinByDefaultSeparatorWithNullParams() {
        Object result = method.execute(List.of("first", "second"), null, null, new Object[]{null});
        assertThat(result).isEqualTo("first,second");
    }

    @Test
    void joinByProvidedSeparator() {
        final Object result = method.execute(List.of("first", "second"), null, null, new String[]{";"});
        assertThat(result).isInstanceOf(String.class);
        assertThat(result).isEqualTo("first;second");
    }
}
