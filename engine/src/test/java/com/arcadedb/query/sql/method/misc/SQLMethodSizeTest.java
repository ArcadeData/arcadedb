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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;

class SQLMethodSizeTest {
    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodSize();
    }

    @Test
    void testNulIsReturnedAsZero() {
        Object result = method.execute(null, null, null, null, null);
        assertThat(result).isInstanceOf(Number.class);
        assertThat(result).isEqualTo(0);
    }

    @Test
    void testSizeOfIdentifiable() {
        Database db = Mockito.mock(Database.class);
        RID rid = new RID(db, 1, 1);
        Object result = method.execute(null, null, null, rid, null);
        assertThat(result).isInstanceOf(Number.class);
        assertThat(result).isEqualTo(1);
    }

    @Test
    void testSizeOfMultiValue() {
        Database db = Mockito.mock(Database.class);
        RID rid = new RID(db, 1, 1);
        Collection<RID> multiValue = List.of(rid, rid, rid);
        Object result = method.execute(null, null, null, multiValue, null);
        assertThat(result).isInstanceOf(Number.class);
        assertThat(result).isEqualTo(3);
    }

}
