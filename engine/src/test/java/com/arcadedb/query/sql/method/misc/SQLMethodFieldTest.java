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
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SQLMethodFieldTest {

    private SQLMethod method;

    @BeforeEach
    void setUp() {
        method = new SQLMethodField();

    }

    @Test
    void testNulIParamsReturnedAsNull() {
        Object result = method.execute(null, null, null, null, new Object[]{null});
        assertThat(result).isNull();
    }

    @Test
    void testFieldValue() {

        Database database = Mockito.mock(Database.class);

        DocumentType type = Mockito.mock(DocumentType.class);

//        MutableDocument doc = new MutableDocument(database, type, null);
//        doc.set("name", "Foo");
//        doc.set("surname", "Bar");

//        Object result = method.execute(null, doc, null, null, new Object[]{"name"});
//        assertThat(result).isNotNull();
//        assertThat(result).isEqualTo("Foo");

    }
}
