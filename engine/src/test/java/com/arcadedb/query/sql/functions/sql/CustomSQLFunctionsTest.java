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
package com.arcadedb.query.sql.functions.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

public class CustomSQLFunctionsTest {

    @Test
    public void testRandom() throws Exception {
        TestHelper.executeInNewDatabase("testRandom", (db) -> {
            ResultSet result = db.query("sql", "select math_random() as random");
            assertTrue((Double) result.next().getProperty("random") > 0);
        });
    }

    @Test
    public void testLog10() throws Exception {
        TestHelper.executeInNewDatabase("testRandom", (db) -> {
            ResultSet result = db.query("sql", "select math_log10(10000) as log10");
            assertEquals(result.next().getProperty("log10"), 4.0, 0.0001);
        });
    }

    @Test
    public void testAbsInt() throws Exception {
        TestHelper.executeInNewDatabase("testRandom", (db) -> {
            ResultSet result = db.query("sql", "select math_abs(-5) as abs");
            assertTrue((Integer) result.next().getProperty("abs") == 5);
        });
    }

    @Test
    public void testAbsDouble() throws Exception {
        TestHelper.executeInNewDatabase("testRandom", (db) -> {
            ResultSet result = db.query("sql", "select math_abs(-5.0d) as abs");
            assertTrue((Double) result.next().getProperty("abs") == 5.0);
        });
    }

    @Test
    public void testAbsFloat() throws Exception {
        TestHelper.executeInNewDatabase("testRandom", (db) -> {
            ResultSet result = db.query("sql", "select math_abs(-5.0f) as abs");
            assertTrue((Float) result.next().getProperty("abs") == 5.0);
        });
    }

    @Test
    public void testNonExistingFunction() {
        assertThrows(QueryParsingException.class, () ->
                TestHelper.executeInNewDatabase("testRandom", (db) -> {
                    ResultSet result = db.query("sql", "select math_min('boom', 'boom') as boom");
                    result.next();
                })
        );
    }
}
