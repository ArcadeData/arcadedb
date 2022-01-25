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
package com.arcadedb.query.sql.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionConvertTest {

    @Test
    public void testSQLConversions() throws Exception {
        TestHelper.executeInNewDatabase("testSQLConvert", (db) -> db.transaction(() -> {
            db.command("sql", "create document type TestConversion");

            db.command("sql", "insert into TestConversion set string = 'Jay', date = sysdate(), number = 33");

            Document doc = db.query("sql", "select from TestConversion limit 1").next().toElement();

            db.command("sql", "update TestConversion set selfrid = 'foo" + doc.getIdentity() + "'");

            ResultSet results = db.query("sql", "select string.asString() as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof String);

            results = db.query("sql", "select number.asDate() as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Date);

            results = db.query("sql", "select number.asDateTime() as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Date);

            results = db.query("sql", "select number.asInteger() as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Integer);

            results = db.query("sql", "select number.asLong() as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Long);

            results = db.query("sql", "select number.asFloat() as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Float);

            results = db.query("sql", "select number.asDecimal() as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof BigDecimal);

            results = db.query("sql", "select number.convert('LONG') as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Long);

            results = db.query("sql", "select number.convert('SHORT') as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Short);

            results = db.query("sql", "select number.convert('DOUBLE') as convert from TestConversion");
            assertNotNull(results);

            assertTrue(results.next().getProperty("convert") instanceof Double);

            results = db.query("sql", "select selfrid.substring(3).convert('LINK').string as convert from TestConversion");
            assertNotNull(results);

            assertEquals(results.next().getProperty("convert"), "Jay");
        }));
    }
}
