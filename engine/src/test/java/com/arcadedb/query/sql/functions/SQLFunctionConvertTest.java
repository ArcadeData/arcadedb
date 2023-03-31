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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionConvertTest {
  public SQLFunctionConvertTest() {
    GlobalConfiguration.resetAll();
  }

  @Test
  public void testSQLConversions() throws Exception {
    TestHelper.executeInNewDatabase("testSQLConvert", (db) -> db.transaction(() -> {
      db.command("sql", "create document type TestConversion");

      db.command("sql", "insert into TestConversion set string = 'Jay', date = sysdate(), number = 33, dateAsString = '2011-12-03T10:15:30.388'");

      final Document doc = db.query("sql", "select from TestConversion limit 1").next().toElement();

      db.command("sql", "update TestConversion set selfrid = 'foo" + doc.getIdentity() + "'");

      ResultSet results = db.query("sql", "select string.asString() as convert from TestConversion");
      assertNotNull(results);
      Object convert = results.next().getProperty("convert");
      assertTrue(convert instanceof String, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.asDate() as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Date, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select dateAsString.asDate(\"yyyy-MM-dd'T'HH:mm:ss.SSS\") as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Date, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.asDateTime() as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Date, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select dateAsString.asDateTime(\"yyyy-MM-dd'T'HH:mm:ss.SSS\") as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Date, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.asInteger() as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Integer, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.asLong() as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Long, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.asFloat() as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Float, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.asDecimal() as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof BigDecimal, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select \"100000.123\".asDecimal()*1000 as convert");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof BigDecimal, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.convert('LONG') as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Long, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.convert('SHORT') as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Short, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select number.convert('DOUBLE') as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertTrue(convert instanceof Double, "Found " + convert.getClass() + " instead");

      results = db.query("sql", "select selfrid.substring(3).convert('LINK').string as convert from TestConversion");
      assertNotNull(results);
      convert = results.next().getProperty("convert");
      assertEquals(convert, "Jay");
    }));
  }
}
