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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.*;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

      db.command("sql",
          "insert into TestConversion set string = 'Jay', date = sysdate(), number = 33, dateAsString = '2011-12-03T10:15:30.388', list = ['A', 'B']");

      final Document doc = db.query("sql", "select from TestConversion limit 1").next().toElement();

      db.command("sql", "update TestConversion set selfrid = 'foo" + doc.getIdentity() + "'");

      ResultSet results = db.query("sql", "select string.asString() as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      Object convert = results.next().getProperty("convert");
      assertThat(convert instanceof String).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.asDate() as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof Date).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select dateAsString.asDate(\"yyyy-MM-dd'T'HH:mm:ss.SSS\") as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof LocalDate).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.asDateTime() as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof LocalDateTime).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select dateAsString.asDateTime(\"yyyy-MM-dd'T'HH:mm:ss.SSS\") as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof LocalDateTime).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.asInteger() as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof Integer).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.asLong() as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof Long).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.asFloat() as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof Float).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.asDecimal() as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof BigDecimal).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select \"100000.123\".asDecimal()*1000 as convert");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof BigDecimal).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.convert('LONG') as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof Long).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.convert('SHORT') as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof Short).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select number.convert('DOUBLE') as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert instanceof Double).as("Found " + convert.getClass() + " instead").isTrue();

      results = db.query("sql", "select selfrid.substring(3).convert('LINK').string as convert from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("convert");
      assertThat(convert).isEqualTo("Jay");

      results = db.query("sql", "select list.transform('toLowerCase') as list from TestConversion");
      assertThat((Iterator<? extends Result>) results).isNotNull();
      convert = results.next().getProperty("list");
      final List list = (List) convert;
      assertThat(list.containsAll(List.of("a", "b"))).isTrue();
    }));
  }
}
