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
package com.arcadedb.query.sql.function.sql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.function.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.query.sql.method.misc.SQLMethodHash;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.security.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.stream.*;

import static com.arcadedb.TestHelper.checkActiveDatabases;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SQLFunctionsTest {
  private final DatabaseFactory factory = new DatabaseFactory("./target/databases/SQLFunctionsTest");
  private       Database        database;

  @BeforeEach
  public void beforeEach() {
    checkActiveDatabases();
    FileUtils.deleteRecursively(new File("./target/databases/SQLFunctionsTest"));
    database = factory.create();
    database.getSchema().createDocumentType("V");
    database.getSchema().createDocumentType("Account");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("Account").set("id", i).save();
      }
    });

    database.getSchema().createDocumentType("City");
    database.getSchema().createDocumentType("Country");
    database.transaction(() -> {
      final MutableDocument italy = database.newDocument("Country").set("name", "Italy").save();
      final MutableDocument usa = database.newDocument("Country").set("name", "USA").save();

      database.newDocument("City").set("name", "Rome").set("country", italy).save();
      database.newDocument("City").set("name", "Grosseto").set("country", italy).save();
      database.newDocument("City").set("name", "Miami").set("country", usa).save();
      database.newDocument("City").set("name", "Austin").set("country", usa).save();
    });
  }

  @AfterEach
  public void afterEach() {
    if (database != null)
      database.drop();
    checkActiveDatabases();
    FileUtils.deleteRecursively(new File("./target/databases/SQLFunctionsTest"));
  }

  @Test
  public void queryMax() {
    final ResultSet result = database.command("sql", "select max(id) as max from Account");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Integer>getProperty("max")).isNotNull();
    }
  }

  @Test
  public void testSelectSize() {
    final ResultSet result = database.query("sql", "select @this.size() as size from Account");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Integer>getProperty("size")).isNotNull();
      assertThat(d.<Integer>getProperty("size")).isGreaterThan(0);
    }
  }

  @Test
  public void queryMaxInline() {
    final ResultSet result = database.command("sql", "select max(1,2,7,0,-2,3) as max");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Number>getProperty("max")).isNotNull();

      assertThat(((Number) d.getProperty("max")).intValue()).isEqualTo(7);
    }
  }

  @Test
  public void queryMin() {
    final ResultSet result = database.command("sql", "select min(id) as min from Account");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Number>getProperty("min")).isNotNull();
      assertThat(((Number) d.getProperty("min")).longValue()).isEqualTo(0l);
    }
  }

  @Test
  public void queryMinInline() {
    final ResultSet result = database.command("sql", "select min(1,2,7,0,-2,3) as min");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Number>getProperty("min")).isNotNull();
      assertThat(((Number) d.getProperty("min")).intValue()).isEqualTo(-2);
    }
  }

  @Test
  public void querySum() {
    final ResultSet result = database.command("sql", "select sum(id) as sum from Account");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Number>getProperty("sum")).isNotNull();
    }
  }

  @Test
  public void queryCount() {
    final ResultSet result = database.command("sql", "select count(*) as total from Account");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Number>getProperty("total")).isNotNull();
      assertThat(((Number) d.getProperty("total")).longValue() > 0).isTrue();
    }
  }

  @Test
  public void queryCountWithConditions() {
    final DocumentType indexed = database.getSchema().getOrCreateDocumentType("Indexed");
    indexed.createProperty("key", Type.STRING);

    database.transaction(() -> {
      indexed.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "key");

      database.newDocument("Indexed").set("key", "one").save();
      database.newDocument("Indexed").set("key", "two").save();

      final ResultSet result = database.command("sql", "select count(*) as total from Indexed where key > 'one'");

      assertThat(result.hasNext()).isTrue();
      for (final ResultSet it = result; it.hasNext(); ) {
        final Result d = it.next();
        assertThat(d.<Long>getProperty("total")).isNotNull();
        assertThat(((Number) d.getProperty("total")).longValue()).isGreaterThan(0);
      }
    });
  }

  @Test
  public void queryDistinct() {
    final ResultSet result = database.command("sql", "select distinct(name) as name from City");

    assertThat(result.hasNext()).isTrue();

    final Set<String> cities = new HashSet<>();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result city = it.next();
      final String cityName = city.getProperty("name");
      assertThat(cities.contains(cityName)).isFalse();
      cities.add(cityName);
    }
  }

  @Test
  public void queryFunctionRenamed() {
    final ResultSet result = database.command("sql", "select distinct(name) from City");

    assertThat(result.hasNext()).isTrue();

    for (final ResultSet it = result; it.hasNext(); ) {
      final Result city = it.next();
      assertThat(city.hasProperty("name")).isTrue();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void queryUnionAllAsAggregationNotRemoveDuplicates() {
    ResultSet result = database.command("sql", "select from City");
    final int count = (int) CollectionUtils.countEntries(result);

    result = database.command("sql", "select unionAll(name) as name from City");
    assertThat(result.hasNext()).isTrue();
    final Collection<Object> citiesFound = result.next().getProperty("name");
    assertThat(count).isEqualTo(citiesFound.size());
  }

  @Test
  public void querySetNotDuplicates() {
    final ResultSet result = database.command("sql", "select set(name) as name from City");

    assertThat(result.hasNext()).isTrue();

    final Collection<Object> citiesFound = result.next().getProperty("name");
    assertThat(citiesFound.size() > 1).isTrue();

    final Set<String> cities = new HashSet<String>();
    for (final Object city : citiesFound) {
      assertThat(cities.contains(city.toString())).isFalse();
      cities.add(city.toString());
    }
  }

  @Test
  public void queryList() {
    final ResultSet result = database.command("sql", "select list(name) as names from City");

    assertThat(result.hasNext()).isTrue();

    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      final List<Object> citiesFound = d.getProperty("names");
      assertThat(citiesFound.size() > 1).isTrue();
    }
  }

  @Test
  public void testSelectMap() {
    final ResultSet result = database.query("sql", "select list( 1, 4, 5.00, 'john', map( 'kAA', 'vAA' ) ) as myresult");

    assertThat(result.hasNext()).isTrue();

    final Result document = result.next();
    final List myresult = document.getProperty("myresult");
    assertThat(myresult).isNotNull();

    assertThat(myresult.remove(Integer.valueOf(1))).isTrue();
    assertThat(myresult.remove(Integer.valueOf(4))).isTrue();
    assertThat(myresult.remove(Float.valueOf(5))).isTrue();
    assertThat(myresult.remove("john")).isTrue();

    assertThat(myresult.size()).isEqualTo(1);

    assertThat(myresult.getFirst() instanceof Map).as("The object is: " + myresult.getClass()).isTrue();
    final Map map = (Map) myresult.getFirst();

    final String value = (String) map.get("kAA");
    assertThat(value).isEqualTo("vAA");

    assertThat(map.size()).isEqualTo(1);
  }

  @Test
  public void querySet() {
    final ResultSet result = database.command("sql", "select set(name) as names from City");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      final Set<Object> citiesFound = d.getProperty("names");
      assertThat(citiesFound.size() > 1).isTrue();
    }
  }

  @Test
  public void queryMap() {
    final ResultSet result = database.command("sql", "select map(name, country.name) as names from City");

    assertThat(result.hasNext()).isTrue();

    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      final Map<Object, Object> citiesFound = d.getProperty("names");
      assertThat(citiesFound.size()).isEqualTo(1);
    }
  }

  @Test
  public void queryUnionAllAsInline() {
    final ResultSet result = database.command("sql", "select unionAll(name, country) as edges from City");

    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.getPropertyNames().size()).isEqualTo(1);
      assertThat(d.hasProperty("edges")).isTrue();
    }
  }

  @Test
  public void queryComposedAggregates() {
    final ResultSet result = database.command("sql",
        "select MIN(id) as min, max(id) as max, AVG(id) as average, sum(id) as total from Account");

    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Number>getProperty("min")).isNotNull();
      assertThat(d.<Number>getProperty("max")).isNotNull();
      assertThat(d.<Number>getProperty("average")).isNotNull();
      assertThat(d.<Number>getProperty("total")).isNotNull();

      assertThat(((Number) d.getProperty("max")).longValue() > ((Number) d.getProperty("average")).longValue()).isTrue();
      assertThat(((Number) d.getProperty("average")).longValue() >= ((Number) d.getProperty("min")).longValue()).isTrue();
      assertThat(((Number) d.getProperty("total")).longValue() >= ((Number) d.getProperty("max")).longValue()).as(
          "Total " + d.getProperty("total") + " max " + d.getProperty("max")).isTrue();
    }
  }

  @Test
  public void queryFormat() {
    final ResultSet result = database.command("sql",
        "select format('%d - %s (%s)', nr, street, type, dummy ) as output from Account");
    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<String>getProperty("output")).isNotNull();
    }
  }

  @Test
  public void querySysdateNoFormat() {
    final ResultSet result = database.command("sql", "select sysdate() as date from Account");

    assertThat(result.hasNext()).isTrue();
    Object lastDate = null;
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Object>getProperty("date")).isNotNull();

      if (lastDate != null)
        d.getProperty("date").equals(lastDate);

      lastDate = d.getProperty("date");
    }
  }

  @Test
  public void querySysdateWithFormat() {
    ResultSet result = database.command("sql", "select sysdate().format('dd-MM-yyyy') as date from Account");

    assertThat(result.hasNext()).isTrue();
    Object lastDate = null;
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();

      final String date = d.getProperty("date");

      assertThat(date).isNotNull();
      assertThat(date.length()).isEqualTo(10);

      if (lastDate != null)
        d.getProperty("date").equals(lastDate);

      lastDate = d.getProperty("date");
    }

    result = database.command("sql", "select sysdate().format('yyyy-MM-dd HH:mm:ss') as date");

    assertThat(result.hasNext()).isTrue();
    lastDate = null;
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();

      final String date = d.getProperty("date");

      assertThat(date).isNotNull();
      assertThat(date.length()).isEqualTo(19);

      if (lastDate != null)
        d.getProperty("date").equals(lastDate);

      lastDate = d.getProperty("date");
    }

    result = database.command("sql", "select sysdate().format('yyyy-MM-dd HH:mm:ss', 'GMT-5') as date");

    assertThat(result.hasNext()).isTrue();
    lastDate = null;
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();

      final String date = d.getProperty("date");

      assertThat(date).isNotNull();
      assertThat(date.length()).isEqualTo(19);

      if (lastDate != null)
        d.getProperty("date").equals(lastDate);

      lastDate = d.getProperty("date");
    }
  }

  @Test
  public void queryDate() throws InterruptedException {

//    System.out.println("dateTimeformat = " + database.getSchema().getDateTimeFormat());
//    System.out.println("database.getSchema().getDateFormat() = " + database.getSchema().getDateFormat());
//
//    System.out.println("DATE_IMPLEMENTATION = " + GlobalConfiguration.DATE_IMPLEMENTATION.getValue());
//    System.out.println("DATE_TIME_IMPLEMENTATION = " + GlobalConfiguration.DATE_TIME_IMPLEMENTATION.getValue());

    ResultSet result = database.command("sql", "select count(*) as tot from Account");
    assertThat(result.hasNext()).isTrue();
    final long tot = result.next().<Long>getProperty("tot");

    database.transaction(() -> {
      final ResultSet result2 = database.command("sql", "update Account set created = date()");
    });
    result = database.command("sql", "select count(*) as tot from Account where created is not null");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("tot")).isEqualTo(tot);

    final String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
//    final String pattern = GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString();

    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(pattern);
    LocalDateTime now = LocalDateTime.now();
    String formattedDate = now.format(timeFormatter);
//    System.out.println("formattedDate = " + formattedDate);
    String query = "select count() as tot from Account where created <= date('" + formattedDate + "', \"" + pattern + "\")";
    result = database.command("sql", query);

    assertThat(result.next().<Long>getProperty("tot")).isEqualTo(tot)
        .withFailMessage("Failed on querying by date with formattedDate=%s pattern=%s", formattedDate, pattern);

  }

  @Test
  public void queryUndefinedFunction() {
    try {
      database.command("sql", "select blaaaa(salary) as max from Account");
      fail("");
    } catch (final CommandExecutionException e) {
      // EXPECTED
    }
  }

  @Test
  public void queryCustomFunction() {
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract("bigger") {
      @Override
      public String getSyntax() {
        return "bigger(<first>, <second>)";
      }

      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        if (params[0] == null || params[1] == null)
          // CHECK BOTH EXPECTED PARAMETERS
          return null;

        if (!(params[0] instanceof Number) || !(params[1] instanceof Number))
          // EXCLUDE IT FROM THE RESULT SET
          return null;

        // USE DOUBLE TO AVOID LOSS OF PRECISION
        final double v1 = ((Number) params[0]).doubleValue();
        final double v2 = ((Number) params[1]).doubleValue();

        return Math.max(v1, v2);
      }
    });

    final ResultSet result = database.command("sql", "select from Account where bigger(id,1000) = 1000");

    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat((Integer) d.getProperty("id") <= 1000).isTrue();
    }

    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().unregister("bigger");
  }

  @Test
  public void queryAsLong() {
    final long moreThanInteger = 1 + (long) Integer.MAX_VALUE;
    final String sql =
        "select numberString.asLong() as value from ( select '" + moreThanInteger + "' as numberString from Account ) limit 1";
    final ResultSet result = database.command("sql", sql);

    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Long>getProperty("value")).isNotNull();
      assertThat(d.getProperty("value") instanceof Long).isTrue();
      assertThat((Long) d.getProperty("value")).isEqualTo(moreThanInteger);
    }
  }

  @Test
  public void testHashMethod() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    final ResultSet result = database.command("sql", "select name, name.hash() as n256, name.hash('sha-512') as n512 from City");

    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      final String name = d.getProperty("name");

      assertThat(d.<String>getProperty("n256")).isEqualTo(SQLMethodHash.createHash(name, "SHA-256"));
      assertThat(d.<String>getProperty("n512")).isEqualTo(SQLMethodHash.createHash(name, "SHA-512"));
    }
  }

  @Test
  public void testFirstFunction() {
    final List<Long> sequence = new ArrayList<>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }
    database.transaction(() -> {
      database.newDocument("V").set("sequence", sequence).save();
      sequence.removeFirst();
      database.newDocument("V").set("sequence", sequence).save();
    });

    final ResultSet result = database.command("sql", "select first(sequence) as first from V where sequence is not null");

    assertThat(result.hasNext()).isTrue();
    assertThat((Long) result.next().getProperty("first")).isEqualTo(0);
    assertThat(result.hasNext()).isTrue();
    assertThat((Long) result.next().getProperty("first")).isEqualTo(1);
  }

  @Test
  public void testFirstAndLastFunctionsWithMultipleValues() {
    database.transaction(() -> {
      database.command("sqlscript",//
          "CREATE DOCUMENT TYPE mytype;\n" +//
              "INSERT INTO mytype SET value = 1;\n" +//
              "INSERT INTO mytype SET value = [1,2,3];\n" +//
              "INSERT INTO mytype SET value = [1];\n" +//
              "INSERT INTO mytype SET value = map(\"a\",1,\"b\",2);");
    });

    final ResultSet result = database.query("sql", "SELECT first(value) as first, last(value) as last FROM mytype");

    final List<Result> array = result.stream().collect(Collectors.toList());

    assertThat(array).hasSize(4);
    for (final Result r : array) {
      assertThat(r.hasProperty("first")).isTrue();
      assertThat(r.<Integer>getProperty("first")).isNotNull();

      assertThat(r.hasProperty("last")).isTrue();
      assertThat(r.<Integer>getProperty("last")).isNotNull();
    }
  }

  @Test
  public void testLastFunction() {
    final List<Long> sequence = new ArrayList<Long>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }

    database.transaction(() -> {
      database.newDocument("V").set("sequence2", sequence).save();
      sequence.removeLast();
      database.newDocument("V").set("sequence2", sequence).save();
    });

    final ResultSet result = database.command("sql", "select last(sequence2) as last from V where sequence2 is not null");

    assertThat(result.hasNext()).isTrue();
    assertThat((Long) result.next().getProperty("last")).isEqualTo(99);
    assertThat(result.hasNext()).isTrue();
    assertThat((Long) result.next().getProperty("last")).isEqualTo(98);
  }

  @Test
  public void querySplit() {
    final String sql = "select v.split('-') as value from ( select '1-2-3' as v ) limit 1";

    final ResultSet result = database.command("sql", sql);

    assertThat(result.hasNext()).isTrue();
    for (final ResultSet it = result; it.hasNext(); ) {
      final Result d = it.next();
      assertThat(d.<Object>getProperty("value")).isNotNull();
      assertThat(d.getProperty("value").getClass().isArray()).isTrue();

      final Object[] array = d.getProperty("value");

      assertThat(array.length).isEqualTo(3);
      assertThat(array[0]).isEqualTo("1");
      assertThat(array[1]).isEqualTo("2");
      assertThat(array[2]).isEqualTo("3");
    }
  }

  @Test
  public void CheckAllFunctions() {
    final DefaultSQLFunctionFactory fFactory = ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory();
    for (String fName : fFactory.getFunctionNames()) {
      final SQLFunction f = fFactory.getFunctionInstance(fName);
      assertThat(f).isNotNull();

      assertThat(f.getName().isEmpty()).isFalse();
      assertThat(f.getSyntax().isEmpty()).isFalse();
    }
  }

}
