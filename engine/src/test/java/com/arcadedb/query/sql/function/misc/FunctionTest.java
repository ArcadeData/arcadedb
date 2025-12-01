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

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

class FunctionTest extends TestHelper {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("V"))
        database.getSchema().createVertexType("V");

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newVertex("V");
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner" + i);

        v.save();
      }
    });
  }

  @Test
  void countFunction() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT count(*) as count FROM V WHERE id < :id", params);

      final AtomicInteger counter = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getIdentity().isPresent()).isFalse();
        assertThat(((Number) record.getProperty("count")).intValue()).isEqualTo(10);
        counter.incrementAndGet();
      }
      assertThat(counter.get()).isEqualTo(1);
    });
  }

  @Test
  void ifEvalFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", "SELECT id, if( eval( 'id > 3' ), 'high', 'low') as value FROM V");

      assertThat(rs.hasNext()).isTrue();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getIdentity().isPresent()).isFalse();

        final Object value = record.getProperty("value");
        if ((Integer) record.getProperty("id") > 3)
          assertThat(value).isEqualTo("high");
        else
          assertThat(value).isEqualTo("low");
      }
    });
  }

  @Test
  void ifFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", "SELECT id, if( ( id > 3 ), 'high', 'low') as value FROM V");

      assertThat(rs.hasNext()).isTrue();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getIdentity().isPresent()).isFalse();

        final Object value = record.getProperty("value");
        if ((Integer) record.getProperty("id") > 3)
          assertThat(value).isEqualTo("high");
        else
          assertThat(value).isEqualTo("low");
      }
    });
  }

  @Test
  void avgFunction() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":id", 10);
      final ResultSet rs = database.command("SQL", "SELECT avg(id) as avg FROM V WHERE id < :id", params);

      final AtomicInteger counter = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getIdentity().isPresent()).isFalse();
        assertThat(((Number) record.getProperty("avg")).intValue()).isEqualTo(4);
        counter.incrementAndGet();
      }
      assertThat(counter.get()).isEqualTo(1);
    });
  }

  @Test
  void maxFunction() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      final ResultSet rs = database.command("SQL", "SELECT max(id) as max FROM V", params);

      final AtomicInteger counter = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getIdentity().isPresent()).isFalse();
        assertThat(((Number) record.getProperty("max")).intValue()).isEqualTo(TOT - 1);
        counter.incrementAndGet();
      }
      assertThat(counter.get()).isEqualTo(1);
    });
  }

  @Test
  void minFunction() {
    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      final ResultSet rs = database.command("SQL", "SELECT min(id) as min FROM V", params);

      final AtomicInteger counter = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();
        assertThat(record.getIdentity().isPresent()).isFalse();
        assertThat(((Number) record.getProperty("min")).intValue()).isEqualTo(0);
        counter.incrementAndGet();
      }
      assertThat(counter.get()).isEqualTo(1);
    });
  }

  @Test
  void allFunctionsHaveSyntax() {
    final SQLQueryEngine sqlEngine = (SQLQueryEngine) database.getQueryEngine("sql");
    for (final String name : sqlEngine.getFunctionFactory().getFunctionNames()) {
      assertThat(sqlEngine.getFunction(name).getName()).isNotNull();
      assertThat(sqlEngine.getFunction(name).getSyntax()).isNotNull();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/1037
   */
  @Test
  void typeOnAggregations() {
    database.transaction(() -> {
      database.command("sqlscript",
          """
              CREATE DOCUMENT TYPE doc;
              INSERT INTO doc (num) VALUES (1), (2), (3), (4);
              """);

      ResultSet rs = database.query("SQL", "SELECT num.type() as type FROM doc");
      assertThat((String) rs.nextIfAvailable().getProperty("type")).isEqualTo("INTEGER");

      rs = database.query("SQL", "SELECT unionall(num) as type FROM doc");
      assertThat((Object) rs.nextIfAvailable().getProperty("type")).isInstanceOf(ArrayList.class);

      rs = database.query("SQL", "SELECT unionall(num).type() as type FROM doc");
      assertThat((String) rs.nextIfAvailable().getProperty("type")).isEqualTo("LIST");

      rs = database.query("SQL", "SELECT unionall(num)[0] as type FROM doc");
      assertThat((Integer) rs.nextIfAvailable().getProperty("type")).isEqualTo(1);

      rs = database.query("SQL", "SELECT max(num).type() as type FROM doc");
      assertThat((String) rs.nextIfAvailable().getProperty("type")).isEqualTo("INTEGER");
    });
  }

  @Test
  void functionDate() {
    database.transaction(() -> {
      ResultSet rs = database.query("SQL", "SELECT date(\"2023-03-04\",\"yyyy-MM-dd\") as date");
      assertThat((Object) rs.nextIfAvailable().getProperty("date")).isNotNull();

      rs = database.query("SQL", "SELECT date(\"2023-03-04\",\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\") as date");
      assertThat((Object) rs.nextIfAvailable().getProperty("date")).isNull();

      rs = database.query("SQL", "SELECT date(\"2023-03-04\",\"yyyy-MM-dd HH:mm:ss\") as date");
      assertThat((Object) rs.nextIfAvailable().getProperty("date")).isNull();

      rs = database.query("SQL", "SELECT date(\"2023-03-04\",\"yyyy-MM-dd'T'HH:mm:ss\") as date");
      assertThat((Object) rs.nextIfAvailable().getProperty("date")).isNull();

      rs = database.query("SQL", "select date(\"2023-03-04 12:12:12\",\"yyyy-MM-dd HH:mm:ss\") as date");
      assertThat((Object) rs.nextIfAvailable().getProperty("date")).isNotNull();
    });
  }
}
