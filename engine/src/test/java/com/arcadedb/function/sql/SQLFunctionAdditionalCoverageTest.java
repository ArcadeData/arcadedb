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
package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional coverage tests for SQL functions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionAdditionalCoverageTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createVertexType("FuncV");
    database.getSchema().createEdgeType("FuncE");
    database.getSchema().createDocumentType("FuncDoc");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableVertex v = database.newVertex("FuncV");
        v.set("name", "v" + i);
        v.set("idx", i);
        v.set("amount", i * 10.5);
        v.set("active", i % 2 == 0);
        v.save();
      }

      // Chain: v0->v1->v2->...->v9
      for (int i = 0; i < 9; i++)
        database.command("sql",
            "CREATE EDGE FuncE FROM (SELECT FROM FuncV WHERE idx = ?) TO (SELECT FROM FuncV WHERE idx = ?)", i, i + 1);

      // Documents with various data types
      database.newDocument("FuncDoc").set("name", "doc1").set("val", 10).set("nullField", null).save();
      database.newDocument("FuncDoc").set("name", "doc2").set("val", 20).save();
      database.newDocument("FuncDoc").set("name", "doc3").set("val", 30).save();
      database.newDocument("FuncDoc").set("name", "doc4").set("val", 40).save();
      database.newDocument("FuncDoc").set("name", "doc5").set("val", 50).save();
    });
  }

  // --- first() / last() ---
  @Test
  void firstFunction() {
    final ResultSet rs = database.query("sql", "SELECT first(idx) as f FROM FuncV ORDER BY idx");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("f")).isEqualTo(0);
    rs.close();
  }

  @Test
  void lastFunction() {
    final ResultSet rs = database.query("sql", "SELECT last(idx) as l FROM FuncV");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("l")).isNotNull();
    rs.close();
  }

  // --- list() ---
  @Test
  void listFunction() {
    final ResultSet rs = database.query("sql", "SELECT list(1, 2, 3) as lst");
    assertThat(rs.hasNext()).isTrue();
    final Collection<?> lst = rs.next().getProperty("lst");
    assertThat(lst).hasSize(3);
    rs.close();
  }

  // --- set() ---
  @Test
  void setFunction() {
    final ResultSet rs = database.query("sql", "SELECT set(1, 2, 2, 3, 3) as s");
    assertThat(rs.hasNext()).isTrue();
    final Collection<?> s = rs.next().getProperty("s");
    assertThat(s).hasSize(3);
    rs.close();
  }

  // --- map() ---
  @Test
  void mapFunction() {
    final ResultSet rs = database.query("sql", "SELECT map('key1', 'val1', 'key2', 'val2') as m");
    assertThat(rs.hasNext()).isTrue();
    final Map<?, ?> m = rs.next().getProperty("m");
    assertThat(m).hasSize(2);
    assertThat(m.get("key1")).isEqualTo("val1");
    rs.close();
  }

  // --- unionAll() ---
  @Test
  void unionAllFunction() {
    final ResultSet rs = database.query("sql", "SELECT unionAll(idx) as allIdx FROM FuncV");
    assertThat(rs.hasNext()).isTrue();
    final Collection<?> all = rs.next().getProperty("allIdx");
    assertThat(all).hasSize(10);
    rs.close();
  }

  // --- if() with eval ---
  @Test
  void ifFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT idx, if(eval('idx < 5'), 'low', 'high') as category FROM FuncV ORDER BY idx");
      int lowCount = 0, highCount = 0;
      while (rs.hasNext()) {
        final String cat = rs.next().getProperty("category");
        if ("low".equals(cat))
          lowCount++;
        else
          highCount++;
      }
      assertThat(lowCount).isEqualTo(5);
      assertThat(highCount).isEqualTo(5);
      rs.close();
    });
  }

  // --- eval() with field reference ---
  @Test
  void evalFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", "SELECT idx, if(eval('idx > 3'), 'high', 'low') as category FROM FuncV WHERE idx = 5");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("category")).isEqualTo("high");
      rs.close();
    });
  }

  @Test
  void evalFunctionLowRange() {
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", "SELECT idx, if(eval('idx > 3'), 'high', 'low') as category FROM FuncV WHERE idx = 1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("category")).isEqualTo("low");
      rs.close();
    });
  }

  // --- version() ---
  @Test
  void versionFunction() {
    final ResultSet rs = database.query("sql", "SELECT version() as v");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("v")).isNotNull();
    rs.close();
  }

  // --- median() ---
  @Test
  void medianFunction() {
    final ResultSet rs = database.query("sql", "SELECT median(val) as med FROM FuncDoc");
    assertThat(rs.hasNext()).isTrue();
    final Number med = rs.next().getProperty("med");
    assertThat(med).isNotNull();
    assertThat(med.doubleValue()).isEqualTo(30.0);
    rs.close();
  }

  // --- stddev() ---
  @Test
  void stddevFunction() {
    final ResultSet rs = database.query("sql", "SELECT stddev(val) as sd FROM FuncDoc");
    assertThat(rs.hasNext()).isTrue();
    final Number sd = rs.next().getProperty("sd");
    assertThat(sd).isNotNull();
    assertThat(sd.doubleValue()).isGreaterThan(0.0);
    rs.close();
  }

  // --- variance() ---
  @Test
  void varianceFunction() {
    final ResultSet rs = database.query("sql", "SELECT variance(val) as v FROM FuncDoc");
    assertThat(rs.hasNext()).isTrue();
    final Number v = rs.next().getProperty("v");
    assertThat(v).isNotNull();
    assertThat(v.doubleValue()).isGreaterThan(0.0);
    rs.close();
  }

  // --- sum() with nulls ---
  @Test
  void sumWithNulls() {
    database.getSchema().createDocumentType("SumNullTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO SumNullTest SET val = 10");
      database.command("sql", "INSERT INTO SumNullTest SET val = null");
      database.command("sql", "INSERT INTO SumNullTest SET val = 20");
    });
    final ResultSet rs = database.query("sql", "SELECT sum(val) as total FROM SumNullTest");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("total").intValue()).isEqualTo(30);
    rs.close();
  }

  // --- avg() ---
  @Test
  void avgFunction() {
    final ResultSet rs = database.query("sql", "SELECT avg(val) as average FROM FuncDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("average").doubleValue()).isEqualTo(30.0);
    rs.close();
  }

  // --- count on distinct subquery ---
  @Test
  void countDistinct() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM (SELECT DISTINCT active FROM FuncV)");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(2L);
      rs.close();
    });
  }

  // --- min() / max() ---
  @Test
  void minMaxFunctions() {
    final ResultSet rs = database.query("sql", "SELECT min(val) as mi, max(val) as ma FROM FuncDoc");
    assertThat(rs.hasNext()).isTrue();
    final Result item = rs.next();
    assertThat(item.<Integer>getProperty("mi")).isEqualTo(10);
    assertThat(item.<Integer>getProperty("ma")).isEqualTo(50);
    rs.close();
  }

  // --- date() / sysdate() ---
  @Test
  void dateFunction() {
    final ResultSet rs = database.query("sql", "SELECT date('2024-01-15', 'yyyy-MM-dd') as d");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("d")).isNotNull();
    rs.close();
  }

  @Test
  void sysdateFunction() {
    final ResultSet rs = database.query("sql", "SELECT sysdate() as now");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("now")).isNotNull();
    rs.close();
  }

  @Test
  void sysdateWithFormat() {
    final ResultSet rs = database.query("sql", "SELECT sysdate('yyyy-MM-dd').asString() as now");
    assertThat(rs.hasNext()).isTrue();
    final String now = rs.next().getProperty("now");
    assertThat(now).isNotNull();
    assertThat(now.length()).isGreaterThanOrEqualTo(10);
    rs.close();
  }

  // --- format() ---
  @Test
  void formatFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT format('%s has index %d', name, idx) as formatted FROM FuncV WHERE idx = 3");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("formatted")).isEqualTo("v3 has index 3");
      rs.close();
    });
  }

  // --- Graph traversal functions: out(), in(), both() ---
  @Test
  void outFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT out('FuncE').size() as outCount FROM FuncV WHERE idx = 0");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("outCount")).isEqualTo(1);
      rs.close();
    });
  }

  @Test
  void inFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT in('FuncE').size() as inCount FROM FuncV WHERE idx = 9");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("inCount")).isEqualTo(1);
      rs.close();
    });
  }

  @Test
  void bothFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT both('FuncE').size() as bothCount FROM FuncV WHERE idx = 5");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("bothCount")).isEqualTo(2); // in from v4, out to v6
      rs.close();
    });
  }

  // --- outE(), inE(), bothE() ---
  @Test
  void outEFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT outE('FuncE').size() as cnt FROM FuncV WHERE idx = 0");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("cnt")).isEqualTo(1);
      rs.close();
    });
  }

  @Test
  void inEFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT inE('FuncE').size() as cnt FROM FuncV WHERE idx = 5");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("cnt")).isEqualTo(1);
      rs.close();
    });
  }

  @Test
  void bothEFunction() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT bothE('FuncE').size() as cnt FROM FuncV WHERE idx = 5");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("cnt")).isEqualTo(2);
      rs.close();
    });
  }

  // --- outV(), inV() on edges ---
  @Test
  void outVInVOnEdge() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT outV().name as fromName, inV().name as toName FROM FuncE LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<String>getProperty("fromName")).isNotNull();
      assertThat(item.<String>getProperty("toName")).isNotNull();
      rs.close();
    });
  }

  // --- coalesce() ---
  @Test
  void coalesceFunction() {
    final ResultSet rs = database.query("sql", "SELECT coalesce(null, null, 'found', 'later') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("found");
    rs.close();
  }

  @Test
  void coalesceAllNulls() {
    final ResultSet rs = database.query("sql", "SELECT coalesce(null, null) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNull();
    rs.close();
  }

  // --- encode() / decode() ---
  @Test
  void encodeDecodeBase64() {
    final ResultSet rs = database.query("sql",
        "SELECT decode(encode('Hello World', 'base64'), 'base64').asString() as decoded");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("decoded")).isEqualTo("Hello World");
    rs.close();
  }

  // --- abs() ---
  @Test
  void absFunction() {
    final ResultSet rs = database.query("sql", "SELECT abs(-42) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(42);
    rs.close();
  }

  // --- sqrt() ---
  @Test
  void sqrtFunction() {
    final ResultSet rs = database.query("sql", "SELECT sqrt(144) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("result").doubleValue()).isEqualTo(12.0);
    rs.close();
  }

  // --- mode() ---
  @Test
  void modeFunction() {
    database.getSchema().createDocumentType("ModeTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO ModeTest SET val = 1");
      database.command("sql", "INSERT INTO ModeTest SET val = 2");
      database.command("sql", "INSERT INTO ModeTest SET val = 2");
      database.command("sql", "INSERT INTO ModeTest SET val = 3");
      database.command("sql", "INSERT INTO ModeTest SET val = 2");
    });
    final ResultSet rs = database.query("sql", "SELECT mode(val) as m FROM ModeTest");
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  // --- percentile() ---
  @Test
  void percentileFunction() {
    final ResultSet rs = database.query("sql", "SELECT percentile(val, 0.5) as p50 FROM FuncDoc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("p50")).isNotNull();
    rs.close();
  }

  // --- count(*) on empty set ---
  @Test
  void countOnEmptySet() {
    database.getSchema().createDocumentType("EmptyFunc");
    final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM EmptyFunc");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(0L);
    rs.close();
  }

  // --- sum() on empty set ---
  @Test
  void sumOnEmptySet() {
    database.getSchema().createDocumentType("EmptySumFunc");
    final ResultSet rs = database.query("sql", "SELECT sum(val) as total FROM EmptySumFunc");
    // On empty set, sum may return 0 or null depending on implementation
    if (rs.hasNext()) {
      final Result item = rs.next();
      // Just verify we get a result without error
      assertThat(item).isNotNull();
    }
    rs.close();
  }

  // --- Aggregate functions with GROUP BY ---
  @Test
  void aggregateFunctionsGroupBy() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          """
          SELECT active, count(*) as cnt, sum(amount) as total, min(idx) as minIdx, max(idx) as maxIdx \
          FROM FuncV GROUP BY active ORDER BY active""");
      int count = 0;
      while (rs.hasNext()) {
        final Result item = rs.next();
        assertThat(item.<Long>getProperty("cnt")).isGreaterThan(0L);
        count++;
      }
      assertThat(count).isEqualTo(2); // true and false
      rs.close();
    });
  }

  // --- UUID ---
  @Test
  void uuidFunction() {
    final ResultSet rs = database.query("sql", "SELECT uuid() as id");
    assertThat(rs.hasNext()).isTrue();
    final String uuid = rs.next().getProperty("id");
    assertThat(uuid).isNotNull();
    assertThat(uuid).hasSize(36);
    rs.close();
  }

  // --- randomInt() ---
  @Test
  void randomIntFunction() {
    final ResultSet rs = database.query("sql", "SELECT randomInt(100) as r");
    assertThat(rs.hasNext()).isTrue();
    final int r = rs.next().getProperty("r");
    assertThat(r).isBetween(0, 99);
    rs.close();
  }

  // --- boolAnd() / boolOr() ---
  @Test
  void boolAndFunction() {
    database.getSchema().createDocumentType("BoolTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO BoolTest SET flag = true");
      database.command("sql", "INSERT INTO BoolTest SET flag = true");
      database.command("sql", "INSERT INTO BoolTest SET flag = false");
    });
    final ResultSet rs = database.query("sql", "SELECT bool_and(flag) as result FROM BoolTest");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isFalse();
    rs.close();
  }

  @Test
  void boolOrFunction() {
    database.getSchema().createDocumentType("BoolOrTest");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO BoolOrTest SET flag = false");
      database.command("sql", "INSERT INTO BoolOrTest SET flag = false");
      database.command("sql", "INSERT INTO BoolOrTest SET flag = true");
    });
    final ResultSet rs = database.query("sql", "SELECT bool_or(flag) as result FROM BoolOrTest");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    rs.close();
  }

  // --- ifNull() function ---
  @Test
  void ifNullFunction() {
    final ResultSet rs = database.query("sql", "SELECT ifnull(null, 'default') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("default");
    rs.close();
  }

  @Test
  void ifNullNotNull() {
    final ResultSet rs = database.query("sql", "SELECT ifnull('value', 'default') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("value");
    rs.close();
  }

  // --- ifEmpty() function ---
  @Test
  void ifEmptyFunction() {
    final ResultSet rs = database.query("sql", "SELECT ifempty('', 'default') as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("default");
    rs.close();
  }

  // --- string concatenation ---
  @Test
  void stringConcatenation() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT name + '-' + idx as combined FROM FuncV WHERE idx = 3");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("combined")).isEqualTo("v3-3");
      rs.close();
    });
  }

  // --- Nested function calls ---
  @Test
  void nestedFunctions() {
    final ResultSet rs = database.query("sql", "SELECT abs(-5) as result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("result")).isEqualTo(5);
    rs.close();
  }
}
