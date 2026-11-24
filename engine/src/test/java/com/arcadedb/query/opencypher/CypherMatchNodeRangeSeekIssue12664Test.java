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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The MATCH NODE of the traditional step plan, which runs the statements the cost-based optimizer does not (a MATCH after a WITH, an
 * UNWIND or an OPTIONAL MATCH), sought an index for an equality, an IN list and the prefix of a composite key only: a range or a
 * STARTS WITH on an indexed property scanned the whole label, once per input row (issue #12664).
 * <p>
 * The step checks every candidate against the WHERE, so the range it reads only has to hold every vertex the walk of the label
 * answers. Every statement here is therefore run on a type with the index and on an identical type without it, and must answer
 * the same: over the operand kinds a caller can bind, bounds only a row can tell, an index inherited from a supertype, a composite
 * key, a range too large for the index, and statements that write what they read.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherMatchNodeRangeSeekIssue12664Test {
  private static final String PATH = "target/databases/CypherMatchNodeRangeSeekIssue12664Test";
  private static final int    ROWS = 1000;

  /** One row per type, holding what each property type holds. */
  private static final Map<String, Object> STORED   = new LinkedHashMap<>();
  /** What a caller can bind: every kind a property type can or cannot hold. */
  private static final Map<String, Object> OPERANDS = new LinkedHashMap<>();

  static {
    STORED.put("BOOLEAN", true);
    STORED.put("INTEGER", 7);
    STORED.put("LONG", 7L);
    STORED.put("DOUBLE", 1.5);
    STORED.put("DECIMAL", new BigDecimal("1.5"));
    STORED.put("STRING", "abc");
    STORED.put("DATE", LocalDate.of(2020, 1, 1));
    STORED.put("DATETIME", LocalDateTime.of(2020, 1, 1, 0, 0));

    OPERANDS.put("String", "zzz");
    OPERANDS.put("Prefix", "ab");
    OPERANDS.put("NumericString", "5");
    OPERANDS.put("Integer", 7);
    OPERANDS.put("SmallInteger", 3);
    OPERANDS.put("Long", 7L);
    OPERANDS.put("Double", 1.5);
    OPERANDS.put("Fraction", 6.5);
    OPERANDS.put("NaN", Double.NaN);
    OPERANDS.put("BigDecimal", new BigDecimal("1.5"));
    OPERANDS.put("Boolean", true);
    OPERANDS.put("False", false);
    OPERANDS.put("LocalDate", LocalDate.of(2020, 1, 1));
    OPERANDS.put("EarlierDate", LocalDate.of(2019, 1, 1));
    OPERANDS.put("LocalDateTime", LocalDateTime.of(2020, 1, 1, 0, 0));
    OPERANDS.put("List", List.of(7));
    OPERANDS.put("Map", Map.of("a", 7));
  }

  /** Statements of the step plan, {@code %s} the type, {@code $x} the operand and {@code $v} what the property holds. */
  private static final List<String> SHAPES = List.of(
      "WITH $x AS x MATCH (n:%s) WHERE n.p > x RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE n.p >= x RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE n.p < x RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE n.p <= x RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE x < n.p RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE x >= n.p RETURN n.id AS id",
      "UNWIND [$x, $v] AS x MATCH (n:%s) WHERE n.p >= x RETURN n.id AS id",
      "UNWIND [$x, null, $v] AS x MATCH (n:%s) WHERE n.p <= x RETURN n.id AS id",
      "UNWIND [1, 2] AS i MATCH (n:%s) WHERE n.p > $x RETURN n.id AS id",
      "UNWIND [1, 2] AS i MATCH (n:%s) WHERE n.p <= $x RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE n.p >= x AND n.p <= $v RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE n.p >= $v AND n.p <= x RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE n.p > x AND n.p < x RETURN n.id AS id",
      "UNWIND [1] AS i OPTIONAL MATCH (n:%s) WHERE n.p >= $x RETURN n.id AS id",
      "UNWIND [1] AS i OPTIONAL MATCH (n:%s) WHERE n.p < $x RETURN n.id AS id",
      "WITH $x AS x MATCH (n:%s) WHERE n.p STARTS WITH x RETURN n.id AS id",
      "UNWIND [$x, $v] AS x MATCH (n:%s) WHERE n.p STARTS WITH x RETURN n.id AS id",
      "UNWIND [1, 2] AS i MATCH (n:%s) WHERE n.p STARTS WITH $x RETURN n.id AS id");

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aRangeAfterAWithAnUnwindOrAnOptionalMatchSeeksTheIndex() {
    createNumbered();

    final Map<String, Object> parameters = Map.of("lo", ROWS - 9, "hi", 5, "p", "c19");
    for (final String statement : new String[] {
        "WITH $lo AS lo MATCH (n:P) WHERE n.id > lo RETURN n.id AS id",
        "UNWIND [1, 2, 3] AS x MATCH (n:P) WHERE n.id > $lo RETURN n.id AS id",
        "UNWIND [1, 2, 3] AS x MATCH (n:P) WHERE n.id <= x RETURN n.id AS id",
        "MATCH (a:P {id: 5}) OPTIONAL MATCH (n:P) WHERE n.id > $lo RETURN n.id AS id",
        "WITH $lo AS lo MATCH (n:P) WHERE n.id >= lo AND n.id < lo + 3 RETURN n.id AS id",
        "WITH $lo AS lo MATCH (n:P) WHERE lo < n.id RETURN n.id AS id" })
      assertThat(profile(statement, parameters)).as(statement).contains("MATCH NODE (n:P) [index: P[id], range");

    for (final String statement : new String[] {
        "UNWIND ['c19'] AS p MATCH (n:P) WHERE n.city STARTS WITH p RETURN n.id AS id",
        "WITH 1 AS one MATCH (n:P) WHERE n.city STARTS WITH $p RETURN n.id AS id" })
      assertThat(profile(statement, parameters)).as(statement).contains("MATCH NODE (n:P) [index: P[city], range");

    // the leading column of a composite key, and an index declared on a supertype
    assertThat(profile("WITH $hi AS hi MATCH (n:Pair) WHERE n.a < hi RETURN n.id AS id", parameters))
        .contains("MATCH NODE (n:Pair) [index: Pair[a, b], range: a");
    assertThat(profile("WITH $lo AS lo MATCH (n:Sub) WHERE n.id > lo RETURN n.id AS id", parameters))
        .contains("MATCH NODE (n:Sub) [index: Base[id], range");

    // a range of more entries than are read whole is still served by the index, in physical order...
    assertThat(profile("WITH 300 AS hi MATCH (n:P) WHERE n.id <= hi RETURN n.id AS id", parameters))
        .contains("MATCH NODE (n:P) [index: P[id], range");
    // ...until it holds most of the label, which the walk serves better
    assertThat(profile("WITH 10 AS lo MATCH (n:P) WHERE n.id > lo RETURN n.id AS id", parameters))
        .contains("MATCH NODE (n:P) [filter:");
    // a hash index has no order to read a range in
    database.command("sql", "CREATE INDEX ON N (id) UNIQUE_HASH").close();
    assertThat(profile("WITH $lo AS lo MATCH (n:N) WHERE n.id > lo RETURN n.id AS id", parameters))
        .contains("MATCH NODE (n:N) [filter:");
  }

  @Test
  void theRowsOfARangeSeekAreTheRowsOfTheWalk() {
    createNumbered();

    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("lo", ROWS - 9);
    parameters.put("hi", 5);
    parameters.put("p", "c19");
    parameters.put("none", null);
    final List<String> differences = new ArrayList<>();
    for (final String statement : new String[] {
        "WITH $lo AS lo MATCH (n:%s) WHERE n.id > lo RETURN n.id AS id",
        "UNWIND [1, 2, 3] AS x MATCH (n:%s) WHERE n.id > $lo RETURN n.id AS id",
        "UNWIND [1, 2, 3, null, 2.5, 'a', 5000, -1] AS x MATCH (n:%s) WHERE n.id <= x RETURN n.id AS id",
        "UNWIND [1, 2, 3, null, 2.5, 'a', 5000, -1] AS x MATCH (n:%s) WHERE n.id > x AND n.id < x + 4 RETURN n.id AS id",
        "MATCH (a:P {id: 5}) OPTIONAL MATCH (n:%s) WHERE n.id > $lo RETURN n.id AS id",
        "MATCH (a:P {id: 5}) OPTIONAL MATCH (n:%s) WHERE n.id > 100000 RETURN n.id AS id",
        "WITH $none AS lo MATCH (n:%s) WHERE n.id > lo RETURN n.id AS id",
        "WITH 11.5 AS lo, 11.9 AS hi MATCH (n:%s) WHERE n.id > lo AND n.id < hi RETURN n.id AS id",
        "WITH 11.5 AS lo, 13.2 AS hi MATCH (n:%s) WHERE n.id >= lo AND n.id <= hi RETURN n.id AS id",
        "WITH 1e19 AS lo MATCH (n:%s) WHERE n.id < lo RETURN n.id AS id",
        "WITH 1e19 AS lo MATCH (n:%s) WHERE n.id > lo RETURN n.id AS id",
        // a range of more entries than are read whole, and one that holds most of the label
        "WITH 300 AS lo MATCH (n:%s) WHERE n.id <= lo RETURN n.id AS id",
        "UNWIND [300, 400] AS lo MATCH (n:%s) WHERE n.id <= lo RETURN n.id AS id",
        "WITH 10 AS lo MATCH (n:%s) WHERE n.id > lo RETURN n.id AS id",
        "UNWIND [10, 20, 990] AS lo MATCH (n:%s) WHERE n.id > lo RETURN n.id AS id",
        // the other predicates of the WHERE still decide every row
        "WITH 300 AS hi MATCH (n:%s) WHERE n.id <= hi AND n.city = 'c7' RETURN n.id AS id",
        "WITH 300 AS hi MATCH (n:%s {city: 'c7'}) WHERE n.id <= hi RETURN n.id AS id",
        "WITH 300 AS hi MATCH (n:%s) WHERE n.id <= hi OR n.city = 'c7' RETURN n.id AS id",
        "WITH 300 AS hi MATCH (n:%s) WHERE NOT (n.id <= hi) RETURN n.id AS id",
        "WITH 30 AS hi MATCH (n:%s) WHERE n.id <= hi AND n.id <= n.other RETURN n.id AS id",
        "UNWIND ['c19', 'c1', '', 'zz', null, 7] AS p MATCH (n:%s) WHERE n.city STARTS WITH p RETURN n.id AS id",
        "UNWIND ['c19', 'c5'] AS p MATCH (n:%s) WHERE n.city >= p AND n.city < p + '5' RETURN n.id AS id",
        "WITH 1 AS one MATCH (n:%s) WHERE n.city STARTS WITH $p RETURN n.id AS id",
        "UNWIND [1, 2] AS one MATCH (n:%s) WHERE n.id > $lo RETURN n.id AS id LIMIT 3" }) {
      final String indexed = run(String.format(statement, "P"), parameters);
      final String scanned = run(String.format(statement, "N"), parameters);
      if (!indexed.equals(scanned))
        differences.add(statement + " | " + indexed + " | " + scanned);
    }
    // a composite key sought by its leading column, and an index declared on a supertype
    for (final String[] pair : new String[][] {
        { "UNWIND [5, 300, 2000] AS hi MATCH (n:Pair) WHERE n.a < hi RETURN n.id AS id",
            "UNWIND [5, 300, 2000] AS hi MATCH (n:N) WHERE n.id < hi RETURN n.id AS id" },
        { "UNWIND [5, 300] AS lo MATCH (n:Pair) WHERE n.a > lo AND n.b = 1 RETURN n.id AS id",
            "UNWIND [5, 300] AS lo MATCH (n:N) WHERE n.id > lo AND n.id %% 7 = 1 RETURN n.id AS id" },
        { "UNWIND [5, 995] AS lo MATCH (n:Sub) WHERE n.id > lo RETURN n.id AS id",
            "UNWIND [5, 995] AS lo MATCH (n:N) WHERE n.id > lo AND n.id %% 2 = 0 RETURN n.id AS id" },
        { "UNWIND [5, 995] AS lo MATCH (n:Base) WHERE n.id > lo RETURN n.id AS id",
            "UNWIND [5, 995] AS lo MATCH (n:N) WHERE n.id > lo RETURN n.id AS id" } }) {
      final String indexed = run(pair[0], parameters);
      final String scanned = run(String.format(pair[1]), parameters);
      if (!indexed.equals(scanned))
        differences.add(pair[0] + " | " + indexed + " | " + scanned);
    }
    assertThat(differences).as("statement | with the index | without the index").isEmpty();
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "NOTUNIQUE", "UNIQUE", "UNIQUE_HASH", "NOTUNIQUE NULL_STRATEGY INDEX" })
  void everyOperandKindAnswersWithTheIndexAsWithoutIt(final String indexKind) {
    for (final Map.Entry<String, Object> stored : STORED.entrySet()) {
      final String property = stored.getKey();
      for (final String prefix : new String[] { "I_", "N_" }) {
        final String type = prefix + property;
        database.command("sql", "CREATE VERTEX TYPE " + type).close();
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
        database.command("sql", "CREATE PROPERTY " + type + ".p " + property).close();
      }
      database.command("sql", "CREATE INDEX ON I_" + property + " (p) " + indexKind).close();
      database.transaction(() -> {
        for (final String prefix : new String[] { "I_", "N_" }) {
          database.newVertex(prefix + property).set("id", 1).set("p", stored.getValue()).save();
          // a vertex with no value for the key answers no range
          database.newVertex(prefix + property).set("id", 2).save();
        }
      });
    }

    final List<String> differences = new ArrayList<>();
    for (final Map.Entry<String, Object> stored : STORED.entrySet())
      for (final Map.Entry<String, Object> operand : OPERANDS.entrySet()) {
        final Map<String, Object> parameters = Map.of("x", operand.getValue(), "v", stored.getValue());
        for (final String shape : SHAPES) {
          final String indexed = run(String.format(shape, "I_" + stored.getKey()), parameters);
          final String scanned = run(String.format(shape, "N_" + stored.getKey()), parameters);
          if (!indexed.equals(scanned))
            differences.add(stored.getKey() + " | " + shape + " | " + operand.getKey() + " | " + indexed + " | " + scanned);
        }
      }
    assertThat(differences).as("cases where the indexed type and the one without the index answer differently on %s "
        + "(property | statement | operand | with the index | without the index)", indexKind).isEmpty();

    // the differential is worth something only if the indexed type is read through its index
    if (!indexKind.equals("UNIQUE_HASH"))
      for (final String property : STORED.keySet())
        assertThat(profile("WITH $v AS x MATCH (n:I_" + property + ") WHERE n.p >= x RETURN n.id AS id",
            Map.of("v", STORED.get(property)))).as("plan on %s", property).contains("[index: I_" + property + "[p], range");
  }

  /**
   * A bound the key type rounds would start or end the range one key off: {@code > 0.1000000001} sought as {@code > 0.1f} misses
   * the vertex that holds 0.1f. Such a bound is left to the walk.
   */
  @Test
  void aBoundTheKeyTypeRoundsIsNotSought() {
    final Map<String, Object> stored = new LinkedHashMap<>();
    stored.put("FLOAT", 0.1f);
    stored.put("DOUBLE", 0.1d);
    stored.put("DECIMAL", new BigDecimal("0.1"));
    for (final Map.Entry<String, Object> entry : stored.entrySet())
      for (final String prefix : new String[] { "I_", "N_" }) {
        final String type = prefix + entry.getKey();
        database.command("sql", "CREATE VERTEX TYPE " + type).close();
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
        database.command("sql", "CREATE PROPERTY " + type + ".p " + entry.getKey()).close();
        if (prefix.equals("I_"))
          database.command("sql", "CREATE INDEX ON " + type + " (p) NOTUNIQUE").close();
        database.transaction(() -> database.newVertex(type).set("id", 1).set("p", entry.getValue()).save());
      }

    final List<Object> operands = List.of(0.1000000001d, 0.0999999999d, 0.1d, 0.1f, (double) 0.1f, Math.nextUp(0.1d),
        Math.nextDown(0.1d), Math.nextUp(0.1f), Math.nextDown(0.1f), new BigDecimal("0.1"), new BigDecimal("0.1000000000000000001"),
        new BigDecimal("0.0999999999999999999"), new BigDecimal(0.1d), new BigDecimal(0.1f), 0.5d, 0.5f, 1e300, 1);
    final List<String> differences = new ArrayList<>();
    for (final String property : stored.keySet())
      for (final Object operand : operands)
        for (final String operator : new String[] { ">", ">=", "<", "<=" }) {
          final String statement = "WITH $x AS x MATCH (n:%s) WHERE n.p " + operator + " x RETURN n.id AS id";
          final String indexed = run(String.format(statement, "I_" + property), Map.of("x", operand));
          final String scanned = run(String.format(statement, "N_" + property), Map.of("x", operand));
          if (!indexed.equals(scanned))
            differences.add(property + " " + operator + " " + operand.getClass().getSimpleName() + " " + operand + " | " + indexed
                + " | " + scanned);
        }
    assertThat(differences).as("property, operator and operand | with the index | without the index").isEmpty();
  }

  /**
   * The walk compares a floating point bound with a key of another floating point type in its own way: a DECIMAL against a double
   * in double precision, a DOUBLE against a float as the float it narrows to, and an infinity against a DECIMAL that no key of the
   * index can be built from. The index orders the keys exactly, so those bounds are left to the walk too.
   */
  @Test
  void aFloatingPointBoundOfAnotherTypeThanTheKeyAnswersAsTheWalk() {
    final Map<String, List<Object>> stored = new LinkedHashMap<>();
    stored.put("FLOAT", List.of(0.1f, 0.5f, Math.nextUp(0.1f), Math.nextDown(0.5f)));
    stored.put("DOUBLE", List.of(0.1d, 0.0999999999d, 0.1000000001d, (double) 0.1f, 0.5d, Math.nextUp(0.5d), Math.nextDown(0.5d)));
    stored.put("DECIMAL", List.of(new BigDecimal("0.1"), new BigDecimal("1"), new BigDecimal("0.5"),
        new BigDecimal("0.50000000000000000001"), new BigDecimal("0.49999999999999999999")));
    for (final Map.Entry<String, List<Object>> entry : stored.entrySet())
      for (final String prefix : new String[] { "I_", "N_" }) {
        final String type = prefix + entry.getKey();
        database.command("sql", "CREATE VERTEX TYPE " + type).close();
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
        database.command("sql", "CREATE PROPERTY " + type + ".p " + entry.getKey()).close();
        if (prefix.equals("I_"))
          database.command("sql", "CREATE INDEX ON " + type + " (p) NOTUNIQUE").close();
        database.transaction(() -> {
          for (int i = 0; i < entry.getValue().size(); i++)
            database.newVertex(type).set("id", i).set("p", entry.getValue().get(i)).save();
        });
      }

    final List<Object> operands = List.of(0.1d, 0.1f, 0.5d, 0.5f, (double) 0.1f, 0.0999999999d, new BigDecimal("0.5"),
        new BigDecimal("0.1"), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 1);
    final List<String> differences = new ArrayList<>();
    for (final String property : stored.keySet())
      for (final Object operand : operands)
        for (final String operator : new String[] { ">", ">=", "<", "<=" }) {
          final String statement = "WITH $x AS x MATCH (n:%s) WHERE n.p " + operator + " x RETURN n.id AS id";
          final String indexed = run(String.format(statement, "I_" + property), Map.of("x", operand));
          final String scanned = run(String.format(statement, "N_" + property), Map.of("x", operand));
          if (!indexed.equals(scanned))
            differences.add(property + " " + operator + " " + operand.getClass().getSimpleName() + " " + operand + " | " + indexed
                + " | " + scanned);
        }
    assertThat(differences).as("property, operator and operand | with the index | without the index").isEmpty();
  }

  @Test
  void aStatementThatWritesWhatItReadsMeetsEveryVertexOnce() {
    createNumbered();

    // the key the range reads is moved ahead of the cursor: a walk of the index would meet the vertex again
    for (final String type : new String[] { "P", "N" })
      database.transaction(() -> database.command("opencypher",
          "WITH 990 AS lo MATCH (n:" + type + ") WHERE n.id > lo SET n.id = n.id + 5 RETURN n.id AS id").close());
    assertThat(run("MATCH (n:P) WHERE n.id > 990 RETURN n.id AS id", Map.of()))
        .isEqualTo(run("MATCH (n:N) WHERE n.id > 990 RETURN n.id AS id", Map.of()));
    assertThat(run("MATCH (n:P) WHERE n.id > 990 RETURN n.id AS id", Map.of())).contains("1005").doesNotContain("1006");

    // more vertices than the seek reads whole
    for (final String type : new String[] { "P", "N" })
      database.transaction(() -> database.command("opencypher",
          "WITH 600 AS lo MATCH (n:" + type + ") WHERE n.id > lo SET n.id = n.id + 2000 RETURN n.id AS id").close());
    assertThat(run("MATCH (n:P) RETURN n.id AS id", Map.of())).isEqualTo(run("MATCH (n:N) RETURN n.id AS id", Map.of()));
    assertThat(run("MATCH (n:P) WHERE n.id > 3000 RETURN n.id AS id", Map.of())).isEqualTo("rows [3001, 3002, 3003, 3004, 3005]");

    // a vertex created for one row is not a candidate of the next (issue #6602)
    for (final String type : new String[] { "P", "N" })
      database.transaction(() -> database.command("opencypher",
          "UNWIND [1, 2, 3] AS x MATCH (n:" + type + ") WHERE n.id > 3003 CREATE (:" + type + " {id: n.id + 100 * x, city: 'new'})")
          .close());
    assertThat(run("MATCH (n:P) WHERE n.city = 'new' RETURN n.id AS id", Map.of()))
        .isEqualTo(run("MATCH (n:N) WHERE n.city = 'new' RETURN n.id AS id", Map.of()));
    for (final String type : new String[] { "P", "N" })
      database.transaction(() -> database.command("opencypher",
          "UNWIND [3100, 3101] AS x MATCH (n:" + type + ") WHERE n.id > x CREATE (:" + type + " {id: n.id * 10 + x - 3100, city: 'newer'})")
          .close());
    assertThat(run("MATCH (n:P) WHERE n.city = 'newer' RETURN n.id AS id", Map.of()))
        .isEqualTo(run("MATCH (n:N) WHERE n.city = 'newer' RETURN n.id AS id", Map.of()));
  }

  @Test
  void aTypeTheUserMayNotReadRefusesTheRangeWhateverTheBoundsAre() {
    createNumbered();
    deny("P", "Pair", "Sub");

    final List<String> notRefused = new ArrayList<>();
    for (final String statement : new String[] {
        "WITH $v AS lo MATCH (n:P) WHERE n.id > lo RETURN n.id AS id",
        "UNWIND [$v] AS lo MATCH (n:P) WHERE n.id >= lo AND n.id < lo + 2 RETURN n.id AS id",
        "UNWIND [1, 2] AS x MATCH (n:P) WHERE n.id > $v RETURN n.id AS id",
        "MATCH (a:N {id: 5}) OPTIONAL MATCH (n:P) WHERE n.id > $v RETURN n.id AS id",
        "WITH toString($v) AS p MATCH (n:P) WHERE n.city STARTS WITH p RETURN n.id AS id",
        "WITH $v AS lo MATCH (n:Pair) WHERE n.a > lo RETURN n.id AS id",
        // the index of Base holds the vertices of Sub, and the walk of Base opens the buckets of Sub
        "WITH $v AS lo MATCH (n:Base) WHERE n.id > lo RETURN n.id AS id" })
      // bounds that hold vertices of the denied type, bounds that hold none, and one no key can be compared with
      for (final Object value : new Object[] { 3, ROWS * 10, -5, "a", 2.5 })
        if (!(catchThrowable(() -> run(statement, Map.of("v", value), false)) instanceof SecurityException))
          notRefused.add(statement + " with $v = " + value);
    assertThat(notRefused).as("a denied type refuses the statement, it does not tell what its keys are").isEmpty();

    // a type the user may read is answered
    assertThat(run("WITH 995 AS lo MATCH (n:N) WHERE n.id > lo RETURN n.id AS id", Map.of())).isEqualTo("rows [1000, 996, 997, 998, 999]");
  }

  /**
   * P and N hold the same vertices, P with an index on id and one on city; Pair holds them under a composite key, and Base and its
   * subtype Sub share them under an index declared on Base (the odd ids in Base, the even ones in Sub).
   */
  private void createNumbered() {
    for (final String type : new String[] { "P", "N", "Base" }) {
      database.command("sql", "CREATE VERTEX TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER").close();
      database.command("sql", "CREATE PROPERTY " + type + ".other INTEGER").close();
      database.command("sql", "CREATE PROPERTY " + type + ".city STRING").close();
    }
    database.command("sql", "CREATE VERTEX TYPE Sub EXTENDS Base").close();
    database.command("sql", "CREATE VERTEX TYPE Pair").close();
    database.command("sql", "CREATE PROPERTY Pair.id INTEGER").close();
    database.command("sql", "CREATE PROPERTY Pair.a INTEGER").close();
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER").close();
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE").close();
    database.command("sql", "CREATE INDEX ON P (city) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON Base (id) UNIQUE").close();
    database.command("sql", "CREATE INDEX ON Pair (a, b) NOTUNIQUE").close();
    database.transaction(() -> {
      for (int i = 1; i <= ROWS; i++) {
        for (final String type : new String[] { "P", "N" })
          database.newVertex(type).set("id", i).set("other", i % 20).set("city", "c" + i % 200).save();
        database.newVertex(i % 2 == 0 ? "Sub" : "Base").set("id", i).save();
        database.newVertex("Pair").set("id", i).set("a", i).set("b", i % 7).save();
      }
      // vertices with no value for the keys
      for (final String type : new String[] { "P", "N" })
        database.newVertex(type).set("other", 1).save();
    });
  }

  private String profile(final String statement, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + statement, parameters)) {
      rs.stream().count();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private String run(final String statement, final Map<String, Object> parameters) {
    return run(statement, parameters, true);
  }

  /** The ids the statement answers, sorted, or the class of the exception at the bottom of what it threw. */
  private String run(final String statement, final Map<String, Object> parameters, final boolean catchErrors) {
    try (final ResultSet rs = database.query("opencypher", statement, parameters)) {
      final List<String> ids = new ArrayList<>();
      while (rs.hasNext())
        ids.add(String.valueOf(rs.next().<Object>getProperty("id")));
      Collections.sort(ids);
      return "rows " + ids;
    } catch (final RuntimeException e) {
      if (!catchErrors)
        throw e;
      Throwable root = e;
      while (root.getCause() != null)
        root = root.getCause();
      return root.getClass().getSimpleName();
    }
  }

  /** Denies the read of every record of the types, and grants everything else. */
  private void deny(final String... typeNames) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : typeNames)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
