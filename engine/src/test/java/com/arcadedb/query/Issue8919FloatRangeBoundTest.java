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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8919: a bound a FLOAT property cannot hold is read the same way by the index (which converts it to its FLOAT key), by a
 * scan and by Cypher, and a value is never equal to a bound and below it at once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8919FloatRangeBoundTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      for (final String type : new String[] { "I", "N" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".v FLOAT");
        database.command("sql", "CREATE PROPERTY " + type + ".d DOUBLE");
      }
      database.command("sql", "CREATE INDEX ON I (v) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (d) NOTUNIQUE");
      for (final String type : new String[] { "I", "N" }) {
        database.command("sql", "INSERT INTO " + type + " SET id = 1, v = 1.0, d = 1.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 2, v = 16777216, d = 9007199254740992.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 3, v = 100000, d = 5.0");
      }
    });
  }

  @Test
  void sqlOrderingAgreesWithEqualityOnAnUnrepresentableFloatBound() {
    for (final String type : new String[] { "I", "N" }) {
      assertThat(sql("SELECT id FROM " + type + " WHERE v = 16777217.0")).as(type + " =").containsExactly(2);
      assertThat(sql("SELECT id FROM " + type + " WHERE v >= 16777217.0")).as(type + " >=").containsExactly(2);
      assertThat(sql("SELECT id FROM " + type + " WHERE v > 16777217.0")).as(type + " >").isEmpty();
      assertThat(sql("SELECT id FROM " + type + " WHERE v <= 16777217.0")).as(type + " <=").containsExactly(1, 2, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v < 16777217.0")).as(type + " <").containsExactly(1, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v <> 16777217.0")).as(type + " <>").containsExactly(1, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v >= 16777217.0 OR v < 16777217.0")).as(type + " >= or <").containsExactly(1, 2, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v BETWEEN 16777217.0 AND 16777218.0")).as(type + " between").containsExactly(2);
    }
  }

  @Test
  void cypherOrderingAgreesWithEqualityOnAnUnrepresentableFloatBound() {
    for (final String type : new String[] { "I", "N" }) {
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v = 16777217.0 RETURN n.id AS id")).as(type + " =").containsExactly(2);
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v >= 16777217.0 RETURN n.id AS id")).as(type + " >=").containsExactly(2);
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v > 16777217.0 RETURN n.id AS id")).as(type + " >").isEmpty();
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v <= 16777217.0 RETURN n.id AS id")).as(type + " <=").containsExactly(1, 2, 3);
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v < 16777217.0 RETURN n.id AS id")).as(type + " <").containsExactly(1, 3);
    }
  }

  @Test
  void aValueIsNeverEqualToABoundAndBelowIt() {
    for (final String type : new String[] { "I", "N" }) {
      final List<Integer> equal = sql("SELECT id FROM " + type + " WHERE v = 16777217.0");
      final List<Integer> less = sql("SELECT id FROM " + type + " WHERE v < 16777217.0");
      assertThat(less).as(type).doesNotContainAnyElementsOf(equal);
    }
  }

  @Test
  void exactLongBoundAboveTwoToThe53OnADoubleAgreesBetweenIndexAndScan() {
    for (final String type : new String[] { "I", "N" }) {
      assertThat(sql("SELECT id FROM " + type + " WHERE d = 9007199254740993")).as(type + " =").isEmpty();
      assertThat(sql("SELECT id FROM " + type + " WHERE d < 9007199254740993")).as(type + " <").containsExactly(1, 2, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE d >= 9007199254740993")).as(type + " >=").isEmpty();
      assertThat(sql("SELECT id FROM " + type + " WHERE d > 9007199254740991")).as(type + " > 2^53-1").containsExactly(2);
    }
  }

  @Test
  void lossyIntegerBoundsInBetweenAndInAgreeBetweenIndexAndScan() {
    for (final String type : new String[] { "I", "N" }) {
      assertThat(sql("SELECT id FROM " + type + " WHERE d IN [9007199254740993]")).as(type + " IN").isEmpty();
      assertThat(sql("SELECT id FROM " + type + " WHERE d IN [9007199254740993, 5]")).as(type + " IN 2").containsExactly(3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v IN [16777217]")).as(type + " FLOAT IN").isEmpty();
    }
    // BETWEEN: the indexed type answers as the unindexed one does
    assertThat(sql("SELECT id FROM I WHERE d BETWEEN 9007199254740993 AND 9007199254740995"))
        .isEqualTo(sql("SELECT id FROM N WHERE d BETWEEN 9007199254740993 AND 9007199254740995"));
    assertThat(sql("SELECT id FROM I WHERE v BETWEEN 16777217 AND 16777219"))
        .isEqualTo(sql("SELECT id FROM N WHERE v BETWEEN 16777217 AND 16777219"));
  }

  @Test
  void cypherIntegerBoundAboveTwoToThe53OnADoubleAgreesBetweenIndexAndScan() {
    for (final String type : new String[] { "I", "N" }) {
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.d = 9007199254740993 RETURN n.id AS id")).as(type + " =").isEmpty();
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.d < 9007199254740993 RETURN n.id AS id")).as(type + " <").containsExactly(1, 2, 3);
    }
  }

  private List<Integer> sql(final String statement) {
    return ids(database.query("sql", statement + " ORDER BY id"));
  }

  private List<Integer> cypher(final String statement) {
    return ids(database.query("opencypher", statement + " ORDER BY id", Map.of()));
  }

  private static List<Integer> ids(final ResultSet rs) {
    final List<Integer> ids = new ArrayList<>();
    try (rs) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }
}
