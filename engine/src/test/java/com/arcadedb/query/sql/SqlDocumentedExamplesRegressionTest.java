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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the SQL examples printed in the documentation (issues #9094, #9076, #9048).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlDocumentedExamplesRegressionTest {

  // ---- #9094: lineString()/polygon() over a list of point() results ----

  @Test
  void lineStringOverPointResults() throws Exception {
    TestHelper.executeInNewDatabase("Issue9094LineString", db -> {
      try (final ResultSet rs = db.query("sql", "SELECT geo.lineString([geo.point(0, 0), geo.point(10, 10), geo.point(20, 0)]) AS line")) {
        assertThat((String) rs.next().getProperty("line")).isEqualTo("LINESTRING (0 0, 10 10, 20 0)");
      }
      try (final ResultSet rs = db.query("sql",
          "SELECT lineString([point(10, 10), point(20, 10), point(20, 20), point(10, 20), point(30, 30)]) AS line")) {
        assertThat((String) rs.next().getProperty("line")).isEqualTo("LINESTRING (10 10, 20 10, 20 20, 10 20, 30 30)");
      }
    });
  }

  @Test
  void polygonOverPointResults() throws Exception {
    TestHelper.executeInNewDatabase("Issue9094Polygon", db -> {
      try (final ResultSet rs = db.query("sql",
          "SELECT geo.polygon([geo.point(0, 0), geo.point(10, 0), geo.point(10, 10), geo.point(0, 10), geo.point(0, 0)]) AS poly")) {
        assertThat((String) rs.next().getProperty("poly")).isEqualTo("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
      }
      try (final ResultSet rs = db.query("sql",
          "SELECT polygon([point(10, 10), point(20, 10), point(20, 20), point(10, 20)]) AS poly")) {
        assertThat((String) rs.next().getProperty("poly")).isEqualTo("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))");
      }
    });
  }

  @Test
  void mixedVertexSpellingsAndInvalidElement() throws Exception {
    TestHelper.executeInNewDatabase("Issue9094Mixed", db -> {
      try (final ResultSet rs = db.query("sql", "SELECT geo.lineString([[0, 0], geo.point(10, 10)]) AS line")) {
        assertThat((String) rs.next().getProperty("line")).isEqualTo("LINESTRING (0 0, 10 10)");
      }
      assertThatThrownBy(() -> db.query("sql", "SELECT geo.lineString(['LINESTRING (0 0, 1 1)', [1, 1]]) AS line").next())
          .hasMessageContaining("Invalid point element");
      assertThatThrownBy(() -> db.query("sql", "SELECT geo.lineString(['foo', [1, 1]]) AS line").next())
          .hasMessageContaining("Invalid point element");
    });
  }

  // ---- #9076: CONTAINSALL / CONTAINSANY with any parenthesised condition ----

  @Test
  void containsAllWithNonOrCondition() throws Exception {
    TestHelper.executeInNewDatabase("Issue9076ContainsAll", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Clients");
      db.transaction(() -> {
        db.command("sql", "INSERT INTO Clients SET name = 'a', m = {\"x\": 1, \"y\": 2}");
        db.command("sql", "INSERT INTO Clients SET name = 'b', m = {\"x\": 1, \"y\": -2}");
      });
      // the condition is evaluated against each element: a number has no "name"
      assertThat(count(db, "SELECT FROM Clients WHERE m.values() CONTAINSALL (name IS NOT NULL)")).isEqualTo(0);
      assertThat(count(db, "SELECT FROM Clients WHERE m.values() CONTAINSALL (@this > 0)")).isEqualTo(1);
      assertThat(count(db, "SELECT FROM Clients WHERE m.values() CONTAINSALL (@this > 0 AND @this < 2)")).isEqualTo(0);
      assertThat(count(db, "SELECT FROM Clients WHERE m.values() CONTAINSALL (@this > 0 OR @this < 0)")).isEqualTo(2);
      assertThat(count(db, "SELECT FROM Clients WHERE m.values() CONTAINSANY (@this > 1)")).isEqualTo(1);
      assertThat(count(db, "SELECT FROM Clients WHERE m.values() CONTAINSANY (@this IS NULL)")).isEqualTo(0);
    });
  }

  @Test
  void containsAllIsNotNullOverEmbeddedDocuments() throws Exception {
    TestHelper.executeInNewDatabase("Issue9076Embedded", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Basket");
      db.transaction(() -> {
        db.command("sql", "INSERT INTO Basket SET id = 1, items = [{\"name\": \"x\"}, {\"name\": \"y\"}]");
        db.command("sql", "INSERT INTO Basket SET id = 2, items = [{\"name\": \"x\"}, {\"other\": 1}]");
      });
      assertThat(count(db, "SELECT FROM Basket WHERE items CONTAINSALL (name IS NOT NULL)")).isEqualTo(1);
      assertThat(count(db, "SELECT FROM Basket WHERE items CONTAINSANY (name IS NOT NULL)")).isEqualTo(2);
    });
  }

  @Test
  void containsAnyConditionNeedsOneMatchingElement() throws Exception {
    TestHelper.executeInNewDatabase("Issue9076Any", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Bag");
      db.transaction(() -> {
        db.command("sql", "INSERT INTO Bag SET id = 1, l = [1, 2]");
        db.command("sql", "INSERT INTO Bag SET id = 2, l = [-1, -2]");
        db.command("sql", "INSERT INTO Bag SET id = 3, l = []");
      });
      assertThat(count(db, "SELECT FROM Bag WHERE l CONTAINSANY (@this = 2)")).isEqualTo(1);
      assertThat(count(db, "SELECT FROM Bag WHERE l CONTAINSANY (@this > 1)")).isEqualTo(1);
      assertThat(count(db, "SELECT FROM Bag WHERE l CONTAINSANY (@this < 0)")).isEqualTo(1);
      assertThat(count(db, "SELECT FROM Bag WHERE l CONTAINSANY (@this > 5)")).isEqualTo(0);
    });
  }

  @Test
  void quotedPropertyNamedLikeAFunctionNamespace() throws Exception {
    TestHelper.executeInNewDatabase("Issue9076Namespace", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Clients");
      db.transaction(() -> db.command("sql", "INSERT INTO Clients SET map = {\"x\": 1, \"y\": 2}"));
      try (final ResultSet rs = db.query("sql", "SELECT `map`.values() AS v FROM Clients")) {
        assertThat(rs.next().<Collection<Object>>getProperty("v")).containsExactlyInAnyOrder(1, 2);
      }
      assertThat(count(db, "SELECT FROM Clients WHERE `map`.values() CONTAINSALL [1, 2]")).isEqualTo(1);
    });
  }

  @Test
  void containsAllOverAnEmptyListIsVacuouslyTrue() throws Exception {
    TestHelper.executeInNewDatabase("Issue9076Empty", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Bag");
      db.transaction(() -> db.command("sql", "INSERT INTO Bag SET l = []"));
      assertThat(count(db, "SELECT FROM Bag WHERE l CONTAINSALL (@this > 0)")).isEqualTo(1);
      assertThat(count(db, "SELECT FROM Bag WHERE l CONTAINSANY (@this > 0)")).isEqualTo(0);
    });
  }

  // ---- #9048: a null argument is an absent value or a typed error, never a raw NullPointerException ----

  @Test
  void nullArgumentsAreNotNullPointerExceptions() throws Exception {
    TestHelper.executeInNewDatabase("Issue9048", db -> {
      assertThat(scalar(db, "SELECT randomInt(null) AS r")).isNull();
      assertThat(scalar(db, "SELECT encode('hello', null) AS r")).isNull();
      assertThat(scalar(db, "SELECT 'abc'.hash(null) AS r")).isEqualTo(scalar(db, "SELECT 'abc'.hash() AS r"));
      assertThat(scalar(db, "SELECT 'abc'.normalize(null) AS r")).isEqualTo(scalar(db, "SELECT 'abc'.normalize() AS r"));
      assertThat(scalar(db, "SELECT '2024-01-15'.asDate(null) AS r")).isEqualTo(scalar(db, "SELECT '2024-01-15'.asDate() AS r"));
      assertThat(scalar(db, "SELECT '2024-01-15 10:00:00'.asDateTime(null) AS r"))
          .isEqualTo(scalar(db, "SELECT '2024-01-15 10:00:00'.asDateTime() AS r"));
      assertThatThrownBy(() -> scalar(db, "SELECT vectorNeighbors(null, null, 3) AS r"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("index name is null");
    });
  }

  private static long count(final Database db, final String sql) {
    try (final ResultSet rs = db.query("sql", sql)) {
      long n = 0;
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
      return n;
    }
  }

  private static Object scalar(final Database db, final String sql) {
    try (final ResultSet rs = db.query("sql", sql)) {
      return rs.next().getProperty("r");
    }
  }
}
