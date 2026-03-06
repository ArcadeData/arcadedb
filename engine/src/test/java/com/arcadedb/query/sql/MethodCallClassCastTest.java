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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for ClassCastException in MethodCall when $current is not Identifiable/Result.
 * <p>
 * MethodCall.execute() had two unsafe (Identifiable) casts on the $current context variable:
 * one in the graph filtered function path and one in the method execution path.
 * When $current was a non-standard type (e.g., stale or null in certain LET/UNIONALL patterns),
 * these casts would throw ClassCastException instead of gracefully handling the value.
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MethodCallClassCastTest extends TestHelper {

  @Test
  void testMethodCallWithNonIdentifiableCurrent() {
    // Test that method calls work correctly when $current might not be Identifiable.
    // This can happen in complex queries with LET, UNIONALL, and method chains.
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Product IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE HasChild IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE Branch IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Product.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.ID IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product._isDeleted IF NOT EXISTS BOOLEAN");
      database.command("sql", "CREATE PROPERTY Product._isDisabled IF NOT EXISTS BOOLEAN");
      database.command("sql", "CREATE PROPERTY Product.CreatedOn IF NOT EXISTS DATETIME");

      database.command("sql",
          "INSERT INTO Product SET Name = 'Alpha', ID = 'P1', _isDeleted = false, _isDisabled = false, CreatedOn = sysdate()");
      database.command("sql",
          "INSERT INTO Product SET Name = 'Beta', ID = 'P2', _isDeleted = false, _isDisabled = false, CreatedOn = sysdate()");
    });

    // Reproduce the client's query pattern: SELECT ($c) with LET, UNIONALL, edge traversals,
    // method calls (.size(), .toLowerCase()), and ORDER BY with method call
    database.transaction(() -> {
      final String query =
          "SELECT ( $c ) LET " +
              "$a = (SELECT count(ID) FROM Product WHERE " +
              "inE('HasChild')[_isDeleted <> true].size() = 0 " +
              "AND (inE('Branch')[_isDeleted <> true] is NULL or inE('Branch')[_isDeleted <> true].size() = 0) " +
              "AND @type = \"Product\" AND _isDeleted <> true AND _isDisabled <> true LIMIT -1), " +
              "$b = (SELECT *, @rid, @type, " +
              "out('HasChild')[_isDeleted <> true].size() as CSize " +
              "FROM Product WHERE " +
              "inE('HasChild')[_isDeleted <> true].size() = 0 " +
              "AND (inE('Branch')[_isDeleted <> true] is NULL or inE('Branch')[_isDeleted <> true].size() = 0) " +
              "AND @type = \"Product\" AND _isDeleted <> true AND _isDisabled <> true " +
              "ORDER BY Name.toLowerCase() asc, CreatedOn desc LIMIT -1), " +
              "$c = UNIONALL( $a, $b ) limit -1";

      final ResultSet rs = database.command("sql", query);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      // Should return 1 row with $c containing the UNIONALL of $a (count) and $b (records)
      assertThat(results).hasSize(1);
    });
  }

  @Test
  void testMethodCallOnStringInLetWithUnionAll() {
    // Test method calls (.replace(), .toLowerCase()) within LET subqueries combined with UNIONALL
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE DocA IF NOT EXISTS");
      database.command("sql", "CREATE DOCUMENT TYPE DocB IF NOT EXISTS");
      database.command("sql", "INSERT INTO DocA SET name = 'Hello World', value = 1");
      database.command("sql", "INSERT INTO DocA SET name = 'Test Data', value = 2");
      database.command("sql", "INSERT INTO DocB SET name = 'Hello World B', value = 10");
    });

    database.transaction(() -> {
      // Method calls (.toLowerCase()) in ORDER BY within LET subquery + UNIONALL
      final ResultSet rs = database.query("sql",
          "SELECT expand($c) LET " +
              "$a = (SELECT name FROM DocA ORDER BY name.toLowerCase() asc), " +
              "$b = (SELECT name FROM DocB ORDER BY name.toLowerCase() asc), " +
              "$c = UNIONALL($a, $b)");

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      assertThat(results).hasSize(3);
    });
  }

  @Test
  void testSizeMethodOnEdgeTraversalInLetSubquery() {
    // Test .size() method call on edge traversal results within LET subqueries
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Node IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE Link IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Node.name IF NOT EXISTS STRING");

      database.command("sql", "INSERT INTO Node SET name = 'A'");
      database.command("sql", "INSERT INTO Node SET name = 'B'");
      database.command("sql", "INSERT INTO Node SET name = 'C'");
      database.command("sql",
          "CREATE EDGE Link FROM (SELECT FROM Node WHERE name = 'A') TO (SELECT FROM Node WHERE name = 'B')");
    });

    database.transaction(() -> {
      // .size() on edge traversals within LET + UNIONALL
      final ResultSet rs = database.query("sql",
          "SELECT expand($c) LET " +
              "$a = (SELECT count(*) FROM Node WHERE out('Link').size() > 0), " +
              "$b = (SELECT name, out('Link').size() as linkCount FROM Node WHERE out('Link').size() = 0), " +
              "$c = UNIONALL($a, $b)");

      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());

      // $a = 1 count result (node A has outgoing Link), $b = 2 records (B and C have no outgoing Link)
      assertThat(results).hasSize(3);
    });
  }
}
