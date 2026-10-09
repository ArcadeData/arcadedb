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
package com.arcadedb.e2e;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL statements whose execution goes through code that reflects or instantiates by class name. They all pass on the JVM
 * and used to fail in the native image only (#9495): the planner copies the AST (UPDATE/DELETE's WHERE-SELECT, LET
 * sub-queries, CREATE VERTEX, MATCH path items), some functions are created by class ({@code cchShortestPath}) and the
 * {@code math_*} functions call {@link Math} through {@code Method.invoke}.
 */
class SqlStatementsIT extends ArcadeContainerTemplate {
  private static final String DATABASE = "sqlstatements";

  private RemoteDatabase database;

  @BeforeEach
  void setUp() {
    final RemoteServer server = new RemoteServer(host, httpPort, "root", "playwithdata");
    if (server.exists(DATABASE))
      server.drop(DATABASE);
    server.create(DATABASE);

    database = new RemoteDatabase(host, httpPort, DATABASE, "root", "playwithdata");
    database.setTimeout(60_000);
    database.command("sql", "CREATE VERTEX TYPE Probe");
    database.command("sql", "CREATE EDGE TYPE Link");
    database.command("sql", "INSERT INTO Probe SET id = 1, v = 0");
    database.command("sql", "INSERT INTO Probe SET id = 2, v = 0");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.close();
  }

  @Test
  void updateAndDeleteCopyTheirWhereSelect() {
    assertThat(single("UPDATE Probe SET v = 1 WHERE id = 1").<Number>getProperty("count").longValue()).isEqualTo(1L);
    assertThat(single("SELECT v FROM Probe WHERE id = 1").<Number>getProperty("v").intValue()).isEqualTo(1);
    // a positional parameter goes through the same plan-cache key
    assertThat(single("UPDATE Probe SET v = ? WHERE id = ?", 7, 2).<Number>getProperty("count").longValue()).isEqualTo(1L);
    assertThat(single("SELECT v FROM Probe WHERE id = 2").<Number>getProperty("v").intValue()).isEqualTo(7);

    assertThat(single("DELETE FROM Probe WHERE id = 2").<Number>getProperty("count").longValue()).isEqualTo(1L);
    assertThat(single("SELECT count(*) AS n FROM Probe").<Number>getProperty("n").longValue()).isEqualTo(1L);
  }

  @Test
  void letSubQueryAndUnionAll() {
    final List<?> a = single("SELECT $a AS a LET $a = (SELECT id FROM Probe WHERE id = 1)").getProperty("a");
    assertThat(a).hasSize(1);

    final List<?> u = single("SELECT unionAll($a, $b) AS u LET $a = (SELECT id FROM Probe WHERE id = 1), "
        + "$b = (SELECT id FROM Probe WHERE id = 2)").getProperty("u");
    assertThat(u).hasSize(2);
  }

  @Test
  void createVertexAndMatch() {
    assertThat(single("CREATE VERTEX Probe SET id = 3, name = 'Grace'").<String>getProperty("name")).isEqualTo("Grace");
    database.command("sql", "CREATE EDGE Link FROM (SELECT FROM Probe WHERE id = 1) TO (SELECT FROM Probe WHERE id = 3) SET w = 1");

    final Result row = single("MATCH {type: Probe, as: a, where: (id = 1)}.out('Link'){as: b} RETURN b.name AS name");
    assertThat(row.<String>getProperty("name")).isEqualTo("Grace");
  }

  @Test
  void graphFunctionsCreatedByClass() {
    database.command("sql", "CREATE EDGE Link FROM (SELECT FROM Probe WHERE id = 1) TO (SELECT FROM Probe WHERE id = 2) SET w = 1");
    final String from = "(SELECT FROM Probe WHERE id = 1)";
    final String to = "(SELECT FROM Probe WHERE id = 2)";
    assertThat(single("SELECT shortestPath(" + from + ", " + to + ") AS p").<List<?>>getProperty("p")).hasSize(2);
    assertThat(single("SELECT cchShortestPath(" + from + ", " + to + ", 'w') AS p").<List<?>>getProperty("p")).hasSize(2);
  }

  @Test
  void mathFunctionsInvokeJavaLangMath() {
    assertThat(single("SELECT math_abs(-5) AS r").<Number>getProperty("r").intValue()).isEqualTo(5);
    assertThat(single("SELECT math_max(3, 7) AS r").<Number>getProperty("r").intValue()).isEqualTo(7);
    assertThat(single("SELECT math_sqrt(16.0) AS r").<Number>getProperty("r").doubleValue()).isEqualTo(4.0);
    assertThat(single("SELECT math_floorMod(7, 3) AS r").<Number>getProperty("r").intValue()).isEqualTo(1);
  }

  @Test
  void convertToAJavaClass() {
    assertThat(single("SELECT '12'.convert('java.lang.Integer') AS c").<Number>getProperty("c").intValue()).isEqualTo(12);
  }

  @Test
  void scriptReadsTheRightPositionalParameter() {
    database.command("sql", "UPDATE Probe SET sku = 'S' + id");
    // the standalone statement caches a plan reading parameter #0, the script's second statement reads #1 (#9247)
    assertThat(single("SELECT id FROM Probe WHERE sku = ?", "S1").<Number>getProperty("id").intValue()).isEqualTo(1);
    try (final ResultSet rs = database.command("sqlscript", "SELECT id FROM Probe WHERE id = ?; SELECT id FROM Probe WHERE sku = ?;", 1,
        "S2")) {
      assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(2);
    }
  }

  private Result single(final String sql, final Object... args) {
    try (final ResultSet rs = database.command("sql", sql, args)) {
      assertThat(rs.hasNext()).as(sql).isTrue();
      return rs.next();
    }
  }
}
