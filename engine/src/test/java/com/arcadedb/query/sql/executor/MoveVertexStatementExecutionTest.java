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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * original @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 * Ported by @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MoveVertexStatementExecutionTest extends TestHelper {

  @Test
  void moveVertex() {
    String vertexClassName1 = "testMoveVertexV1";
    String vertexClassName2 = "testMoveVertexV2";
    String edgeClassName = "testMoveVertexE";
    database.getSchema().createVertexType(vertexClassName1);
    database.getSchema().createVertexType(vertexClassName2);
    database.getSchema().createEdgeType(edgeClassName);

    database.setAutoTransaction(true);

    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'a'");
    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'b'");
    database.command("sql",
        "create edge "
            + edgeClassName
            + " from (select from "
            + vertexClassName1
            + " where name = 'a' ) to (select from "
            + vertexClassName1
            + " where name = 'b' )");

    database.command("sql",
        "MOVE VERTEX (select from "
            + vertexClassName1
            + " where name = 'a') to type:" + vertexClassName2);
    ResultSet rs = database.query("sql", "select from " + vertexClassName1);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    rs = database.query("sql", "select from " + vertexClassName2);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    rs = database.query("sql", "select expand(out()) from " + vertexClassName2);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    rs = database.query("sql", "select expand(in()) from " + vertexClassName1);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void moveVertexBatch() {
    String vertexClassName1 = "testMoveVertexBatchV1";
    String vertexClassName2 = "testMoveVertexBatchV2";
    String edgeClassName = "testMoveVertexBatchE";
    database.getSchema().createVertexType(vertexClassName1);
    database.getSchema().createVertexType(vertexClassName2);
    database.getSchema().createEdgeType(edgeClassName);

    database.setAutoTransaction(true);

    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'a'");
    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'b'");
    database.command("sql",
        "create edge "
            + edgeClassName
            + " from (select from "
            + vertexClassName1
            + " where name = 'a' ) to (select from "
            + vertexClassName1
            + " where name = 'b' )");

    database.command("sql",
        "MOVE VERTEX (select from "
            + vertexClassName1
            + " where name = 'a') to type:" + vertexClassName2 + " BATCH 2");
    ResultSet rs = database.query("sql", "select from " + vertexClassName1);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    rs = database.query("sql", "select from " + vertexClassName2);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    rs = database.query("sql", "select expand(out()) from " + vertexClassName2);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    rs = database.query("sql", "select expand(in()) from " + vertexClassName1);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  /**
   * Regression test for issue #4347: a single script combining the CREATE and MOVE statements
   * (as reported in the issue from the Studio "SQL Script" mode) must succeed.
   */
  @Test
  void moveVertexByRidInSingleScript() {
    database.transaction(() -> database.command("sqlscript", """
        CREATE VERTEX TYPE testMoveVertexRidSingleA;
        CREATE VERTEX TYPE testMoveVertexRidSingleB;
        CREATE VERTEX testMoveVertexRidSingleA;
        """).close());

    final String[] holder = new String[1];
    database.transaction(() -> {
      try (final ResultSet selectRs = database.query("sql", "select from testMoveVertexRidSingleA")) {
        assertThat(selectRs.hasNext()).isTrue();
        holder[0] = selectRs.next().getElement().get().getIdentity().toString();
      }
    });

    // Reproduces the exact pattern from the issue screenshot: a SELECT followed by MOVE VERTEX <RID> in the same script.
    database.transaction(() -> database.command("sqlscript", """
        SELECT FROM testMoveVertexRidSingleA;
        MOVE VERTEX %s TO TYPE:testMoveVertexRidSingleB;
        """.formatted(holder[0])).close());

    try (final ResultSet check = database.query("sql", "select from testMoveVertexRidSingleA")) {
      assertThat(check.hasNext()).isFalse();
    }
    try (final ResultSet check = database.query("sql", "select from testMoveVertexRidSingleB")) {
      assertThat(check.hasNext()).isTrue();
      check.next();
      assertThat(check.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #4347: MOVE VERTEX from an SQL script using a literal RID
   * caused java.lang.StackOverflowError because the base Statement.createExecutionPlan(CommandContext)
   * recursed into itself for any subclass that did not override it (including MoveVertexStatement).
   */
  @Test
  void moveVertexByRidInScript() {
    database.transaction(() -> database.command("sqlscript", """
        CREATE VERTEX TYPE testMoveVertexRidScriptA;
        CREATE VERTEX TYPE testMoveVertexRidScriptB;
        CREATE VERTEX testMoveVertexRidScriptA;
        """).close());

    final String[] holder = new String[1];
    database.transaction(() -> {
      try (final ResultSet selectRs = database.query("sql", "select from testMoveVertexRidScriptA")) {
        assertThat(selectRs.hasNext()).isTrue();
        holder[0] = selectRs.next().getElement().get().getIdentity().toString();
      }
    });

    database.transaction(
        () -> database.command("sqlscript", "MOVE VERTEX " + holder[0] + " TO TYPE:testMoveVertexRidScriptB;").close());

    try (final ResultSet check = database.query("sql", "select from testMoveVertexRidScriptA")) {
      assertThat(check.hasNext()).isFalse();
    }
    try (final ResultSet check = database.query("sql", "select from testMoveVertexRidScriptB")) {
      assertThat(check.hasNext()).isTrue();
      check.next();
      assertThat(check.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #4347: MOVE VERTEX with a direct RID source
   * caused java.lang.StackOverflowError because the RID expression was not
   * recognized as a record source and was rendered as a type identifier of the form "(#X:Y)".
   */
  @Test
  void moveVertexByRid() {
    final String typeA = "testMoveVertexRidA";
    final String typeB = "testMoveVertexRidB";
    database.getSchema().createVertexType(typeA);
    database.getSchema().createVertexType(typeB);

    database.setAutoTransaction(true);

    final ResultSet created = database.command("sql", "create vertex " + typeA);
    assertThat(created.hasNext()).isTrue();
    final String rid = created.next().getElement().get().getIdentity().toString();
    created.close();

    database.command("sql", "MOVE VERTEX " + rid + " TO TYPE:" + typeB).close();

    ResultSet rs = database.query("sql", "select from " + typeA);
    assertThat(rs.hasNext()).isFalse();
    rs.close();

    rs = database.query("sql", "select from " + typeB);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }
}
