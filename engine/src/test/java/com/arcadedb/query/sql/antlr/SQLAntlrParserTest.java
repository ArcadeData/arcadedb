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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic tests for the ANTLR SQL parser.
 * Tests the parser infrastructure and basic SQL statement parsing.
 */
public class SQLAntlrParserTest {

  @Test
  public void testSimpleSelect() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT * FROM User";
    final Statement stmt = parser.parse(sql);

    assertNotNull(stmt, "Statement should not be null");
    assertTrue(stmt instanceof SelectStatement, "Statement should be a SelectStatement");

    final SelectStatement select = (SelectStatement) stmt;
    assertNotNull(select.projection, "Projection should not be null");
    assertNotNull(select.target, "FROM clause should not be null");

    System.out.println("✓ Simple SELECT parsed successfully");
  }

  @Test
  public void testSelectWithWhere() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT name FROM User WHERE age > 18";
    final Statement stmt = parser.parse(sql);

    assertNotNull(stmt, "Statement should not be null");
    assertTrue(stmt instanceof SelectStatement, "Statement should be a SelectStatement");

    final SelectStatement select = (SelectStatement) stmt;
    assertNotNull(select.projection, "Projection should not be null");
    assertNotNull(select.target, "FROM clause should not be null");
    assertNotNull(select.whereClause, "WHERE clause should not be null");

    System.out.println("✓ SELECT with WHERE parsed successfully");
  }

  @Test
  public void testSelectWithLimit() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT * FROM User LIMIT 10";
    final Statement stmt = parser.parse(sql);

    assertNotNull(stmt, "Statement should not be null");
    assertTrue(stmt instanceof SelectStatement, "Statement should be a SelectStatement");

    final SelectStatement select = (SelectStatement) stmt;
    assertNotNull(select.limit, "LIMIT clause should not be null");

    System.out.println("✓ SELECT with LIMIT parsed successfully");
  }

  @Test
  public void testMultipleStatements() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT * FROM User; SELECT * FROM Product";
    final java.util.List<Statement> statements = parser.parseScript(sql);

    assertNotNull(statements, "Statements list should not be null");
    assertEquals(2, statements.size(), "Should have 2 statements");

    System.out.println("✓ Multiple statements parsed successfully");
  }
}
