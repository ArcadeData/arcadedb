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

import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.*;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for SQLASTBuilder.
 * Tests the ANTLR visitor that builds ArcadeDB's internal AST from SQL parse trees.
 */
class SQLASTBuilderTest {

  // Helper to parse and build AST
  private Statement parseStatement(final String sql) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(sql));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final SQLParser parser = new SQLParser(tokens);
    final SQLASTBuilder builder = new SQLASTBuilder();
    return builder.visitParse(parser.parse());
  }

  private List<Statement> parseScript(final String sql) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(sql));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final SQLParser parser = new SQLParser(tokens);
    final SQLASTBuilder builder = new SQLASTBuilder();
    return builder.visitParseScript(parser.parseScript());
  }

  private Expression parseExpression(final String expr) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final SQLParser parser = new SQLParser(tokens);
    final SQLASTBuilder builder = new SQLASTBuilder();
    return builder.visitParseExpression(parser.parseExpression());
  }

  private WhereClause parseCondition(final String condition) {
    final SQLLexer lexer = new SQLLexer(CharStreams.fromString(condition));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final SQLParser parser = new SQLParser(tokens);
    final SQLASTBuilder builder = new SQLASTBuilder();
    return builder.visitParseCondition(parser.parseCondition());
  }

  // ========== SELECT Statement Tests ==========

  @Test
  void shouldBuildSimpleSelectStatement() {
    final Statement stmt = parseStatement("SELECT * FROM User");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.projection).isNotNull();
    assertThat(select.target).isNotNull();
  }

  @Test
  void shouldBuildSelectWithProjection() {
    final Statement stmt = parseStatement("SELECT name, age FROM User");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.projection).isNotNull();
    assertThat(select.projection.getItems()).isNotEmpty();
  }

  @Test
  void shouldBuildSelectWithWhereClause() {
    final Statement stmt = parseStatement("SELECT * FROM User WHERE age > 18");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.whereClause).isNotNull();
  }

  @Test
  void shouldBuildSelectWithOrderBy() {
    final Statement stmt = parseStatement("SELECT * FROM User ORDER BY name");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.orderBy).isNotNull();
  }

  @Test
  void shouldBuildSelectWithGroupBy() {
    final Statement stmt = parseStatement("SELECT category, COUNT(*) FROM Product GROUP BY category");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.groupBy).isNotNull();
  }

  @Test
  void shouldBuildSelectWithLimit() {
    final Statement stmt = parseStatement("SELECT * FROM User LIMIT 10");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.limit).isNotNull();
  }

  @Test
  void shouldBuildSelectWithSkip() {
    final Statement stmt = parseStatement("SELECT * FROM User SKIP 5");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.skip).isNotNull();
  }

  @Test
  void shouldBuildSelectWithSkipAndLimit() {
    final Statement stmt = parseStatement("SELECT * FROM User SKIP 5 LIMIT 10");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.skip).isNotNull();
    assertThat(select.limit).isNotNull();
  }

  @Test
  void shouldBuildSelectWithTimeout() {
    final Statement stmt = parseStatement("SELECT * FROM User TIMEOUT 5000");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.timeout).isNotNull();
  }

  @Test
  void shouldBuildSelectWithUnwind() {
    final Statement stmt = parseStatement("SELECT * FROM User UNWIND tags");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.unwind).isNotNull();
  }

  @Test
  void shouldBuildSelectWithLet() {
    final Statement stmt = parseStatement("SELECT * FROM User LET $temp = 1");

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.letClause).isNotNull();
  }

  // ========== INSERT Statement Tests ==========

  @Test
  void shouldBuildInsertStatement() {
    final Statement stmt = parseStatement("INSERT INTO User SET name = 'John', age = 30");

    assertThat(stmt).isInstanceOf(InsertStatement.class);
    final InsertStatement insert = (InsertStatement) stmt;
    assertThat(insert.getTargetType()).isNotNull();
  }

  @Test
  void shouldBuildInsertWithValues() {
    final Statement stmt = parseStatement("INSERT INTO User (name, age) VALUES ('John', 30)");

    assertThat(stmt).isInstanceOf(InsertStatement.class);
  }

  // ========== UPDATE Statement Tests ==========

  @Test
  void shouldBuildUpdateStatement() {
    final Statement stmt = parseStatement("UPDATE User SET name = 'John' WHERE id = 1");

    assertThat(stmt).isInstanceOf(UpdateStatement.class);
    final UpdateStatement update = (UpdateStatement) stmt;
    assertThat(update.getWhereClause()).isNotNull();
  }

  // ========== DELETE Statement Tests ==========

  @Test
  void shouldBuildDeleteStatement() {
    final Statement stmt = parseStatement("DELETE FROM User WHERE id = 1");

    assertThat(stmt).isInstanceOf(DeleteStatement.class);
    final DeleteStatement delete = (DeleteStatement) stmt;
    assertThat(delete.getWhereClause()).isNotNull();
  }

  // ========== CREATE Statement Tests ==========

  @Test
  void shouldBuildCreateClass() {
    final Statement stmt = parseStatement("CREATE VERTEX TYPE User");

    assertThat(stmt).isInstanceOf(CreateVertexTypeStatement.class);
  }

  @Test
  void shouldBuildCreateProperty() {
    final Statement stmt = parseStatement("CREATE PROPERTY User.name STRING");

    assertThat(stmt).isInstanceOf(CreatePropertyStatement.class);
  }

  @Test
  void shouldBuildCreateIndex() {
    final Statement stmt = parseStatement("CREATE INDEX ON User (name)");

    assertThat(stmt).isInstanceOf(CreateIndexStatement.class);
  }

  // ========== MATCH Statement Tests ==========

  @Test
  void shouldBuildMatchStatement() {
    final Statement stmt = parseStatement("MATCH {type: User, as: user} RETURN user");

    assertThat(stmt).isInstanceOf(MatchStatement.class);
    final MatchStatement match = (MatchStatement) stmt;
    assertThat(match.getReturnItems()).isNotEmpty();
  }

  // ========== Expression Tests ==========

  @Test
  void shouldBuildSimpleExpression() {
    final Expression expr = parseExpression("1 + 2");

    assertThat(expr).isNotNull();
  }

  @Test
  void shouldBuildComplexExpression() {
    final Expression expr = parseExpression("(a + b) * c");

    assertThat(expr).isNotNull();
  }

  @Test
  void shouldBuildFunctionCallExpression() {
    final Expression expr = parseExpression("COUNT(*)");

    assertThat(expr).isNotNull();
  }

  // ========== WHERE Condition Tests ==========

  @Test
  void shouldBuildSimpleWhereCondition() {
    final WhereClause where = parseCondition("age > 18");

    assertThat(where).isNotNull();
  }

  @Test
  void shouldBuildComplexWhereCondition() {
    final WhereClause where = parseCondition("(age > 18 AND name = 'John') OR status = 'active'");

    assertThat(where).isNotNull();
  }

  // ========== Script Tests ==========

  @Test
  void shouldBuildMultipleStatements() {
    final List<Statement> statements = parseScript("SELECT * FROM User; SELECT * FROM Product;");

    assertThat(statements).hasSize(2);
    assertThat(statements.get(0)).isInstanceOf(SelectStatement.class);
    assertThat(statements.get(1)).isInstanceOf(SelectStatement.class);
  }

  @Test
  void shouldBuildMixedStatements() {
    final List<Statement> statements = parseScript(
        "CREATE VERTEX TYPE User; INSERT INTO User SET name = 'John'; SELECT * FROM User;"
    );

    assertThat(statements).hasSize(3);
    assertThat(statements.get(0)).isInstanceOf(CreateVertexTypeStatement.class);
    assertThat(statements.get(1)).isInstanceOf(InsertStatement.class);
    assertThat(statements.get(2)).isInstanceOf(SelectStatement.class);
  }

  // ========== Edge Cases ==========

  @Test
  void shouldHandleNestedSubqueries() {
    final Statement stmt = parseStatement(
        "SELECT * FROM (SELECT * FROM User WHERE age > 18) WHERE name LIKE 'J%'"
    );

    assertThat(stmt).isInstanceOf(SelectStatement.class);
  }

  @Test
  void shouldHandleComplexJoins() {
    final Statement stmt = parseStatement(
        "SELECT * FROM User, Address WHERE User.addressId = Address.id"
    );

    assertThat(stmt).isInstanceOf(SelectStatement.class);
  }

  @Test
  void shouldHandleAggregateWithGroupBy() {
    final Statement stmt = parseStatement(
        "SELECT category, AVG(price) as avg_price FROM Product GROUP BY category HAVING AVG(price) > 100"
    );

    assertThat(stmt).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.groupBy).isNotNull();
  }
}
