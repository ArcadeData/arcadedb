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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic tests for the ANTLR SQL parser.
 * Tests the parser infrastructure and basic SQL statement parsing.
 */
class SQLAntlrParserTest {

  @Test
  void simpleSelect() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT * FROM User";
    final Statement stmt = parser.parse(sql);

    assertThat(stmt).as("Statement should not be null").isNotNull();
    assertThat(stmt).as("Statement should be a SelectStatement").isInstanceOf(SelectStatement.class);

    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.projection).as("Projection should not be null").isNotNull();
    assertThat(select.target).as("FROM clause should not be null").isNotNull();

    //System.out.println("✓ Simple SELECT parsed successfully");
  }

  @Test
  void selectWithWhere() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT name FROM User WHERE age > 18";
    final Statement stmt = parser.parse(sql);

    assertThat(stmt).as("Statement should not be null").isNotNull();
    assertThat(stmt).as("Statement should be a SelectStatement").isInstanceOf(SelectStatement.class);

    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.projection).as("Projection should not be null").isNotNull();
    assertThat(select.target).as("FROM clause should not be null").isNotNull();
    assertThat(select.whereClause).as("WHERE clause should not be null").isNotNull();

    //System.out.println("✓ SELECT with WHERE parsed successfully");
  }

  @Test
  void selectWithLimit() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT * FROM User LIMIT 10";
    final Statement stmt = parser.parse(sql);

    assertThat(stmt).as("Statement should not be null").isNotNull();
    assertThat(stmt).as("Statement should be a SelectStatement").isInstanceOf(SelectStatement.class);

    final SelectStatement select = (SelectStatement) stmt;
    assertThat(select.limit).as("LIMIT clause should not be null").isNotNull();

    //System.out.println("✓ SELECT with LIMIT parsed successfully");
  }

  @Test
  void multipleStatements() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    final String sql = "SELECT * FROM User; SELECT * FROM Product";
    final List<Statement> statements = parser.parseScript(sql);

    assertThat(statements).as("Statements list should not be null").isNotNull();
    assertThat(statements.size()).as("Should have 2 statements").isEqualTo(2);

    //System.out.println("✓ Multiple statements parsed successfully");
  }

  @Test
  void selectWithMultiLetSubqueries() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    // Basic multi-LET with subqueries and LIMIT -1
    assertThat(parser.parse("SELECT $c LET $a = (SELECT FROM V LIMIT -1), $b = (SELECT FROM V LIMIT -1)"))
        .isInstanceOf(SelectStatement.class);

    // SELECT ($c) with multiple LET and UNIONALL
    assertThat(parser.parse(
        "SELECT ( $c ) LET $a = (SELECT count(ID) FROM V LIMIT -1), "
            + "$b = (SELECT * FROM V ORDER BY name LIMIT -1), "
            + "$c = UNIONALL( $a, $b ) limit -1"))
        .isInstanceOf(SelectStatement.class);
  }

  @Test
  void orderByParenthesizedDirection() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    // OrientDB-style parenthesized ORDER BY: (expr ASC), (expr DESC)
    assertThat(parser.parse(
        "SELECT FROM V ORDER BY (Name.toLowerCase() asc), (CreatedOn desc) LIMIT -1"))
        .isInstanceOf(SelectStatement.class);

    // Parenthesized ORDER BY inside LET subquery
    assertThat(parser.parse(
        "SELECT $c LET $a = (SELECT FROM V LIMIT -1), "
            + "$b = (SELECT FROM V ORDER BY (Name.toLowerCase() asc), (CreatedOn desc) LIMIT -1)"))
        .isInstanceOf(SelectStatement.class);
  }

  @Test
  void complexSelectWithLetSubqueriesAndOrderBy() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);

    // Full complex query with multiple LET subqueries, edge traversals,
    // nested projections, parenthesized ORDER BY, and UNIONALL
    assertThat(parser.parse(
        "SELECT ( $c ) LET $a = (SELECT count(ID) FROM Product WHERE "
            + "inE('HasChild')[_isDeleted <> true].size() = 0 AND "
            + "(inE('Branch')[_isDeleted <> true] is NULL or inE('Branch')[_isDeleted <> true].size() = 0) AND "
            + "@type = \"Product\" AND _isDeleted <> true AND _isDisabled <> true LIMIT -1), "
            + "$b = (SELECT *,@rid, @type, State.Color as _State_Color, State.ID as _State_ID, "
            + "State.StateType as _State_StateType, CompoundID, GenericName, TherapeuticArea, TradeName, "
            + "!_scheme, CreatedBy.Email as _CreatedBy_Email, ModifiedBy.Email as _ModifiedBy_Email, "
            + " out('HasChild')[_isDeleted <> true].size() as CSize , "
            + "_documentType.ID as _documentType_ID, Owner.Email as _Owner_Email, "
            + "in('HasFavoriteObject').ID as _favoritedBy, "
            + "out('IsVariantOf')[_isDeleted <> true].ID as _variantsID, "
            + "inE('Branch')[_isDeleted <> true].Name[0] as BranchName ,"
            + "_lastApprovedRecord:{*} as _lastApproved FROM Product WHERE "
            + "inE('HasChild')[_isDeleted <> true].size() = 0 AND "
            + "(inE('Branch')[_isDeleted <> true] is NULL or inE('Branch')[_isDeleted <> true].size() = 0) AND "
            + "@type = \"Product\" AND _isDeleted <> true AND _isDisabled <> true "
            + "ORDER BY (Name.toLowerCase() asc), (CreatedOn desc) LIMIT -1), "
            + "$c = UNIONALL( $a, $b ) limit -1"))
        .isInstanceOf(SelectStatement.class);
  }
}
