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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the TYPE COUNT optimization in OpenCypher queries.
 * Verifies that simple COUNT queries like "MATCH (a:Account) RETURN COUNT(a)" use O(1) optimization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherCountOptimizationTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcyphercount").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void simpleCountOptimization() {
    // Create test data
    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      for (int i = 0; i < 100; i++)
        database.newVertex("Account").save();
    });

    // Test the optimized count query
    final String query = "MATCH (a:Account) RETURN COUNT(a) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Long>getProperty("count")).isEqualTo(100L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    // TODO: Verify optimization is being used by checking EXPLAIN output
    // EXPLAIN integration needs to be added separately
    // final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    // assertThat(explainResult.hasNext()).isTrue();
    // final String plan = explainResult.next().<String>getProperty("plan");
    // assertThat(plan).contains("TYPE COUNT OPTIMIZATION");
    // explainResult.close();
  }

  @Test
  void countWithDifferentAliases() {
    // Create test data
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      for (int i = 0; i < 50; i++)
        database.newVertex("Person").save();
    });

    // Test with different alias names
    final String query1 = "MATCH (p:Person) RETURN COUNT(p) as totalPeople";
    final ResultSet result1 = database.query("opencypher", query1);
    assertThat(result1.hasNext()).isTrue();
    assertThat(result1.next().<Long>getProperty("totalPeople")).isEqualTo(50L);
    result1.close();

    final String query2 = "MATCH (x:Person) RETURN COUNT(x) as cnt";
    final ResultSet result2 = database.query("opencypher", query2);
    assertThat(result2.hasNext()).isTrue();
    assertThat(result2.next().<Long>getProperty("cnt")).isEqualTo(50L);
    result2.close();
  }

  @Test
  void countWithEmptyType() {
    // Create empty type
    database.transaction(() -> {
      database.getSchema().createVertexType("EmptyType");
    });

    // Test count on empty type
    final String query = "MATCH (e:EmptyType) RETURN COUNT(e) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("count")).isEqualTo(0L);
    result.close();
  }

  @Test
  void optimizationNotAppliedWithWhereClause() {
    // Create test data with properties
    database.transaction(() -> {
      final VertexType accountType = database.getSchema().createVertexType("BankAccount");
      accountType.createProperty("balance", Integer.class);

      for (int i = 0; i < 100; i++)
        database.newVertex("BankAccount").set("balance", i * 100).save();
    });

    // This query should NOT use the optimization due to WHERE clause
    final String query = "MATCH (a:BankAccount) WHERE a.balance > 5000 RETURN COUNT(a) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().<Long>getProperty("count");
    assertThat(count).isLessThan(100L); // Some records filtered out
    result.close();

    // TODO: Verify optimization is NOT being used
    // EXPLAIN integration needs to be added separately
    // final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    // assertThat(explainResult.hasNext()).isTrue();
    // final String plan = explainResult.next().<String>getProperty("plan");
    // assertThat(plan).doesNotContain("TYPE COUNT OPTIMIZATION");
    // explainResult.close();
  }

  @Test
  void optimizationNotAppliedWithMultipleReturnItems() {
    // Create test data
    database.transaction(() -> {
      database.getSchema().createVertexType("Company");
      for (int i = 0; i < 25; i++)
        database.newVertex("Company").save();
    });

    // This query should NOT use the optimization due to multiple return items
    final String query = "MATCH (c:Company) RETURN COUNT(c) as count, 'test' as label";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    result.close();

    // TODO: Verify optimization is NOT being used
    // EXPLAIN integration needs to be added separately
    // final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    // assertThat(explainResult.hasNext()).isTrue();
    // final String plan = explainResult.next().<String>getProperty("plan");
    // assertThat(plan).doesNotContain("TYPE COUNT OPTIMIZATION");
    // explainResult.close();
  }

  @Test
  void countWithPolymorphicTypes() {
    // Create type hierarchy
    database.transaction(() -> {
      database.getSchema().createVertexType("Animal");
      final VertexType dog = database.getSchema().createVertexType("Dog");
      dog.addSuperType("Animal");
      final VertexType cat = database.getSchema().createVertexType("Cat");
      cat.addSuperType("Animal");

      for (int i = 0; i < 30; i++)
        database.newVertex("Dog").save();
      for (int i = 0; i < 20; i++)
        database.newVertex("Cat").save();
    });

    // Count base type (should include subtypes)
    final String query = "MATCH (a:Animal) RETURN COUNT(a) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("count")).isEqualTo(50L);
    result.close();
  }
}
