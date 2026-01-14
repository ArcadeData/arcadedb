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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug test for EXISTS() expression.
 */
public class CypherExistsDebugTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/databases/cypherexistsdebug").create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Company");
      database.getSchema().createEdgeType("WORKS_AT");

      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
      final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").save();
      final MutableVertex company = database.newVertex("Company").set("name", "Acme Corp").save();

      alice.newEdge("WORKS_AT", company, new Object[0]).save();
      bob.newEdge("WORKS_AT", company, new Object[0]).save();
    });
  }

  @AfterEach
  public void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  public void debugExists() {
    System.out.println("=== Testing EXISTS expression ===");

    // First, test basic query without EXISTS
    System.out.println("\n1. Basic MATCH query:");
    ResultSet results = database.query("opencypher", "MATCH (p:Person) RETURN p.name ORDER BY p.name");
    while (results.hasNext()) {
      System.out.println("  Person: " + results.next().getProperty("p.name"));
    }

    // Test WORKS_AT relationships
    System.out.println("\n2. Check WORKS_AT relationships:");
    results = database.query("opencypher", "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name");
    while (results.hasNext()) {
      final Result r = results.next();
      System.out.println("  " + r.getProperty("p.name") + " works at " + r.getProperty("c.name"));
    }

    // Test EXISTS expression
    System.out.println("\n3. Test EXISTS expression:");
    try {
      final String query = "MATCH (p:Person) WHERE EXISTS { (p)-[:WORKS_AT]->(:Company) } RETURN p.name ORDER BY p.name";
      System.out.println("  Query: " + query);

      results = database.query("opencypher", query);
      System.out.println("  Query executed successfully");
      int count = 0;
      while (results.hasNext()) {
        final String name = results.next().getProperty("p.name");
        System.out.println("  Found: " + name);
        count++;
      }
      System.out.println("  Total results: " + count);
      System.out.println("  Expected: Alice, Bob (2 results)");
    } catch (Exception e) {
      System.out.println("  ERROR: " + e.getMessage());
      e.printStackTrace();
    }

    // Try to get execution plan
    System.out.println("\n4. Check execution plan:");
    try {
      results = database.query("opencypher",
          "EXPLAIN MATCH (p:Person) WHERE EXISTS { (p)-[:WORKS_AT]->(:Company) } RETURN p.name");
      if (results.hasNext()) {
        final Result r = results.next();
        System.out.println("  Plan: " + r.toJSON());
      }
    } catch (Exception e) {
      System.out.println("  EXPLAIN not supported or error: " + e.getMessage());
    }
  }
}
