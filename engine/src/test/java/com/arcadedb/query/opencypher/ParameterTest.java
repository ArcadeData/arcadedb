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
package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test parameter handling in Cypher queries.
 */
class ParameterTest {

  @Test
  void testSimpleParameterQuery() {
    final Database database = new DatabaseFactory("./target/testparams").create();
    try {
      database.getSchema().getOrCreateVertexType("Person");

      // Create test data
      database.transaction(() -> {
        database.command("opencypher", "CREATE (n:Person {name: 'Alice', age: 30})");
        database.command("opencypher", "CREATE (n:Person {name: 'Bob', age: 25})");
        database.command("opencypher", "CREATE (n:Person {name: 'Charlie', age: 35})");
      });

      // Test simple parameter query
      final ResultSet result = database.query("opencypher",
          "MATCH (p:Person) WHERE p.age >= $minAge RETURN p.name, p.age",
          Map.of("minAge", 30));

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        System.out.println("Found: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        count++;
      }

      System.out.println("Total count: " + count);
      assertThat(count).isEqualTo(2); // Alice and Charlie

    } finally {
      database.drop();
    }
  }

  @Test
  void testPositionalParameters() {
    final Database database = new DatabaseFactory("./target/testparams2").create();
    try {
      database.getSchema().getOrCreateVertexType("Person");

      // Test CREATE with positional parameters (like in original CypherTest)
      database.transaction(() -> {
        for (int i = 0; i < 50; i++) {
          final ResultSet createResult = database.command("opencypher", "CREATE (n:Person {name: $1, age: $2}) return n",
              Map.of("1", "Jay", "2", i));
          // Consume the result set to force execution
          while (createResult.hasNext()) {
            createResult.next();
          }
        }
      });

      // Verify data was created
      final ResultSet allResults = database.query("opencypher", "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age");
      int totalCount = 0;
      while (allResults.hasNext()) {
        final Result row = allResults.next();
        System.out.println("Created: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        totalCount++;
      }
      System.out.println("Total created: " + totalCount);

      // Test query with named parameter
      final ResultSet result = database.query("opencypher",
          "MATCH (p:Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age",
          Map.of("p1", 25));

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        System.out.println("Filtered: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        count++;
      }

      System.out.println("Filtered count: " + count);
      assertThat(count).isEqualTo(25); // Ages 25-49

    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File("./target/testparams2"));
    }
  }

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File("./target/testparams"));
  }
}
