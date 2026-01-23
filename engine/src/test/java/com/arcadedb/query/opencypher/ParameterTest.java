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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        //System.out.println("Found: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        count++;
      }

      //System.out.println("Total count: " + count);
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
        //System.out.println("Created: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        totalCount++;
      }
      //System.out.println("Total created: " + totalCount);

      // Test query with named parameter
      final ResultSet result = database.query("opencypher",
          "MATCH (p:Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age",
          Map.of("p1", 25));

      int count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        //System.out.println("Filtered: " + row.getProperty("p.name") + ", age: " + row.getProperty("p.age"));
        count++;
      }

      //System.out.println("Filtered count: " + count);
      assertThat(count).isEqualTo(25); // Ages 25-49

    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File("./target/testparams2"));
    }
  }

  /**
   * Issue #2364: 16 params or more in cypher = BOOM | 15 is fine
   * https://github.com/ArcadeData/arcadedb/issues/2364
   * Verifies native OpenCypher engine handles 16+ parameters correctly.
   */
  @Test
  void testManyParametersInIdQuery() {
    final Database database = new DatabaseFactory("./target/testparams3").create();
    try {
      database.getSchema().getOrCreateVertexType("CHUNK");

      // Create 20 test nodes
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 20; i++) {
          final RID rid = database.newVertex("CHUNK").set("name", "chunk" + i).save().getIdentity();
          rids.add(rid);
        }
      });

      // Test with 15 parameters - baseline
      final StringBuilder query15 = new StringBuilder("MATCH (n:CHUNK) WHERE ID(n) IN [");
      final Map<String, Object> params15 = new HashMap<>();
      for (int i = 0; i < 15; i++) {
        if (i > 0) query15.append(", ");
        query15.append("$id_").append(i);
        params15.put("id_" + i, rids.get(i).toString());
      }
      query15.append("] RETURN n");

      final ResultSet result15 = database.query("opencypher", query15.toString(), params15);
      int count15 = 0;
      while (result15.hasNext()) {
        result15.next();
        count15++;
      }
      assertThat(count15).isEqualTo(15);

      // Test with 16 parameters - this was the boundary that previously failed
      final StringBuilder query16 = new StringBuilder("MATCH (n:CHUNK) WHERE ID(n) IN [");
      final Map<String, Object> params16 = new HashMap<>();
      for (int i = 0; i < 16; i++) {
        if (i > 0) query16.append(", ");
        query16.append("$id_").append(i);
        params16.put("id_" + i, rids.get(i).toString());
      }
      query16.append("] RETURN n");

      final ResultSet result16 = database.query("opencypher", query16.toString(), params16);
      int count16 = 0;
      while (result16.hasNext()) {
        result16.next();
        count16++;
      }
      assertThat(count16).isEqualTo(16);

      // Test with 20 parameters to ensure higher counts work
      final StringBuilder query20 = new StringBuilder("MATCH (n:CHUNK) WHERE ID(n) IN [");
      final Map<String, Object> params20 = new HashMap<>();
      for (int i = 0; i < 20; i++) {
        if (i > 0) query20.append(", ");
        query20.append("$id_").append(i);
        params20.put("id_" + i, rids.get(i).toString());
      }
      query20.append("] RETURN n");

      final ResultSet result20 = database.query("opencypher", query20.toString(), params20);
      int count20 = 0;
      while (result20.hasNext()) {
        result20.next();
        count20++;
      }
      assertThat(count20).isEqualTo(20);

    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File("./target/testparams3"));
    }
  }

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File("./target/testparams"));
  }
}
