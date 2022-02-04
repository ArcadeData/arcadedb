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
package org.apache.tinkerpop.gremlin.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeCypher;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGremlin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.concurrent.*;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherTest {
  @Test
  public void testCypher() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().getOrCreateVertexType("Person");

      graph.getDatabase().transaction(() -> {
        for (int i = 0; i < 50; i++)
          graph.getDatabase().newVertex("Person").set("name", "Jay").set("age", i).save();
      });

      ResultSet result = graph.cypher("MATCH (p:Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age")//
          .setParameter("p1", 25).execute();

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        //System.out.println(row);

        Assertions.assertEquals("Jay", row.getProperty("p.name"));
        Assertions.assertTrue(row.getProperty("p.age") instanceof Number);
        Assertions.assertTrue((int) row.getProperty("p.age") > lastAge);

        lastAge = row.getProperty("p.age");
      }

      Assertions.assertEquals(25, i);

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testCypherSyntaxError() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().createVertexType("Person");

      try {
        graph.cypher("MATCH (p::Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age")//
            .setParameter("p1", 25).execute();
        Assertions.fail();
      } catch (QueryParsingException e) {
        // EXPECTED
      }

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testCypherFromDatabase() throws ExecutionException, InterruptedException {
    final Database database = new DatabaseFactory("./target/testcypher").create();
    try {

      database.getSchema().createVertexType("Person");

      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newVertex("Person").set("name", "Jay").set("age", i).save();
      });

      ResultSet result = database.query("cypher", "MATCH (p:Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age", "p1", 25);

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        //System.out.println(row);

        Assertions.assertEquals("Jay", row.getProperty("p.name"));
        Assertions.assertTrue(row.getProperty("p.age") instanceof Number);
        Assertions.assertTrue((int) row.getProperty("p.age") > lastAge);

        lastAge = row.getProperty("p.age");
      }

      Assertions.assertEquals(25, i);

    } finally {
      if (database.isTransactionActive())
        database.commit();

      database.drop();
    }
  }

  @Test
  public void testCypherParse() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      final ArcadeCypher cypherReadOnly = graph.cypher("MATCH (p:Person) WHERE p.age >= 25 RETURN p.name, p.age ORDER BY p.age");

      Assertions.assertTrue(cypherReadOnly.parse().isIdempotent());
      Assertions.assertFalse(cypherReadOnly.parse().isDDL());

      final ArcadeGremlin cypherWrite = graph.cypher("CREATE (n:Person)");

      Assertions.assertFalse(cypherWrite.parse().isIdempotent());
      Assertions.assertFalse(cypherWrite.parse().isDDL());

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testVertexCreationIdentity() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      final ArcadeCypher cypherReadOnly = graph.cypher("CREATE (i:User {name: 'RAMS'}) return i");

      Assertions.assertFalse(cypherReadOnly.parse().isIdempotent());
      Assertions.assertFalse(cypherReadOnly.parse().isDDL());

      final ResultSet result = cypherReadOnly.execute();

      Assertions.assertTrue(result.hasNext());
      final Result row = result.next();
      Assertions.assertNotNull(row.getIdentity().get());

    } finally {
      graph.drop();
    }
  }

  /**
   * https://github.com/ArcadeData/arcadedb/issues/314
   */
  @Test
  public void testIssue314() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().getOrCreateVertexType("Person");

      ResultSet p1 = graph.cypher("CREATE (p:Person {label:\"First\"}) return p").execute();
      Assertions.assertTrue(p1.hasNext());
      RID p1RID = p1.next().getIdentity().get();

      ResultSet p2 = graph.cypher("CREATE (p:Person {label:\"Second\"}) return p").execute();
      Assertions.assertTrue(p2.hasNext());
      RID p2RID = p2.next().getIdentity().get();

      final ArcadeCypher query = graph.cypher("MATCH (a),(b) WHERE a.label = \"First\" AND b.label = \"Second\" RETURN a,b");
      final ResultSet result = query.execute();

      Assertions.assertTrue(result.hasNext());
      final Result row = result.next();
      Assertions.assertNotNull(row.getProperty("a"));
      Assertions.assertEquals(p1RID, ((Result) row.getProperty("a")).getIdentity().get());
      Assertions.assertNotNull(row.getProperty("b"));
      Assertions.assertEquals(p2RID, ((Result) row.getProperty("b")).getIdentity().get());

    } finally {
      graph.drop();
    }
  }

  @BeforeEach
  @AfterEach
  public void clean() {
    FileUtils.deleteRecursively(new File("./target/testcypher"));
  }
}
