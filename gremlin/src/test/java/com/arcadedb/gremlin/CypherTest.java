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
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.cypher.ArcadeCypher;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherTest {
  @Test
  void cypher() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().getOrCreateVertexType("Person");

      graph.getDatabase().transaction(() -> {
        for (int i = 0; i < 50; i++)
          graph.cypher(" CREATE (n:Person {name: $1 , age: $2 }) return n", Map.of("1", "Jay", "2", i)).execute();
      });

      final ResultSet result = graph.cypher("MATCH (p:Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age")//
          .setParameter("p1", 25).execute();

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        assertThat(row.<String>getProperty("p.name")).isEqualTo("Jay");
        assertThat(row.getProperty("p.age") instanceof Number).isTrue();
        assertThat(row.<Integer>getProperty("p.age") > lastAge).isTrue();

        lastAge = row.getProperty("p.age");
      }

      assertThat(i).isEqualTo(25);

    } finally {
      graph.drop();
      assertThat(graph.getGremlinJavaEngine()).isNull();
      assertThat(graph.getGremlinGroovyEngine()).isNull();
    }
  }

  @Test
  void cypherSyntaxError() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().createVertexType("Person");

      assertThatThrownBy(() -> graph.cypher("MATCH (p::Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age")//
          .setParameter("p1", 25).execute()).isInstanceOf(CommandParsingException.class);

    } finally {
      graph.drop();
      assertThat(graph.getGremlinJavaEngine()).isNull();
      assertThat(graph.getGremlinGroovyEngine()).isNull();
    }
  }

  @Test
  void cypherFromDatabase() {
    final Database database = new DatabaseFactory("./target/testcypher").create();
    try {

      database.getSchema().createVertexType("Person");

      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newVertex("Person").set("name", "Jay").set("age", i).save();
      });

      final ResultSet result = database.query("cypher", "MATCH (p:Person) WHERE p.age >= $p1 RETURN p.name, p.age ORDER BY p.age",
          "p1", 25);

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        assertThat(row.<String>getProperty("p.name")).isEqualTo("Jay");
        assertThat(row.getProperty("p.age") instanceof Number).isTrue();
        assertThat((int) row.getProperty("p.age") > lastAge).isTrue();

        lastAge = row.getProperty("p.age");
      }

      assertThat(i).isEqualTo(25);

    } finally {
      if (database.isTransactionActive())
        database.commit();

      database.drop();
    }
  }

  @Test
  void cypherParse() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      final ArcadeCypher cypherReadOnly = graph.cypher("MATCH (p:Person) WHERE p.age >= 25 RETURN p.name, p.age ORDER BY p.age");

      QueryEngine.AnalyzedQuery parse = cypherReadOnly.parse();
      assertThat(parse.isIdempotent()).isTrue();
      assertThat(parse.isDDL()).isFalse();

      final ArcadeGremlin cypherWrite = graph.cypher("CREATE (n:Person)");

      QueryEngine.AnalyzedQuery parse1 = cypherWrite.parse();
      assertThat(parse1.isIdempotent()).isFalse();
      assertThat(parse1.isDDL()).isFalse();

    } finally {
      graph.drop();
      assertThat(graph.getGremlinJavaEngine()).isNull();
      assertThat(graph.getGremlinGroovyEngine()).isNull();
    }
  }

  @Test
  void vertexCreationIdentity() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      final ArcadeCypher cypherReadOnly = graph.cypher("CREATE (i:User {name: 'RAMS'}) return i");

      assertThat(cypherReadOnly.parse().isIdempotent()).isFalse();
      assertThat(cypherReadOnly.parse().isDDL()).isFalse();

      final ResultSet result = cypherReadOnly.execute();

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.getIdentity().get()).isNotNull();

    } finally {
      graph.drop();
      assertThat(graph.getGremlinJavaEngine()).isNull();
      assertThat(graph.getGremlinGroovyEngine()).isNull();
    }
  }

  /**
   * https://github.com/ArcadeData/arcadedb/issues/314
   */
  @Test
  void issue314() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().getOrCreateVertexType("Person");

      final ResultSet p1 = graph.cypher("CREATE (p:Person {label:\"First\"}) return p").execute();
      assertThat(p1.hasNext()).isTrue();
      final RID p1RID = p1.next().getIdentity().get();

      final ResultSet p2 = graph.cypher("CREATE (p:Person {label:\"Second\"}) return p").execute();
      assertThat(p2.hasNext()).isTrue();
      final RID p2RID = p2.next().getIdentity().get();

      final ArcadeCypher query = graph.cypher("MATCH (a),(b) WHERE a.label = \"First\" AND b.label = \"Second\" RETURN a,b");
      final ResultSet result = query.execute();

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(row.<Object>getProperty("a")).isNotNull();
      assertThat(((Result) row.getProperty("a")).getIdentity().get()).isEqualTo(p1RID);
      assertThat(row.<Object>getProperty("b")).isNotNull();
      assertThat(((Result) row.getProperty("b")).getIdentity().get()).isEqualTo(p2RID);

    } finally {
      graph.drop();
      assertThat(graph.getGremlinJavaEngine()).isNull();
      assertThat(graph.getGremlinGroovyEngine()).isNull();
    }
  }

  /**
   * Cypher: "delete" query causes "groovy.lang.MissingMethodException" error
   * https://github.com/ArcadeData/arcadedb/issues/734
   * This test uses Groovy engine to reproduce the issue, it does't work with the defautl java Gremlin engine (default from v25.12.1).
   */
  @Test
  void issue734() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("groovy");
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      final ResultSet p1 = graph.cypher("CREATE (p:Person) RETURN p").execute();
      assertThat(p1.hasNext()).isTrue();
      p1.next().getIdentity().get();

      graph.cypher("MATCH (p) DELETE p").execute();

    } finally {
      GlobalConfiguration.GREMLIN_ENGINE.reset();
      graph.drop();
      assertThat(graph.getGremlinJavaEngine()).isNull();
      assertThat(graph.getGremlinGroovyEngine()).isNull();
    }
  }

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File("./target/testcypher"));
  }
}
