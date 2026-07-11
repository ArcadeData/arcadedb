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
package com.arcadedb.gremlin.antlr;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the engine's ANTLR 4.13.2 SQL/Cypher parsers (v4 ATN) and TinkerPop's relocated
 * ANTLR 4.9.1 Gremlin parser (v3 ATN) load and run in the same JVM without an
 * InvalidClassException "Could not deserialize ATN" error.
 */
class AntlrCoexistenceIT {

  @Test
  void engineAntlr413AndGremlinAntlr491Coexist() {
    final String directory = "./target/antlr-coexistence";
    FileUtils.deleteRecursively(new File(directory));

    final ArcadeGraph graph = ArcadeGraph.open(directory);
    try {
      final BasicDatabase database = graph.getDatabase();
      database.getSchema().createVertexType("Person");
      database.transaction(() ->
          database.newVertex("Person").set("name", "Jay").set("age", 25).save());

      // Engine ANTLR SQL parser (org.antlr 4.13.2, v4 ATN).
      final ResultSet sql = database.query("sql", "SELECT name, age FROM Person WHERE age = 25");
      assertThat(sql.hasNext()).isTrue();
      assertThat(sql.next().<String>getProperty("name")).isEqualTo("Jay");

      // Engine ANTLR Cypher parser (org.antlr 4.13.2, v4 ATN).
      final ResultSet cypher = database.query("cypher", "MATCH (p:Person) RETURN p.name AS name");
      assertThat(cypher.hasNext()).isTrue();
      assertThat(cypher.next().<String>getProperty("name")).isEqualTo("Jay");

      // TinkerPop Gremlin parser (relocated com.arcadedb.gremlin.shaded.org.antlr 4.9.1, v3 ATN).
      final ResultSet gremlin = graph.gremlin("g.V().hasLabel('Person').count()").execute();
      assertThat((Long) gremlin.next().getProperty("result")).isEqualTo(1L);
    } finally {
      graph.drop();
    }
  }
}
