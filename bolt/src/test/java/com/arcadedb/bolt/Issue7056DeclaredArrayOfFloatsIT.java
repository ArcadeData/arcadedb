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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/**
 * Regression test for issue #7056: reading back a property declared ARRAY_OF_FLOATS over Bolt returned the Java
 * array's {@code toString()} - e.g. {@code [F@294b13ce} - as a plain string, silently, while the same row read over
 * HTTP/SQL returned the values. Declaring the property is what triggered it: an undeclared one is stored as a List
 * and was mapped correctly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7056DeclaredArrayOfFloatsIT extends BaseGraphServerTest {

  private static final List<Double> EMBEDDING = List.of(0.1, 0.2, 0.3, 0.4);

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void declaredArrayOfFloatsReadsBackAsAList() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE VERTEX TYPE Entity7056");
    db.command("sql", "CREATE PROPERTY Entity7056.emb ARRAY_OF_FLOATS");

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {

      session.run("CREATE (n:Entity7056 {uuid:'a'}) SET n.emb = $v", Map.of("v", EMBEDDING)).consume();

      final Result result = session.run("MATCH (n:Entity7056 {uuid:'a'}) RETURN n.emb AS e");
      assertThat(result.hasNext()).isTrue();

      final Value value = result.next().get("e");
      assertThat(value.type().name())
          .as("a declared ARRAY_OF_FLOATS must come back as a list, not as the array's identity string")
          .isNotEqualTo("STRING");

      final List<Object> read = value.asList();
      assertThat(read).hasSize(EMBEDDING.size());
      for (int i = 0; i < EMBEDDING.size(); i++)
        assertThat(((Number) read.get(i)).doubleValue()).isCloseTo(EMBEDDING.get(i), offset(1e-6));
    }
  }

  @Test
  void declaredArrayOfFloatsIsAlsoAListInsideANode() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE VERTEX TYPE EntityNode7056");
    db.command("sql", "CREATE PROPERTY EntityNode7056.emb ARRAY_OF_FLOATS");

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {

      session.run("CREATE (n:EntityNode7056 {uuid:'b'}) SET n.emb = $v", Map.of("v", EMBEDDING)).consume();

      final Result result = session.run("MATCH (n:EntityNode7056 {uuid:'b'}) RETURN n");
      assertThat(result.hasNext()).isTrue();

      // The whole node travels through the same property mapper, so the value must not degrade there either.
      final Node node = result.next().get("n").asNode();
      final List<Object> read = node.get("emb").asList();
      assertThat(read).hasSize(EMBEDDING.size());
      for (int i = 0; i < EMBEDDING.size(); i++)
        assertThat(((Number) read.get(i)).doubleValue()).isCloseTo(EMBEDDING.get(i), offset(1e-6));
    }
  }
}
