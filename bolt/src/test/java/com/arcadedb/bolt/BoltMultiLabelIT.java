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
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for multi-label support over BOLT protocol.
 * Tests that Neo4j Java driver correctly receives multiple labels.
 */
public class BoltMultiLabelIT extends BaseGraphServerTest {

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
    return GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder()
            .withoutEncryption()
            .build()
    );
  }

  @Test
  void createVertexWithMultipleLabels() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create vertex with two labels
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Alice'})");

        // Query and verify labels
        final Result result = session.run("MATCH (n:MLPerson:MLDeveloper {name: 'Alice'}) RETURN n");
        assertThat(result.hasNext()).isTrue();

        final Node node = result.next().get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Alice");

        // Verify node has both labels
        final Iterable<String> labels = node.labels();
        final List<String> labelList = new ArrayList<>();
        labels.forEach(labelList::add);

        assertThat(labelList).containsExactlyInAnyOrder("MLDeveloper", "MLPerson");
      }
    }
  }

  @Test
  void createVertexWithThreeLabels() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create vertex with three labels
        session.run("CREATE (n:MLPerson:MLDeveloper:MLManager {name: 'Bob'})");

        // Query and verify labels
        final Result result = session.run("MATCH (n:MLPerson {name: 'Bob'}) RETURN n");
        assertThat(result.hasNext()).isTrue();

        final Node node = result.next().get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Bob");

        // Verify node has all three labels
        final Iterable<String> labels = node.labels();
        final List<String> labelList = new ArrayList<>();
        labels.forEach(labelList::add);

        assertThat(labelList).containsExactlyInAnyOrder("MLDeveloper", "MLManager", "MLPerson");
      }
    }
  }

  @Test
  void matchByFirstLabel() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create multi-label vertex
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Carol'})");

        // Match by Person label
        final Result result = session.run("MATCH (n:MLPerson) RETURN n");
        assertThat(result.hasNext()).isTrue();

        final Node node = result.next().get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Carol");
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void matchBySecondLabel() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create multi-label vertex
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Dave'})");

        // Match by Developer label
        final Result result = session.run("MATCH (n:MLDeveloper) RETURN n");
        assertThat(result.hasNext()).isTrue();

        final Node node = result.next().get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Dave");
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void matchByBothLabels() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create multi-label vertex
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Eve'})");

        // Match by both labels
        final Result result = session.run("MATCH (n:MLPerson:MLDeveloper) RETURN n");
        assertThat(result.hasNext()).isTrue();

        final Node node = result.next().get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Eve");
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void labelsFunction() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create multi-label vertex
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Frank'})");

        // Use labels() function
        final Result result = session.run("MATCH (n:MLPerson {name: 'Frank'}) RETURN labels(n) as labels");
        assertThat(result.hasNext()).isTrue();

        final List<Object> labels = result.next().get("labels").asList();
        assertThat(labels).containsExactlyInAnyOrder("MLDeveloper", "MLPerson");
      }
    }
  }

  @Test
  void singleLabelVertex() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create single-label vertex
        session.run("CREATE (n:MLSingleLabel {name: 'Grace'})");

        // Query and verify single label
        final Result result = session.run("MATCH (n:MLSingleLabel {name: 'Grace'}) RETURN n");
        assertThat(result.hasNext()).isTrue();

        final Node node = result.next().get("n").asNode();
        assertThat(node.get("name").asString()).isEqualTo("Grace");

        // Verify node has exactly one label
        final Iterable<String> labels = node.labels();
        final List<String> labelList = new ArrayList<>();
        labels.forEach(labelList::add);

        assertThat(labelList).containsExactly("MLSingleLabel");
      }
    }
  }

  @Test
  void mixedSingleAndMultiLabelVertices() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create both single and multi-label vertices
        session.run("CREATE (n:MLMixed {name: 'Henry'})");
        session.run("CREATE (n:MLMixed:MLExtra {name: 'Ivy'})");

        // Query by MLMixed - should find both
        final Result result = session.run("MATCH (n:MLMixed) RETURN n.name as name ORDER BY name");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("name").asString()).isEqualTo("Henry");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("name").asString()).isEqualTo("Ivy");
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void nodeHasLabelMethod() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create multi-label vertex
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Jack'})");

        // Query and verify hasLabel
        final Result result = session.run("MATCH (n:MLPerson {name: 'Jack'}) RETURN n");
        assertThat(result.hasNext()).isTrue();

        final Node node = result.next().get("n").asNode();
        assertThat(node.hasLabel("MLPerson")).isTrue();
        assertThat(node.hasLabel("MLDeveloper")).isTrue();
        assertThat(node.hasLabel("MLManager")).isFalse();
      }
    }
  }

  @Test
  void matchByNonExistentLabelCombination() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create multi-label vertex without Manager
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Kate'})");

        // Match by Person:Manager should not find anything
        final Result result = session.run("MATCH (n:MLPerson:MLManager) RETURN n");
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void multipleVerticesWithDifferentLabelCombinations() {
    try (Driver driver = getDriver()) {
      try (Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        // Create vertices with different label combinations
        session.run("CREATE (n:MLPerson:MLDeveloper {name: 'Liam'})");
        session.run("CREATE (n:MLPerson:MLManager {name: 'Mia'})");
        session.run("CREATE (n:MLDeveloper:MLManager {name: 'Noah'})");

        // Match by MLPerson - should find Liam and Mia
        final Result personResult = session.run("MATCH (n:MLPerson) RETURN n.name as name ORDER BY name");
        assertThat(personResult.hasNext()).isTrue();
        assertThat(personResult.next().get("name").asString()).isEqualTo("Liam");
        assertThat(personResult.hasNext()).isTrue();
        assertThat(personResult.next().get("name").asString()).isEqualTo("Mia");
        assertThat(personResult.hasNext()).isFalse();

        // Match by MLDeveloper - should find Liam and Noah
        final Result devResult = session.run("MATCH (n:MLDeveloper) RETURN n.name as name ORDER BY name");
        assertThat(devResult.hasNext()).isTrue();
        assertThat(devResult.next().get("name").asString()).isEqualTo("Liam");
        assertThat(devResult.hasNext()).isTrue();
        assertThat(devResult.next().get("name").asString()).isEqualTo("Noah");
        assertThat(devResult.hasNext()).isFalse();

        // Match by MLManager - should find Mia and Noah
        final Result mgrResult = session.run("MATCH (n:MLManager) RETURN n.name as name ORDER BY name");
        assertThat(mgrResult.hasNext()).isTrue();
        assertThat(mgrResult.next().get("name").asString()).isEqualTo("Mia");
        assertThat(mgrResult.hasNext()).isTrue();
        assertThat(mgrResult.next().get("name").asString()).isEqualTo("Noah");
        assertThat(mgrResult.hasNext()).isFalse();
      }
    }
  }
}
