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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #13664 (#13538, #13544): what a Bolt client is answered for a schema rule that exists already and for a uniqueness
 * violation, compared with Neo4j 5.26. Clients make their schema setup idempotent by ignoring the codes that mean "already there"
 * (neomodel ignores EquivalentSchemaRuleAlreadyExists, IndexAlreadyExists and ConstraintAlreadyExists), and tell a duplicate
 * from another failed constraint by Neo4j's sentence ("already exists with label").
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class Issue13664BoltSchemaConformanceIT extends BaseBoltServerTest {
  private static final String SCHEMA = "Neo.ClientError.Schema.";

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

  @Test
  void anExistingConstraintIsAnsweredAsNeo4jAnswersIt() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE").consume();

      final Neo4jException sameNameSameDefinition = failure(session, "CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE");
      assertThat(sameNameSameDefinition.code()).isEqualTo(SCHEMA + "EquivalentSchemaRuleAlreadyExists");
      assertThat(sameNameSameDefinition.getMessage()).startsWith("An equivalent constraint already exists").contains("name='c1'");

      final Neo4jException otherNameSameDefinition = failure(session, "CREATE CONSTRAINT c2 FOR (n:P) REQUIRE n.a IS UNIQUE");
      assertThat(otherNameSameDefinition.code()).isEqualTo(SCHEMA + "ConstraintAlreadyExists");
      assertThat(otherNameSameDefinition.getMessage()).startsWith("Constraint already exists: Constraint( name='c1'")
          .contains("type='UNIQUENESS'", "schema=(:P {a})");

      final Neo4jException sameNameOtherDefinition = failure(session, "CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.b IS UNIQUE");
      assertThat(sameNameOtherDefinition.code()).isEqualTo(SCHEMA + "ConstraintWithNameAlreadyExists");

      // IF NOT EXISTS is a no-op in every case, as in Neo4j
      session.run("CREATE CONSTRAINT c1 IF NOT EXISTS FOR (n:P) REQUIRE n.a IS UNIQUE").consume();
      session.run("CREATE CONSTRAINT c2 IF NOT EXISTS FOR (n:P) REQUIRE n.a IS UNIQUE").consume();
    }
  }

  /** An unnamed constraint is named from its definition, so asking for it twice asks for the same name twice. */
  @Test
  void anUnnamedConstraintAskedForTwiceIsAnEquivalentRule() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT FOR (n:Q) REQUIRE n.a IS UNIQUE").consume();
      assertThat(failure(session, "CREATE CONSTRAINT FOR (n:Q) REQUIRE n.a IS UNIQUE").code())
          .isEqualTo(SCHEMA + "EquivalentSchemaRuleAlreadyExists");
    }
  }

  @Test
  void aNodeKeyOverAnExistingKeyUnderAnotherNameIsAConstraintThatExists() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT k1 FOR (n:K) REQUIRE n.a IS NODE KEY").consume();
      final Neo4jException otherName = failure(session, "CREATE CONSTRAINT k2 FOR (n:K) REQUIRE n.a IS NODE KEY");
      assertThat(otherName.code()).isEqualTo(SCHEMA + "ConstraintAlreadyExists");
      assertThat(otherName.getMessage()).contains("name='k1'", "type='NODE_KEY'");
      assertThat(failure(session, "CREATE CONSTRAINT k1 FOR (n:K) REQUIRE n.a IS NODE KEY").code())
          .isEqualTo(SCHEMA + "EquivalentSchemaRuleAlreadyExists");
    }
  }

  @Test
  void anExistingRangeIndexIsAnsweredAsNeo4jAnswersIt() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE INDEX i1 FOR (n:P) ON (n.name)").consume();
      assertIndexConflicts(session, "CREATE INDEX i1 FOR (n:P) ON (n.name)", "CREATE INDEX i2 FOR (n:P) ON (n.name)",
          "CREATE INDEX i1 FOR (n:P) ON (n.other)", "(:P {name})");
    }
  }

  @Test
  void anUnnamedIndexAskedForTwiceIsAnEquivalentRule() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE INDEX FOR (n:R) ON (n.name)").consume();
      assertThat(failure(session, "CREATE INDEX FOR (n:R) ON (n.name)").code()).isEqualTo(SCHEMA + "EquivalentSchemaRuleAlreadyExists");
    }
  }

  @Test
  void anExistingFullTextIndexIsAnsweredAsNeo4jAnswersIt() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE FULLTEXT INDEX f1 FOR (n:P) ON EACH [n.body]").consume();
      assertIndexConflicts(session, "CREATE FULLTEXT INDEX f1 FOR (n:P) ON EACH [n.body]",
          "CREATE FULLTEXT INDEX f2 FOR (n:P) ON EACH [n.body]", "CREATE FULLTEXT INDEX f1 FOR (n:P) ON EACH [n.title]",
          "(:P {body})");
    }
  }

  @Test
  void anExistingRelationshipFullTextIndexIsAnsweredAsNeo4jAnswersIt() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE FULLTEXT INDEX rf1 FOR ()-[r:REL]-() ON EACH [r.body]").consume();
      assertIndexConflicts(session, "CREATE FULLTEXT INDEX rf1 FOR ()-[r:REL]-() ON EACH [r.body]",
          "CREATE FULLTEXT INDEX rf2 FOR ()-[r:REL]-() ON EACH [r.body]", "CREATE FULLTEXT INDEX rf1 FOR ()-[r:REL]-() ON EACH [r.title]",
          "()-[:REL {body}]-()");
    }
  }

  @Test
  void anExistingVectorIndexIsAnsweredAsNeo4jAnswersIt() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run(vectorIndex("v1", "emb")).consume();
      assertIndexConflicts(session, vectorIndex("v1", "emb"), vectorIndex("v2", "emb"), vectorIndex("v1", "emb2"), "(:P {emb})");
    }
  }

  @Test
  void aDuplicatedNodeKeyIsWordedAsNeo4jWordsIt() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT n_u FOR (n:Human) REQUIRE n.name IS UNIQUE").consume();
      session.run("CREATE CONSTRAINT n_c FOR (n:Human) REQUIRE (n.a, n.b) IS UNIQUE").consume();
      session.run("CREATE (:Human {name: 'Tim', a: 1, b: 1})").consume();

      final Neo4jException oneProperty = failure(session, "CREATE (:Human {name: 'Tim'})");
      assertThat(oneProperty.code()).isEqualTo(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR);
      assertThat(oneProperty.getMessage()).matches("Node\\(-?\\d+\\) already exists with label `Human` and property `name` = 'Tim'.*")
          .contains("Duplicated key");

      final Neo4jException twoProperties = failure(session, "CREATE (:Human {name: 'Other', a: 1, b: 1})");
      assertThat(twoProperties.getMessage())
          .matches("Node\\(-?\\d+\\) already exists with label `Human` and properties `a` = 1, `b` = 1.*");

      final Neo4jException viaSet = failure(session,
          "CREATE (:Human {name: 'Z'}) WITH 1 AS x MATCH (n:Human {name: 'Z'}) SET n.name = 'Tim'");
      assertThat(viaSet.getMessage()).matches("Node\\(-?\\d+\\) already exists with label `Human` and property `name` = 'Tim'.*");
    }
  }

  @Test
  void aDuplicatedRelationshipKeyIsWordedAsNeo4jWordsIt() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT r_u FOR ()-[r:KNOWS]-() REQUIRE r.uid IS UNIQUE").consume();
      session.run("CREATE (:Human {name: 'p1'})-[:KNOWS {uid: 'u'}]->(:Human {name: 'p2'})").consume();

      final Neo4jException duplicate = failure(session,
          "MATCH (a:Human {name: 'p1'}), (b:Human {name: 'p2'}) CREATE (a)-[:KNOWS {uid: 'u'}]->(b)");
      assertThat(duplicate.code()).isEqualTo(BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR);
      assertThat(duplicate.getMessage()).matches("Relationship\\(-?\\d+\\) already exists with type `KNOWS` and property `uid` = 'u'.*");
    }
  }

  private static void assertIndexConflicts(final Session session, final String sameNameSameDefinition,
      final String otherNameSameDefinition, final String sameNameOtherDefinition, final String schema) {
    final Neo4jException equivalent = failure(session, sameNameSameDefinition);
    assertThat(equivalent.code()).as(sameNameSameDefinition).isEqualTo(SCHEMA + "EquivalentSchemaRuleAlreadyExists");
    assertThat(equivalent.getMessage()).startsWith("An equivalent index already exists");

    final Neo4jException otherName = failure(session, otherNameSameDefinition);
    assertThat(otherName.code()).as(otherNameSameDefinition).isEqualTo(SCHEMA + "IndexAlreadyExists");
    assertThat(otherName.getMessage()).startsWith("There already exists an index " + schema + ".");

    assertThat(failure(session, sameNameOtherDefinition).code()).as(sameNameOtherDefinition)
        .isEqualTo(SCHEMA + "IndexWithNameAlreadyExists");
  }

  private static String vectorIndex(final String name, final String property) {
    return "CREATE VECTOR INDEX " + name + " FOR (n:P) ON n." + property
        + " OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }";
  }

  private static Neo4jException failure(final Session session, final String statement) {
    final Neo4jException error = catchThrowableOfType(Neo4jException.class, () -> session.run(statement).consume());
    assertThat(error).as(statement).isNotNull();
    return error;
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
