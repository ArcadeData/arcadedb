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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9705: {@code CREATE INDEX <name>} ignored the name, so {@code DROP INDEX <name>} dropped nothing, and an unknown name
 * reported a removal without {@code IF EXISTS}. Index and constraint names share one namespace, as in Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9705CypherIndexNameTest extends TestHelper {

  @Test
  void createIndexNamesTheIndexAndDropIndexDropsIt() {
    final QueryStatistics created = stats("CREATE INDEX person_name FOR (n:Person) ON (n.name)");
    assertThat(created.getIndexesAdded()).isEqualTo(1);
    assertThat(database.getSchema().existsIndex("person_name")).isTrue();
    assertThat(database.getSchema().existsIndex("Person[name]")).isFalse();
    assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name").getName()).isEqualTo("person_name");

    // the index serves the queries under its name too
    command("CREATE (:Person {name: 'Ann'})");
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person) WHERE n.name = 'Ann' RETURN count(n) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    }

    final QueryStatistics dropped = stats("DROP INDEX person_name");
    assertThat(dropped.getIndexesRemoved()).isEqualTo(1);
    assertThat(database.getSchema().existsIndex("person_name")).isFalse();
    assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name")).isNull();
  }

  @Test
  void theNameSurvivesAReopen() {
    command("CREATE INDEX person_name FOR (n:Person) ON (n.name)");
    reopenDatabase();
    assertThat(database.getSchema().existsIndex("person_name")).isTrue();
    assertThat(stats("DROP INDEX person_name").getIndexesRemoved()).isEqualTo(1);
    assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name")).isNull();
  }

  @Test
  void unnamedIndexKeepsTheGeneratedName() {
    command("CREATE INDEX FOR (n:Person) ON (n.name)");
    assertThat(database.getSchema().existsIndex("Person[name]")).isTrue();
    assertThat(stats("DROP INDEX `Person[name]`").getIndexesRemoved()).isEqualTo(1);
    assertThat(database.getSchema().existsIndex("Person[name]")).isFalse();
  }

  @Test
  void dropOfAnUnknownIndexRaisesUnlessIfExists() {
    assertThatThrownBy(() -> command("DROP INDEX nosuchindex")).rootCause().isInstanceOf(SchemaException.class).hasMessageContaining("nosuchindex");
    assertThat(stats("DROP INDEX nosuchindex IF EXISTS").getIndexesRemoved()).isZero();
  }

  @Test
  void aTakenNameRaisesUnlessIfNotExists() {
    command("CREATE INDEX idx FOR (n:Person) ON (n.name)");

    // the same name on other properties
    assertThatThrownBy(() -> command("CREATE INDEX idx FOR (n:Person) ON (n.age)")).rootCause().isInstanceOf(SchemaException.class)
        .hasMessageContaining("idx");
    assertThat(database.getSchema().getType("Person").existsProperty("age")).isFalse();
    assertThat(stats("CREATE INDEX idx IF NOT EXISTS FOR (n:Person) ON (n.age)").getIndexesAdded()).isZero();
    assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("age")).isNull();

    // an equivalent index under another name: refused, or a no-op with IF NOT EXISTS
    assertThatThrownBy(() -> command("CREATE INDEX other FOR (n:Person) ON (n.name)")).isInstanceOf(RuntimeException.class);
    assertThat(stats("CREATE INDEX other IF NOT EXISTS FOR (n:Person) ON (n.name)").getIndexesAdded()).isZero();
    assertThat(database.getSchema().existsIndex("other")).isFalse();
    assertThat(database.getSchema().existsIndex("idx")).isTrue();
  }

  @Test
  void indexAndConstraintNamesShareOneNamespace() {
    command("CREATE CONSTRAINT c_email FOR (n:Person) REQUIRE n.email IS UNIQUE");
    assertThatThrownBy(() -> command("CREATE INDEX c_email FOR (n:Person) ON (n.name)")).rootCause().isInstanceOf(SchemaException.class)
        .hasMessageContaining("constraint");
    assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name")).isNull();

    command("CREATE INDEX i_name FOR (n:Person) ON (n.name)");
    assertThatThrownBy(() -> command("CREATE CONSTRAINT i_name FOR (n:Person) REQUIRE n.age IS UNIQUE"))
        .rootCause().isInstanceOf(SchemaException.class).hasMessageContaining("index");

    // a plain index is no constraint: DROP CONSTRAINT does not reach it
    assertThatThrownBy(() -> command("DROP CONSTRAINT i_name")).rootCause().isInstanceOf(SchemaException.class);
    assertThat(database.getSchema().existsIndex("i_name")).isTrue();
  }

  @Test
  void theIndexOfAConstraintIsDroppedThroughTheConstraint() {
    command("CREATE CONSTRAINT c_email FOR (n:Person) REQUIRE n.email IS UNIQUE");
    final String indexName = database.getSchema().getType("Person").getPolymorphicIndexByProperties("email").getName();

    assertThatThrownBy(() -> command("DROP INDEX `" + indexName + "`")).rootCause().isInstanceOf(SchemaException.class)
        .hasMessageContaining("c_email");
    assertThat(database.getSchema().existsIndex(indexName)).isTrue();

    command("DROP CONSTRAINT c_email");
    assertThat(database.getSchema().existsIndex(indexName)).isFalse();
  }

  private void command(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private QueryStatistics stats(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getStatistics().orElseThrow();
    }
  }
}
