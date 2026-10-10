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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9646: {@code DROP CONSTRAINT <name>} must undo what {@code CREATE CONSTRAINT <name>} did
 * (the unique index for UNIQUE, the MANDATORY flag for NOT NULL, both for NODE KEY), and must raise on an unknown name
 * unless {@code IF EXISTS} is given.
 */
class CypherDropNamedConstraintTest extends TestHelper {

  @Test
  void dropNamedUniqueConstraintRemovesTheIndex() {
    command("CREATE CONSTRAINT cname FOR (n:T599) REQUIRE n.v IS UNIQUE");
    assertThat(uniqueIndexOn("T599", "v")).isTrue();

    final QueryStatistics stats = stats("DROP CONSTRAINT cname");

    assertThat(stats.getConstraintsRemoved()).isEqualTo(1);
    assertThat(uniqueIndexOn("T599", "v")).isFalse();
    // the duplicate the constraint used to refuse is accepted now
    database.transaction(() -> {
      database.newVertex("T599").set("v", "a").save();
      database.newVertex("T599").set("v", "a").save();
    });
  }

  @Test
  void dropNamedNotNullConstraintClearsTheMandatoryFlag() {
    command("CREATE CONSTRAINT cname FOR (n:T134) REQUIRE n.v IS NOT NULL");
    assertThat(property("T134", "v").isMandatory()).isTrue();

    final QueryStatistics stats = stats("DROP CONSTRAINT cname");

    assertThat(stats.getConstraintsRemoved()).isEqualTo(1);
    assertThat(property("T134", "v").isMandatory()).isFalse();
    database.transaction(() -> database.newVertex("T134").save());
  }

  @Test
  void dropNamedNodeKeyConstraintRemovesIndexAndMandatoryFlag() {
    command("CREATE CONSTRAINT cname FOR (n:T473) REQUIRE n.v IS NODE KEY");
    assertThat(uniqueIndexOn("T473", "v")).isTrue();
    assertThat(property("T473", "v").isMandatory()).isTrue();

    final QueryStatistics stats = stats("DROP CONSTRAINT cname");

    assertThat(stats.getConstraintsRemoved()).isEqualTo(1);
    assertThat(uniqueIndexOn("T473", "v")).isFalse();
    assertThat(property("T473", "v").isMandatory()).isFalse();
  }

  @Test
  void dropNamedRelationshipConstraint() {
    command("CREATE CONSTRAINT rname FOR ()-[r:R9646]-() REQUIRE r.v IS UNIQUE");
    assertThat(uniqueIndexOn("R9646", "v")).isTrue();

    command("DROP CONSTRAINT rname");

    assertThat(uniqueIndexOn("R9646", "v")).isFalse();
  }

  @Test
  void dropUnknownConstraintRaises() {
    assertThatThrownBy(() -> command("DROP CONSTRAINT nosuchconstraint"))
        .rootCause().isInstanceOf(SchemaException.class)
        .hasMessageContaining("nosuchconstraint");
  }

  @Test
  void dropUnknownConstraintIfExistsIsANoOp() {
    final QueryStatistics stats = stats("DROP CONSTRAINT nosuchconstraint IF EXISTS");
    assertThat(stats.getConstraintsRemoved()).isZero();
  }

  @Test
  void droppingTwiceRaisesTheSecondTime() {
    command("CREATE CONSTRAINT cname FOR (n:T1) REQUIRE n.v IS UNIQUE");
    command("DROP CONSTRAINT cname");

    assertThatThrownBy(() -> command("DROP CONSTRAINT cname")).rootCause().isInstanceOf(SchemaException.class);
    assertThat(stats("DROP CONSTRAINT cname IF EXISTS").getConstraintsRemoved()).isZero();
  }

  @Test
  void createWithAnExistingNameRaisesAndChangesNothing() {
    command("CREATE CONSTRAINT cname FOR (n:T2) REQUIRE n.v IS UNIQUE");

    assertThatThrownBy(() -> command("CREATE CONSTRAINT cname FOR (n:T2) REQUIRE n.w IS NOT NULL"))
        .rootCause().isInstanceOf(SchemaException.class)
        .hasMessageContaining("cname");
    assertThat(database.getSchema().getType("T2").getPropertyIfExists("w")).isNull();

    // IF NOT EXISTS with a name already taken is a no-op
    final QueryStatistics stats = stats("CREATE CONSTRAINT cname IF NOT EXISTS FOR (n:T2) REQUIRE n.w IS NOT NULL");
    assertThat(stats.getConstraintsAdded()).isZero();
    assertThat(database.getSchema().getType("T2").getPropertyIfExists("w")).isNull();
  }

  @Test
  void theNameIsUniqueAcrossTypes() {
    command("CREATE CONSTRAINT cname FOR (n:T3) REQUIRE n.v IS UNIQUE");
    assertThatThrownBy(() -> command("CREATE CONSTRAINT cname FOR (n:T4) REQUIRE n.v IS UNIQUE"))
        .rootCause().isInstanceOf(SchemaException.class);
    assertThat(database.getSchema().existsType("T4") ? uniqueIndexOn("T4", "v") : false).isFalse();
  }

  @Test
  void mandatoryFlagSurvivesWhileAnotherConstraintStillRequiresIt() {
    command("CREATE CONSTRAINT nn FOR (n:T5) REQUIRE n.v IS NOT NULL");
    command("CREATE CONSTRAINT key FOR (n:T5) REQUIRE n.v IS NODE KEY");

    command("DROP CONSTRAINT nn");
    // the NODE KEY still requires the property
    assertThat(property("T5", "v").isMandatory()).isTrue();
    assertThat(uniqueIndexOn("T5", "v")).isTrue();

    command("DROP CONSTRAINT key");
    assertThat(property("T5", "v").isMandatory()).isFalse();
    assertThat(uniqueIndexOn("T5", "v")).isFalse();
  }

  @Test
  void sharedUniqueIndexSurvivesWhileAnotherConstraintStillUsesIt() {
    command("CREATE CONSTRAINT u FOR (n:T6) REQUIRE n.v IS UNIQUE");
    command("CREATE CONSTRAINT k IF NOT EXISTS FOR (n:T6) REQUIRE n.v IS NODE KEY");

    command("DROP CONSTRAINT u");
    assertThat(uniqueIndexOn("T6", "v")).isTrue();
    assertThat(property("T6", "v").isMandatory()).isTrue();

    command("DROP CONSTRAINT k");
    assertThat(uniqueIndexOn("T6", "v")).isFalse();
    assertThat(property("T6", "v").isMandatory()).isFalse();
  }

  @Test
  void dropDoesNotUndoStateTheConstraintDidNotCreate() {
    // MANDATORY and the unique index declared outside Cypher before the constraint existed
    final DocumentType type = database.getSchema().createVertexType("T7");
    type.createProperty("v", Type.STRING).setMandatory(true);
    type.createProperty("w", Type.STRING);
    database.getSchema().buildTypeIndex("T7", new String[] { "w" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    command("CREATE CONSTRAINT nn FOR (n:T7) REQUIRE n.v IS NOT NULL");
    command("CREATE CONSTRAINT u IF NOT EXISTS FOR (n:T7) REQUIRE n.w IS UNIQUE");
    command("DROP CONSTRAINT nn");
    // the unique index was already there, so the IF NOT EXISTS was a no-op that claimed no name
    assertThat(stats("DROP CONSTRAINT u IF EXISTS").getConstraintsRemoved()).isZero();

    assertThat(property("T7", "v").isMandatory()).isTrue();
    assertThat(uniqueIndexOn("T7", "w")).isTrue();
  }

  @Test
  void namedConstraintSurvivesReopen() {
    command("CREATE CONSTRAINT cname FOR (n:T8) REQUIRE n.v IS NODE KEY");

    reopenDatabase();

    command("DROP CONSTRAINT cname");
    assertThat(uniqueIndexOn("T8", "v")).isFalse();
    assertThat(property("T8", "v").isMandatory()).isFalse();
  }

  @Test
  void dropByIndexNameStillWorks() {
    command("CREATE CONSTRAINT FOR (n:T9) REQUIRE n.v IS UNIQUE");
    final String indexName = database.getSchema().getType("T9").getAllIndexes(false).iterator().next().getName();

    command("DROP CONSTRAINT `" + indexName + "`");

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
      final Optional<QueryStatistics> s = rs.getStatistics();
      assertThat(s).isPresent();
      return s.get();
    }
  }

  private Property property(final String typeName, final String propertyName) {
    final Property p = database.getSchema().getType(typeName).getPropertyIfExists(propertyName);
    assertThat(p).isNotNull();
    return p;
  }

  private boolean uniqueIndexOn(final String typeName, final String propertyName) {
    for (final TypeIndex idx : database.getSchema().getType(typeName).getAllIndexes(false))
      if (idx.isUnique() && idx.getPropertyNames().equals(List.of(propertyName)))
        return true;
    return false;
  }
}
