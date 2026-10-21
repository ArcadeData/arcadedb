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
    assertThat(mandatory("T134", "v")).isTrue();

    final QueryStatistics stats = stats("DROP CONSTRAINT cname");

    assertThat(stats.getConstraintsRemoved()).isEqualTo(1);
    assertThat(mandatory("T134", "v")).isFalse();
    database.transaction(() -> database.newVertex("T134").save());
  }

  @Test
  void dropNamedNodeKeyConstraintRemovesIndexAndMandatoryFlag() {
    command("CREATE CONSTRAINT cname FOR (n:T473) REQUIRE n.v IS NODE KEY");
    assertThat(uniqueIndexOn("T473", "v")).isTrue();
    assertThat(mandatory("T473", "v")).isTrue();

    final QueryStatistics stats = stats("DROP CONSTRAINT cname");

    assertThat(stats.getConstraintsRemoved()).isEqualTo(1);
    assertThat(uniqueIndexOn("T473", "v")).isFalse();
    assertThat(mandatory("T473", "v")).isFalse();
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
    // the name is checked before anything is created, the type included
    assertThat(database.getSchema().existsType("T4")).isFalse();
  }

  @Test
  void mandatoryFlagSurvivesWhileAnotherConstraintStillRequiresIt() {
    command("CREATE CONSTRAINT nn FOR (n:T5) REQUIRE n.v IS NOT NULL");
    command("CREATE CONSTRAINT key FOR (n:T5) REQUIRE n.v IS NODE KEY");

    command("DROP CONSTRAINT nn");
    // the NODE KEY still requires the property
    assertThat(mandatory("T5", "v")).isTrue();
    assertThat(uniqueIndexOn("T5", "v")).isTrue();

    command("DROP CONSTRAINT key");
    assertThat(mandatory("T5", "v")).isFalse();
    assertThat(uniqueIndexOn("T5", "v")).isFalse();
  }

  @Test
  void sharedUniqueIndexSurvivesWhileAnotherConstraintStillUsesIt() {
    command("CREATE CONSTRAINT u FOR (n:T6) REQUIRE n.v IS UNIQUE");
    command("CREATE CONSTRAINT k IF NOT EXISTS FOR (n:T6) REQUIRE n.v IS NODE KEY");

    command("DROP CONSTRAINT u");
    assertThat(uniqueIndexOn("T6", "v")).isTrue();
    assertThat(mandatory("T6", "v")).isTrue();

    command("DROP CONSTRAINT k");
    assertThat(uniqueIndexOn("T6", "v")).isFalse();
    assertThat(mandatory("T6", "v")).isFalse();
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

    assertThat(mandatory("T7", "v")).isTrue();
    assertThat(uniqueIndexOn("T7", "w")).isTrue();
  }

  @Test
  void namedConstraintSurvivesReopen() {
    command("CREATE CONSTRAINT cname FOR (n:T8) REQUIRE n.v IS NODE KEY");

    reopenDatabase();

    command("DROP CONSTRAINT cname");
    assertThat(uniqueIndexOn("T8", "v")).isFalse();
    assertThat(mandatory("T8", "v")).isFalse();
  }

  @Test
  void compositeConstraintsTransferTheirIndex() {
    command("CREATE CONSTRAINT u FOR (n:T10) REQUIRE (n.a, n.b) IS UNIQUE");
    command("CREATE CONSTRAINT k IF NOT EXISTS FOR (n:T10) REQUIRE (n.a, n.b) IS NODE KEY");

    command("DROP CONSTRAINT u");
    assertThat(uniqueIndexOn("T10", "a", "b")).isTrue();
    assertThat(mandatory("T10", "a")).isTrue();
    assertThat(mandatory("T10", "b")).isTrue();

    command("DROP CONSTRAINT k");
    assertThat(uniqueIndexOn("T10", "a", "b")).isFalse();
    assertThat(mandatory("T10", "a")).isFalse();
    assertThat(mandatory("T10", "b")).isFalse();
  }

  @Test
  void droppingTheTypeReleasesTheName() {
    command("CREATE CONSTRAINT cname FOR (n:T11) REQUIRE n.v IS UNIQUE");
    database.getSchema().dropType("T11");

    command("CREATE CONSTRAINT cname FOR (n:T12) REQUIRE n.v IS UNIQUE");
    command("DROP CONSTRAINT cname");
    assertThat(uniqueIndexOn("T12", "v")).isFalse();
  }

  @Test
  void dropAfterTheIndexWasRemovedElsewhereStillSucceeds() {
    command("CREATE CONSTRAINT cname FOR (n:T13) REQUIRE n.v IS NODE KEY");
    final String indexName = database.getSchema().getType("T13").getAllIndexes(false).iterator().next().getName();
    database.command("sql", "DROP INDEX `" + indexName + "`");

    assertThat(stats("DROP CONSTRAINT cname").getConstraintsRemoved()).isEqualTo(1);
    assertThat(mandatory("T13", "v")).isFalse();
  }

  /**
   * A named IS TYPED constraint that declared the property lifts the type restriction when dropped (#9706): the property is
   * undeclared again.
   */
  @Test
  void dropNamedTypedConstraintLiftsTheDeclaredType() {
    command("CREATE CONSTRAINT t FOR (n:T14) REQUIRE n.v IS TYPED INTEGER");
    assertThat(property("T14", "v").getType()).isEqualTo(Type.LONG);

    assertThat(stats("DROP CONSTRAINT t").getConstraintsRemoved()).isEqualTo(1);
    assertThat(database.getSchema().getType("T14").getPropertyIfExists("v")).isNull();
    assertThatThrownBy(() -> command("DROP CONSTRAINT t")).rootCause().isInstanceOf(SchemaException.class);
  }

  @Test
  void ifNotExistsOverAnEquivalentUnnamedConstraintClaimsNoName() {
    command("CREATE CONSTRAINT FOR (n:T15) REQUIRE n.v IS NOT NULL");

    final QueryStatistics created = stats("CREATE CONSTRAINT nn IF NOT EXISTS FOR (n:T15) REQUIRE n.v IS NOT NULL");
    assertThat(created.getConstraintsAdded()).isZero();

    // nothing was created under that name, so there is nothing to drop and the flag stays
    assertThatThrownBy(() -> command("DROP CONSTRAINT nn")).rootCause().isInstanceOf(SchemaException.class);
    assertThat(mandatory("T15", "v")).isTrue();
  }

  @Test
  void anUnreadableEntryDoesNotBlockDroppingAnotherConstraint() {
    command("CREATE CONSTRAINT a FOR (n:T16) REQUIRE n.v IS NOT NULL");
    command("CREATE CONSTRAINT b FOR (n:T16) REQUIRE n.w IS NOT NULL");
    database.getSchema().getType("T16").setCustomValue("cypher.constraint.b", "not json {");

    command("DROP CONSTRAINT a");
    assertThat(mandatory("T16", "v")).isFalse();

    // the unreadable one can still be dropped by name; it owns nothing it can be trusted to undo
    command("DROP CONSTRAINT b");
    assertThatThrownBy(() -> command("DROP CONSTRAINT b")).rootCause().isInstanceOf(SchemaException.class);
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

  /** Whether the property is declared MANDATORY; a property a drop left undeclared is not. */
  private boolean mandatory(final String typeName, final String propertyName) {
    final Property p = database.getSchema().getType(typeName).getPropertyIfExists(propertyName);
    return p != null && p.isMandatory();
  }

  private boolean uniqueIndexOn(final String typeName, final String... propertyNames) {
    for (final TypeIndex idx : database.getSchema().getType(typeName).getAllIndexes(false))
      if (idx.isUnique() && idx.getPropertyNames().equals(List.of(propertyNames)))
        return true;
    return false;
  }
}
