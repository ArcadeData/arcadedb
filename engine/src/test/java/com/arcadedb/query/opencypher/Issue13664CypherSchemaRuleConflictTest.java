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
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.EquivalentIndexException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #13664: a Cypher {@code CREATE INDEX} / {@code CREATE CONSTRAINT} over a rule that exists says which of Neo4j's three
 * cases it is - the same rule under the same name, another rule under the same name, the same rule under another name - so a
 * wire protocol can answer it as Neo4j does.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13664CypherSchemaRuleConflictTest extends TestHelper {

  @Test
  void aConstraintSaysWhetherTheNameOrTheDefinitionIsTaken() {
    cypher("CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE");

    final AlreadyExistsException sameName = taken("CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE");
    assertThat(sameName.getKind()).isEqualTo(AlreadyExistsException.Kind.CONSTRAINT);
    assertThat(sameName.isEquivalent()).isTrue();
    assertThat(sameName).hasMessageStartingWith("An equivalent constraint already exists");

    final AlreadyExistsException otherDefinition = taken("CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.b IS UNIQUE");
    assertThat(otherDefinition.isEquivalent()).isFalse();
    // another kind over the same properties is another definition too
    assertThat(taken("CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS NOT NULL").isEquivalent()).isFalse();
    assertThat(taken("CREATE CONSTRAINT c1 FOR (n:Other) REQUIRE n.a IS UNIQUE").isEquivalent()).isFalse();

    final EquivalentIndexException otherName = equivalent("CREATE CONSTRAINT c2 FOR (n:P) REQUIRE n.a IS UNIQUE");
    assertThat(otherName.getKind()).isEqualTo(AlreadyExistsException.Kind.CONSTRAINT);
    assertThat(otherName.isUnderAnotherName()).isTrue();
    assertThat(otherName).hasMessageStartingWith("Constraint already exists: Constraint( name='c1', type='UNIQUENESS', schema=(:P {a}) )");
  }

  @Test
  void aTypeConstraintIsTheSameOnlyWithTheSameType() {
    cypher("CREATE CONSTRAINT t1 FOR (n:T) REQUIRE n.a IS :: INTEGER");
    assertThat(taken("CREATE CONSTRAINT t1 FOR (n:T) REQUIRE n.a IS :: INTEGER").isEquivalent()).isTrue();
    assertThat(taken("CREATE CONSTRAINT t1 FOR (n:T) REQUIRE n.a IS :: STRING").isEquivalent()).isFalse();
  }

  @Test
  void anIndexSaysWhetherTheNameOrTheDefinitionIsTaken() {
    cypher("CREATE INDEX i1 FOR (n:P) ON (n.name)");

    final AlreadyExistsException sameName = taken("CREATE INDEX i1 FOR (n:P) ON (n.name)");
    assertThat(sameName.getKind()).isEqualTo(AlreadyExistsException.Kind.INDEX);
    assertThat(sameName.isEquivalent()).isTrue();

    assertThat(taken("CREATE INDEX i1 FOR (n:P) ON (n.other)").isEquivalent()).isFalse();
    assertThat(taken("CREATE FULLTEXT INDEX i1 FOR (n:P) ON EACH [n.name]").isEquivalent()).isFalse();

    final EquivalentIndexException otherName = equivalent("CREATE INDEX i2 FOR (n:P) ON (n.name)");
    assertThat(otherName.getKind()).isEqualTo(AlreadyExistsException.Kind.INDEX);
    assertThat(otherName.isUnderAnotherName()).isTrue();
    assertThat(otherName).hasMessageStartingWith("There already exists an index (:P {name}).");
  }

  /** A name taken by a constraint is never an equivalent index, and the other way round. */
  @Test
  void anIndexAndAConstraintAreNeverEquivalent() {
    cypher("CREATE CONSTRAINT u1 FOR (n:U) REQUIRE n.a IS UNIQUE");
    final AlreadyExistsException indexOverConstraint = taken("CREATE INDEX u1 FOR (n:U) ON (n.a)");
    assertThat(indexOverConstraint.getKind()).isEqualTo(AlreadyExistsException.Kind.CONSTRAINT);
    assertThat(indexOverConstraint.isEquivalent()).isFalse();

    cypher("CREATE INDEX x1 FOR (n:X) ON (n.a)");
    final AlreadyExistsException constraintOverIndex = taken("CREATE CONSTRAINT x1 FOR (n:X) REQUIRE n.a IS UNIQUE");
    assertThat(constraintOverIndex.getKind()).isEqualTo(AlreadyExistsException.Kind.INDEX);
    assertThat(constraintOverIndex.isEquivalent()).isFalse();
  }

  @Test
  void anUnnamedRuleAskedForTwiceIsTheSameRuleUnderTheSameName() {
    cypher("CREATE INDEX FOR (n:N) ON (n.a)");
    assertThat(equivalent("CREATE INDEX FOR (n:N) ON (n.a)").isUnderAnotherName()).isFalse();

    cypher("CREATE CONSTRAINT FOR (n:M) REQUIRE n.a IS UNIQUE");
    final EquivalentIndexException constraint = equivalent("CREATE CONSTRAINT FOR (n:M) REQUIRE n.a IS UNIQUE");
    assertThat(constraint.getKind()).isEqualTo(AlreadyExistsException.Kind.CONSTRAINT);
    assertThat(constraint.isUnderAnotherName()).isFalse();
  }

  @Test
  void aRelationshipRuleIsWrittenAsARelationshipPattern() {
    cypher("CREATE CONSTRAINT r1 FOR ()-[r:KNOWS]-() REQUIRE r.uid IS UNIQUE");
    assertThat(equivalent("CREATE CONSTRAINT r2 FOR ()-[r:KNOWS]-() REQUIRE r.uid IS UNIQUE"))
        .hasMessageStartingWith("Constraint already exists: Constraint( name='r1', type='RELATIONSHIP_UNIQUENESS', "
            + "schema=()-[:KNOWS {uid}]-() )");
  }

  private void cypher(final String statement) {
    database.command("opencypher", statement).close();
  }

  private AlreadyExistsException taken(final String statement) {
    final Throwable error = catchThrowable(() -> cypher(statement));
    final AlreadyExistsException taken = CauseChain.find(error, AlreadyExistsException.class);
    assertThat(taken).as(statement).isNotNull();
    return taken;
  }

  private EquivalentIndexException equivalent(final String statement) {
    final Throwable error = catchThrowable(() -> cypher(statement));
    final EquivalentIndexException equivalent = CauseChain.find(error, EquivalentIndexException.class);
    assertThat(equivalent).as(statement).isNotNull();
    return equivalent;
  }
}
