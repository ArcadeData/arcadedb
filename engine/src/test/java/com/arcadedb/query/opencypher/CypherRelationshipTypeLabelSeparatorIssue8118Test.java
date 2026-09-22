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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #8118, a follow-up to #8100: a relationship type carrying
 * {@link Labels#LABEL_SEPARATOR} ({@code ~}) is cosmetic on its own - an edge type is never composite - but it
 * collides with the vertex namespace ArcadeDB shares between vertex and edge types. Once an edge type named
 * e.g. {@code A~B} exists, the composite vertex type that the ordinary label set {@code [A, B]} computes can
 * never be created again, so {@code CREATE (n:A:B)} fails for good.
 * <p>
 * Every write path that can introduce a brand-new edge type now routes the name through
 * {@link Labels#requireUsableRelationshipTypeName}, guarded by {@code !schema.existsType(...)} so that writing to,
 * or indexing, an edge type that already exists keeps working regardless of its name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRelationshipTypeLabelSeparatorIssue8118Test extends TestHelper {

  @Test
  void createPatternRejectsARelationshipTypeCarryingTheSeparator() {
    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "CREATE (a:P {id:1})-[:`A~B`]->(b:P {id:2})").close()))
        .hasMessageContaining("~");
  }

  @Test
  void mergePatternRejectsARelationshipTypeCarryingTheSeparator() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id:1}), (:P {id:2})").close());

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (a:P {id:1}), (b:P {id:2}) MERGE (a)-[:`A~B`]->(b)").close()))
        .hasMessageContaining("~");
  }

  @Test
  void mergeRelationshipProcedureRejectsARelationshipTypeCarryingTheSeparator() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id:1}), (:P {id:2})").close());

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", """
        MATCH (a:P {id:1}), (b:P {id:2})
        CALL merge.relationship(a, 'A~B', {}, {}, b) YIELD rel
        RETURN rel
        """).close())).hasMessageContaining("~");
  }

  @Test
  void createIndexOnARelationshipTypeCarryingTheSeparatorIsRejected() {
    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "CREATE INDEX FOR ()-[r:`A~B`]-() ON (r.id)").close()))
        .hasMessageContaining("~");
  }

  @Test
  void createConstraintOnARelationshipTypeCarryingTheSeparatorIsRejected() {
    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "CREATE CONSTRAINT FOR ()-[r:`A~B`]-() REQUIRE r.id IS UNIQUE").close()))
        .hasMessageContaining("~");
  }

  /**
   * The whole point of the guard: once it refuses the colliding edge type, the composite vertex type for the
   * same label set must remain creatable.
   */
  @Test
  void theCompositeVertexTypeStaysCreatableAfterTheRelationshipTypeIsRejected() {
    assertThatThrownBy(() -> database.transaction(() ->
        database.command("opencypher", "CREATE (a:P {id:1})-[:`A~B`]->(b:P {id:2})").close()));

    database.transaction(() -> database.command("opencypher", "CREATE (n:A:B {id:9}) RETURN n").close());

    assertThat(database.getSchema().existsType("A~B")).isTrue();
    assertThat(database.getSchema().getType("A~B")).isInstanceOf(com.arcadedb.schema.VertexType.class);
  }

  /**
   * The guard only applies on the path that creates a brand-new type. An edge type that already exists -
   * however it got its name - must remain fully usable: a second edge of the same type is not re-validated.
   */
  @Test
  void anAlreadyExistingRelationshipTypeCarryingTheSeparatorStaysUsable() {
    database.getSchema().createEdgeType("Pre~Existing");

    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {id:1})-[:`Pre~Existing`]->(b:P {id:2})").close());

    assertThat(database.countType("Pre~Existing", true)).isEqualTo(1);
  }
}
