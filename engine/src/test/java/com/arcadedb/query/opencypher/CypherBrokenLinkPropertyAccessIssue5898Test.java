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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5898: dereferencing a node-valued property whose stored LINK/RID no
 * longer resolves must surface as a Cypher-flavored {@link CommandExecutionException}
 * ("TypeError: Cannot access property ...") rather than a raw {@link RecordNotFoundException},
 * which the HTTP command API turns into an unfriendly 500 instead of a client error.
 * <p>
 * Two property-access paths dereference a persisted LINK and both are covered here:
 * <ul>
 *   <li>the variable-bound path, {@code WITH holder.ref AS r RETURN r.id}
 *       ({@code PropertyAccessExpression});</li>
 *   <li>the chained path, {@code holder.ref.id}, reachable from ordinary Cypher since #5893
 *       ({@code CypherExpressionBuilder.ChainedPropertyAccessExpression}).</li>
 * </ul>
 * A LINK to a non-vertex record (a plain document or an edge) is not broken at all - it is an
 * ordinary property-bearing record - so both paths must read through it exactly like the adjacent
 * {@code instanceof Document} branch already does for a live value.
 */
class CypherBrokenLinkPropertyAccessIssue5898Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherbrokenlink5898").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void chainedAccessOnDanglingLinkRaisesCypherTypeError() {
    createHolderWithDanglingLink();

    database.transaction(() -> assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) RETURN holder.ref.id AS referencedId")) {
        rs.stream().forEach(r -> {
        });
      }
    })
        .as("a dangling LINK must not escape as a raw RecordNotFoundException")
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("TypeError: Cannot access property 'id'")
        .hasMessageContaining("the linked record does not exist")
        // AbstractServerHttpHandler classifies a CommandExecutionException by its cause when it has one, so keeping
        // the RecordNotFoundException as the cause would route the HTTP response back to the generic
        // "Error on transaction commit" arm this fix exists to avoid.
        .as("the engine-level cause must not be attached")
        .hasNoCause());
  }

  @Test
  void variableBoundAccessOnDanglingLinkRaisesCypherTypeError() {
    createHolderWithDanglingLink();

    database.transaction(() -> assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) WITH holder.ref AS r RETURN r.id AS referencedId")) {
        rs.stream().forEach(r -> {
        });
      }
    })
        .as("a dangling LINK must not escape as a raw RecordNotFoundException")
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("TypeError: Cannot access property 'id'")
        .hasMessageContaining("the linked record does not exist")
        .as("the engine-level cause must not be attached")
        .hasNoCause());
  }

  @Test
  void chainedAccessOnNonVertexLinkReadsThroughTheLink() {
    createHolderLinkingToDocument();

    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) RETURN holder.ref.id AS referencedId")) {
        assertThat(rs.hasNext()).isTrue();
        final Object referencedId = rs.next().getProperty("referencedId");
        assertThat(referencedId).as("a LINK to a plain document is a live, property-bearing record").isNotNull();
        assertThat(((Number) referencedId).intValue()).isEqualTo(99);
      }
    });
  }

  @Test
  void variableBoundAccessOnNonVertexLinkReadsThroughTheLink() {
    createHolderLinkingToDocument();

    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) WITH holder.ref AS r RETURN r.id AS referencedId")) {
        assertThat(rs.hasNext()).isTrue();
        final Object referencedId = rs.next().getProperty("referencedId");
        assertThat(referencedId).as("a LINK to a plain document is a live, property-bearing record").isNotNull();
        assertThat(((Number) referencedId).intValue()).isEqualTo(99);
      }
    });
  }

  @Test
  void chainedAccessOnEdgeLinkReadsThroughTheLink() {
    createHolderLinkingToEdge();

    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) RETURN holder.ref.id AS referencedId")) {
        assertThat(rs.hasNext()).isTrue();
        final Object referencedId = rs.next().getProperty("referencedId");
        assertThat(referencedId).as("a LINK to an edge is a live, property-bearing record").isNotNull();
        assertThat(((Number) referencedId).intValue()).isEqualTo(77);
      }
    });
  }

  @Test
  void variableBoundAccessOnEdgeLinkReadsThroughTheLink() {
    createHolderLinkingToEdge();

    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) WITH holder.ref AS r RETURN r.id AS referencedId")) {
        assertThat(rs.hasNext()).isTrue();
        final Object referencedId = rs.next().getProperty("referencedId");
        assertThat(referencedId).as("a LINK to an edge is a live, property-bearing record").isNotNull();
        assertThat(((Number) referencedId).intValue()).isEqualTo(77);
      }
    });
  }

  /**
   * Persists a holder vertex whose {@code ref} property is a LINK to a vertex that is then deleted,
   * leaving the RID dangling. The delete runs in its own transaction so the value read back is a
   * plain unresolvable RID and not the in-transaction deleted-entity marker.
   */
  private void createHolderWithDanglingLink() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (holder:T {role: 'holder'}), (target:T {role: 'target', id: 42}) "
            + "SET holder.ref = target RETURN holder").close());

    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE role = 'target'").close());
  }

  /**
   * Persists a holder vertex whose {@code ref} property is a LINK to a plain (non-vertex) document.
   */
  private void createHolderLinkingToDocument() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D");
      database.getSchema().getOrCreateVertexType("T");
    });

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("D");
      doc.set("id", 99);
      doc.save();
      final RID docRid = doc.getIdentity();

      final MutableVertex holder = database.newVertex("T");
      holder.set("role", "holder");
      holder.set("ref", docRid);
      holder.save();
    });
  }

  /**
   * Persists a holder vertex whose {@code ref} property is a LINK to an edge. An edge is stored differently from a
   * vertex or a plain document, so it gets its own fixture instead of being covered by analogy: what the fix relies on
   * is that all three are {@link com.arcadedb.database.Document}s, and only running one proves it.
   */
  private void createHolderLinkingToEdge() {
    database.transaction(() -> {
      database.getSchema().getOrCreateVertexType("T");
      database.getSchema().getOrCreateEdgeType("E");
    });

    database.transaction(() -> {
      final MutableVertex from = database.newVertex("T");
      from.set("role", "from");
      from.save();

      final MutableVertex to = database.newVertex("T");
      to.set("role", "to");
      to.save();

      final MutableEdge edge = from.newEdge("E", to, "id", 77);
      edge.save();

      final MutableVertex holder = database.newVertex("T");
      holder.set("role", "holder");
      holder.set("ref", edge.getIdentity());
      holder.save();
    });
  }
}
