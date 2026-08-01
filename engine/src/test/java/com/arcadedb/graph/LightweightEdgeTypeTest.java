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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code LIGHTWEIGHT} and {@code UNIQUE} declared on the edge type, through both the schema API and SQL.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LightweightEdgeTypeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().buildVertexType().withName("V").create());
  }

  @Test
  void newEdgeOnALightweightTypeStoresNoRecord() {
    database.transaction(
        () -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true).create());

    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> {
      final Edge edge = database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b);
      assertThat(edge).isInstanceOf(LightEdge.class);
      assertThat(edge.getIdentity().getPosition()).isNegative();
    });

    // the edge exists on the graph...
    database.transaction(() -> assertThat(database.lookupByRID(a, true).asVertex().countEdges(Vertex.DIRECTION.OUT))
        .isEqualTo(1));
    // ...but no record was allocated for it
    assertThat(count("select count(@rid) as c from Follows")).isZero();
    assertThat(count("select count(*) as c from Follows")).isZero();
  }

  @Test
  void aLightweightTypeRejectsProperties() {
    database.transaction(
        () -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true).create());

    final RID a = newVertex(0);
    final RID b = newVertex(1);

    assertThatThrownBy(() -> database.transaction(
        () -> database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b, "since", 2020)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LIGHTWEIGHT");

    assertThatThrownBy(() -> database.transaction(
        () -> database.getSchema().getType("Follows").createProperty("since", Type.INTEGER)))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("LIGHTWEIGHT");
  }

  @Test
  void theReturnedEdgeIsAMutableEdgeThatRefusesToMutate() {
    database.transaction(
        () -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true).create());

    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> {
      // The declared return type is unchanged, so existing code keeps compiling and running.
      final MutableEdge edge = database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b);

      assertThat(edge.getOut()).isEqualTo(a);
      assertThat(edge.getIn()).isEqualTo(b);
      assertThat(edge.isDirty()).isFalse();
      // save() is a no-op rather than a throw: the edge is already connected.
      assertThat(edge.save()).isSameAs(edge);

      assertThatThrownBy(() -> edge.set("since", 2020)).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("LIGHTWEIGHT");
    });
  }

  @Test
  void uniqueOnALightweightTypeRejectsTheSecondEdge() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true)
        .withUnique(true).create());

    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b));

    assertThatThrownBy(() -> database.transaction(
        () -> database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b)))
        .isInstanceOf(DuplicatedKeyException.class);

    // the reverse direction is a different ordered pair and stays allowed
    database.transaction(() -> database.lookupByRID(b, true).asVertex().modify().newEdge("Follows", a));

    database.transaction(() -> assertThat(database.lookupByRID(a, true).asVertex().countEdges(Vertex.DIRECTION.OUT))
        .isEqualTo(1));
  }

  @Test
  void uniqueOnARegularTypeIsEnforcedByAnIndex() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withUnique(true).create());

    assertThat(database.getSchema().existsIndex("Follows[@out,@in]")).isTrue();

    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b, "since", 2020));

    assertThatThrownBy(() -> database.transaction(
        () -> database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b, "since", 2021)))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void theIndexBackingAUniqueDeclarationCannotBeDropped() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withUnique(true).create());

    assertThatThrownBy(() -> database.getSchema().dropIndex("Follows[@out,@in]"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("unique = false");

    // withdrawing the declaration drops the index with it
    database.command("sql", "alter type Follows with unique = false").close();

    assertThat(database.getSchema().existsIndex("Follows[@out,@in]")).isFalse();
    assertThat(((EdgeType) database.getSchema().getType("Follows")).isUnique()).isFalse();
  }

  /**
   * The guard protects a user-issued DROP INDEX only. Dropping the type itself cascades to its indexes and must not
   * trip over its own constraint.
   */
  @Test
  void aUniqueEdgeTypeCanStillBeDropped() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withUnique(true).create());
    assertThat(database.getSchema().existsIndex("Follows[@out,@in]")).isTrue();

    database.getSchema().dropType("Follows");

    assertThat(database.getSchema().existsType("Follows")).isFalse();
    assertThat(database.getSchema().existsIndex("Follows[@out,@in]")).isFalse();
  }

  /**
   * Declaring UNIQUE on a populated regular type builds the index over existing data, so it reports the actual
   * duplicate rather than failing somewhere in its own rollback.
   */
  @Test
  void declaringUniqueOnATypeThatAlreadyHasDuplicatesReportsTheDuplicate() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").create());

    final RID a = newVertex(0);
    final RID b = newVertex(1);
    database.transaction(() -> {
      final MutableVertex source = database.lookupByRID(a, true).asVertex().modify();
      source.newEdge("Follows", b, "n", 1);
      source.newEdge("Follows", b, "n", 2);
    });

    // The index builder wraps it, but the duplicate is what surfaces as the root cause. What this pins is that the
    // failure reports the offending data rather than dying in its own rollback (the rollback drops the half-built
    // index, and that drop must not be blocked by the constraint it was being built for).
    assertThatThrownBy(() -> database.command("sql", "alter type Follows with unique = true").close())
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(database.getSchema().existsIndex("Follows[@out,@in]")).isFalse();
  }

  /**
   * A vertex that crosses the super-node threshold has its edge list striped, and {@link StripedEdgeList} routes
   * every lookup to the stripe that can hold the neighbour. UNIQUE has to be routed the same way: walking the base
   * chain of a striped list finds nothing, so the constraint would silently stop being enforced exactly on the
   * vertices where duplicates are most likely.
   */
  @Test
  void uniqueIsEnforcedOnASuperNode() {
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    try {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(16);

      database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true)
          .withUnique(true).create());

      final RID hub = newVertex(0);
      final RID[] targets = new RID[64];
      for (int i = 0; i < targets.length; i++)
        targets[i] = newVertex(i + 1);

      database.transaction(() -> {
        final MutableVertex source = database.lookupByRID(hub, true).asVertex().modify();
        for (final RID target : targets)
          source.newEdge("Follows", target);
      });

      database.transaction(() -> assertThat(database.lookupByRID(hub, true).asVertex()
          .countEdges(Vertex.DIRECTION.OUT)).as("precondition: the hub is well past the threshold")
          .isEqualTo(targets.length));

      // a duplicate of an edge buried in some stripe must still be rejected
      assertThatThrownBy(() -> database.transaction(
          () -> database.lookupByRID(hub, true).asVertex().modify().newEdge("Follows", targets[7])))
          .isInstanceOf(DuplicatedKeyException.class);

      // ...and a genuinely new neighbour must still be accepted
      final RID fresh = newVertex(999);
      database.transaction(() -> database.lookupByRID(hub, true).asVertex().modify().newEdge("Follows", fresh));

      database.transaction(() -> assertThat(database.lookupByRID(hub, true).asVertex()
          .countEdges(Vertex.DIRECTION.OUT)).isEqualTo(targets.length + 1));
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }

  @Test
  void flagsSurviveAReopen() {
    database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true)
        .withUnique(true).create());

    reopenDatabase();

    final EdgeType type = (EdgeType) database.getSchema().getType("Follows");
    assertThat(type.isLightweight()).isTrue();
    assertThat(type.isUnique()).isTrue();
  }

  @Test
  void sqlDeclaresBothFlags() {
    database.command("sql", "create edge type Follows lightweight unique").close();

    final EdgeType type = (EdgeType) database.getSchema().getType("Follows");
    assertThat(type.isLightweight()).isTrue();
    assertThat(type.isUnique()).isTrue();

    // modifiers are order-independent
    database.command("sql", "create edge type Likes unique lightweight").close();
    final EdgeType likes = (EdgeType) database.getSchema().getType("Likes");
    assertThat(likes.isLightweight()).isTrue();
    assertThat(likes.isUnique()).isTrue();
  }

  @Test
  void sqlCreateEdgeMakesALightweightEdgeOnALightweightType() {
    database.command("sql", "create edge type Follows lightweight").close();

    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> database.command("sql", "create edge Follows from " + a + " to " + b).close());

    database.transaction(() -> {
      final Edge edge = database.lookupByRID(a, true).asVertex().getEdges(Vertex.DIRECTION.OUT).iterator().next();
      assertThat(edge).isInstanceOf(LightEdge.class);
      assertThat(edge.getIn()).isEqualTo(b);
    });
    assertThat(count("select count(@rid) as c from Follows")).isZero();
  }

  @Test
  void alterTypeTogglesTheFlags() {
    database.command("sql", "create edge type Follows").close();

    database.command("sql", "alter type Follows with lightweight = true").close();
    assertThat(((EdgeType) database.getSchema().getType("Follows")).isLightweight()).isTrue();

    database.command("sql", "alter type Follows with unique = true").close();
    assertThat(((EdgeType) database.getSchema().getType("Follows")).isUnique()).isTrue();
    // a lightweight type has no records, so UNIQUE must not have built an index
    assertThat(database.getSchema().existsIndex("Follows[@out,@in]")).isFalse();
  }

  @Test
  void aPopulatedTypeCannotBecomeLightweight() {
    database.command("sql", "create edge type Follows").close();

    final RID a = newVertex(0);
    final RID b = newVertex(1);
    database.transaction(() -> database.lookupByRID(a, true).asVertex().modify().newEdge("Follows", b, "since", 2020));

    assertThatThrownBy(() -> database.command("sql", "alter type Follows with lightweight = true").close())
        .hasMessageContaining("already contains edge records");
  }

  // ---------------------------------------------------------------- helpers

  private RID newVertex(final int id) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V");
      v.set("id", id);
      v.save();
      rid[0] = v.getIdentity();
    });
    return rid[0];
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
