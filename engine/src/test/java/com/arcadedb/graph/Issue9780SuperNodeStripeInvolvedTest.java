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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9780: the super-node stripe pool of a vertex type ({@code <type>_sn_stripe_<slot>}) was in no list the type
 * answers with, so {@link DocumentType#getInvolvedBuckets()} left it out. The per-file permission map built from that
 * list resolved no type for the pool's files (default-allow for every grant), and an explicit type lock did not lock them,
 * so an explicitly locked append to a promoted vertex failed the commit-time lock coverage check.
 * <p>
 * The pool is named after its type, so a type rename must carry it along: left behind under the old name it would drop
 * out of the type's involved buckets again, and be adopted by the next type created under that name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9780SuperNodeStripeInvolvedTest extends TestHelper {
  private static final int STRIPES = 4;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 16);
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES, STRIPES);
    database.getSchema().createVertexType("V", 1);
    database.getSchema().createEdgeType("E");
  }

  @Test
  void stripeBucketsResolveToTheirVertexType() {
    final RID hub = createPromotedHub("V");

    assertStripePoolInvolved("V");
    assertThat(countOut(hub)).isEqualTo(400);
  }

  @Test
  void stripeBucketsStayMappedAcrossReopen() {
    createPromotedHub("V");

    reopenDatabase();

    assertStripePoolInvolved("V");
  }

  /** The issue's lock half: one edge appended to a promoted hub under an explicit lock of its vertex and edge types. */
  @Test
  void explicitTypeLockCoversTheStripePool() {
    final RID hub = createPromotedHub("V");

    database.transaction(() -> {
      database.acquireLock().type("V").type("E").lock();
      final MutableVertex other = database.newVertex("V").save();
      for (int i = 0; i < 2 * STRIPES; i++)
        hub.asVertex().newEdge("E", other);
    });

    assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
  }

  @Test
  void aSubtypePoolResolvesToTheSubtype() {
    database.getSchema().buildVertexType().withName("Child").withSuperType("V").withTotalBuckets(1).create();
    createPromotedHub("Child");

    assertStripePoolInvolved("Child");
    assertThat(database.getSchema().getType("V").getInvolvedBuckets().stream().map(Bucket::getName))
        .as("the parent does not claim its subtype's pool").noneMatch(n -> n.startsWith("Child_sn_stripe_"));
  }

  /**
   * Locking a parent type locks its subtypes' records: the edge-list buckets and the stripe pool of a subtype's vertices come
   * with them, or an edge appended to a subtype vertex under the parent's lock fails the commit.
   */
  @Test
  void explicitParentTypeLockCoversTheSubtypeEdgeBucketsAndStripePool() {
    database.getSchema().buildVertexType().withName("Child").withSuperType("V").withTotalBuckets(1).create();
    final RID hub = createPromotedHub("Child");

    database.transaction(() -> {
      database.acquireLock().type("V").type("E").lock();
      final MutableVertex plain = database.newVertex("Child").save();
      final MutableVertex other = database.newVertex("Child").save();
      plain.newEdge("E", other);
      for (int i = 0; i < 2 * STRIPES; i++)
        hub.asVertex().newEdge("E", other);
    });

    assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
  }

  /**
   * The parent's lock reaches every level below it, and a type reached through two supertypes (a diamond) is covered once:
   * edges appended to promoted hubs two levels down commit under a lock of the root type alone.
   */
  @Test
  void explicitRootTypeLockCoversAMultiLevelAndDiamondHierarchy() {
    final Schema schema = database.getSchema();
    schema.buildVertexType().withName("Left").withSuperType("V").withTotalBuckets(1).create();
    schema.buildVertexType().withName("Right").withSuperType("V").withTotalBuckets(1).create();
    schema.buildVertexType().withName("Bottom").withSuperType("Left").withTotalBuckets(1).create().addSuperType("Right");
    assertThat(schema.getType("Bottom").getSuperTypes()).hasSize(2);

    final RID hub = createPromotedHub("Bottom");

    database.transaction(() -> {
      database.acquireLock().type("V").type("E").lock();
      final MutableVertex other = database.newVertex("Bottom").save();
      for (int i = 0; i < 2 * STRIPES; i++)
        hub.asVertex().newEdge("E", other);
    });

    assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
  }

  /**
   * A rename moves the pool under the new name, so it stays the type's own, keeps serving the promoted vertices, and is
   * dropped with the type rather than left behind for a later type created under the old name.
   */
  @Test
  void aRenamedTypeCarriesItsStripePool() {
    final RID hub = createPromotedHub("V");

    database.getSchema().getType("V").rename("W");

    for (int i = 0; i < STRIPES; i++) {
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("V", i))).isFalse();
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("W", i))).isTrue();
    }
    assertStripePoolInvolved("W");

    database.transaction(() -> {
      database.acquireLock().type("W").type("E").lock();
      final MutableVertex other = database.newVertex("W").save();
      for (int i = 0; i < 2 * STRIPES; i++)
        hub.asVertex().newEdge("E", other);
    });
    assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);

    reopenDatabase();
    assertStripePoolInvolved("W");
    assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);

    database.transaction(() -> database.command("sql", "DELETE FROM W"));
    database.getSchema().dropType("E");
    database.getSchema().dropType("W");
    for (int i = 0; i < STRIPES; i++)
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("W", i)))
          .as("the pool is dropped with its type").isFalse();
  }

  /**
   * A bucket already holding one of the pool's new names refuses the rename before anything moves: the type keeps its name
   * and its pool, and the bucket in the way is left alone.
   */
  @Test
  void aRenameThatWouldCollideWithAStripeNameLeavesEverythingInPlace() {
    final RID hub = createPromotedHub("V");
    final Bucket inTheWay = database.getSchema().createBucket(StripedEdgeList.stripeBucketName("W", 1));

    assertThatThrownBy(() -> database.getSchema().getType("V").rename("W")).isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().existsType("V")).isTrue();
    assertThat(database.getSchema().existsType("W")).isFalse();
    assertStripePoolInvolved("V");
    assertThat(database.getSchema().getBucketByName(StripedEdgeList.stripeBucketName("W", 1)).getFileId())
        .isEqualTo(inTheWay.getFileId());
    assertThat(database.getSchema().getInvolvedTypeByBucketId(inTheWay.getFileId())).isNull();
    assertThat(countOut(hub)).isEqualTo(400);
  }

  /**
   * The pool is walked past the configured stripe count for as long as it stays contiguous, so lowering
   * {@code arcadedb.graph.supernodeStripes} after a promotion does not take the extra slots out of the type's grants or lock.
   */
  @Test
  void aShrunkStripeCountStillCoversTheWholePool() {
    final RID hub = createPromotedHub("V");

    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES, 2);
    // Any schema change rebuilds the per-file maps, now under the shrunk count
    database.getSchema().createDocumentType("Unrelated");

    assertStripePoolInvolved("V");
    database.transaction(() -> {
      database.acquireLock().type("V").type("E").lock();
      final MutableVertex other = database.newVertex("V").save();
      for (int i = 0; i < 2 * STRIPES; i++)
        hub.asVertex().newEdge("E", other);
    });
    assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
  }

  /** The one parser of the pool's naming convention: a rename's rollback reads the slot back from a moved bucket's name. */
  @Test
  void stripeSlotIsReadOnlyFromAPoolName() {
    assertThat(StripedEdgeList.stripeSlotOf("V", StripedEdgeList.stripeBucketName("V", 0))).isZero();
    assertThat(StripedEdgeList.stripeSlotOf("V", StripedEdgeList.stripeBucketName("V", 13))).isEqualTo(13);
    assertThat(StripedEdgeList.stripeSlotOf("V", StripedEdgeList.stripeBucketName("W", 1))).isEqualTo(-1);
    assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_")).isEqualTo(-1);
    assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_01")).isEqualTo(-1);
    assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_1_out_edges")).isEqualTo(-1);
    assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_99999999999")).isEqualTo(-1);
    assertThat(StripedEdgeList.stripeSlotOf("V", "V_0_out_edges")).isEqualTo(-1);
  }

  private RID createPromotedHub(final String typeName) {
    final RID[] hub = new RID[1];
    database.transaction(() -> {
      final MutableVertex h = database.newVertex(typeName).save();
      hub[0] = h.getIdentity();
      for (int i = 0; i < 400; i++)
        h.newEdge("E", database.newVertex(typeName).save());
    });
    assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName(typeName, 0)))
        .as("the hub of '%s' was promoted to the striped layout", typeName).isTrue();
    return hub[0];
  }

  private long countOut(final RID hub) {
    return database.lookupByRID(hub, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "E");
  }

  private void assertStripePoolInvolved(final String typeName) {
    final Schema schema = database.getSchema();
    final DocumentType type = schema.getType(typeName);
    final List<Integer> involved = type.getInvolvedBuckets().stream().map(Bucket::getFileId).toList();
    for (int i = 0; i < STRIPES; i++) {
      final Bucket stripe = schema.getBucketByName(StripedEdgeList.stripeBucketName(typeName, i));
      assertThat(involved).as("stripe %d is one of the type's involved buckets", i).contains(stripe.getFileId());
      assertThat(schema.getInvolvedTypeByBucketId(stripe.getFileId()))
          .as("the per-file permission map resolves stripe %d to its type", i).isSameAs(type);
      assertThat(schema.getTypeByBucketId(stripe.getFileId()))
          .as("the stripe stays out of the primary-bucket map: the pool's collision guard relies on it").isNull();
    }
  }
}
