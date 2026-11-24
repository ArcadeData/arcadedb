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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.StripedEdgeList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The buckets a type keeps for its own use - the paired bucket of its EXTERNAL values, the edge-list buckets of a vertex bucket
 * and the stripe pool of a vertex type - stay with that type: another type cannot take them as one of its buckets, by
 * {@code ALTER TYPE ... BUCKET}, by {@code CREATE TYPE ... BUCKET}, by the type builder or by the Java API. And a vertex bucket
 * cannot be given to a vertex type that would pair it with a bucket another type already owns.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class InternalBucketAttachTest extends TestHelper {

  @Override
  protected void beginTest() {
    final DocumentType doc = database.getSchema().createDocumentType("Doc");
    doc.createProperty("blob", Type.STRING).setExternal(true);
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("Link");
    database.getSchema().createDocumentType("Other");
    database.getSchema().createVertexType("OtherNode");
  }

  @Test
  void theExternalBucketOfATypeCannotBeAttachedToAnotherTypeBySql() {
    final int externalId = database.getSchema().getBucketByName("Doc_0_ext").getFileId();

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Other BUCKET +Doc_0_ext")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Doc_0_ext").hasMessageContaining("internal bucket of the type 'Doc'");

    assertThat(database.getSchema().getType("Other").getBuckets(false)).extracting(Bucket::getName).containsExactly("Other_0");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(externalId).getName()).isEqualTo("Doc");
    assertThat(database.getSchema().getTypeByBucketId(externalId)).isNull();
  }

  @Test
  void theExternalBucketOfATypeCannotBeAttachedByTheJavaApi() {
    final Bucket external = database.getSchema().getBucketByName("Doc_0_ext");

    assertThatThrownBy(() -> database.getSchema().getType("Other").addBucket(external)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("internal bucket of the type 'Doc'");
    assertThatThrownBy(() -> database.getSchema().getType("Other").addBucket(database.getSchema().getBucketById(external.getFileId())))
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Doc'");

    assertThat(database.getSchema().getType("Other").getBuckets(false)).extracting(Bucket::getName).containsExactly("Other_0");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(external.getFileId()).getName()).isEqualTo("Doc");
  }

  @Test
  void theEdgeListBucketsOfAVertexBucketCannotBeAttachedToAnotherType() {
    final int outId = database.getSchema().getBucketByName("Node_0_out_edges").getFileId();
    final int inId = database.getSchema().getBucketByName("Node_0_in_edges").getFileId();

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Other BUCKET +Node_0_out_edges")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("internal bucket of the type 'Node'");
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE OtherNode BUCKET +Node_0_in_edges"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Node'");

    assertThat(database.getSchema().getType("Other").getBuckets(false)).extracting(Bucket::getName).containsExactly("Other_0");
    assertThat(database.getSchema().getType("OtherNode").getBuckets(false)).extracting(Bucket::getName)
        .containsExactly("OtherNode_0");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(outId).getName()).isEqualTo("Node");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(inId).getName()).isEqualTo("Node");
    assertThat(database.getSchema().getTypeByBucketId(outId)).isNull();
    assertThat(database.getSchema().getTypeByBucketId(inId)).isNull();
  }

  @Test
  void aStripeBucketOfAVertexTypeCannotBeAttachedToAnotherType() {
    final Bucket stripe = database.getSchema().createBucket(StripedEdgeList.stripeBucketName("Node", 0));
    assertThat(database.getSchema().getInvolvedTypeByBucketId(stripe.getFileId()).getName()).isEqualTo("Node");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Other BUCKET +" + stripe.getName())).isInstanceOf(SchemaException.class)
        .hasMessageContaining("internal bucket of the type 'Node'");

    assertThat(database.getSchema().getType("Other").getBuckets(false)).extracting(Bucket::getName).containsExactly("Other_0");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(stripe.getFileId()).getName()).isEqualTo("Node");
    assertThat(database.getSchema().getTypeByBucketId(stripe.getFileId())).isNull();
  }

  @Test
  void aNewTypeCannotBeCreatedOnTheInternalBucketsOfAnotherType() {
    final Bucket stripe = database.getSchema().createBucket(StripedEdgeList.stripeBucketName("Node", 0));

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Taker1 BUCKET Doc_0_ext")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("internal bucket of the type 'Doc'");
    assertThatThrownBy(() -> database.command("sql", "CREATE VERTEX TYPE Taker2 BUCKET Node_0_out_edges"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Node'");
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Taker3 BUCKET Node_0_in_edges"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Node'");
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Taker4 BUCKET " + stripe.getName()))
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Node'");
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Taker5 BUCKET " + stripe.getFileId()))
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Node'");

    for (int i = 1; i <= 5; i++)
      assertThat(database.getSchema().existsType("Taker" + i)).as("Taker%d", i).isFalse();
    assertThat(database.getSchema().getInvolvedTypeByBucketId(database.getSchema().getBucketByName("Doc_0_ext").getFileId()).getName())
        .isEqualTo("Doc");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(stripe.getFileId()).getName()).isEqualTo("Node");
  }

  @Test
  void theTypeBuilderRefusesTheInternalBucketsOfAnotherType() {
    final Bucket external = database.getSchema().getBucketByName("Doc_0_ext");
    final Bucket edgeList = database.getSchema().getBucketByName("Node_0_out_edges");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Taker").withBuckets(List.of(external)).create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Doc'");
    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Taker").withBuckets(List.of(edgeList)).create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("internal bucket of the type 'Node'");

    assertThat(database.getSchema().existsType("Taker")).isFalse();
  }

  @Test
  void aTypeNamedLikeTheStripePoolOfAnotherTypeDoesNotReuseItsBuckets() {
    // The first bucket of a type called Node_sn_stripe is named like the first slot of the stripe pool of Node
    final Bucket stripe = database.getSchema().createBucket(StripedEdgeList.stripeBucketName("Node", 0));
    final String typeName = "Node_sn_stripe";
    assertThat(typeName + "_0").isEqualTo(stripe.getName());

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE " + typeName)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("internal bucket of the type 'Node'");

    assertThat(database.getSchema().existsType(typeName)).isFalse();
    assertThat(database.getSchema().getInvolvedTypeByBucketId(stripe.getFileId()).getName()).isEqualTo("Node");
    assertThat(database.getSchema().getTypeByBucketId(stripe.getFileId())).isNull();
  }

  @Test
  void aVertexTypeIsRefusedABucketWhoseEdgeListBucketBelongsToAnotherType() {
    database.getSchema().createBucket("Spare_out_edges");
    database.command("sql", "CREATE DOCUMENT TYPE Squatter BUCKET Spare_out_edges");
    final Bucket spare = database.getSchema().createBucket("Spare");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE OtherNode BUCKET +Spare")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Spare_out_edges").hasMessageContaining("already associated to the type 'Squatter'");

    assertThat(database.getSchema().getType("OtherNode").getBuckets(false)).extracting(Bucket::getName)
        .containsExactly("OtherNode_0");
    assertThat(database.getSchema().getTypeByBucketId(spare.getFileId())).isNull();
    assertThat(database.getSchema().getTypeByBucketName("Spare_out_edges").getName()).isEqualTo("Squatter");
    assertThat(database.getSchema().getBucketByName("Spare_out_edges")).isNotNull();

    assertThatThrownBy(() -> database.command("sql", "CREATE VERTEX TYPE Fresh BUCKET Spare")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("already associated to the type 'Squatter'");
    assertThat(database.getSchema().existsType("Fresh")).isFalse();
  }

  @Test
  void theRefusalHoldsAfterAReopen() {
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Other BUCKET +Doc_0_ext")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Other BUCKET +Node_0_out_edges")).isInstanceOf(SchemaException.class);

    reopenDatabase();

    assertThat(database.getSchema().getType("Other").getBuckets(false)).extracting(Bucket::getName).containsExactly("Other_0");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(database.getSchema().getBucketByName("Doc_0_ext").getFileId()).getName())
        .isEqualTo("Doc");
    assertThat(
        database.getSchema().getInvolvedTypeByBucketId(database.getSchema().getBucketByName("Node_0_out_edges").getFileId()).getName())
        .isEqualTo("Node");
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Other BUCKET +Doc_0_ext")).isInstanceOf(SchemaException.class);
  }

  @Test
  void aBucketOfNoTypeStillAttachesAndTheTypesKeepGrowingAndRenaming() {
    final Bucket free = database.getSchema().createBucket("Free");
    database.command("sql", "ALTER TYPE Other BUCKET +Free");
    assertThat(database.getSchema().getTypeByBucketId(free.getFileId()).getName()).isEqualTo("Other");

    // A type that grows takes new buckets, and the paired bucket and the edge-list buckets of each follow it
    database.command("sql", "ALTER TYPE Doc BUCKET +Doc_extra");
    database.command("sql", "ALTER TYPE Node BUCKET +Node_extra");
    final Bucket docExtra = database.getSchema().getBucketByName("Doc_extra");
    assertThat(database.getSchema().getTypeByBucketId(docExtra.getFileId()).getName()).isEqualTo("Doc");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(database.getSchema().getBucketByName("Doc_extra_ext").getFileId())
        .getName()).isEqualTo("Doc");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(database.getSchema().getBucketByName("Node_extra_out_edges").getFileId())
        .getName()).isEqualTo("Node");

    // The edge-list buckets travel with the type on a rename
    database.getSchema().getType("Node").rename("Renamed");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(database.getSchema().getBucketByName("Renamed_0_out_edges").getFileId())
        .getName()).isEqualTo("Renamed");

    reopenDatabase();

    assertThat(database.getSchema().getType("Other").getBuckets(false)).extracting(Bucket::getName).contains("Other_0", "Free");
    assertThat(database.getSchema().getInvolvedTypeByBucketId(database.getSchema().getBucketByName("Renamed_0_in_edges").getFileId())
        .getName()).isEqualTo("Renamed");
  }

  @Test
  void aTypeKeepsWorkingWithItsOwnInternalBucketsAfterTheRefusals() {
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Other BUCKET +Doc_0_ext")).isInstanceOf(SchemaException.class);

    database.transaction(() -> {
      database.newDocument("Doc").set("blob", "x".repeat(2000)).save();
      database.newVertex("Node").save();
    });

    assertThat(database.countType("Doc", false)).isEqualTo(1L);
    assertThat(database.countBucket("Doc_0_ext")).isEqualTo(1L);
  }
}
