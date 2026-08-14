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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6168, item 5: "reached by name" now has one home, so the composition side and the recognition side cannot
 * drift apart the way they had started to (the convention lived in {@code UnreferencedFiles}, in
 * {@code GraphDatabaseChecker}, in {@code GraphEngine}, in {@code StripedEdgeList} and twice as a bare
 * {@code "_ext"} literal in the schema package).
 * <p>
 * The load-bearing test is {@link #recognisesAConventionNobodyDeclared}: recognition matches the SHAPE and is not
 * driven by {@link InternalBucketNaming.Convention}, which is what stops the next feature that names buckets after
 * their owner from being discovered as a false orphan in CI - the way the super-node stripe pools of #5156 were, on
 * #6152, taking 17 test classes red.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InternalBucketNamingTest {

  @Test
  void composesEveryConventionTheEngineUses() {
    assertThat(InternalBucketNaming.outEdgesBucketName("V_0")).isEqualTo("V_0_out_edges");
    assertThat(InternalBucketNaming.inEdgesBucketName("V_0")).isEqualTo("V_0_in_edges");
    assertThat(InternalBucketNaming.superNodeStripeBucketName("Hub", 3)).isEqualTo("Hub_sn_stripe_3");
    assertThat(InternalBucketNaming.externalPropertyBucketName("Doc_0")).isEqualTo("Doc_0_ext");
  }

  @Test
  void attributesEachComposedNameBackToTheOwnerThatComposedIt() {
    final Set<String> owners = Set.of("V_0", "Hub", "Doc_0");

    assertThat(InternalBucketNaming.ownerOf("V_0_out_edges", owners)).isEqualTo("V_0");
    assertThat(InternalBucketNaming.ownerOf("V_0_in_edges", owners)).isEqualTo("V_0");
    assertThat(InternalBucketNaming.ownerOf("Hub_sn_stripe_3", owners)).isEqualTo("Hub");
    assertThat(InternalBucketNaming.ownerOf("Doc_0_ext", owners)).isEqualTo("Doc_0");
  }

  /**
   * The point of the general rule. A convention that does not exist yet - and therefore is not in the enum - is
   * still recognised, because what is matched is {@code <owner>_<anything>} and not a list of suffixes.
   */
  @Test
  void recognisesAConventionNobodyDeclared() {
    assertThat(InternalBucketNaming.ownerOf("Hub_some_future_thing_7", Set.of("Hub"))).isEqualTo("Hub");
  }

  /** An owner name may itself contain underscores, so every prefix has to be tried, not only the first. */
  @Test
  void triesEveryUnderscorePrefixSoOwnersMayContainUnderscores() {
    assertThat(InternalBucketNaming.ownerOf("My_Type_0_out_edges", Set.of("My_Type_0"))).isEqualTo("My_Type_0");
    // The SHORTEST matching prefix wins. Pinned so the tie-break is a documented property rather than an accident
    // of the loop, but it is arbitrary: what the only consumer asks is "owned by anything?", and both answers say
    // yes. See ownerOf's javadoc before using the returned NAME for anything attribution-sensitive.
    assertThat(InternalBucketNaming.ownerOf("My_Type_0_out_edges", Set.of("My", "My_Type_0"))).isEqualTo("My");
  }

  @Test
  void reportsNoOwnerWhenTheNameIsDerivedFromNothingKnown() {
    assertThat(InternalBucketNaming.ownerOf("orphan_bucket", Set.of("Hub", "V_0"))).isNull();
    assertThat(InternalBucketNaming.isDerivedFromAnOwner("orphan_bucket", Set.of("Hub"))).isFalse();
    // No underscore at all: nothing to derive from.
    assertThat(InternalBucketNaming.ownerOf("detachedbucket", Set.of("detached"))).isNull();
    // A name that IS an owner's is not derived from it - it is the owner, and an owner's own bucket is claimed
    // through the schema rather than through this rule.
    assertThat(InternalBucketNaming.ownerOf("Hub", Set.of("Hub"))).isNull();
  }

  /** The suffix check a type rename uses to refuse to carry along an internal bucket it does not recognise. */
  @Test
  void recognisesAnEdgeListBucketNameByItsSuffixAlone() {
    assertThat(InternalBucketNaming.isEdgeListBucketName("V_0_out_edges")).isTrue();
    assertThat(InternalBucketNaming.isEdgeListBucketName("V_0_in_edges")).isTrue();
    assertThat(InternalBucketNaming.isEdgeListBucketName("V_0")).isFalse();
    assertThat(InternalBucketNaming.isEdgeListBucketName("Hub_sn_stripe_0")).isFalse();
  }

  /**
   * The suffix check the schema uses to warn a user before {@code CREATE BUCKET} sets up a collision that only
   * surfaces later, as a {@code SchemaException} on an EXTERNAL property change.
   */
  @Test
  void recognisesAnExternalPropertyBucketNameByItsSuffixAlone() {
    assertThat(InternalBucketNaming.looksLikeAnExternalPropertyBucketName("Doc_0_ext")).isTrue();
    assertThat(InternalBucketNaming.looksLikeAnExternalPropertyBucketName("whatever_ext")).isTrue();
    assertThat(InternalBucketNaming.looksLikeAnExternalPropertyBucketName("Doc_0")).isFalse();
  }
}
