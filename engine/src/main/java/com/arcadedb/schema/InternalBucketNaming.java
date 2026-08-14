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

import java.util.Set;

/**
 * The one home of "reached by name": the bucket names the engine composes from the name of the schema object that
 * owns them, and the rule that recognises one (issue #6168, item 5).
 * <p>
 * A bucket can be referenced by the engine without any type listing it among its buckets. The reference is a NAMING
 * CONVENTION - {@code V_0_out_edges} belongs to vertex bucket {@code V_0}, {@code Hub_sn_stripe_3} to type
 * {@code Hub}, {@code Doc_0_ext} to primary bucket {@code Doc_0} - and until this class existed each feature composed
 * its own names and every consumer that had to ask "is this bucket referenced?" re-encoded the convention on its own.
 * That cost real time: #6152's unreferenced-file classifier shipped with an explicit list of the conventions it knew
 * about, CI found the one the list was missing, and 17 test classes went red reporting every promoted super-node's
 * stripe pool as an orphan.
 * <p>
 * <b>Two halves, and they answer different questions.</b>
 * <ul>
 *   <li>{@link Convention} and the {@code composeXXX} methods are the DECLARATION side: a feature that names buckets
 *       after their owner declares its convention here and gets recognition with it, instead of leaving it to be
 *       discovered later by whoever is debugging a false positive.</li>
 *   <li>{@link #ownerOf} is the RECOGNITION side, and it is deliberately NOT driven by the enum. It matches the
 *       SHAPE - {@code <owner>_<anything>} for an owner the caller knows about - so it covers conventions that were
 *       never declared here, including the ones not written yet. Enumerating them is not a closed problem; that is
 *       precisely what CI proved on #6152. The enum documents and composes, it does not gate.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class InternalBucketNaming {

  /**
   * The conventions the engine composes today, each with the suffix it appends to its owner's name.
   * <p>
   * A declaration, not a whitelist: {@link #ownerOf} recognises the shape rather than this list, so adding a
   * constant here buys composition and documentation, and forgetting to add one does NOT make the new buckets look
   * like orphans.
   */
  public enum Convention {
    /** Outgoing edge-list bucket of a VERTEX BUCKET: {@code <vertexBucket>_out_edges} (GraphEngine). */
    OUT_EDGES("_out_edges"),
    /** Incoming edge-list bucket of a VERTEX BUCKET: {@code <vertexBucket>_in_edges} (GraphEngine). */
    IN_EDGES("_in_edges"),
    /** Super-node stripe pool of a VERTEX TYPE: {@code <type>_sn_stripe_<slot>} (StripedEdgeList, #5156). */
    SUPER_NODE_STRIPE("_sn_stripe_"),
    /** Paired external-property bucket of a PRIMARY BUCKET: {@code <primaryBucket>_ext} (LocalDocumentType). */
    EXTERNAL_PROPERTY("_ext");

    private final String suffix;

    Convention(final String suffix) {
      this.suffix = suffix;
    }

    /** The text appended to the owner's name. For {@link #SUPER_NODE_STRIPE} the slot ordinal follows it. */
    public String suffix() {
      return suffix;
    }

    /** The name this convention gives to the bucket owned by {@code ownerName}. */
    public String compose(final String ownerName) {
      return ownerName + suffix;
    }
  }

  private InternalBucketNaming() {
  }

  /** Name of the outgoing edge-list bucket of the given vertex bucket. */
  public static String outEdgesBucketName(final String vertexBucketName) {
    return Convention.OUT_EDGES.compose(vertexBucketName);
  }

  /** Name of the incoming edge-list bucket of the given vertex bucket. */
  public static String inEdgesBucketName(final String vertexBucketName) {
    return Convention.IN_EDGES.compose(vertexBucketName);
  }

  /** Name of the {@code slot}-th bucket of the super-node stripe pool of the given type. */
  public static String superNodeStripeBucketName(final String typeName, final int slot) {
    return Convention.SUPER_NODE_STRIPE.compose(typeName) + slot;
  }

  /** Name of the external-property bucket paired to the given primary bucket. */
  public static String externalPropertyBucketName(final String primaryBucketName) {
    return Convention.EXTERNAL_PROPERTY.compose(primaryBucketName);
  }

  /**
   * Whether the name is shaped like one of a vertex bucket's two edge-list buckets. Asked when a type rename has to
   * carry its internal buckets along and must refuse to rename anything it does not recognise.
   */
  public static boolean isEdgeListBucketName(final String bucketName) {
    return bucketName.endsWith(Convention.OUT_EDGES.suffix()) || bucketName.endsWith(Convention.IN_EDGES.suffix());
  }

  /**
   * Whether the name is shaped like an external-property bucket's. Used to warn a user creating a bucket that would
   * collide with the pairing before the collision turns into a {@code SchemaException} on a later property change.
   */
  public static boolean looksLikeAnExternalPropertyBucketName(final String bucketName) {
    return bucketName.endsWith(Convention.EXTERNAL_PROPERTY.suffix());
  }

  /**
   * The owner among {@code ownerNames} whose name this bucket's is derived from, or {@code null} if none is -
   * {@code Hub_sn_stripe_3} for {@code Hub}, {@code V_0_out_edges} for {@code V_0}, {@code Doc_0_ext} for
   * {@code Doc_0}.
   * <p>
   * <b>The general rule, deliberately.</b> It does not consult {@link Convention}, so a bucket named after its owner
   * by a convention nobody declared here is still recognised. What it costs is a false NEGATIVE - a genuinely
   * orphaned bucket that happens to be named after a surviving type is attributed to it - which is the right side to
   * fail on for a caller whose findings invite deletion.
   * <p>
   * Matched by successive underscore-delimited prefixes rather than by scanning {@code ownerNames}, so the cost is a
   * few hash lookups per candidate instead of one pass over every type in the schema. Owner names may themselves
   * contain underscores ({@code V_0}), which is why every prefix is tried and not only the first.
   * <p>
   * <b>The SHORTEST matching prefix wins</b>, and that tie-break is arbitrary rather than principled: with both
   * {@code My} and {@code My_Type_0} in the owner set, {@code My_Type_0_out_edges} is attributed to {@code My}. It
   * does not matter to the question this method exists to answer - "is this bucket owned by anything?", which is
   * what {@link #isDerivedFromAnOwner} asks and is the only thing consumed today. A caller that uses the returned
   * NAME for something attribution-sensitive (reporting it to an operator, deciding what to delete alongside it)
   * has to establish which owner it wants first; this method will not have chosen it for them.
   *
   * @param bucketName the bucket whose ownership is in question
   * @param ownerNames names of the schema objects that can own a bucket: types, and buckets a type already claims
   *
   * @return the shortest prefix of {@code bucketName} that is in {@code ownerNames}, or {@code null} if none is
   */
  public static String ownerOf(final String bucketName, final Set<String> ownerNames) {
    for (int underscore = bucketName.indexOf('_'); underscore > 0; underscore = bucketName.indexOf('_',
        underscore + 1)) {
      final String candidate = bucketName.substring(0, underscore);
      if (ownerNames.contains(candidate))
        return candidate;
    }

    return null;
  }

  /** Whether {@link #ownerOf} finds an owner for this bucket, for callers that need only the predicate. */
  public static boolean isDerivedFromAnOwner(final String bucketName, final Set<String> ownerNames) {
    return ownerOf(bucketName, ownerNames) != null;
  }
}
