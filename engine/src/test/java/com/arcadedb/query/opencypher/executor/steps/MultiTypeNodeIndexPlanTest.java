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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The plan of a node pattern that names no single type (issue #12522): which index answers which vertex bucket, and which buckets
 * are walked. The invariant of the plan is that every vertex bucket is answered by exactly one part, whatever the hierarchy above
 * it looks like, since a bucket answered twice is a vertex returned twice and a bucket answered by none is a vertex lost.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MultiTypeNodeIndexPlanTest extends TestHelper {

  private static Map<String, Object> pinned(final Object... keysAndValues) {
    final Map<String, Object> pinned = new HashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      pinned.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return pinned;
  }

  /** How many parts answer each bucket, by bucket id: a seek by the buckets it answers for, a walk by the buckets it reads. */
  private int[] answersPerBucket(final MultiTypeNodeIndexPlan plan) {
    final int[] answers = new int[maxBucketId() + 1];
    for (final MultiTypeNodeIndexPlan.Part part : plan.parts()) {
      if (part.isSeek()) {
        final BitSet buckets = part.buckets();
        for (int id = buckets.nextSetBit(0); id >= 0; id = buckets.nextSetBit(id + 1))
          answers[id]++;
      } else
        for (final String bucketName : part.bucketNames())
          answers[database.getSchema().getBucketByName(bucketName).getFileId()]++;
    }
    return answers;
  }

  private int maxBucketId() {
    int max = 0;
    for (final Bucket bucket : database.getSchema().getBuckets())
      max = Math.max(max, bucket.getFileId());
    return max;
  }

  private void assertEveryVertexBucketIsAnsweredOnce(final MultiTypeNodeIndexPlan plan) {
    final int[] answers = answersPerBucket(plan);
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof VertexType)
        for (final Bucket bucket : type.getBuckets(false))
          assertThat(answers[bucket.getFileId()]).as("the bucket %s of the type %s is answered by exactly one part", bucket.getName(), type.getName())
              .isEqualTo(1);
  }

  @Test
  void everyVertexBucketIsAnsweredByExactlyOnePart() {
    database.transaction(() -> {
      // two branches with an index each and a type under both
      final VertexType left = database.getSchema().createVertexType("Left");
      left.createProperty("uuid", String.class);
      left.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final VertexType right = database.getSchema().createVertexType("Right");
      right.createProperty("uuid", String.class);
      right.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      database.getSchema().createVertexType("Diamond").addSuperType(left).addSuperType(right);

      // a hierarchy under one index
      final VertexType base = database.getSchema().createVertexType("Base");
      base.createProperty("uuid", String.class);
      base.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final VertexType mid = database.getSchema().createVertexType("Mid");
      mid.addSuperType(base);
      final VertexType leaf = database.getSchema().createVertexType("Leaf");
      leaf.addSuperType(mid);

      // a type with two indexes that both answer the pinned value, and a sub type that carries them
      final VertexType twice = database.getSchema().createVertexType("Twice");
      twice.createProperty("uuid", String.class);
      twice.createProperty("tag", String.class);
      twice.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      twice.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid", "tag");
      database.getSchema().createVertexType("UnderTwice").addSuperType(twice);

      // no index at all
      database.getSchema().createVertexType("Plain").createProperty("uuid", String.class);
    });

    final MultiTypeNodeIndexPlan plan = MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("uuid", "x"));

    assertThat(plan).isNotNull();
    assertEveryVertexBucketIsAnsweredOnce(plan);
    assertThat(plan.parts().stream().filter(MultiTypeNodeIndexPlan.Part::isSeek).count()).as("one seek per index that answers a bucket")
        .isGreaterThanOrEqualTo(3);
    assertThat(plan.parts().stream().filter(part -> !part.isSeek()).map(MultiTypeNodeIndexPlan.Part::typeName).toList())
        .as("only the type that no index covers is walked").containsExactly("Plain");
  }

  @Test
  void theBucketsOfAPatternThatSelectsSomeTypesAreAnsweredOnceToo() {
    database.transaction(() -> {
      final VertexType a = database.getSchema().createVertexType("A");
      a.createProperty("uuid", String.class);
      a.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final VertexType b = database.getSchema().createVertexType("B");
      b.createProperty("uuid", String.class);
      b.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      database.getSchema().createVertexType("A~B").addSuperType(a).addSuperType(b);
    });

    // (n:A:B) selects the composite type only: it holds no index of its own, the indexes of the two types above it answer
    final MultiTypeNodeIndexPlan plan = MultiTypeNodeIndexPlan.of(database.getSchema(), List.of("A", "B"), false, pinned("uuid", "x"));

    assertThat(plan).isNotNull();
    assertThat(plan.parts()).hasSize(1);
    final MultiTypeNodeIndexPlan.Part part = plan.parts().getFirst();
    assertThat(part.isSeek()).isTrue();
    assertThat(part.wholeIndex()).as("the index also covers the buckets of A or of B, which the labels do not select").isFalse();
    final BitSet expected = new BitSet();
    for (final Bucket bucket : database.getSchema().getType("A~B").getBuckets(false))
      expected.set(bucket.getFileId());
    assertThat(part.buckets()).isEqualTo(expected);
  }

  @Test
  void noIndexMeansNoPlan() {
    database.transaction(() -> database.getSchema().createVertexType("Plain").createProperty("uuid", String.class));

    assertThat(MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("uuid", "x"))).isNull();
  }

  @Test
  void aValueTheIndexCannotKeyMeansNoPlan() {
    database.transaction(() -> {
      final VertexType numeric = database.getSchema().createVertexType("Numeric");
      numeric.createProperty("id", Long.class);
      numeric.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
    });

    assertThat(MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("id", 1L))).isNotNull();
    // an index answers the pattern, but not with these values: told apart from no index at all, which no row can change
    assertThat(MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("id", "1"))).as("the text of a number")
        .isSameAs(MultiTypeNodeIndexPlan.UNKEYABLE);
    assertThat(MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("id", 1.0d))).as("a floating value for an integer key")
        .isSameAs(MultiTypeNodeIndexPlan.UNKEYABLE);
    assertThat(MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("other", 1L))).as("a property no index is on").isNull();
  }

  @Test
  void aPrefixOfACompositeKeyIsNoPlanAndTheLongestKeyAnswersFirst() {
    database.transaction(() -> {
      final VertexType pair = database.getSchema().createVertexType("Pair");
      pair.createProperty("a", String.class);
      pair.createProperty("b", String.class);
      pair.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a", "b");
      pair.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    });

    final MultiTypeNodeIndexPlan single = MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("a", "x"));
    assertThat(single).isNotNull();
    assertThat(single.parts().getFirst().propertyNames()).as("only the index on a alone is an equality on its whole key").containsExactly("a");

    final MultiTypeNodeIndexPlan both = MultiTypeNodeIndexPlan.of(database.getSchema(), List.of(), false, pinned("a", "x", "b", "y"));
    assertThat(both).isNotNull();
    assertThat(both.parts().getFirst().propertyNames()).as("the longest key narrows most").containsExactly("a", "b");
    assertThat(both.parts()).as("the bucket of the type is answered once").hasSize(1);
  }

  @Test
  void whatTheWalkComparesIsWhatTheIndexIsKeyedBy() {
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.STRING, "x")).isTrue();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.STRING, 1)).isFalse();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.BOOLEAN, Boolean.TRUE)).isTrue();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.BOOLEAN, "true")).isFalse();
    for (final Type integral : new Type[] { Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG }) {
      assertThat(MultiTypeNodeIndexPlan.isKeyableBy(integral, 1)).as("%s", integral).isTrue();
      assertThat(MultiTypeNodeIndexPlan.isKeyableBy(integral, 1L)).as("%s", integral).isTrue();
      assertThat(MultiTypeNodeIndexPlan.isKeyableBy(integral, 1.0d)).as("a double against %s: the walk compares as doubles", integral).isFalse();
      assertThat(MultiTypeNodeIndexPlan.isKeyableBy(integral, "1")).as("%s", integral).isFalse();
    }
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.DOUBLE, 1.5d)).isTrue();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.DOUBLE, 1.5f)).isTrue();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.DOUBLE, 3L)).isTrue();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.DOUBLE, 9007199254740993L)).as("a long a double cannot hold").isFalse();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.DOUBLE, Long.MIN_VALUE)).isFalse();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.DECIMAL, 1)).isFalse();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.DATETIME, 1L)).isFalse();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(null, 1)).isFalse();
    assertThat(MultiTypeNodeIndexPlan.isKeyableBy(Type.STRING, null)).isFalse();
  }
}
