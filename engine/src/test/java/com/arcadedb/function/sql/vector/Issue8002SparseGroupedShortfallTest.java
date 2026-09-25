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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.vector.GroupAdmissionState;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8002: a grouped {@code vector.sparseNeighbors} left an admitted group short of its
 * {@code groupSize} when the distinct-group limit was binding and the group's best member ranked far above its
 * others. The admission rule, taken literally over the score-ordered candidates, is "the {@code limit} groups with the
 * highest peaks, each with its {@code groupSize} best members".
 * <p>
 * Every permutation-like insertion order and bucket layout below must give that answer: the defect showed differently
 * depending on which rows the ascending-RID traversal met first and on how the rows were spread across buckets.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8002SparseGroupedShortfallTest extends TestHelper {
  private static final String TYPE_NAME = "SD";
  private static final String INDEX     = "SD[tokens,weights]";

  /** The issue's corpus, in the issue's order. */
  private static final String[] CATEGORY = { "c", "a", "a", "b", "b", "c", "c" };
  private static final float[]  SCORE    = { 0.99f, 0.90f, 0.80f, 0.70f, 0.60f, 0.50f, 0.40f };

  /** Insertion orders, as indexes into {@link #CATEGORY} / {@link #SCORE}. */
  private static final int[][] ORDERS = {
      { 0, 1, 2, 3, 4, 5, 6 }, // the issue's order
      { 1, 2, 3, 4, 0, 5, 6 }, // a and b fill the slots, then c's peak evicts b
      { 1, 2, 3, 4, 5, 0, 6 }, // c's trailing member is met BEFORE its peak
      { 5, 6, 1, 2, 3, 4, 0 }, // c's peak comes last
      { 6, 5, 4, 3, 2, 1, 0 }, // descending RID = ascending score
  };

  @ParameterizedTest
  @ValueSource(ints = { 1, 3, 8 })
  void everyAdmittedGroupGetsItsFullGroupSize(final int buckets) {
    for (int o = 0; o < ORDERS.length; o++) {
      final String typeName = TYPE_NAME + o;
      final int[] order = ORDERS[o];
      createType(typeName, buckets);
      database.transaction(() -> {
        for (final int i : order) {
          final MutableDocument d = database.newDocument(typeName);
          d.set("category", CATEGORY[i]);
          d.set("tokens", new int[] { 1 });
          d.set("weights", new float[] { SCORE[i] });
          d.save();
        }
      });

      assertThat(query(typeName)).as("order #%d, %d bucket(s)", o, buckets)
          .containsExactly("c:0.99", "a:0.9", "a:0.8", "c:0.5");
    }
  }

  /**
   * The cross-bucket half: every bucket answers its own top-{@code limit} groups, so a bucket in which {@code c}'s
   * trailing member ranks third locally never hands it over, although {@code c} wins globally on a peak that lives in
   * another bucket.
   */
  @Test
  void groupWhosePeakLivesInAnotherBucketStillGetsItsTrailingMembers() {
    final List<String> buckets = createType(TYPE_NAME, 2);
    database.transaction(() -> {
      insert(TYPE_NAME, buckets.get(0), 1, 2, 3, 4, 5, 6);
      insert(TYPE_NAME, buckets.get(1), 0);
    });
    assertThat(query(TYPE_NAME)).containsExactly("c:0.99", "a:0.9", "a:0.8", "c:0.5");
  }

  /**
   * The in-transaction half (#7966 contract): the committed rows alone rank {@code c} out, a pending row promotes it,
   * and the answer inside the transaction must equal the one the same search gives after the commit.
   */
  @Test
  void pendingRowPromotingAGroupAlsoBringsItsCommittedMembers() {
    final List<String> buckets = createType(TYPE_NAME, 1);
    database.transaction(() -> insert(TYPE_NAME, buckets.get(0), 1, 2, 3, 4, 5, 6));
    database.transaction(() -> {
      insert(TYPE_NAME, buckets.get(0), 0);
      assertThat(query(TYPE_NAME)).as("inside the transaction").containsExactly("c:0.99", "a:0.9", "a:0.8", "c:0.5");
    });
    assertThat(query(TYPE_NAME)).as("after the commit").containsExactly("c:0.99", "a:0.9", "a:0.8", "c:0.5");
  }

  /**
   * The dense twin of the cross-bucket case: {@code vector.neighbors} merges one grouped search per bucket through the
   * same admission, so it drops the same trailing member.
   */
  @Test
  void denseGroupWhosePeakLivesInAnotherBucketStillGetsItsTrailingMembers() {
    final List<String> buckets = new ArrayList<>();
    database.transaction(() -> {
      final DocumentType t = database.getSchema().buildDocumentType().withName("DD").withTotalBuckets(2).create();
      t.createProperty("category", Type.STRING);
      t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex("DD", new String[] { "embedding" }).withLSMVectorType().withDimensions(2)
          .withSimilarity("COSINE").create();
      for (final Bucket b : t.getBuckets(false))
        buckets.add(b.getName());
    });
    database.transaction(() -> {
      for (int i = 0; i < CATEGORY.length; i++) {
        final MutableDocument d = database.newDocument("DD");
        d.set("category", CATEGORY[i]);
        // cosine similarity to [1, 0] equals SCORE[i], so distance ranks the rows exactly as the sparse score does
        d.set("embedding", new float[] { SCORE[i], (float) Math.sqrt(1.0 - SCORE[i] * SCORE[i]) });
        d.save(buckets.get(i == 0 ? 1 : 0));
      }
    });

    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`('DD[embedding]', [1.0, 0.0], 2, { groupBy: 'category', groupSize: 2 }))")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("category") + ":" + Math.round(((float[]) r.getProperty("embedding"))[0] * 100) / 100f);
      }
    }
    assertThat(rows).containsExactly("c:0.99", "a:0.9", "a:0.8", "c:0.5");
  }

  /**
   * Randomised over bucket layouts, limits and group sizes, both from the committed state and from inside a transaction
   * holding part of the corpus as pending rows: the answer must always be the score-ordered admission rule applied to
   * the whole corpus.
   */
  @Test
  void randomisedMultiBucketAnswersMatchTheAdmissionRule() {
    final Random rnd = new Random(8002L);
    final List<String> buckets = createType(TYPE_NAME, 4);
    final List<String> categoryOf = new ArrayList<>();
    final List<Float> scoreOf = new ArrayList<>();
    final Runnable insertBatch = () -> {
      for (int i = 0; i < 150; i++) {
        final String category = "k" + rnd.nextInt(12);
        // Mostly low scores, with the odd row far above the rest: a group's peak and its other members end up in
        // different buckets.
        final float score = rnd.nextInt(20) == 0 ? 0.9f + rnd.nextFloat() * 0.1f : rnd.nextFloat() * 0.5f;
        final MutableDocument d = database.newDocument(TYPE_NAME);
        d.set("category", category);
        d.set("tokens", new int[] { 1 });
        d.set("weights", new float[] { score });
        d.save(buckets.get(rnd.nextInt(buckets.size())));
        categoryOf.add(category);
        scoreOf.add(score);
      }
    };
    database.transaction(insertBatch::run);

    for (int q = 0; q < 20; q++) {
      final int limit = 1 + rnd.nextInt(5);
      final int groupSize = 1 + rnd.nextInt(4);
      assertThat(query(TYPE_NAME, limit, groupSize)).as("committed, limit %d, groupSize %d", limit, groupSize)
          .isEqualTo(expected(categoryOf, scoreOf, limit, groupSize));
    }

    database.transaction(() -> {
      insertBatch.run();
      for (int q = 0; q < 20; q++) {
        final int limit = 1 + rnd.nextInt(5);
        final int groupSize = 1 + rnd.nextInt(4);
        assertThat(query(TYPE_NAME, limit, groupSize)).as("in transaction, limit %d, groupSize %d", limit, groupSize)
            .isEqualTo(expected(categoryOf, scoreOf, limit, groupSize));
      }
    });
  }

  /**
   * The dense twin of the randomised test: {@code vector.neighbors} over four buckets, each small enough that one beam
   * covers its whole graph, so every per-bucket search is exact and the answer must be the admission rule applied to
   * the whole corpus by cosine distance.
   */
  @Test
  void randomisedDenseMultiBucketAnswersMatchTheAdmissionRule() {
    final Random rnd = new Random(80022L);
    final List<String> buckets = new ArrayList<>();
    database.transaction(() -> {
      final DocumentType t = database.getSchema().buildDocumentType().withName("DR").withTotalBuckets(4).create();
      t.createProperty("category", Type.STRING);
      t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex("DR", new String[] { "embedding" }).withLSMVectorType().withDimensions(2)
          .withSimilarity("COSINE").create();
      for (final Bucket b : t.getBuckets(false))
        buckets.add(b.getName());
    });
    final List<String> categoryOf = new ArrayList<>();
    final List<Float> scoreOf = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 160; i++) {
        final String category = "k" + rnd.nextInt(12);
        // Distinct similarities to [1, 0]: 0.001 steps, shuffled, with the odd group peak far above the rest.
        final float score = rnd.nextInt(20) == 0 ? 0.9f + i * 0.0005f : 0.1f + i * 0.004f;
        final MutableDocument d = database.newDocument("DR");
        d.set("category", category);
        d.set("embedding", new float[] { score, (float) Math.sqrt(1.0 - score * score) });
        d.save(buckets.get(rnd.nextInt(buckets.size())));
        categoryOf.add(category);
        scoreOf.add(score);
      }
    });

    for (int q = 0; q < 20; q++) {
      final int limit = 1 + rnd.nextInt(5);
      final int groupSize = 1 + rnd.nextInt(4);
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(`vector.neighbors`('DR[embedding]', [1.0, 0.0], " + limit + ", { groupBy: 'category', groupSize: "
              + groupSize + " }))")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          rows.add(r.getProperty("category") + ":" + ((float[]) r.getProperty("embedding"))[0]);
        }
      }
      assertThat(rows).as("limit %d, groupSize %d", limit, groupSize).isEqualTo(expected(categoryOf, scoreOf, limit, groupSize));
    }
  }

  private static List<String> expected(final List<String> categoryOf, final List<Float> scoreOf, final int limit,
      final int groupSize) {
    final List<Integer> order = new ArrayList<>();
    for (int i = 0; i < scoreOf.size(); i++)
      order.add(i);
    order.sort((a, b) -> Float.compare(scoreOf.get(b), scoreOf.get(a)));
    final GroupAdmissionState admission = new GroupAdmissionState(limit, groupSize);
    final List<String> out = new ArrayList<>();
    for (final int i : order) {
      if (admission.isFull())
        break;
      if (admission.admit(categoryOf.get(i)))
        out.add(categoryOf.get(i) + ":" + scoreOf.get(i));
    }
    return out;
  }

  private List<String> query(final String typeName, final int limit, final int groupSize) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`('" + typeName + "[tokens,weights]', [1], [1.0], " + limit
            + ", { groupBy: 'category', groupSize: " + groupSize + " }))")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("category") + ":" + ((Number) r.getProperty("score")).floatValue());
      }
    }
    return rows;
  }

  private List<String> createType(final String typeName, final int buckets) {
    final List<String> names = new ArrayList<>();
    database.transaction(() -> {
      final DocumentType t = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
      t.createProperty("category", Type.STRING);
      t.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      t.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex(typeName, new String[] { "tokens", "weights" }).withSparseVectorType()
          .withDimensions(4).create();
      for (final Bucket b : t.getBuckets(false))
        names.add(b.getName());
    });
    return names;
  }

  private void insert(final String typeName, final String bucket, final int... rows) {
    for (final int i : rows) {
      final MutableDocument d = database.newDocument(typeName);
      d.set("category", CATEGORY[i]);
      d.set("tokens", new int[] { 1 });
      d.set("weights", new float[] { SCORE[i] });
      d.save(bucket);
    }
  }

  private List<String> query(final String typeName) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`('" + typeName + "[tokens,weights]', [1], [1.0], 2, { groupBy: 'category', groupSize: 2 }))")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("category") + ":" + Math.round(((Number) r.getProperty("score")).floatValue() * 100) / 100f);
      }
    }
    return rows;
  }
}
