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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Oracle tests for the dimension decision of an LSM_VECTOR index created with {@code sizeOnFirstVector} (issue #10915): the first
 * vector written sizes the index and its siblings on the other buckets of the type, a node that did not see the decision (a
 * database reopened before the schema was saved, a follower) takes the size from the vectors its records hold, and a query is
 * compared with the size so decided. The oracle is the exact cosine ranking of the live set, and the dimensions the index
 * reports.
 * <p>
 * A node that did not see the decision is simulated the way the engine documents it: the persisted schema says the index waits for
 * its dimensions ({@code dimensions = 0}, {@code sizeOnFirstVector = true}) while its pages hold vectors.
 */
@Tag("vector")
class VectorSizeOnFirstVectorOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int BUCKETS    = 3;
  private static final int RECORDS    = 30;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // one test removes records under the index on purpose
    return false;
  }

  @Test
  void theFirstVectorSizesEveryBucketIndexOfTheType() {
    createUndecidedType();
    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      assertThat(bucketIndex.getMetadata().dimensions).isZero();
      assertThat(bucketIndex.getMetadata().sizeOnFirstVector).isTrue();
    }

    final Map<RID, float[]> live = new LinkedHashMap<>();
    insert(live, 1, DIMENSIONS);

    // one record lives in one bucket only: the other two indexes got the size from the decision, not from a vector of their own
    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      assertThat(bucketIndex.getMetadata().dimensions).as(bucketIndex.getName()).isEqualTo(DIMENSIONS);
      assertThat(bucketIndex.getMetadata().sizeOnFirstVector).as(bucketIndex.getName()).isFalse();
    }

    // so a vector of another size routed to a sibling bucket is refused, as by any sized index, whatever bucket it lands in
    for (int attempt = 0; attempt < BUCKETS * 2; attempt++) {
      final int seed = 100 + attempt;
      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument("Doc").set("embedding", vector(DIMENSIONS - 3, seed)).save()))
          .hasStackTraceContaining("does not match index dimension " + DIMENSIONS);
    }
    assertThat(typeIndex().countEntries()).isEqualTo(1);

    insert(live, RECORDS, DIMENSIONS);
    assertThat(typeIndex().countEntries()).isEqualTo(RECORDS);
    assertMatchesBruteForce(live, 5);

    // the schema keeps the decision on every bucket index
    reopenDatabase();
    for (final LSMVectorIndex bucketIndex : bucketIndexes())
      assertThat(bucketIndex.getMetadata().dimensions).as(bucketIndex.getName()).isEqualTo(DIMENSIONS);
    assertMatchesBruteForce(live, 5);
  }

  @Test
  void aQueryAgainstAnIndexThatHoldsNoVectorYetAnswersEmptyWhateverItsLength() {
    createUndecidedType();

    for (final int length : new int[] { 1, DIMENSIONS, DIMENSIONS + 5 })
      for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
        assertThat(bucketIndex.findNeighborsFromVector(vector(length, 1), 3)).as("length %d on %s", length, bucketIndex.getName())
            .isEmpty();
        assertThat(bucketIndex.getMetadata().dimensions).as("a query decides nothing").isZero();
        assertThat(bucketIndex.getMetadata().sizeOnFirstVector).isTrue();
      }
  }

  @Test
  void anIndexThatDidNotSeeTheDecisionTakesItFromTheVectorsItsRecordsHold() {
    createUndecidedType();
    final Map<RID, float[]> live = new LinkedHashMap<>();
    insert(live, RECORDS, DIMENSIONS);
    forgetTheDecision();

    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      assertThat(bucketIndex.getMetadata().dimensions).as("the schema still waits for the size").isZero();
      assertThat(bucketIndex.getMetadata().sizeOnFirstVector).isTrue();
    }

    // a query of another length is compared with the size the records fix, not accepted for its own length
    final LSMVectorIndex first = bucketIndexes().getFirst();
    assertThatThrownBy(() -> first.findNeighborsFromVector(vector(DIMENSIONS + 2, 1), 3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Query vector dimension " + (DIMENSIONS + 2))
        .hasMessageContaining("does not match index dimension " + DIMENSIONS);
    assertThat(first.getMetadata().dimensions).as("taken from the record").isEqualTo(DIMENSIONS);
    assertThat(first.getMetadata().sizeOnFirstVector).isFalse();
    for (final LSMVectorIndex bucketIndex : bucketIndexes())
      assertThat(bucketIndex.getMetadata().dimensions).as("published to the sibling %s", bucketIndex.getName())
          .isEqualTo(DIMENSIONS);

    assertMatchesBruteForce(live, 5);
  }

  @Test
  void aQueryOfTheRightLengthSizesAnIndexThatDidNotSeeTheDecision() {
    createUndecidedType();
    final Map<RID, float[]> live = new LinkedHashMap<>();
    insert(live, RECORDS, DIMENSIONS);
    forgetTheDecision();

    // the very first call on the reopened index is the search: no put, no graph build came before it
    assertMatchesBruteForce(live, 5);
    for (final LSMVectorIndex bucketIndex : bucketIndexes())
      assertThat(bucketIndex.getMetadata().dimensions).as(bucketIndex.getName()).isEqualTo(DIMENSIONS);
  }

  @Test
  void aWriteIsHeldToTheSizeTheRecordsFixOnAnIndexThatDidNotSeeTheDecision() {
    createUndecidedType();
    final Map<RID, float[]> live = new LinkedHashMap<>();
    insert(live, RECORDS, DIMENSIONS);
    forgetTheDecision();

    // a vector of another length must be refused, not size the index a second time (issue #10993)
    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("Doc").set("embedding", vector(DIMENSIONS + 4, 500)).save()))
        .hasStackTraceContaining("does not match index dimension " + DIMENSIONS);
    for (final LSMVectorIndex bucketIndex : bucketIndexes())
      assertThat(bucketIndex.getMetadata().dimensions).as(bucketIndex.getName()).isEqualTo(DIMENSIONS);

    insert(live, RECORDS + 5, DIMENSIONS);
    assertMatchesBruteForce(live, 5);
  }

  @Test
  void anIndexWhoseEntriesLeadToNoRecordIsNotSizedByASearchButByTheNextWrite() {
    createUndecidedType();
    final Map<RID, float[]> live = new LinkedHashMap<>();
    insert(live, RECORDS, DIMENSIONS);
    // the records vanish without the index being told: its entries lead to no vector the dimensions can be taken from
    database.transaction(() -> {
      for (final RID rid : live.keySet())
        deleteRecordAtLowLevel(database, rid);
    });
    forgetTheDecision();

    // asked twice: the second look is answered from what the first one found, and neither decides anything
    final LSMVectorIndex first = bucketIndexes().getFirst();
    for (int round = 0; round < 2; round++) {
      assertThat(first.findNeighborsFromVector(vector(DIMENSIONS, 1), 3)).hasSizeLessThanOrEqualTo(3);
      assertThat(first.getMetadata().dimensions).isZero();
      assertThat(first.getMetadata().sizeOnFirstVector).isTrue();
    }

    // a vector written afterwards sizes the index, and its siblings, whatever its length
    final Map<RID, float[]> fresh = new LinkedHashMap<>();
    insert(fresh, 3, DIMENSIONS + 1);
    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      assertThat(bucketIndex.getMetadata().dimensions).as(bucketIndex.getName()).isEqualTo(DIMENSIONS + 1);
      assertThat(bucketIndex.getMetadata().sizeOnFirstVector).isFalse();
    }
  }

  @Test
  void aCommitReplayedOnAnIndexThatDidNotSeeTheDecisionSizesItOnTheVectorsItReplays() {
    createUndecidedType();
    final Map<RID, float[]> live = new LinkedHashMap<>();

    // The transaction queues its index entry while the index is sized, as the node that ran the put did. The node that replays the
    // commit did not see that decision: its index still waits for the size when the entries are applied
    database.begin();
    final RID rid = database.newDocument("Doc").set("embedding", vector(DIMENSIONS, 7)).save().getIdentity();
    live.put(rid, vector(DIMENSIONS, 7));
    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      assertThat(bucketIndex.getMetadata().dimensions).isEqualTo(DIMENSIONS);
      bucketIndex.getMetadata().dimensions = 0;
      bucketIndex.getMetadata().sizeOnFirstVector = true;
    }
    database.commit();

    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      assertThat(bucketIndex.getMetadata().dimensions).as(bucketIndex.getName()).isEqualTo(DIMENSIONS);
      assertThat(bucketIndex.getMetadata().sizeOnFirstVector).isFalse();
    }
    assertThat(typeIndex().countEntries()).as("the replayed entry is indexed, not skipped as a mismatch").isEqualTo(1);
    assertMatchesBruteForce(live, 1);
  }

  @Test
  void aWriteOutsideAnyTransactionIsAppliedAtOnceAndNotQueuedOnATransactionThatIsNotThere() {
    createUndecidedType();
    final Map<RID, float[]> live = new LinkedHashMap<>();
    insert(live, RECORDS, DIMENSIONS);

    final Map.Entry<RID, float[]> entry = live.entrySet().iterator().next();
    final LSMVectorIndex holder = bucketIndexOf(entry.getKey());
    final long before = holder.countEntries();
    assertThat(database.isTransactionActive()).isFalse();

    // Without a transaction there is no commit to apply a queued entry later: the index applies the call itself, and its page writes
    // refuse to run outside a transaction. A call wrongly queued would return as if it had worked and be dropped with the context
    assertThatThrownBy(() -> holder.remove(new Object[] { entry.getValue() }, entry.getKey()))
        .isInstanceOf(IndexException.class).hasRootCauseInstanceOf(TransactionException.class);
    assertThatThrownBy(() -> holder.put(new Object[] { vector(DIMENSIONS, 900) }, new RID[] { entry.getKey() }))
        .hasRootCauseInstanceOf(TransactionException.class);

    assertThat(holder.countEntries()).as("nothing was queued for a commit that does not exist").isLessThanOrEqualTo(before);
  }

  @Test
  void anUpdateWritesOneVectorAndOneTombstonePerRecordNotOnePerPhaseOfTheCommit() {
    createUndecidedType();
    final Map<RID, float[]> live = new LinkedHashMap<>();
    insert(live, RECORDS, DIMENSIONS);
    for (final LSMVectorIndex bucketIndex : bucketIndexes())
      assertThat(bucketIndex.getStats().get("deletedVectors")).as("nothing is superseded yet in %s", bucketIndex.getName()).isZero();

    // the commit serializes the updated records once more (its first phase), and the index entries must not be applied twice
    database.transaction(() -> {
      int seed = 1000;
      for (final Map.Entry<RID, float[]> entry : live.entrySet()) {
        final float[] updated = vector(DIMENSIONS, seed++);
        entry.setValue(updated);
        entry.getKey().asDocument().modify().set("embedding", updated).save();
      }
    });

    long superseded = 0;
    long active = 0;
    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      superseded += bucketIndex.getStats().get("deletedVectors");
      active += bucketIndex.getStats().get("activeVectors");
    }
    assertThat(superseded).as("one tombstone per updated record").isEqualTo(RECORDS);
    assertThat(active).as("one live vector per record").isEqualTo(RECORDS);
    assertMatchesBruteForce(live, 5);
  }

  // ------------------------------------------------------------------------------------------------------- helpers

  private void createUndecidedType() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + BUCKETS);
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    // no SQL spelling: the index that waits for its first vector is created by the builder, or by CREATE VECTOR INDEX of Cypher
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withMetadata(new JSONObject().put("similarity", "COSINE").put("sizeOnFirstVector", true));
    builder.create();
  }

  /**
   * The state of a node that did not see the decision (a follower applying the pages of the node that decided): its index holds
   * the vectors and the location of each, and waits for a size.
   */
  private void forgetTheDecision() {
    for (final LSMVectorIndex bucketIndex : bucketIndexes()) {
      bucketIndex.getMetadata().dimensions = 0;
      bucketIndex.getMetadata().sizeOnFirstVector = true;
    }
  }

  private void insert(final Map<RID, float[]> live, final int total, final int dimensions) {
    final int first = live.size();
    database.transaction(() -> {
      for (int i = first; i < total; i++) {
        final float[] vector = vector(dimensions, i + 1);
        final RID rid = database.newDocument("Doc").set("embedding", vector).save().getIdentity();
        live.put(rid, vector);
      }
    });
  }

  private TypeIndex typeIndex() {
    return (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
  }

  private List<LSMVectorIndex> bucketIndexes() {
    final List<LSMVectorIndex> all = new ArrayList<>();
    for (final IndexInternal onBucket : typeIndex().getIndexesOnBuckets())
      all.add((LSMVectorIndex) onBucket);
    return all;
  }

  private LSMVectorIndex bucketIndexOf(final RID rid) {
    for (final LSMVectorIndex bucketIndex : bucketIndexes())
      if (bucketIndex.getAssociatedBucketId() == rid.getBucketId())
        return bucketIndex;
    throw new AssertionError("No bucket index for " + rid);
  }

  /** The k live vectors most similar to the query by cosine, over ALL buckets: what an exact search answers. */
  private void assertMatchesBruteForce(final Map<RID, float[]> live, final int k) {
    final Random random = new Random(5);
    for (int probe = 0; probe < 4; probe++) {
      final float[] query = vector(live.values().iterator().next().length, random.nextInt(1000) + 2000);
      final List<RID> expected = live.entrySet().stream()
          .sorted(Comparator.comparingDouble((Map.Entry<RID, float[]> e) -> -cosine(query, e.getValue())))
          .limit(k).map(Map.Entry::getKey).toList();

      final List<Pair<RID, Float>> merged = new ArrayList<>();
      for (final LSMVectorIndex bucketIndex : bucketIndexes())
        merged.addAll(bucketIndex.findNeighborsFromVector(query, k, 100));
      // what a bucket index returns is a distance: smaller is closer
      final List<RID> actual = merged.stream().sorted(Comparator.comparingDouble((Pair<RID, Float> p) -> p.getSecond()))
          .limit(k).map(Pair::getFirst).toList();
      assertThat(actual).as("probe %d", probe).containsExactlyElementsOf(expected);
    }
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }

  /** A non-zero vector distinct per seed, spread over the sphere so the exact ranking has no ties. */
  private static float[] vector(final int dimensions, final int seed) {
    final Random random = new Random(seed * 7919L + dimensions);
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      vector[i] = (float) random.nextGaussian();
    vector[0] += 0.5f;
    return vector;
  }
}
