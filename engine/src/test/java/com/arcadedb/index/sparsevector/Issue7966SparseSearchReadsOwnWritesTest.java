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
package com.arcadedb.index.sparsevector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7966: a sparse-vector search issued inside the transaction that wrote the row did not
 * see it.
 * <p>
 * Writes queue on {@code TransactionIndexContext} and reach the engine's memtable only at commit replay, while
 * {@code PaginatedSparseVectorEngine.topK} reads "an atomic snapshot of the current memtable + segments". So a
 * caller that wrote a sparse vector and ran {@code vector.sparseNeighbors} in the same transaction got nothing
 * back - and, as with the dense index in issue #7378, got FEWER results rather than an error, which an application
 * cannot tell apart from "no match".
 * <p>
 * The property every test here pins is the one #7378 established for the dense index: <b>a search inside the
 * transaction returns what the same search returns right after the commit.</b> That is what makes it a contract
 * rather than a heuristic, and it covers the update and delete cases as well as the insert one - a row the
 * transaction re-weighted must be scored at its new weight, and one it deleted must be gone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7966SparseSearchReadsOwnWritesTest extends TestHelper {

  private static final int    DIMENSIONS = 128;
  private static final String TYPE_NAME  = "SparseDoc";
  private static final String IDX_NAME   = "SparseDoc[tokens,weights]";

  @Test
  void aRowInsertedInTheTransactionIsFoundByTheSameTransaction() {
    createSchema();
    // A committed corpus to search against, so the answer is a ranking and not just "the one row".
    seed(20);

    final Map<RID, Float> inside = new LinkedHashMap<>();
    final RID[] created = new RID[1];
    database.transaction(() -> {
      created[0] = newDoc(new int[] { 3, 9, 27 }, new float[] { 0.8f, 0.6f, 0.4f }).save().getIdentity();
      inside.putAll(search(new int[] { 3, 9, 27 }, new float[] { 1f, 1f, 1f }, 5));
    });

    assertThat(inside)
        .as("the row this transaction inserted must be visible to its own search")
        .containsKey(created[0]);
    assertThat(inside.get(created[0])).isCloseTo(1.8f, Offset.offset(1e-4f));
    assertAgrees(inside, search(new int[] { 3, 9, 27 }, new float[] { 1f, 1f, 1f }, 5));
  }

  @Test
  void aRowReweightedInTheTransactionIsScoredAtItsNewWeight() {
    createSchema();
    seed(20);

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = newDoc(new int[] { 5 }, new float[] { 0.2f }).save().getIdentity());

    final Map<RID, Float> inside = new LinkedHashMap<>();
    database.transaction(() -> {
      database.lookupByRID(rid[0], true).asDocument(true).modify().set("weights", new float[] { 0.9f }).save();
      inside.putAll(search(new int[] { 5 }, new float[] { 1f }, 5));
    });

    assertThat(inside.get(rid[0]))
        .as("the search must score the row at the weight this transaction gave it, not the committed one")
        .isCloseTo(0.9f, Offset.offset(1e-4f));
    assertAgrees(inside, search(new int[] { 5 }, new float[] { 1f }, 5));
  }

  @Test
  void aRowDeletedInTheTransactionIsGoneFromTheSameTransaction() {
    createSchema();
    seed(20);

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = newDoc(new int[] { 11 }, new float[] { 0.95f }).save().getIdentity());

    final Map<RID, Float> inside = new LinkedHashMap<>();
    database.transaction(() -> {
      database.lookupByRID(rid[0], true).asDocument(true).modify().delete();
      inside.putAll(search(new int[] { 11 }, new float[] { 1f }, 5));
    });

    assertThat(inside)
        .as("a row this transaction deleted must not come back from its own search")
        .doesNotContainKey(rid[0]);
    assertThat(indexLevelRids(new int[] { 11 }, new float[] { 1f }, 5))
        .as("nor from the index itself: the SQL layer drops it only because the record lookup fails, which is a "
            + "second line of defence and not the contract")
        .doesNotContain(rid[0]);
    assertAgrees(inside, search(new int[] { 11 }, new float[] { 1f }, 5));
  }

  /**
   * The answer may not depend on whether the function fanned its per-bucket searches out to the scoring pool: a
   * {@code getTransactionIfExists()} on a pool worker answers null, so an overlay resolved inside the index would
   * make the two plans disagree. Ten buckets take the fan-out; one takes the serial path.
   */
  @Test
  void theMultiBucketFanOutAndTheSerialPlanGiveTheSameAnswer() {
    createSchema(10);
    seed(60);

    final Map<RID, Float> fannedOut = new LinkedHashMap<>();
    final RID[] created = new RID[1];
    database.transaction(() -> {
      created[0] = newDoc(new int[] { 2, 4, 8 }, new float[] { 0.7f, 0.7f, 0.7f }).save().getIdentity();
      fannedOut.putAll(search(new int[] { 2, 4, 8 }, new float[] { 1f, 1f, 1f }, 10));
    });

    assertThat(fannedOut)
        .as("the row must be visible however many sub-indexes the function had to search")
        .containsKey(created[0]);
    assertAgrees(fannedOut, search(new int[] { 2, 4, 8 }, new float[] { 1f, 1f, 1f }, 10));
  }

  /**
   * The grouped plan pushes its per-group caps into the DAAT loop, so a pending row cannot simply be appended to
   * its result: the caps have to be re-applied over the union. What must hold is the same invariant.
   */
  @Test
  void theGroupedPlanAlsoSeesTheTransactionsOwnRows() {
    createSchema();
    database.transaction(() -> {
      database.getSchema().getType(TYPE_NAME).createProperty("category", Type.STRING);
    });
    seedWithCategories(20);

    final Map<RID, Float> inside = new LinkedHashMap<>();
    final RID[] created = new RID[1];
    database.transaction(() -> {
      created[0] = newDoc(new int[] { 6, 12 }, new float[] { 0.9f, 0.9f }).set("category", "fresh").save().getIdentity();
      inside.putAll(searchGrouped(new int[] { 6, 12 }, new float[] { 1f, 1f }, 5, 2));
    });

    assertThat(inside)
        .as("the grouped plan must see the row this transaction inserted")
        .containsKey(created[0]);
    assertAgrees(inside, searchGrouped(new int[] { 6, 12 }, new float[] { 1f, 1f }, 5, 2));
  }

  /**
   * The invariant: the search inside the transaction returned the same rows, in the same rank order, at the same
   * scores as the one after the commit.
   * <p>
   * Scores are compared with a tolerance rather than bitwise. The pending row is scored by summing the query's
   * dimensions in query order, while after the commit it is summed by the BMW traversal in the order its terms
   * leave the traversal - the same arithmetic, a different association, so the two can differ by a float ULP
   * (1.8 against 1.8000001 on the very first run of this test). Demanding bit equality would be pinning the
   * summation order of an implementation detail.
   */
  private static void assertAgrees(final Map<RID, Float> inside, final Map<RID, Float> afterCommit) {
    assertThat(inside.keySet())
        .as("a search inside the transaction must return the same rows in the same order as after the commit")
        .containsExactlyElementsOf(afterCommit.keySet());
    for (final Map.Entry<RID, Float> row : afterCommit.entrySet())
      assertThat(inside.get(row.getKey()))
          .as("the score of %s must be the same inside the transaction as after the commit", row.getKey())
          .isCloseTo(row.getValue(), Offset.offset(1e-5f));
  }

  /** What the index itself answers, bypassing the SQL layer's own record-lookup filter. */
  private List<RID> indexLevelRids(final int[] tokens, final float[] weights, final int k) {
    final List<RID> rids = new ArrayList<>();
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    for (final IndexInternal sub : typeIndex.getIndexesOnBuckets())
      for (final RidScore r : ((LSMSparseVectorIndex) sub).topK(tokens, weights, k, null))
        rids.add(r.rid());
    return rids;
  }

  // ---------- helpers ----------

  private void createSchema() {
    createSchema(1);
  }

  private void createSchema(final int buckets) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, buckets);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();
    });
    // The fan-out must be reachable in this test, whatever the machine's core count would otherwise decide.
    database.getConfiguration().setValue(GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS, 4);
  }

  private void seed(final int docs) {
    database.transaction(() -> {
      for (int i = 0; i < docs; i++)
        newDoc(dims(i), weights()).save();
    });
  }

  private void seedWithCategories(final int docs) {
    database.transaction(() -> {
      for (int i = 0; i < docs; i++)
        newDoc(dims(i), weights()).set("category", "c" + (i % 4)).save();
    });
  }

  private MutableDocument newDoc(final int[] tokens, final float[] weights) {
    return database.newDocument(TYPE_NAME).set("tokens", tokens).set("weights", weights);
  }

  private static int[] dims(final int seed) {
    final int[] d = new int[3];
    for (int i = 0; i < d.length; i++)
      d[i] = (seed * 5 + i * 13 + 1) % DIMENSIONS;
    java.util.Arrays.sort(d);
    for (int i = 1; i < d.length; i++)
      if (d[i] <= d[i - 1])
        d[i] = d[i - 1] + 1;
    return d;
  }

  private static float[] weights() {
    return new float[] { 0.3f, 0.25f, 0.2f };
  }

  /** RID to score, in rank order, so a comparison catches a reordering as well as a missing row. */
  private Map<RID, Float> search(final int[] tokens, final float[] weights, final int k) {
    return collect(database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, tokens, weights, k));
  }

  private Map<RID, Float> searchGrouped(final int[] tokens, final float[] weights, final int k, final int groupSize) {
    final Map<String, Object> options = new LinkedHashMap<>();
    options.put("groupBy", "category");
    options.put("groupSize", groupSize);
    return collect(database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, ?))",
        IDX_NAME, tokens, weights, k, options));
  }

  private static Map<RID, Float> collect(final ResultSet rs) {
    final Map<RID, Float> out = new LinkedHashMap<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      out.put(r.getProperty("@rid"), ((Number) r.getProperty("score")).floatValue());
    }
    return out;
  }
}
