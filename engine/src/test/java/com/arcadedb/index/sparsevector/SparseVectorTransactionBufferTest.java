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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Write-path regression test for {@code LSM_SPARSE_VECTOR} (issue #5411).
 * <p>
 * A sparse vector contributes one posting per non-zero dimension, so a single record produces
 * tens to hundreds of transaction index entries instead of the one entry an LSM-Tree index
 * produces. Routing each of those through {@link TransactionIndexContext}'s key-ordered
 * {@code TreeMap} (plus a per-key {@code HashMap}) made transaction bookkeeping dominate the
 * build: profiling a 100k x 130-nnz load showed ~87% of wall clock inside
 * {@code TransactionIndexContext}, against ~14% in the memtable that actually stores the data.
 * <p>
 * The sparse index declares {@link IndexInternal#isTransactionKeyOrderRequired()} {@code false}
 * so its postings land on an append-only lane that replays in insertion order. These tests pin
 * both halves: that the ordered lane stays unused, and that the ordering semantics the ordered
 * lane used to provide (last operation on a posting wins; rollback discards) are preserved.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseVectorTransactionBufferTest extends TestHelper {

  private static final int    DIMENSIONS = 256;
  private static final String TYPE_NAME  = "BufferedSparseDoc";
  private static final String IDX_NAME   = "BufferedSparseDoc[tokens,weights]";

  @Test
  void postingsSkipTheKeyOrderedTransactionMap() {
    createSchema();

    final int docs = 20;
    final int nnz  = 8;

    database.begin();
    for (int i = 0; i < docs; i++)
      newDoc(dims(nnz, i), weights(nnz)).save();

    final TransactionIndexContext changes = ((DatabaseInternal) database).getTransaction().getIndexChanges();
    for (final String subIndex : subIndexNames()) {
      assertThat(changes.getIndexKeys(subIndex))
          .as("sparse postings must not populate the key-ordered TreeMap lane of index '%s'", subIndex)
          .isNull();
    }
    // ...while still being queued: the totals must account for every posting of every record.
    int queued = 0;
    for (final String subIndex : subIndexNames())
      queued += changes.getTotalEntriesByIndex(subIndex);
    assertThat(queued).as("every posting must be queued for commit replay").isEqualTo(docs * nnz);
    assertThat(changes.getTotalEntries()).isEqualTo(docs * nnz);
    assertThat(changes.isEmpty()).isFalse();

    database.commit();

    // The postings really reached the engine.
    assertThat(topKRids(dims(nnz, 0), weights(nnz), 5)).isNotEmpty();
  }

  @Test
  void rollbackDiscardsQueuedPostings() {
    createSchema();

    database.begin();
    newDoc(new int[] { 1, 2, 3 }, new float[] { 1f, 1f, 1f }).save();
    assertThat(((DatabaseInternal) database).getTransaction().getIndexChanges().isEmpty()).isFalse();
    database.rollback();

    assertThat(((DatabaseInternal) database).getTransaction() == null || ((DatabaseInternal) database).getTransaction().getIndexChanges().isEmpty()).isTrue();
    assertThat(topKRids(new int[] { 1, 2, 3 }, new float[] { 1f, 1f, 1f }, 5)).isEmpty();
  }

  @Test
  void deleteAfterInsertInTheSameTransactionLeavesNoPosting() {
    createSchema();

    database.transaction(() -> {
      final MutableDocument doc = newDoc(new int[] { 4, 5 }, new float[] { 0.5f, 0.5f });
      doc.save();
      doc.delete();
    });

    assertThat(topKRids(new int[] { 4, 5 }, new float[] { 1f, 1f }, 5)).isEmpty();
  }

  @Test
  void updateInTheSameTransactionKeepsTheLastWeight() {
    createSchema();

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = newDoc(new int[] { 7 }, new float[] { 0.1f });
      doc.save();
      rid[0] = doc.getIdentity();
      doc.set("weights", new float[] { 0.9f });
      doc.save();
    });

    // A single document with a single dim: the surviving weight decides the score.
    final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, new int[] { 7 }, new float[] { 1.0f }, 5);
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat((RID) r.getProperty("@rid")).isEqualTo(rid[0]);
    assertThat(((Number) r.getProperty("score")).floatValue()).isCloseTo(0.9f, Offset.offset(1e-4f));
  }

  @Test
  void reinsertAfterDeleteInTheSameTransactionKeepsThePosting() {
    createSchema();

    database.transaction(() -> newDoc(new int[] { 9 }, new float[] { 0.4f }).save());

    database.transaction(() -> {
      final MutableDocument doc = database.iterateType(TYPE_NAME, false).next().asDocument().modify();
      doc.delete();
      newDoc(new int[] { 9 }, new float[] { 0.7f }).save();
    });

    final List<RID> found = topKRids(new int[] { 9 }, new float[] { 1.0f }, 5);
    assertThat(found).hasSize(1);
  }

  // ---------- helpers ----------

  private void createSchema() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();
    });
  }

  private MutableDocument newDoc(final int[] tokens, final float[] weights) {
    return database.newDocument(TYPE_NAME).set("tokens", tokens).set("weights", weights);
  }

  private static int[] dims(final int nnz, final int seed) {
    final int[] d = new int[nnz];
    for (int i = 0; i < nnz; i++)
      d[i] = (seed * 3 + i * 7) % DIMENSIONS;
    java.util.Arrays.sort(d);
    for (int i = 1; i < nnz; i++)
      if (d[i] <= d[i - 1])
        d[i] = d[i - 1] + 1;
    return d;
  }

  private static float[] weights(final int nnz) {
    final float[] w = new float[nnz];
    for (int i = 0; i < nnz; i++)
      w[i] = 0.1f + i * 0.05f;
    return w;
  }

  private List<String> subIndexNames() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    final List<String> names = new ArrayList<>();
    for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
      names.add(idx.getName());
    return names;
  }

  private List<RID> topKRids(final int[] tokens, final float[] weights, final int k) {
    final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, tokens, weights, k);
    final List<RID> rids = new ArrayList<>();
    while (rs.hasNext())
      rids.add((RID) rs.next().getProperty("@rid"));
    return rids;
  }
}
