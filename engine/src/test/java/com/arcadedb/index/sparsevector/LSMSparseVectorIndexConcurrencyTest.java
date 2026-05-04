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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrency regression test for {@link LSMSparseVectorIndex} (issue #4065).
 * <p>
 * Validates that the sparse vector index behaves correctly under parallel writers and readers.
 * Concretely, while writer threads insert documents, reader threads run {@code vector.sparseNeighbors}
 * queries and must never see exceptions or stale state that violates the index contract.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class LSMSparseVectorIndexConcurrencyTest extends TestHelper {

  private static final String TYPE_NAME       = "ConcurrentSparseDoc";
  private static final String IDX_NAME        = "ConcurrentSparseDoc[tokens,weights]";
  private static final int    DIMENSIONS      = 200;
  private static final int    NNZ_PER_DOC     = 6;
  private static final int    WRITER_THREADS  = 4;
  private static final int    DOCS_PER_WRITER = 50;
  private static final int    READER_THREADS  = 4;
  private static final int    QUERIES_PER_READER = 30;

  @Test
  void parallelInsertsAndQueriesAreCorrect() throws Exception {
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

    final ExecutorService executor = Executors.newFixedThreadPool(WRITER_THREADS + READER_THREADS);
    final CountDownLatch latch = new CountDownLatch(WRITER_THREADS + READER_THREADS);
    final AtomicInteger writerErrors = new AtomicInteger();
    final AtomicInteger readerErrors = new AtomicInteger();
    final List<Throwable> failures = new CopyOnWriteArrayList<>();

    for (int t = 0; t < WRITER_THREADS; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          final Random rnd = new Random(0xC0FFEEL ^ threadId);
          for (int i = 0; i < DOCS_PER_WRITER; i++) {
            final int[]   indices = new int[NNZ_PER_DOC];
            final float[] values  = new float[NNZ_PER_DOC];
            final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
            for (int j = 0; j < NNZ_PER_DOC; j++) {
              int dim;
              do {
                dim = rnd.nextInt(DIMENSIONS);
              } while (!picked.add(dim));
              indices[j] = dim;
              values[j] = 0.1f + rnd.nextFloat();
            }
            database.transaction(() -> {
              final MutableDocument doc = database.newDocument(TYPE_NAME);
              doc.set("tokens", indices);
              doc.set("weights", values);
              doc.save();
            });
          }
        } catch (final Throwable e) {
          writerErrors.incrementAndGet();
          failures.add(e);
        } finally {
          latch.countDown();
        }
      });
    }

    for (int t = 0; t < READER_THREADS; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          final Random rnd = new Random(0xBADF00DL ^ threadId);
          for (int i = 0; i < QUERIES_PER_READER; i++) {
            final int[]   queryIndices = new int[NNZ_PER_DOC];
            final float[] queryValues  = new float[NNZ_PER_DOC];
            final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
            for (int j = 0; j < NNZ_PER_DOC; j++) {
              int dim;
              do {
                dim = rnd.nextInt(DIMENSIONS);
              } while (!picked.add(dim));
              queryIndices[j] = dim;
              queryValues[j] = 0.1f + rnd.nextFloat();
            }
            try (ResultSet rs = database.query("sql",
                "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
                IDX_NAME, queryIndices, queryValues, 5)) {
              while (rs.hasNext()) {
                final var row = rs.next();
                final Object ridObj = row.getProperty("@rid");
                assertThat(ridObj).isInstanceOf(RID.class);
                assertThat(((Number) row.getProperty("score")).floatValue()).isGreaterThanOrEqualTo(0.0f);
              }
            }
            Thread.sleep(1);
          }
        } catch (final Throwable e) {
          readerErrors.incrementAndGet();
          failures.add(e);
        } finally {
          latch.countDown();
        }
      });
    }

    final boolean finishedInTime = latch.await(2, TimeUnit.MINUTES);
    executor.shutdown();
    assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

    assertThat(finishedInTime).as("workers must finish within budget").isTrue();
    assertThat(writerErrors.get()).as("writer errors: " + failures).isZero();
    assertThat(readerErrors.get()).as("reader errors: " + failures).isZero();

    final long docCount = database.countType(TYPE_NAME, false);
    assertThat(docCount).isEqualTo((long) WRITER_THREADS * DOCS_PER_WRITER);

    // Final correctness probe: a query that hits the densest part of the dim space must return
    // results. We probe a small head of dims so a corrupted index, dropped postings, or scrambled
    // dimMaxWeight cache would all show up as either zero results or non-monotonic scores.
    final int[]   probeIdx = new int[] { 0, 1, 2 };
    final float[] probeVal = new float[] { 1.0f, 1.0f, 1.0f };
    try (ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, probeIdx, probeVal, 10)) {
      final Set<RID>    seen   = new HashSet<>();
      float             prev   = Float.POSITIVE_INFINITY;
      int               returned = 0;
      while (rs.hasNext()) {
        final var row = rs.next();
        final RID rid = (RID) row.getProperty("@rid");
        final float score = ((Number) row.getProperty("score")).floatValue();

        assertThat(seen.add(rid)).as("results must be distinct RIDs").isTrue();
        assertThat(score).as("score for %s is non-negative under dot product", rid)
            .isGreaterThanOrEqualTo(0.0f);
        assertThat(score).as("results must arrive in non-increasing score order")
            .isLessThanOrEqualTo(prev);
        prev = score;
        returned++;
      }
      // Some of our writer-thread inserts must overlap dim 0/1/2 given the random distribution
      // and the corpus size (200 docs each with 6 nnz over a 200-dim space). If we ever get zero
      // hits, the writer/reader race corrupted something - flag it loudly.
      assertThat(returned).as("steady-state probe must return at least one neighbour")
          .isPositive();
    }
  }
}
