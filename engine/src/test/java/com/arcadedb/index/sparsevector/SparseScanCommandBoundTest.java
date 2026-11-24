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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.WorkGuard;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sparse scan stops with the abort of the guard it is given - the deadline of the command, or the termination of its
 * statement - from inside the loops that walk the postings, not only between searches.
 * <p>
 * The postings are served by cursors that terminate the statement after a fixed number of moves, so the point where the bound
 * is reached does not depend on the speed of the machine, and the number of postings read after it is what bounds the abort.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SparseScanCommandBoundTest extends TestHelper {
  private static final int DOCS            = 40_000;
  private static final int DIMS            = 3;
  private static final int K               = 10;
  private static final int TERMINATE_AFTER = 1_000;
  /** What the scan may still read once the bound is reached: a few check intervals of 1024 steps, with a posting or so each. */
  private static final int SLACK           = 12_000;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  private int  previousWindow;
  private int  previousPartitions;
  private long previousMinPostings;

  @BeforeEach
  void rememberTheScanSettings() {
    previousWindow = GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.getValueAsInteger();
    previousPartitions = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    previousMinPostings = GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong();
  }

  @AfterEach
  void restoreTheScanSettings() {
    GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(previousWindow);
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(previousPartitions);
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(previousMinPostings);
  }

  @Test
  void theScanReadsEveryPostingWhenNothingStopsIt() throws Exception {
    final Memtable memtable = ascendingCorpus();
    final AtomicInteger moves = new AtomicInteger();
    try (final RunningQuery statement = openStatement()) {
      final DimCursor[] cursors = cursorsOver(memtable, statement, moves, Integer.MAX_VALUE);
      final List<RidScore> answer = BmwScorer.topK(queryDims(), queryWeights(), cursors, K, null, null,
          WorkGuard.forCommandDeadline(null));
      closeAll(cursors);

      assertThat(answer).hasSize(K);
      assertThat(moves.get()).as("the corpus is walked to its end, so the bounds below are not met by pruning").isGreaterThan(DOCS);
    }
  }

  @Test
  void aTerminationEndsTheWindowedScanWithinAFewChecks() throws Exception {
    GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(16_384);
    assertTerminationEndsTheScan();
  }

  @Test
  void aTerminationEndsTheDocumentAtATimeScanWithinAFewChecks() throws Exception {
    GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(0);
    assertTerminationEndsTheScan();
  }

  @Test
  void aTerminationEndsAGroupedScanWithinAFewChecks() throws Exception {
    final Memtable memtable = ascendingCorpus();
    final AtomicInteger resolved = new AtomicInteger();
    try (final RunningQuery statement = openStatement()) {
      final Function<RID, Object> resolver = rid -> {
        if (resolved.incrementAndGet() == TERMINATE_AFTER)
          statement.terminate("root");
        return rid.getPosition() % 5;
      };

      assertThatThrownBy(() -> BmwScorer.topKGrouped(queryDims(), queryWeights(),
          () -> cursorsOver(memtable, statement, new AtomicInteger(), Integer.MAX_VALUE), 3, 2, resolver, null, null,
          WorkGuard.forCommandDeadline(null))).isInstanceOf(QueryTerminatedException.class);

      assertThat(resolved.get()).as("candidates resolved once the statement was terminated").isLessThan(TERMINATE_AFTER + SLACK);
    }
  }

  @Test
  void aReachedDeadlineEndsTheScanBeforeItReadsAnything() throws Exception {
    final Memtable memtable = ascendingCorpus();
    final AtomicInteger moves = new AtomicInteger();
    final DimCursor[] cursors = cursorsOver(memtable, null, moves, Integer.MAX_VALUE);
    try {
      assertThatThrownBy(() -> BmwScorer.topK(queryDims(), queryWeights(), cursors, K, null, null,
          WorkGuard.forDeadline("the scan", System.currentTimeMillis() - 1))).isInstanceOf(TimeoutException.class);
      assertThat(moves.get()).as("cursors started or moved").isZero();
    } finally {
      closeAll(cursors);
    }
  }

  @Test
  void aReachedDeadlineOpensNoCursorOfAGroupedScan() {
    final Memtable memtable = ascendingCorpus();
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger moves = new AtomicInteger();
    final BmwScorer.CursorSource source = () -> {
      opened.incrementAndGet();
      return cursorsOver(memtable, null, moves, Integer.MAX_VALUE);
    };

    assertThatThrownBy(() -> BmwScorer.topKGrouped(queryDims(), queryWeights(), source, 3, 2, rid -> rid.getPosition() % 5, null,
        null, WorkGuard.forDeadline("the scan", System.currentTimeMillis() - 1))).isInstanceOf(TimeoutException.class);
    assertThat(opened.get()).as("cursor sets opened").isZero();
    assertThat(moves.get()).as("cursors started or moved").isZero();
  }

  @Test
  void aWaitForTasksQueuedOnThePoolEndsWithTheTerminationOfTheStatement() throws Exception {
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger finished = new AtomicInteger();
    try (final RunningQuery statement = openStatement()) {
      final List<Callable<Integer>> tasks = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        final int id = i;
        tasks.add(() -> {
          // The statement is terminated once the first task runs; the tasks themselves go on until released
          if (id == 0)
            statement.terminate("root");
          release.await(60, TimeUnit.SECONDS);
          finished.incrementAndGet();
          return id;
        });
      }

      try {
        assertThatThrownBy(() -> SparseVectorScoringPool.runAll(tasks, false, WorkGuard.forCommandDeadline(null)))
            .isInstanceOf(QueryTerminatedException.class);
        assertThat(finished.get()).as("the wait ended while the tasks were still held").isZero();
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void aTerminationReachesTheCallerAsItIsWhenTheQueryIsSplitIntoRanges() throws Exception {
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(4);
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1L);

    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine((DatabaseInternal) database, "splitBound",
        SegmentParameters.builder().weightQuantization(WeightQuantization.FP32).build());
        final RunningQuery statement = openStatement()) {
      final Random random = new Random(0xB0DL);
      database.transaction(() -> {
        for (int i = 1; i <= 6_000; i++)
          for (int dim = 0; dim < DIMS; dim++)
            engine.put(dim, new RID(0, i), 0.1f + random.nextFloat());
        engine.flush();
      });

      final WorkGuard guard = WorkGuard.forCommandDeadline(null);
      assertThat(engine.topK(queryDims(), queryWeights(), K, guard)).as("the control, not yet terminated").hasSize(K);
      assertThat(engine.partitionedQueryCount()).as("the query is split, or this proves nothing about ranges").isEqualTo(1L);

      statement.terminate("root");
      assertThatThrownBy(() -> engine.topK(queryDims(), queryWeights(), K, guard)).isInstanceOf(QueryTerminatedException.class);
      assertThat(engine.partitionedQueryCount()).isEqualTo(2L);
    }
  }

  @Test
  void aTerminationStopsTheDocumentFrequencyCountsOfEveryBucket() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("CountedDoc").withTotalBuckets(3).create();
      database.command("sql", "CREATE PROPERTY CountedDoc.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY CountedDoc.weights ARRAY_OF_FLOATS");
      database.getSchema().buildTypeIndex("CountedDoc", new String[] { "tokens", "weights" }).withSparseVectorType()
          .withDimensions(16).withModifier("IDF").create();
      for (int i = 0; i < 90; i++) {
        final MutableDocument document = database.newDocument("CountedDoc");
        document.set("tokens", new int[] { 1, 2, 3 });
        document.set("weights", new float[] { 1f, 1f, 1f });
        document.save();
      }
    });

    final TypeIndex typeIndex = database.getSchema().getType("CountedDoc").getPolymorphicIndexByProperties("tokens", "weights");
    final List<LSMSparseVectorIndex> buckets = new ArrayList<>();
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      buckets.add((LSMSparseVectorIndex) bucketIndex);
    assertThat(buckets).hasSizeGreaterThan(1);
    final int[] indices = { 1, 2, 3 };
    final float[] values = { 1f, 1f, 1f };

    assertThat(buckets.get(0).effectiveWeights(indices, values, WorkGuard.forCommandDeadline(null)))
        .as("the control: the counts complete when nothing stops them").hasSize(3);

    try (final RunningQuery statement = openStatement()) {
      statement.terminate("root");
      final WorkGuard guard = WorkGuard.forCommandDeadline(null);

      assertThatThrownBy(() -> buckets.get(0).effectiveWeights(indices, values, guard)).isInstanceOf(QueryTerminatedException.class);

      // The same bound read by tasks that run on the scoring pool: the abort comes back as it is, not wrapped
      final List<Callable<List<RidScore>>> searches = new ArrayList<>();
      for (final LSMSparseVectorIndex bucket : buckets)
        searches.add(() -> bucket.topKWeighted(indices, values, K, null, null, guard.withoutInterrupt()));
      assertThatThrownBy(() -> SparseVectorScoringPool.runAll(searches, false)).isInstanceOf(QueryTerminatedException.class);
    }
  }

  private void assertTerminationEndsTheScan() throws Exception {
    final Memtable memtable = ascendingCorpus();
    final AtomicInteger moves = new AtomicInteger();
    try (final RunningQuery statement = openStatement()) {
      final DimCursor[] cursors = cursorsOver(memtable, statement, moves, TERMINATE_AFTER);
      try {
        assertThatThrownBy(() -> BmwScorer.topK(queryDims(), queryWeights(), cursors, K, null, null,
            WorkGuard.forCommandDeadline(null))).isInstanceOf(QueryTerminatedException.class);
      } finally {
        closeAll(cursors);
      }

      assertThat(moves.get()).as("postings read once the statement was terminated, of %d", DOCS * DIMS)
          .isGreaterThanOrEqualTo(TERMINATE_AFTER).isLessThan(TERMINATE_AFTER + SLACK);
    }
  }

  private RunningQuery openStatement() {
    return registry.register("scanBound", "admin", "test", null, "scan-bound", RunningQuery.Client.NONE);
  }

  /** Every document carries every dim with a weight that grows with its RID, so each one enters the answer and nothing prunes. */
  private static Memtable ascendingCorpus() {
    final Memtable memtable = new Memtable();
    for (int i = 1; i <= DOCS; i++)
      for (int dim = 0; dim < DIMS; dim++)
        memtable.put(dim, new RID(0, i), 0.001f * i);
    return memtable;
  }

  private static DimCursor[] cursorsOver(final Memtable memtable, final RunningQuery statement, final AtomicInteger moves,
      final int terminateAfter) {
    final DimCursor[] cursors = new DimCursor[DIMS];
    for (int dim = 0; dim < DIMS; dim++)
      cursors[dim] = new DimCursor(dim, List.of(new CountingCursor(new MemtableSourceCursor(memtable, dim), statement, moves,
          terminateAfter)));
    return cursors;
  }

  private static void closeAll(final DimCursor[] cursors) {
    for (final DimCursor cursor : cursors)
      cursor.close();
  }

  private static int[] queryDims() {
    final int[] dims = new int[DIMS];
    for (int i = 0; i < DIMS; i++)
      dims[i] = i;
    return dims;
  }

  private static float[] queryWeights() {
    final float[] weights = new float[DIMS];
    Arrays.fill(weights, 1.0f);
    return weights;
  }

  /** Forwards to a cursor, counts the postings it moves over and terminates the statement when the count reaches a limit. */
  private static final class CountingCursor implements SourceCursor {
    private final SourceCursor  delegate;
    private final RunningQuery  statement;
    private final AtomicInteger moves;
    private final int           terminateAfter;

    CountingCursor(final SourceCursor delegate, final RunningQuery statement, final AtomicInteger moves, final int terminateAfter) {
      this.delegate = delegate;
      this.statement = statement;
      this.moves = moves;
      this.terminateAfter = terminateAfter;
    }

    private void moved() {
      if (moves.incrementAndGet() == terminateAfter && statement != null)
        statement.terminate("root");
    }

    @Override
    public void start() throws IOException {
      // Counted but never the point where the statement is terminated: starting a cursor reads its first block
      moves.incrementAndGet();
      delegate.start();
    }

    @Override
    public boolean advance() throws IOException {
      moved();
      return delegate.advance();
    }

    @Override
    public boolean seekTo(final RID target) throws IOException {
      moved();
      return delegate.seekTo(target);
    }

    @Override
    public boolean seekTo(final int bucketId, final long position) throws IOException {
      moved();
      return delegate.seekTo(bucketId, position);
    }

    @Override
    public RID currentRid() {
      return delegate.currentRid();
    }

    @Override
    public int currentBucketId() {
      return delegate.currentBucketId();
    }

    @Override
    public long currentPosition() {
      return delegate.currentPosition();
    }

    @Override
    public float currentWeight() {
      return delegate.currentWeight();
    }

    @Override
    public boolean isTombstone() {
      return delegate.isTombstone();
    }

    @Override
    public boolean isExhausted() {
      return delegate.isExhausted();
    }

    @Override
    public float upperBoundRemaining() {
      return delegate.upperBoundRemaining();
    }

    @Override
    public long documentFrequency() {
      return delegate.documentFrequency();
    }

    @Override
    public float blockMaxAt(final RID rid) {
      return delegate.blockMaxAt(rid);
    }

    @Override
    public float blockMaxAt(final int bucketId, final long position) {
      return delegate.blockMaxAt(bucketId, position);
    }

    @Override
    public RID blockEndAt(final RID rid) {
      return delegate.blockEndAt(rid);
    }

    @Override
    public RID blockEndAt(final int bucketId, final long position) {
      return delegate.blockEndAt(bucketId, position);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
