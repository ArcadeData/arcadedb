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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A statement terminated while a vector search is INSIDE one of its loops stops at the next periodic check of the guard, not at the
 * end of the loop (issue #9896, the contract of {@link WorkGuard#checkPeriodically}: every 1024 iterations). The existing tests end
 * a search that is already terminated when it starts, or one that waits for a build; a check removed from a loop whose length is data
 * (the allow-list, the ordinals, the delta buffer) still ended such a search at the next loop that has one, so nothing saw it.
 * <p>
 * The oracle is a count, not a clock: each test terminates the registered statement from inside the loop under test, at its 300th
 * step (a read of a vector record, a member of the allow-list handed out, a membership question about a delta row), and counts the
 * steps taken after that. A loop that checks the guard stops within one interval, so the search throws and the count stays near 1024;
 * one that does not runs on to its end, thousands of steps further.
 */
@Tag("vector")
class VectorSearchGuardMidLoopOracleTest extends TestHelper {
  private static final int DIMENSIONS    = 8;
  private static final int ROWS          = 6000;
  private static final int TERMINATE_AT  = 300;
  /** One interval of the guard plus slack for the steps the loop takes around a check. */
  private static final int STEPS_ALLOWED = 1100;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();
  private       LSMVectorIndex       index;
  private       List<RID>            rids;
  private       List<float[]>        vectors;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);
    // The whole index is the widest allow-list the pre-filter plan accepts
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 1.0f);

    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withMetadata(new JSONObject().put("similarity", "COSINE").put("dimensions", DIMENSIONS));
    builder.create();

    rids = new ArrayList<>();
    vectors = new ArrayList<>();
    insert(ROWS);
    index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
  }

  @Test
  void anAllowListScanOfTheOrdinalsStopsWithinOneIntervalOfTheTermination() {
    // 3000 of the 6000 rows: the plan scores the allow-list ordinal by ordinal, one record read each
    final Set<RID> allowList = new HashSet<>(rids.subList(0, 3000));
    final Steps reads = new Steps();
    assertSearchStopsAtTheNextCheck(reads, guard -> index.findNeighborsFromVector(vectors.get(7), 10, 50, allowList, guard),
        () -> registerReadListener(reads));
  }

  @Test
  void aFullScanOfTheOrdinalsStopsWithinOneIntervalOfTheTermination() {
    // every row: the allow-list is as long as the index, so the plan walks all ordinals instead of resolving the list
    final Set<RID> allowList = new HashSet<>(rids);
    final Steps reads = new Steps();
    assertSearchStopsAtTheNextCheck(reads, guard -> index.findNeighborsFromVector(vectors.get(7), 10, 50, allowList, guard),
        () -> registerReadListener(reads));
  }

  @Test
  void theNarrowingOfAnAllowListStopsWithinOneIntervalOfTheTermination() {
    final Steps members = new Steps();
    final Set<RID> allowList = new CountingSet(new HashSet<>(rids.subList(0, 3000)), members, false);
    assertSearchStopsAtTheNextCheck(members, guard -> index.findNeighborsFromVector(vectors.get(7), 10, 50, allowList, guard),
        () -> () -> {
        });
  }

  @Test
  void theScanOfTheDeltaBufferStopsWithinOneIntervalOfTheTermination() {
    // rows committed after the graph was built wait in the delta buffer, and every search scans the buffer
    final int firstDelta = rids.size();
    insert(3000);
    final Set<RID> deltaRows = new HashSet<>(rids.subList(firstDelta, rids.size()));
    final Steps questions = new Steps();
    final Set<RID> allowList = new CountingSet(deltaRows, questions, true);
    assertSearchStopsAtTheNextCheck(questions, guard -> index.findNeighborsFromVector(vectors.get(7), 10, 50, allowList, guard),
        () -> () -> {
        });
  }

  @Test
  void theWalkOfTheGraphIsEndedByATerminationThatArrivesInsideIt() {
    // asking for every row makes the walk visit the whole graph, scoring each node it admits. How many records a walk reads
    // between two checks depends on the graph (a node costs the reads of the neighbours it expands), so no allowance of steps is
    // asserted: nothing after the walk checks the guard, so a walk that does not stop is a search that is not ended at all.
    final Steps reads = new Steps();
    assertSearchStopsAtTheNextCheck(reads, guard -> index.findNeighborsFromVector(vectors.get(7), ROWS, ROWS, null, guard),
        () -> registerReadListener(reads), Integer.MAX_VALUE);
  }

  private interface Search {
    Object run(WorkGuard guard);
  }

  private void assertSearchStopsAtTheNextCheck(final Steps steps, final Search search, final Supplier<Runnable> arm) {
    assertSearchStopsAtTheNextCheck(steps, search, arm, STEPS_ALLOWED);
  }

  private void assertSearchStopsAtTheNextCheck(final Steps steps, final Search search, final Supplier<Runnable> arm,
      final int allowed) {
    try (final RunningQuery query = registry.register(database.getName(), "admin", "test", null, "tag-9896", RunningQuery.Client.NONE)) {
      final WorkGuard guard = WorkGuard.forCommandDeadline(null);
      steps.terminateAt(query, TERMINATE_AT);
      final Runnable disarm = arm.get();
      try {
        assertThatThrownBy(() -> search.run(guard)).satisfies(e -> {
          Throwable cause = e;
          while (cause.getCause() != null && !(cause instanceof QueryTerminatedException))
            cause = cause.getCause();
          assertThat(cause).as("the search was ended by the termination").isInstanceOf(QueryTerminatedException.class);
        });
      } finally {
        disarm.run();
      }
      assertThat(steps.total()).as("the loop reached the step the statement is terminated at").isGreaterThan(TERMINATE_AT);
      assertThat(steps.after()).as("steps taken after the termination: one interval of the guard, not the rest of the loop")
          .isLessThanOrEqualTo(allowed);
    }
  }

  private Runnable registerReadListener(final Steps reads) {
    final BeforeRecordReadListener listener = record -> {
      reads.step();
      return true;
    };
    database.getSchema().getType("Doc").getEvents().registerListener(listener);
    return () -> database.getSchema().getType("Doc").getEvents().unregisterListener(listener);
  }

  private void insert(final int count) {
    final Random random = new Random(11L + rids.size());
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian();
        vector[0] += 0.5f;
        rids.add(database.newDocument("Doc").set("embedding", vector).save().getIdentity());
        vectors.add(vector);
      }
    });
  }

  /** Counts the steps of a loop, and terminates the statement at one of them. */
  private static final class Steps {
    private final AtomicInteger total = new AtomicInteger();
    private final AtomicInteger after = new AtomicInteger();
    private       RunningQuery  query;
    private       int           at;

    void terminateAt(final RunningQuery query, final int at) {
      this.query = query;
      this.at = at;
    }

    void step() {
      final int n = total.incrementAndGet();
      if (n == at)
        query.terminate("test");
      else if (n > at)
        after.incrementAndGet();
    }

    int total() {
      return total.get();
    }

    int after() {
      return after.get();
    }
  }

  /** A set that counts the members it hands out (or the membership questions it answers) and delegates the rest. */
  private static final class CountingSet extends AbstractSet<RID> {
    private final Set<RID> members;
    private final Steps    steps;
    private final boolean  countQuestions;

    CountingSet(final Set<RID> members, final Steps steps, final boolean countQuestions) {
      this.members = members;
      this.steps = steps;
      this.countQuestions = countQuestions;
    }

    @Override
    public Iterator<RID> iterator() {
      final Iterator<RID> delegate = members.iterator();
      if (countQuestions)
        return delegate;
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return delegate.hasNext();
        }

        @Override
        public RID next() {
          steps.step();
          return delegate.next();
        }
      };
    }

    @Override
    public boolean contains(final Object o) {
      if (countQuestions)
        steps.step();
      return members.contains(o);
    }

    @Override
    public int size() {
      return members.size();
    }
  }
}
