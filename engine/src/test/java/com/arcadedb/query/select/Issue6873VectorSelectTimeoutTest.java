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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/6873
 * <p>
 * Follow-up to #6815. That fix moved the native {@code Select} deadline to the consumer, covering the four sources
 * {@code SelectExecutor.buildIterator()} can return. The vector k-NN search is a fifth plan shape that never goes
 * through {@code buildIterator()}: {@link SelectExecutor#executeVector()} called neither {@code startTimeout()} nor
 * {@code checkForTimeout()}, so
 * <pre>{@code db.select().fromType("Product").timeout(1, MILLISECONDS, true).nearestTo("embedding", q, 25).vertices()}</pre>
 * compiled and silently dropped the budget.
 * <p>
 * <b>Why these tests drive a clock instead of racing one.</b> {@code executeVector()} is <b>eager</b> - it returns a
 * fully materialized {@code List} - so, unlike the lazy {@code SelectIterator} of the #6815 tests, no consumer can
 * pace it with its own {@code Thread.sleep()}. Measured on this fixture a 50-vector search finishes in ~0.25 ms, a
 * quarter of the smallest expressible budget, so an "a 1 ms budget expired" assertion is a straight wall-clock race
 * that a <i>faster</i> machine turns red - exactly what {@code CLAUDE.md} forbids. The deadline is therefore driven
 * through {@link SelectExecutor#clock}, the seam the production code reads its time from: the elapsed time inside the
 * measured window becomes an input of the test rather than a property of the machine, while the real
 * {@code startTimeout()} arithmetic and the real throw-or-truncate decision still run.
 * <p>
 * The remaining tests use the public fluent API and assert only load-invariant properties (a generous budget must not
 * truncate; no budget must return everything), which a JVM stall can never turn red.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6873VectorSelectTimeoutTest extends TestHelper {

  private static final int  DIMENSIONS = 8;
  private static final int  PRODUCTS   = 50;
  private static final int  K          = 25;
  /** Enough buckets that the {@code Spread} fixture puts rows in more than one bucket index. */
  private static final int  SPREAD_BUCKETS = 4;
  /** Virtual milliseconds. Never compared against a real clock: {@link StepClock} decides when it is exceeded. */
  private static final long BUDGET_MS  = 1_000;

  /**
   * A time source that reports "no time has passed" for the first {@code freeReadings} calls and then jumps past any
   * deadline armed from its first reading. Every decision it drives is a function of the call count alone, so the
   * outcome of a test using it is identical on a fast machine, a slow machine and a machine that stalls mid-run.
   */
  private static final class StepClock implements LongSupplier {
    private final int freeReadings;
    private       int readings;

    private StepClock(final int freeReadings) {
      this.freeReadings = freeReadings;
    }

    @Override
    public long getAsLong() {
      return ++readings <= freeReadings ? 0 : BUDGET_MS + 1;
    }
  }

  /**
   * The head of the issue's repro, with the wall clock replaced by a deadline that has already expired by the time
   * the first bucket index would be searched. Before the fix no {@code TimeoutException} was possible at all here:
   * {@code executeVector()} consulted no deadline whatsoever.
   */
  @Test
  void vectorSearchThrowsWhenTheDeadlineExpiredBeforeTheFirstIndex() {
    createSchemaAndData();

    // ONLY startTimeout() READS THE CLOCK BEFORE EXPIRY, SO THE VERY FIRST PER-INDEX CHECK IS ALREADY PAST THE BUDGET
    assertThatThrownBy(() -> searchWithClock(new StepClock(1), true, false))//
        .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
  }

  /**
   * Same shape with {@code exceptionOnTimeout = false}: the search must give up and hand back what it has - nothing,
   * since it expired before searching a single index - instead of ignoring the budget and returning the full k.
   */
  @Test
  void vectorSearchTruncatesInsteadOfThrowingWhenAskedNotToThrow() {
    createSchemaAndData();

    assertThat(searchWithClock(new StepClock(1), false, false)).isEmpty();
  }

  /**
   * The second checkpoint, in the result-assembly loop: the budget survives the neighbour search of every index and
   * expires after exactly two results have been assembled. Assembly is not free - each iteration loads a record and
   * may run the post-filter WHERE - so a deadline honoured only around the index search would return the full k here.
   */
  @Test
  void vectorSearchStopsAssemblingResultsOnceTheDeadlineExpires() {
    createSchemaAndData();

    final int assembled = 2;
    // EVERY CHECKPOINT BEFORE ASSEMBLY STAYS INSIDE THE BUDGET, THEN ONE READING PER ASSEMBLED RESULT
    final StepClock clock = new StepClock(readingsBeforeAssembly("Product") + assembled);

    final List<SelectVectorResult<Vertex>> results = searchWithClock(clock, false, false);

    assertThat(results).hasSize(assembled);
    assertThat(results).allSatisfy(r -> assertThat(r.getDocument()).isNotNull());
  }

  /**
   * The throwing counterpart of the assembly-loop checkpoint.
   */
  @Test
  void vectorSearchThrowsWhenTheDeadlineExpiresDuringAssembly() {
    createSchemaAndData();

    final StepClock clock = new StepClock(readingsBeforeAssembly("Product") + 2);

    assertThatThrownBy(() -> searchWithClock(clock, true, false))//
        .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
  }

  /**
   * The checkpoint between the last index search and the merge sort. A search that expires inside the
   * <i>last</i> index's own (uninterruptible) call leaves the loop on its bound, never on the per-index checkpoint, so
   * without this third check the sort of up to {@code indexes * k} pairs would still run on an expired budget.
   * <p>
   * An index with nothing in it is what makes the checkpoint observable: with no neighbour to assemble, the loop that
   * follows the sort never runs a single iteration, so the assembly checkpoint can never fire and only this one can
   * report the expiry.
   */
  @Test
  void vectorSearchStopsBeforeSortingWhenTheLastIndexSearchExhaustedTheBudget() {
    createSchemaAndData();

    // EVERY PER-INDEX CHECKPOINT IS STILL INSIDE THE BUDGET; THE PRE-SORT ONE IS THE FIRST READING PAST IT
    final StepClock clock = new StepClock(readingsBeforeAssembly("Empty") - 1);

    assertThatThrownBy(() -> searchWithClock("Empty", clock, true, false))//
        .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
  }

  /**
   * The post-filter WHERE runs inside the assembly loop, so the deadline has to bound that shape too. The filter can
   * only drop results, never add any, so the bound stays an upper one.
   */
  @Test
  void vectorSearchWithAPostFilterAlsoStopsOnTheDeadline() {
    createSchemaAndData();

    final int assembled = 2;
    final StepClock clock = new StepClock(readingsBeforeAssembly("Product") + assembled);

    final List<SelectVectorResult<Vertex>> results = searchWithClock(clock, false, true);

    assertThat(results).hasSizeLessThanOrEqualTo(assembled);
    results.forEach(r -> assertThat(r.getDocument().getString("category")).isEqualTo("electronics"));
  }

  /**
   * The contract for a non-throwing expiry detected <i>before</i> the assembly loop: an empty list, not a partial one.
   * <p>
   * {@code allNeighbors} is not an answer - it holds RIDs and distances, and promoting any of them into a
   * {@code SelectVectorResult} means loading a record and possibly running the post-filter WHERE, i.e. doing more work
   * past a deadline that has already gone. So the rule is the one {@code executeCount()} and {@code SelectIterator}
   * follow: hand back what was already produced, never produce more. This test pins that down with a fixture whose
   * rows really are spread over several bucket indexes, so the budget dies with real candidates already accumulated.
   */
  @Test
  void aNonThrowingExpiryBetweenIndexesReturnsEmptyRatherThanUnassembledNeighbours() {
    createSchemaAndData();

    // THE FIXTURE HAS TO ACTUALLY EXERCISE THE CASE: MORE THAN ONE BUCKET INDEX, AND NEIGHBOURS TO BE FOUND IN THEM
    assertThat(countVectorBucketIndexes("Spread")).isGreaterThan(1);
    assertThat(search("Spread", null)).isNotEmpty();

    // startTimeout() PLUS THE FIRST INDEX'S CHECKPOINT ARE FREE, SO INDEX 1 IS SEARCHED IN FULL AND CONTRIBUTES
    // CANDIDATES; THE SECOND INDEX'S CHECKPOINT IS THE FIRST READING PAST THE BUDGET
    final List<SelectVectorResult<Vertex>> results = searchWithClock("Spread", new StepClock(2), false, false);

    assertThat(results).isEmpty();
  }

  /**
   * The per-index checkpoint gates the approximate search exactly as it gates the exact one - the two differ only in
   * which {@code LSMVectorIndex} call the loop body makes, and the deadline sits in front of both.
   */
  @Test
  void approximateVectorSearchIsBoundedByTheSameDeadline() {
    createSchemaAndData();

    assertThatThrownBy(() -> searchWithClock("Product", new StepClock(1), true, false, true))//
        .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");

    assertThat(searchWithClock("Product", new StepClock(1), false, false, true)).isEmpty();

    // AND, LOAD-INVARIANT, A BUDGET NOBODY CAN EXHAUST LEAVES THE APPROXIMATE SEARCH ALONE
    final List<SelectVectorResult<Vertex>> unbounded = database.select().fromType("Product")//
        .nearestTo("embedding", queryVector(), K).approximate(true).vertices();
    assertThat(database.select().fromType("Product").timeout(1, TimeUnit.HOURS, true)//
        .nearestTo("embedding", queryVector(), K).approximate(true).vertices()).hasSize(unbounded.size());
  }

  /**
   * Load-invariant: a budget nobody can exhaust must return exactly what an unbounded search returns. This is the
   * assertion that would catch a deadline armed wrongly (for instance one that expires immediately), and a JVM stall
   * can only make the real clock read later - which this test never looks at.
   */
  @Test
  void aGenerousTimeoutDoesNotTruncateTheResultSet() {
    createSchemaAndData();

    final int unbounded = search(null).size();
    assertThat(unbounded).isPositive();

    assertThat(search(select -> select.timeout(1, TimeUnit.HOURS, true))).hasSize(unbounded);
    assertThat(search(select -> select.timeout(1, TimeUnit.HOURS, false))).hasSize(unbounded);
  }

  /**
   * {@code TimeUnit.toMillis()} saturates rather than rejecting, so an effectively infinite budget reaches the
   * executor as {@code Long.MAX_VALUE}; a plain {@code now + timeoutInMs} would overflow into a deadline already in
   * the past and truncate the very first result. Deterministic: no clock reading decides the outcome.
   */
  @Test
  void anEffectivelyInfiniteTimeoutNeverExpiresOnTheVectorPath() {
    createSchemaAndData();

    final int unbounded = search(null).size();

    assertThat(search(select -> select.timeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS, true))).hasSize(unbounded);
    assertThat(search(select -> select.timeout(Long.MAX_VALUE, TimeUnit.DAYS, true))).hasSize(unbounded);
  }

  /**
   * The deadline machinery must stay completely inert when {@code timeout()} was never called - the sentinel path
   * that keeps the check a single predicted compare and never reads a clock at all.
   */
  @Test
  void noTimeoutStillReturnsEveryNeighbour() {
    createSchemaAndData();

    final List<SelectVectorResult<Vertex>> results = search(null);
    assertThat(results).isNotEmpty().hasSizeLessThanOrEqualTo(K);
    for (int i = 1; i < results.size(); i++)
      assertThat(results.get(i).getDistance()).isGreaterThanOrEqualTo(results.get(i - 1).getDistance());
  }

  /**
   * Runs the k-NN search through an executor whose time source is {@code clock}, mirroring exactly what
   * {@code SelectVectorBuilder.vertices()} does around {@link SelectExecutor#executeVector()}.
   */
  private List<SelectVectorResult<Vertex>> searchWithClock(final StepClock clock, final boolean exceptionOnTimeout,
      final boolean withPostFilter) {
    return searchWithClock("Product", clock, exceptionOnTimeout, withPostFilter);
  }

  private List<SelectVectorResult<Vertex>> searchWithClock(final String typeName, final StepClock clock,
      final boolean exceptionOnTimeout, final boolean withPostFilter) {
    return searchWithClock(typeName, clock, exceptionOnTimeout, withPostFilter, false);
  }

  private List<SelectVectorResult<Vertex>> searchWithClock(final String typeName, final StepClock clock,
      final boolean exceptionOnTimeout, final boolean withPostFilter, final boolean approximate) {
    final Select select = database.select().fromType(typeName).timeout(BUDGET_MS, TimeUnit.MILLISECONDS, exceptionOnTimeout);
    final SelectVectorBuilder builder = select.nearestTo("embedding", queryVector(), K).approximate(approximate);
    if (withPostFilter)
      builder.where().property("category").eq().value("electronics");
    select.compile();

    final SelectExecutor executor = new SelectExecutor(select);
    executor.clock = clock;
    return executor.executeVector();
  }

  /**
   * Runs the same search through the public fluent API, optionally applying a timeout to the select first.
   */
  private List<SelectVectorResult<Vertex>> search(final UnaryOperator<Select> withTimeout) {
    return search("Product", withTimeout);
  }

  private List<SelectVectorResult<Vertex>> search(final String typeName, final UnaryOperator<Select> withTimeout) {
    final Select select = database.select().fromType(typeName);
    return (withTimeout == null ? select : withTimeout.apply(select))//
        .nearestTo("embedding", queryVector(), K).vertices();
  }

  /**
   * Mirrors the index selection {@link SelectExecutor#executeVector()} performs, so the expected number of per-index
   * deadline checks is derived from the schema instead of assumed.
   */
  private int countVectorBucketIndexes(final String typeName) {
    final TypeIndex typeIndex = database.getSchema().getType(typeName).getPolymorphicIndexByProperties("embedding");
    int count = 0;
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof LSMVectorIndex)
        count++;
    return count;
  }

  /**
   * How many times {@code executeVector()} reads the clock before it starts assembling results: once to arm the
   * deadline, once per bucket index, and once between the last index search and the merge sort. Derived from the
   * schema rather than assumed, so it stays exact whatever the machine's default bucket count is.
   */
  private int readingsBeforeAssembly(final String typeName) {
    return 1 + countVectorBucketIndexes(typeName) + 1;
  }

  private static float[] queryVector() {
    final float[] query = new float[DIMENSIONS];
    Arrays.fill(query, 0.5f);
    return query;
  }

  private void createSchemaAndData() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Product IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Product.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.category IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.embedding IF NOT EXISTS ARRAY_OF_FLOATS");

      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Product (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 8,
            "similarity" : "EUCLIDEAN",
            "maxConnections" : 16,
            "beamWidth" : 100
          }""");

      // SAME SHAPE, SPREAD OVER SEVERAL BUCKETS SO MORE THAN ONE BUCKET INDEX HOLDS ROWS: THAT IS WHAT MAKES AN
      // EXPIRY *BETWEEN* INDEXES OBSERVABLE, WITH REAL CANDIDATES ALREADY ACCUMULATED WHEN IT HAPPENS
      database.command("sql", "CREATE VERTEX TYPE Spread IF NOT EXISTS BUCKETS " + SPREAD_BUCKETS);
      database.command("sql", "CREATE PROPERTY Spread.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Spread (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 8,
            "similarity" : "EUCLIDEAN",
            "maxConnections" : 16,
            "beamWidth" : 100
          }""");

      // SAME SHAPE, NO ROWS: A VECTOR SEARCH OVER IT RETURNS NO NEIGHBOUR AT ALL, WHICH IS WHAT MAKES THE PRE-SORT
      // CHECKPOINT DISTINGUISHABLE FROM THE ASSEMBLY ONE
      database.command("sql", "CREATE VERTEX TYPE Empty IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Empty.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Empty (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 8,
            "similarity" : "EUCLIDEAN",
            "maxConnections" : 16,
            "beamWidth" : 100
          }""");

      final Random rng = new Random(42);
      for (int i = 0; i < PRODUCTS; i++) {
        final float[] spread = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          spread[j] = rng.nextFloat();
        database.newVertex("Spread").set("embedding", spread).save();
      }
      for (int i = 0; i < PRODUCTS; i++) {
        final float[] vec = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vec[j] = rng.nextFloat();
        final String category = i < PRODUCTS / 2 ? "electronics" : "clothing";
        database.newVertex("Product").set("name", "Product" + i, "category", category, "embedding", vec).save();
      }
    });

    closeAndReopenDatabase();
  }

  private void closeAndReopenDatabase() {
    final String dbPath = database.getDatabasePath();
    database.close();
    database = new DatabaseFactory(dbPath).open();
  }
}
