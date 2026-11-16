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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.MultiIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SelectTimeoutTest {

  /**
   * Regression for #6815: timeout() was a no-op for index cursors and single-bucket scans.
   * https://github.com/ArcadeData/arcadedb/issues/6815
   * <p>
   * {@code SelectExecutor.buildIterator()} installed the caller's {@code timeout()} on the source iterator only when
   * that source happened to be a {@link MultiIterator}. Two of the four sources it can return are
   * not: an index-answered plan yields a {@code MultiIndexCursor}, and a single-bucket {@code fromBuckets(...)} yields
   * the bucket iterator itself. Neither carries a deadline, so {@code timeout()} was a silent no-op for every indexed
   * query - by far the common shape - and for a one-bucket scan, on {@code documents()}/{@code vertices()}/
   * {@code edges()}, {@code count()} and {@code exists()} alike.
   * <p>
   * The deadline is now owned by the consumer ({@link SelectExecutor#checkForTimeout()}), which every source shape goes
   * through, so it no longer depends on which iterator the planner picked.
   * <p>
   * The assertions here are one-sided on purpose: each states that a bounded operation gave up, never that it survived
   * for at least some duration. A JVM stall can only make a deadline expire sooner, so it cannot turn a passing run red.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeoutNotInstalled extends TestHelper {

    // BIG ENOUGH THAT A FULL SCAN CANNOT FINISH INSIDE THE 1 ms BUDGET count()/exists() ARE GIVEN BELOW. A SLOWER
    // MACHINE ONLY MAKES THAT MORE TRUE, WHICH IS THE DIRECTION THAT KEEPS THESE ASSERTIONS STABLE
    private static final int SCAN_ROWS = 20_000;
    private static final int ROWS      = 100;

    public TimeoutNotInstalled() {
      autoStartTx = false;
    }

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("V")//
          .createProperty("id", Type.INTEGER)//
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);

      database.getSchema().createDocumentType("Scan")//
          .createProperty("id", Type.INTEGER)//
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);

      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex("V").set("id", i, "name", "John").save();
        for (int i = 0; i < SCAN_ROWS; i++)
          database.newDocument("Scan").set("id", i, "name", "John").save();
      });
    }

    /**
     * The issue's own repro: an indexed WHERE leaf makes the plan index-answered, so the source is a
     * {@code MultiIndexCursor} and the timeout used to be dropped on the floor.
     */
    @Test
    void indexAnsweredIterationThrowsOnTimeout() {
      assertThatThrownBy(() -> {
        final SelectIterator<Vertex> iter = database.select().fromType("V")//
            .where().property("id").ge().value(0)//
            .timeout(1, TimeUnit.MILLISECONDS, true).vertices();
        drainSlowly(iter);
      }).isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
    }

    /**
     * Same plan shape with {@code exceptionOnTimeout = false}: the iteration must end early rather than throw, and the
     * result set must therefore be a strict prefix of the full one.
     */
    @Test
    void indexAnsweredIterationTruncatesWhenNotThrowing() {
      final SelectIterator<Vertex> iter = database.select().fromType("V")//
          .where().property("id").ge().value(0)//
          .timeout(1, TimeUnit.MILLISECONDS, false).vertices();

      final List<Vertex> got = drainSlowly(iter);
      assertThat(got.size()).isLessThan(ROWS);
    }

    /**
     * The second uninstrumented source: {@code fromBuckets(oneBucket)} returns {@code database.iterateBucket(...)},
     * a plain bucket iterator with no deadline of its own.
     */
    @Test
    void singleBucketIterationThrowsOnTimeout() {
      final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

      assertThatThrownBy(() -> {
        final SelectIterator<Vertex> iter = database.select().fromBuckets(bucket)//
            .where().property("name").eq().value("John")//
            .timeout(1, TimeUnit.MILLISECONDS, true).vertices();
        drainSlowly(iter);
      }).isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
    }

    @Test
    void singleBucketIterationTruncatesWhenNotThrowing() {
      final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

      final SelectIterator<Vertex> iter = database.select().fromBuckets(bucket)//
          .where().property("name").eq().value("John")//
          .timeout(1, TimeUnit.MILLISECONDS, false).vertices();

      final List<Vertex> got = drainSlowly(iter);
      assertThat(got.size()).isLessThan(ROWS);
    }

    /**
     * {@code count()} never builds a {@link SelectIterator}: it drains {@code buildIterator()} in its own loop, so it
     * needs the deadline enforced there too.
     */
    @Test
    void countThrowsOnTimeout() {
      assertThatThrownBy(() -> database.select().fromType("Scan")//
          .where().property("id").ge().value(0)//
          .timeout(1, TimeUnit.MILLISECONDS, true).count())//
          .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
    }

    /**
     * The non-throwing counterpart returns the partial tally instead of the full one.
     */
    @Test
    void countTruncatesWhenNotThrowing() {
      final long count = database.select().fromType("Scan")//
          .where().property("id").ge().value(0)//
          .timeout(1, TimeUnit.MILLISECONDS, false).count();

      assertThat(count).isLessThan(SCAN_ROWS);
    }

    /**
     * {@code exists()} has the same shape as {@code count()}. A WHERE that matches nothing forces the full scan the
     * deadline has to cut short - a matching WHERE would return on the first record, long before any budget expires.
     */
    @Test
    void existsThrowsOnTimeout() {
      assertThatThrownBy(() -> database.select().fromType("Scan")//
          .where().property("name").eq().value("NoSuchName")//
          .timeout(1, TimeUnit.MILLISECONDS, true).exists())//
          .isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
    }

    @Test
    void existsReturnsFalseWhenNotThrowing() {
      assertThat(database.select().fromType("Scan")//
          .where().property("name").eq().value("NoSuchName")//
          .timeout(1, TimeUnit.MILLISECONDS, false).exists()).isFalse();
    }

    /**
     * A select with no timeout must keep streaming the whole type: the deadline machinery has to stay inert when
     * {@code timeout()} was never called.
     */
    @Test
    void noTimeoutStillReturnsEverything() {
      assertThat(database.select().fromType("V")//
          .where().property("id").ge().value(0).vertices().toList()).hasSize(ROWS);
      assertThat(database.select().fromType("Scan")//
          .where().property("id").ge().value(0).count()).isEqualTo(SCAN_ROWS);
    }

    /**
     * A deadline so far away it can never expire must behave like no deadline at all. Computing it as
     * {@code now + timeoutInMs} overflows into a negative value once {@code timeoutInMs} approaches {@code Long.MAX_VALUE}
     * - which {@link TimeUnit#toMillis} saturates to rather than rejecting - turning an effectively infinite budget into
     * one that has already expired, so the very first record throws. Deterministic: no clock reading decides the outcome.
     */
    @Test
    void anEffectivelyInfiniteTimeoutNeverExpires() {
      assertThat(database.select().fromType("V")//
          .where().property("id").ge().value(0)//
          .timeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS, true).vertices().toList()).hasSize(ROWS);

      assertThat(database.select().fromType("Scan")//
          .where().property("id").ge().value(0)//
          .timeout(Long.MAX_VALUE, TimeUnit.DAYS, true).count()).isEqualTo(SCAN_ROWS);

      assertThat(database.select().fromType("Scan")//
          .where().property("name").eq().value("John")//
          .timeout(Long.MAX_VALUE, TimeUnit.DAYS, true).exists()).isTrue();
    }

    /**
     * The single-bucket source shape has to survive the same saturation.
     */
    @Test
    void anEffectivelyInfiniteTimeoutNeverExpiresOnASingleBucket() {
      final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

      assertThat(database.select().fromBuckets(bucket)//
          .where().property("name").eq().value("John")//
          .timeout(Long.MAX_VALUE, TimeUnit.DAYS, true).vertices().toList()).hasSize(ROWS);
    }

    private static List<Vertex> drainSlowly(final SelectIterator<Vertex> iter) {
      final List<Vertex> got = new ArrayList<>();
      while (iter.hasNext()) {
        got.add(iter.next());
        try {
          Thread.sleep(2);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      return got;
    }
  }

  /**
   * Regression for #6816: MultiIterator ignored exceptionOnTimeout=false.
   * https://github.com/ArcadeData/arcadedb/issues/6816
   * <p>
   * {@link MultiIterator} stored {@code exceptionOnTimeout} but its hot-path check in {@code hasNextInternal()} ignored
   * it and threw unconditionally. {@code checkForTimeout()} - the method that does honour the flag - was only reachable
   * from {@code getNextPartial()} and {@code countEntries()}, both of which run only *after* the throwing check has
   * already passed, i.e. only while the deadline has NOT expired. So its non-throwing branch was dead code, and the one
   * producer of {@code exceptionOnTimeout == false} in the engine - {@code Select.timeout(v, unit, false)}, meaning
   * "stop early and give me what you have" - got a {@link TimeoutException} instead.
   * <p>
   * The direct {@link MultiIterator} tests below are deterministic: the deadline is set on an iterator whose
   * {@code beginTime} is already in the past, so no assertion depends on how fast the scan runs.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MultiIteratorTimeoutTruncation extends TestHelper {

    private static final int ROWS = 100;

    public MultiIteratorTimeoutTruncation() {
      autoStartTx = false;
    }

    @Override
    protected void beginTest() {
      // NO INDEX ON name: THE WHERE LEAF BELOW MUST NOT BE INDEX-ANSWERED, SO THE SOURCE REALLY IS A MultiIterator
      database.getSchema().createVertexType("V").createProperty("name", Type.STRING);

      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex("V").set("id", i, "name", "John").save();
      });
    }

    /**
     * The unit-level statement of the bug: with the flag off, an expired deadline must end the iteration, not throw.
     */
    @Test
    void expiredDeadlineWithoutExceptionEndsIteration() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());
      assertThat(it.hasNext()).isTrue();
      assertThat(it.next()).isEqualTo(1);

      expireDeadline(it, false);

      assertThatNoException().isThrownBy(it::hasNext);
      assertThat(it.hasNext()).isFalse();
    }

    /**
     * The flag on keeps the pre-existing behaviour every other caller relies on ({@code LocalDatabase.iterateType()}
     * passes {@code true}).
     */
    @Test
    void expiredDeadlineWithExceptionStillThrows() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());
      assertThat(it.hasNext()).isTrue();

      expireDeadline(it, true);

      assertThatThrownBy(it::hasNext).isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
    }

    /**
     * A deadline that has not expired must leave the iteration completely alone.
     */
    @Test
    void unexpiredDeadlineIteratesEverything() {
      final MultiIterator<Integer> it = new MultiIterator<Integer>().addIterator(List.of(1, 2, 3).iterator());
      it.setTimeout(TimeUnit.MINUTES.toMillis(10), false);

      final List<Integer> got = new ArrayList<>();
      while (it.hasNext())
        got.add(it.next());

      assertThat(got).containsExactly(1, 2, 3);
    }

    /**
     * The end-to-end shape from the issue: an unindexed WHERE over a type, so {@code buildIterator()} really does hand
     * back a {@link MultiIterator}, asked not to throw. Only the upper bound is asserted - a JVM stall can only cut the
     * result set shorter, never longer, so this cannot flake in the other direction.
     */
    @Test
    void selectTruncatesInsteadOfThrowing() {
      final SelectIterator<Vertex> iter = database.select().fromType("V")//
          .where().property("name").eq().value("John")//
          .timeout(1, TimeUnit.MILLISECONDS, false).vertices();

      final List<Vertex> got = new ArrayList<>();
      assertThatNoException().isThrownBy(() -> {
        while (iter.hasNext()) {
          got.add(iter.next());
          Thread.sleep(2);
        }
      });

      assertThat(got.size()).isLessThan(ROWS);
    }

    /**
     * ...and the throwing counterpart of the same plan shape is unchanged.
     */
    @Test
    void selectStillThrowsWhenAskedTo() {
      assertThatThrownBy(() -> {
        final SelectIterator<Vertex> iter = database.select().fromType("V")//
            .where().property("name").eq().value("John")//
            .timeout(1, TimeUnit.MILLISECONDS, true).vertices();
        while (iter.hasNext()) {
          iter.next();
          Thread.sleep(2);
        }
      }).isInstanceOf(TimeoutException.class).hasMessageContaining("Timeout on iteration");
    }

    /**
     * Puts the iterator's deadline in the past without sleeping: {@code beginTime} is captured at construction, so a
     * zero-millisecond budget is already spent by the time the constructor has returned and the first record consumed.
     */
    private static void expireDeadline(final MultiIterator<?> it, final boolean exceptionOnTimeout) {
      it.setTimeout(0, exceptionOnTimeout);
      // GUARANTEE STRICTLY MORE THAN 0 ms HAVE ELAPSED SINCE beginTime, SINCE THE CHECK IS `elapsed > timeout`
      final long begin = System.currentTimeMillis();
      while (System.currentTimeMillis() == begin)
        Thread.onSpinWait();
    }
  }

  /**
   * Regression for #6873: the vector k-NN search dropped the timeout budget.
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
  @Nested
  class VectorTimeout extends TestHelper {

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
}
