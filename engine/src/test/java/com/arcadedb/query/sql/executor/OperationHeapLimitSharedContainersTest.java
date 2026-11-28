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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.arcadedb.query.sql.executor.HeapEstimator.HASH_ENTRY_BYTES;
import static com.arcadedb.query.sql.executor.HeapEstimator.OBJECT_BYTES;
import static com.arcadedb.query.sql.executor.HeapEstimator.REFERENCE_BYTES;
import static com.arcadedb.query.sql.executor.HeapEstimator.RESULT_BYTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11812: the large containers the elements of one operation share are charged once, and what is charged stays what
 * the elements hold: a container nested in another one met for the first time, a top-K heap replacing rows that hold
 * containers of their own, and rows whose containers are their own past the sampled ones.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class OperationHeapLimitSharedContainersTest {

  @BeforeEach
  void setUp() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1024L);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void rowsSharingAContainerThatHoldsAnotherOneAreChargedForBothOnce() {
    final List<Object> child = nulls(2_000);
    final List<Object> parent = new ArrayList<>(Collections.nCopies(32, child));
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final int rows = 2_000;
    for (int i = 1; i <= rows; i++)
      limit.add(i, row("p", parent, "i", i));

    // The first row is charged the parent and the list in it, met once there; every row is charged its own entries and the
    // reference to the parent, the rows not estimated included
    final long parentWithItsList = new HeapEstimator.Shared().add(parent);
    final long rowOnItsOwn = RESULT_BYTES + 2L * HASH_ENTRY_BYTES + REFERENCE_BYTES + OBJECT_BYTES + REFERENCE_BYTES;
    assertThat(limit.getChargedBytes()).isEqualTo(parentWithItsList - REFERENCE_BYTES + rows * rowOnItsOwn);
    limit.release();
  }

  @Test
  void aReleasedOperationForgetsTheContainersItHeld() {
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final List<Object> shared = nulls(2_000);
    limit.add(1, row("l", shared, "i", 1));
    final long withTheList = limit.getChargedBytes();

    limit.release();
    assertThat(limit.getChargedBytes()).isZero();

    // What the operation holds from here on is a new buffer: the list is charged in full again, not as a reference to a list
    // the first buffer held
    limit.add(1, row("l", shared, "i", 2));
    assertThat(limit.getChargedBytes()).isEqualTo(withTheList);
    limit.release();
  }

  @Test
  void aTopKHeapReplacingADistinctContainerKeepsItsChargeAndForgetsTheOneThatLeft() {
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY").estimateEachElement();
    List<Object> held = nulls(2_000);
    limit.add(1, held);
    final long oneList = limit.getChargedBytes();
    assertThat(limit.sharedContainers()).isEqualTo(1);

    for (int i = 0; i < 100; i++) {
      final List<Object> next = nulls(2_000);
      limit.replace(held, next);
      held = next;
    }
    assertThat(limit.getChargedBytes()).isEqualTo(oneList);

    // The lists that left the heap are not kept reachable by the operation: it remembers the one the heap holds now
    assertThat(limit.sharedContainers()).isEqualTo(1);

    // and nothing at all once it is released, as an owner does when its buffer goes
    limit.release();
    assertThat(limit.sharedContainers()).isZero();
  }

  @Test
  void aTopKHeapGivesASharedContainerBackWithTheLastRowThatHeldIt() {
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY").estimateEachElement();
    final List<Object> shared = nulls(2_000);
    final ResultInternal a = row("l", shared, "i", 1);
    final ResultInternal b = row("l", shared, "i", 2);
    limit.add(1, a);
    limit.add(2, b);
    final long twoRows = limit.getChargedBytes();

    // A row holding the same list takes a row's place: the list stays charged once
    final ResultInternal c = row("l", shared, "i", 3);
    limit.replace(a, c);
    assertThat(limit.getChargedBytes()).isEqualTo(twoRows);

    // The rows that held it leave for rows of their own: the list stays charged while one holds it, and goes with the last one
    final ResultInternal d = row("l", "x", "i", 4);
    final ResultInternal e = row("l", "y", "i", 5);
    limit.replace(b, d);
    assertThat(limit.getChargedBytes()).isEqualTo(HeapEstimator.estimate(c) + HeapEstimator.estimate(d) + 2L * REFERENCE_BYTES);
    limit.replace(c, e);
    assertThat(limit.getChargedBytes()).isEqualTo(HeapEstimator.estimate(d) + HeapEstimator.estimate(e) + 2L * REFERENCE_BYTES);
    limit.release();
  }

  @Test
  void rowsWithContainersOfTheirOwnAreChargedForThemPastTheSampledOnes() {
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final int rows = 200;
    long expected = 0L;
    for (int i = 1; i <= rows; i++) {
      final ResultInternal row = row("l", nulls(20_000), "i", i);
      expected += HeapEstimator.estimate(row) + REFERENCE_BYTES;
      limit.add(i, row);
    }
    assertThat(limit.getChargedBytes()).isEqualTo(expected);
    limit.release();
  }

  /**
   * Each row holds a large container of its own, and those containers all hold one large child: the child is charged once,
   * not once more for every sample. Met first inside another container, it is part of the first sampled row's estimate until
   * a later sample shares it, and must leave the average the rows not estimated are charged then, as a child held directly
   * does.
   */
  @Test
  void rowsWithContainersOfTheirOwnSharingALargeChildAreChargedForTheChildOnce() {
    final List<Object> child = nulls(20_000);
    final int rows = 2_000;
    final OperationHeapLimit sampled = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final OperationHeapLimit exact = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY").estimateEachElement();
    for (int i = 1; i <= rows; i++) {
      final List<Object> own = nulls(HeapEstimator.SAMPLED_ELEMENTS + 1);
      own.set(0, child);
      final ResultInternal row = row("l", own, "i", i);
      sampled.add(i, row);
      exact.add(i, row);
    }

    // The first row's own list holds the child among the elements an estimate looks at, and extrapolates it to the one it does not
    // (17 elements, 16 looked at): about one more child, which the average spreads over the rows not estimated. The estimate errs
    // high, never low, and by less than a second child: a child charged again by every row, or by every sampled one, is a multiple
    // of its size, and one charged once more is more than the extrapolation of the first
    final long childBytes = HeapEstimator.estimate(child);
    final long exactBytes = exact.getChargedBytes();
    assertThat(sampled.getChargedBytes()).as("the child is charged once, as when every row is estimated")
        .isBetween(exactBytes - rows, exactBytes + childBytes + childBytes / 2);
    sampled.release();
    exact.release();
  }

  /**
   * The first row meets a large child by one path and then again inside a container another path leads to, {@code [[child], parent]},
   * and the rows after it hold that container only: the child is shared with them through it, and leaves what the first row holds
   * on its own together with the container. Left in, its bytes are spread over every row not estimated, a multiple of its size.
   */
  @Test
  void aChildMetBeforeItsContainerInTheSameRowLeavesTheSampledAverageWithThatContainer() {
    final List<Object> child = nulls(100_000);
    final List<Object> parent = nulls(HeapEstimator.SAMPLED_ELEMENTS + 1);
    parent.set(0, child);
    final int rows = 1_000;
    final OperationHeapLimit sampled = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final OperationHeapLimit exact = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY").estimateEachElement();
    for (int i = 1; i <= rows; i++) {
      final ResultInternal row = row("l", i == 1 ? List.of(List.of(child), parent) : List.of(List.of(), parent), "i", i);
      sampled.add(i, row);
      exact.add(i, row);
    }

    final long childBytes = HeapEstimator.estimate(child);
    final long exactBytes = exact.getChargedBytes();
    assertThat(sampled.getChargedBytes()).as("the child is charged once, as when every row is estimated")
        .isBetween(exactBytes - childBytes / 4, exactBytes + childBytes / 4);
    sampled.release();
    exact.release();
  }

  /** A vector every row of a query carries (an UNWIND over a parameter) is in memory once, whatever its element type. */
  @Test
  void rowsSharingALargePrimitiveArrayAreChargedForItOnce() {
    final float[] vector = new float[100_000];
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final int rows = 200;
    for (int i = 1; i <= rows; i++)
      limit.add(i, row("v", vector, "i", i));

    // The array once, charged to the first row, and every row's own entries and its reference to it
    final long rowOnItsOwn = RESULT_BYTES + 2L * HASH_ENTRY_BYTES + REFERENCE_BYTES + OBJECT_BYTES + REFERENCE_BYTES;
    assertThat(limit.getChargedBytes()).isEqualTo(HeapEstimator.estimate(vector) - REFERENCE_BYTES + rows * rowOnItsOwn);
    assertThat(limit.sharedContainers()).isEqualTo(1);
    limit.release();
  }

  @Test
  void aSmallPrimitiveArrayIsNotRememberedAndIsChargedByEveryRowThatHoldsIt() {
    final float[] small = new float[HeapEstimator.SHARED_ARRAY_BYTES / 8];
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    limit.add(1, row("v", small, "i", 1));
    limit.add(2, row("v", small, "i", 2));

    assertThat(limit.sharedContainers()).isZero();
    assertThat(limit.getChargedBytes()).isEqualTo(2L * (HeapEstimator.estimate(row("v", small, "i", 1)) + REFERENCE_BYTES));
    limit.release();
  }

  /**
   * What a walk counts of a container depends on how deep it starts: the first elements of what it holds down to the fourth level,
   * one a level below. A list met three levels down by one row is charged for what that walk finds, which must not stand for the
   * same list held directly by another row: each sighting is charged for the arrays it holds, and gives back what it was charged.
   */
  @Test
  void aContainerMetDeepIsNotWhatARowHoldingItDirectlyIsChargedAsAReferenceTo() {
    final List<Object> vectors = new ArrayList<>();
    for (int i = 0; i < 20; i++)
      vectors.add(new float[1_024]);
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");

    final ResultInternal deepRow = row("l", List.of(List.of(vectors)), "i", 1);
    final ResultInternal directRow = row("l", vectors, "i", 2);

    limit.add(1, deepRow);
    final long deep = limit.getChargedBytes();
    assertThat(deep).as("the arrays three levels down are charged for their size").isGreaterThanOrEqualTo(HeapEstimator.estimate(vectors));

    limit.add(2, directRow);
    assertThat(limit.getChargedBytes() - deep).as("held directly, the list is charged for what it holds")
        .isGreaterThanOrEqualTo(HeapEstimator.estimate(vectors));

    // and each of the two sightings gives back what it was charged: the rows leave for empty ones
    final ResultInternal empty = new ResultInternal();
    limit.replace(deepRow, empty);
    limit.replace(directRow, empty);
    assertThat(limit.sharedContainers()).isZero();
    assertThat(limit.getChargedBytes()).isEqualTo(2L * (HeapEstimator.estimate(empty) + REFERENCE_BYTES));
    limit.release();
  }

  /**
   * A buffer emptied with {@code release(long)} and filled again - the rows a join buffers for each of its outer rows - is a new
   * buffer: what was learnt of the old rows (an average that leaves out the lists they shared, while the registry that charged the
   * first of them is gone) must not decide what the first rows of the new one are charged.
   */
  @Test
  void anOperationFilledAgainAfterAReleaseEstimatesItsFirstRowsAgain() {
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY");
    final List<Object> first = nulls(2_000);
    for (int i = 1; i <= 200; i++)
      limit.add(i, row("l", first, "i", i));
    limit.release();
    assertThat(limit.getChargedBytes()).isZero();

    final List<Object> second = nulls(20_000);
    for (int i = 1; i <= 3; i++)
      limit.add(i, row("l", second, "i", i));

    assertThat(limit.getChargedBytes()).as("the list the new rows share is charged once").isGreaterThanOrEqualTo(HeapEstimator.estimate(second));
    limit.release();
  }

  /**
   * A top-K heap whose rows share large containers at random - lists that hold lists, maps and arrays that hold them, primitive
   * arrays, the same one under two names of a row, at different depths of a row, a private one that holds a shared one - and replaces
   * rows at random, gives back exactly what it charged: when every row has gone for an empty one, what is charged is the empty
   * rows and no container is remembered. A container given back too early, too late or twice leaves the charge above or below that.
   */
  @Test
  void aTopKHeapGivesBackExactlyWhatItChargedWhateverTheRowsShare() {
    final Random random = new Random(11812L);
    final List<Object> pool = sharedContainers(random);
    final OperationHeapLimit limit = OperationHeapLimit.of(new BasicCommandContext(), "ORDER BY").estimateEachElement();
    final int heapSize = 24;
    final ResultInternal[] held = new ResultInternal[heapSize];
    for (int i = 0; i < heapSize; i++) {
      held[i] = randomRow(random, pool);
      limit.add(i + 1, held[i]);
    }
    for (int step = 0; step < 4_000; step++) {
      final int slot = random.nextInt(heapSize);
      final ResultInternal next = randomRow(random, pool);
      limit.replace(held[slot], next);
      held[slot] = next;
    }
    assertThat(limit.sharedContainers()).as("the containers the heap holds are remembered").isPositive();

    final ResultInternal empty = new ResultInternal();
    for (int slot = 0; slot < heapSize; slot++)
      limit.replace(held[slot], empty);

    assertThat(limit.getChargedBytes()).isEqualTo(heapSize * (HeapEstimator.estimate(empty) + REFERENCE_BYTES));
    assertThat(limit.sharedContainers()).isZero();
    limit.release();
  }

  /** Large containers, the later ones holding earlier ones among their first entries, the ones an estimate looks at. */
  private static List<Object> sharedContainers(final Random random) {
    final List<Object> pool = new ArrayList<>();
    final List<Object> leaves = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      leaves.add(nulls(HeapEstimator.SAMPLED_ELEMENTS + 4 + random.nextInt(10)));
    pool.addAll(leaves);

    final List<Object> middles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      final List<Object> middle = nulls(HeapEstimator.SAMPLED_ELEMENTS + 4);
      holdSome(random, middle, leaves);
      middles.add(middle);
    }
    pool.addAll(middles);

    final List<Object> lower = new ArrayList<>(leaves);
    lower.addAll(middles);
    // Primitive arrays from the size a Shared remembers them at
    final Object[] primitives = { new float[300], new int[400], new long[200], new double[150], new byte[2_000] };
    lower.addAll(List.of(primitives));
    pool.addAll(List.of(primitives));
    for (int i = 0; i < 4; i++) {
      final List<Object> top = nulls(HeapEstimator.SAMPLED_ELEMENTS + 4);
      holdSome(random, top, lower);
      pool.add(top);
    }
    for (int i = 0; i < 3; i++) {
      final Map<String, Object> map = new LinkedHashMap<>();
      for (int k = 0; k < HeapEstimator.SAMPLED_ELEMENTS + 4; k++)
        map.put("k" + k, null);
      map.put("k0", lower.get(random.nextInt(lower.size())));
      map.put("k1", lower.get(random.nextInt(lower.size())));
      pool.add(map);
    }
    for (int i = 0; i < 3; i++) {
      final Object[] array = new Object[HeapEstimator.SAMPLED_ELEMENTS + 4];
      array[0] = lower.get(random.nextInt(lower.size()));
      array[1] = lower.get(random.nextInt(lower.size()));
      pool.add(array);
    }
    return pool;
  }

  private static void holdSome(final Random random, final List<Object> container, final List<Object> candidates) {
    final int held = 1 + random.nextInt(3);
    for (int i = 0; i < held; i++)
      container.set(i, candidates.get(random.nextInt(candidates.size())));
  }

  private static ResultInternal randomRow(final Random random, final List<Object> pool) {
    final ResultInternal row = new ResultInternal();
    final int properties = 1 + random.nextInt(3);
    for (int p = 0; p < properties; p++) {
      final Object value = switch (random.nextInt(7)) {
        case 0, 1, 2 -> pool.get(random.nextInt(pool.size()));
        case 3 -> {
          // A container of the row's own that holds a shared one
          final List<Object> own = nulls(HeapEstimator.SAMPLED_ELEMENTS + 4);
          own.set(random.nextInt(HeapEstimator.SAMPLED_ELEMENTS), pool.get(random.nextInt(pool.size())));
          yield own;
        }
        case 4 -> nulls(3);
        case 5 -> {
          // A shared one one or two levels further down than when a row holds it as it is
          Object wrapped = pool.get(random.nextInt(pool.size()));
          for (int levels = 1 + random.nextInt(2); levels > 0; levels--)
            wrapped = List.of(wrapped);
          yield wrapped;
        }
        default -> "text" + random.nextInt(100);
      };
      row.setProperty("p" + p, value);
    }
    if (random.nextInt(8) == 0)
      // The same container under a second name
      row.setProperty("again", row.getProperty("p0"));
    return row;
  }

  private static List<Object> nulls(final int size) {
    return new ArrayList<>(Collections.nCopies(size, null));
  }

  private static ResultInternal row(final String name, final Object value, final String otherName, final Object otherValue) {
    final ResultInternal row = new ResultInternal();
    row.setProperty(name, value);
    row.setProperty(otherName, otherValue);
    return row;
  }
}
