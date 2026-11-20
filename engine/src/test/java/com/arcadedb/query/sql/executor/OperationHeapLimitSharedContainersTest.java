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

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.arcadedb.query.sql.executor.HeapEstimator.HASH_ENTRY_BYTES;
import static com.arcadedb.query.sql.executor.HeapEstimator.OBJECT_BYTES;
import static com.arcadedb.query.sql.executor.HeapEstimator.REFERENCE_BYTES;
import static com.arcadedb.query.sql.executor.HeapEstimator.RESULT_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
    final WeakReference<List<Object>> first = new WeakReference<>(held);

    for (int i = 0; i < 100; i++) {
      final List<Object> next = nulls(2_000);
      limit.replace(held, next);
      held = next;
    }
    assertThat(limit.getChargedBytes()).isEqualTo(oneList);

    // The list that left the heap is not kept reachable by the operation
    await().atMost(Duration.ofSeconds(30)).until(() -> {
      System.gc();
      return first.get() == null;
    });
    limit.release();
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

    final long childBytes = HeapEstimator.estimate(child);
    assertThat(sampled.getChargedBytes()).as("the child is charged once, as when every row is estimated")
        .isBetween(exact.getChargedBytes() - childBytes, exact.getChargedBytes() + childBytes);
    sampled.release();
    exact.release();
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
