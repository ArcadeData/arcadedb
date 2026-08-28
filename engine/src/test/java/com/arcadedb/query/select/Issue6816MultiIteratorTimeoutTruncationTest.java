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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.MultiIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
public class Issue6816MultiIteratorTimeoutTruncationTest extends TestHelper {

  private static final int ROWS = 100;

  public Issue6816MultiIteratorTimeoutTruncationTest() {
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
