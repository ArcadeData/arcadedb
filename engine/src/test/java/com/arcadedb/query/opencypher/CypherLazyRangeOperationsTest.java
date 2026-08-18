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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.LongRangeList;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6353: {@code range()} is lazy since advisory GHSA-xmjm-8q85-g778, but several
 * functions copied the range they were handed into an {@code ArrayList} before answering, which put the heap
 * exhaustion straight back. {@code arcadedb.queryMaxRangeSize} is documented as the cap on what a query may
 * <i>materialise</i>, so an operator may legitimately raise it - a range that stays lazy costs nothing at any
 * length - and every one of those copies then turned a free range into tens of GB of boxed longs.
 * <p>
 * A range beheaded, reversed, sorted, de-duplicated, flattened, or cut at either end is still an arithmetic
 * progression, so each of those has an exact answer in constant space. The proof that nothing is copied is the
 * type of the result: only a {@link LongRangeList} can describe a billion elements without allocating them.
 * The elapsed-time assertions are the second half of that proof - a copy cannot serve a billion-element range
 * in the time allowed, whether it finishes or dies trying.
 * <p>
 * {@code coll.insert()} and {@code coll.union()} are deliberately absent: inserting into a progression, or
 * merging two of them, does not give a progression back, so those copies are the answer rather than a defect.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLazyRangeOperationsTest {
  private static final String HUGE = "range(0, 999999999)";

  private Database database;
  private long     previousMaxRangeSize;

  @BeforeEach
  void setup() {
    previousMaxRangeSize = GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getValueAsLong();
    database = new DatabaseFactory("./target/databases/cypherlazyrangeops").create();
  }

  @AfterEach
  void teardown() {
    GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(previousMaxRangeSize);
    if (database != null)
      database.drop();
  }

  /**
   * The structural claim, and the one that cannot flake: each of these answers a range with a range. A copy
   * would answer with an ArrayList, and would be as big as the input.
   */
  @Test
  void everyExactAnswerStaysARange() {
    assertThat(single("RETURN tail(range(1, 5)) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN reverse(range(1, 5)) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.sort(range(1, 5)) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.sort(range(5, 1, -1)) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.distinct(range(1, 5)) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.toSet(range(1, 5)) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.flatten(range(1, 5)) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.flatten(range(1, 5), 0) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.remove(range(1, 5), 0, 2) AS r")).isInstanceOf(LongRangeList.class);
    assertThat(single("RETURN coll.remove(range(1, 5), 3, 2) AS r")).isInstanceOf(LongRangeList.class);
  }

  /**
   * The answers themselves, checked against the same operation on a materialised list of the same content.
   * Laziness that gets the answer wrong is worse than the copy it replaces.
   */
  @Test
  void theLazyAnswerIsTheMaterialisedAnswer() {
    assertThat(single("RETURN tail(range(3, 6)) AS r")).isEqualTo(List.of(4L, 5L, 6L));
    assertThat(single("RETURN tail(range(3, 3)) AS r")).isEqualTo(List.of());
    assertThat(single("RETURN tail(range(5, 1)) AS r")).isEqualTo(List.of());
    assertThat(single("RETURN tail(range(10, 4, -3)) AS r")).isEqualTo(List.of(7L, 4L));

    assertThat(single("RETURN reverse(range(1, 4)) AS r")).isEqualTo(List.of(4L, 3L, 2L, 1L));
    assertThat(single("RETURN reverse(range(1, 1)) AS r")).isEqualTo(List.of(1L));
    assertThat(single("RETURN reverse(range(5, 1)) AS r")).isEqualTo(List.of());
    assertThat(single("RETURN reverse(range(0, 10, 3)) AS r")).isEqualTo(List.of(9L, 6L, 3L, 0L));
    assertThat(single("RETURN reverse(range(10, 1, -3)) AS r")).isEqualTo(List.of(1L, 4L, 7L, 10L));
    assertThat(single("RETURN reverse(reverse(range(0, 10, 3))) AS r")).isEqualTo(List.of(0L, 3L, 6L, 9L));

    assertThat(single("RETURN coll.sort(range(1, 4)) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
    assertThat(single("RETURN coll.sort(range(10, 1, -3)) AS r")).isEqualTo(List.of(1L, 4L, 7L, 10L));
    assertThat(single("RETURN coll.sort(range(5, 1)) AS r")).isEqualTo(List.of());

    assertThat(single("RETURN coll.distinct(range(1, 4)) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
    assertThat(single("RETURN coll.toSet(range(1, 4)) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
    assertThat(single("RETURN coll.flatten(range(1, 4)) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
    assertThat(single("RETURN coll.flatten(range(1, 4), 3) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
    assertThat(single("RETURN coll.flatten(range(1, 4), 0) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
    assertThat(single("RETURN coll.flatten(range(1, 4), null) AS r")).isNull();

    // Prefix and suffix stay ranges, the middle cut does not - but all three must answer the same as the copy.
    assertThat(single("RETURN coll.remove(range(1, 5), 0) AS r")).isEqualTo(List.of(2L, 3L, 4L, 5L));
    assertThat(single("RETURN coll.remove(range(1, 5), 0, 2) AS r")).isEqualTo(List.of(3L, 4L, 5L));
    assertThat(single("RETURN coll.remove(range(1, 5), 4) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
    assertThat(single("RETURN coll.remove(range(1, 5), 3, 7) AS r")).isEqualTo(List.of(1L, 2L, 3L));
    assertThat(single("RETURN coll.remove(range(1, 5), 1, 2) AS r")).isEqualTo(List.of(1L, 4L, 5L));
    assertThat(single("RETURN coll.remove(range(1, 5), 0, 0) AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L, 5L));
    assertThat(single("RETURN coll.remove(range(1, 5), 0, 5) AS r")).isEqualTo(List.of());
  }

  /**
   * The reported cost. With the limit disabled a billion-element range is legal and free while it stays lazy;
   * a copy of it is ~24 GB of boxed longs, so none of these can be answered by one - the bound IS the claim,
   * and loosening it deletes the test. The {@code @Timeout} stays as a hang detector.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aHugeRangeIsNeverCopied() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, -1L);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(single("RETURN size(tail(" + HUGE + ")) AS r")).isEqualTo(999_999_999L);
    assertThat(single("RETURN head(tail(" + HUGE + ")) AS r")).isEqualTo(1L);
    assertThat(single("RETURN head(reverse(" + HUGE + ")) AS r")).isEqualTo(999_999_999L);
    assertThat(single("RETURN last(reverse(" + HUGE + ")) AS r")).isEqualTo(0L);
    assertThat(single("RETURN head(coll.sort(" + HUGE + ")) AS r")).isEqualTo(0L);
    assertThat(single("RETURN head(coll.sort(range(999999999, 0, -1))) AS r")).isEqualTo(0L);
    assertThat(single("RETURN size(coll.distinct(" + HUGE + ")) AS r")).isEqualTo(1_000_000_000L);
    assertThat(single("RETURN size(coll.toSet(" + HUGE + ")) AS r")).isEqualTo(1_000_000_000L);
    assertThat(single("RETURN size(coll.flatten(" + HUGE + ")) AS r")).isEqualTo(1_000_000_000L);
    assertThat(single("RETURN size(coll.remove(" + HUGE + ", 0, 10)) AS r")).isEqualTo(999_999_990L);
    assertThat(single("RETURN size(coll.remove(" + HUGE + ", 999999990, 10)) AS r")).isEqualTo(999_999_990L);
    // Not a copy but a walk: coll.indexOf() indexes the range, which LongRangeList answers by division.
    assertThat(single("RETURN coll.indexOf(" + HUGE + ", 999999998) AS r")).isEqualTo(999_999_998L);
    stopwatch.assertStayedUnder(5_000L, "twelve exact answers over a lazy range, none of them a billion-element copy");
  }

  /** A range carried through a variable or nested inside another lazy answer is still a range. */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void lazinessSurvivesComposition() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, -1L);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(single("WITH " + HUGE + " AS l RETURN size(tail(reverse(l))) AS r")).isEqualTo(999_999_999L);
    assertThat(single("RETURN head(coll.sort(reverse(tail(" + HUGE + ")))) AS r")).isEqualTo(1L);
    assertThat(single("RETURN size(tail(" + HUGE + ")[0..999999998]) AS r")).isEqualTo(999_999_998L);
    stopwatch.assertStayedUnder(5_000L, "composing exact answers keeps the range lazy instead of copying it once per step");
  }

  /** Nothing above changes what these functions do to anything that is not a range. */
  @Test
  void nonRangeArgumentsAreUnaffected() {
    assertThat(single("RETURN reverse('abc') AS r")).isEqualTo("cba");
    assertThat(single("RETURN reverse([1, 'a', true]) AS r")).isEqualTo(List.of(true, "a", 1L));
    assertThat(single("RETURN reverse(null) AS r")).isNull();
    assertThat(single("RETURN tail([1, 2, 3]) AS r")).isEqualTo(List.of(2L, 3L));
    assertThat(single("RETURN tail(null) AS r")).isNull();
    assertThat(single("RETURN coll.sort([3, 1, 2]) AS r")).isEqualTo(List.of(1L, 2L, 3L));
    assertThat(single("RETURN coll.distinct([1, 1, 2]) AS r")).isEqualTo(List.of(1L, 2L));
    assertThat(single("RETURN coll.flatten([[1, 2], [3]]) AS r")).isEqualTo(List.of(1L, 2L, 3L));
    assertThat(single("RETURN coll.remove([1, 2, 3], 0) AS r")).isEqualTo(List.of(2L, 3L));
    assertThat(single("RETURN coll.remove([1, 2, 3], 1, 2) AS r")).isEqualTo(List.of(1L));
    // A collected list is a plain ArrayList even when it was born as a range.
    assertThat(single("UNWIND range(1, 3) AS i WITH collect(i) AS l RETURN reverse(l) AS r")).isEqualTo(List.of(3L, 2L, 1L));
  }

  /** The result of a lazy answer must persist and render like any other list. */
  @Test
  void aLazyAnswerCanBeStoredAsAProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Holder {values: reverse(range(1, 4))})"));
    assertThat(single("MATCH (n:Holder) RETURN n.values AS r")).isEqualTo(List.of(4L, 3L, 2L, 1L));
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }
}
