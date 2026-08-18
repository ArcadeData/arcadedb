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
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #6323 item 2: {@code IN} answered membership by walking the list element by element,
 * so against the lazy {@code range()} of advisory GHSA-xmjm-8q85-g778 the cost was the POSITION of the match -
 * instant for an element near the start, three seconds locally and about twenty on a CI runner for one near the
 * end of {@code range(0, 999999999)}, and the full walk for every miss. A range is an arithmetic progression:
 * membership in it is a division, and its elements are never null, so the three-valued logic that forces the walk
 * on a general list has nothing to discover here.
 * <p>
 * The walk is not replaced by {@code List.contains}: membership is equality, and Cypher's {@code =} coerces
 * numerically ({@code 5.0 = 5} is true, as it is in Neo4j) where {@code equals()} does not. What the fast path
 * must therefore preserve is the answer of the walk itself, which is what {@link #answersExactlyAsTheWalkDoes()}
 * pins down, operand by operand, against the same values in a materialised list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherInRangeMembershipTest {
  private Database database;
  private long     previousMaxRangeSize;

  @BeforeEach
  void setUp() {
    previousMaxRangeSize = GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getValueAsLong();
    database = new DatabaseFactory("./target/databases/cypher-in-range-membership").create();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(previousMaxRangeSize);
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * The answer against a range must be the answer against the same elements materialised, for every operand the
   * {@code =} comparator treats differently: numeric coercion, the RID-string interop, NaN, and the types that are
   * simply not equal to a number. This is the drift guard - the fast path decides some of these from the operand's
   * type alone, so a new coercion in {@code ComparisonExpression} that this one does not learn shows up here.
   */
  @Test
  void answersExactlyAsTheWalkDoes() {
    final List<Object> operands = new ArrayList<>(Arrays.asList(
        5L, 0L, 10L, 11L, -1L,                                  // longs inside, on both ends, and outside
        5.0d, 5.5d, 10.0d, 11.0d, -0.0d,                        // doubles: coerced numerically, fraction never matches
        Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
        (float) 3.0, (short) 3, (byte) 3, 3,                    // every boxed integral width reaches the same answer
        BigInteger.valueOf(7), new BigDecimal("7.0"), new BigDecimal("7.5"),
        BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN),
        "5", "#1:0", "not a rid", true, false,
        List.of(5L), Map.of("a", 5L),
        null));

    for (final Object operand : operands) {
      final Map<String, Object> parameters = new HashMap<>();
      parameters.put("v", operand);

      assertThat(single("RETURN $v IN range(0, 10) AS r", parameters))
          .as("%s IN range(0, 10)", operand)
          .isEqualTo(single("RETURN $v IN [0,1,2,3,4,5,6,7,8,9,10] AS r", parameters));

      assertThat(single("RETURN $v IN range(10, 0, -2) AS r", parameters))
          .as("%s IN range(10, 0, -2)", operand)
          .isEqualTo(single("RETURN $v IN [10,8,6,4,2,0] AS r", parameters));

      assertThat(single("RETURN NOT $v IN range(0, 10) AS r", parameters))
          .as("NOT %s IN range(0, 10)", operand)
          .isEqualTo(single("RETURN NOT $v IN [0,1,2,3,4,5,6,7,8,9,10] AS r", parameters));

      assertThat(single("RETURN $v IN range(5, 1) AS r", parameters))
          .as("%s IN <empty range>", operand)
          .isEqualTo(single("RETURN $v IN [] AS r", parameters));
    }
  }

  /** The three-valued logic a range cannot violate: no element of it is null, so only the left operand can be. */
  @Test
  void nullSemanticsAreUnchanged() {
    assertThat(single("RETURN null IN range(0, 10) AS r", Map.of())).isNull();
    assertThat(single("RETURN NOT null IN range(0, 10) AS r", Map.of())).isNull();
    assertThat(single("RETURN null IN range(5, 1) AS r", Map.of())).isEqualTo(false);
    assertThat(single("RETURN NOT null IN range(5, 1) AS r", Map.of())).isEqualTo(true);
    // A general list still reports what only the walk can know: an element that compares null-uncertain.
    assertThat(single("RETURN 5 IN [1, null, 3] AS r", Map.of())).isNull();
    assertThat(single("RETURN 1 IN [1, null, 3] AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN null IN [] AS r", Map.of())).isEqualTo(false);
  }

  /** Neo4j's numeric coercion: an integral double is the integer it denotes, a fractional one is nothing. */
  @Test
  void numericCoercionIsPreserved() {
    assertThat(single("RETURN 5.0 IN range(0, 10) AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN 5.5 IN range(0, 10) AS r", Map.of())).isEqualTo(false);
    assertThat(single("RETURN 6 IN range(0, 10, 2) AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN 5 IN range(0, 10, 2) AS r", Map.of())).isEqualTo(false);
    assertThat(single("RETURN -4 IN range(10, -10, -2) AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN -5 IN range(10, -10, -2) AS r", Map.of())).isEqualTo(false);
  }

  /**
   * The 2^53 boundary, where the fast path stops answering and hands back to the walk. It is inclusive, and that
   * is load-bearing rather than over-cautious: 2^53+1 is not representable as a double and rounds ties-to-even
   * down onto 2^53, so BOTH longs convert to the same double. A fast path that took 2^53 as unambiguous would
   * pick one of them and answer false for a range that holds only the other - which is what the second case here
   * would catch. The walk compares element to operand as doubles, so it says true for both.
   */
  @Test
  void theExactDoubleBoundaryIsAnsweredAsTheWalkAnswersIt() {
    final long twoTo53 = 1L << 53;
    assertThat((double) (twoTo53 + 1)).as("the premise: 2^53+1 rounds onto 2^53").isEqualTo((double) twoTo53);

    // A range that holds 2^53 but not 2^53+1, and one that holds 2^53+1 but not 2^53.
    final String holdsTheEven = "range(" + twoTo53 + ", " + (twoTo53 + 4) + ", 4)";
    final String holdsTheOdd = "range(" + (twoTo53 + 1) + ", " + (twoTo53 + 5) + ", 4)";
    final Map<String, Object> asDouble = Map.of("v", (double) twoTo53);

    assertThat(single("RETURN $v IN " + holdsTheEven + " AS r", asDouble))
        .isEqualTo(single("RETURN $v IN [" + twoTo53 + ", " + (twoTo53 + 4) + "] AS r", asDouble));
    assertThat(single("RETURN $v IN " + holdsTheOdd + " AS r", asDouble))
        .isEqualTo(single("RETURN $v IN [" + (twoTo53 + 1) + ", " + (twoTo53 + 5) + "] AS r", asDouble));

    // Below the boundary a double still names exactly one long, so the fast path answers it.
    assertThat(single("RETURN 9007199254740991.0 IN range(9007199254740991, 9007199254740991) AS r", Map.of()))
        .isEqualTo(true);
  }

  /**
   * The complexity claim, and the reported cost: none of these can be answered by a walk in the time allowed, and
   * loosening the bound deletes the test. The element asked for sits at the very end of the range, which is the
   * worst case for the walk and the same case as any other for the arithmetic.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void membershipInAHugeRangeIsConstantCost() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, -1L);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(single("RETURN 999999998 IN range(0, 999999999) AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN 1000000000 IN range(0, 999999999) AS r", Map.of())).isEqualTo(false);
    assertThat(single("RETURN 999999998.0 IN range(0, 999999999) AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN 0.5 IN range(0, 999999999) AS r", Map.of())).isEqualTo(false);
    assertThat(single("RETURN 'nope' IN range(0, 999999999) AS r", Map.of())).isEqualTo(false);
    assertThat(single("RETURN null IN range(0, 999999999) AS r", Map.of())).isNull();
    assertThat(single("RETURN NOT 999999998 IN range(0, 999999999) AS r", Map.of())).isEqualTo(false);
    stopwatch.assertStayedUnder(2_000L, "membership in a range is arithmetic, not a walk of a billion elements");
  }

  /** A general list keeps its walk: this is a fast path for ranges, not a change to IN. */
  @Test
  void generalListsStillWalk() {
    assertThat(single("RETURN 3 IN [1, 2, 3] AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN 4 IN [1, 2, 3] AS r", Map.of())).isEqualTo(false);
    assertThat(single("RETURN 'b' IN ['a', 'b'] AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN 3.0 IN [1, 2, 3] AS r", Map.of())).isEqualTo(true);
    assertThat(single("UNWIND range(1, 5) AS i WITH collect(i) AS l RETURN 3 IN l AS r", Map.of())).isEqualTo(true);
  }

  /** A range reached through a variable, a parameter or a slice is still a range. */
  @Test
  void rangeReachedIndirectlyIsStillFast() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, -1L);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(single("WITH range(0, 999999999) AS l RETURN 999999998 IN l AS r", Map.of())).isEqualTo(true);
    // Slicing a range used to copy the slice into an ArrayList, which for a slice this size exhausted the heap -
    // the very failure LongRangeList was introduced to remove (advisory GHSA-xmjm-8q85-g778). A slice of a range
    // is a range, and LongRangeList.subList already returns one.
    assertThat(single("RETURN 999999998 IN range(0, 999999999)[0..1000000000] AS r", Map.of())).isEqualTo(true);
    assertThat(single("RETURN size(range(0, 999999999)[10..1000000000]) AS r", Map.of())).isEqualTo(999_999_990L);
    stopwatch.assertStayedUnder(2_000L, "a range carried through a variable or a slice is still an arithmetic answer");
  }

  /**
   * A slice is evaluated by two callers - the AST node and the aggregation-aware evaluator - and both now answer a
   * malformed bound by saying what is wrong with the query, instead of one of them casting straight to a Number
   * and raising a bare ClassCastException.
   */
  @Test
  void aNonNumericSliceBoundIsReportedTheSameWayOnBothPaths() {
    for (final String query : new String[] {
        "RETURN range(0, 10)[$v..3] AS r",
        "RETURN collect(range(0, 10))[0][$v..3] AS r" }) {
      final Throwable thrown = catchThrowable(() -> single(query, Map.of("v", "not a number")));
      assertThat(thrown).as("%s", query).isNotNull();

      final StringBuilder messages = new StringBuilder();
      for (Throwable current = thrown; current != null; current = current.getCause())
        messages.append(current.getMessage()).append(" | ");
      assertThat(messages.toString()).as("%s", query).contains("Slice index must be a number");
    }
  }

  private Object single(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }
}
