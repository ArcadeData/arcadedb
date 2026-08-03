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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.LongRangeList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for advisory GHSA-xmjm-8q85-g778: {@code RETURN range(0, 9999999999)} materialised every
 * element into an ArrayList, so a single authenticated request exhausted the JVM heap with an
 * OutOfMemoryError and degraded the whole server.
 * <p>
 * The range is now a lazy list ({@link LongRangeList}) that occupies a constant amount of heap, and a range
 * bigger than {@code arcadedb.queryMaxRangeSize} is rejected up-front as a client error (HTTP 400) before a
 * single element is produced. Reference semantics (Neo4j): range() is lazy there too and refuses ranges longer
 * than what a list can hold.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRangeHeapExhaustionTest {
  private Database database;
  private long     previousMaxRangeSize;

  @BeforeEach
  void setup() {
    previousMaxRangeSize = GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getValueAsLong();
    database = new DatabaseFactory("./target/databases/cypherrangeghsaxmjm").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:Sample {num: 42})"));
  }

  @AfterEach
  void teardown() {
    GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(previousMaxRangeSize);
    if (database != null)
      database.drop();
  }

  /** The reported PoC: it must fail fast instead of filling the heap. */
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void reportedPocIsRejectedWithoutExhaustingHeap() {
    final Throwable thrown = catchThrowable(() -> consume("RETURN range(0, 9999999999) AS v"));
    assertThat(thrown).isNotNull();
    assertThat(rootMessage(thrown)).contains("10000000000").contains(GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getKey());
    // CommandSemanticException extends CommandParsingException, which the HTTP layer maps to 400 (not 500).
    assertThat(hasInChain(thrown, CommandParsingException.class)).isTrue();
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void oversizedRangeIsRejectedInsideUnwindToo() {
    final Throwable thrown = catchThrowable(() -> consume("UNWIND range(0, 9999999999) AS i RETURN count(i) AS c"));
    assertThat(thrown).isNotNull();
    assertThat(hasInChain(thrown, CommandParsingException.class)).isTrue();
  }

  /** The per-database limit is honoured, and the boundary value is still allowed. */
  @Test
  void configuredLimitIsEnforcedPerDatabase() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, 100L);

    assertThat(((List<?>) single("RETURN range(1, 100) AS r"))).hasSize(100);

    final Throwable thrown = catchThrowable(() -> consume("RETURN range(1, 101) AS r"));
    assertThat(thrown).isNotNull();
    assertThat(rootMessage(thrown)).contains("101 elements").contains("100");
  }

  /**
   * With the limit disabled the range stays usable but must remain lazy: a billion elements would need tens of
   * GB if materialised, so answering within the timeout is the proof that nothing is copied.
   */
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void hugeRangeIsLazyWhenTheLimitIsDisabled() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, -1L);
    assertThat(single("RETURN size(range(0, 999999999)) AS r")).isEqualTo(1_000_000_000L);
    assertThat(single("RETURN range(0, 999999999)[123456789] AS r")).isEqualTo(123_456_789L);
    assertThat(single("RETURN 999999998 IN range(0, 999999999) AS r")).isEqualTo(true);
  }

  /** A range within the limit is not copied into an ArrayList any more. */
  @Test
  void rangeIsReturnedAsALazyList() {
    assertThat(single("RETURN range(0, 4) AS r")).isInstanceOf(LongRangeList.class);
  }

  /** Every documented range() semantics must survive the rewrite. */
  @Test
  void semanticsArePreserved() {
    assertThat(single("RETURN range(0, 5) AS r")).isEqualTo(List.of(0L, 1L, 2L, 3L, 4L, 5L));
    assertThat(single("RETURN range(0, 10, 3) AS r")).isEqualTo(List.of(0L, 3L, 6L, 9L));
    assertThat(single("RETURN range(10, 1, -3) AS r")).isEqualTo(List.of(10L, 7L, 4L, 1L));
    assertThat(single("RETURN range(5, 1) AS r")).isEqualTo(List.of());
    assertThat(single("RETURN size(range(1, 100)) AS r")).isEqualTo(100L);
    assertThat(single("RETURN head(range(3, 9)) AS r")).isEqualTo(3L);
    assertThat(single("RETURN last(range(3, 9)) AS r")).isEqualTo(9L);
    assertThat(single("RETURN tail(range(3, 6)) AS r")).isEqualTo(List.of(4L, 5L, 6L));
    assertThat(single("RETURN reverse(range(1, 4)) AS r")).isEqualTo(List.of(4L, 3L, 2L, 1L));
    assertThat(single("RETURN [x IN range(1, 5) WHERE x % 2 = 0 | x * 10] AS r")).isEqualTo(List.of(20L, 40L));
  }

  @Test
  void unwindStillStreamsTheRange() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND range(1, 100000) AS i RETURN count(i) AS c, sum(i) AS s")) {
      final Result row = rs.next();
      assertThat((Object) row.getProperty("c")).isEqualTo(100000L);
      assertThat(((Number) row.getProperty("s")).longValue()).isEqualTo(5000050000L);
    }
  }

  /** A range stored in a record must be persisted like any other list. */
  @Test
  void rangeCanBeStoredAsAProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Holder {values: range(1, 4)})"));
    assertThat(single("MATCH (n:Holder) RETURN n.values AS r")).isEqualTo(List.of(1L, 2L, 3L, 4L));
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private static String rootMessage(final Throwable thrown) {
    final StringBuilder buffer = new StringBuilder();
    for (Throwable current = thrown; current != null; current = current.getCause())
      buffer.append(current.getMessage()).append(" | ");
    return buffer.toString();
  }

  private static boolean hasInChain(final Throwable thrown, final Class<? extends Throwable> type) {
    for (Throwable current = thrown; current != null; current = current.getCause())
      if (type.isInstance(current))
        return true;
    return false;
  }
}
