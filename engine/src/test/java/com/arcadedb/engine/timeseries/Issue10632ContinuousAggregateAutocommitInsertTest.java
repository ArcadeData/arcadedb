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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10632: a SQL {@code INSERT} into a TIMESERIES type registered the refresh of the continuous aggregates over the type as an
 * after-commit callback of the current transaction. With no transaction open (an embedded autocommit {@code database.command()})
 * that callback never ran when the statement ended, so the aggregate was not refreshed by the INSERT at all; it stayed on the
 * inactive transaction context and ran at the next unrelated commit instead.
 * <p>
 * With no transaction open the aggregates are now refreshed once when the statement ends, also for a multi-row INSERT.
 */
class Issue10632ContinuousAggregateAutocommitInsertTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  /** An hour boundary, so that the buckets of the default grid start where the test says they do. */
  private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

  private record Bucket(long start, String tag, double total) {
  }

  private void createSourceAndAggregate() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
  }

  private void insert(final long ts, final String tag, final double v) {
    database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", ts, tag, v).close();
  }

  private ContinuousAggregateImpl aggregate() {
    return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
  }

  private List<Bucket> buckets() {
    final List<Bucket> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM A ORDER BY hr, h")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), row.getProperty("h"),
            ((Number) row.getProperty("total")).doubleValue()));
      }
    }
    return result;
  }

  // ---- the repro of the issue

  @Test
  void anAutocommitInsertRefreshesTheAggregate() {
    createSourceAndAggregate();
    assertThat(database.isTransactionActive()).isFalse();
    final long before = aggregate().getRefreshCount();

    insert(H0 + 1_000, "a", 10);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10));

    insert(H0 + HOUR + 1_000, "a", 20);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 2);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 20));
  }

  /** The issue's own sequence: two autocommit INSERTs, a REFRESH, then a further autocommit INSERT. */
  @Test
  void anAutocommitInsertAfterAnExplicitRefreshRefreshesTheAggregate() {
    createSourceAndAggregate();
    insert(H0 + 1_000, "a", 10);
    insert(H0 + HOUR + 1_000, "a", 20);
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
    final long before = aggregate().getRefreshCount();

    insert(H0 + HOUR + 2_000, "a", 5);

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 10), new Bucket(H0 + HOUR, "a", 25));
  }

  // ---- once per statement, not once per row

  @Test
  void aMultiRowAutocommitInsertRefreshesOncePerStatement() {
    createSourceAndAggregate();
    final long before = aggregate().getRefreshCount();

    database.command("sql", "INSERT INTO S CONTENT [" //
        + "{\"ts\": " + (H0 + 1_000) + ", \"h\": \"a\", \"v\": 1}," //
        + "{\"ts\": " + (H0 + 2_000) + ", \"h\": \"a\", \"v\": 2}," //
        + "{\"ts\": " + (H0 + HOUR + 1_000) + ", \"h\": \"b\", \"v\": 4}]").close();

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3), new Bucket(H0 + HOUR, "b", 4));
  }

  // ---- the transactional path keeps its after-commit refresh

  @Test
  void anInsertInsideATransactionRefreshesOnceAtCommit() {
    createSourceAndAggregate();
    final long before = aggregate().getRefreshCount();

    database.transaction(() -> {
      insert(H0 + 1_000, "a", 10);
      insert(H0 + 2_000, "a", 5);
      // not before the commit
      assertThat(aggregate().getRefreshCount()).isEqualTo(before);
    });

    assertThat(aggregate().getRefreshCount()).isEqualTo(before + 1);
    assertThat(buckets()).containsExactly(new Bucket(H0, "a", 15));
  }

  // ---- nothing is left behind on the inactive transaction context

  @Test
  void anAutocommitInsertLeavesNoCallbackForTheNextUnrelatedCommit() {
    createSourceAndAggregate();
    database.command("sql", "CREATE DOCUMENT TYPE Other");

    insert(H0 + 1_000, "a", 10);
    final long afterInsert = aggregate().getRefreshCount();

    assertThat(((DatabaseInternal) database).getTransaction().hasCallbackKey("ca-refresh:A")).isFalse();

    // a commit that touches nothing of S must not refresh the aggregate
    database.transaction(() -> database.newDocument("Other").set("x", 1).save());

    assertThat(aggregate().getRefreshCount()).isEqualTo(afterInsert);
  }
}
