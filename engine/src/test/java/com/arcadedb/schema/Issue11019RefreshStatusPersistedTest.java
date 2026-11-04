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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11019: a refresh that failed set the in-memory status to ERROR (or STALE) and nothing wrote it to the schema, so a clean
 * close and reopen republished VALID over the rows the failed refresh left behind. The status an operator reads is the status of
 * the last refresh that ran, whether or not the process that ran it is still alive.
 */
class Issue11019RefreshStatusPersistedTest extends TestHelper {

  private void createViewWithUniqueRegion(final String refreshMode) {
    database.command("sql", "CREATE DOCUMENT TYPE Sale");
    for (final String region : new String[] { "EU", "US", "APAC" })
      insertSale(region);
    database.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT region, amount FROM Sale REFRESH " + refreshMode);
    database.command("sql", "CREATE PROPERTY mv.region STRING");
    database.command("sql", "CREATE INDEX ON mv (region) UNIQUE");
  }

  private void insertSale(final String region) {
    database.transaction(() -> database.command("sql", "INSERT INTO Sale SET region = ?, amount = 10", region));
  }

  private void insertSample(final long ts) {
    database.transaction(() -> database.command("sql", "INSERT INTO S SET ts = ?, h = 's1', v = 1.0", ts));
  }

  private String viewStatus() {
    try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:materializedViews WHERE name = 'mv'")) {
      final Result row = rs.next();
      return row.getProperty("status");
    }
  }

  private String aggregateStatus() {
    try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:continuousAggregates WHERE name = 'ca'")) {
      final Result row = rs.next();
      return row.getProperty("status");
    }
  }

  @Test
  void aFailedManualRefreshStaysErrorAfterReopen() {
    createViewWithUniqueRegion("MANUAL");
    insertSale("EU");

    assertThatThrownBy(() -> database.command("sql", "REFRESH MATERIALIZED VIEW mv")).hasMessageContaining("Duplicated key [EU]");
    assertThat(viewStatus()).isEqualTo("ERROR");

    reopenDatabase();

    assertThat(viewStatus()).isEqualTo("ERROR");
  }

  @Test
  void aRefreshThatRecoversFromErrorIsValidAfterReopen() {
    createViewWithUniqueRegion("MANUAL");
    insertSale("EU");
    assertThatThrownBy(() -> database.command("sql", "REFRESH MATERIALIZED VIEW mv")).isNotNull();
    reopenDatabase();
    assertThat(viewStatus()).isEqualTo("ERROR");

    database.transaction(() -> database.command("sql", "DELETE FROM Sale WHERE region = 'EU' LIMIT 1"));
    database.command("sql", "REFRESH MATERIALIZED VIEW mv");
    assertThat(viewStatus()).isEqualTo("VALID");

    reopenDatabase();

    assertThat(viewStatus()).isEqualTo("VALID");
  }

  @Test
  void anIncrementalViewMarkedStaleStaysStaleAfterReopen() {
    createViewWithUniqueRegion("INCREMENTAL");

    // the commit of this insert triggers the incremental refresh, which hits the duplicate and marks the view STALE
    insertSale("EU");
    assertThat(viewStatus()).isEqualTo("STALE");

    reopenDatabase();

    assertThat(viewStatus()).isEqualTo("STALE");
  }

  @Test
  void aFailedContinuousAggregateRefreshStaysErrorAfterReopen() {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
    insertSample(1700000000000L);
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE ca AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
    database.command("sql", "CREATE PROPERTY ca.h STRING");
    database.command("sql", "CREATE INDEX ON ca (h) UNIQUE");
    assertThat(aggregateStatus()).isEqualTo("VALID");

    // a second bucket for the same tag: the unique index on h refuses it
    insertSample(1700000000000L + 7200000L);
    assertThatThrownBy(() -> database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca")).isNotNull();
    assertThat(aggregateStatus()).isEqualTo("ERROR");

    reopenDatabase();

    assertThat(aggregateStatus()).isEqualTo("ERROR");
  }
}
