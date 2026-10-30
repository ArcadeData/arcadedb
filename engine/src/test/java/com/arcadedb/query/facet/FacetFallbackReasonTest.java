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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.LogCapture;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Semaphore;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review A, answer 2c: a FACETS query that reads a bucket from its records counts once under the reason of its first scanned
 * facet and bucket ({@code query.fallback.scans} by {@code reason}, the sum unchanged), and logs one FINE line per scanned facet and
 * bucket naming the reason. A query refused the scan permit counts and logs no scan.
 */
class FacetFallbackReasonTest extends TestHelper {
  private static final String LINE = "FACETS facet '%s' of bucket 'Asset_0' (type 'Asset') answered by a record scan: %s";

  @BeforeEach
  void createAssets() throws InterruptedException {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE PROPERTY Asset.author STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument("Asset").set("brand", "b" + i % 3).set("author", "a" + i % 4).save();
    });
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
  }

  @Test
  void aPropertyWithoutAColumnarIndexScansUnderNoIndex() {
    try (final LogCapture log = new LogCapture()) {
      final FacetResults facets = facets("SELECT FROM Asset FACETS author TERMS, brand TERMS");
      assertThat(facets.get("author").source()).isEqualTo("scan");
      assertThat(facets.get("brand").source()).isEqualTo("index");
      assertCountedOnceUnder("no_index");
      assertThat(log.messages(Level.FINE, "answered by a record scan")).containsExactly(LINE.formatted("author", "no COLUMNAR index"));
    }
  }

  @Test
  void anExpressionFacetScansUnderExpression() {
    try (final LogCapture log = new LogCapture()) {
      assertThat(facets("SELECT FROM Asset FACETS x: brand.toLowerCase() TERMS").get("x").source()).isEqualTo("scan");
      assertCountedOnceUnder("expression");
      assertThat(log.messages(Level.FINE, "answered by a record scan")).containsExactly(LINE.formatted("x", "expression facet"));
    }
  }

  @Test
  void aTypeWithReadEventsScansUnderReadEvents() {
    final AfterRecordReadListener listener = record -> record;
    database.getSchema().getType("Asset").getEvents().registerListener(listener);
    try (final LogCapture log = new LogCapture()) {
      assertThat(facets("SELECT FROM Asset FACETS brand TERMS").get("brand").source()).isEqualTo("scan");
      assertCountedOnceUnder("read_events");
      assertThat(log.messages(Level.FINE, "answered by a record scan")).containsExactly(LINE.formatted("brand", "read events"));
    } finally {
      database.getSchema().getType("Asset").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void ownWritesOverThePatchBudgetScanUnderOwnWrites() {
    final long budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValueAsLong();
    final double ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValueAsDouble();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(0L);
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
    try (final LogCapture log = new LogCapture()) {
      database.begin();
      try {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Asset LIMIT 2")) {
          while (rs.hasNext())
            ((Document) rs.next().getRecord().orElseThrow()).modify().set("brand", "own").save();
        }
        final FacetResults facets = facets("SELECT FROM Asset FACETS brand TERMS");
        assertThat(facets.get("brand").source()).isEqualTo("scan");
        assertThat(facets.exact()).isTrue();
      } finally {
        database.rollback();
      }
      assertCountedOnceUnder("own_writes");
      assertThat(log.messages(Level.FINE, "answered by a record scan")).containsExactly(LINE.formatted("brand", "own writes over patch budget"));
    } finally {
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
  }

  @Test
  void aRefusedScanPermitCountsAndLogsNoScan() {
    final Semaphore permits = manager().fallbackScans();
    final int held = permits.drainPermits();
    try (final LogCapture log = new LogCapture()) {
      final FacetResult author = facets("SELECT FROM Asset FACETS author TERMS").get("author");
      assertThat(author.reason()).isEqualTo(FacetExecutor.FALLBACK_SCANS_CAPPED);
      assertThat(stats().queryFallbackScansCount()).isZero();
      assertThat(log.messages(Level.FINE, "answered by a record scan")).isEmpty();
    } finally {
      permits.release(held);
    }
  }

  private void assertCountedOnceUnder(final String reason) {
    assertThat(stats().queryFallbackScansCount()).as("the sum of every reason").isEqualTo(1);
    for (final String each : ColumnarStats.FALLBACK_SCAN_REASONS)
      assertThat(stats().queryFallbackScansCount(each)).as(each).isEqualTo(each.equals(reason) ? 1 : 0);
  }

  private FacetResults facets(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rs.next();
      return rs.getFacets();
    }
  }

  private ColumnarStats stats() {
    return manager().getStats();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
