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
package com.arcadedb;

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #5149: {@code SELECT count(*)} is served from a cached per-bucket record counter
 * ({@code LocalBucket.cachedRecordCount}) while {@code count()}/{@code count(field)} does a full scan. When the
 * cached counter drifts, {@code count(*)} silently disagrees with the real record count, and no user-facing
 * command reconciled it. {@code CHECK DATABASE ... FIX} now repairs the drift.
 */
class Issue5149CountStarCacheDriftTest extends TestHelper {
  private static final int TOTAL = 100;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Customer");
      for (int i = 0; i < TOTAL; i++)
        database.command("sql", "INSERT INTO Customer SET id = ?", i);
    });
  }

  private long countStar() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Customer")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long countScan() {
    // count(@rid) does NOT match the count(*) fast-path optimization, so it performs a real record scan.
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM Customer")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void driftCachedCounter() {
    // Simulate a drifted cached counter (e.g. a dropped insert/delta or a stale persisted statistics.json)
    // by forcing every Customer bucket's cached record count to 0.
    for (final Bucket b : database.getSchema().getType("Customer").getBuckets(false))
      ((LocalBucket) b).setCachedRecordCount(0);
  }

  @Test
  void countStarDivergesFromScanWhenCachedCounterDrifts() {
    // Baseline: both paths agree before any drift.
    assertThat(countScan()).isEqualTo(TOTAL);
    assertThat(countStar()).isEqualTo(TOTAL);

    driftCachedCounter();

    // The bug: count(*) trusts the (now wrong) cached counter while the scan stays correct.
    assertThat(countScan()).isEqualTo(TOTAL);
    assertThat(countStar()).isEqualTo(0L);
  }

  @Test
  void checkDatabaseFixReconcilesDriftedCountStar() {
    driftCachedCounter();
    assertThat(countStar()).isEqualTo(0L);
    assertThat(countScan()).isEqualTo(TOTAL);

    database.command("sql", "CHECK DATABASE TYPE Customer FIX");

    // After the fix, count(*) is reconciled to the real record count.
    assertThat(countStar()).isEqualTo(TOTAL);
    assertThat(countStar()).isEqualTo(countScan());
  }

  @Test
  void checkDatabaseWithoutFixDoesNotMutateCachedCounter() {
    driftCachedCounter();

    // A read-only CHECK (no FIX) must not touch the cached counter.
    database.command("sql", "CHECK DATABASE TYPE Customer");

    for (final Bucket b : database.getSchema().getType("Customer").getBuckets(false))
      assertThat(((LocalBucket) b).getCachedRecordCount()).isEqualTo(0L);
  }
}
