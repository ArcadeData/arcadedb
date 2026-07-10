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

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hardening regression test for issue #4539, complementing the white-box {@code AggregateProjectionCalculationStepTest}.
 * <p>
 * {@code AggregateProjectionCalculationStep#syncPull} used to return one extra row per batch ({@code localNext <= nRecords}
 * in {@code hasNext()} versus {@code localNext > nRecords} in {@code next()}). The overshoot is invisible to a consumer that
 * fully drains the result, because the {@code nextItem} cursor stays consistent, so it is only exercised end-to-end when a
 * downstream step consumes a single batch and trusts the requested batch size. {@code SkipExecutionStep} does exactly that:
 * it pulls {@code min(100, skip)} rows in a single block and discards the whole returned batch, so {@code GROUP BY ... SKIP n}
 * used to skip {@code n + 1} groups and return one row too few.
 * <p>
 * This test drives the real SQL pipeline (no mocked steps) to lock in the user-visible behaviour.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AggregateProjectionSkipBatchSizeTest extends TestHelper {

  @Test
  void groupByWithSkipReturnsCorrectRowCount() {
    database.getSchema().createDocumentType("Sale");

    // 10 distinct groups, one document each.
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Sale").set("region", "R" + i).set("amount", i).save();
    });

    // 10 groups: SKIP n must always leave exactly 10 - n groups. The aggregate batch feeds SKIP directly (no
    // re-batching step in between), so an off-by-one in the upstream batch would discard one valid group.
    assertThat(countGroupsWithSkip(0)).isEqualTo(10);
    assertThat(countGroupsWithSkip(1)).isEqualTo(9);
    assertThat(countGroupsWithSkip(3)).isEqualTo(7);
    assertThat(countGroupsWithSkip(5)).isEqualTo(5);
    assertThat(countGroupsWithSkip(9)).isEqualTo(1);
    assertThat(countGroupsWithSkip(10)).isEqualTo(0);
  }

  private int countGroupsWithSkip(final int skip) {
    int count = 0;
    try (final ResultSet result = database.query("sql",
        "SELECT region, count(*) AS c FROM Sale GROUP BY region SKIP " + skip)) {
      while (result.hasNext()) {
        result.next();
        count++;
      }
    }
    return count;
  }
}
