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
import com.arcadedb.TestHelper;
import com.arcadedb.query.ParallelScanProducerPool;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A result set read in part and left open parks the producers of its parallel scan on their full channels, each holding a
 * thread of the JVM-wide producer pool until the result set is closed or the abandonment timeout (10 minutes) passes. Closing,
 * or dropping, the database did not release them: they kept the threads of a database that was gone for the whole timeout,
 * and every parallel scan of every other database of the JVM ran with fewer producers, or none, meanwhile. In the test suite
 * that was a scan whose test failed before closing it, and the next test classes that need the pool failed with it
 * ({@code Issue8594ParallelScanLimitReleaseTest} saw all of its threads busy).
 * <p>
 * The abandonment timeout is left at its default: a regression keeps the threads until the hang detector fires.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelScanReleasedOnDatabaseCloseTest extends TestHelper {
  private static final String TYPE_NAME = "Doc";
  // UNITS OF 8 PAGES, OVER 16,000 ROWS EACH, FAR MORE THAN A UNIT'S CHANNEL HOLDS (4096): A PRODUCER NOBODY DRAINS PARKS
  private static final int    RECORDS   = 100_000;

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void closingTheDatabaseReleasesTheProducersOfAScanLeftOpen() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 8);
    database.getSchema().createDocumentType(TYPE_NAME);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("id", i).save();
    });

    final String query = "SELECT FROM " + TYPE_NAME;
    try (final ResultSet plan = database.query("sql", "EXPLAIN " + query)) {
      assertThat(plan.next().<String>getProperty("executionPlanAsString")).as("the scan must run in parallel, or this test proves nothing")
          .contains("(parallel)");
    }

    // NEVER CLOSED, ON PURPOSE: ONE ROW READ, THE PRODUCERS FILL THEIR CHANNELS AND PARK
    final ResultSet rs = database.query("sql", query);
    assertThat(rs.hasNext()).isTrue();
    rs.next();
    Awaitility.await("the producers of the scan left open hold pool threads, or this test proves nothing").atMost(30, TimeUnit.SECONDS)
        .until(() -> ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() > 0);

    reopenDatabase();

    Awaitility.await("the producers of a closed database's scan give their pool threads back").atMost(30, TimeUnit.SECONDS)
        .until(() -> ParallelScanProducerPool.getInstance().getPoolStats().activeThreads() == 0);

    // A CONSUMER THAT COMES BACK FAILS INSTEAD OF WAITING FOR ROWS NO PRODUCER IS LEFT TO PRODUCE
    assertThatThrownBy(() -> {
      while (rs.hasNext())
        rs.next();
    }).as("a scan its database closed under it must fail, not answer a truncated result nor hang");

    // THE REOPENED DATABASE SCANS IN PARALLEL AS BEFORE
    try (final ResultSet count = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE id >= 0")) {
      assertThat(count.next().<Long>getProperty("c")).isEqualTo(RECORDS);
    }
  }
}
