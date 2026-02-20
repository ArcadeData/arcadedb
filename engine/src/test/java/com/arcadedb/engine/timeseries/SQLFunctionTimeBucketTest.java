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

import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the time_bucket() SQL function.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionTimeBucketTest {

  private final SQLFunctionTimeBucket fn = new SQLFunctionTimeBucket();

  @Test
  public void testHourBucket() {
    // 2026-02-20T10:35:00Z -> should truncate to 2026-02-20T10:00:00Z
    final long ts = 1771580100000L; // ~2026-02-20T10:35:00Z
    final Date result = (Date) fn.execute(null, null, null, new Object[] { "1h", ts }, null);

    // Should be truncated to nearest hour
    assertThat(result.getTime() % 3600000L).isEqualTo(0L);
    assertThat(result.getTime()).isLessThanOrEqualTo(ts);
    assertThat(result.getTime()).isGreaterThan(ts - 3600000L);
  }

  @Test
  public void testMinuteBucket() {
    final long ts = 1771580100000L; // some timestamp
    final Date result = (Date) fn.execute(null, null, null, new Object[] { "5m", ts }, null);

    // Should be truncated to 5-minute boundary
    assertThat(result.getTime() % (5 * 60000L)).isEqualTo(0L);
    assertThat(result.getTime()).isLessThanOrEqualTo(ts);
  }

  @Test
  public void testSecondBucket() {
    final long ts = 1771580123456L;
    final Date result = (Date) fn.execute(null, null, null, new Object[] { "1s", ts }, null);

    assertThat(result.getTime() % 1000L).isEqualTo(0L);
    assertThat(result.getTime()).isLessThanOrEqualTo(ts);
  }

  @Test
  public void testDayBucket() {
    final long ts = 1771580100000L;
    final Date result = (Date) fn.execute(null, null, null, new Object[] { "1d", ts }, null);

    assertThat(result.getTime() % 86400000L).isEqualTo(0L);
    assertThat(result.getTime()).isLessThanOrEqualTo(ts);
  }

  @Test
  public void testWeekBucket() {
    final long ts = 1771580100000L;
    final Date result = (Date) fn.execute(null, null, null, new Object[] { "1w", ts }, null);

    assertThat(result.getTime() % (7 * 86400000L)).isEqualTo(0L);
    assertThat(result.getTime()).isLessThanOrEqualTo(ts);
  }

  @Test
  public void testWithDateObject() {
    final Date input = new Date(1771580100000L);
    final Date result = (Date) fn.execute(null, null, null, new Object[] { "1h", input }, null);

    assertThat(result.getTime() % 3600000L).isEqualTo(0L);
  }

  @Test
  public void testExactBoundary() {
    // Timestamp already on an hour boundary
    final long ts = 3600000L * 5; // exactly 05:00:00 UTC epoch
    final Date result = (Date) fn.execute(null, null, null, new Object[] { "1h", ts }, null);

    assertThat(result.getTime()).isEqualTo(ts);
  }

  @Test
  public void testInvalidInterval() {
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { "1x", 12345L }, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMissingParams() {
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { "1h" }, null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
