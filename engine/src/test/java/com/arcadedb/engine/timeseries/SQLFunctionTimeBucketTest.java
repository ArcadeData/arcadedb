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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the time_bucket() SQL function.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionTimeBucketTest {

  private final SQLFunctionTimeBucket fn = new SQLFunctionTimeBucket();

  /**
   * Issue #7610: the function returns a UTC-anchored LocalDateTime, not a java.util.Date (see
   * SQLFunctionTimeBucket for why), so tests compare bucket boundaries as epoch millis.
   */
  private static long bucketMs(final Object result) {
    return ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  @Test
  void hourBucket() {
    // 2026-02-20T10:35:00Z -> should truncate to 2026-02-20T10:00:00Z
    final long ts = 1771580100000L; // ~2026-02-20T10:35:00Z
    final Object result = fn.execute(null, null, null, new Object[] { "1h", ts }, null);

    // Should be truncated to nearest hour
    assertThat(bucketMs(result) % 3600000L).isEqualTo(0L);
    assertThat(bucketMs(result)).isLessThanOrEqualTo(ts);
    assertThat(bucketMs(result)).isGreaterThan(ts - 3600000L);
  }

  @Test
  void minuteBucket() {
    final long ts = 1771580100000L; // some timestamp
    final Object result = fn.execute(null, null, null, new Object[] { "5m", ts }, null);

    // Should be truncated to 5-minute boundary
    assertThat(bucketMs(result) % (5 * 60000L)).isEqualTo(0L);
    assertThat(bucketMs(result)).isLessThanOrEqualTo(ts);
  }

  @Test
  void secondBucket() {
    final long ts = 1771580123456L;
    final Object result = fn.execute(null, null, null, new Object[] { "1s", ts }, null);

    assertThat(bucketMs(result) % 1000L).isEqualTo(0L);
    assertThat(bucketMs(result)).isLessThanOrEqualTo(ts);
  }

  @Test
  void dayBucket() {
    final long ts = 1771580100000L;
    final Object result = fn.execute(null, null, null, new Object[] { "1d", ts }, null);

    assertThat(bucketMs(result) % 86400000L).isEqualTo(0L);
    assertThat(bucketMs(result)).isLessThanOrEqualTo(ts);
  }

  @Test
  void weekBucket() {
    final long ts = 1771580100000L;
    final Object result = fn.execute(null, null, null, new Object[] { "1w", ts }, null);

    assertThat(bucketMs(result) % (7 * 86400000L)).isEqualTo(0L);
    assertThat(bucketMs(result)).isLessThanOrEqualTo(ts);
  }

  @Test
  void withDateObject() {
    final Date input = new Date(1771580100000L);
    final Object result = fn.execute(null, null, null, new Object[] { "1h", input }, null);

    assertThat(bucketMs(result) % 3600000L).isEqualTo(0L);
  }

  @Test
  void exactBoundary() {
    // Timestamp already on an hour boundary
    final long ts = 3600000L * 5; // exactly 05:00:00 UTC epoch
    final Object result = fn.execute(null, null, null, new Object[] { "1h", ts }, null);

    assertThat(bucketMs(result)).isEqualTo(ts);
  }

  @Test
  void invalidInterval() {
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { "1x", 12345L }, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void missingParams() {
    assertThatThrownBy(() -> fn.execute(null, null, null, new Object[] { "1h" }, null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
