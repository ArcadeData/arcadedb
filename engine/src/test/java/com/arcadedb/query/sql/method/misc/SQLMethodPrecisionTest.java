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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.NanoClock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.temporal.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodPrecisionTest {
  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodPrecision();
  }

  @Test
  void testRequiredArgs() {
    try {
      method.execute(null, null, null, null, new Object[] { null });
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }
  }

  @Test
  void testLocalDateTime() {
    final LocalDateTime now = LocalDateTime.now();

    Object result = method.execute(null, null, null, now, new String[] { "microsecond" });
    assertThat(result).isInstanceOf(LocalDateTime.class);
    Assertions.assertEquals(ChronoUnit.MICROS, DateUtils.getPrecision(((LocalDateTime) result).getNano()));

    result = method.execute(null, null, null, now, new String[] { "millisecond" });
    assertThat(result).isInstanceOf(LocalDateTime.class);
    Assertions.assertEquals(ChronoUnit.MILLIS, DateUtils.getPrecision(((LocalDateTime) result).getNano()));
  }

  @Test
  void testZonedDateTime() {
    final ZonedDateTime now = ZonedDateTime.now();

    Object result = method.execute(null, null, null, now, new String[] { "microsecond" });
    assertThat(result).isInstanceOf(ZonedDateTime.class);
    Assertions.assertEquals(ChronoUnit.MICROS, DateUtils.getPrecision(((ZonedDateTime) result).getNano()));

    result = method.execute(null, null, null, now, new String[] { "millisecond" });
    assertThat(result).isInstanceOf(ZonedDateTime.class);
    Assertions.assertEquals(ChronoUnit.MILLIS, DateUtils.getPrecision(((ZonedDateTime) result).getNano()));
  }

  @Test
  void testInstant() {
    final Instant now = new NanoClock().instant();

    Object result = method.execute(null, null, null, now, new String[] { "microsecond" });
    assertThat(result).isInstanceOf(Instant.class);
    Assertions.assertEquals(ChronoUnit.MICROS, DateUtils.getPrecision(((Instant) result).getNano()));

    result = method.execute(null, null, null, now, new String[] { "millisecond" });
    assertThat(result).isInstanceOf(Instant.class);
    Assertions.assertEquals(ChronoUnit.MILLIS, DateUtils.getPrecision(((Instant) result).getNano()));

    result = method.execute(null, null, null, now, new String[] { "nanosecond" });
    assertThat(result).isInstanceOf(Instant.class);
    Assertions.assertEquals(ChronoUnit.NANOS, DateUtils.getPrecision(((Instant) result).getNano()));
  }

  @Test
  void testDate() {
    final Date now = new Date();
    Object result = method.execute(null, null, null, now, new String[] { "millisecond" });
    assertThat(result).isInstanceOf(Date.class);
    Assertions.assertEquals(now, result);
  }

}
