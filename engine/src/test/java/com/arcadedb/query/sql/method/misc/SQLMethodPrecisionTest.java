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
import java.util.concurrent.*;

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
  void testLocalDateTime() throws Exception {
    testPrecision("microsecond", () -> LocalDateTime.now());
    testPrecision("millisecond", () -> LocalDateTime.now());
  }

  @Test
  void testZonedDateTime() throws Exception {
    testPrecision("microsecond", () -> ZonedDateTime.now());
    testPrecision("millisecond", () -> ZonedDateTime.now());
  }

  @Test
  void testInstant() throws Exception {
    testPrecision("microsecond", () -> new NanoClock().instant());
    testPrecision("millisecond", () -> new NanoClock().instant());
    testPrecision("nanosecond", () -> new NanoClock().instant());
  }

  @Test
  void testDate() {
    final Date now = new Date();
    Object result = method.execute(null, null, null, now, new String[] { "millisecond" });
    assertThat(result).isInstanceOf(Date.class);
    Assertions.assertEquals(now, result);
  }

  private void testPrecision(final String precisionAsString, final Callable<Object> getNow) throws Exception {
    final ChronoUnit precision = DateUtils.parsePrecision(precisionAsString);

    // NANOS COULD END WITH 000 AND THEREFORE HE TEST WON'T PASS, RETRY MULTIPLE TIME IN CASE
    Object result = null;
    for (int retry = 0; retry < 10; retry++) {
      final Object now = getNow.call();

      result = method.execute(null, null, null, now, new String[] { precisionAsString });
      assertThat(result).isInstanceOf(now.getClass());

      if (DateUtils.getPrecision(DateUtils.getNanos(result)) == precision)
        break;
    }
    Assertions.assertEquals(precision, DateUtils.getPrecision(DateUtils.getNanos(result)));
  }
}
